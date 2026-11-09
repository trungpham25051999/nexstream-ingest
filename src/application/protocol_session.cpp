#include "nexstream/ingest/application/protocol_session.hpp"
#include <cmath>
#include <iomanip>
#include <locale>
#include <sstream>
#include <utility>

namespace nexstream::ingest::application {
namespace transport = nexstream::transport::v1;

namespace {
std::string json_escape(const std::string& input) {
    std::ostringstream output;
    for (const unsigned char value : input) {
        switch (value) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (value < 0x20U) output << "\\u" << std::hex << std::setw(4)
                                          << std::setfill('0') << static_cast<int>(value);
                else output << static_cast<char>(value);
        }
    }
    return output.str();
}
std::string event_json(const nexstream::telemetry::v1::SystemMetricsSnapshot& value) {
    std::ostringstream output;
    output.imbue(std::locale::classic());
    output << "{\"schema_version\":1,\"host_id\":\"" << json_escape(value.host_id())
           << "\",\"message_id\":\"" << json_escape(value.message_id())
           << "\",\"sample_timestamp_ms\":" << value.sample_timestamp_ms()
           << ",\"cpu_usage_percent\":" << value.cpu_usage_percent()
           << ",\"memory_used_bytes\":" << value.memory_used_bytes()
           << ",\"memory_total_bytes\":" << value.memory_total_bytes() << '}';
    return output.str();
}
}  // namespace

ProtocolSession::ProtocolSession(IAsyncEventProducer& producer, IAsyncAuthenticator& authenticator,
                                 std::uint64_t connection_token, SessionLimits limits)
    : producer_(producer), authenticator_(authenticator), connection_token_(connection_token), limits_(limits) {}

bool ProtocolSession::valid_common(const transport::Frame& frame) const {
    return frame.protocol_version() == 1 && frame.body_case() != transport::Frame::BODY_NOT_SET &&
           frame.ByteSizeLong() <= 65'536;
}

SessionResult ProtocolSession::fail(std::string detail) {
    state_ = SessionState::kFailed;
    transport::Frame response;
    response.set_protocol_version(1);
    auto* error = response.mutable_protocol_error();
    error->set_code(transport::PERMANENT_FAILURE);
    error->set_detail(std::move(detail));
    return {{std::move(response)}, true};
}

SessionResult ProtocolSession::on_frame(const transport::Frame& frame, std::size_t wire_bytes) {
    if (!valid_common(frame)) return fail("invalid contract frame");
    if (state_ == SessionState::kFailed || state_ == SessionState::kComplete)
        return fail("frame after terminal state");
    if (state_ == SessionState::kAwaitDelivery)
        return fail("frame after BatchEnd");

    if (state_ == SessionState::kAwaitAuth) {
        if (!frame.has_auth_request()) return fail("AuthRequest required");
        const auto& auth = frame.auth_request();
        const bool valid_fields = !auth.agent_id().empty() && !auth.host_id().empty() &&
            !auth.key_id().empty() && !auth.secret().empty() &&
            auth.agent_id().size() <= limits_.max_identifier_bytes &&
            auth.host_id().size() <= limits_.max_identifier_bytes &&
            auth.key_id().size() <= limits_.max_identifier_bytes &&
            auth.secret().size() <= limits_.max_secret_bytes;
        if (!valid_fields) return fail("invalid AuthRequest");
        AuthAttempt attempt{{connection_token_}, auth.agent_id(), auth.host_id(), auth.key_id(), auth.secret()};
        const auto enqueue = authenticator_.authenticate(std::move(attempt));
        if (enqueue != AuthEnqueueResult::kAccepted) {
            transport::Frame response; response.set_protocol_version(1);
            auto* result = response.mutable_auth_result(); result->set_code(transport::AUTH_RETRY_LATER);
            result->set_detail("AUTH_BACKPRESSURE"); state_ = SessionState::kFailed;
            return {{std::move(response)}, true};
        }
        host_id_ = auth.host_id(); state_ = SessionState::kAwaitAuthCompletion; return {};
    }

    if (state_ == SessionState::kAwaitAuthCompletion) return fail("authentication pending");

    if (state_ == SessionState::kAwaitBatchStart) {
        if (!frame.has_batch_start()) return fail("BatchStart required");
        const auto& start = frame.batch_start();
        if (start.batch_id().empty() || start.batch_id().size() > limits_.max_identifier_bytes ||
            start.host_id() != host_id_ || start.expected_count() > limits_.max_batch_samples)
            return fail("invalid BatchStart");
        batch_id_ = start.batch_id();
        expected_count_ = start.expected_count();
        batch_bytes_ = wire_bytes;
        state_ = SessionState::kReceivingBatch;
        return {};
    }

    if (frame.has_telemetry()) {
        if (!frame.telemetry().has_snapshot()) return fail("snapshot required");
        const auto& snapshot = frame.telemetry().snapshot();
        if (snapshot.host_id() != host_id_ || snapshot.message_id().empty() ||
            snapshot.message_id().size() > limits_.max_identifier_bytes ||
            snapshot.sample_timestamp_ms() <= 0 ||
            !std::isfinite(snapshot.cpu_usage_percent()) ||
            snapshot.cpu_usage_percent() < 0.0 || snapshot.cpu_usage_percent() > 100.0 ||
            snapshot.memory_used_bytes() > snapshot.memory_total_bytes())
            return fail("invalid telemetry");
        if (deliveries_.size() >= limits_.max_batch_samples ||
            wire_bytes > limits_.max_batch_bytes - batch_bytes_)
            return fail("batch limit exceeded");
        batch_bytes_ += wire_bytes;
        const auto request_id = next_request_id_++;
        auto payload = event_json(snapshot);
        TelemetryEvent event{{connection_token_, request_id}, snapshot.host_id(), std::move(payload), 0};
        event.bytes = event.key.size() + event.value.size();
        const auto enqueue = producer_.enqueue(std::move(event));
        const auto code = enqueue == EnqueueResult::kQueueFull ? transport::RETRY_LATER
            : enqueue == EnqueueResult::kUnavailable ? transport::RETRYABLE_FAILURE
                                                     : transport::UNKNOWN;
        deliveries_.push_back({request_id, snapshot.message_id(), code,
                               enqueue != EnqueueResult::kAccepted});
        return {};
    }

    if (!frame.has_batch_end()) return fail("Telemetry or BatchEnd required");
    const auto& end = frame.batch_end();
    if (wire_bytes > limits_.max_batch_bytes - batch_bytes_ ||
        end.batch_id() != batch_id_ || end.sent_count() != deliveries_.size() ||
        (expected_count_ != 0 && expected_count_ != deliveries_.size()))
        return fail("batch count or identity mismatch");
    batch_ended_ = true;
    state_ = SessionState::kAwaitDelivery;
    return finalize_if_ready();
}

SessionResult ProtocolSession::on_auth_completion(const AuthCompletion& completion) {
    if (completion.correlation.connection_token != connection_token_ ||
        state_ != SessionState::kAwaitAuthCompletion) return {};
    transport::Frame response; response.set_protocol_version(1); auto* result = response.mutable_auth_result();
    if (completion.status == AuthStatus::kAuthenticated) {
        result->set_code(transport::AUTHENTICATED); result->set_detail("REGISTRY_AUTHENTICATED");
        state_ = SessionState::kAwaitBatchStart; return {{std::move(response)}, false};
    }
    if (completion.status == AuthStatus::kHostNotAuthorized) result->set_code(transport::HOST_NOT_AUTHORIZED);
    else if (completion.status == AuthStatus::kRetryLater) result->set_code(transport::AUTH_RETRY_LATER);
    else result->set_code(transport::INVALID_CREDENTIALS);
    result->set_detail("REGISTRY_AUTH_REJECTED"); state_ = SessionState::kFailed;
    return {{std::move(response)}, true};
}

SessionResult ProtocolSession::on_delivery(const DeliveryCompletion& completion) {
    if (completion.correlation.connection_token != connection_token_ ||
        state_ == SessionState::kFailed || state_ == SessionState::kComplete)
        return {};
    for (auto& delivery : deliveries_) {
        if (delivery.request_id != completion.correlation.request_id || delivery.resolved) continue;
        delivery.resolved = true;
        delivery.code = completion.status == DeliveryStatus::kSuccess ? transport::SUCCESS
            : completion.status == DeliveryStatus::kUnknown ? transport::UNKNOWN
                                                            : transport::RETRYABLE_FAILURE;
        break;
    }
    return finalize_if_ready();
}

SessionResult ProtocolSession::finalize_if_ready() {
    if (!batch_ended_) return {};
    for (const auto& delivery : deliveries_) if (!delivery.resolved) return {};
    state_ = SessionState::kComplete;
    transport::Frame response;
    response.set_protocol_version(1);
    auto* ack = response.mutable_batch_ack();
    ack->set_batch_id(batch_id_);
    auto overall = transport::SUCCESS;
    for (const auto& delivery : deliveries_) {
        if (delivery.code == transport::UNKNOWN) { overall = transport::UNKNOWN; break; }
        if (delivery.code == transport::RETRYABLE_FAILURE) overall = transport::RETRYABLE_FAILURE;
        else if (delivery.code == transport::RETRY_LATER && overall == transport::SUCCESS)
            overall = transport::RETRY_LATER;
    }
    ack->set_overall_result(overall);
    for (const auto& delivery : deliveries_) {
        auto* item = ack->add_message_results();
        item->set_message_id(delivery.message_id);
        item->set_code(delivery.code);
        item->set_detail(delivery.code == transport::SUCCESS ? "KAFKA_DELIVERY_CONFIRMED"
                                                              : "KAFKA_DELIVERY_NOT_CONFIRMED");
    }
    return {{std::move(response)}, true};
}
}  // namespace nexstream::ingest::application
