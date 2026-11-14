#include "nexstream/ingest/application/protocol_session.hpp"
#include "nexstream/ingest/infrastructure/completion_channel.hpp"
#include "mock_producer.hpp"
#include "mock_authenticator.hpp"
#include "test_harness.hpp"

namespace app = nexstream::ingest::application;
namespace transport = nexstream::transport::v1;
using nexstream::ingest::application::ProtocolSession;

namespace {
transport::Frame auth() {
    transport::Frame frame; frame.set_protocol_version(1); auto* value = frame.mutable_auth_request();
    value->set_agent_id("agent"); value->set_host_id("host"); value->set_key_id("key");
    value->set_secret("test-secret"); return frame;
}
transport::Frame start(std::uint32_t count) {
    transport::Frame frame; frame.set_protocol_version(1); auto* value = frame.mutable_batch_start();
    value->set_batch_id("batch"); value->set_host_id("host"); value->set_expected_count(count);
    return frame;
}
transport::Frame telemetry(std::uint32_t index) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_telemetry()->mutable_snapshot();
    value->set_host_id("host"); value->set_message_id("message-" + std::to_string(index));
    value->set_sample_timestamp_ms(index + 1); value->set_cpu_usage_percent(10);
    value->set_memory_used_bytes(1); value->set_memory_total_bytes(2); return frame;
}
transport::Frame end(std::uint32_t count) {
    transport::Frame frame; frame.set_protocol_version(1); auto* value = frame.mutable_batch_end();
    value->set_batch_id("batch"); value->set_sent_count(count); return frame;
}
void open_batch(ProtocolSession& session, nexstream::ingest::infrastructure::AuthCompletionChannel& channel,
                std::uint32_t count) {
    (void)session.on_frame(auth(), 20); (void)session.on_auth_completion(channel.drain(1)[0]);
    (void)session.on_frame(start(count), 20);
    for (std::uint32_t i = 0; i < count; ++i) (void)session.on_frame(telemetry(i), 60);
}
}  // namespace

int main() {
    TestHarness test;
    nexstream::ingest::infrastructure::CompletionChannel channel(4);
    MockProducer producer(channel);
    nexstream::ingest::infrastructure::AuthCompletionChannel auth_channel;
    MockAuthenticator authenticator(auth_channel);
    ProtocolSession session(producer, authenticator, 101);
    open_batch(session, auth_channel, 3);
    auto result = session.on_delivery({producer.event(1).correlation, app::DeliveryStatus::kSuccess});
    test.expect(result.responses.empty(), "out-of-order completion waits for remaining records");
    result = session.on_delivery({producer.event(0).correlation, app::DeliveryStatus::kUnknown});
    test.expect(result.responses.empty(), "completion before BatchEnd does not ACK");
    result = session.on_frame(end(3), 20);
    test.expect(result.responses.empty(), "BatchEnd waits for unresolved delivery");
    result = session.on_delivery({producer.event(2).correlation,
                                  app::DeliveryStatus::kRetryableFailure});
    test.expect(result.responses.size() == 1 && result.responses[0].has_batch_ack(),
                "all delivery reports finalize ACK");
    const auto& ack = result.responses[0].batch_ack();
    test.expect(ack.overall_result() == transport::UNKNOWN,
                "partial result with unknown has UNKNOWN aggregate");
    test.expect(ack.message_results(0).code() == transport::UNKNOWN &&
                    ack.message_results(1).code() == transport::SUCCESS &&
                    ack.message_results(2).code() == transport::RETRYABLE_FAILURE,
                "per-record partial results preserved in original order");

    MockProducer full_producer(channel, MockProducer::Mode::kQueueFull);
    ProtocolSession full_session(full_producer, authenticator, 102);
    open_batch(full_session, auth_channel, 1);
    result = full_session.on_frame(end(1), 20);
    test.expect(result.responses[0].batch_ack().overall_result() == transport::RETRY_LATER,
                "producer queue full fast-rejects with RETRY_LATER");

    nexstream::ingest::infrastructure::CompletionChannel bounded(1);
    test.expect(bounded.try_push({{1, 1}, app::DeliveryStatus::kSuccess}),
                "first completion admitted");
    test.expect(!bounded.try_push({{1, 2}, app::DeliveryStatus::kSuccess}) && bounded.dropped() == 1,
                "completion queue is bounded and reports drop");
    test.expect(bounded.drain(1).size() == 1, "bounded completion drains safely");

    result = full_session.on_delivery({{999, 1}, app::DeliveryStatus::kSuccess});
    test.expect(result.responses.empty(), "wrong connection correlation is ignored");
    return test.finish();
}
