#include "nexstream/ingest/application/protocol_session.hpp"
#include "test_harness.hpp"
#include "mock_producer.hpp"
#include "mock_authenticator.hpp"

namespace transport = nexstream::transport::v1;
using nexstream::ingest::application::ProtocolSession;
using nexstream::ingest::application::SessionLimits;
using nexstream::ingest::application::SessionState;

namespace {
transport::Frame auth() {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_auth_request();
    value->set_agent_id("agent"); value->set_host_id("host"); value->set_key_id("key");
    value->set_secret("test-secret"); return frame;
}
transport::Frame start(std::uint32_t count = 1) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_batch_start();
    value->set_batch_id("batch"); value->set_host_id("host"); value->set_expected_count(count);
    return frame;
}
transport::Frame telemetry() {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_telemetry()->mutable_snapshot();
    value->set_host_id("host"); value->set_message_id("message");
    value->set_sample_timestamp_ms(1); value->set_cpu_usage_percent(10);
    value->set_memory_used_bytes(1); value->set_memory_total_bytes(2); return frame;
}
transport::Frame end(std::uint32_t count = 1) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_batch_end(); value->set_batch_id("batch");
    value->set_sent_count(count); return frame;
}
}  // namespace

int main() {
    TestHarness test;
    nexstream::ingest::infrastructure::CompletionChannel channel;
    MockProducer producer(channel);
    nexstream::ingest::infrastructure::AuthCompletionChannel auth_channel;
    MockAuthenticator authenticator(auth_channel);
    ProtocolSession invalid_order(producer, authenticator, 1);
    auto result = invalid_order.on_frame(start(), 20);
    test.expect(result.close_after_flush && result.responses[0].has_protocol_error(),
                "TC-TCP-010 BatchStart before auth rejected");

    ProtocolSession session(producer, authenticator, 2);
    result = session.on_frame(auth(), 30);
    test.expect(result.responses.empty(), "authentication is asynchronous");
    result = session.on_auth_completion(auth_channel.drain(1)[0]);
    test.expect(result.responses[0].auth_result().code() == transport::AUTHENTICATED,
                "registry auth completion accepts fixture");
    test.expect(session.on_frame(start(), 20).responses.empty(), "BatchStart accepted after auth");
    test.expect(session.on_frame(telemetry(), 60).responses.empty(), "telemetry accepted");
    result = session.on_frame(end(), 20);
    test.expect(result.responses.empty(), "BatchEnd waits for delivery report");
    result = session.on_delivery({producer.event(0).correlation,
                                  nexstream::ingest::application::DeliveryStatus::kSuccess});
    test.expect(result.close_after_flush && result.responses[0].has_batch_ack(),
                "delivery completion queues BatchAck");
    test.expect(result.responses[0].batch_ack().overall_result() == transport::SUCCESS,
                "SUCCESS only follows producer completion");

    ProtocolSession over_count(producer, authenticator, 3);
    result = over_count.on_frame(auth(), 30);
    result = over_count.on_auth_completion(auth_channel.drain(1)[0]);
    result = over_count.on_frame(start(51), 20);
    test.expect(result.responses[0].has_protocol_error(), "TC-TCP-011 sample count bounded");

    SessionLimits limits; limits.max_batch_bytes = 64;
    ProtocolSession over_bytes(producer, authenticator, 4, limits);
    result = over_bytes.on_frame(auth(), 30);
    result = over_bytes.on_auth_completion(auth_channel.drain(1)[0]);
    result = over_bytes.on_frame(start(), 20);
    result = over_bytes.on_frame(telemetry(), 65);
    test.expect(result.responses[0].has_protocol_error() &&
                    over_bytes.state() == SessionState::kFailed,
                "TC-TCP-011 aggregate batch bytes bounded");
    return test.finish();
}
