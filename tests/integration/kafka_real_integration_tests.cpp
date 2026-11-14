#include <poll.h>
#include "nexstream/ingest/infrastructure/kafka_producer.hpp"
#include "test_harness.hpp"

namespace app = nexstream::ingest::application;
namespace infra = nexstream::ingest::infrastructure;

int main() {
    TestHarness test;
    infra::CompletionChannel channel(8);
    infra::KafkaProducerConfig config;
    config.bootstrap_servers = NEXSTREAM_KAFKA_BROKER;
    config.topic = "telemetry.events.v1";
    config.max_pending_records = 8;
    config.max_pending_bytes = 16'384;
    config.message_timeout_ms = 5'000;
    infra::KafkaProducer producer(config, channel);
    const std::string payload =
        "{\"schema_version\":1,\"host_id\":\"real-kafka-host\","
        "\"message_id\":\"real-kafka-message\",\"sample_timestamp_ms\":1800000000000}";
    app::TelemetryEvent event{{77, 1}, "real-kafka-host", payload,
                              payload.size() + std::string("real-kafka-host").size()};
    test.expect(producer.enqueue(std::move(event)) == app::EnqueueResult::kAccepted,
                "real broker accepts async enqueue");
    pollfd descriptor{channel.fd(), POLLIN, 0};
    test.expect(::poll(&descriptor, 1, 10'000) == 1, "real broker emits delivery report");
    const auto completions = channel.drain(8);
    test.expect(completions.size() == 1 &&
                    completions[0].status == app::DeliveryStatus::kSuccess,
                "real broker delivery report confirms SUCCESS");
    test.expect(producer.pending_records() == 0, "real delivery releases pending record");
    return test.finish();
}
