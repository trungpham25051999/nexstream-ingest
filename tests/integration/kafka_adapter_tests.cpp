#include <chrono>
#include <poll.h>
#include "nexstream/ingest/infrastructure/kafka_producer.hpp"
#include "test_harness.hpp"

namespace app = nexstream::ingest::application;
namespace infra = nexstream::ingest::infrastructure;

int main() {
    TestHarness test;
    infra::CompletionChannel channel(8);
    infra::KafkaProducerConfig config;
    config.bootstrap_servers = "127.0.0.1:1";
    config.topic = "telemetry.events.v1";
    config.max_pending_records = 1;
    config.max_pending_bytes = 1'024;
    config.message_timeout_ms = 300;
    config.poll_timeout_ms = 10;
    infra::KafkaProducer producer(config, channel);

    app::TelemetryEvent first{{55, 1}, "host", "{\"message_id\":\"m1\"}", 26};
    test.expect(producer.enqueue(first) == app::EnqueueResult::kAccepted,
                "real adapter accepts bounded async enqueue while broker unavailable");
    app::TelemetryEvent second{{55, 2}, "host", "{\"message_id\":\"m2\"}", 26};
    test.expect(producer.enqueue(second) == app::EnqueueResult::kQueueFull,
                "real adapter admission control fast-rejects queue full");

    pollfd descriptor{channel.fd(), POLLIN, 0};
    test.expect(::poll(&descriptor, 1, 3'000) == 1, "unavailable broker resolves by message timeout");
    const auto completions = channel.drain(8);
    test.expect(completions.size() == 1 &&
                    completions[0].status == app::DeliveryStatus::kUnknown,
                "Kafka timeout remains UNKNOWN, never SUCCESS");
    test.expect(producer.pending_records() == 0 && producer.pending_bytes() == 0,
                "delivery callback releases pending budgets");
    return test.finish();
}
