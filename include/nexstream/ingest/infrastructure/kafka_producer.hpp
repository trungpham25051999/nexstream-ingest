#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include "nexstream/ingest/application/event_producer.hpp"
#include "nexstream/ingest/infrastructure/completion_channel.hpp"

struct rd_kafka_s;
struct rd_kafka_message_s;

namespace nexstream::ingest::infrastructure {
struct KafkaProducerConfig {
    std::string bootstrap_servers{"127.0.0.1:9092"};
    std::string topic{"telemetry.events.v1"};
    std::size_t max_pending_records{4'096};
    std::size_t max_pending_bytes{16 * 1024 * 1024};
    int message_timeout_ms{3'000};
    int poll_timeout_ms{50};
};

class KafkaProducer final : public application::IAsyncEventProducer {
  public:
    KafkaProducer(KafkaProducerConfig config, CompletionChannel& completions);
    ~KafkaProducer() override;
    KafkaProducer(const KafkaProducer&) = delete;
    KafkaProducer& operator=(const KafkaProducer&) = delete;
    [[nodiscard]] application::EnqueueResult enqueue(application::TelemetryEvent event) override;
    [[nodiscard]] std::size_t pending_records() const noexcept;
    [[nodiscard]] std::size_t pending_bytes() const noexcept;
  private:
    struct DeliveryContext;
    static void delivery_report(rd_kafka_s*, const rd_kafka_message_s*, void*);
    void poll_loop();
    KafkaProducerConfig config_;
    CompletionChannel& completions_;
    rd_kafka_s* producer_{nullptr};
    std::atomic_bool running_{true};
    std::thread poll_thread_;
    mutable std::mutex pending_mutex_;
    std::size_t pending_records_{0};
    std::size_t pending_bytes_{0};
};
}  // namespace nexstream::ingest::infrastructure
