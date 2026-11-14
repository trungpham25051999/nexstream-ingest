#include "nexstream/ingest/infrastructure/kafka_producer.hpp"
#include <librdkafka/rdkafka.h>
#include <array>
#include <stdexcept>
#include <utility>

namespace nexstream::ingest::infrastructure {
struct KafkaProducer::DeliveryContext {
    KafkaProducer* owner;
    application::CorrelationId correlation;
    std::size_t bytes;
};

namespace {
void set_config(rd_kafka_conf_t* conf, const char* key, const std::string& value) {
    std::array<char, 512> error{};
    if (rd_kafka_conf_set(conf, key, value.c_str(), error.data(), error.size()) != RD_KAFKA_CONF_OK)
        throw std::runtime_error(std::string("librdkafka config ") + key + ": " + error.data());
}
}  // namespace

KafkaProducer::KafkaProducer(KafkaProducerConfig config, CompletionChannel& completions)
    : config_(std::move(config)), completions_(completions) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    try {
        set_config(conf, "bootstrap.servers", config_.bootstrap_servers);
        set_config(conf, "acks", "all");
        set_config(conf, "enable.idempotence", "true");
        set_config(conf, "retries", "3");
        set_config(conf, "max.in.flight.requests.per.connection", "5");
        set_config(conf, "message.timeout.ms", std::to_string(config_.message_timeout_ms));
        set_config(conf, "queue.buffering.max.messages",
                   std::to_string(config_.max_pending_records));
        set_config(conf, "queue.buffering.max.kbytes",
                   std::to_string((config_.max_pending_bytes + 1023) / 1024));
    } catch (...) {
        rd_kafka_conf_destroy(conf); throw;
    }
    rd_kafka_conf_set_opaque(conf, this);
    rd_kafka_conf_set_dr_msg_cb(conf, &KafkaProducer::delivery_report);
    std::array<char, 512> error{};
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error.data(), error.size());
    if (!producer_) throw std::runtime_error(std::string("rd_kafka_new: ") + error.data());
    poll_thread_ = std::thread([this] { poll_loop(); });
}

KafkaProducer::~KafkaProducer() {
    running_.store(false, std::memory_order_release);
    if (poll_thread_.joinable()) poll_thread_.join();
    if (producer_) {
        rd_kafka_flush(producer_, 1'000);
        rd_kafka_destroy(producer_);
    }
}

application::EnqueueResult KafkaProducer::enqueue(application::TelemetryEvent event) {
    {
        std::lock_guard lock(pending_mutex_);
        if (pending_records_ >= config_.max_pending_records ||
            event.bytes > config_.max_pending_bytes - pending_bytes_)
            return application::EnqueueResult::kQueueFull;
        ++pending_records_;
        pending_bytes_ += event.bytes;
    }
    auto context = std::make_unique<DeliveryContext>(
        DeliveryContext{this, event.correlation, event.bytes});
    const auto error = rd_kafka_producev(
        producer_, RD_KAFKA_V_TOPIC(config_.topic.c_str()), RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_KEY(const_cast<char*>(event.key.data()), event.key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(event.value.data()), event.value.size()),
        RD_KAFKA_V_OPAQUE(context.get()), RD_KAFKA_V_END);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::lock_guard lock(pending_mutex_);
        --pending_records_; pending_bytes_ -= event.bytes;
        return error == RD_KAFKA_RESP_ERR__QUEUE_FULL ? application::EnqueueResult::kQueueFull
                                                      : application::EnqueueResult::kUnavailable;
    }
    context.release();
    return application::EnqueueResult::kAccepted;
}

void KafkaProducer::delivery_report(rd_kafka_s*, const rd_kafka_message_s* message, void*) {
    std::unique_ptr<DeliveryContext> context(
        static_cast<DeliveryContext*>(message->_private));
    if (!context) return;
    auto* owner = context->owner;
    {
        std::lock_guard lock(owner->pending_mutex_);
        --owner->pending_records_; owner->pending_bytes_ -= context->bytes;
    }
    auto status = application::DeliveryStatus::kRetryableFailure;
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) status = application::DeliveryStatus::kSuccess;
    else if (message->err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT ||
             message->err == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT ||
             message->err == RD_KAFKA_RESP_ERR__TIMED_OUT)
        status = application::DeliveryStatus::kUnknown;
    (void)owner->completions_.try_push({context->correlation, status});
}

void KafkaProducer::poll_loop() {
    while (running_.load(std::memory_order_acquire))
        rd_kafka_poll(producer_, config_.poll_timeout_ms);
}
std::size_t KafkaProducer::pending_records() const noexcept {
    std::lock_guard lock(pending_mutex_); return pending_records_;
}
std::size_t KafkaProducer::pending_bytes() const noexcept {
    std::lock_guard lock(pending_mutex_); return pending_bytes_;
}
}  // namespace nexstream::ingest::infrastructure
