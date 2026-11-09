#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

namespace nexstream::ingest::application {
struct CorrelationId { std::uint64_t connection_token{0}; std::uint64_t request_id{0}; };
struct TelemetryEvent {
    CorrelationId correlation;
    std::string key;
    std::string value;
    std::size_t bytes{0};
};
enum class EnqueueResult { kAccepted, kQueueFull, kUnavailable };
enum class DeliveryStatus { kSuccess, kRetryableFailure, kUnknown };
struct DeliveryCompletion { CorrelationId correlation; DeliveryStatus status; };

class IAsyncEventProducer {
  public:
    virtual ~IAsyncEventProducer() = default;
    [[nodiscard]] virtual EnqueueResult enqueue(TelemetryEvent event) = 0;
};
}  // namespace nexstream::ingest::application
