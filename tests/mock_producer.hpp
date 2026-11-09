#pragma once
#include <cstddef>
#include <utility>
#include <vector>
#include <mutex>
#include "nexstream/ingest/application/event_producer.hpp"
#include "nexstream/ingest/infrastructure/completion_channel.hpp"

class MockProducer final : public nexstream::ingest::application::IAsyncEventProducer {
  public:
    enum class Mode { kManual, kImmediateSuccess, kImmediateUnknown, kImmediateFailure,
                      kQueueFull, kUnavailable };
    MockProducer(nexstream::ingest::infrastructure::CompletionChannel& channel,
                 Mode mode = Mode::kManual, std::size_t capacity = 4'096)
        : channel_(channel), mode_(mode), capacity_(capacity) {}
    nexstream::ingest::application::EnqueueResult enqueue(
        nexstream::ingest::application::TelemetryEvent event) override {
        using namespace nexstream::ingest::application;
        std::lock_guard lock(mutex_);
        if (mode_ == Mode::kQueueFull || events_.size() >= capacity_) return EnqueueResult::kQueueFull;
        if (mode_ == Mode::kUnavailable) return EnqueueResult::kUnavailable;
        events_.push_back(std::move(event));
        if (mode_ != Mode::kManual) {
            const auto status = mode_ == Mode::kImmediateSuccess ? DeliveryStatus::kSuccess
                : mode_ == Mode::kImmediateUnknown ? DeliveryStatus::kUnknown
                                                   : DeliveryStatus::kRetryableFailure;
            (void)channel_.try_push({events_.back().correlation, status});
        }
        return EnqueueResult::kAccepted;
    }
    std::size_t event_count() const { std::lock_guard lock(mutex_); return events_.size(); }
    nexstream::ingest::application::TelemetryEvent event(std::size_t index) const {
        std::lock_guard lock(mutex_); return events_.at(index);
    }
  private:
    nexstream::ingest::infrastructure::CompletionChannel& channel_;
    Mode mode_;
    std::size_t capacity_;
    std::vector<nexstream::ingest::application::TelemetryEvent> events_;
    mutable std::mutex mutex_;
};
