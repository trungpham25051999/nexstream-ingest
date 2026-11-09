#pragma once
#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>
#include "nexstream/ingest/application/event_producer.hpp"
#include "nexstream/ingest/infrastructure/unique_fd.hpp"

namespace nexstream::ingest::infrastructure {
class CompletionChannel {
  public:
    explicit CompletionChannel(std::size_t max_completions = 4'096);
    [[nodiscard]] int fd() const noexcept { return event_fd_.get(); }
    [[nodiscard]] bool try_push(application::DeliveryCompletion completion);
    [[nodiscard]] std::vector<application::DeliveryCompletion> drain(std::size_t max_count);
    [[nodiscard]] std::size_t dropped() const noexcept;
  private:
    UniqueFd event_fd_;
    const std::size_t max_completions_;
    mutable std::mutex mutex_;
    std::deque<application::DeliveryCompletion> queue_;
    std::size_t dropped_{0};
};
}  // namespace nexstream::ingest::infrastructure
