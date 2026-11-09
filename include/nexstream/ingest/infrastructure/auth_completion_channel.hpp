#pragma once
#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>
#include "nexstream/ingest/application/authenticator.hpp"
#include "nexstream/ingest/infrastructure/unique_fd.hpp"

namespace nexstream::ingest::infrastructure {
class AuthCompletionChannel {
  public:
    explicit AuthCompletionChannel(std::size_t max_completions = 4'096);
    [[nodiscard]] bool try_push(application::AuthCompletion completion);
    [[nodiscard]] std::vector<application::AuthCompletion> drain(std::size_t max_count);
    [[nodiscard]] int fd() const noexcept { return event_fd_.get(); }
    [[nodiscard]] std::size_t dropped() const noexcept;
  private:
    UniqueFd event_fd_;
    const std::size_t max_completions_;
    mutable std::mutex mutex_;
    std::deque<application::AuthCompletion> queue_;
    std::size_t dropped_{0};
};
}  // namespace nexstream::ingest::infrastructure
