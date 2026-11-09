#include "nexstream/ingest/infrastructure/completion_channel.hpp"
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>

namespace nexstream::ingest::infrastructure {
CompletionChannel::CompletionChannel(std::size_t max_completions)
    : event_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      max_completions_(max_completions) {
    if (!event_fd_ || max_completions == 0) throw std::invalid_argument("invalid completion channel");
}
bool CompletionChannel::try_push(application::DeliveryCompletion completion) {
    {
        std::lock_guard lock(mutex_);
        if (queue_.size() >= max_completions_) { ++dropped_; return false; }
        queue_.push_back(std::move(completion));
    }
    const std::uint64_t one = 1;
    ssize_t result;
    do { result = ::write(event_fd_.get(), &one, sizeof(one)); } while (result < 0 && errno == EINTR);
    return result == static_cast<ssize_t>(sizeof(one)) || (result < 0 && errno == EAGAIN);
}
std::vector<application::DeliveryCompletion> CompletionChannel::drain(std::size_t max_count) {
    std::uint64_t value = 0;
    while (::read(event_fd_.get(), &value, sizeof(value)) < 0 && errno == EINTR) {}
    std::vector<application::DeliveryCompletion> output;
    std::lock_guard lock(mutex_);
    const auto count = std::min(max_count, queue_.size());
    output.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        output.push_back(std::move(queue_.front())); queue_.pop_front();
    }
    if (!queue_.empty()) {
        const std::uint64_t one = 1;
        (void)::write(event_fd_.get(), &one, sizeof(one));
    }
    return output;
}
std::size_t CompletionChannel::dropped() const noexcept {
    std::lock_guard lock(mutex_); return dropped_;
}
}  // namespace nexstream::ingest::infrastructure
