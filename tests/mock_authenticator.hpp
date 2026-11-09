#pragma once
#include <mutex>
#include <vector>
#include "nexstream/ingest/application/authenticator.hpp"
#include "nexstream/ingest/infrastructure/auth_completion_channel.hpp"

class MockAuthenticator final : public nexstream::ingest::application::IAsyncAuthenticator {
  public:
    explicit MockAuthenticator(nexstream::ingest::infrastructure::AuthCompletionChannel& channel,
        nexstream::ingest::application::AuthStatus status = nexstream::ingest::application::AuthStatus::kAuthenticated,
        bool drop_completion = false)
        : channel_(channel), status_(status), drop_completion_(drop_completion) {}
    nexstream::ingest::application::AuthEnqueueResult authenticate(
        nexstream::ingest::application::AuthAttempt attempt) override {
        bool drop = false;
        { std::lock_guard lock(mutex_); attempts_.push_back(attempt); drop = drop_completion_; }
        if (drop) return nexstream::ingest::application::AuthEnqueueResult::kAccepted;
        return channel_.try_push({attempt.correlation, status_})
            ? nexstream::ingest::application::AuthEnqueueResult::kAccepted
            : nexstream::ingest::application::AuthEnqueueResult::kQueueFull;
    }
    [[nodiscard]] std::uint64_t last_token() const {
        std::lock_guard lock(mutex_); return attempts_.empty() ? 0 : attempts_.back().correlation.connection_token;
    }
    void set_drop(bool value) { std::lock_guard lock(mutex_); drop_completion_ = value; }
  private:
    nexstream::ingest::infrastructure::AuthCompletionChannel& channel_;
    nexstream::ingest::application::AuthStatus status_;
    bool drop_completion_;
    mutable std::mutex mutex_;
    std::vector<nexstream::ingest::application::AuthAttempt> attempts_;
};
