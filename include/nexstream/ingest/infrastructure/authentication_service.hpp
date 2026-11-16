#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "nexstream/ingest/application/authenticator.hpp"
#include "nexstream/ingest/application/credential_registry.hpp"
#include "nexstream/ingest/infrastructure/auth_completion_channel.hpp"
#include "nexstream/ingest/infrastructure/credential_cache.hpp"

namespace nexstream::ingest::infrastructure {
struct AuthenticationConfig {
    std::size_t worker_count{4};
    std::size_t max_queued_attempts{1'024};
    std::size_t max_queued_secret_bytes{512 * 1'024};
    std::size_t max_cache_entries{4'096};
    std::chrono::milliseconds cache_ttl{5'000};
    std::chrono::milliseconds reconcile_interval{1'000};
};
class AuthenticationService final : public application::IAsyncAuthenticator {
  public:
    AuthenticationService(std::shared_ptr<application::ICredentialRegistry> registry,
                          AuthCompletionChannel& completions, AuthenticationConfig config = {});
    ~AuthenticationService() override;
    [[nodiscard]] application::AuthEnqueueResult authenticate(application::AuthAttempt attempt) override;
    void invalidate(const std::string& key_id);
    [[nodiscard]] std::size_t cache_size() const noexcept { return cache_.size(); }
    [[nodiscard]] std::size_t queued_attempts() const noexcept;
    [[nodiscard]] std::uint64_t reconciliation_count() const noexcept { return reconciliations_.load(); }
  private:
    void worker_loop();
    void invalidation_loop();
    [[nodiscard]] application::AuthStatus verify(const application::AuthAttempt& attempt,
                                                  const application::CredentialVerifier& credential) const;
    static void wipe(std::string& value) noexcept;
    std::shared_ptr<application::ICredentialRegistry> registry_;
    AuthCompletionChannel& completions_;
    AuthenticationConfig config_;
    CredentialCache cache_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<application::AuthAttempt> queue_;
    std::size_t queued_secret_bytes_{0};
    std::atomic_bool running_{true};
    std::vector<std::thread> workers_;
    std::thread invalidation_thread_;
    std::atomic_uint64_t observed_version_{0};
    std::atomic_uint64_t reconciliations_{0};
};
}  // namespace nexstream::ingest::infrastructure
