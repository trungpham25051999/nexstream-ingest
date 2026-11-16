#include "nexstream/ingest/infrastructure/authentication_service.hpp"
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace nexstream::ingest::infrastructure {
AuthenticationService::AuthenticationService(
    std::shared_ptr<application::ICredentialRegistry> registry,
    AuthCompletionChannel& completions, AuthenticationConfig config)
    : registry_(std::move(registry)), completions_(completions), config_(config),
      cache_(config.max_cache_entries, config.cache_ttl) {
    if (!registry_ || config_.worker_count == 0 || config_.max_queued_attempts == 0 ||
        config_.max_queued_secret_bytes == 0) throw std::invalid_argument("invalid auth config");
    if (const auto version = registry_->current_version()) observed_version_.store(*version);
    workers_.reserve(config_.worker_count);
    for (std::size_t i = 0; i < config_.worker_count; ++i)
        workers_.emplace_back([this] { worker_loop(); });
    invalidation_thread_ = std::thread([this] { invalidation_loop(); });
}
AuthenticationService::~AuthenticationService() {
    running_.store(false); queue_cv_.notify_all();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    if (invalidation_thread_.joinable()) invalidation_thread_.join();
    std::lock_guard lock(queue_mutex_); for (auto& attempt : queue_) wipe(attempt.secret);
}
application::AuthEnqueueResult AuthenticationService::authenticate(application::AuthAttempt attempt) {
    if (auto credential = cache_.get(attempt.key_id)) {
        const auto status = verify(attempt, *credential); wipe(attempt.secret);
        return completions_.try_push({attempt.correlation, status})
            ? application::AuthEnqueueResult::kAccepted : application::AuthEnqueueResult::kQueueFull;
    }
    std::lock_guard lock(queue_mutex_);
    if (!running_.load()) { wipe(attempt.secret); return application::AuthEnqueueResult::kUnavailable; }
    if (queue_.size() >= config_.max_queued_attempts ||
        attempt.secret.size() > config_.max_queued_secret_bytes - queued_secret_bytes_) {
        wipe(attempt.secret); return application::AuthEnqueueResult::kQueueFull;
    }
    queued_secret_bytes_ += attempt.secret.size(); queue_.push_back(std::move(attempt));
    queue_cv_.notify_one(); return application::AuthEnqueueResult::kAccepted;
}
void AuthenticationService::worker_loop() {
    while (true) {
        application::AuthAttempt attempt;
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !running_.load() || !queue_.empty(); });
            if (!running_.load() && queue_.empty()) return;
            attempt = std::move(queue_.front()); queue_.pop_front();
            queued_secret_bytes_ -= attempt.secret.size();
        }
        auto status = application::AuthStatus::kRetryLater;
        const auto lookup = registry_->lookup(attempt.key_id);
        if (lookup.status == application::RegistryLookupStatus::kFound && lookup.credential) {
            cache_.put(*lookup.credential); status = verify(attempt, *lookup.credential);
        } else if (lookup.status == application::RegistryLookupStatus::kNotFound) {
            status = application::AuthStatus::kInvalidCredentials;
        }
        wipe(attempt.secret); (void)completions_.try_push({attempt.correlation, status});
    }
}
application::AuthStatus AuthenticationService::verify(
    const application::AuthAttempt& attempt,
    const application::CredentialVerifier& credential) const {
    if (attempt.agent_id != credential.agent_id || attempt.host_id != credential.host_id)
        return application::AuthStatus::kHostNotAuthorized;
    const auto now = std::chrono::system_clock::now();
    if (credential.state != application::CredentialState::kActive ||
        now < credential.not_before || now >= credential.expires_at)
        return application::AuthStatus::kInvalidCredentials;
    if (credential.iterations == 0 || credential.iterations > static_cast<std::uint32_t>(std::numeric_limits<int>::max()) ||
        credential.salt.empty() || credential.verifier.empty())
        return application::AuthStatus::kInvalidCredentials;
    std::vector<unsigned char> derived(credential.verifier.size());
    const auto ok = PKCS5_PBKDF2_HMAC(attempt.secret.data(), static_cast<int>(attempt.secret.size()),
        credential.salt.data(), static_cast<int>(credential.salt.size()),
        static_cast<int>(credential.iterations), EVP_sha256(), static_cast<int>(derived.size()), derived.data());
    const bool match = ok == 1 && CRYPTO_memcmp(derived.data(), credential.verifier.data(), derived.size()) == 0;
    OPENSSL_cleanse(derived.data(), derived.size());
    return match ? application::AuthStatus::kAuthenticated
                 : application::AuthStatus::kInvalidCredentials;
}
void AuthenticationService::invalidation_loop() {
    auto next_reconcile = std::chrono::steady_clock::now() + config_.reconcile_interval;
    while (running_.load()) {
        for (const auto& change : registry_->wait_for_changes(std::chrono::milliseconds(100))) {
            cache_.invalidate(change.key_id);
            observed_version_.store(std::max(observed_version_.load(), change.version));
        }
        if (std::chrono::steady_clock::now() >= next_reconcile) {
            if (const auto version = registry_->current_version(); version && *version > observed_version_.load()) {
                cache_.invalidate_all(); observed_version_.store(*version); ++reconciliations_;
            }
            next_reconcile = std::chrono::steady_clock::now() + config_.reconcile_interval;
        }
    }
}
void AuthenticationService::invalidate(const std::string& key_id) { cache_.invalidate(key_id); }
std::size_t AuthenticationService::queued_attempts() const noexcept {
    std::lock_guard lock(queue_mutex_); return queue_.size();
}
void AuthenticationService::wipe(std::string& value) noexcept {
    if (!value.empty()) OPENSSL_cleanse(value.data(), value.size());
    value.clear();
}
}  // namespace nexstream::ingest::infrastructure
