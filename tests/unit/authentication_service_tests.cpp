#include <openssl/evp.h>
#include <atomic>
#include <array>
#include <chrono>
#include <mutex>
#include <poll.h>
#include <thread>
#include <unordered_map>
#include "nexstream/ingest/infrastructure/authentication_service.hpp"
#include "test_harness.hpp"

namespace app = nexstream::ingest::application;
namespace infra = nexstream::ingest::infrastructure;
using namespace std::chrono_literals;

namespace {
app::CredentialVerifier credential(std::string key, std::string secret,
                                   app::CredentialState state = app::CredentialState::kActive) {
    app::CredentialVerifier value; value.key_id = std::move(key); value.agent_id = "agent-1";
    value.host_id = "host-1"; value.salt.assign(16, 0x5a); value.verifier.resize(32);
    value.iterations = 100'000; value.state = state;
    value.not_before = std::chrono::system_clock::now() - 1min;
    value.expires_at = std::chrono::system_clock::now() + 1h; value.change_version = 1;
    (void)PKCS5_PBKDF2_HMAC(secret.data(), static_cast<int>(secret.size()), value.salt.data(),
        static_cast<int>(value.salt.size()), static_cast<int>(value.iterations), EVP_sha256(),
        static_cast<int>(value.verifier.size()), value.verifier.data());
    return value;
}
class FakeRegistry final : public app::ICredentialRegistry {
  public:
    app::RegistryLookup lookup(const std::string& key_id) override {
        ++lookups; std::lock_guard lock(mutex);
        if (!available) return {app::RegistryLookupStatus::kUnavailable, std::nullopt};
        const auto found = values.find(key_id);
        if (found == values.end()) return {app::RegistryLookupStatus::kNotFound, std::nullopt};
        return {app::RegistryLookupStatus::kFound, found->second};
    }
    std::optional<std::uint64_t> current_version() override {
        std::lock_guard lock(mutex); return available ? std::optional(version) : std::nullopt;
    }
    std::vector<app::CredentialChange> wait_for_changes(std::chrono::milliseconds timeout) override {
        std::this_thread::sleep_for(timeout); std::lock_guard lock(mutex);
        auto result = std::move(notifications); notifications.clear(); return result;
    }
    std::mutex mutex; std::unordered_map<std::string, app::CredentialVerifier> values;
    std::vector<app::CredentialChange> notifications; bool available{true}; std::uint64_t version{1};
    std::atomic_size_t lookups{0};
};
class SlowRegistry final : public app::ICredentialRegistry {
  public:
    app::RegistryLookup lookup(const std::string&) override {
        std::this_thread::sleep_for(250ms); return {app::RegistryLookupStatus::kUnavailable, std::nullopt};
    }
    std::optional<std::uint64_t> current_version() override { return 0; }
    std::vector<app::CredentialChange> wait_for_changes(std::chrono::milliseconds timeout) override {
        std::this_thread::sleep_for(timeout); return {};
    }
};
app::AuthCompletion authenticate(infra::AuthenticationService& service,
                                 infra::AuthCompletionChannel& channel, std::uint64_t token,
                                 std::string key, std::string secret,
                                 std::string agent = "agent-1", std::string host = "host-1") {
    const auto accepted = service.authenticate({{token}, std::move(agent), std::move(host),
                                                std::move(key), std::move(secret)});
    if (accepted != app::AuthEnqueueResult::kAccepted) return {{token}, app::AuthStatus::kRetryLater};
    pollfd descriptor{channel.fd(), POLLIN, 0};
    if (::poll(&descriptor, 1, 2'000) != 1) return {{token}, app::AuthStatus::kRetryLater};
    auto completions = channel.drain(1); return completions.empty()
        ? app::AuthCompletion{{token}, app::AuthStatus::kRetryLater} : completions.front();
}
}  // namespace

int main() {
    TestHarness test; auto registry = std::make_shared<FakeRegistry>();
    registry->values.emplace("key-a", credential("key-a", "correct-secret"));
    registry->values.emplace("key-b", credential("key-b", "rotation-secret"));
    registry->values.emplace("revoked", credential("revoked", "secret", app::CredentialState::kRevoked));
    auto expired = credential("expired", "secret"); expired.expires_at = std::chrono::system_clock::now() - 1s;
    registry->values.emplace("expired", expired);
    infra::AuthCompletionChannel channel(512); infra::AuthenticationConfig config;
    config.worker_count = 4; config.max_queued_attempts = 128; config.cache_ttl = 500ms;
    config.reconcile_interval = 100ms;
    infra::AuthenticationService service(registry, channel, config);

    test.expect(authenticate(service, channel, 1, "key-a", "correct-secret").status == app::AuthStatus::kAuthenticated,
                "valid credential authenticates");
    const auto after_miss = registry->lookups.load();
    test.expect(authenticate(service, channel, 2, "key-a", "wrong").status == app::AuthStatus::kInvalidCredentials,
                "invalid secret rejected with constant-time verifier");
    test.expect(registry->lookups.load() == after_miss, "fresh cache hit avoids registry lookup");
    test.expect(authenticate(service, channel, 3, "key-a", "correct-secret", "agent-1", "other-host").status == app::AuthStatus::kHostNotAuthorized,
                "host identity mismatch rejected");
    test.expect(authenticate(service, channel, 4, "revoked", "secret").status == app::AuthStatus::kInvalidCredentials,
                "revoked key rejected");
    test.expect(authenticate(service, channel, 5, "expired", "secret").status == app::AuthStatus::kInvalidCredentials,
                "expired key rejected");
    test.expect(authenticate(service, channel, 6, "key-b", "rotation-secret").status == app::AuthStatus::kAuthenticated,
                "second active rotation credential accepted");
    test.expect(authenticate(service, channel, 61, "key-a", "correct-secret").status == app::AuthStatus::kAuthenticated,
                "cache refreshed immediately before registry outage");

    { std::lock_guard lock(registry->mutex); registry->available = false; }
    test.expect(authenticate(service, channel, 7, "key-a", "correct-secret").status == app::AuthStatus::kAuthenticated,
                "fresh cache remains usable during registry outage");
    test.expect(authenticate(service, channel, 8, "cold-miss", "secret").status == app::AuthStatus::kRetryLater,
                "cold miss fails closed while registry unavailable");
    std::this_thread::sleep_for(520ms);
    test.expect(authenticate(service, channel, 9, "key-a", "correct-secret").status == app::AuthStatus::kRetryLater,
                "expired cache entry fails closed during outage");

    const auto revocation_started = std::chrono::steady_clock::now();
    { std::lock_guard lock(registry->mutex); registry->available = true;
      registry->values["key-a"].state = app::CredentialState::kRevoked;
      registry->values["key-a"].change_version = 2; registry->version = 2;
      registry->notifications.push_back({"key-a", 2}); }
    std::this_thread::sleep_for(140ms);
    test.expect(authenticate(service, channel, 10, "key-a", "correct-secret").status == app::AuthStatus::kInvalidCredentials,
                "LISTEN-style invalidation propagates revocation");
    const auto revocation_lag = std::chrono::steady_clock::now() - revocation_started;
    test.expect(revocation_lag < 500ms, "measured fixture revocation propagation remains under 500 ms");

    { std::lock_guard lock(registry->mutex); registry->values["key-b"].state = app::CredentialState::kRevoked;
      registry->values["key-b"].change_version = 3; registry->version = 3; }
    std::this_thread::sleep_for(140ms);
    test.expect(service.reconciliation_count() >= 1, "missed notification reconciled by durable version");
    test.expect(authenticate(service, channel, 11, "key-b", "rotation-secret").status == app::AuthStatus::kInvalidCredentials,
                "reconciliation invalidates stale credential");

    std::atomic_size_t accepted{0}; std::vector<std::thread> clients;
    { std::lock_guard lock(registry->mutex); registry->values["key-c"] = credential("key-c", "stress-secret"); registry->version = 4; }
    test.expect(authenticate(service, channel, 999, "key-c", "stress-secret").status == app::AuthStatus::kAuthenticated,
                "stress credential warmup succeeds");
    for (std::uint64_t i = 0; i < 100; ++i) clients.emplace_back([&, i] {
        if (service.authenticate({{1000 + i}, "agent-1", "host-1", "key-c", "stress-secret"}) ==
            app::AuthEnqueueResult::kAccepted) ++accepted;
    });
    for (auto& client : clients) client.join();
    // Match production: exactly one Reactor consumes this eventfd/queue. Concurrent pollers
    // can all wake for one completion, then race to drain it and falsely report empty results.
    std::array<bool, 100> received{};
    std::size_t success = 0;
    const auto stress_deadline = std::chrono::steady_clock::now() + 2s;
    while (success < 100 && std::chrono::steady_clock::now() < stress_deadline) {
        pollfd descriptor{channel.fd(), POLLIN, 0};
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            stress_deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0 || ::poll(&descriptor, 1, static_cast<int>(remaining)) != 1) break;
        for (const auto& completion : channel.drain(100)) {
            const auto id = completion.correlation.connection_token;
            if (id >= 1000 && id < 1100 && !received[id - 1000] &&
                completion.status == app::AuthStatus::kAuthenticated) {
                received[id - 1000] = true;
                ++success;
            }
        }
    }
    test.expect(accepted.load() == 100 && success == 100 && service.cache_size() <= config.max_cache_entries,
                "100 concurrent authentications complete within bounded cache");

    auto slow_registry = std::make_shared<SlowRegistry>(); infra::AuthCompletionChannel slow_channel(8);
    infra::AuthenticationConfig slow_config; slow_config.worker_count = 1;
    slow_config.max_queued_attempts = 1; slow_config.max_queued_secret_bytes = 16;
    infra::AuthenticationService slow_service(slow_registry, slow_channel, slow_config);
    test.expect(slow_service.authenticate({{2001}, "a", "h", "k", "12345678"}) == app::AuthEnqueueResult::kAccepted,
                "first slow cache miss admitted");
    for (int i = 0; i < 100 && slow_service.queued_attempts() != 0; ++i) std::this_thread::sleep_for(1ms);
    test.expect(slow_service.authenticate({{2002}, "a", "h", "k", "12345678"}) == app::AuthEnqueueResult::kAccepted,
                "one bounded queued miss admitted");
    test.expect(slow_service.authenticate({{2003}, "a", "h", "k", "12345678"}) == app::AuthEnqueueResult::kQueueFull,
                "worker queue fast-rejects overload");
    return test.finish();
}
