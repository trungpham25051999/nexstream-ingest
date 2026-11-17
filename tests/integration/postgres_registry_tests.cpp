#include <libpq-fe.h>
#include <openssl/evp.h>
#include <chrono>
#include <array>
#include <cstdlib>
#include <memory>
#include <poll.h>
#include <string>
#include <thread>
#include "nexstream/ingest/infrastructure/authentication_service.hpp"
#include "nexstream/ingest/infrastructure/postgres_credential_registry.hpp"
#include "test_harness.hpp"

#ifndef NEXSTREAM_POSTGRES_TEST_CONN
#error NEXSTREAM_POSTGRES_TEST_CONN is required
#endif
namespace app = nexstream::ingest::application;
namespace infra = nexstream::ingest::infrastructure;
using namespace std::chrono_literals;
namespace {
std::string hex(const unsigned char* data, std::size_t size) {
    constexpr char digits[] = "0123456789abcdef"; std::string output(size * 2, '0');
    for (std::size_t i = 0; i < size; ++i) { output[i * 2] = digits[data[i] >> 4U]; output[i * 2 + 1] = digits[data[i] & 0xfU]; }
    return output;
}
bool command(PGconn* connection, const std::string& sql) {
    auto* result = PQexec(connection, sql.c_str()); const bool ok = result &&
        (PQresultStatus(result) == PGRES_COMMAND_OK || PQresultStatus(result) == PGRES_TUPLES_OK);
    if (result) PQclear(result); return ok;
}
app::AuthStatus authenticate(infra::AuthenticationService& service, infra::AuthCompletionChannel& channel,
                             std::uint64_t token, const char* key, const char* secret) {
    if (service.authenticate({{token}, "pg-agent", "pg-host", key, secret}) != app::AuthEnqueueResult::kAccepted)
        return app::AuthStatus::kRetryLater;
    pollfd descriptor{channel.fd(), POLLIN, 0}; if (::poll(&descriptor, 1, 3'000) != 1) return app::AuthStatus::kRetryLater;
    const auto results = channel.drain(1); return results.empty() ? app::AuthStatus::kRetryLater : results[0].status;
}
}  // namespace
int main() {
    TestHarness test;
    if (std::getenv("NEXSTREAM_EXPECT_POSTGRES_UNAVAILABLE")) {
        auto registry = std::make_shared<infra::PostgresCredentialRegistry>(NEXSTREAM_POSTGRES_TEST_CONN);
        test.expect(registry->lookup("unavailable-key").status == app::RegistryLookupStatus::kUnavailable,
                    "real stopped PostgreSQL fails lookup closed");
        test.expect(!registry->current_version().has_value(), "reconciliation reports unavailable registry");
        return test.finish();
    }
    PGconn* connection = PQconnectdb(NEXSTREAM_POSTGRES_TEST_CONN);
    test.expect(connection && PQstatus(connection) == CONNECTION_OK, "real PostgreSQL connection established");
    if (!connection || PQstatus(connection) != CONNECTION_OK) { if (connection) PQfinish(connection); return test.finish(); }
    std::array<unsigned char, 16> salt{}; salt.fill(0x44); std::array<unsigned char, 32> verifier{};
    const std::string secret = "integration-secret";
    (void)PKCS5_PBKDF2_HMAC(secret.data(), static_cast<int>(secret.size()), salt.data(), salt.size(),
        100'000, EVP_sha256(), verifier.size(), verifier.data());
    test.expect(command(connection, "TRUNCATE agent_credentials, credential_change_events, agents RESTART IDENTITY CASCADE"), "registry fixture reset");
    test.expect(command(connection, "INSERT INTO agents(id,agent_id,host_id) VALUES ('11111111-1111-1111-1111-111111111111','pg-agent','pg-host')"), "agent inserted");
    test.expect(command(connection, "INSERT INTO credential_change_events(key_id,event_type) VALUES ('pg-key','created')"), "change event inserted");
    const std::string insert = "INSERT INTO agent_credentials(key_id,agent_id,secret_salt,secret_verifier,iterations,status,not_before,expires_at,change_version) VALUES ('pg-key','11111111-1111-1111-1111-111111111111',decode('" + hex(salt.data(), salt.size()) + "','hex'),decode('" + hex(verifier.data(), verifier.size()) + "','hex'),100000,'active',now()-interval '1 minute',now()+interval '1 hour',1)";
    test.expect(command(connection, insert), "hashed credential inserted without plaintext secret");

    auto registry = std::make_shared<infra::PostgresCredentialRegistry>(NEXSTREAM_POSTGRES_TEST_CONN);
    infra::AuthCompletionChannel channel; infra::AuthenticationConfig config;
    config.worker_count = 2; config.cache_ttl = 5s; config.reconcile_interval = 100ms;
    infra::AuthenticationService service(registry, channel, config);
    test.expect(authenticate(service, channel, 1, "pg-key", secret.c_str()) == app::AuthStatus::kAuthenticated,
                "real PostgreSQL valid credential authenticates");
    service.invalidate("pg-key");
    test.expect(authenticate(service, channel, 2, "pg-key", "wrong") == app::AuthStatus::kInvalidCredentials,
                "real PostgreSQL invalid secret rejected");
    test.expect(command(connection, "INSERT INTO credential_change_events(key_id,event_type) VALUES ('pg-key','revoked'); UPDATE agent_credentials SET status='revoked',change_version=2 WHERE key_id='pg-key'"), "credential revoked with durable version");
    std::this_thread::sleep_for(250ms);
    test.expect(authenticate(service, channel, 3, "pg-key", secret.c_str()) == app::AuthStatus::kInvalidCredentials,
                "LISTEN/NOTIFY revocation reaches live service");
    PQfinish(connection); return test.finish();
}
