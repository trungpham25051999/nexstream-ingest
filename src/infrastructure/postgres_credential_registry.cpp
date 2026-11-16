#include "nexstream/ingest/infrastructure/postgres_credential_registry.hpp"
#include <libpq-fe.h>
#include <charconv>
#include <chrono>
#include <memory>
#include <poll.h>
#include <thread>

namespace nexstream::ingest::infrastructure {
namespace {
struct ConnDeleter { void operator()(PGconn* value) const { if (value) PQfinish(value); } };
struct ResultDeleter { void operator()(PGresult* value) const { if (value) PQclear(value); } };
using Conn = std::unique_ptr<PGconn, ConnDeleter>;
using Result = std::unique_ptr<PGresult, ResultDeleter>;
std::vector<unsigned char> decode_hex(const char* text) {
    std::vector<unsigned char> output; const std::string_view value(text ? text : "");
    if (value.size() % 2 != 0) return output;
    output.reserve(value.size() / 2);
    for (std::size_t i = 0; i < value.size(); i += 2) {
        unsigned int byte = 0; const auto parsed = std::from_chars(value.data() + i, value.data() + i + 2, byte, 16);
        if (parsed.ec != std::errc{}) return {};
        output.push_back(static_cast<unsigned char>(byte));
    }
    return output;
}
template <typename T> bool parse_number(const char* value, T& output) {
    const std::string_view text(value ? value : ""); const auto result = std::from_chars(text.data(), text.data() + text.size(), output);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}
}  // namespace
PostgresCredentialRegistry::PostgresCredentialRegistry(std::string connection_string)
    : connection_string_(std::move(connection_string)) {}
PostgresCredentialRegistry::~PostgresCredentialRegistry() {
    std::lock_guard lock(listener_mutex_); if (listener_) PQfinish(reinterpret_cast<PGconn*>(listener_));
}
pg_conn* PostgresCredentialRegistry::connect() const {
    auto* connection = PQconnectdb(connection_string_.c_str());
    if (!connection || PQstatus(connection) != CONNECTION_OK) { if (connection) PQfinish(connection); return nullptr; }
    return reinterpret_cast<pg_conn*>(connection);
}
application::RegistryLookup PostgresCredentialRegistry::lookup(const std::string& key_id) {
    Conn connection(reinterpret_cast<PGconn*>(connect()));
    if (!connection) return {application::RegistryLookupStatus::kUnavailable, std::nullopt};
    const char* values[]{key_id.c_str()};
    Result result(PQexecParams(connection.get(),
        "SELECT c.key_id,a.agent_id,a.host_id,encode(c.secret_salt,'hex'),"
        "encode(c.secret_verifier,'hex'),c.iterations,c.status,"
        "(extract(epoch from c.not_before)*1000)::bigint,"
        "(extract(epoch from c.expires_at)*1000)::bigint,c.change_version "
        "FROM agent_credentials c JOIN agents a ON a.id=c.agent_id "
        "WHERE c.key_id=$1 AND a.active=true", 1, nullptr, values, nullptr, nullptr, 0));
    if (!result || PQresultStatus(result.get()) != PGRES_TUPLES_OK)
        return {application::RegistryLookupStatus::kUnavailable, std::nullopt};
    if (PQntuples(result.get()) != 1)
        return {application::RegistryLookupStatus::kNotFound, std::nullopt};
    application::CredentialVerifier credential;
    credential.key_id = PQgetvalue(result.get(), 0, 0); credential.agent_id = PQgetvalue(result.get(), 0, 1);
    credential.host_id = PQgetvalue(result.get(), 0, 2); credential.salt = decode_hex(PQgetvalue(result.get(), 0, 3));
    credential.verifier = decode_hex(PQgetvalue(result.get(), 0, 4));
    std::int64_t not_before_ms = 0, expires_ms = 0;
    if (!parse_number(PQgetvalue(result.get(), 0, 5), credential.iterations) ||
        !parse_number(PQgetvalue(result.get(), 0, 7), not_before_ms) ||
        !parse_number(PQgetvalue(result.get(), 0, 8), expires_ms) ||
        !parse_number(PQgetvalue(result.get(), 0, 9), credential.change_version))
        return {application::RegistryLookupStatus::kUnavailable, std::nullopt};
    const std::string_view state(PQgetvalue(result.get(), 0, 6));
    credential.state = state == "active" ? application::CredentialState::kActive
        : state == "expired" ? application::CredentialState::kExpired : application::CredentialState::kRevoked;
    credential.not_before = std::chrono::system_clock::time_point(std::chrono::milliseconds(not_before_ms));
    credential.expires_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(expires_ms));
    return {application::RegistryLookupStatus::kFound, std::move(credential)};
}
std::optional<std::uint64_t> PostgresCredentialRegistry::current_version() {
    Conn connection(reinterpret_cast<PGconn*>(connect())); if (!connection) return std::nullopt;
    Result result(PQexec(connection.get(), "SELECT COALESCE(max(version),0) FROM credential_change_events"));
    std::uint64_t value = 0;
    if (!result || PQresultStatus(result.get()) != PGRES_TUPLES_OK || PQntuples(result.get()) != 1 ||
        !parse_number(PQgetvalue(result.get(), 0, 0), value)) return std::nullopt;
    return value;
}
bool PostgresCredentialRegistry::ensure_listener() {
    if (listener_ && PQstatus(reinterpret_cast<PGconn*>(listener_)) == CONNECTION_OK) return true;
    if (listener_) { PQfinish(reinterpret_cast<PGconn*>(listener_)); listener_ = nullptr; }
    listener_ = connect(); if (!listener_) return false;
    Result result(PQexec(reinterpret_cast<PGconn*>(listener_), "LISTEN credential_changes"));
    if (!result || PQresultStatus(result.get()) != PGRES_COMMAND_OK) {
        PQfinish(reinterpret_cast<PGconn*>(listener_)); listener_ = nullptr; return false;
    }
    return true;
}
std::vector<application::CredentialChange> PostgresCredentialRegistry::wait_for_changes(
    std::chrono::milliseconds timeout) {
    std::lock_guard lock(listener_mutex_); std::vector<application::CredentialChange> output;
    if (!ensure_listener()) { std::this_thread::sleep_for(timeout); return output; }
    pollfd descriptor{PQsocket(reinterpret_cast<PGconn*>(listener_)), POLLIN, 0};
    if (::poll(&descriptor, 1, static_cast<int>(timeout.count())) <= 0) return output;
    if (PQconsumeInput(reinterpret_cast<PGconn*>(listener_)) != 1) { PQfinish(reinterpret_cast<PGconn*>(listener_)); listener_ = nullptr; return output; }
    while (auto* notification = PQnotifies(reinterpret_cast<PGconn*>(listener_))) {
        application::CredentialChange change; const std::string_view payload(notification->extra);
        const auto delimiter = payload.find(':');
        if (delimiter != std::string_view::npos) {
            change.key_id = payload.substr(0, delimiter);
            (void)parse_number(std::string(payload.substr(delimiter + 1)).c_str(), change.version);
            output.push_back(std::move(change));
        }
        PQfreemem(notification);
    }
    return output;
}
}  // namespace nexstream::ingest::infrastructure
