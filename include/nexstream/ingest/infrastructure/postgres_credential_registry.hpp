#pragma once
#include <mutex>
#include <string>
#include "nexstream/ingest/application/credential_registry.hpp"

struct pg_conn;
namespace nexstream::ingest::infrastructure {
class PostgresCredentialRegistry final : public application::ICredentialRegistry {
  public:
    explicit PostgresCredentialRegistry(std::string connection_string);
    ~PostgresCredentialRegistry() override;
    [[nodiscard]] application::RegistryLookup lookup(const std::string& key_id) override;
    [[nodiscard]] std::optional<std::uint64_t> current_version() override;
    [[nodiscard]] std::vector<application::CredentialChange> wait_for_changes(
        std::chrono::milliseconds timeout) override;
  private:
    [[nodiscard]] pg_conn* connect() const;
    bool ensure_listener();
    std::string connection_string_;
    std::mutex listener_mutex_;
    pg_conn* listener_{nullptr};
};
}  // namespace nexstream::ingest::infrastructure
