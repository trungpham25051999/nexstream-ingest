#pragma once
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace nexstream::ingest::application {
enum class CredentialState { kActive, kRevoked, kExpired };
struct CredentialVerifier {
    std::string key_id;
    std::string agent_id;
    std::string host_id;
    std::vector<unsigned char> salt;
    std::vector<unsigned char> verifier;
    std::uint32_t iterations{0};
    CredentialState state{CredentialState::kRevoked};
    std::chrono::system_clock::time_point not_before{};
    std::chrono::system_clock::time_point expires_at{};
    std::uint64_t change_version{0};
};
enum class RegistryLookupStatus { kFound, kNotFound, kUnavailable };
struct RegistryLookup { RegistryLookupStatus status; std::optional<CredentialVerifier> credential; };
struct CredentialChange { std::string key_id; std::uint64_t version{0}; };

class ICredentialRegistry {
  public:
    virtual ~ICredentialRegistry() = default;
    [[nodiscard]] virtual RegistryLookup lookup(const std::string& key_id) = 0;
    [[nodiscard]] virtual std::optional<std::uint64_t> current_version() = 0;
    [[nodiscard]] virtual std::vector<CredentialChange> wait_for_changes(
        std::chrono::milliseconds timeout) = 0;
};
}  // namespace nexstream::ingest::application
