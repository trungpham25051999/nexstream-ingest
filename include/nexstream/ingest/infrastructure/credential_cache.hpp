#pragma once
#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include "nexstream/ingest/application/credential_registry.hpp"

namespace nexstream::ingest::infrastructure {
class CredentialCache {
  public:
    CredentialCache(std::size_t max_entries, std::chrono::milliseconds ttl);
    [[nodiscard]] std::optional<application::CredentialVerifier> get(const std::string& key_id);
    void put(application::CredentialVerifier credential);
    void invalidate(const std::string& key_id);
    void invalidate_all();
    [[nodiscard]] std::size_t size() const noexcept;
  private:
    struct Entry {
        application::CredentialVerifier credential;
        std::chrono::steady_clock::time_point cached_at;
        std::list<std::string>::iterator lru;
    };
    const std::size_t max_entries_;
    const std::chrono::milliseconds ttl_;
    mutable std::mutex mutex_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, Entry> entries_;
};
}  // namespace nexstream::ingest::infrastructure
