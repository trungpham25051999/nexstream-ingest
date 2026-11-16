#include "nexstream/ingest/infrastructure/credential_cache.hpp"
#include <stdexcept>

namespace nexstream::ingest::infrastructure {
CredentialCache::CredentialCache(std::size_t max_entries, std::chrono::milliseconds ttl)
    : max_entries_(max_entries), ttl_(ttl) {
    if (max_entries == 0 || ttl.count() <= 0) throw std::invalid_argument("invalid cache bounds");
}
std::optional<application::CredentialVerifier> CredentialCache::get(const std::string& key_id) {
    std::lock_guard lock(mutex_); const auto found = entries_.find(key_id);
    if (found == entries_.end()) return std::nullopt;
    if (std::chrono::steady_clock::now() - found->second.cached_at > ttl_) {
        lru_.erase(found->second.lru); entries_.erase(found); return std::nullopt;
    }
    lru_.splice(lru_.begin(), lru_, found->second.lru); return found->second.credential;
}
void CredentialCache::put(application::CredentialVerifier credential) {
    std::lock_guard lock(mutex_); const auto key = credential.key_id;
    if (const auto found = entries_.find(key); found != entries_.end()) {
        lru_.erase(found->second.lru); entries_.erase(found);
    }
    while (entries_.size() >= max_entries_) { entries_.erase(lru_.back()); lru_.pop_back(); }
    lru_.push_front(key);
    entries_.emplace(key, Entry{std::move(credential), std::chrono::steady_clock::now(), lru_.begin()});
}
void CredentialCache::invalidate(const std::string& key_id) {
    std::lock_guard lock(mutex_); const auto found = entries_.find(key_id);
    if (found != entries_.end()) { lru_.erase(found->second.lru); entries_.erase(found); }
}
void CredentialCache::invalidate_all() { std::lock_guard lock(mutex_); entries_.clear(); lru_.clear(); }
std::size_t CredentialCache::size() const noexcept { std::lock_guard lock(mutex_); return entries_.size(); }
}  // namespace nexstream::ingest::infrastructure
