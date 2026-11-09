#pragma once
#include <cstdint>
#include <string>

namespace nexstream::ingest::application {
struct AuthCorrelation { std::uint64_t connection_token{0}; };
struct AuthAttempt {
    AuthCorrelation correlation;
    std::string agent_id;
    std::string host_id;
    std::string key_id;
    std::string secret;
};
enum class AuthEnqueueResult { kAccepted, kQueueFull, kUnavailable };
enum class AuthStatus { kAuthenticated, kInvalidCredentials, kHostNotAuthorized, kRetryLater };
struct AuthCompletion { AuthCorrelation correlation; AuthStatus status; };

class IAsyncAuthenticator {
  public:
    virtual ~IAsyncAuthenticator() = default;
    [[nodiscard]] virtual AuthEnqueueResult authenticate(AuthAttempt attempt) = 0;
};
}  // namespace nexstream::ingest::application
