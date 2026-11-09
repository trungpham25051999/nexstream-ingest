#pragma once
#include <cstddef>
#include <string>
#include <vector>
#include "nexstream/ingest/application/event_producer.hpp"
#include "nexstream/ingest/application/authenticator.hpp"
#include "nexstream/transport/v1/transport.pb.h"

namespace nexstream::ingest::application {
struct SessionLimits {
    std::size_t max_batch_samples{50};
    std::size_t max_batch_bytes{1'048'576};
    std::size_t max_identifier_bytes{128};
    std::size_t max_secret_bytes{512};
};
enum class SessionState {
    kAwaitAuth, kAwaitAuthCompletion, kAwaitBatchStart, kReceivingBatch, kAwaitDelivery, kComplete, kFailed
};
struct SessionResult {
    std::vector<nexstream::transport::v1::Frame> responses;
    bool close_after_flush{false};
};

class ProtocolSession {
  public:
    ProtocolSession(IAsyncEventProducer& producer, IAsyncAuthenticator& authenticator,
                    std::uint64_t connection_token,
                    SessionLimits limits = {});
    [[nodiscard]] SessionResult on_frame(const nexstream::transport::v1::Frame& frame,
                                         std::size_t wire_bytes);
    [[nodiscard]] SessionResult on_delivery(const DeliveryCompletion& completion);
    [[nodiscard]] SessionResult on_auth_completion(const AuthCompletion& completion);
    [[nodiscard]] SessionState state() const noexcept { return state_; }
    [[nodiscard]] std::size_t batch_bytes() const noexcept { return batch_bytes_; }
  private:
    [[nodiscard]] SessionResult fail(std::string detail);
    [[nodiscard]] bool valid_common(const nexstream::transport::v1::Frame& frame) const;
    [[nodiscard]] SessionResult finalize_if_ready();
    struct DeliveryRecord {
        std::uint64_t request_id;
        std::string message_id;
        nexstream::transport::v1::ResultCode code;
        bool resolved;
    };
    IAsyncEventProducer& producer_;
    IAsyncAuthenticator& authenticator_;
    std::uint64_t connection_token_;
    SessionLimits limits_;
    SessionState state_{SessionState::kAwaitAuth};
    std::string host_id_;
    std::string batch_id_;
    std::vector<DeliveryRecord> deliveries_;
    std::size_t expected_count_{0};
    std::size_t batch_bytes_{0};
    std::uint64_t next_request_id_{1};
    bool batch_ended_{false};
};
}  // namespace nexstream::ingest::application
