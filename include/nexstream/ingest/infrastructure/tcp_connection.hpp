#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>
#include "nexstream/ingest/application/protocol_session.hpp"
#include "nexstream/ingest/infrastructure/frame_decoder.hpp"
#include "nexstream/ingest/infrastructure/unique_fd.hpp"

namespace nexstream::ingest::infrastructure {
struct ConnectionLimits {
    std::size_t max_frame_bytes{65'536};
    std::size_t max_receive_buffer_bytes{65'540};
    std::size_t max_send_buffer_bytes{131'072};
    std::size_t read_budget_per_tick{65'536};
    std::size_t write_budget_per_tick{65'536};
    std::size_t max_frames_per_tick{32};
    std::size_t test_write_chunk_bytes{0};
    int test_socket_send_buffer_bytes{0};
    std::chrono::milliseconds session_timeout{10'000};
    application::SessionLimits session{};
};

class TcpConnection {
  public:
    TcpConnection(UniqueFd fd, std::uint32_t id, std::uint32_t generation,
                  ConnectionLimits limits, application::IAsyncEventProducer& producer,
                  application::IAsyncAuthenticator& authenticator);
    [[nodiscard]] int fd() const noexcept { return fd_.get(); }
    [[nodiscard]] std::uint32_t id() const noexcept { return id_; }
    [[nodiscard]] std::uint32_t generation() const noexcept { return generation_; }
    [[nodiscard]] std::uint64_t token() const noexcept;
    [[nodiscard]] bool wants_write() const noexcept { return send_offset_ < send_buffer_.size(); }
    [[nodiscard]] bool closed() const noexcept { return closed_; }
    [[nodiscard]] bool expired(std::chrono::steady_clock::time_point now) const noexcept;
    [[nodiscard]] std::size_t read_eagain_count() const noexcept { return read_eagain_count_; }
    [[nodiscard]] std::size_t write_eagain_count() const noexcept { return write_eagain_count_; }
    void on_readable();
    void on_writable();
    void on_peer_event();
    void on_timeout();
    void on_delivery(const application::DeliveryCompletion& completion);
    void on_auth_completion(const application::AuthCompletion& completion);
  private:
    void queue_frame(const nexstream::transport::v1::Frame& frame);
    void fail_and_close();
    UniqueFd fd_;
    std::uint32_t id_;
    std::uint32_t generation_;
    ConnectionLimits limits_;
    FrameDecoder decoder_;
    application::ProtocolSession session_;
    std::vector<std::byte> send_buffer_;
    std::size_t send_offset_{0};
    std::chrono::steady_clock::time_point deadline_;
    bool close_after_flush_{false};
    bool closed_{false};
    std::size_t read_eagain_count_{0};
    std::size_t write_eagain_count_{0};
};
}  // namespace nexstream::ingest::infrastructure
