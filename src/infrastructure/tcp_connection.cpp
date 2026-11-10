#include "nexstream/ingest/infrastructure/tcp_connection.hpp"
#include <algorithm>
#include <array>
#include <cerrno>
#include <sys/socket.h>

namespace nexstream::ingest::infrastructure {
TcpConnection::TcpConnection(UniqueFd fd, std::uint32_t id, std::uint32_t generation,
                             ConnectionLimits limits, application::IAsyncEventProducer& producer,
                             application::IAsyncAuthenticator& authenticator)
    : fd_(std::move(fd)), id_(id), generation_(generation), limits_(limits),
      decoder_(limits.max_frame_bytes, limits.max_receive_buffer_bytes),
      session_(producer, authenticator, (static_cast<std::uint64_t>(generation) << 32U) | id, limits.session),
      deadline_(std::chrono::steady_clock::now() + limits.session_timeout) {}

std::uint64_t TcpConnection::token() const noexcept {
    return (static_cast<std::uint64_t>(generation_) << 32U) | id_;
}

bool TcpConnection::expired(std::chrono::steady_clock::time_point now) const noexcept {
    return now >= deadline_;
}

void TcpConnection::queue_frame(const nexstream::transport::v1::Frame& frame) {
    const auto wire = encode_frame(frame);
    const auto pending = send_buffer_.size() - send_offset_;
    if (wire.size() > limits_.max_send_buffer_bytes - pending) { fail_and_close(); return; }
    if (send_offset_ > 0 && pending > 0) {
        std::move(send_buffer_.begin() + static_cast<std::ptrdiff_t>(send_offset_),
                  send_buffer_.end(), send_buffer_.begin());
        send_buffer_.resize(pending);
        send_offset_ = 0;
    } else if (pending == 0) {
        send_buffer_.clear(); send_offset_ = 0;
    }
    send_buffer_.insert(send_buffer_.end(), wire.begin(), wire.end());
}

void TcpConnection::fail_and_close() { closed_ = true; fd_.reset(); }

void TcpConnection::on_readable() {
    std::array<std::byte, 16'384> buffer{};
    std::size_t read_total = 0;
    std::size_t frames_total = 0;
    while (!closed_ && read_total < limits_.read_budget_per_tick &&
           frames_total < limits_.max_frames_per_tick) {
        // A framed message occupies at least five bytes (4-byte header + 1-byte payload).
        // Limiting each recv by the remaining frame budget prevents a coalesced stream from
        // causing unbounded per-tick parser work while retaining level-triggered progress.
        const auto frame_budget_bytes = (limits_.max_frames_per_tick - frames_total) * 5;
        const auto request = std::min({buffer.size(), limits_.read_budget_per_tick - read_total,
                                       frame_budget_bytes});
        const auto count = ::recv(fd_.get(), buffer.data(), request, 0);
        if (count > 0) {
            const auto received = static_cast<std::size_t>(count);
            read_total += received;
            auto decoded = decoder_.feed(std::span<const std::byte>(buffer.data(), received));
            if (decoded.error != DecodeError::kNone) { fail_and_close(); return; }
            frames_total += decoded.frames.size();
            for (const auto& frame : decoded.frames) {
                auto result = session_.on_frame(frame, frame.ByteSizeLong() + 4);
                for (const auto& response : result.responses) queue_frame(response);
                close_after_flush_ = close_after_flush_ || result.close_after_flush;
                if (closed_) return;
            }
            continue;
        }
        if (count == 0) { fail_and_close(); return; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { ++read_eagain_count_; return; }
        fail_and_close();
    }
}

void TcpConnection::on_writable() {
    std::size_t written_total = 0;
    while (!closed_ && wants_write() && written_total < limits_.write_budget_per_tick) {
        auto request = std::min(send_buffer_.size() - send_offset_,
                                limits_.write_budget_per_tick - written_total);
        if (limits_.test_write_chunk_bytes > 0)
            request = std::min(request, limits_.test_write_chunk_bytes);
        const auto count = ::send(fd_.get(), send_buffer_.data() + send_offset_, request,
                                  MSG_NOSIGNAL);
        if (count > 0) {
            const auto sent = static_cast<std::size_t>(count);
            send_offset_ += sent; written_total += sent; continue;
        }
        if (count < 0 && errno == EINTR) continue;
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ++write_eagain_count_; return;
        }
        fail_and_close(); return;
    }
    if (!wants_write()) {
        send_buffer_.clear(); send_offset_ = 0;
        if (close_after_flush_) fail_and_close();
    }
}

void TcpConnection::on_peer_event() {
    if (!wants_write()) fail_and_close(); else close_after_flush_ = true;
}
void TcpConnection::on_timeout() { fail_and_close(); }
void TcpConnection::on_delivery(const application::DeliveryCompletion& completion) {
    if (closed_) return;
    auto result = session_.on_delivery(completion);
    for (const auto& response : result.responses) queue_frame(response);
    close_after_flush_ = close_after_flush_ || result.close_after_flush;
}
void TcpConnection::on_auth_completion(const application::AuthCompletion& completion) {
    if (closed_) return;
    auto result = session_.on_auth_completion(completion);
    for (const auto& response : result.responses) queue_frame(response);
    close_after_flush_ = close_after_flush_ || result.close_after_flush;
}
}  // namespace nexstream::ingest::infrastructure
