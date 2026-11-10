#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>
#include "nexstream/ingest/infrastructure/tcp_acceptor.hpp"
#include "nexstream/ingest/infrastructure/tcp_connection.hpp"
#include "nexstream/ingest/infrastructure/completion_channel.hpp"
#include "nexstream/ingest/infrastructure/auth_completion_channel.hpp"
#include "nexstream/ingest/infrastructure/unique_fd.hpp"

namespace nexstream::ingest::infrastructure {
struct ReactorConfig {
    std::string_view listen_address{"127.0.0.1"};
    std::uint16_t port{0};
    int backlog{256};
    std::size_t max_connections{1'024};
    std::size_t accept_budget_per_tick{64};
    int epoll_wait_timeout_ms{25};
    std::size_t max_completions_per_tick{256};
    ConnectionLimits connection{};
};

class EpollReactor {
  public:
    EpollReactor(application::IAsyncEventProducer& producer, CompletionChannel& completions,
                 application::IAsyncAuthenticator& authenticator,
                 AuthCompletionChannel& auth_completions,
                 ReactorConfig config = {});
    ~EpollReactor() = default;
    EpollReactor(const EpollReactor&) = delete;
    EpollReactor& operator=(const EpollReactor&) = delete;
    void run();
    void run_once();
    void request_stop();
    [[nodiscard]] std::uint16_t port() const noexcept { return acceptor_.port(); }
    [[nodiscard]] std::size_t read_eagain_count() const noexcept {
        return read_eagain_count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t write_eagain_count() const noexcept {
        return write_eagain_count_.load(std::memory_order_relaxed);
    }
  private:
    void accept_ready();
    void update_interest(TcpConnection& connection);
    void remove_closed();
    ReactorConfig config_;
    application::IAsyncEventProducer& producer_;
    CompletionChannel& completions_;
    application::IAsyncAuthenticator& authenticator_;
    AuthCompletionChannel& auth_completions_;
    TcpAcceptor acceptor_;
    UniqueFd epoll_fd_;
    UniqueFd stop_fd_;
    std::unordered_map<std::uint64_t, std::unique_ptr<TcpConnection>> connections_;
    std::atomic_bool stop_requested_{false};
    std::uint32_t next_id_{1};
    std::uint32_t next_generation_{1};
    std::atomic_size_t read_eagain_count_{0};
    std::atomic_size_t write_eagain_count_{0};
};
}  // namespace nexstream::ingest::infrastructure
