#include "nexstream/ingest/infrastructure/epoll_reactor.hpp"
#include <array>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nexstream::ingest::infrastructure {
namespace {
constexpr std::uint64_t kListenerToken = 0;
constexpr std::uint64_t kStopToken = ~std::uint64_t{0};
constexpr std::uint64_t kCompletionToken = ~std::uint64_t{1};
constexpr std::uint64_t kAuthCompletionToken = ~std::uint64_t{2};
void add_epoll(int epoll_fd, int fd, std::uint32_t events, std::uint64_t token) {
    epoll_event event{}; event.events = events; event.data.u64 = token;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
        throw std::runtime_error("epoll_ctl ADD failed");
}
}  // namespace

EpollReactor::EpollReactor(application::IAsyncEventProducer& producer,
                           CompletionChannel& completions,
                           application::IAsyncAuthenticator& authenticator,
                           AuthCompletionChannel& auth_completions, ReactorConfig config)
    : config_(config), producer_(producer), completions_(completions),
      authenticator_(authenticator), auth_completions_(auth_completions),
      acceptor_(config.listen_address, config.port, config.backlog),
      epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      stop_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    if (!epoll_fd_ || !stop_fd_) throw std::runtime_error("epoll/eventfd creation failed");
    add_epoll(epoll_fd_.get(), acceptor_.fd(), EPOLLIN, kListenerToken);
    add_epoll(epoll_fd_.get(), stop_fd_.get(), EPOLLIN, kStopToken);
    add_epoll(epoll_fd_.get(), completions_.fd(), EPOLLIN, kCompletionToken);
    add_epoll(epoll_fd_.get(), auth_completions_.fd(), EPOLLIN, kAuthCompletionToken);
}

void EpollReactor::request_stop() {
    stop_requested_.store(true, std::memory_order_release);
    const std::uint64_t one = 1;
    while (::write(stop_fd_.get(), &one, sizeof(one)) < 0 && errno == EINTR) {}
}

void EpollReactor::run() {
    while (!stop_requested_.load(std::memory_order_acquire)) run_once();
}

void EpollReactor::accept_ready() {
    for (std::size_t accepted = 0; accepted < config_.accept_budget_per_tick; ++accepted) {
        auto fd = acceptor_.accept_one();
        if (!fd) return;
        if (connections_.size() >= config_.max_connections) continue;
        if (config_.connection.test_socket_send_buffer_bytes > 0) {
            const int size = config_.connection.test_socket_send_buffer_bytes;
            ::setsockopt(fd.get(), SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
        }
        const auto id = next_id_++;
        const auto generation = next_generation_++;
        auto connection = std::make_unique<TcpConnection>(std::move(fd), id, generation,
                                                           config_.connection, producer_, authenticator_);
        add_epoll(epoll_fd_.get(), connection->fd(),
                  EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP, connection->token());
        connections_.emplace(connection->token(), std::move(connection));
    }
}

void EpollReactor::update_interest(TcpConnection& connection) {
    if (connection.closed()) return;
    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    if (connection.wants_write()) event.events |= EPOLLOUT;
    event.data.u64 = connection.token();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, connection.fd(), &event) < 0 && errno != ENOENT)
        connection.on_peer_event();
}

void EpollReactor::remove_closed() {
    const auto now = std::chrono::steady_clock::now();
    for (auto it = connections_.begin(); it != connections_.end();) {
        auto& connection = *it->second;
        if (!connection.closed() && connection.expired(now)) connection.on_timeout();
        if (connection.closed()) {
            read_eagain_count_.fetch_add(connection.read_eagain_count(), std::memory_order_relaxed);
            write_eagain_count_.fetch_add(connection.write_eagain_count(), std::memory_order_relaxed);
            it = connections_.erase(it);
        } else ++it;
    }
}

void EpollReactor::run_once() {
    std::array<epoll_event, 128> events{};
    int count;
    do {
        count = ::epoll_wait(epoll_fd_.get(), events.data(), static_cast<int>(events.size()),
                             config_.epoll_wait_timeout_ms);
    } while (count < 0 && errno == EINTR);
    if (count < 0) throw std::runtime_error("epoll_wait failed");
    for (int i = 0; i < count; ++i) {
        const auto token = events[static_cast<std::size_t>(i)].data.u64;
        const auto flags = events[static_cast<std::size_t>(i)].events;
        if (token == kListenerToken) { accept_ready(); continue; }
        if (token == kStopToken) { stop_requested_.store(true, std::memory_order_release); continue; }
        if (token == kCompletionToken) {
            for (const auto& completion : completions_.drain(config_.max_completions_per_tick)) {
                const auto found = connections_.find(completion.correlation.connection_token);
                if (found == connections_.end()) continue;
                found->second->on_delivery(completion);
                update_interest(*found->second);
            }
            continue;
        }
        if (token == kAuthCompletionToken) {
            for (const auto& completion : auth_completions_.drain(config_.max_completions_per_tick)) {
                const auto found = connections_.find(completion.correlation.connection_token);
                if (found == connections_.end()) continue;
                found->second->on_auth_completion(completion);
                update_interest(*found->second);
            }
            continue;
        }
        const auto found = connections_.find(token);
        if (found == connections_.end()) continue;  // stale event: ID/generation no longer exists.
        auto& connection = *found->second;
        if ((flags & EPOLLIN) != 0U) connection.on_readable();
        if (!connection.closed() && (flags & EPOLLOUT) != 0U) connection.on_writable();
        if (!connection.closed() && (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U)
            connection.on_peer_event();
        update_interest(connection);
    }
    remove_closed();
}
}  // namespace nexstream::ingest::infrastructure
