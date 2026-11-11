#include <arpa/inet.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <csignal>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>
#include "nexstream/ingest/infrastructure/epoll_reactor.hpp"
#include "nexstream/ingest/infrastructure/frame_decoder.hpp"
#include "test_harness.hpp"
#include "mock_producer.hpp"
#include "mock_authenticator.hpp"

namespace transport = nexstream::transport::v1;
using nexstream::ingest::infrastructure::EpollReactor;
using nexstream::ingest::infrastructure::ReactorConfig;
using nexstream::ingest::infrastructure::encode_frame;
using namespace std::chrono_literals;

namespace {
void noop_signal_handler(int) {}
class ClientFd {
  public:
    explicit ClientFd(int fd = -1) : fd_(fd) {}
    ~ClientFd() { if (fd_ >= 0) ::close(fd_); }
    ClientFd(const ClientFd&) = delete;
    ClientFd& operator=(const ClientFd&) = delete;
    ClientFd(ClientFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ClientFd& operator=(ClientFd&& other) noexcept {
        if (this != &other) { if (fd_ >= 0) ::close(fd_); fd_ = other.fd_; other.fd_ = -1; }
        return *this;
    }
    int get() const { return fd_; }
  private: int fd_;
};

struct ServerFixture {
    explicit ServerFixture(std::chrono::milliseconds timeout = 2s,
                           MockProducer::Mode mode = MockProducer::Mode::kImmediateUnknown,
                           bool drop_auth = false)
        : producer(channel, mode), authenticator(auth_channel,
              nexstream::ingest::application::AuthStatus::kAuthenticated, drop_auth),
          reactor(producer, channel, authenticator, auth_channel, make_config(timeout)),
        thread([this] { reactor.run(); }) {}
    ~ServerFixture() { reactor.request_stop(); thread.join(); }
    static ReactorConfig make_config(std::chrono::milliseconds timeout) {
        ReactorConfig config; config.port = 0; config.max_connections = 256;
        config.epoll_wait_timeout_ms = 5; config.connection.session_timeout = timeout;
        config.connection.test_write_chunk_bytes = 7;
        config.connection.test_socket_send_buffer_bytes = 1'024; return config;
    }
    nexstream::ingest::infrastructure::CompletionChannel channel;
    MockProducer producer;
    nexstream::ingest::infrastructure::AuthCompletionChannel auth_channel;
    MockAuthenticator authenticator;
    EpollReactor reactor;
    std::thread thread;
};

ClientFd connect_client(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("client socket failed");
    timeval timeout{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in endpoint{}; endpoint.sin_family = AF_INET; endpoint.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&endpoint), sizeof(endpoint)) < 0) {
        ::close(fd); throw std::runtime_error("connect failed");
    }
    return ClientFd(fd);
}

bool send_all(int fd, const std::vector<std::byte>& bytes, std::size_t chunk = 0) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto request = chunk == 0 ? bytes.size() - offset : std::min(chunk, bytes.size() - offset);
        const auto count = ::send(fd, bytes.data() + offset, request, MSG_NOSIGNAL);
        if (count <= 0) return false;
        offset += static_cast<std::size_t>(count);
    }
    return true;
}

bool read_exact(int fd, void* output, std::size_t size) {
    auto* data = static_cast<std::byte*>(output);
    std::size_t offset = 0;
    while (offset < size) {
        const auto count = ::recv(fd, data + offset, size - offset, 0);
        if (count <= 0) return false;
        offset += static_cast<std::size_t>(count);
    }
    return true;
}

bool receive_frame(int fd, transport::Frame& frame) {
    std::uint8_t header[4]{};
    if (!read_exact(fd, header, sizeof(header))) return false;
    const auto size = (static_cast<std::uint32_t>(header[0]) << 24U) |
                      (static_cast<std::uint32_t>(header[1]) << 16U) |
                      (static_cast<std::uint32_t>(header[2]) << 8U) |
                      static_cast<std::uint32_t>(header[3]);
    if (size == 0 || size > 65'536) return false;
    std::vector<std::byte> payload(size);
    return read_exact(fd, payload.data(), payload.size()) &&
           frame.ParseFromArray(payload.data(), static_cast<int>(payload.size()));
}

transport::Frame auth_frame() {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_auth_request();
    value->set_agent_id("integration-agent"); value->set_host_id("integration-host");
    value->set_key_id("fixture-key"); value->set_secret("test-secret"); return frame;
}
transport::Frame start_frame(std::uint32_t count) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_batch_start(); value->set_batch_id("batch-01");
    value->set_host_id("integration-host"); value->set_expected_count(count); return frame;
}
transport::Frame telemetry_frame(std::uint32_t index) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_telemetry()->mutable_snapshot();
    value->set_host_id("integration-host"); value->set_message_id("message-" + std::to_string(index));
    value->set_sample_timestamp_ms(1'800'000'000'000 + index);
    value->set_cpu_usage_percent(25); value->set_memory_used_bytes(1); value->set_memory_total_bytes(2);
    return frame;
}
transport::Frame telemetry_frame_with_long_id(std::uint32_t index) {
    auto frame = telemetry_frame(index);
    frame.mutable_telemetry()->mutable_snapshot()->set_message_id(
        "message-" + std::to_string(index) + "-" + std::string(108, 'x'));
    return frame;
}
transport::Frame end_frame(std::uint32_t count) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* value = frame.mutable_batch_end(); value->set_batch_id("batch-01");
    value->set_sent_count(count); return frame;
}

bool authenticate(int fd, std::size_t chunk = 0) {
    if (!send_all(fd, encode_frame(auth_frame()), chunk)) return false;
    transport::Frame response;
    return receive_frame(fd, response) && response.has_auth_result() &&
           response.auth_result().code() == transport::AUTHENTICATED;
}
}  // namespace

int main() {
    TestHarness test;
    struct sigaction action{};
    action.sa_handler = noop_signal_handler;
    sigemptyset(&action.sa_mask);
    sigaction(SIGUSR1, &action, nullptr);
    ServerFixture server;

    pthread_kill(server.thread.native_handle(), SIGUSR1);
    {
        auto client = connect_client(server.reactor.port());
        test.expect(authenticate(client.get()), "TC-TCP-006 reactor resumes after EINTR");
    }

    {
        auto client = connect_client(server.reactor.port());
        test.expect(authenticate(client.get(), 1), "fragmented TCP AuthRequest succeeds");
        std::vector<std::byte> coalesced = encode_frame(start_frame(1));
        const auto telemetry = encode_frame(telemetry_frame(1));
        const auto end = encode_frame(end_frame(1));
        coalesced.insert(coalesced.end(), telemetry.begin(), telemetry.end());
        coalesced.insert(coalesced.end(), end.begin(), end.end());
        test.expect(send_all(client.get(), coalesced), "coalesced protocol frames sent");
        transport::Frame ack;
        test.expect(receive_frame(client.get(), ack) && ack.has_batch_ack(),
                    "coalesced frames produce BatchAck");
        test.expect(ack.batch_ack().overall_result() == transport::UNKNOWN,
                    "stub ACK explicitly not Kafka-confirmed");
    }

    {
        auto client = connect_client(server.reactor.port());
        test.expect(send_all(client.get(), encode_frame(start_frame(0))), "invalid transition sent");
        transport::Frame error;
        test.expect(receive_frame(client.get(), error) && error.has_protocol_error(),
                    "TC-TCP-010 invalid transition returns protocol error");
    }

    {
        auto client = connect_client(server.reactor.port());
        const std::vector<std::byte> oversized{std::byte{0}, std::byte{1}, std::byte{0}, std::byte{1}};
        test.expect(send_all(client.get(), oversized), "oversized header sent");
        std::byte value{};
        test.expect(::recv(client.get(), &value, 1, 0) == 0, "oversized frame closes connection");
    }

    {
        auto client = connect_client(server.reactor.port());
        const auto wire = encode_frame(auth_frame());
        send_all(client.get(), std::vector<std::byte>(wire.begin(), wire.begin() + 6));
    }
    {
        auto client = connect_client(server.reactor.port());
        test.expect(authenticate(client.get()), "server survives disconnect mid-frame and fd reuse");
    }

    {
        auto slow = connect_client(server.reactor.port());
        test.expect(authenticate(slow.get()), "slow-client auth succeeds");
        test.expect(send_all(slow.get(), encode_frame(start_frame(50))), "large batch starts");
        for (std::uint32_t i = 0; i < 50; ++i)
            test.expect(send_all(slow.get(), encode_frame(telemetry_frame_with_long_id(i))),
                        "batch frame sent");
        test.expect(send_all(slow.get(), encode_frame(end_frame(50))), "large batch ends");
        std::this_thread::sleep_for(50ms);
        transport::Frame ack;
        test.expect(receive_frame(slow.get(), ack) && ack.batch_ack().message_results_size() == 50,
                    "partial-write path preserves full ACK for slow reader");
    }

    {
        std::vector<ClientFd> clients;
        clients.reserve(100);
        for (int i = 0; i < 100; ++i) clients.push_back(connect_client(server.reactor.port()));
        for (auto& client : clients)
            test.expect(send_all(client.get(), encode_frame(auth_frame())), "concurrent auth sent");
        for (auto& client : clients) {
            transport::Frame response;
            test.expect(receive_frame(client.get(), response) && response.has_auth_result(),
                        "100 concurrent clients make progress");
        }
    }

    test.expect(server.reactor.read_eagain_count() > 0,
                "TC-TCP-006 nonblocking read EAGAIN observed without spin");

    {
        ServerFixture delayed(100ms, MockProducer::Mode::kManual);
        {
            auto client = connect_client(delayed.reactor.port());
            test.expect(authenticate(client.get()), "manual producer session authenticates");
            send_all(client.get(), encode_frame(start_frame(1)));
            send_all(client.get(), encode_frame(telemetry_frame(1)));
            send_all(client.get(), encode_frame(end_frame(1)));
            for (int i = 0; i < 100 && delayed.producer.event_count() == 0; ++i)
                std::this_thread::sleep_for(1ms);
        }
        test.expect(delayed.producer.event_count() == 1,
                    "delivery remains pending after client disconnect");
        (void)delayed.channel.try_push({delayed.producer.event(0).correlation,
                                       nexstream::ingest::application::DeliveryStatus::kSuccess});
        std::this_thread::sleep_for(20ms);

        auto timeout_client = connect_client(delayed.reactor.port());
        test.expect(authenticate(timeout_client.get()), "timeout session authenticates");
        send_all(timeout_client.get(), encode_frame(start_frame(1)));
        send_all(timeout_client.get(), encode_frame(telemetry_frame(2)));
        send_all(timeout_client.get(), encode_frame(end_frame(1)));
        for (int i = 0; i < 100 && delayed.producer.event_count() < 2; ++i)
            std::this_thread::sleep_for(1ms);
        std::this_thread::sleep_for(130ms);
        std::byte value{};
        test.expect(::recv(timeout_client.get(), &value, 1, 0) == 0,
                    "connection timeout closes while Kafka result is pending");
        (void)delayed.channel.try_push({delayed.producer.event(1).correlation,
                                       nexstream::ingest::application::DeliveryStatus::kSuccess});
        std::this_thread::sleep_for(20ms);
        auto next = connect_client(delayed.reactor.port());
        test.expect(authenticate(next.get()),
                    "late completions after disconnect/timeout do not affect new connection");
    }

    {
        ServerFixture timeout_server(100ms);
        auto client = connect_client(timeout_server.reactor.port());
        std::this_thread::sleep_for(180ms);
        std::byte value{};
        test.expect(::recv(client.get(), &value, 1, 0) == 0, "idle connection timeout closes client");
    }

    {
        ServerFixture auth_timeout(100ms, MockProducer::Mode::kImmediateUnknown, true);
        auto client = connect_client(auth_timeout.reactor.port());
        test.expect(send_all(client.get(), encode_frame(auth_frame())), "pending auth request sent");
        std::this_thread::sleep_for(180ms); std::byte value{};
        test.expect(::recv(client.get(), &value, 1, 0) == 0,
                    "auth timeout closes connection without accepting telemetry");
    }

    {
        ServerFixture auth_disconnect(500ms, MockProducer::Mode::kImmediateUnknown, true);
        {
            auto client = connect_client(auth_disconnect.reactor.port());
            test.expect(send_all(client.get(), encode_frame(auth_frame())), "auth sent before disconnect");
            for (int i = 0; i < 100 && auth_disconnect.authenticator.last_token() == 0; ++i)
                std::this_thread::sleep_for(1ms);
        }
        const auto stale_token = auth_disconnect.authenticator.last_token();
        test.expect(stale_token != 0, "disconnected auth attempt captured stable token");
        (void)auth_disconnect.auth_channel.try_push({{stale_token},
            nexstream::ingest::application::AuthStatus::kAuthenticated});
        auth_disconnect.authenticator.set_drop(false); std::this_thread::sleep_for(20ms);
        auto next = connect_client(auth_disconnect.reactor.port());
        test.expect(authenticate(next.get()), "late auth completion does not authorize reused connection");
    }

    return test.finish();
}
