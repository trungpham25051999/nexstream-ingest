#include <array>
#include <cerrno>
#include <cstddef>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include "nexstream/ingest/infrastructure/frame_decoder.hpp"
#include "nexstream/ingest/infrastructure/tcp_connection.hpp"
#include "test_harness.hpp"
#include "mock_producer.hpp"
#include "mock_authenticator.hpp"

using nexstream::ingest::infrastructure::ConnectionLimits;
using nexstream::ingest::infrastructure::TcpConnection;
using nexstream::ingest::infrastructure::UniqueFd;
using nexstream::ingest::infrastructure::encode_frame;

int main() {
    TestHarness test;
    int sockets[2]{};
    test.expect(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets) == 0,
                "nonblocking socketpair created");
    UniqueFd peer(sockets[1]);
    ConnectionLimits limits;
    limits.test_write_chunk_bytes = 7;
    nexstream::ingest::infrastructure::CompletionChannel channel;
    MockProducer producer(channel);
    nexstream::ingest::infrastructure::AuthCompletionChannel auth_channel;
    MockAuthenticator authenticator(auth_channel);
    TcpConnection connection(UniqueFd(sockets[0]), 7, 11, limits, producer, authenticator);

    nexstream::transport::v1::Frame auth;
    auth.set_protocol_version(1);
    auto* value = auth.mutable_auth_request();
    value->set_agent_id("agent"); value->set_host_id("host"); value->set_key_id("key");
    value->set_secret("test-secret");
    const auto wire = encode_frame(auth);
    test.expect(::send(peer.get(), wire.data(), wire.size(), MSG_NOSIGNAL) ==
                    static_cast<ssize_t>(wire.size()),
                "auth request written to socketpair");
    connection.on_readable();
    const auto auth_results = auth_channel.drain(1);
    connection.on_auth_completion(auth_results[0]);
    test.expect(connection.wants_write(), "auth response is buffered");

    std::array<std::byte, 4096> filler{};
    while (true) {
        const auto count = ::send(connection.fd(), filler.data(), filler.size(), MSG_NOSIGNAL);
        if (count > 0) continue;
        if (count < 0 && errno == EINTR) continue;
        test.expect(count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                    "server send queue filled to real EAGAIN");
        break;
    }
    connection.on_writable();
    test.expect(connection.write_eagain_count() == 1 && connection.wants_write(),
                "TC-TCP-006 EAGAIN retains pending output without spin or truncation");
    return test.finish();
}
