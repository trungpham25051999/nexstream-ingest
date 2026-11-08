#include "nexstream/ingest/infrastructure/tcp_acceptor.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>

namespace nexstream::ingest::infrastructure {
TcpAcceptor::TcpAcceptor(std::string_view address, std::uint16_t port, int backlog) {
    fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd_) throw std::runtime_error("socket failed");
    int one = 1;
    if (::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
        throw std::runtime_error("setsockopt SO_REUSEADDR failed");
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    const std::string address_string(address);
    if (::inet_pton(AF_INET, address_string.c_str(), &endpoint.sin_addr) != 1)
        throw std::invalid_argument("invalid IPv4 listen address");
    if (::bind(fd_.get(), reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) < 0)
        throw std::runtime_error("bind failed");
    if (::listen(fd_.get(), backlog) < 0) throw std::runtime_error("listen failed");
    socklen_t size = sizeof(endpoint);
    if (::getsockname(fd_.get(), reinterpret_cast<sockaddr*>(&endpoint), &size) < 0)
        throw std::runtime_error("getsockname failed");
    port_ = ntohs(endpoint.sin_port);
}

UniqueFd TcpAcceptor::accept_one() const {
    const int client = ::accept4(fd_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (client < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return {};
        if (errno == EINTR) return {};
        throw std::runtime_error("accept4 failed");
    }
    return UniqueFd(client);
}
}  // namespace nexstream::ingest::infrastructure
