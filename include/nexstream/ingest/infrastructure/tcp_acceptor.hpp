#pragma once
#include <cstdint>
#include <string_view>
#include "nexstream/ingest/infrastructure/unique_fd.hpp"

namespace nexstream::ingest::infrastructure {
class TcpAcceptor {
  public:
    TcpAcceptor(std::string_view address, std::uint16_t port, int backlog);
    [[nodiscard]] int fd() const noexcept { return fd_.get(); }
    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] UniqueFd accept_one() const;
  private:
    UniqueFd fd_;
    std::uint16_t port_{0};
};
}  // namespace nexstream::ingest::infrastructure
