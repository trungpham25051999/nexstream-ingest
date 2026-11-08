#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>
#include "nexstream/transport/v1/transport.pb.h"

namespace nexstream::ingest::infrastructure {
enum class DecodeError { kNone, kZeroLength, kFrameTooLarge, kMalformedProtobuf, kBufferLimit };
struct DecodeResult {
    std::vector<nexstream::transport::v1::Frame> frames;
    DecodeError error{DecodeError::kNone};
};

class FrameDecoder {
  public:
    explicit FrameDecoder(std::size_t max_frame_bytes = 65'536,
                          std::size_t max_buffer_bytes = 65'540);
    [[nodiscard]] DecodeResult feed(std::span<const std::byte> bytes);
    [[nodiscard]] std::size_t buffered_bytes() const noexcept;
    [[nodiscard]] bool failed() const noexcept { return failed_; }
  private:
    std::size_t max_frame_bytes_;
    std::size_t max_buffer_bytes_;
    std::array<std::byte, 4> header_{};
    std::size_t header_size_{0};
    std::uint32_t expected_payload_{0};
    std::vector<std::byte> payload_;
    bool failed_{false};
};

[[nodiscard]] std::vector<std::byte> encode_frame(
    const nexstream::transport::v1::Frame& frame);
}  // namespace nexstream::ingest::infrastructure
