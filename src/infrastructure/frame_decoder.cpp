#include "nexstream/ingest/infrastructure/frame_decoder.hpp"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace nexstream::ingest::infrastructure {
FrameDecoder::FrameDecoder(std::size_t max_frame_bytes, std::size_t max_buffer_bytes)
    : max_frame_bytes_(max_frame_bytes), max_buffer_bytes_(max_buffer_bytes) {
    if (max_frame_bytes_ == 0 || max_buffer_bytes_ < 4 ||
        max_frame_bytes_ + 4 > max_buffer_bytes_) {
        throw std::invalid_argument("invalid FrameDecoder limits");
    }
}

DecodeResult FrameDecoder::feed(std::span<const std::byte> bytes) {
    DecodeResult result;
    if (failed_) { result.error = DecodeError::kMalformedProtobuf; return result; }
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        if (expected_payload_ == 0) {
            const auto count = std::min<std::size_t>(4 - header_size_, bytes.size() - offset);
            std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), count,
                        header_.begin() + static_cast<std::ptrdiff_t>(header_size_));
            header_size_ += count;
            offset += count;
            if (header_size_ < 4) break;
            expected_payload_ =
                (std::to_integer<std::uint32_t>(header_[0]) << 24U) |
                (std::to_integer<std::uint32_t>(header_[1]) << 16U) |
                (std::to_integer<std::uint32_t>(header_[2]) << 8U) |
                std::to_integer<std::uint32_t>(header_[3]);
            if (expected_payload_ == 0) {
                failed_ = true; result.error = DecodeError::kZeroLength; return result;
            }
            if (expected_payload_ > max_frame_bytes_) {
                failed_ = true; result.error = DecodeError::kFrameTooLarge; return result;
            }
            if (static_cast<std::size_t>(expected_payload_) + 4 > max_buffer_bytes_) {
                failed_ = true; result.error = DecodeError::kBufferLimit; return result;
            }
            payload_.clear();
            payload_.reserve(expected_payload_);
        }

        const auto needed = static_cast<std::size_t>(expected_payload_) - payload_.size();
        const auto count = std::min(needed, bytes.size() - offset);
        payload_.insert(payload_.end(), bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                        bytes.begin() + static_cast<std::ptrdiff_t>(offset + count));
        offset += count;
        if (payload_.size() != expected_payload_) continue;

        nexstream::transport::v1::Frame frame;
        if (!frame.ParseFromArray(payload_.data(), static_cast<int>(payload_.size()))) {
            failed_ = true; result.error = DecodeError::kMalformedProtobuf; return result;
        }
        result.frames.push_back(std::move(frame));
        header_size_ = 0;
        expected_payload_ = 0;
        payload_.clear();
    }
    return result;
}

std::size_t FrameDecoder::buffered_bytes() const noexcept {
    return header_size_ + payload_.size();
}

std::vector<std::byte> encode_frame(const nexstream::transport::v1::Frame& frame) {
    const auto size = frame.ByteSizeLong();
    if (size == 0 || size > 65'536) throw std::length_error("frame cannot be encoded");
    std::vector<std::byte> wire(size + 4);
    const auto value = static_cast<std::uint32_t>(size);
    wire[0] = static_cast<std::byte>((value >> 24U) & 0xffU);
    wire[1] = static_cast<std::byte>((value >> 16U) & 0xffU);
    wire[2] = static_cast<std::byte>((value >> 8U) & 0xffU);
    wire[3] = static_cast<std::byte>(value & 0xffU);
    if (!frame.SerializeToArray(wire.data() + 4, static_cast<int>(size)))
        throw std::runtime_error("protobuf serialization failed");
    return wire;
}
}  // namespace nexstream::ingest::infrastructure
