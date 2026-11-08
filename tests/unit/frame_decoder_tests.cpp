#include <array>
#include <cstddef>
#include <vector>
#include "nexstream/ingest/infrastructure/frame_decoder.hpp"
#include "test_harness.hpp"

using nexstream::ingest::infrastructure::DecodeError;
using nexstream::ingest::infrastructure::FrameDecoder;
using nexstream::ingest::infrastructure::encode_frame;

namespace {
nexstream::transport::v1::Frame auth_frame() {
    nexstream::transport::v1::Frame frame;
    frame.set_protocol_version(1);
    auto* auth = frame.mutable_auth_request();
    auth->set_agent_id("agent"); auth->set_host_id("host"); auth->set_key_id("key");
    auth->set_secret("test-secret");
    return frame;
}
}  // namespace

int main() {
    TestHarness test;
    const auto wire = encode_frame(auth_frame());

    FrameDecoder partial_header;
    auto result = partial_header.feed(std::span(wire).first(1));
    test.expect(result.frames.empty() && result.error == DecodeError::kNone,
                "TC-TCP-001 one-byte header waits");
    result = partial_header.feed(std::span(wire).subspan(1, 3));
    test.expect(result.frames.empty(), "TC-TCP-001 remaining header waits for payload");
    result = partial_header.feed(std::span(wire).subspan(4));
    test.expect(result.frames.size() == 1 && result.frames[0].has_auth_request(),
                "TC-TCP-001 fragmented header reassembles");

    FrameDecoder partial_payload;
    const auto split = 4 + (wire.size() - 4) / 2;
    result = partial_payload.feed(std::span(wire).first(split));
    test.expect(result.frames.empty(), "TC-TCP-002 partial payload waits");
    result = partial_payload.feed(std::span(wire).subspan(split));
    test.expect(result.frames.size() == 1, "TC-TCP-002 payload delivered once");

    std::vector<std::byte> coalesced = wire;
    coalesced.insert(coalesced.end(), wire.begin(), wire.end());
    FrameDecoder multiple;
    result = multiple.feed(coalesced);
    test.expect(result.frames.size() == 2, "TC-TCP-003 coalesced frames retain order");

    const std::array<std::byte, 4> zero{};
    FrameDecoder zero_decoder;
    result = zero_decoder.feed(zero);
    test.expect(result.error == DecodeError::kZeroLength && zero_decoder.failed(),
                "TC-TCP-004 zero length rejected");
    const std::array<std::byte, 4> oversized{std::byte{0x00}, std::byte{0x01},
                                                std::byte{0x00}, std::byte{0x01}};
    FrameDecoder oversized_decoder;
    result = oversized_decoder.feed(oversized);
    test.expect(result.error == DecodeError::kFrameTooLarge &&
                    oversized_decoder.buffered_bytes() == 4,
                "TC-TCP-004 oversized length rejected before payload allocation");

    const std::array<std::byte, 5> malformed{std::byte{0}, std::byte{0}, std::byte{0},
                                             std::byte{1}, std::byte{0xff}};
    FrameDecoder malformed_decoder;
    result = malformed_decoder.feed(malformed);
    test.expect(result.error == DecodeError::kMalformedProtobuf,
                "TC-TCP-005 malformed protobuf rejected");
    return test.finish();
}
