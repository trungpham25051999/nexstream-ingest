# Sprint 02 and Sprint 04 test cases

| ID | Scenario | Actual observation | Status |
|---|---|---|---|
| TC-TCP-001 | Header split 1+3 | Header reassembled and frame delivered once | PASS |
| TC-TCP-002 | Fragmented payload | Waited for complete payload, delivered once | PASS |
| TC-TCP-003 | Coalesced frames | Frames decoded and processed in order | PASS |
| TC-TCP-004 | Zero/65,537-byte length | Rejected before payload allocation | PASS |
| TC-TCP-005 | Truncated/malformed Protobuf | Parser rejected safely | PASS |
| TC-TCP-006 | EINTR/EAGAIN/short I/O | Signal interrupted epoll safely; real socket EAGAIN retained output; 7-byte writes completed | PASS |
| TC-TCP-007 | Disconnect mid-frame | Connection removed; subsequent client succeeded | PASS |
| TC-TCP-008 | Concurrent clients | 100 simultaneous clients authenticated and made progress | PASS |
| TC-TCP-009 | fd reuse/stale event | New connection after mid-frame close received only its own response | PASS |
| TC-TCP-010 | BatchStart before Auth | ProtocolError returned; session closed | PASS |
| TC-TCP-011 | Batch count/byte limits | 51 samples and >1 MiB modeled input rejected | PASS |

Additional executed cases: idle timeout, host/count validation, 50-record slow-reader ACK, and the
rule that delivery stub ACK remains `UNKNOWN`. TC-TCP-008 is a functional 100-client test, not a
throughput, latency, RSS, or capacity benchmark.

## Sprint 04

| ID | Scenario | Actual observation | Status |
|---|---|---|---|
| TC-KAFKA-001 | Versioned contracts install/export | Independent `find_package(nexstream_contracts 0.1)` consumer built and ran | PASS |
| TC-KAFKA-002 | Protobuf v1 compatibility | Existing unit and golden-vector tests passed unchanged | PASS |
| TC-KAFKA-003 | Async enqueue/correlation | Stable connection/request correlation; key is `host_id` | PASS |
| TC-KAFKA-004 | SUCCESS gating | Batch stayed pending until every delivery completion succeeded | PASS |
| TC-KAFKA-005 | Partial delivery | Mixed per-record results produced non-success aggregate and record results | PASS |
| TC-KAFKA-006 | Timeout/uncertain result | Delivery timeout mapped to UNKNOWN, never SUCCESS | PASS |
| TC-KAFKA-007 | Producer queue full | Bounded admission rejected immediately with RETRY_LATER | PASS |
| TC-KAFKA-008 | Completion queue/eventfd | Bounded queue woke Reactor; overflow could not manufacture success | PASS |
| TC-KAFKA-009 | Disconnect/late callback | Stable token discarded completion for closed/reused connection safely | PASS |
| TC-KAFKA-010 | Kafka unavailable | Producer remained bounded and returned non-success | PASS |
| TC-KAFKA-011 | Real Redpanda delivery | Consumed record used topic `telemetry.events.v1` and key `publisher-cli-host` | PASS |
| TC-KAFKA-012 | Broker restart | SUCCESS before stop, unresolved during outage, SUCCESS after restart | PASS |

Debug, Release and ASan/UBSan each passed 6/6 CTest executables. Real-broker cases used Redpanda
v24.3.18 and are functional integrations, not benchmarks.

## Sprint 05

| ID | Scenario | Actual observation | Status |
|---|---|---|---|
| TC-AUTH-001 | Registry migration/storage | PostgreSQL schema accepted only salt + 32-byte verifier; no plaintext column | PASS |
| TC-AUTH-002 | Valid/invalid secret | Real and unit paths authenticated valid PBKDF2 verifier and rejected wrong secret | PASS |
| TC-AUTH-003 | Revoked/expired key | Both states rejected | PASS |
| TC-AUTH-004 | Agent/host binding | Host mismatch returned HOST_NOT_AUTHORIZED | PASS |
| TC-AUTH-005 | Cache cold/hit/TTL | Cold lookup populated bounded cache; hit avoided DB; TTL expired entry | PASS |
| TC-AUTH-006 | Registry outage | Fresh cache worked; cold and expired misses failed closed | PASS |
| TC-AUTH-007 | Rotation | Two distinct active key IDs authenticated during transition | PASS |
| TC-AUTH-008 | Revocation notification | LISTEN/NOTIFY invalidated cached verifier; fixture lag measured under 500 ms | PASS |
| TC-AUTH-009 | Missed notification | Durable change version reconciliation invalidated all stale entries | PASS |
| TC-AUTH-010 | Worker backpressure | One worker/one queued miss fast-rejected the next attempt | PASS |
| TC-AUTH-011 | Auth timeout/disconnect | Pending auth timed out; late/disconnected tokens could not authorize data | PASS |
| TC-AUTH-012 | Concurrent authentication | 100 concurrent cache-hit authentications completed with bounded cache | PASS |
| TC-AUTH-013 | PostgreSQL unavailable/restart | Real stopped DB returned unavailable; test passed again after restart | PASS |
| TC-AUTH-014 | HAProxy TLS 1.3 | Correct CA and `localhost` hostname verified successfully | PASS |
| TC-AUTH-015 | Certificate hostname failure | Wrong hostname failed verification | PASS |
| TC-AUTH-016 | TLS version floor | TLS 1.2 handshake was rejected | PASS |
| TC-AUTH-017 | Monitoring Agent TLS client | No Monitoring Agent implementation repository exists | BLOCKED |
| TC-AUTH-018 | Full Docker Compose topology | Compose plugin unavailable; components tested independently | NOT RUN |

No production secret or private key was committed. TLS material was generated under `/tmp`, used
for local verification and deleted. The 100-client case is functional stress, not a benchmark.
