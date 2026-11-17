# Sprint 05 status

Updated: 2026-11-17

| Component | Status | Evidence |
|---|---|---|
| Versioned contracts CMake package | VERIFIED | package consumer and v1 golden vectors |
| Async librdkafka producer | VERIFIED | mock/unavailable tests plus local Redpanda integration |
| Kafka-confirmed SUCCESS ACK | VERIFIED | real broker delivery and Publisher/Gateway flow |
| Correlation and partial delivery | VERIFIED | TC-KAFKA-003..006 |
| Bounded completion queue + eventfd | VERIFIED | unit/TCP integration tests |
| Queue/memory backpressure | VERIFIED | queue-full fast rejection tests |
| Disconnect/stale connection safety | VERIFIED | stable token and late-completion tests |
| Broker outage/restart | VERIFIED | local Redpanda stop/start scenario |
| Debug/Release/ASan+UBSan | VERIFIED | 6/6 CTest executables per configuration |
| PostgreSQL credential registry | VERIFIED | PostgreSQL 16 migration/integration test |
| Bounded auth cache/workers/eventfd | VERIFIED | auth unit and TCP integration tests |
| Rotation/revocation/reconciliation | VERIFIED | dual-key, NOTIFY and durable-version tests |
| HAProxy TLS 1.3 termination | VERIFIED | HAProxy 2.9.12 and OpenSSL verification tests |
| Monitoring Agent TLS transport | BLOCKED | no Monitoring Agent implementation repository exists |
| Full Compose topology | NOT RUN | Compose plugin unavailable; boundaries tested independently |
| Performance benchmark | NOT RUN | concurrency test is functional, not a benchmark |

Kafka-confirmed evidence applies only to local Redpanda integration runs. `MockProducer` tests are
behavioral evidence, not proof of Kafka delivery.
