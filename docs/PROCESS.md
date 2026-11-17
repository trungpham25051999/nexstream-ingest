# nexstream-ingest processing guide through Sprint 05

## Implemented flow

`accept4(nonblocking)` → epoll LT → bounded recv → incremental 4-byte BE decoder → Protobuf parse
→ protocol/session validation → async registry authentication → Kafka delivery tracking → bounded
nonblocking send buffer → close.

State order is `AuthRequest → auth completion → BatchStart → Telemetry* → BatchEnd`. Cache misses
use bounded workers and libpq; an eventfd completion with stable connection token returns to epoll.
No telemetry reaches Kafka before authentication. Kafka SUCCESS still requires delivery reports;
UNKNOWN remains non-success.

## Ownership and failure behavior

- Reactor exclusively owns connections and their movable RAII file descriptors.
- epoll data carries `(connection_id,generation)`; stale events are discarded if the token no longer
  exists, avoiding fd-reuse and raw-pointer lifetime errors.
- Decoder validates length before payload reserve. Receive/send/batch/connection counts are bounded.
- Reads/writes retry `EINTR`, stop on `EAGAIN`, and have per-tick byte/frame budgets.
- ERR/HUP/RDHUP, malformed input, timeout, and peer disconnect release connection state.

Evidence: `tests/results/2026-11-17-sprint-05/`. TLS terminates at HAProxy. Public deployment,
production credential provisioning and a Monitoring Agent implementation are not provided.
