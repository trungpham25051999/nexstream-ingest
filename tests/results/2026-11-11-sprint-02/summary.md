# Sprint 02 test execution

- Date: 2026-11-11
- Revision: local uncommitted `main` workspace
- OS/kernel: Debian GNU/Linux, 6.1.0-40-amd64, x86_64
- Toolchain: GCC 12.2.0, CMake 3.25.1, Protobuf 3.21.12
- Contract hashes: telemetry `29b531448235c06fcdbc1dc2b11efc31d8172555fba2f5e487b88dd8f6eb1547`;
  transport `8499b1c40fd9268a2b8fada9a756570a8b0e985f49fe027c246f7708b72cbc6b`

| Test ID | Observed | Status |
|---|---|---|
| TC-TCP-001 | 1+3 header fragmentation reassembled | PASS |
| TC-TCP-002 | payload fragments delivered exactly once | PASS |
| TC-TCP-003 | coalesced frames decoded in order | PASS |
| TC-TCP-004 | zero and 65,537-byte lengths rejected before payload reserve | PASS |
| TC-TCP-005 | malformed Protobuf rejected | PASS |
| TC-TCP-006 | epoll EINTR resumed; real send EAGAIN retained buffer; forced 7-byte writes completed | PASS |
| TC-TCP-007 | mid-frame disconnect cleaned safely; next client succeeded | PASS |
| TC-TCP-008 | 100 simultaneous clients authenticated and progressed | PASS |
| TC-TCP-009 | new connection after fd release received only its own response | PASS |
| TC-TCP-010 | BatchStart before Auth returned ProtocolError | PASS |
| TC-TCP-011 | 51 samples and modeled >1 MiB batch rejected | PASS |

Totals for Sprint 02 cases: **PASS 11, FAIL 0, BLOCKED 0, NOT RUN 0**.

Debug, Release, and ASan/UBSan each passed 4/4 CTest executables. The TCP integration test also
passed five consecutive Debug repetitions. Functional concurrency is not a benchmark; no
throughput, latency percentile, CPU, or RSS claim is made.
