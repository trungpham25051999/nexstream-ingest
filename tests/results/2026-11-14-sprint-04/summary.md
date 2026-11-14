# Sprint 04 actual results

| Classification | Count |
|---|---:|
| PASS | 12 |
| FAIL | 0 |
| BLOCKED | 0 |
| NOT RUN | 0 |

- Debug, Release and ASan/UBSan: 6/6 CTest executables passed in each configuration.
- Contracts v1: 2/2 tests passed, including golden vectors; package consumer passed.
- Publisher package-migration regression: 4/4 tests passed.
- Real Redpanda: direct delivery, Gateway/Publisher delivery and broker restart passed.
- During outage Publisher exited 3 with `delivery unresolved or retryable`; after restart it exited
  0 with `delivery success`.

No performance benchmark was run.

See `real-broker.log` for the captured broker health, consumed record and restart outcomes.
