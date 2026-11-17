# Sprint 05 actual results

| Classification | Count |
|---|---:|
| PASS | 16 |
| FAIL | 0 |
| BLOCKED | 1 |
| NOT RUN | 1 |

- Debug, Release and ASan/UBSan: 8/8 CTest executables passed in each configuration.
- Real PostgreSQL 16 migration, lookup, invalid-secret and revocation test passed.
- Real PostgreSQL stop produced fail-closed unavailable results; restart rerun passed.
- HAProxy 2.9.12 config validation passed. OpenSSL TLS 1.3 chain/hostname verification passed;
  wrong hostname and TLS 1.2 failed as expected.
- 100 concurrent cache-hit authentication operations passed. This is not a benchmark.
- Monitoring Agent TLS code is BLOCKED because no Agent implementation repository exists.
- Full Compose topology is NOT RUN because the environment has no Docker Compose plugin; PostgreSQL,
  HAProxy and Gateway boundaries were executed independently.
- Sprint 04 review found no test-proven ACK contract defect. Completion overflow remains bounded and
  cannot manufacture SUCCESS; Kafka timeout remains UNKNOWN; pending records/bytes remain bounded.

No benchmark ran. No private key or production credential was committed.
