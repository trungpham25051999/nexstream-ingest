# nexstream-ingest

[![CI](https://github.com/trungpham25051999/nexstream-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/trungpham25051999/nexstream-ingest/actions/workflows/ci.yml) ![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus) ![epoll](https://img.shields.io/badge/I%2FO-epoll-555) ![Kafka](https://img.shields.io/badge/Kafka-librdkafka-231F20?logo=apachekafka) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Sprint 07 container integration: `NEXSTREAM_LISTEN_ADDRESS` overrides the default `127.0.0.1`.
Use `0.0.0.0` only on the isolated development backend network; no Gateway port is published by Compose.

Sprint 05 C++20 Gateway with bounded Linux `epoll` Level-Triggered I/O, asynchronous librdkafka
delivery, and PostgreSQL-backed per-Agent authentication. Cache misses run on a bounded worker
queue and return through `eventfd`; the Reactor never performs database I/O.

## Important scope warning

TLS terminates at HAProxy and the backend must remain private. The Gateway requires PostgreSQL
conninfo at startup; there is no authentication stub in the executable. Kafka is real only with a
real broker. Never put passwords in committed configuration—use a libpq `passfile` runtime secret.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DCMAKE_PREFIX_PATH=/absolute/path/to/nexstream-contracts-prefix \
  -DRDKAFKA_ROOT=/absolute/path/to/librdkafka-prefix \
  -DLIBPQ_ROOT=/absolute/path/to/libpq-prefix
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

Run locally on loopback:

```sh
LD_LIBRARY_PATH=/absolute/dependency/lib \
  ./build/nexstream-ingest --port 9000 --kafka-bootstrap 127.0.0.1:9092 \
  --postgres 'host=127.0.0.1 dbname=nexstream user=nexstream passfile=/run/secrets/pgpass'
```

The default topic is `telemetry.events.v1`; the Kafka key is `host_id`. Producer durability uses
idempotence and `acks=all`. Kafka polling runs off the epoll thread and reports bounded completions
through `eventfd`.

## Resource and fairness limits

Defaults include 1,024 connections, 64 KiB frames, 128 KiB send buffer/connection, 1 MiB batch,
50 records/batch, 10-second session deadline, 4,096 pending Kafka records, 16 MiB pending Kafka
bytes, and a 4,096-entry completion queue. Queue exhaustion is rejected immediately; there is no
unbounded application retry or buffering.

Authentication defaults: four workers, 1,024 queued misses, 512 KiB queued secret bytes, 4,096
cache entries, five-second TTL and one-second version reconciliation. Fresh cache entries may work
during registry outage; cold/expired misses fail closed. Worker-owned secret strings are cleansed
after verification and secrets are never logged.

See [status](docs/STATUS.md), [processing design](docs/PROCESS.md), and
[actual tests](tests/results/README.md).
