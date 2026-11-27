# Building nexstream-ingest

Requirements: Linux, CMake 3.20+, GCC/Clang with C++20, Protobuf 3.21+, OpenSSL, libpq and
librdkafka development packages (Ubuntu 24.04:
`protobuf-compiler libprotobuf-dev libssl-dev libpq-dev librdkafka-dev`).

Ingest consumes the installed, versioned `nexstream-contracts` package (pinned to `v0.1.0`):

```sh
git clone --branch v0.1.0 https://github.com/trungpham25051999/nexstream-contracts.git
cmake -S nexstream-contracts -B contracts-build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build contracts-build
cmake --install contracts-build --prefix "$HOME/.local/nexstream-contracts"

cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DCMAKE_PREFIX_PATH="$HOME/.local/nexstream-contracts" -DLIBPQ_ROOT=/
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

Optional real-service tests: `-DNEXSTREAM_KAFKA_BROKER=host:port` (Redpanda/Kafka) and
`-DNEXSTREAM_POSTGRES_TEST_CONN=<conninfo>` (credential registry from `nexstream-deploy`).
Sanitizers: add `-DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"`. CI runs Debug, Release and ASan+UBSan.
