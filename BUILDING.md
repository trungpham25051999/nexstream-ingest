# Building nexstream-ingest

Requirements: Linux, CMake 3.20+, GCC/Clang with C++20, protoc and libprotobuf 3.21+, and a reviewed
Sprint 01 `nexstream-contracts` checkout.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DNEXSTREAM_CONTRACTS_ROOT=/absolute/path/to/nexstream-contracts
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

The source checkout path is an interim workspace integration. Before release, publish and pin a
contracts artifact so Ingest does not depend on another repository's mutable checkout.
