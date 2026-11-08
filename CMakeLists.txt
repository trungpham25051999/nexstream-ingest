cmake_minimum_required(VERSION 3.20)
project(nexstream_ingest VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(nexstream_contracts 0.1 REQUIRED CONFIG)
find_package(Threads REQUIRED)

add_library(nexstream_ingest_core
    src/infrastructure/frame_decoder.cpp
    src/infrastructure/tcp_acceptor.cpp)
add_library(nexstream::ingest_core ALIAS nexstream_ingest_core)
target_link_libraries(nexstream_ingest_core PUBLIC nexstream::contracts_validation)
target_include_directories(nexstream_ingest_core PUBLIC include)
target_compile_options(nexstream_ingest_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

include(CTest)
if(BUILD_TESTING)
    add_executable(nexstream_ingest_frame_decoder_tests tests/unit/frame_decoder_tests.cpp)
    target_link_libraries(nexstream_ingest_frame_decoder_tests PRIVATE nexstream_ingest_core)
    target_include_directories(nexstream_ingest_frame_decoder_tests PRIVATE tests)
    add_test(NAME frame_decoder_unit COMMAND nexstream_ingest_frame_decoder_tests)
    set_tests_properties(frame_decoder_unit PROPERTIES LABELS unit)
endif()
