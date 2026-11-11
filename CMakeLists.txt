cmake_minimum_required(VERSION 3.20)
project(nexstream_ingest VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(nexstream_contracts 0.1 REQUIRED CONFIG)
find_package(Threads REQUIRED)

add_library(nexstream_ingest_core
    src/application/protocol_session.cpp
    src/infrastructure/frame_decoder.cpp
    src/infrastructure/completion_channel.cpp
    src/infrastructure/auth_completion_channel.cpp
    src/infrastructure/tcp_acceptor.cpp
    src/infrastructure/tcp_connection.cpp
    src/infrastructure/epoll_reactor.cpp)
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

    add_executable(nexstream_ingest_protocol_session_tests tests/unit/protocol_session_tests.cpp)
    target_link_libraries(nexstream_ingest_protocol_session_tests PRIVATE nexstream_ingest_core)
    target_include_directories(nexstream_ingest_protocol_session_tests PRIVATE tests)
    add_test(NAME protocol_session_unit COMMAND nexstream_ingest_protocol_session_tests)
    set_tests_properties(protocol_session_unit PROPERTIES LABELS unit)

    add_executable(nexstream_ingest_connection_io_tests tests/unit/tcp_connection_io_tests.cpp)
    target_link_libraries(nexstream_ingest_connection_io_tests PRIVATE nexstream_ingest_core)
    target_include_directories(nexstream_ingest_connection_io_tests PRIVATE tests)
    add_test(NAME connection_io_unit COMMAND nexstream_ingest_connection_io_tests)
    set_tests_properties(connection_io_unit PROPERTIES LABELS unit)

    add_executable(nexstream_ingest_tcp_integration_tests tests/integration/tcp_gateway_tests.cpp)
    target_link_libraries(nexstream_ingest_tcp_integration_tests PRIVATE nexstream_ingest_core Threads::Threads)
    target_include_directories(nexstream_ingest_tcp_integration_tests PRIVATE tests)
    add_test(NAME tcp_gateway_integration COMMAND nexstream_ingest_tcp_integration_tests)
    set_tests_properties(tcp_gateway_integration PROPERTIES LABELS integration TIMEOUT 30)
endif()
