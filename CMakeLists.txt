cmake_minimum_required(VERSION 3.20)
project(nexstream_ingest VERSION 0.5.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(nexstream_contracts 0.1 REQUIRED CONFIG)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
set(LIBPQ_ROOT "" CACHE PATH "Optional local libpq prefix")
find_path(LIBPQ_INCLUDE_DIR libpq-fe.h HINTS "${LIBPQ_ROOT}/usr/include/postgresql" "${LIBPQ_ROOT}/include")
find_library(LIBPQ_LIBRARY NAMES pq HINTS "${LIBPQ_ROOT}/usr/lib/x86_64-linux-gnu" "${LIBPQ_ROOT}/lib")
if(NOT LIBPQ_INCLUDE_DIR OR NOT LIBPQ_LIBRARY)
    message(FATAL_ERROR "libpq not found; set LIBPQ_ROOT")
endif()
add_library(PostgreSQL::PQ UNKNOWN IMPORTED)
set_target_properties(PostgreSQL::PQ PROPERTIES IMPORTED_LOCATION "${LIBPQ_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBPQ_INCLUDE_DIR}")
option(NEXSTREAM_ENABLE_KAFKA "Build the librdkafka adapter" ON)
set(RDKAFKA_ROOT "" CACHE PATH "Optional local librdkafka prefix")
set(NEXSTREAM_KAFKA_BROKER "" CACHE STRING "Broker for opt-in real Kafka integration test")
set(NEXSTREAM_POSTGRES_TEST_CONN "" CACHE STRING "Conninfo for opt-in real PostgreSQL test")
if(NEXSTREAM_ENABLE_KAFKA)
    find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h
        HINTS "${RDKAFKA_ROOT}/usr/include" "${RDKAFKA_ROOT}/include")
    find_library(RDKAFKA_LIBRARY NAMES rdkafka
        HINTS "${RDKAFKA_ROOT}/usr/lib/x86_64-linux-gnu" "${RDKAFKA_ROOT}/lib")
    if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
        message(FATAL_ERROR "librdkafka not found; set RDKAFKA_ROOT or disable NEXSTREAM_ENABLE_KAFKA")
    endif()
    add_library(RdKafka::rdkafka UNKNOWN IMPORTED)
    set_target_properties(RdKafka::rdkafka PROPERTIES
        IMPORTED_LOCATION "${RDKAFKA_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKAFKA_INCLUDE_DIR}")
endif()

add_library(nexstream_ingest_core
    src/application/protocol_session.cpp
    src/infrastructure/frame_decoder.cpp
    src/infrastructure/completion_channel.cpp
    src/infrastructure/auth_completion_channel.cpp
    src/infrastructure/credential_cache.cpp
    src/infrastructure/authentication_service.cpp
    src/infrastructure/postgres_credential_registry.cpp
    src/infrastructure/tcp_acceptor.cpp
    src/infrastructure/tcp_connection.cpp
    src/infrastructure/epoll_reactor.cpp)
if(NEXSTREAM_ENABLE_KAFKA)
    target_sources(nexstream_ingest_core PRIVATE src/infrastructure/kafka_producer.cpp)
    target_link_libraries(nexstream_ingest_core PUBLIC RdKafka::rdkafka Threads::Threads)
    target_compile_definitions(nexstream_ingest_core PUBLIC NEXSTREAM_HAS_KAFKA=1)
endif()
add_library(nexstream::ingest_core ALIAS nexstream_ingest_core)
target_link_libraries(nexstream_ingest_core PUBLIC nexstream::contracts_validation)
target_link_libraries(nexstream_ingest_core PUBLIC OpenSSL::Crypto PostgreSQL::PQ)
target_include_directories(nexstream_ingest_core PUBLIC include)
target_compile_options(nexstream_ingest_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(nexstream-ingest src/bootstrap/main.cpp)
target_link_libraries(nexstream-ingest PRIVATE nexstream_ingest_core)
target_compile_options(nexstream-ingest PRIVATE -Wall -Wextra -Wpedantic)

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

    add_executable(nexstream_ingest_delivery_tests tests/unit/delivery_tracking_tests.cpp)
    target_link_libraries(nexstream_ingest_delivery_tests PRIVATE nexstream_ingest_core)
    target_include_directories(nexstream_ingest_delivery_tests PRIVATE tests)
    add_test(NAME delivery_tracking_unit COMMAND nexstream_ingest_delivery_tests)
    set_tests_properties(delivery_tracking_unit PROPERTIES LABELS unit)

    add_executable(nexstream_ingest_authentication_tests tests/unit/authentication_service_tests.cpp)
    target_link_libraries(nexstream_ingest_authentication_tests PRIVATE nexstream_ingest_core)
    target_include_directories(nexstream_ingest_authentication_tests PRIVATE tests)
    add_test(NAME authentication_service_unit COMMAND nexstream_ingest_authentication_tests)
    set_tests_properties(authentication_service_unit PROPERTIES LABELS unit TIMEOUT 30)

    if(NEXSTREAM_POSTGRES_TEST_CONN)
        add_executable(nexstream_ingest_postgres_tests tests/integration/postgres_registry_tests.cpp)
        target_link_libraries(nexstream_ingest_postgres_tests PRIVATE nexstream_ingest_core)
        target_include_directories(nexstream_ingest_postgres_tests PRIVATE tests)
        target_compile_definitions(nexstream_ingest_postgres_tests PRIVATE
            NEXSTREAM_POSTGRES_TEST_CONN="${NEXSTREAM_POSTGRES_TEST_CONN}")
        add_test(NAME postgres_registry_integration COMMAND nexstream_ingest_postgres_tests)
        set_tests_properties(postgres_registry_integration PROPERTIES LABELS "integration;postgres-real" TIMEOUT 20)
    endif()

    if(NEXSTREAM_ENABLE_KAFKA)
        add_executable(nexstream_ingest_kafka_adapter_tests tests/integration/kafka_adapter_tests.cpp)
        target_link_libraries(nexstream_ingest_kafka_adapter_tests PRIVATE nexstream_ingest_core)
        target_include_directories(nexstream_ingest_kafka_adapter_tests PRIVATE tests)
        add_test(NAME kafka_unavailable_integration COMMAND nexstream_ingest_kafka_adapter_tests)
        set_tests_properties(kafka_unavailable_integration PROPERTIES LABELS integration TIMEOUT 10)
        if(NEXSTREAM_KAFKA_BROKER)
            add_executable(nexstream_ingest_kafka_real_tests
                tests/integration/kafka_real_integration_tests.cpp)
            target_link_libraries(nexstream_ingest_kafka_real_tests PRIVATE nexstream_ingest_core)
            target_include_directories(nexstream_ingest_kafka_real_tests PRIVATE tests)
            target_compile_definitions(nexstream_ingest_kafka_real_tests PRIVATE
                NEXSTREAM_KAFKA_BROKER="${NEXSTREAM_KAFKA_BROKER}")
            add_test(NAME kafka_real_integration COMMAND nexstream_ingest_kafka_real_tests)
            set_tests_properties(kafka_real_integration PROPERTIES LABELS "integration;kafka-real"
                TIMEOUT 15)
        endif()
    endif()

    add_executable(nexstream_ingest_tcp_integration_tests tests/integration/tcp_gateway_tests.cpp)
    target_link_libraries(nexstream_ingest_tcp_integration_tests PRIVATE nexstream_ingest_core Threads::Threads)
    target_include_directories(nexstream_ingest_tcp_integration_tests PRIVATE tests)
    add_test(NAME tcp_gateway_integration COMMAND nexstream_ingest_tcp_integration_tests)
    set_tests_properties(tcp_gateway_integration PROPERTIES LABELS integration TIMEOUT 30)
endif()
