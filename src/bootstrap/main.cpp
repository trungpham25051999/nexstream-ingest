#include <charconv>
#include <cstdint>
#include <iostream>
#include <string_view>
#include <cstdlib>
#include "nexstream/ingest/infrastructure/epoll_reactor.hpp"
#include "nexstream/ingest/infrastructure/completion_channel.hpp"
#include "nexstream/ingest/infrastructure/auth_completion_channel.hpp"
#include "nexstream/ingest/infrastructure/authentication_service.hpp"
#include "nexstream/ingest/infrastructure/postgres_credential_registry.hpp"
#if NEXSTREAM_HAS_KAFKA
#include "nexstream/ingest/infrastructure/kafka_producer.hpp"
#endif

int main(int argc, char** argv) {
    std::uint16_t port = 9000;
    std::string kafka_bootstrap = "127.0.0.1:9092";
    std::string postgres_connection;
    if ((argc == 3 || argc == 5 || argc == 7) && std::string_view(argv[1]) == "--port") {
        unsigned int parsed = 0;
        const std::string_view value(argv[2]);
        const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
        if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || parsed > 65'535) {
            std::cerr << "invalid port\n"; return 2;
        }
        port = static_cast<std::uint16_t>(parsed);
        for (int index = 3; index + 1 < argc; index += 2) {
            if (std::string_view(argv[index]) == "--kafka-bootstrap") kafka_bootstrap = argv[index + 1];
            else if (std::string_view(argv[index]) == "--postgres") postgres_connection = argv[index + 1];
            else { std::cerr << "usage error\n"; return 2; }
        }
    } else if (argc != 1) {
        std::cerr << "usage: nexstream-ingest --port PORT [--kafka-bootstrap HOST:PORT] --postgres CONNINFO\n";
        return 2;
    }
    try {
        nexstream::ingest::infrastructure::ReactorConfig config;
        config.port = port;
        if (const char* address = std::getenv("NEXSTREAM_LISTEN_ADDRESS")) config.listen_address = address;
#if NEXSTREAM_HAS_KAFKA
        nexstream::ingest::infrastructure::CompletionChannel completions;
        nexstream::ingest::infrastructure::KafkaProducerConfig kafka_config;
        kafka_config.bootstrap_servers = kafka_bootstrap;
        nexstream::ingest::infrastructure::KafkaProducer producer(kafka_config, completions);
        if (postgres_connection.empty()) { std::cerr << "--postgres is required (no auth stub)\n"; return 2; }
        nexstream::ingest::infrastructure::AuthCompletionChannel auth_completions;
        auto registry = std::make_shared<nexstream::ingest::infrastructure::PostgresCredentialRegistry>(postgres_connection);
        nexstream::ingest::infrastructure::AuthenticationService authenticator(registry, auth_completions);
        nexstream::ingest::infrastructure::EpollReactor reactor(producer, completions, authenticator,
                                                                 auth_completions, config);
        std::cout << "nexstream-ingest gateway listening on " << config.listen_address << ":"
                  << reactor.port() << '\n';
        std::cout << "Kafka bootstrap=" << kafka_bootstrap
                  << "; PostgreSQL authentication active; TLS terminates at HAProxy.\n";
        reactor.run();
#else
        std::cerr << "Kafka support was disabled at build time\n"; return 2;
#endif
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n'; return 1;
    }
    return 0;
}
