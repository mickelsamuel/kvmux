#include "config.hpp"

#include <catch2/catch_test_macros.hpp>

using kvmux::config::BackendType;
using kvmux::config::ConfigError;
using kvmux::config::load_string;
using kvmux::config::RoutingPolicy;

namespace {
constexpr const char* kMinimal = R"(
[[backends]]
name = "ollama"
type = "ollama"
base_url = "http://127.0.0.1:11434"
models = ["llama3.2:3b"]
)";
}

TEST_CASE("config: full valid surface parses with correct values", "[config]") {
    const char* toml = R"(
[server]
listen = "0.0.0.0"
port = 8080
max_concurrent_streams = 256
queue_depth = 64
queue_timeout_ms = 5000
request_timeout_ms = 300000

[metrics]
port = 9090

[routing]
policy = "prefix_affinity"
prefix_bytes = 1024
load_threshold = 0.8

[[backends]]
name = "vllm-a"
type = "vllm"
base_url = "http://127.0.0.1:8001"
models = ["Qwen/Qwen2.5-3B-Instruct"]
max_in_flight = 64
health_interval_ms = 2000

[[backends]]
name = "llama-1"
type = "llamacpp"
base_url = "http://127.0.0.1:8002"
models = ["qwen2.5-3b-q4"]
max_in_flight = 8

[[backends]]
name = "ollama"
type = "ollama"
base_url = "http://127.0.0.1:11434"
models = ["llama3.2:3b"]
max_in_flight = 8
)";
    auto cfg = load_string(toml);

    CHECK(cfg.server.listen == "0.0.0.0");
    CHECK(cfg.server.port == 8080);
    CHECK(cfg.server.max_concurrent_streams == 256);
    CHECK(cfg.server.queue_depth == 64);
    CHECK(cfg.server.queue_timeout_ms == 5000);
    CHECK(cfg.server.request_timeout_ms == 300000);
    CHECK(cfg.metrics.port == 9090);
    CHECK(cfg.routing.policy == RoutingPolicy::PrefixAffinity);
    CHECK(cfg.routing.prefix_bytes == 1024);
    CHECK(cfg.routing.load_threshold == 0.8);

    REQUIRE(cfg.backends.size() == 3);
    CHECK(cfg.backends[0].name == "vllm-a");
    CHECK(cfg.backends[0].type == BackendType::Vllm);
    CHECK(cfg.backends[0].base_url == "http://127.0.0.1:8001");
    CHECK(cfg.backends[0].models == std::vector<std::string>{"Qwen/Qwen2.5-3B-Instruct"});
    CHECK(cfg.backends[0].max_in_flight == 64);
    CHECK(cfg.backends[0].health_interval_ms == 2000);
    CHECK(cfg.backends[1].type == BackendType::Llamacpp);
    CHECK(cfg.backends[1].max_in_flight == 8);
    CHECK(cfg.backends[1].health_interval_ms == 2000); // default applied
    CHECK(cfg.backends[2].type == BackendType::Ollama);
}

TEST_CASE("config: minimal config applies defaults", "[config]") {
    auto cfg = load_string(kMinimal);
    CHECK(cfg.server.port == 8080);
    CHECK(cfg.server.max_concurrent_streams == 256);
    CHECK(cfg.metrics.port == 9090);
    CHECK(cfg.routing.policy == RoutingPolicy::PrefixAffinity);
    CHECK(cfg.routing.prefix_bytes == 1024);
    REQUIRE(cfg.backends.size() == 1);
    CHECK(cfg.backends[0].max_in_flight == 8); // default
}

TEST_CASE("config: round_robin policy parses", "[config]") {
    std::string toml = std::string("[routing]\npolicy = \"round_robin\"\n") + kMinimal;
    auto cfg = load_string(toml);
    CHECK(cfg.routing.policy == RoutingPolicy::RoundRobin);
}

TEST_CASE("config: unknown top-level key is an error", "[config][failfast]") {
    std::string toml = std::string("[bogus]\nx = 1\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: unknown key inside [server] is an error", "[config][failfast]") {
    std::string toml = std::string("[server]\nlissten = \"0.0.0.0\"\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: unknown key inside a backend is an error", "[config][failfast]") {
    const char* toml = R"(
[[backends]]
name = "ollama"
type = "ollama"
base_url = "http://127.0.0.1:11434"
models = ["llama3.2:3b"]
typpo = true
)";
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: no backends is an error", "[config][failfast]") {
    REQUIRE_THROWS_AS(load_string("[server]\nport = 8080\n"), ConfigError);
}

TEST_CASE("config: missing required backend field is an error", "[config][failfast]") {
    const char* toml = R"(
[[backends]]
name = "x"
type = "ollama"
models = ["m"]
)"; // no base_url
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: invalid backend type is an error", "[config][failfast]") {
    const char* toml = R"(
[[backends]]
name = "x"
type = "tensorrt"
base_url = "http://x"
models = ["m"]
)";
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: invalid routing policy is an error", "[config][failfast]") {
    std::string toml = std::string("[routing]\npolicy = \"magic\"\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: out-of-range port is an error", "[config][failfast]") {
    std::string toml = std::string("[server]\nport = 70000\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: wrong type for port is an error", "[config][failfast]") {
    std::string toml = std::string("[server]\nport = \"8080\"\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: load_threshold out of [0,1] is an error", "[config][failfast]") {
    std::string toml = std::string("[routing]\nload_threshold = 1.5\n") + kMinimal;
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: duplicate backend names is an error", "[config][failfast]") {
    const char* toml = R"(
[[backends]]
name = "dup"
type = "ollama"
base_url = "http://a"
models = ["m"]

[[backends]]
name = "dup"
type = "vllm"
base_url = "http://b"
models = ["m"]
)";
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: empty models array is an error", "[config][failfast]") {
    const char* toml = R"(
[[backends]]
name = "x"
type = "ollama"
base_url = "http://x"
models = []
)";
    REQUIRE_THROWS_AS(load_string(toml), ConfigError);
}

TEST_CASE("config: malformed TOML is a ConfigError (not a crash)", "[config][failfast]") {
    REQUIRE_THROWS_AS(load_string("[server\nport = 8080"), ConfigError);
}
