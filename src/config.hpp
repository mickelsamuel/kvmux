#pragma once

// TOML configuration loader. Fail-fast: any structural problem, type mismatch,
// out-of-range value, or unknown key is a hard error with a TOML source line.
// The schema here is the complete v1 configuration surface; anything not
// represented is, by design, not configurable.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace kvmux::config {

enum class BackendType { Vllm, Llamacpp, Ollama };

const char* to_string(BackendType type) noexcept;

struct BackendConfig {
    std::string name;
    BackendType type = BackendType::Vllm;
    std::string base_url;
    std::vector<std::string> models;
    int max_in_flight = 8;
    int health_interval_ms = 2000;
};

struct ServerConfig {
    std::string listen = "0.0.0.0";
    int port = 8080;
    int max_concurrent_streams = 256;
    int queue_depth = 64;
    int queue_timeout_ms = 5000;
    int request_timeout_ms = 300000;
};

struct MetricsConfig {
    int port = 9090;
};

enum class RoutingPolicy { PrefixAffinity, RoundRobin };

const char* to_string(RoutingPolicy policy) noexcept;

struct RoutingConfig {
    RoutingPolicy policy = RoutingPolicy::PrefixAffinity;
    int prefix_bytes = 1024;
    double load_threshold = 0.8;
};

struct Config {
    ServerConfig server;
    MetricsConfig metrics;
    RoutingConfig routing;
    std::vector<BackendConfig> backends;
};

// Raised on any configuration problem. `what()` is a human-readable message
// that, where toml++ provides one, includes the source line/column.
class ConfigError : public std::runtime_error {
  public:
    explicit ConfigError(const std::string& message) : std::runtime_error(message) {}
};

// Load + validate from a file path. Throws ConfigError on any problem.
Config load_file(const std::string& path);

// Load + validate from an in-memory TOML string (used by tests). `source_name`
// is used in error messages.
Config load_string(const std::string& toml, const std::string& source_name = "<string>");

} // namespace kvmux::config
