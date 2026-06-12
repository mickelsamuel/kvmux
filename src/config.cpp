#include "config.hpp"

#include <algorithm>
#include <fstream>
#include <set>
#include <sstream>

#define TOML_EXCEPTIONS 1
#include <toml++/toml.hpp>

namespace kvmux::config {

const char* to_string(BackendType type) noexcept {
    switch (type) {
    case BackendType::Vllm:
        return "vllm";
    case BackendType::Llamacpp:
        return "llamacpp";
    case BackendType::Ollama:
        return "ollama";
    }
    return "unknown";
}

const char* to_string(RoutingPolicy policy) noexcept {
    switch (policy) {
    case RoutingPolicy::PrefixAffinity:
        return "prefix_affinity";
    case RoutingPolicy::RoundRobin:
        return "round_robin";
    }
    return "unknown";
}

namespace {

[[noreturn]] void fail(const std::string& msg) {
    throw ConfigError(msg);
}

std::string node_loc(const toml::node& n) {
    const auto& src = n.source();
    std::ostringstream os;
    os << " (line " << src.begin.line << ", col " << src.begin.column << ")";
    return os.str();
}

// Reject any key in `tbl` not present in `allowed`.
void reject_unknown_keys(const toml::table& tbl, const std::set<std::string>& allowed,
                         const std::string& section) {
    for (const auto& [key, node] : tbl) {
        if (!allowed.contains(std::string(key.str()))) {
            fail("unknown key '" + std::string(key.str()) + "' in [" + section + "]" +
                 node_loc(node));
        }
    }
}

// Typed required/optional getters with fail-fast messages.
std::string req_string(const toml::table& tbl, const std::string& key, const std::string& section) {
    auto node = tbl.get(key);
    if (!node) {
        fail("missing required key '" + key + "' in [" + section + "]");
    }
    if (!node->is_string()) {
        fail("key '" + key + "' in [" + section + "] must be a string" + node_loc(*node));
    }
    return node->value<std::string>().value();
}

std::string opt_string(const toml::table& tbl, const std::string& key, const std::string& section,
                       const std::string& dflt) {
    auto node = tbl.get(key);
    if (!node) {
        return dflt;
    }
    if (!node->is_string()) {
        fail("key '" + key + "' in [" + section + "] must be a string" + node_loc(*node));
    }
    return node->value<std::string>().value();
}

int opt_int(const toml::table& tbl, const std::string& key, const std::string& section, int dflt,
            int min_val, int max_val) {
    auto node = tbl.get(key);
    if (!node) {
        return dflt;
    }
    if (!node->is_integer()) {
        fail("key '" + key + "' in [" + section + "] must be an integer" + node_loc(*node));
    }
    auto v = node->value<std::int64_t>().value();
    if (v < min_val || v > max_val) {
        fail("key '" + key + "' in [" + section + "] = " + std::to_string(v) +
             " is out of range [" + std::to_string(min_val) + ", " + std::to_string(max_val) + "]" +
             node_loc(*node));
    }
    return static_cast<int>(v);
}

double opt_double(const toml::table& tbl, const std::string& key, const std::string& section,
                  double dflt, double min_val, double max_val) {
    auto node = tbl.get(key);
    if (!node) {
        return dflt;
    }
    double v = 0.0;
    if (node->is_floating_point()) {
        v = node->value<double>().value();
    } else if (node->is_integer()) {
        v = static_cast<double>(node->value<std::int64_t>().value());
    } else {
        fail("key '" + key + "' in [" + section + "] must be a number" + node_loc(*node));
    }
    if (v < min_val || v > max_val) {
        fail("key '" + key + "' in [" + section + "] = " + std::to_string(v) +
             " is out of range [" + std::to_string(min_val) + ", " + std::to_string(max_val) + "]" +
             node_loc(*node));
    }
    return v;
}

BackendType parse_backend_type(const std::string& s, const toml::node& n) {
    if (s == "vllm") {
        return BackendType::Vllm;
    }
    if (s == "llamacpp") {
        return BackendType::Llamacpp;
    }
    if (s == "ollama") {
        return BackendType::Ollama;
    }
    fail("backend 'type' must be one of vllm|llamacpp|ollama, got '" + s + "'" + node_loc(n));
}

RoutingPolicy parse_policy(const std::string& s, const toml::node& n) {
    if (s == "prefix_affinity") {
        return RoutingPolicy::PrefixAffinity;
    }
    if (s == "round_robin") {
        return RoutingPolicy::RoundRobin;
    }
    fail("routing 'policy' must be prefix_affinity|round_robin, got '" + s + "'" + node_loc(n));
}

void parse_server(const toml::table& root, Config& cfg) {
    auto node = root.get("server");
    if (!node) {
        return; // all defaults
    }
    if (!node->is_table()) {
        fail("[server] must be a table" + node_loc(*node));
    }
    const auto& tbl = *node->as_table();
    reject_unknown_keys(tbl,
                        {"listen", "port", "max_concurrent_streams", "queue_depth",
                         "queue_timeout_ms", "request_timeout_ms"},
                        "server");
    auto& s = cfg.server;
    s.listen = opt_string(tbl, "listen", "server", s.listen);
    s.port = opt_int(tbl, "port", "server", s.port, 1, 65535);
    s.max_concurrent_streams =
        opt_int(tbl, "max_concurrent_streams", "server", s.max_concurrent_streams, 1, 1000000);
    s.queue_depth = opt_int(tbl, "queue_depth", "server", s.queue_depth, 0, 1000000);
    s.queue_timeout_ms = opt_int(tbl, "queue_timeout_ms", "server", s.queue_timeout_ms, 0, 3600000);
    s.request_timeout_ms =
        opt_int(tbl, "request_timeout_ms", "server", s.request_timeout_ms, 1, 86400000);
}

void parse_metrics(const toml::table& root, Config& cfg) {
    auto node = root.get("metrics");
    if (!node) {
        return;
    }
    if (!node->is_table()) {
        fail("[metrics] must be a table" + node_loc(*node));
    }
    const auto& tbl = *node->as_table();
    reject_unknown_keys(tbl, {"port"}, "metrics");
    cfg.metrics.port = opt_int(tbl, "port", "metrics", cfg.metrics.port, 1, 65535);
}

void parse_routing(const toml::table& root, Config& cfg) {
    auto node = root.get("routing");
    if (!node) {
        return;
    }
    if (!node->is_table()) {
        fail("[routing] must be a table" + node_loc(*node));
    }
    const auto& tbl = *node->as_table();
    reject_unknown_keys(tbl, {"policy", "prefix_bytes", "load_threshold"}, "routing");
    auto& r = cfg.routing;
    if (auto p = tbl.get("policy")) {
        if (!p->is_string()) {
            fail("routing 'policy' must be a string" + node_loc(*p));
        }
        r.policy = parse_policy(p->value<std::string>().value(), *p);
    }
    r.prefix_bytes = opt_int(tbl, "prefix_bytes", "routing", r.prefix_bytes, 1, 1048576);
    r.load_threshold = opt_double(tbl, "load_threshold", "routing", r.load_threshold, 0.0, 1.0);
}

void parse_backends(const toml::table& root, Config& cfg) {
    auto node = root.get("backends");
    if (!node) {
        fail("at least one [[backends]] entry is required");
    }
    if (!node->is_array_of_tables()) {
        fail("[[backends]] must be an array of tables" + node_loc(*node));
    }
    const auto& arr = *node->as_array();
    if (arr.empty()) {
        fail("at least one [[backends]] entry is required");
    }

    std::set<std::string> seen_names;
    for (const auto& elem : arr) {
        const auto& tbl = *elem.as_table();
        reject_unknown_keys(
            tbl, {"name", "type", "base_url", "models", "max_in_flight", "health_interval_ms"},
            "[backends]]");

        BackendConfig b;
        b.name = req_string(tbl, "name", "[backends]]");
        if (!seen_names.insert(b.name).second) {
            fail("duplicate backend name '" + b.name + "'");
        }

        auto type_node = tbl.get("type");
        if (!type_node || !type_node->is_string()) {
            fail("backend '" + b.name + "' missing required string key 'type'");
        }
        b.type = parse_backend_type(type_node->value<std::string>().value(), *type_node);

        b.base_url = req_string(tbl, "base_url", "[backends]]");

        auto models_node = tbl.get("models");
        if (!models_node || !models_node->is_array() || models_node->as_array()->empty()) {
            fail("backend '" + b.name + "' requires a non-empty 'models' array");
        }
        for (const auto& m : *models_node->as_array()) {
            if (!m.is_string()) {
                fail("backend '" + b.name + "' 'models' entries must be strings" + node_loc(m));
            }
            b.models.push_back(m.value<std::string>().value());
        }

        b.max_in_flight = opt_int(tbl, "max_in_flight", "[backends]]", b.max_in_flight, 1, 1000000);
        b.health_interval_ms =
            opt_int(tbl, "health_interval_ms", "[backends]]", b.health_interval_ms, 100, 3600000);

        cfg.backends.push_back(std::move(b));
    }
}

Config validate(const toml::table& root) {
    // Top-level unknown-section rejection.
    reject_unknown_keys(root, {"server", "metrics", "routing", "backends"}, "<root>");

    Config cfg;
    parse_server(root, cfg);
    parse_metrics(root, cfg);
    parse_routing(root, cfg);
    parse_backends(root, cfg);

    // Cross-field check: queue_depth/timeout relate to admission; nothing fatal
    // beyond ranges already enforced. Model-name uniqueness across backends is
    // intentionally allowed (same model served by multiple backends).
    return cfg;
}

} // namespace

Config load_string(const std::string& toml_text, const std::string& source_name) {
    try {
        toml::table root = toml::parse(toml_text, source_name);
        return validate(root);
    } catch (const toml::parse_error& e) {
        std::ostringstream os;
        os << "TOML parse error in " << source_name << ": " << e.description() << " (line "
           << e.source().begin.line << ", col " << e.source().begin.column << ")";
        throw ConfigError(os.str());
    }
}

Config load_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw ConfigError("cannot open config file: " + path);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return load_string(ss.str(), path);
}

} // namespace kvmux::config
