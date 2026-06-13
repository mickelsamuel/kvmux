#pragma once

// Gateway: owns config, per-backend upstream clients, health monitors, circuit
// breakers, per-backend in-flight limiters, the global admission gate, and the
// routing policy. Implements request handling for the served endpoints.
//
// M3 adds, on top of the M2 single-backend spine:
//   * per-backend health monitors (vLLM/llama.cpp/Ollama quirk probes)
//   * per-backend circuit breakers (5 fails -> OPEN 10s -> half-open probe)
//   * eligibility = HEALTHY + breaker-allows + serves-model + per-backend slot
//   * failover: zero-bytes-sent -> retry once on another eligible backend;
//     bytes-sent -> terminal SSE error event (never silent truncation)
//   * admission control: global cap + per-backend max_in_flight + bounded FIFO
//     wait-queue -> 429 + Retry-After on full/timeout
//   * /v1/models aggregation across all backends
//
// Routing policy selection and the full Prometheus metrics surface land in M4;
// the routing seam (RouteOrder over eligible candidates) is already in place.

#include "admission/controller.hpp"
#include "config.hpp"
#include "health/checker.hpp"
#include "health/circuit_breaker.hpp"
#include "metrics/registry.hpp"
#include "routing/prefix_affinity.hpp"
#include "routing/round_robin.hpp"
#include "upstream/client.hpp"

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/string_body.hpp>
#include <memory>
#include <string>
#include <vector>

namespace kvmux::openai {
struct ChatCompletionRequest;
}

namespace kvmux::server {

namespace beast = boost::beast;
namespace http = boost::beast::http;

class Gateway {
  public:
    Gateway(boost::asio::io_context& ioc, config::Config cfg);

    // Spawn the per-backend health-monitor coroutines on the io_context. Call
    // once after construction, before serving. (Tests that don't need live
    // health can skip this and drive state directly.)
    void start_health_monitors();

    // Stop all health monitors (graceful shutdown / teardown).
    void stop_health_monitors();

    // Handle one parsed HTTP request on `stream`. Returns true if the connection
    // may be reused (keep-alive honored), false if it should close.
    boost::asio::awaitable<bool> handle(beast::tcp_stream& stream,
                                        http::request<http::string_body> req, bool keep_alive);

    void begin_drain() noexcept { draining_.store(true); }
    bool draining() const noexcept { return draining_.load(); }
    int in_flight() const noexcept { return in_flight_.load(); }

    const config::Config& cfg() const noexcept { return cfg_; }

    // The metrics registry (scraped by the /metrics listener on the metrics port).
    metrics::Registry& registry() noexcept { return registry_; }
    const metrics::Registry& registry() const noexcept { return registry_; }

    // Render the current Prometheus exposition (refreshes the live gauges first).
    std::string render_metrics();

    // --- Test/diagnostic hooks ------------------------------------------------
    // Override a backend's observed health state (used by integration tests that
    // do not run a real health endpoint).
    void set_health_for_test(std::size_t backend_index, health::HealthState st);
    health::HealthState health_of(std::size_t backend_index) const;
    health::BreakerState breaker_of(std::size_t backend_index) const;

  private:
    struct Backend {
        config::BackendConfig conf;
        std::unique_ptr<upstream::Client> client;
        std::unique_ptr<health::HealthMonitor> health;
        std::unique_ptr<health::CircuitBreaker> breaker;
        std::unique_ptr<admission::PerBackendLimiter> limiter;
        // Test-only forced health override (nullopt -> use the monitor).
        std::atomic<int> forced_health{-1}; // -1 = unset, else HealthState value
    };

    // Indices of backends that serve `model` (regardless of current health).
    std::vector<std::size_t> backends_for_model(const std::string& model) const;

    // Observed health for a backend (test override wins if set).
    health::HealthState observed_health(const Backend& b) const;

    boost::asio::awaitable<bool> handle_chat(beast::tcp_stream& stream,
                                             const http::request<http::string_body>& req,
                                             bool keep_alive);
    boost::asio::awaitable<bool> handle_models(beast::tcp_stream& stream,
                                               const http::request<http::string_body>& req,
                                               bool keep_alive);

    boost::asio::awaitable<bool> write_json(beast::tcp_stream& stream, unsigned version,
                                            bool keep_alive, int status,
                                            const std::string& json_body);

    // Order the eligible candidates by the configured policy, accumulating the
    // affinity spill count into `spills_out`.
    std::vector<std::size_t> order_candidates(const openai::ChatCompletionRequest& creq,
                                              const std::vector<routing::Candidate>& candidates,
                                              int& spills_out);

    // Snapshot the admission gate's running/waiting counts into the registry.
    // Must be called on the io_context thread (the request path) so the gate is
    // not read from another thread.
    void publish_admission_gauges();

    boost::asio::io_context& ioc_;
    config::Config cfg_;
    std::vector<std::unique_ptr<Backend>> backends_;
    std::unique_ptr<admission::GlobalGate> gate_;
    routing::RoundRobin round_robin_;
    routing::PrefixAffinity prefix_affinity_;
    metrics::Registry registry_;
    std::atomic<bool> draining_{false};
    std::atomic<int> in_flight_{0};
};

} // namespace kvmux::server
