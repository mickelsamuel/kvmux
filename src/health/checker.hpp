#pragma once

// Per-backend health checking (locked spec, 03_PLAN.md / findings Q4.2).
//
// States: HEALTHY / NOT_READY / DEAD.
//   * HEALTHY  — backend is up and serving; eligible for routing.
//   * NOT_READY — backend is up but loading (retry later); NOT eligible, NOT dead.
//   * DEAD     — backend is unreachable / failing liveness; NOT eligible.
//
// The probe logic per backend type is exactly:
//   vLLM      GET /health, status-code-only (200 -> HEALTHY, 503 -> NOT_READY,
//             anything else / unreachable -> DEAD). Never /ping.
//   llama.cpp GET /health: 200 -> HEALTHY; 503 (loading model) -> NOT_READY
//             (retry, not dead); unreachable / other -> DEAD.
//   Ollama    no /health. Liveness GET / must return body containing
//             "Ollama is running"; readiness GET /api/tags must be 200. Live
//             but not ready -> NOT_READY; live + ready -> HEALTHY; not live ->
//             DEAD.
//
// `evaluate_*` are pure functions over probe results so they are unit-testable
// without sockets. `HealthMonitor` drives the periodic async probes on the
// io_context and exposes the latest state atomically for the routing path.

#include "config.hpp"
#include "upstream/client.hpp"

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace kvmux::health {

enum class HealthState { Healthy, NotReady, Dead };

const char* to_string(HealthState state) noexcept;

// --- Pure evaluation of a single probe result -------------------------------

// One probe outcome (an HTTP GET result, or a failure to connect).
struct ProbeResult {
    bool connected = false; // TCP+HTTP exchange completed
    int http_status = 0;    // HTTP status (0 if never received)
    std::string body;       // response body (used only for Ollama liveness)
};

// vLLM / llama.cpp share the /health status-code semantics (200 healthy, 503
// not-ready/loading, else dead). The body is ignored.
HealthState evaluate_health_endpoint(const ProbeResult& health);

// Ollama: combine the liveness probe (GET /, body must contain
// "Ollama is running") and the readiness probe (GET /api/tags == 200).
HealthState evaluate_ollama(const ProbeResult& liveness, const ProbeResult& readiness);

// --- Periodic monitor -------------------------------------------------------

// Owns the latest health state for one backend and runs a periodic probe loop.
// The probe loop is a coroutine spawned on the gateway's io_context; it stops
// when stop() is called (used by graceful shutdown / teardown).
class HealthMonitor {
  public:
    HealthMonitor(boost::asio::io_context& ioc, config::BackendType type, upstream::Client& client,
                  std::chrono::milliseconds interval);

    HealthMonitor(const HealthMonitor&) = delete;
    HealthMonitor& operator=(const HealthMonitor&) = delete;

    HealthState state() const noexcept { return state_.load(std::memory_order_relaxed); }
    bool healthy() const noexcept { return state() == HealthState::Healthy; }

    // The probe loop. Runs one probe immediately, then every `interval` until
    // stop() is requested. Spawn with co_spawn(..., detached).
    boost::asio::awaitable<void> run();

    // Run exactly one probe now and update state (also used by tests).
    boost::asio::awaitable<void> probe_once();

    void stop() noexcept { stopped_.store(true, std::memory_order_relaxed); }

  private:
    boost::asio::io_context& ioc_;
    config::BackendType type_;
    upstream::Client& client_;
    std::chrono::milliseconds interval_;
    std::atomic<HealthState> state_{HealthState::Dead};
    std::atomic<bool> stopped_{false};
};

} // namespace kvmux::health
