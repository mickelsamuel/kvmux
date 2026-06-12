#include "health/checker.hpp"

#include <catch2/catch_test_macros.hpp>

using kvmux::health::evaluate_health_endpoint;
using kvmux::health::evaluate_ollama;
using kvmux::health::HealthState;
using kvmux::health::ProbeResult;

namespace {
ProbeResult conn(int status, std::string body = "") {
    return ProbeResult{true, status, std::move(body)};
}
ProbeResult unreachable() { return ProbeResult{false, 0, ""}; }
} // namespace

// --- vLLM / llama.cpp /health (status-code-only) ----------------------------

TEST_CASE("health: /health 200 is HEALTHY (vLLM empty body, llama.cpp ok body)", "[health]") {
    CHECK(evaluate_health_endpoint(conn(200)) == HealthState::Healthy);
    CHECK(evaluate_health_endpoint(conn(200, R"({"status":"ok"})")) == HealthState::Healthy);
}

TEST_CASE("health: /health 503 is NOT_READY, never DEAD (llama.cpp loading model)", "[health]") {
    // The locked spec: 503-while-loading = NOT_READY (retry), not DEAD.
    CHECK(evaluate_health_endpoint(conn(503, R"({"error":{"message":"Loading model"}})")) ==
          HealthState::NotReady);
    CHECK(evaluate_health_endpoint(conn(503)) == HealthState::NotReady);
}

TEST_CASE("health: /health unreachable is DEAD", "[health]") {
    CHECK(evaluate_health_endpoint(unreachable()) == HealthState::Dead);
}

TEST_CASE("health: /health other status (500/404) is DEAD", "[health]") {
    CHECK(evaluate_health_endpoint(conn(500)) == HealthState::Dead);
    CHECK(evaluate_health_endpoint(conn(404)) == HealthState::Dead);
}

// --- Ollama (liveness GET / + readiness GET /api/tags) ----------------------

TEST_CASE("health: Ollama live banner + tags 200 is HEALTHY", "[health]") {
    auto live = conn(200, "Ollama is running");
    auto tags = conn(200, R"({"models":[]})");
    CHECK(evaluate_ollama(live, tags) == HealthState::Healthy);
}

TEST_CASE("health: Ollama live but tags not 200 is NOT_READY", "[health]") {
    auto live = conn(200, "Ollama is running");
    CHECK(evaluate_ollama(live, conn(503)) == HealthState::NotReady);
    CHECK(evaluate_ollama(live, unreachable()) == HealthState::NotReady);
}

TEST_CASE("health: Ollama not live (no banner / unreachable) is DEAD", "[health]") {
    // Wrong body on / -> not live.
    CHECK(evaluate_ollama(conn(200, "something else"), conn(200)) == HealthState::Dead);
    // Unreachable / -> dead regardless of tags.
    CHECK(evaluate_ollama(unreachable(), conn(200)) == HealthState::Dead);
}
