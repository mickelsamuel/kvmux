#include "health/checker.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace kvmux::health {

namespace asio = boost::asio;
using namespace std::chrono_literals;

const char* to_string(HealthState state) noexcept {
    switch (state) {
    case HealthState::Healthy:
        return "healthy";
    case HealthState::NotReady:
        return "not_ready";
    case HealthState::Dead:
        return "dead";
    }
    return "unknown";
}

HealthState evaluate_health_endpoint(const ProbeResult& health) {
    if (!health.connected) {
        return HealthState::Dead;
    }
    if (health.http_status == 200) {
        return HealthState::Healthy;
    }
    if (health.http_status == 503) {
        // llama.cpp: "Loading model" -> not ready, retry. vLLM: 503 from /health
        // likewise means not-serving-yet, not a dead process.
        return HealthState::NotReady;
    }
    return HealthState::Dead;
}

HealthState evaluate_ollama(const ProbeResult& liveness, const ProbeResult& readiness) {
    // Liveness: GET / must come back and contain the canonical banner. Ollama
    // returns 200 "Ollama is running" on /.
    const bool live = liveness.connected && liveness.http_status == 200 &&
                      liveness.body.find("Ollama is running") != std::string::npos;
    if (!live) {
        return HealthState::Dead;
    }
    // Readiness: /api/tags == 200 means the model index is queryable.
    if (readiness.connected && readiness.http_status == 200) {
        return HealthState::Healthy;
    }
    return HealthState::NotReady;
}

namespace {
constexpr auto kProbeBudget = 2000ms;

ProbeResult to_probe(const upstream::UnaryResult& r) {
    ProbeResult p;
    p.connected = r.connected;
    p.http_status = r.http_status;
    p.body = r.body;
    return p;
}
} // namespace

HealthMonitor::HealthMonitor(asio::io_context& ioc, config::BackendType type,
                             upstream::Client& client, std::chrono::milliseconds interval)
    : ioc_(ioc), type_(type), client_(client), interval_(interval) {}

asio::awaitable<void> HealthMonitor::probe_once() {
    HealthState next = HealthState::Dead;
    switch (type_) {
    case config::BackendType::Vllm:
    case config::BackendType::Llamacpp: {
        auto r = co_await client_.get("/health", kProbeBudget);
        next = evaluate_health_endpoint(to_probe(r));
        break;
    }
    case config::BackendType::Ollama: {
        auto live = co_await client_.get("/", kProbeBudget);
        auto ready = co_await client_.get("/api/tags", kProbeBudget);
        next = evaluate_ollama(to_probe(live), to_probe(ready));
        break;
    }
    }
    state_.store(next, std::memory_order_relaxed);
    co_return;
}

asio::awaitable<void> HealthMonitor::run() {
    auto executor = co_await asio::this_coro::executor;
    asio::steady_timer timer(executor);
    while (!stopped_.load(std::memory_order_relaxed)) {
        co_await probe_once();
        timer.expires_after(interval_);
        boost::system::error_code ec;
        co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            break; // timer cancelled (shutdown)
        }
    }
    co_return;
}

} // namespace kvmux::health
