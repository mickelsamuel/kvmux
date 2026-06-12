#include "server/gateway.hpp"

#include "openai/errors.hpp"
#include "openai/types.hpp"
#include "server/sse_writer.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/beast/version.hpp>
#include <chrono>

namespace kvmux::server {

namespace asio = boost::asio;
using namespace std::chrono_literals;
using kvmux::openai::ErrorKind;
using json = nlohmann::json;

namespace {
constexpr auto kStreamBudget = 300000ms; // request_timeout_ms default (stream)
constexpr auto kUnaryBudget = 120000ms;  // non-stream default

std::string error_json(ErrorKind kind, const std::string& msg, const std::string& param = "") {
    return openai::make_error(kind, msg, param).dump();
}
} // namespace

Gateway::Gateway(asio::io_context& ioc, config::Config cfg) : ioc_(ioc), cfg_(std::move(cfg)) {
    for (const auto& bc : cfg_.backends) {
        auto b = std::make_unique<Backend>();
        b->conf = bc;
        b->client = std::make_unique<upstream::Client>(ioc_, upstream::Target::parse(bc.base_url));
        b->health = std::make_unique<health::HealthMonitor>(
            ioc_, bc.type, *b->client, std::chrono::milliseconds(bc.health_interval_ms));
        b->breaker = std::make_unique<health::CircuitBreaker>(
            bc.failure_threshold, std::chrono::milliseconds(bc.open_ms));
        b->limiter = std::make_unique<admission::PerBackendLimiter>(bc.max_in_flight);
        backends_.push_back(std::move(b));
    }
    gate_ = std::make_unique<admission::GlobalGate>(
        ioc_, cfg_.server.max_concurrent_streams, cfg_.server.queue_depth,
        std::chrono::milliseconds(cfg_.server.queue_timeout_ms));
}

void Gateway::start_health_monitors() {
    for (auto& b : backends_) {
        asio::co_spawn(ioc_, b->health->run(), asio::detached);
    }
}

void Gateway::stop_health_monitors() {
    for (auto& b : backends_) {
        b->health->stop();
    }
}

void Gateway::set_health_for_test(std::size_t backend_index, health::HealthState st) {
    backends_.at(backend_index)->forced_health.store(static_cast<int>(st));
}

health::HealthState Gateway::observed_health(const Backend& b) const {
    int forced = b.forced_health.load();
    if (forced >= 0) {
        return static_cast<health::HealthState>(forced);
    }
    return b.health->state();
}

health::HealthState Gateway::health_of(std::size_t backend_index) const {
    return observed_health(*backends_.at(backend_index));
}

health::BreakerState Gateway::breaker_of(std::size_t backend_index) const {
    return backends_.at(backend_index)->breaker->state();
}

std::vector<std::size_t> Gateway::backends_for_model(const std::string& model) const {
    std::vector<std::size_t> out;
    for (std::size_t i = 0; i < backends_.size(); ++i) {
        for (const auto& m : backends_[i]->conf.models) {
            if (m == model) {
                out.push_back(i);
                break;
            }
        }
    }
    return out;
}

asio::awaitable<bool> Gateway::write_json(beast::tcp_stream& stream, unsigned version,
                                          bool keep_alive, int status,
                                          const std::string& json_body) {
    http::response<http::string_body> res{static_cast<http::status>(status), version};
    res.set(http::field::server, "kvmux");
    res.set(http::field::content_type, "application/json");
    if (status == 429) {
        res.set(http::field::retry_after, "1");
    }
    res.keep_alive(keep_alive);
    res.body() = json_body;
    res.prepare_payload();
    co_await http::async_write(stream, res, asio::use_awaitable);
    co_return keep_alive;
}

asio::awaitable<bool> Gateway::handle(beast::tcp_stream& stream,
                                      http::request<http::string_body> req, bool keep_alive) {
    const auto version = req.version();
    const std::string target = std::string(req.target());

    if (draining()) {
        co_return co_await write_json(stream, version, false, 503,
                                      error_json(ErrorKind::NoBackend, "gateway is shutting down"));
    }

    if (req.method() == http::verb::get && target == "/healthz") {
        co_return co_await write_json(stream, version, keep_alive, 200, R"({"status":"ok"})");
    }
    if (req.method() == http::verb::get &&
        (target == "/v1/models" || target.rfind("/v1/models", 0) == 0)) {
        co_return co_await handle_models(stream, req, keep_alive);
    }
    if (req.method() == http::verb::post && target == "/v1/chat/completions") {
        in_flight_.fetch_add(1);
        bool ka = false;
        try {
            ka = co_await handle_chat(stream, req, keep_alive);
        } catch (...) {
            in_flight_.fetch_sub(1);
            throw;
        }
        in_flight_.fetch_sub(1);
        co_return ka;
    }

    co_return co_await write_json(stream, version, keep_alive, 404,
                                  error_json(ErrorKind::NotFound, "unknown route"));
}

asio::awaitable<bool> Gateway::handle_models(beast::tcp_stream& stream,
                                             const http::request<http::string_body>& req,
                                             bool keep_alive) {
    // Aggregate one card per (backend, model). De-duplicate model ids so a model
    // served by several backends appears once (OpenAI /v1/models lists models,
    // not replicas). First-seen ordering is preserved.
    json data = json::array();
    std::vector<std::string> seen;
    for (const auto& b : backends_) {
        for (const auto& m : b->conf.models) {
            if (std::find(seen.begin(), seen.end(), m) != seen.end()) {
                continue;
            }
            seen.push_back(m);
            openai::ModelCard card;
            card.id = m;
            json j = card;
            data.push_back(j);
        }
    }
    json out = {{"object", "list"}, {"data", data}};
    co_return co_await write_json(stream, req.version(), keep_alive, 200, out.dump());
}

asio::awaitable<bool> Gateway::handle_chat(beast::tcp_stream& stream,
                                           const http::request<http::string_body>& req,
                                           bool keep_alive) {
    const auto version = req.version();

    json body = json::parse(req.body(), nullptr, /*allow_exceptions=*/false);
    if (body.is_discarded()) {
        co_return co_await write_json(stream, version, keep_alive, 400,
                                      error_json(ErrorKind::InvalidRequest, "invalid JSON body"));
    }
    openai::ChatCompletionRequest creq;
    bool parse_failed = false;
    int parse_status = 0;
    std::string parse_body;
    try {
        creq = openai::ChatCompletionRequest::parse(body);
    } catch (const openai::RequestError& e) {
        parse_failed = true;
        parse_status = openai::status_code(e.kind());
        parse_body = e.to_json().dump();
    }
    if (parse_failed) {
        co_return co_await write_json(stream, version, keep_alive, parse_status, parse_body);
    }

    // Which backends serve this model at all (config-level)?
    std::vector<std::size_t> serving = backends_for_model(creq.model);
    if (serving.empty()) {
        co_return co_await write_json(stream, version, keep_alive, 404,
                                      error_json(ErrorKind::UnknownModel,
                                                 "no backend serves model '" + creq.model + "'",
                                                 "model"));
    }

    // --- Admission: acquire a global slot (FIFO queue, bounded, timed) --------
    admission::AdmitResult adm = co_await gate_->acquire();
    if (!adm.admitted()) {
        const char* why = adm.status == admission::AdmitStatus::QueueFull
                              ? "admission queue is full"
                              : "admission queue wait timed out";
        co_return co_await write_json(stream, version, keep_alive, 429,
                                      error_json(ErrorKind::Admission, why));
    }
    admission::Ticket ticket(*gate_); // releases the global slot on scope exit

    // --- Eligibility: HEALTHY + breaker-allows + serves-model, ordered -------
    // Build the candidate set among serving backends that are HEALTHY. (Breaker
    // admission is checked per-attempt below so the half-open probe slot is
    // consumed atomically at the moment of use.)
    std::vector<routing::Candidate> candidates;
    for (std::size_t idx : serving) {
        Backend& b = *backends_[idx];
        if (observed_health(b) != health::HealthState::Healthy) {
            continue;
        }
        routing::Candidate c;
        c.backend_index = idx;
        c.active_requests = b.limiter->active();
        c.max_in_flight = b.limiter->max_in_flight();
        candidates.push_back(c);
    }
    if (candidates.empty()) {
        // No HEALTHY backend serves the model right now.
        co_return co_await write_json(
            stream, version, keep_alive, 503,
            error_json(ErrorKind::NoBackend, "no eligible backend for model '" + creq.model + "'"));
    }

    // Order the candidates by policy (M3: round-robin; prefix-affinity lands in
    // M4 and reuses this same RouteOrder seam).
    routing::RouteRequest rr;
    routing::RouteOrder ordered = round_robin_.order(rr, candidates);

    // Inject stream_options.include_usage upstream once; forward verbatim.
    json upstream_body = upstream::inject_include_usage(creq.raw);
    std::string upstream_text = upstream_body.dump();

    // ---------------------------------------------------------------------
    // Failover walk. We try eligible backends in `ordered` order. A backend is
    // skippable if its breaker denies it or its per-backend slot is full. The
    // first backend that we actually dispatch to either:
    //   * succeeds -> done,
    //   * fails with ZERO bytes already sent downstream -> retry once on the
    //     next eligible backend (we allow exactly one such retry),
    //   * fails AFTER bytes were sent -> terminal SSE error event, then close
    //     (never silent truncation), no retry.
    // Non-streaming requests mirror this: a connection-level failure with no
    // body written can fail over once.
    // ---------------------------------------------------------------------
    int attempts = 0;
    const int max_attempts = 2; // initial + one zero-byte retry

    for (std::size_t pos = 0; pos < ordered.order.size() && attempts < max_attempts; ++pos) {
        Backend& b = *backends_[ordered.order[pos]];

        // Breaker gate (atomic half-open probe acquisition).
        health::CircuitBreaker::Decision dec = b.breaker->allow_request();
        if (!dec.allowed) {
            continue; // breaker open / probe busy -> next candidate
        }
        // Per-backend in-flight slot. Acquire the count first; the RAII holder
        // only records that we owe one release() on scope exit.
        if (!b.limiter->try_acquire()) {
            // At capacity locally. Balance the breaker decision we took (a denied
            // half-open probe would otherwise wedge the breaker): treat the
            // local skip as a non-event by re-closing on a probe so the probe is
            // not wasted on a capacity skip.
            if (dec.is_probe) {
                b.breaker->on_success(dec.is_probe);
            }
            continue;
        }
        admission::BackendSlot slot(*b.limiter); // releases the slot on scope exit
        ++attempts;

        if (!creq.stream) {
            // ---- Non-streaming path ----
            auto ur = co_await b.client->post_chat(upstream_text, kUnaryBudget);
            if (!ur.connected) {
                b.breaker->on_failure(dec.is_probe);
                // Zero bytes written downstream: eligible for a one-shot retry.
                continue;
            }
            if (ur.http_status >= 500) {
                b.breaker->on_failure(dec.is_probe);
                json wrapped = openai::wrap_upstream_error(ur.body, ur.http_status);
                // 5xx is an upstream failure; but the body is already a complete
                // response (nothing streamed). We may retry once on another
                // backend. If this was our last attempt, surface the error.
                if (attempts < max_attempts && pos + 1 < ordered.order.size()) {
                    continue;
                }
                co_return co_await write_json(stream, version, keep_alive, ur.http_status,
                                              wrapped.dump());
            }
            b.breaker->on_success(dec.is_probe);
            if (ur.http_status >= 400) {
                json wrapped = openai::wrap_upstream_error(ur.body, ur.http_status);
                co_return co_await write_json(stream, version, keep_alive, ur.http_status,
                                              wrapped.dump());
            }
            co_return co_await write_json(stream, version, keep_alive, 200, ur.body);
        }

        // ---- Streaming path ----
        beast::error_code nec;
        stream.socket().set_option(asio::ip::tcp::no_delay(true), nec);

        SseWriter writer(stream, version, keep_alive);
        bool header_started = false;
        bool downstream_dead = false;

        upstream::Client::FrameSink sink =
            [&](const upstream::SseEvent& ev) -> asio::awaitable<bool> {
            if (downstream_dead) {
                co_return false;
            }
            try {
                if (!header_started) {
                    co_await writer.begin();
                    header_started = true;
                }
                co_await writer.write_data(ev.data);
            } catch (...) {
                downstream_dead = true;
                co_return false;
            }
            co_return true;
        };

        auto sr = co_await b.client->stream_chat(upstream_text, sink, kStreamBudget);

        // Did anything reach the client? header_started <=> bytes sent.
        if (!header_started) {
            // Zero bytes downstream. Classify the upstream outcome.
            if (sr.http_status >= 400 || !sr.connected || sr.timed_out || sr.upstream_aborted) {
                b.breaker->on_failure(dec.is_probe);
                // Retry once on the next eligible backend if we have one and
                // budget. Otherwise surface the error as an HTTP status.
                if (attempts < max_attempts && pos + 1 < ordered.order.size()) {
                    continue;
                }
                if (sr.http_status >= 400) {
                    json wrapped = openai::wrap_upstream_error(sr.error_body, sr.http_status);
                    co_return co_await write_json(stream, version, keep_alive, sr.http_status,
                                                  wrapped.dump());
                }
                if (sr.timed_out) {
                    co_return co_await write_json(
                        stream, version, keep_alive, 504,
                        error_json(ErrorKind::UpstreamTimeout, "upstream timed out"));
                }
                co_return co_await write_json(
                    stream, version, keep_alive, 502,
                    error_json(ErrorKind::UpstreamFailure, "upstream connection failed"));
            }
            // Connected, 2xx, but produced nothing: emit a minimal stream + DONE.
            b.breaker->on_success(dec.is_probe);
            co_await writer.begin();
            header_started = true;
            try {
                co_await writer.end();
            } catch (...) {
                co_return false;
            }
            co_return keep_alive;
        }

        // Bytes were sent downstream. No failover from here on.
        if (downstream_dead) {
            // Client went away mid-stream — neither success nor backend failure.
            co_return false;
        }
        if (sr.upstream_aborted || sr.timed_out) {
            // Mid-stream upstream failure after bytes: terminal SSE error event,
            // then close. Never silent truncation. Counts as a backend failure.
            b.breaker->on_failure(dec.is_probe);
            ErrorKind k = sr.timed_out ? ErrorKind::UpstreamTimeout : ErrorKind::UpstreamFailure;
            const char* msg =
                sr.timed_out ? "upstream timed out mid-stream" : "upstream aborted mid-stream";
            try {
                co_await writer.write_data(openai::make_error(k, msg).dump());
                co_await writer.end();
            } catch (...) {
            }
            co_return false;
        }

        // Clean end of stream.
        b.breaker->on_success(dec.is_probe);
        try {
            co_await writer.end();
        } catch (...) {
            co_return false;
        }
        co_return keep_alive;
    }

    // Exhausted eligible candidates (all breaker-denied / at capacity / failed
    // the zero-byte retry) without sending anything downstream.
    co_return co_await write_json(
        stream, version, keep_alive, 503,
        error_json(ErrorKind::NoBackend, "no eligible backend could serve the request"));
}

} // namespace kvmux::server
