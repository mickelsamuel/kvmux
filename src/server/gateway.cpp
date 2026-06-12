#include "server/gateway.hpp"

#include "openai/errors.hpp"
#include "openai/types.hpp"
#include "server/sse_writer.hpp"

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
        Backend b;
        b.conf = bc;
        b.client = std::make_unique<upstream::Client>(ioc_, upstream::Target::parse(bc.base_url));
        backends_.push_back(std::move(b));
    }
}

Gateway::Backend* Gateway::select_backend(const std::string& model) {
    for (auto& b : backends_) {
        for (const auto& m : b.conf.models) {
            if (m == model) {
                return &b;
            }
        }
    }
    return nullptr;
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

    // Graceful drain: refuse new work with a 503 OpenAI error.
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
    // M2: static aggregation from config (one card per configured model).
    json data = json::array();
    for (const auto& b : backends_) {
        for (const auto& m : b.conf.models) {
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

    // Parse + validate the request body.
    json body = json::parse(req.body(), nullptr, /*allow_exceptions=*/false);
    if (body.is_discarded()) {
        co_return co_await write_json(stream, version, keep_alive, 400,
                                      error_json(ErrorKind::InvalidRequest, "invalid JSON body"));
    }
    // co_await is not permitted inside a catch handler, so capture the error
    // outcome here and emit the response after the try/catch.
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

    // Select a backend serving the requested model (M2: first match).
    Backend* backend = select_backend(creq.model);
    if (backend == nullptr) {
        co_return co_await write_json(stream, version, keep_alive, 404,
                                      error_json(ErrorKind::UnknownModel,
                                                 "no backend serves model '" + creq.model + "'",
                                                 "model"));
    }

    // Always inject stream_options.include_usage upstream so we get the usage
    // chunk; forward the (possibly modified) body verbatim otherwise.
    json upstream_body = upstream::inject_include_usage(creq.raw);
    std::string upstream_text = upstream_body.dump();

    if (!creq.stream) {
        // Non-streaming path: buffer the upstream response and relay.
        auto ur = co_await backend->client->post_chat(upstream_text, kUnaryBudget);
        if (!ur.connected) {
            co_return co_await write_json(
                stream, version, keep_alive, 502,
                error_json(ErrorKind::UpstreamFailure, "upstream connection failed"));
        }
        if (ur.http_status >= 400) {
            json wrapped = openai::wrap_upstream_error(ur.body, ur.http_status);
            co_return co_await write_json(stream, version, keep_alive, ur.http_status,
                                          wrapped.dump());
        }
        co_return co_await write_json(stream, version, keep_alive, 200, ur.body);
    }

    // --- Streaming path -----------------------------------------------------
    // Downstream socket is TCP_NODELAY too so re-streamed frames flush promptly.
    beast::error_code nec;
    stream.socket().set_option(boost::asio::ip::tcp::no_delay(true), nec);

    SseWriter writer(stream, version, keep_alive);
    bool header_started = false;
    bool downstream_dead = false;

    // Per-frame async sink: lazily send the SSE header on the first frame, then
    // write+flush each frame ([DONE] passes through verbatim). Awaited per frame
    // by the upstream client, so each frame leaves before the next read.
    upstream::Client::FrameSink sink = [&](const upstream::SseEvent& ev) -> asio::awaitable<bool> {
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
            downstream_dead = true; // client went away
            co_return false;
        }
        co_return true;
    };

    auto sr = co_await backend->client->stream_chat(upstream_text, sink, kStreamBudget);

    // Upstream failed before any byte reached the client: map to an HTTP error.
    if (!header_started && !downstream_dead) {
        if (sr.http_status >= 400) {
            json wrapped = openai::wrap_upstream_error(sr.error_body, sr.http_status);
            co_return co_await write_json(stream, version, keep_alive, sr.http_status,
                                          wrapped.dump());
        }
        if (!sr.connected) {
            co_return co_await write_json(
                stream, version, keep_alive, 502,
                error_json(ErrorKind::UpstreamFailure, "upstream connection failed"));
        }
        if (sr.timed_out) {
            co_return co_await write_json(
                stream, version, keep_alive, 504,
                error_json(ErrorKind::UpstreamTimeout, "upstream timed out"));
        }
        // Connected, 2xx, but produced nothing: emit a minimal stream + [DONE].
        co_await writer.begin();
        header_started = true;
    }

    if (downstream_dead) {
        co_return false; // client gone; close
    }

    // Mid-stream upstream failure AFTER bytes were sent: terminate with an SSE
    // error event, then close — never silently truncate (locked spec).
    if (sr.upstream_aborted || sr.timed_out) {
        ErrorKind k = sr.timed_out ? ErrorKind::UpstreamTimeout : ErrorKind::UpstreamFailure;
        const char* msg =
            sr.timed_out ? "upstream timed out mid-stream" : "upstream aborted mid-stream";
        try {
            co_await writer.write_data(openai::make_error(k, msg).dump());
            co_await writer.end();
        } catch (...) {
            // client gone; nothing else to do
        }
        co_return false; // close after an error termination
    }

    // Clean end of stream ([DONE] already forwarded verbatim by the sink).
    try {
        co_await writer.end();
    } catch (...) {
        co_return false;
    }
    co_return keep_alive;
}

} // namespace kvmux::server
