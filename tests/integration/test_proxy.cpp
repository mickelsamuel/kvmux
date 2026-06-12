// M2 integration tests for the streaming proxy spine.
//
// Hermetic: an in-process mock upstream (a tiny Beast server emitting canned
// SSE) stands in for vLLM/llama.cpp/Ollama, and the real Gateway proxies to it.
// A Beast client drives requests through the Gateway. No Python / network deps,
// so this runs in CI under ASan/TSan.
//
// Scenarios (from the plan's M2 acceptance):
//   * clean stream end-to-end, [DONE] forwarded
//   * usage chunk forwarded (and include_usage injected upstream)
//   * upstream 503 -> OpenAI-shaped error with the right HTTP status
//   * mid-stream abort -> SSE error event after the partial stream

#include "config.hpp"
#include "health/checker.hpp"
#include "server/gateway.hpp"
#include "server/listener.hpp"

#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace config = kvmux::config;
namespace server = kvmux::server;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;
using namespace std::chrono_literals;

namespace {

// ---- In-process mock upstream ---------------------------------------------
// Modes by `model` field: "clean", "usage", "http_503", "abort".
class MockUpstream {
  public:
    MockUpstream() : acceptor_(ioc_) {
        tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
        thread_ = std::thread([this] {
            asio::co_spawn(ioc_, accept_loop(), asio::detached);
            ioc_.run();
        });
    }
    ~MockUpstream() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }

  private:
    asio::awaitable<void> accept_loop() {
        for (;;) {
            auto [ec, sock] = co_await acceptor_.async_accept(asio::as_tuple(asio::use_awaitable));
            if (ec) {
                co_return;
            }
            asio::co_spawn(ioc_, serve(std::move(sock)), asio::detached);
        }
    }

    static std::string chunk(const std::string& payload) { return "data: " + payload + "\n\n"; }

    asio::awaitable<void> serve(tcp::socket sock) {
        beast::tcp_stream stream(std::move(sock));
        beast::flat_buffer buf;
        try {
            http::request<http::string_body> req;
            co_await http::async_read(stream, buf, req, asio::use_awaitable);
            json body = json::parse(req.body(), nullptr, false);
            std::string model = body.is_object() && body.contains("model")
                                    ? body["model"].get<std::string>()
                                    : "clean";
            bool include_usage = body.is_object() && body.contains("stream_options") &&
                                 body["stream_options"].is_object() &&
                                 body["stream_options"].value("include_usage", false);

            if (model == "http_503") {
                http::response<http::string_body> res{http::status::service_unavailable,
                                                      req.version()};
                res.set(http::field::content_type, "application/json");
                res.body() =
                    R"({"error":{"message":"backend is loading","type":"upstream_error","code":503}})";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }

            // Streaming 200 with chunked SSE.
            http::response<http::empty_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "text/event-stream");
            res.chunked(true);
            http::response_serializer<http::empty_body> sr{res};
            co_await http::async_write_header(stream, sr, asio::use_awaitable);

            auto send = [&](const std::string& s) -> asio::awaitable<void> {
                co_await asio::async_write(stream, http::make_chunk(asio::buffer(s)),
                                           asio::use_awaitable);
            };

            co_await send(chunk(
                R"({"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]})"));
            co_await send(chunk(
                R"({"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]})"));

            if (model == "abort") {
                // Drop the connection mid-stream (no finish, no [DONE]).
                beast::error_code ec;
                stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                co_return;
            }

            co_await send(chunk(
                R"({"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]})"));
            co_await send(chunk(
                R"({"object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})"));

            if (include_usage || model == "usage") {
                co_await send(chunk(
                    R"({"object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}})"));
            }
            co_await send("data: [DONE]\n\n");
            co_await asio::async_write(stream, http::make_chunk_last(), asio::use_awaitable);
            beast::error_code ec;
            stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        } catch (...) {
            // client gone / read error
        }
    }

    asio::io_context ioc_;
    tcp::acceptor acceptor_;
    unsigned short port_ = 0;
    std::thread thread_;
};

// ---- Gateway under test, on its own io_context thread ----------------------
class GatewayHarness {
  public:
    explicit GatewayHarness(unsigned short upstream_port) {
        config::Config cfg;
        cfg.server.listen = "127.0.0.1";
        cfg.server.port = 0; // ephemeral; we read it back after bind
        cfg.routing.policy = config::RoutingPolicy::RoundRobin;
        config::BackendConfig bc;
        bc.name = "mock";
        bc.type = config::BackendType::Vllm;
        bc.base_url = "http://127.0.0.1:" + std::to_string(upstream_port);
        bc.models = {"clean", "usage", "http_503", "abort"};
        bc.max_in_flight = 8;
        cfg.backends = {bc};

        gateway_ = std::make_unique<server::Gateway>(ioc_, cfg);
        // M3 makes health a routing precondition; this harness has no real
        // /health endpoint, so mark the single mock backend HEALTHY directly.
        gateway_->set_health_for_test(0, kvmux::health::HealthState::Healthy);

        // Bind our own acceptor so we can learn the ephemeral port, then run the
        // listener loop against it via the same session logic.
        acceptor_ = std::make_unique<tcp::acceptor>(ioc_);
        tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
        acceptor_->open(ep.protocol());
        acceptor_->set_option(asio::socket_base::reuse_address(true));
        acceptor_->bind(ep);
        acceptor_->listen();
        port_ = acceptor_->local_endpoint().port();

        asio::co_spawn(ioc_, run(), asio::detached);
        thread_ = std::thread([this] { ioc_.run(); });
    }
    ~GatewayHarness() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }

  private:
    asio::awaitable<void> run() {
        for (;;) {
            auto [ec, sock] = co_await acceptor_->async_accept(asio::as_tuple(asio::use_awaitable));
            if (ec) {
                co_return;
            }
            asio::co_spawn(ioc_, session(std::move(sock)), asio::detached);
        }
    }
    asio::awaitable<void> session(tcp::socket sock) {
        beast::tcp_stream stream(std::move(sock));
        beast::flat_buffer buf;
        try {
            http::request<http::string_body> req;
            co_await http::async_read(stream, buf, req, asio::use_awaitable);
            bool ka = req.keep_alive();
            stream.expires_never();
            co_await gateway_->handle(stream, std::move(req), ka);
        } catch (...) {
        }
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_send, ec);
    }

    asio::io_context ioc_;
    std::unique_ptr<server::Gateway> gateway_;
    std::unique_ptr<tcp::acceptor> acceptor_;
    unsigned short port_ = 0;
    std::thread thread_;
};

// ---- Blocking client helper ------------------------------------------------
struct Response {
    int status = 0;
    std::string body;
};

Response do_request(unsigned short port, const std::string& req_body) {
    asio::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(results);

    http::request<http::string_body> req{http::verb::post, "/v1/chat/completions", 11};
    req.set(http::field::host, "127.0.0.1");
    req.set(http::field::content_type, "application/json");
    req.body() = req_body;
    req.prepare_payload();
    http::write(stream, req);

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(boost::none);
    http::read(stream, buf, parser);

    Response r;
    r.status = static_cast<int>(parser.get().result_int());
    r.body = parser.get().body();
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    return r;
}

int count_substr(const std::string& hay, const std::string& needle) {
    int n = 0;
    for (std::size_t pos = 0; (pos = hay.find(needle, pos)) != std::string::npos;
         pos += needle.size()) {
        ++n;
    }
    return n;
}

} // namespace

TEST_CASE("integration: clean stream is proxied with [DONE]", "[integration]") {
    MockUpstream up;
    GatewayHarness gw(up.port());
    auto r = do_request(
        gw.port(),
        R"({"model":"clean","messages":[{"role":"user","content":"hi"}],"stream":true})");
    CHECK(r.status == 200);
    CHECK(r.body.find("\"content\":\"Hello\"") != std::string::npos);
    CHECK(r.body.find("\"content\":\" world\"") != std::string::npos);
    CHECK(r.body.find("data: [DONE]") != std::string::npos);
    CHECK(count_substr(r.body, "data: ") >= 4);
}

TEST_CASE("integration: usage chunk is forwarded and include_usage injected", "[integration]") {
    MockUpstream up;
    GatewayHarness gw(up.port());
    // Note: the request does NOT ask for usage; the gateway must inject
    // stream_options.include_usage upstream, so the mock emits the usage chunk.
    auto r = do_request(
        gw.port(),
        R"({"model":"clean","messages":[{"role":"user","content":"hi"}],"stream":true})");
    CHECK(r.status == 200);
    CHECK(r.body.find("\"usage\"") != std::string::npos);
    CHECK(r.body.find("\"total_tokens\":12") != std::string::npos);
    CHECK(r.body.find("data: [DONE]") != std::string::npos);
}

TEST_CASE("integration: upstream 503 becomes an OpenAI-shaped error", "[integration]") {
    MockUpstream up;
    GatewayHarness gw(up.port());
    auto r = do_request(
        gw.port(),
        R"({"model":"http_503","messages":[{"role":"user","content":"hi"}],"stream":true})");
    CHECK(r.status == 503);
    json j = json::parse(r.body, nullptr, false);
    REQUIRE_FALSE(j.is_discarded());
    REQUIRE(j.contains("error"));
    CHECK(j["error"]["code"] == 503);
    CHECK(j["error"].contains("message"));
}

TEST_CASE("integration: mid-stream abort yields a terminal SSE error event", "[integration]") {
    MockUpstream up;
    GatewayHarness gw(up.port());
    auto r = do_request(
        gw.port(),
        R"({"model":"abort","messages":[{"role":"user","content":"hi"}],"stream":true})");
    CHECK(r.status == 200); // header already sent before the abort
    CHECK(r.body.find("\"content\":\"Hello\"") != std::string::npos); // partial stream delivered
    // Terminal SSE error event, never a silent truncation.
    CHECK(r.body.find("\"error\"") != std::string::npos);
    CHECK(r.body.find("upstream aborted mid-stream") != std::string::npos);
    // And no [DONE] (the stream did not complete cleanly).
    CHECK(r.body.find("[DONE]") == std::string::npos);
}

TEST_CASE("integration: non-streaming request is proxied", "[integration]") {
    MockUpstream up;
    GatewayHarness gw(up.port());
    // The mock always streams; for a non-stream request the gateway uses the
    // unary path. The mock returns chunked SSE even for stream:false, so this
    // exercises the unary read of a chunked body. We assert the gateway relays
    // a 200 with a body.
    auto r = do_request(
        gw.port(),
        R"({"model":"clean","messages":[{"role":"user","content":"hi"}],"stream":false})");
    CHECK(r.status == 200);
    CHECK_FALSE(r.body.empty());
}
