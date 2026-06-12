// M3 integration tests: multi-backend health, circuit breaker, failover, and
// admission control. Hermetic — in-process Beast mock upstreams + the real
// Gateway, driven over real sockets. Runs in CI under ASan/TSan.
//
// Acceptance scenarios (plan M3):
//   * kill a backend mid-run -> breaker opens -> traffic shifts to the survivor
//     (zero-byte retry-once works)
//   * 503-while-loading llama.cpp is treated NOT_READY (so it is not eligible,
//     but is not DEAD) — verified via the health evaluator + gateway eligibility
//   * Ollama liveness via GET / banner (health monitor against a mock Ollama)
//   * admission queue overflow -> 429 with Retry-After

#include "config.hpp"
#include "health/checker.hpp"
#include "server/gateway.hpp"
#include "upstream/client.hpp"

#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace config = kvmux::config;
namespace server = kvmux::server;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;
using namespace std::chrono_literals;

namespace {

// A controllable mock upstream. Behavior is switched live via atomics so a test
// can "kill" a backend mid-run. Serves /v1/chat/completions (SSE) and /health.
class MockBackend {
  public:
    enum class Mode { Ok, ConnReset, Http503Stream, Health503, HealthDead, Hang };

    MockBackend() : acceptor_(ioc_) {
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
    ~MockBackend() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }
    void set_mode(Mode m) { mode_.store(static_cast<int>(m)); }
    Mode mode() const { return static_cast<Mode>(mode_.load()); }
    int served_chats() const { return served_.load(); }
    int open_chats() const { return open_.load(); }
    // Release any chats parked in Hang mode (lets the held slot free up).
    void release_hang() { release_.store(true); }

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
            const std::string target = std::string(req.target());
            const Mode m = mode();

            if (target == "/health") {
                int status = 200;
                if (m == Mode::Health503) {
                    status = 503;
                } else if (m == Mode::HealthDead) {
                    // Simulate dead by refusing: 500.
                    status = 500;
                }
                http::response<http::string_body> res{static_cast<http::status>(status),
                                                      req.version()};
                res.set(http::field::content_type, "application/json");
                res.body() = status == 200 ? R"({"status":"ok"})" : R"({"error":"loading"})";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }

            // Ollama liveness/readiness endpoints (no /health).
            if (target == "/") {
                http::response<http::string_body> res{http::status::ok, req.version()};
                res.body() = "Ollama is running";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }
            if (target == "/api/tags") {
                int status = (m == Mode::Health503) ? 503 : 200;
                http::response<http::string_body> res{static_cast<http::status>(status),
                                                      req.version()};
                res.set(http::field::content_type, "application/json");
                res.body() = R"({"models":[]})";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }

            // Chat completions.
            if (m == Mode::ConnReset) {
                // Kill the connection before any response byte: a zero-byte
                // upstream failure that must trigger failover.
                beast::error_code ec;
                stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                co_return;
            }
            if (m == Mode::Http503Stream) {
                http::response<http::string_body> res{http::status::service_unavailable,
                                                      req.version()};
                res.set(http::field::content_type, "application/json");
                res.body() = R"({"error":{"message":"overloaded","type":"upstream_error","code":503}})";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }

            served_.fetch_add(1);
            open_.fetch_add(1);
            struct OpenGuard {
                std::atomic<int>& c;
                ~OpenGuard() { c.fetch_sub(1); }
            } open_guard{open_};
            json body = json::parse(req.body(), nullptr, false);
            std::string model = body.is_object() && body.contains("model")
                                    ? body["model"].get<std::string>()
                                    : "m";

            http::response<http::empty_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "text/event-stream");
            res.chunked(true);
            http::response_serializer<http::empty_body> sr{res};
            co_await http::async_write_header(stream, sr, asio::use_awaitable);
            auto send = [&](const std::string& s) -> asio::awaitable<void> {
                co_await asio::async_write(stream, http::make_chunk(asio::buffer(s)),
                                           asio::use_awaitable);
            };

            if (m == Mode::Hang) {
                // Send the role chunk (so bytes ARE sent downstream), then hold
                // the stream open until released. This deterministically keeps
                // the gateway's admission slot occupied.
                co_await send(chunk(
                    R"({"choices":[{"index":0,"delta":{"role":"assistant","content":"hang"},"finish_reason":null}]})"));
                asio::steady_timer t(co_await asio::this_coro::executor);
                for (int i = 0; i < 2000 && !release_.load(); ++i) {
                    t.expires_after(5ms);
                    co_await t.async_wait(asio::use_awaitable);
                }
                co_await send(chunk(R"({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})"));
                co_await send("data: [DONE]\n\n");
                co_await asio::async_write(stream, http::make_chunk_last(), asio::use_awaitable);
                beast::error_code ec;
                stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                co_return;
            }
            // Tag the content with our port so the test can tell which backend
            // actually served the request.
            const std::string tag = "from:" + std::to_string(port_);
            co_await send(chunk(R"({"choices":[{"index":0,"delta":{"role":"assistant","content":")" +
                                tag + R"("},"finish_reason":null}]})"));
            co_await send(chunk(R"({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})"));
            co_await send("data: [DONE]\n\n");
            co_await asio::async_write(stream, http::make_chunk_last(), asio::use_awaitable);
            beast::error_code ec;
            stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        } catch (...) {
        }
    }

    asio::io_context ioc_;
    tcp::acceptor acceptor_;
    unsigned short port_ = 0;
    std::thread thread_;
    std::atomic<int> mode_{static_cast<int>(Mode::Ok)};
    std::atomic<int> served_{0};
    std::atomic<int> open_{0};
    std::atomic<bool> release_{false};
};

// Multi-backend gateway harness. All backends serve the same model "m".
class Harness {
  public:
    explicit Harness(const std::vector<unsigned short>& upstream_ports, int max_concurrent = 256,
                     int queue_depth = 64, int queue_timeout_ms = 5000, int failure_threshold = 5) {
        config::Config cfg;
        cfg.server.listen = "127.0.0.1";
        cfg.server.port = 0;
        cfg.server.max_concurrent_streams = max_concurrent;
        cfg.server.queue_depth = queue_depth;
        cfg.server.queue_timeout_ms = queue_timeout_ms;
        cfg.routing.policy = config::RoutingPolicy::RoundRobin;
        for (std::size_t i = 0; i < upstream_ports.size(); ++i) {
            config::BackendConfig bc;
            bc.name = "b" + std::to_string(i);
            bc.type = config::BackendType::Vllm;
            bc.base_url = "http://127.0.0.1:" + std::to_string(upstream_ports[i]);
            bc.models = {"m"};
            bc.max_in_flight = 64;
            bc.failure_threshold = failure_threshold;
            bc.open_ms = 10000;
            cfg.backends.push_back(bc);
        }
        gateway_ = std::make_unique<server::Gateway>(ioc_, cfg);
        n_ = upstream_ports.size();

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
    ~Harness() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }
    server::Gateway& gateway() { return *gateway_; }

    // Mark every backend HEALTHY (these tests don't run the live monitor).
    void mark_all_healthy() {
        for (std::size_t i = 0; i < n_; ++i) {
            gateway_->set_health_for_test(i, kvmux::health::HealthState::Healthy);
        }
    }
    void set_health(std::size_t i, kvmux::health::HealthState s) {
        gateway_->set_health_for_test(i, s);
    }

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
    std::size_t n_ = 0;
    std::thread thread_;
};

struct Response {
    int status = 0;
    std::string body;
    std::string retry_after;
};

Response do_chat(unsigned short port, const std::string& body) {
    asio::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(results);
    http::request<http::string_body> req{http::verb::post, "/v1/chat/completions", 11};
    req.set(http::field::host, "127.0.0.1");
    req.set(http::field::content_type, "application/json");
    req.body() = body;
    req.prepare_payload();
    http::write(stream, req);
    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(boost::none);
    http::read(stream, buf, parser);
    Response r;
    r.status = static_cast<int>(parser.get().result_int());
    r.body = parser.get().body();
    if (auto it = parser.get().find(http::field::retry_after); it != parser.get().end()) {
        r.retry_after = std::string(it->value());
    }
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    return r;
}

const char* kChat = R"({"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true})";

} // namespace

TEST_CASE("m3: failover — a dead backend's request retries on the survivor (zero-byte)",
          "[m3][failover]") {
    MockBackend a, b;
    Harness h({a.port(), b.port()});
    h.mark_all_healthy();
    // Kill backend a: every chat connection is reset before any byte.
    a.set_mode(MockBackend::Mode::ConnReset);

    // Many requests: round-robin will pick 'a' for ~half; each must fail over to
    // 'b' with zero bytes sent, so every request still succeeds via 'b'.
    int ok = 0;
    for (int i = 0; i < 8; ++i) {
        auto r = do_chat(h.port(), kChat);
        if (r.status == 200 && r.body.find("[DONE]") != std::string::npos &&
            r.body.find("from:" + std::to_string(b.port())) != std::string::npos) {
            ++ok;
        }
    }
    CHECK(ok == 8);          // all requests served by the survivor
    CHECK(b.served_chats() == 8);
}

TEST_CASE("m3: circuit breaker opens after repeated failures and stops routing to the backend",
          "[m3][breaker]") {
    MockBackend a, b;
    Harness h({a.port(), b.port()}, 256, 64, 5000, /*failure_threshold=*/5);
    h.mark_all_healthy();
    a.set_mode(MockBackend::Mode::ConnReset); // a always fails

    // Drive enough requests that round-robin sends >=5 to 'a', opening its
    // breaker. After it opens, 'a' is skipped entirely (no connection attempts).
    for (int i = 0; i < 20; ++i) {
        auto r = do_chat(h.port(), kChat);
        CHECK(r.status == 200); // always served by 'b'
    }
    CHECK(h.gateway().breaker_of(0) == kvmux::health::BreakerState::Open);
    CHECK(b.served_chats() == 20);
}

TEST_CASE("m3: NOT_READY backend is not eligible but a healthy one serves", "[m3][health]") {
    MockBackend a, b;
    Harness h({a.port(), b.port()});
    // a is loading (NOT_READY), b is healthy.
    h.set_health(0, kvmux::health::HealthState::NotReady);
    h.set_health(1, kvmux::health::HealthState::Healthy);
    for (int i = 0; i < 6; ++i) {
        auto r = do_chat(h.port(), kChat);
        REQUIRE(r.status == 200);
        CHECK(r.body.find("from:" + std::to_string(b.port())) != std::string::npos);
    }
    CHECK(a.served_chats() == 0); // never routed to the NOT_READY backend
}

TEST_CASE("m3: no healthy backend yields 503", "[m3][health]") {
    MockBackend a;
    Harness h({a.port()});
    h.set_health(0, kvmux::health::HealthState::Dead);
    auto r = do_chat(h.port(), kChat);
    CHECK(r.status == 503);
    json j = json::parse(r.body, nullptr, false);
    REQUIRE_FALSE(j.is_discarded());
    CHECK(j["error"]["code"] == 503);
}

TEST_CASE("m3: live health monitor probes each backend type correctly", "[m3][health]") {
    // Drive HealthMonitor::probe_once against the mock for all three backend
    // quirk modules over a real socket.
    MockBackend mb;
    asio::io_context ioc;
    kvmux::upstream::Client client(ioc, kvmux::upstream::Target::parse(
                                            "http://127.0.0.1:" + std::to_string(mb.port())));

    auto probe = [&](config::BackendType type) -> kvmux::health::HealthState {
        kvmux::health::HealthMonitor mon(ioc, type, client, 2000ms);
        asio::co_spawn(ioc, mon.probe_once(), asio::detached);
        ioc.restart();
        ioc.run();
        return mon.state();
    };

    SECTION("vLLM /health 200 -> HEALTHY") {
        mb.set_mode(MockBackend::Mode::Ok);
        CHECK(probe(config::BackendType::Vllm) == kvmux::health::HealthState::Healthy);
    }
    SECTION("llama.cpp /health 503 -> NOT_READY") {
        mb.set_mode(MockBackend::Mode::Health503);
        CHECK(probe(config::BackendType::Llamacpp) == kvmux::health::HealthState::NotReady);
    }
    SECTION("llama.cpp /health 500 -> DEAD") {
        mb.set_mode(MockBackend::Mode::HealthDead);
        CHECK(probe(config::BackendType::Llamacpp) == kvmux::health::HealthState::Dead);
    }
    SECTION("Ollama GET / banner + /api/tags 200 -> HEALTHY") {
        mb.set_mode(MockBackend::Mode::Ok);
        CHECK(probe(config::BackendType::Ollama) == kvmux::health::HealthState::Healthy);
    }
    SECTION("Ollama live but /api/tags 503 -> NOT_READY") {
        mb.set_mode(MockBackend::Mode::Health503);
        CHECK(probe(config::BackendType::Ollama) == kvmux::health::HealthState::NotReady);
    }
}

TEST_CASE("m3: admission queue overflow returns 429 with Retry-After", "[m3][admission]") {
    // max_concurrent=1, queue_depth=0 -> while one request holds the only slot,
    // the next is rejected immediately with 429 + Retry-After: 1.
    MockBackend a;
    a.set_mode(MockBackend::Mode::Hang); // first request holds the slot open
    Harness h({a.port()}, /*max_concurrent=*/1, /*queue_depth=*/0, /*queue_timeout_ms=*/100);
    h.mark_all_healthy();

    // Occupy the single slot with a hung request on a background thread.
    std::thread holder([&] { do_chat(h.port(), kChat); });

    // Wait until the hung request is actually in flight at the backend.
    for (int i = 0; i < 200 && a.open_chats() == 0; ++i) {
        std::this_thread::sleep_for(5ms);
    }
    REQUIRE(a.open_chats() == 1);

    // Now the slot is taken; a new request must be rejected.
    auto r = do_chat(h.port(), kChat);
    CHECK(r.status == 429);
    CHECK(r.retry_after == "1");
    json j = json::parse(r.body, nullptr, false);
    REQUIRE_FALSE(j.is_discarded());
    CHECK(j["error"]["code"] == 429);

    // Release the hung request and let the holder finish.
    a.release_hang();
    holder.join();
}

TEST_CASE("m3: 100-concurrent-stream soak across 3 backends is clean (TSan target)",
          "[m3][soak]") {
    // The plan's M3 acceptance soak: 100 concurrent streams against 3 mock
    // backends through the gateway. Exercises admission, round-robin spread,
    // per-backend limiters, breakers, and the streaming path concurrently —
    // this is the test the TSan job must run green.
    MockBackend a, b, c;
    Harness h({a.port(), b.port(), c.port()}, /*max_concurrent=*/256, /*queue_depth=*/256,
              /*queue_timeout_ms=*/5000);
    h.mark_all_healthy();

    constexpr int kClients = 100;
    std::atomic<int> ok{0};
    std::vector<std::thread> ts;
    ts.reserve(kClients);
    for (int i = 0; i < kClients; ++i) {
        ts.emplace_back([&] {
            auto r = do_chat(h.port(), kChat);
            if (r.status == 200 && r.body.find("[DONE]") != std::string::npos) {
                ++ok;
            }
        });
    }
    for (auto& t : ts) {
        t.join();
    }
    CHECK(ok.load() == kClients);
    // All three backends should have taken a share (round-robin spread).
    CHECK(a.served_chats() > 0);
    CHECK(b.served_chats() > 0);
    CHECK(c.served_chats() > 0);
    CHECK(a.served_chats() + b.served_chats() + c.served_chats() == kClients);
}

TEST_CASE("m3: queued request is admitted once the slot frees (FIFO)", "[m3][admission]") {
    // max_concurrent=1, queue_depth=4, generous timeout: a request that arrives
    // while the slot is busy waits in the queue and is then served (not 429).
    MockBackend a;
    a.set_mode(MockBackend::Mode::Hang);
    Harness h({a.port()}, /*max_concurrent=*/1, /*queue_depth=*/4, /*queue_timeout_ms=*/5000);
    h.mark_all_healthy();

    std::thread holder([&] { do_chat(h.port(), kChat); });
    for (int i = 0; i < 200 && a.open_chats() == 0; ++i) {
        std::this_thread::sleep_for(5ms);
    }
    REQUIRE(a.open_chats() == 1);

    // This request queues; release the holder shortly so it gets served.
    std::atomic<int> queued_status{0};
    std::thread waiter([&] {
        auto r = do_chat(h.port(), kChat);
        queued_status = r.status;
    });
    std::this_thread::sleep_for(50ms);
    a.release_hang(); // free the slot -> the queued request proceeds
    waiter.join();
    holder.join();
    CHECK(queued_status.load() == 200); // served after queueing, not rejected
}
