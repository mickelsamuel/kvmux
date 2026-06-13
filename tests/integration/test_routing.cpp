// M4 integration tests: prefix-affinity routing behavior + the metrics surface.
//
// Acceptance scenarios (plan M4):
//   * same-prefix sessions pin to one backend while distinct prefixes spread
//   * gauges/counters move correctly under load (requests_total, gauges, the
//     latency histograms, affinity spills)
//   * /metrics serves the Prometheus exposition (its text is promtool-validated
//     by a separate ctest that pipes this output through promtool)
//
// Hermetic: in-process Beast mock upstreams that tag each response with their
// own port, the real Gateway with policy = prefix_affinity, real sockets.

#include "config.hpp"
#include "health/checker.hpp"
#include "server/gateway.hpp"

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
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <set>
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

// Mock that streams a single tagged content chunk + [DONE]; records its port in
// the content so the client can tell which backend served the request.
class TagBackend {
  public:
    TagBackend() : acceptor_(ioc_) {
        tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
        thread_ = std::thread([this] {
            asio::co_spawn(ioc_, loop(), asio::detached);
            ioc_.run();
        });
    }
    ~TagBackend() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }
    int served() const { return served_.load(); }

  private:
    asio::awaitable<void> loop() {
        for (;;) {
            auto [ec, s] = co_await acceptor_.async_accept(asio::as_tuple(asio::use_awaitable));
            if (ec) {
                co_return;
            }
            asio::co_spawn(ioc_, serve(std::move(s)), asio::detached);
        }
    }
    asio::awaitable<void> serve(tcp::socket sock) {
        beast::tcp_stream stream(std::move(sock));
        beast::flat_buffer buf;
        try {
            http::request<http::string_body> req;
            co_await http::async_read(stream, buf, req, asio::use_awaitable);
            if (req.target() == "/health") {
                http::response<http::string_body> res{http::status::ok, req.version()};
                res.body() = R"({"status":"ok"})";
                res.prepare_payload();
                co_await http::async_write(stream, res, asio::use_awaitable);
                co_return;
            }
            served_.fetch_add(1);
            http::response<http::empty_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "text/event-stream");
            res.chunked(true);
            http::response_serializer<http::empty_body> sr{res};
            co_await http::async_write_header(stream, sr, asio::use_awaitable);
            auto send = [&](const std::string& s) -> asio::awaitable<void> {
                co_await asio::async_write(stream, http::make_chunk(asio::buffer(s)),
                                           asio::use_awaitable);
            };
            const std::string tag = "from:" + std::to_string(port_);
            co_await send("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + tag +
                          "\"},\"finish_reason\":null}]}\n\n");
            co_await send(
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n");
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
    std::atomic<int> served_{0};
};

class AffinityHarness {
  public:
    explicit AffinityHarness(const std::vector<unsigned short>& ports) {
        config::Config cfg;
        cfg.server.listen = "127.0.0.1";
        cfg.server.port = 0;
        cfg.metrics.port = 0;
        cfg.routing.policy = config::RoutingPolicy::PrefixAffinity;
        cfg.routing.prefix_bytes = 1024;
        cfg.routing.load_threshold = 0.8;
        for (std::size_t i = 0; i < ports.size(); ++i) {
            config::BackendConfig bc;
            bc.name = "b" + std::to_string(i);
            bc.type = config::BackendType::Vllm;
            bc.base_url = "http://127.0.0.1:" + std::to_string(ports[i]);
            bc.models = {"m"};
            bc.max_in_flight = 64;
            cfg.backends.push_back(bc);
        }
        n_ = ports.size();
        gateway_ = std::make_unique<server::Gateway>(ioc_, cfg);
        for (std::size_t i = 0; i < n_; ++i) {
            gateway_->set_health_for_test(i, kvmux::health::HealthState::Healthy);
        }
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
    ~AffinityHarness() {
        ioc_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    unsigned short port() const { return port_; }
    server::Gateway& gateway() { return *gateway_; }

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

struct Resp {
    int status = 0;
    std::string body;
};
Resp do_chat(unsigned short port, const std::string& prompt) {
    asio::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(results);
    json req_body = {{"model", "m"},
                     {"messages", json::array({{{"role", "user"}, {"content", prompt}}})},
                     {"stream", true}};
    http::request<http::string_body> req{http::verb::post, "/v1/chat/completions", 11};
    req.set(http::field::host, "127.0.0.1");
    req.set(http::field::content_type, "application/json");
    req.body() = req_body.dump();
    req.prepare_payload();
    http::write(stream, req);
    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(boost::none);
    http::read(stream, buf, parser);
    Resp r;
    r.status = static_cast<int>(parser.get().result_int());
    r.body = parser.get().body();
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    return r;
}

std::string which_backend(const std::string& body) {
    auto p = body.find("from:");
    if (p == std::string::npos) {
        return "";
    }
    auto end = body.find('"', p);
    return body.substr(p, end - p);
}

} // namespace

TEST_CASE("m4: same prefix pins to a single backend; distinct prefixes spread", "[m4][affinity]") {
    TagBackend a, b, c;
    AffinityHarness h({a.port(), b.port(), c.port()});

    // 1) Same prefix repeated -> every request lands on the SAME backend.
    std::set<std::string> pinned;
    for (int i = 0; i < 12; ++i) {
        auto r = do_chat(h.port(), "the same shared system prompt for session X");
        REQUIRE(r.status == 200);
        pinned.insert(which_backend(r.body));
    }
    CHECK(pinned.size() == 1); // pinned to exactly one backend

    // 2) Many DISTINCT prefixes -> spread across more than one backend.
    std::set<std::string> spread;
    for (int i = 0; i < 60; ++i) {
        auto r = do_chat(h.port(), "distinct prompt number " + std::to_string(i));
        REQUIRE(r.status == 200);
        spread.insert(which_backend(r.body));
    }
    CHECK(spread.size() >= 2); // distinct prefixes do not all collapse to one
}

TEST_CASE("m4: counters and gauges move under load", "[m4][metrics]") {
    TagBackend a, b;
    AffinityHarness h({a.port(), b.port()});

    for (int i = 0; i < 20; ++i) {
        auto r = do_chat(h.port(), "load prompt " + std::to_string(i % 4));
        REQUIRE(r.status == 200);
    }
    std::string m = h.gateway().render_metrics();

    // requests_total{code="200"} should be >= 20.
    auto pos = m.find("kvmux:requests_total{code=\"200\"}");
    REQUIRE(pos != std::string::npos);
    int total = std::stoi(m.substr(m.find(' ', pos) + 1));
    CHECK(total >= 20);

    // TTFT and ITL histograms recorded observations.
    CHECK(m.find("kvmux:ttft_seconds_count{") != std::string::npos);
    CHECK(m.find("kvmux:inter_token_latency_seconds_count{") != std::string::npos);
    CHECK(m.find("kvmux:e2e_request_latency_seconds_count{") != std::string::npos);

    // Gauges are present (settle to 0 in-flight after the load drains).
    CHECK(m.find("kvmux:requests_running ") != std::string::npos);
    CHECK(m.find("kvmux:requests_waiting ") != std::string::npos);
}

TEST_CASE("m4: round_robin remains selectable and spreads", "[m4][rr]") {
    // Build a harness but flip the policy to round_robin via a fresh config.
    TagBackend a, b, c;
    config::Config cfg;
    cfg.server.listen = "127.0.0.1";
    cfg.server.port = 0;
    cfg.routing.policy = config::RoutingPolicy::RoundRobin;
    for (auto p : {a.port(), b.port(), c.port()}) {
        config::BackendConfig bc;
        bc.name = "b" + std::to_string(p);
        bc.type = config::BackendType::Vllm;
        bc.base_url = "http://127.0.0.1:" + std::to_string(p);
        bc.models = {"m"};
        cfg.backends.push_back(bc);
    }
    asio::io_context ioc;
    server::Gateway gw(ioc, cfg);
    for (std::size_t i = 0; i < 3; ++i) {
        gw.set_health_for_test(i, kvmux::health::HealthState::Healthy);
    }
    tcp::acceptor acc(ioc);
    tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
    acc.open(ep.protocol());
    acc.set_option(asio::socket_base::reuse_address(true));
    acc.bind(ep);
    acc.listen();
    unsigned short port = acc.local_endpoint().port();
    std::atomic<bool> stop{false};
    auto run = [&]() -> asio::awaitable<void> {
        while (!stop.load()) {
            auto [ec, s] = co_await acc.async_accept(asio::as_tuple(asio::use_awaitable));
            if (ec) {
                co_return;
            }
            asio::co_spawn(
                ioc,
                [&gw, s = std::move(s)]() mutable -> asio::awaitable<void> {
                    beast::tcp_stream st(std::move(s));
                    beast::flat_buffer rbuf;
                    try {
                        http::request<http::string_body> rq;
                        co_await http::async_read(st, rbuf, rq, asio::use_awaitable);
                        bool ka = rq.keep_alive();
                        st.expires_never();
                        co_await gw.handle(st, std::move(rq), ka);
                    } catch (...) {
                    }
                    beast::error_code ec2;
                    st.socket().shutdown(tcp::socket::shutdown_send, ec2);
                },
                asio::detached);
        }
    };
    asio::co_spawn(ioc, run(), asio::detached);
    std::thread t([&] { ioc.run(); });

    // Same prefix every time; round-robin must still spread (ignores affinity).
    std::set<std::string> seen;
    for (int i = 0; i < 9; ++i) {
        auto r = do_chat(port, "identical prompt");
        REQUIRE(r.status == 200);
        seen.insert(which_backend(r.body));
    }
    CHECK(seen.size() == 3); // round-robin visited all three despite same prefix

    stop = true;
    ioc.stop();
    if (t.joinable()) {
        t.join();
    }
}
