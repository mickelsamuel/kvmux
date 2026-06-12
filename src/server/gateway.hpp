#pragma once

// Gateway: owns config + per-backend upstream clients and implements the request
// handling for the served endpoints. M2 spine: single-backend selection by model
// (first backend that serves the requested model), streaming + non-streaming
// chat proxy, OpenAI error mapping, /v1/models aggregation (static from config),
// /healthz, graceful-drain flag. Health checks, circuit breaker, real routing
// policies, and admission control arrive in M3/M4.

#include "config.hpp"
#include "upstream/client.hpp"

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/string_body.hpp>
#include <memory>
#include <string>
#include <vector>

namespace kvmux::server {

namespace beast = boost::beast;
namespace http = boost::beast::http;

class Gateway {
  public:
    Gateway(boost::asio::io_context& ioc, config::Config cfg);

    // Handle one parsed HTTP request on `stream`. Drives streaming responses
    // directly on the stream (SSE) or returns a buffered response by writing it.
    // `keep_alive` reflects the request's keep-alive intent. Returns true if the
    // connection may be reused (keep-alive honored), false if it should close.
    boost::asio::awaitable<bool> handle(beast::tcp_stream& stream,
                                        http::request<http::string_body> req, bool keep_alive);

    // Begin graceful drain: new requests get 503; in-flight streams finish.
    void begin_drain() noexcept { draining_.store(true); }
    bool draining() const noexcept { return draining_.load(); }

    // Number of in-flight requests (for drain coordination).
    int in_flight() const noexcept { return in_flight_.load(); }

    const config::Config& cfg() const noexcept { return cfg_; }

  private:
    struct Backend {
        config::BackendConfig conf;
        std::unique_ptr<upstream::Client> client;
    };

    // First configured backend that serves `model`, or nullptr.
    Backend* select_backend(const std::string& model);

    boost::asio::awaitable<bool> handle_chat(beast::tcp_stream& stream,
                                             const http::request<http::string_body>& req,
                                             bool keep_alive);
    boost::asio::awaitable<bool> handle_models(beast::tcp_stream& stream,
                                               const http::request<http::string_body>& req,
                                               bool keep_alive);

    // Write a buffered (non-SSE) response and return keep_alive.
    boost::asio::awaitable<bool> write_json(beast::tcp_stream& stream, unsigned version,
                                            bool keep_alive, int status,
                                            const std::string& json_body);

    boost::asio::io_context& ioc_;
    config::Config cfg_;
    std::vector<Backend> backends_;
    std::atomic<bool> draining_{false};
    std::atomic<int> in_flight_{0};
};

} // namespace kvmux::server
