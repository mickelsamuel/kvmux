#include "server/listener.hpp"

#include "server/gateway.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <exception>
#include <iostream>

namespace kvmux::server {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;
using namespace std::chrono_literals;

namespace {

// One connection: keep-alive request loop. Reads a request, dispatches to the
// gateway, repeats while keep-alive holds and the connection is healthy.
asio::awaitable<void> run_session(tcp::socket socket, Gateway& gateway) {
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;
    try {
        bool keep_alive = true;
        while (keep_alive) {
            // Header/read idle timeout. A streaming response sets its own write
            // budget; this guards an idle keep-alive connection.
            stream.expires_after(120s);

            http::request<http::string_body> req;
            co_await http::async_read(stream, buffer, req, asio::use_awaitable);

            bool req_keep_alive = req.keep_alive();
            // Streaming writes can take a long time; disable the stream's own
            // expiry while the gateway drives the response (the upstream client
            // applies the per-request budget instead).
            stream.expires_never();

            keep_alive = co_await gateway.handle(stream, std::move(req), req_keep_alive);
        }
    } catch (const std::exception&) {
        // Read/write error or client closed — end the session quietly.
    }
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_send, ec);
}

} // namespace

asio::awaitable<void> run_listener(tcp::endpoint endpoint, Gateway& gateway) {
    auto executor = co_await asio::this_coro::executor;
    tcp::acceptor acceptor(executor);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(asio::socket_base::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen(asio::socket_base::max_listen_connections);

    std::cout << "kvmux listening on " << endpoint << std::endl;

    for (;;) {
        auto [ec, socket] = co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));
        if (ec) {
            // Acceptor closed (graceful shutdown) or a transient accept error.
            if (ec == asio::error::operation_aborted) {
                break;
            }
            continue;
        }
        asio::co_spawn(executor, run_session(std::move(socket), gateway), asio::detached);
    }
}

namespace {

// One metrics connection: answer a single GET /metrics (or 404), then close. The
// metrics endpoint is low-rate; no keep-alive loop needed.
asio::awaitable<void> run_metrics_session(tcp::socket socket, Gateway& gateway) {
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;
    try {
        stream.expires_after(10s);
        http::request<http::string_body> req;
        co_await http::async_read(stream, buffer, req, asio::use_awaitable);

        http::response<http::string_body> res;
        res.version(req.version());
        res.set(http::field::server, "kvmux");
        if (req.method() == http::verb::get && req.target() == "/metrics") {
            res.result(http::status::ok);
            // Prometheus text exposition format, version 0.0.4.
            res.set(http::field::content_type, "text/plain; version=0.0.4; charset=utf-8");
            res.body() = gateway.render_metrics();
        } else {
            res.result(http::status::not_found);
            res.set(http::field::content_type, "application/json");
            res.body() = R"({"error":{"message":"not found","type":"not_found_error","code":404}})";
        }
        res.keep_alive(false);
        res.prepare_payload();
        co_await http::async_write(stream, res, asio::use_awaitable);
    } catch (const std::exception&) {
        // read/write error or client closed — end quietly
    }
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_send, ec);
}

} // namespace

asio::awaitable<void> run_metrics_listener(tcp::endpoint endpoint, Gateway& gateway) {
    auto executor = co_await asio::this_coro::executor;
    tcp::acceptor acceptor(executor);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(asio::socket_base::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen(asio::socket_base::max_listen_connections);

    std::cout << "kvmux metrics on " << endpoint << "/metrics" << std::endl;

    for (;;) {
        auto [ec, socket] = co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));
        if (ec) {
            if (ec == asio::error::operation_aborted) {
                break;
            }
            continue;
        }
        asio::co_spawn(executor, run_metrics_session(std::move(socket), gateway), asio::detached);
    }
}

} // namespace kvmux::server
