// kvmux — single-binary OpenAI-compatible LLM gateway.
//
// M0 skeleton: a Boost.Beast + Boost.Asio C++20-coroutine HTTP server that
// responds on a couple of routes, proving the toolchain (GCC 13 / Clang 18,
// Boost >= 1.83, C++20 coroutines) end-to-end. The streaming proxy spine lands
// in M2 and replaces this handler.

#include "config.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <cstdlib>
#include <iostream>
#include <string>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;

namespace {

constexpr const char* kVersion = "0.1.0";

http::response<http::string_body> make_response(const http::request<http::string_body>& req) {
    auto build = [&](http::status status, const std::string& ctype, std::string body) {
        http::response<http::string_body> res{status, req.version()};
        res.set(http::field::server, std::string("kvmux/") + kVersion);
        res.set(http::field::content_type, ctype);
        res.keep_alive(req.keep_alive());
        res.body() = std::move(body);
        res.prepare_payload();
        return res;
    };

    if (req.target() == "/healthz") {
        return build(http::status::ok, "application/json", R"({"status":"ok"})");
    }
    if (req.target() == "/") {
        return build(http::status::ok, "text/plain",
                     std::string("kvmux ") + kVersion + " — building in public\n");
    }
    return build(http::status::not_found, "application/json",
                 R"({"error":{"message":"not found","type":"not_found_error","code":404}})");
}

asio::awaitable<void> handle_session(tcp::socket socket) {
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;
    try {
        for (;;) {
            stream.expires_after(std::chrono::seconds(30));
            http::request<http::string_body> req;
            co_await http::async_read(stream, buffer, req, asio::use_awaitable);

            auto res = make_response(req);
            bool keep_alive = res.keep_alive();
            co_await http::async_write(stream, res, asio::use_awaitable);
            if (!keep_alive) {
                break;
            }
        }
    } catch (const std::exception&) {
        // Connection closed / read error — end the session quietly.
    }
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_send, ec);
}

asio::awaitable<void> listen(tcp::endpoint endpoint) {
    auto executor = co_await asio::this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);
    std::cout << "kvmux " << kVersion << " listening on " << endpoint << std::endl;
    for (;;) {
        auto socket = co_await acceptor.async_accept(asio::use_awaitable);
        asio::co_spawn(executor, handle_session(std::move(socket)), asio::detached);
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
            config_path = argv[++i];
        } else if (arg == "--version") {
            std::cout << "kvmux " << kVersion << "\n";
            return 0;
        }
    }

    kvmux::config::ServerConfig server;
    if (!config_path.empty()) {
        try {
            auto cfg = kvmux::config::load_file(config_path);
            server = cfg.server;
            std::cout << "loaded config: " << config_path << " (" << cfg.backends.size()
                      << " backend(s), policy=" << kvmux::config::to_string(cfg.routing.policy)
                      << ")" << std::endl;
        } catch (const kvmux::config::ConfigError& e) {
            std::cerr << "config error: " << e.what() << std::endl;
            return 2;
        }
    }

    try {
        asio::io_context ioc(1);
        asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](const boost::system::error_code&, int) { ioc.stop(); });

        auto address = asio::ip::make_address(server.listen);
        asio::co_spawn(ioc,
                       listen(tcp::endpoint(address, static_cast<unsigned short>(server.port))),
                       [](std::exception_ptr e) {
                           if (e) {
                               try {
                                   std::rethrow_exception(e);
                               } catch (const std::exception& ex) {
                                   std::cerr << "listener error: " << ex.what() << std::endl;
                               }
                           }
                       });

        ioc.run();
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
