#include "upstream/client.hpp"

#include <array>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <stdexcept>
#include <vector>

namespace kvmux::upstream {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;
using namespace std::chrono_literals;

Target Target::parse(const std::string& base_url) {
    // Minimal parser for "http://host:port" (the v1 config form). No TLS in v1.
    std::string s = base_url;
    const std::string scheme = "http://";
    if (s.rfind(scheme, 0) == 0) {
        s = s.substr(scheme.size());
    } else if (s.rfind("https://", 0) == 0) {
        throw std::runtime_error("https upstream not supported in v1: " + base_url);
    }
    // Strip any path.
    auto slash = s.find('/');
    if (slash != std::string::npos) {
        s = s.substr(0, slash);
    }
    Target t;
    auto colon = s.find(':');
    if (colon == std::string::npos) {
        t.host = s;
        t.port = "80";
    } else {
        t.host = s.substr(0, colon);
        t.port = s.substr(colon + 1);
    }
    if (t.host.empty() || t.port.empty()) {
        throw std::runtime_error("could not parse backend base_url: " + base_url);
    }
    return t;
}

struct Client::Impl {
    asio::io_context& ioc;
    Target target;

    explicit Impl(asio::io_context& io, Target t) : ioc(io), target(std::move(t)) {}

    // Resolve + connect a fresh stream with TCP_NODELAY. (M2 opens a connection
    // per request; the keep-alive pool is a small future refinement — see note.)
    asio::awaitable<beast::tcp_stream> connect(std::chrono::milliseconds budget) {
        auto executor = co_await asio::this_coro::executor;
        tcp::resolver resolver(executor);
        beast::tcp_stream stream(executor);
        stream.expires_after(budget);

        auto results =
            co_await resolver.async_resolve(target.host, target.port, asio::use_awaitable);
        co_await stream.async_connect(results, asio::use_awaitable);
        // TCP_NODELAY: do not let Nagle coalesce re-streamed SSE frames.
        stream.socket().set_option(tcp::no_delay(true));
        co_return stream;
    }
};

Client::Client(asio::io_context& ioc, Target target)
    : impl_(std::make_unique<Impl>(ioc, target)), target_(std::move(target)) {}

Client::~Client() = default;

json inject_include_usage(json body) {
    if (!body.is_object()) {
        return body;
    }
    if (!body.contains("stream_options") || !body["stream_options"].is_object()) {
        body["stream_options"] = json::object();
    }
    body["stream_options"]["include_usage"] = true;
    return body;
}

asio::awaitable<StreamResult> Client::stream_chat(std::string request_body, FrameSink sink,
                                                  std::chrono::milliseconds budget) {
    StreamResult result;
    try {
        beast::tcp_stream stream = co_await impl_->connect(budget);
        result.connected = true;

        http::request<http::string_body> req{http::verb::post, "/v1/chat/completions", 11};
        req.set(http::field::host, impl_->target.host);
        req.set(http::field::content_type, "application/json");
        req.set(http::field::accept, "text/event-stream");
        req.body() = std::move(request_body);
        req.prepare_payload();

        stream.expires_after(budget);
        co_await http::async_write(stream, req, asio::use_awaitable);

        // Read the response incrementally with a buffer_body parser so we can
        // forward each SSE frame the instant it arrives.
        beast::flat_buffer buffer;
        http::response_parser<http::buffer_body> parser;
        parser.body_limit(boost::none); // streaming response, no fixed limit

        co_await http::async_read_header(stream, buffer, parser, asio::use_awaitable);
        result.http_status = static_cast<int>(parser.get().result_int());

        if (result.http_status >= 400) {
            // Drain the (small) error body into error_body.
            std::string body;
            std::array<char, 4096> buf{};
            while (!parser.is_done()) {
                parser.get().body().data = buf.data();
                parser.get().body().size = buf.size();
                auto [ec, n] = co_await http::async_read_some(stream, buffer, parser,
                                                              asio::as_tuple(asio::use_awaitable));
                std::size_t consumed = buf.size() - parser.get().body().size;
                body.append(buf.data(), consumed);
                if (ec == http::error::need_buffer) {
                    continue;
                }
                if (ec) {
                    break;
                }
            }
            result.error_body = std::move(body);
            co_return result;
        }

        // 2xx: stream the body, feed bytes to the SSE parser, forward frames.
        // One read can yield several complete frames; collect them from the
        // (synchronous) parser callback, then co_await the sink for each in
        // order so every frame is flushed downstream before the next read.
        SseParser sse;
        bool sink_wants_stop = false;
        bool saw_done = false;
        std::array<char, 8192> buf{};
        std::vector<SseEvent> pending;

        auto drain_pending = [&]() -> asio::awaitable<void> {
            for (auto& ev : pending) {
                if (ev.is_done()) {
                    saw_done = true;
                }
                result.any_frame_forwarded = true;
                bool keep = co_await sink(ev);
                if (!keep) {
                    sink_wants_stop = true;
                    break;
                }
            }
            pending.clear();
        };

        while (!parser.is_done() && !sink_wants_stop) {
            parser.get().body().data = buf.data();
            parser.get().body().size = buf.size();
            stream.expires_after(budget);
            auto [ec, n] = co_await http::async_read_some(stream, buffer, parser,
                                                          asio::as_tuple(asio::use_awaitable));
            std::size_t consumed = buf.size() - parser.get().body().size;
            if (consumed > 0) {
                sse.feed(std::string_view(buf.data(), consumed),
                         [&](const SseEvent& ev) { pending.push_back(ev); });
                co_await drain_pending();
            }
            if (ec == http::error::need_buffer) {
                continue; // body buffer was full; loop to drain more
            }
            if (ec == asio::error::operation_aborted || ec == beast::error::timeout) {
                result.timed_out = true;
                break;
            }
            if (ec) {
                break; // connection error / EOF
            }
        }
        // Flush any trailing complete-but-unterminated frame.
        if (!sink_wants_stop) {
            sse.flush([&](const SseEvent& ev) { pending.push_back(ev); });
            co_await drain_pending();
        }

        // If the upstream closed without sending [DONE] and we weren't asked to
        // stop and didn't time out, treat it as an abort.
        if (!saw_done && !sink_wants_stop && !result.timed_out) {
            result.upstream_aborted = true;
        }

        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    } catch (const std::exception&) {
        if (!result.connected) {
            // connect/resolve failed
        } else if (result.http_status == 0) {
            result.upstream_aborted = true;
        }
    }
    co_return result;
}

asio::awaitable<UnaryResult> Client::post_chat(std::string request_body,
                                               std::chrono::milliseconds budget) {
    UnaryResult result;
    try {
        beast::tcp_stream stream = co_await impl_->connect(budget);
        result.connected = true;

        http::request<http::string_body> req{http::verb::post, "/v1/chat/completions", 11};
        req.set(http::field::host, impl_->target.host);
        req.set(http::field::content_type, "application/json");
        req.body() = std::move(request_body);
        req.prepare_payload();

        stream.expires_after(budget);
        co_await http::async_write(stream, req, asio::use_awaitable);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        co_await http::async_read(stream, buffer, res, asio::use_awaitable);
        result.http_status = static_cast<int>(res.result_int());
        result.body = std::move(res.body());

        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    } catch (const std::exception&) {
        // leave result.connected / http_status reflecting how far we got
    }
    co_return result;
}

asio::awaitable<UnaryResult> Client::get(std::string path, std::chrono::milliseconds budget) {
    UnaryResult result;
    try {
        beast::tcp_stream stream = co_await impl_->connect(budget);
        result.connected = true;

        http::request<http::string_body> req{http::verb::get, path, 11};
        req.set(http::field::host, impl_->target.host);
        req.prepare_payload();

        stream.expires_after(budget);
        co_await http::async_write(stream, req, asio::use_awaitable);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        co_await http::async_read(stream, buffer, res, asio::use_awaitable);
        result.http_status = static_cast<int>(res.result_int());
        result.body = std::move(res.body());

        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    } catch (const std::exception&) {
        // partial result is fine
    }
    co_return result;
}

} // namespace kvmux::upstream
