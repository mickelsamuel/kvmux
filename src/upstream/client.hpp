#pragma once

// Asynchronous upstream HTTP client (Boost.Beast + Asio C++20 coroutines).
//
// M2 scope: a single-backend client that forwards a chat-completions request to
// one upstream, with a small per-backend keep-alive connection pool, TCP_NODELAY
// on the socket (so re-streamed SSE frames are not Nagle-coalesced), and a
// streaming callback invoked per upstream SSE frame for immediate re-streaming.
//
// The full per-backend quirk modules and multi-backend routing are M3; this
// client speaks the common OpenAI /v1 subset that all three backends share.

#include "upstream/sse_parser.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/beast/http/status.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>

namespace kvmux::upstream {

using json = nlohmann::json;

// Result of a streaming upstream call.
struct StreamResult {
    bool connected = false;           // TCP+HTTP exchange started
    bool any_frame_forwarded = false; // at least one SSE frame reached the sink
    int http_status = 0;              // upstream HTTP status (0 if never received)
    std::string error_body;           // upstream error body when http_status >= 400
    bool upstream_aborted = false;    // connection dropped mid-stream (no [DONE])
    bool timed_out = false;           // exceeded the request budget
};

// Result of a non-streaming upstream call.
struct UnaryResult {
    bool connected = false;
    int http_status = 0;
    std::string body; // full response body
    bool timed_out = false;
};

// Identifies one upstream target. Parsed from a base_url ("http://host:port").
struct Target {
    std::string host;
    std::string port; // string form for Asio resolver

    static Target parse(const std::string& base_url); // throws std::runtime_error
};

// One upstream backend client. Owns a tiny keep-alive connection pool to a
// single host:port. Thread-compatible with a single io_context strand per
// connection (M2 runs one io_context).
class Client {
  public:
    Client(boost::asio::io_context& ioc, Target target);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // Async callback invoked for each upstream SSE frame. It is co_awaited per
    // frame, so the downstream write completes (and flushes) before the next
    // upstream read — giving true per-frame flush with no buffering and no
    // concurrency. Resolve to false to request the call to stop forwarding
    // (e.g. the downstream client went away).
    using FrameSink = std::function<boost::asio::awaitable<bool>(const SseEvent&)>;

    // Stream POST /v1/chat/completions. `request_body` is the (already
    // stream_options-injected) JSON body. Forwards each upstream SSE frame to
    // `sink` as it arrives. `budget` bounds the whole call.
    boost::asio::awaitable<StreamResult> stream_chat(std::string request_body, FrameSink sink,
                                                     std::chrono::milliseconds budget);

    // Non-streaming POST /v1/chat/completions: returns the full body.
    boost::asio::awaitable<UnaryResult> post_chat(std::string request_body,
                                                  std::chrono::milliseconds budget);

    // GET a path (e.g. /health, /v1/models). Returns {status, body}.
    boost::asio::awaitable<UnaryResult> get(std::string path, std::chrono::milliseconds budget);

    const Target& target() const noexcept { return target_; }

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Target target_;
};

// Inject stream_options.include_usage=true into a chat request body (idempotent).
// Returns the modified JSON. Used so the gateway always receives the trailing
// usage chunk regardless of what the client requested.
json inject_include_usage(json body);

} // namespace kvmux::upstream
