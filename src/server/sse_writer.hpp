#pragma once

// Downstream SSE writer. Sends a chunked text/event-stream response and writes
// each `data: {...}\n\n` frame immediately with a per-frame flush so inter-token
// latency is preserved end-to-end (the product's ITL-fidelity requirement).

#include <boost/asio/awaitable.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/serializer.hpp>
#include <string>

namespace kvmux::server {

namespace beast = boost::beast;
namespace http = boost::beast::http;

// Owns the chunked SSE response serializer over a downstream tcp_stream. The
// caller writes the header once, then one frame per token, then ends the stream.
class SseWriter {
  public:
    SseWriter(beast::tcp_stream& stream, unsigned http_version, bool keep_alive);

    // Send the SSE response header (text/event-stream, no-cache, chunked).
    boost::asio::awaitable<void> begin();

    // Write one already-formed SSE frame payload as `data: <payload>\n\n` with a
    // per-frame flush. `payload` is the JSON chunk text or the literal [DONE].
    boost::asio::awaitable<void> write_data(const std::string& payload);

    // Finish the chunked stream (sends the terminating empty chunk).
    boost::asio::awaitable<void> end();

    bool header_sent() const noexcept { return header_sent_; }

  private:
    beast::tcp_stream& stream_;
    http::response<http::empty_body> res_;
    http::response_serializer<http::empty_body> serializer_;
    bool header_sent_ = false;
    bool ended_ = false;
};

} // namespace kvmux::server
