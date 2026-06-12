#include "server/sse_writer.hpp"

#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/http/chunk_encode.hpp>
#include <boost/beast/http/write.hpp>

namespace kvmux::server {

namespace asio = boost::asio;

SseWriter::SseWriter(beast::tcp_stream& stream, unsigned http_version, bool keep_alive)
    : stream_(stream), res_{http::status::ok, http_version}, serializer_{res_} {
    res_.set(http::field::content_type, "text/event-stream");
    res_.set(http::field::cache_control, "no-cache");
    res_.set(http::field::connection, keep_alive ? "keep-alive" : "close");
    res_.set(http::field::server, "kvmux");
    // Chunked transfer-encoding: no Content-Length, stream frames as they come.
    res_.chunked(true);
    res_.keep_alive(keep_alive);
}

asio::awaitable<void> SseWriter::begin() {
    if (header_sent_) {
        co_return;
    }
    // Write just the header; the body chunks follow via write_data().
    co_await http::async_write_header(stream_, serializer_, asio::use_awaitable);
    header_sent_ = true;
}

asio::awaitable<void> SseWriter::write_data(const std::string& payload) {
    std::string frame;
    frame.reserve(payload.size() + 8);
    frame += "data: ";
    frame += payload;
    frame += "\n\n";

    // Emit one HTTP chunk carrying exactly this frame, then flush the socket so
    // the bytes leave immediately (TCP_NODELAY is set on the downstream socket
    // by the session). chunk() writes the size-prefixed chunk; the flush is the
    // socket write that async_write performs.
    co_await asio::async_write(stream_, http::make_chunk(asio::buffer(frame)), asio::use_awaitable);
}

asio::awaitable<void> SseWriter::end() {
    if (ended_) {
        co_return;
    }
    co_await asio::async_write(stream_, http::make_chunk_last(), asio::use_awaitable);
    ended_ = true;
}

} // namespace kvmux::server
