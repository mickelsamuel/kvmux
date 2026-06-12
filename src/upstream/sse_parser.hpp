#pragma once

// Line-based Server-Sent-Events parser for the upstream side of the proxy.
//
// Per the SSE spec, an event is terminated by a blank line; fields are
// "name: value" lines, and `data:` fields accumulate (joined by '\n'). kvmux
// only needs the `data` payload of each event — it does not interpret `event:`,
// `id:`, or `retry:` for chat completions, but it tolerates them. The parser is
// incremental: feed arbitrary byte chunks as they arrive off the socket and it
// emits complete events, buffering any partial trailing event.
//
// The data payload is forwarded verbatim. For OpenAI chat completions that is a
// JSON object (chat.completion.chunk) or the literal sentinel `[DONE]`. The
// parser does not parse the JSON; llama.cpp's extra top-level `timings` field
// and vLLM's `continuous_usage_stats` therefore pass through untouched.

#include <functional>
#include <string>

namespace kvmux::upstream {

struct SseEvent {
    // Concatenated data payload (multiple `data:` lines joined with '\n', no
    // trailing newline). For chat completions this is the JSON chunk text or
    // the string "[DONE]".
    std::string data;

    // Optional `event:` field value (empty if absent). Not used by the chat
    // path but surfaced for completeness / future use.
    std::string event;

    // True when data == "[DONE]" (the OpenAI stream-termination sentinel).
    bool is_done() const noexcept { return data == "[DONE]"; }
};

class SseParser {
  public:
    using EventCallback = std::function<void(const SseEvent&)>;

    // Feed a chunk of bytes. Complete events are passed to `on_event` in order.
    // A partial event at the end of `chunk` is retained until completed by a
    // later feed() (or flush()).
    void feed(std::string_view chunk, const EventCallback& on_event);

    // Force-emit a final event if the buffer holds a complete-but-unterminated
    // event (some servers close the connection without a trailing blank line).
    // Returns true if an event was emitted.
    bool flush(const EventCallback& on_event);

    // Bytes currently buffered (incomplete tail). Mostly for tests/diagnostics.
    std::size_t buffered() const noexcept { return line_buf_.size() + cur_data_.size(); }

  private:
    void process_line(std::string_view line, const EventCallback& on_event);
    void dispatch(const EventCallback& on_event);

    std::string line_buf_;    // bytes since the last '\n'
    std::string cur_data_;    // accumulated data for the in-progress event
    std::string cur_event_;   // accumulated event-type field
    bool have_field_ = false; // whether the current event has any field yet
};

} // namespace kvmux::upstream
