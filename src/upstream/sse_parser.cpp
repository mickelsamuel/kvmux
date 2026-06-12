#include "upstream/sse_parser.hpp"

namespace kvmux::upstream {

namespace {

// Strip a single leading space after the colon, per the SSE spec.
std::string_view strip_leading_space(std::string_view v) {
    if (!v.empty() && v.front() == ' ') {
        v.remove_prefix(1);
    }
    return v;
}

} // namespace

void SseParser::feed(std::string_view chunk, const EventCallback& on_event) {
    for (char c : chunk) {
        if (c == '\n') {
            // Drop a trailing CR (handle CRLF line endings).
            std::string_view line(line_buf_);
            if (!line.empty() && line.back() == '\r') {
                line.remove_suffix(1);
            }
            process_line(line, on_event);
            line_buf_.clear();
        } else {
            line_buf_.push_back(c);
        }
    }
}

void SseParser::process_line(std::string_view line, const EventCallback& on_event) {
    if (line.empty()) {
        // Blank line: dispatch the accumulated event (if any).
        dispatch(on_event);
        return;
    }

    // Comment line (starts with ':') — ignore.
    if (line.front() == ':') {
        return;
    }

    // Split into "field" and "value" at the first colon. A line with no colon
    // is a field name with an empty value (spec); we ignore those except to
    // mark that a field was seen.
    auto colon = line.find(':');
    std::string_view field = (colon == std::string_view::npos) ? line : line.substr(0, colon);
    std::string_view value =
        (colon == std::string_view::npos) ? std::string_view{} : line.substr(colon + 1);
    value = strip_leading_space(value);

    have_field_ = true;
    if (field == "data") {
        if (!cur_data_.empty()) {
            cur_data_.push_back('\n'); // multiple data: lines join with '\n'
        }
        cur_data_.append(value);
    } else if (field == "event") {
        cur_event_.assign(value);
    }
    // id:/retry:/unknown fields are accepted but unused.
}

void SseParser::dispatch(const EventCallback& on_event) {
    if (!have_field_) {
        return; // stray blank line between events
    }
    SseEvent ev;
    ev.data = std::move(cur_data_);
    ev.event = std::move(cur_event_);
    cur_data_.clear();
    cur_event_.clear();
    have_field_ = false;
    on_event(ev);
}

bool SseParser::flush(const EventCallback& on_event) {
    // Treat any buffered partial line as a complete line first.
    if (!line_buf_.empty()) {
        std::string_view line(line_buf_);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        process_line(line, on_event);
        line_buf_.clear();
    }
    if (have_field_) {
        dispatch(on_event);
        return true;
    }
    return false;
}

} // namespace kvmux::upstream
