#include "upstream/sse_parser.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

using kvmux::upstream::SseEvent;
using kvmux::upstream::SseParser;

namespace {
std::vector<SseEvent> collect(SseParser& p, std::string_view chunk) {
    std::vector<SseEvent> out;
    p.feed(chunk, [&](const SseEvent& e) { out.push_back(e); });
    return out;
}
} // namespace

TEST_CASE("sse: single complete event", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data: {\"a\":1}\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"a\":1}");
    CHECK_FALSE(ev[0].is_done());
}

TEST_CASE("sse: [DONE] sentinel recognized", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data: [DONE]\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].is_done());
}

TEST_CASE("sse: multiple events in one chunk preserve order", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data: {\"i\":0}\n\ndata: {\"i\":1}\n\ndata: [DONE]\n\n");
    REQUIRE(ev.size() == 3);
    CHECK(ev[0].data == "{\"i\":0}");
    CHECK(ev[1].data == "{\"i\":1}");
    CHECK(ev[2].is_done());
}

TEST_CASE("sse: event split across two feeds is buffered then emitted", "[sse]") {
    SseParser p;
    std::vector<SseEvent> ev;
    auto sink = [&](const SseEvent& e) { ev.push_back(e); };
    p.feed("data: {\"par", sink);
    CHECK(ev.empty());
    p.feed("tial\":true}\n\n", sink);
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"partial\":true}");
}

TEST_CASE("sse: boundary split exactly on newline", "[sse]") {
    SseParser p;
    std::vector<SseEvent> ev;
    auto sink = [&](const SseEvent& e) { ev.push_back(e); };
    p.feed("data: {\"x\":1}\n", sink); // field line done, event not terminated
    CHECK(ev.empty());
    p.feed("\n", sink); // blank line terminates
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"x\":1}");
}

TEST_CASE("sse: CRLF line endings handled", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data: {\"a\":1}\r\n\r\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"a\":1}");
}

TEST_CASE("sse: comment lines ignored", "[sse]") {
    SseParser p;
    auto ev = collect(p, ": keep-alive ping\n\ndata: {\"a\":1}\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"a\":1}");
}

TEST_CASE("sse: multi-line data joined with newline", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data: line1\ndata: line2\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "line1\nline2");
}

TEST_CASE("sse: event field captured", "[sse]") {
    SseParser p;
    auto ev = collect(p, "event: message\ndata: {\"a\":1}\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].event == "message");
    CHECK(ev[0].data == "{\"a\":1}");
}

TEST_CASE("sse: no leading space after colon still parses", "[sse]") {
    SseParser p;
    auto ev = collect(p, "data:{\"a\":1}\n\n");
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"a\":1}");
}

TEST_CASE("sse: flush emits trailing unterminated event", "[sse]") {
    SseParser p;
    std::vector<SseEvent> ev;
    auto sink = [&](const SseEvent& e) { ev.push_back(e); };
    p.feed("data: {\"tail\":1}\n", sink); // no blank line, server closed
    CHECK(ev.empty());
    bool flushed = p.flush(sink);
    CHECK(flushed);
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data == "{\"tail\":1}");
}

TEST_CASE("sse: byte-at-a-time feeding yields identical events", "[sse]") {
    const std::string stream = "data: {\"i\":0}\n\ndata: {\"i\":1}\n\ndata: [DONE]\n\n";
    SseParser p;
    std::vector<SseEvent> ev;
    auto sink = [&](const SseEvent& e) { ev.push_back(e); };
    for (char c : stream) {
        p.feed(std::string_view(&c, 1), sink);
    }
    REQUIRE(ev.size() == 3);
    CHECK(ev[0].data == "{\"i\":0}");
    CHECK(ev[1].data == "{\"i\":1}");
    CHECK(ev[2].is_done());
}

TEST_CASE("sse: llama.cpp finish chunk with top-level timings passes through verbatim", "[sse]") {
    // llama.cpp emits a finish chunk that carries both choices and a top-level
    // timings object. The parser must forward the data payload unchanged.
    const char* chunk =
        R"(data: {"choices":[{"finish_reason":"stop","delta":{}}],"timings":{"predicted_per_second":42.0}})"
        "\n\n";
    SseParser p;
    auto ev = collect(p, chunk);
    REQUIRE(ev.size() == 1);
    CHECK(ev[0].data.find("\"timings\"") != std::string::npos);
    CHECK(ev[0].data.find("predicted_per_second") != std::string::npos);
}
