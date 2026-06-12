#include "upstream/sse_parser.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

using kvmux::upstream::SseEvent;
using kvmux::upstream::SseParser;
using json = nlohmann::json;

namespace {

std::string read_fixture(const std::string& name) {
    std::filesystem::path p = std::filesystem::path(KVMUX_FIXTURE_DIR) / name;
    std::ifstream in(p, std::ios::binary);
    REQUIRE(in.good());
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// Parse a fixture, feeding it in small (7-byte) slices to exercise the
// incremental buffering against a *real* recorded byte stream.
std::vector<SseEvent> parse_chunked(const std::string& raw, std::size_t slice = 7) {
    SseParser p;
    std::vector<SseEvent> events;
    auto sink = [&](const SseEvent& e) { events.push_back(e); };
    for (std::size_t i = 0; i < raw.size(); i += slice) {
        p.feed(std::string_view(raw).substr(i, slice), sink);
    }
    p.flush(sink);
    return events;
}

// Common assertions for an OpenAI chat-completion SSE stream recorded from a
// real backend: at least one data chunk, every non-[DONE] data payload is valid
// JSON of object "chat.completion.chunk", and the stream ends with [DONE].
void assert_valid_chat_stream(const std::vector<SseEvent>& events) {
    REQUIRE_FALSE(events.empty());
    CHECK(events.back().is_done());

    int json_chunks = 0;
    bool saw_content = false;
    bool saw_finish = false;
    for (const auto& ev : events) {
        if (ev.is_done()) {
            continue;
        }
        json j = json::parse(ev.data, nullptr, /*allow_exceptions=*/false);
        REQUIRE_FALSE(j.is_discarded()); // every data: payload must be valid JSON
        ++json_chunks;
        // The OpenAI chunk object type (all three backends set this on /v1).
        if (j.contains("object")) {
            CHECK(j["object"] == "chat.completion.chunk");
        }
        if (j.contains("choices") && j["choices"].is_array() && !j["choices"].empty()) {
            const auto& choice = j["choices"][0];
            if (choice.contains("delta") && choice["delta"].contains("content") &&
                choice["delta"]["content"].is_string() &&
                !choice["delta"]["content"].get<std::string>().empty()) {
                saw_content = true;
            }
            if (choice.contains("finish_reason") && !choice["finish_reason"].is_null()) {
                saw_finish = true;
            }
        }
    }
    CHECK(json_chunks > 0);
    CHECK(saw_content);
    CHECK(saw_finish);
}

} // namespace

// These fixtures are real recorded SSE responses captured in WSL2 from the
// actual backends (see tests/fixtures/README.md for capture provenance).

TEST_CASE("sse-fixture: real Ollama /v1 stream parses to a valid chat stream", "[sse][fixture]") {
    auto raw = read_fixture("ollama_chat_stream.sse");
    auto events = parse_chunked(raw);
    assert_valid_chat_stream(events);
}

TEST_CASE("sse-fixture: real llama.cpp-server /v1 stream parses to a valid chat stream",
          "[sse][fixture]") {
    auto raw = read_fixture("llamacpp_chat_stream.sse");
    auto events = parse_chunked(raw);
    assert_valid_chat_stream(events);
}

TEST_CASE("sse-fixture: llama.cpp finish chunk carries a top-level timings object",
          "[sse][fixture]") {
    auto raw = read_fixture("llamacpp_chat_stream.sse");
    auto events = parse_chunked(raw);
    bool saw_timings = false;
    for (const auto& ev : events) {
        if (ev.is_done()) {
            continue;
        }
        json j = json::parse(ev.data, nullptr, false);
        if (!j.is_discarded() && j.contains("timings")) {
            saw_timings = true;
        }
    }
    CHECK(saw_timings); // documents/guards the llama.cpp quirk (findings Q4.3)
}

TEST_CASE("sse-fixture: usage chunk (include_usage) carries usage with empty choices",
          "[sse][fixture]") {
    // Captured with stream_options.include_usage=true; the trailing usage chunk
    // has empty choices and a usage object (findings Q4.1).
    auto raw = read_fixture("ollama_usage_stream.sse");
    auto events = parse_chunked(raw);
    REQUIRE_FALSE(events.empty());
    CHECK(events.back().is_done());

    bool saw_usage = false;
    for (const auto& ev : events) {
        if (ev.is_done()) {
            continue;
        }
        json j = json::parse(ev.data, nullptr, false);
        REQUIRE_FALSE(j.is_discarded());
        if (j.contains("usage") && j["usage"].is_object()) {
            saw_usage = true;
            CHECK(j["usage"].contains("total_tokens"));
        }
    }
    CHECK(saw_usage);
}

TEST_CASE("sse-fixture: synthetic vLLM stream (documented) parses to a valid chat stream",
          "[sse][fixture]") {
    // vLLM sm_120 wheels did not install under WSL2 this session (logged for
    // M6); this fixture is a DOCUMENTED SYNTHETIC built from findings Q4, not a
    // real capture. See tests/fixtures/README.md.
    auto raw = read_fixture("vllm_chat_stream.synthetic.sse");
    auto events = parse_chunked(raw);
    assert_valid_chat_stream(events);
}
