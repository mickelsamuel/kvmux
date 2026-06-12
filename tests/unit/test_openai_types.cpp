#include "openai/errors.hpp"
#include "openai/types.hpp"

#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

using kvmux::openai::ChatCompletionRequest;
using kvmux::openai::ModelCard;
using kvmux::openai::RequestError;
using json = nlohmann::json;

TEST_CASE("types: valid chat request parses model/messages/stream", "[openai]") {
    json body = {
        {"model", "llama3.2:3b"},
        {"messages", json::array({{{"role", "system"}, {"content", "be terse"}},
                                  {{"role", "user"}, {"content", "hi"}}})},
        {"stream", true},
        {"temperature", 0.7}, // extra field preserved in raw
    };
    auto req = ChatCompletionRequest::parse(body);
    CHECK(req.model == "llama3.2:3b");
    CHECK(req.stream == true);
    REQUIRE(req.messages.size() == 2);
    CHECK(req.messages[0].role == "system");
    CHECK(req.messages[1].role == "user");
    // raw retains the full body incl. the extra field, for verbatim forwarding.
    CHECK(req.raw.contains("temperature"));
    CHECK(req.raw["temperature"] == 0.7);
}

TEST_CASE("types: stream defaults to false when absent", "[openai]") {
    json body = {{"model", "m"}, {"messages", json::array({{{"role", "user"}, {"content", "x"}}})}};
    auto req = ChatCompletionRequest::parse(body);
    CHECK(req.stream == false);
}

TEST_CASE("types: array-valued content is preserved without interpretation", "[openai]") {
    json content = json::array({{{"type", "text"}, {"text", "hello"}}});
    json body = {{"model", "m"},
                 {"messages", json::array({{{"role", "user"}, {"content", content}}})}};
    auto req = ChatCompletionRequest::parse(body);
    REQUIRE(req.messages.size() == 1);
    CHECK(req.messages[0].content.is_array());
    CHECK(req.messages[0].content == content);
}

TEST_CASE("types: missing model throws RequestError", "[openai][failfast]") {
    json body = {{"messages", json::array({{{"role", "user"}, {"content", "x"}}})}};
    REQUIRE_THROWS_AS(ChatCompletionRequest::parse(body), RequestError);
}

TEST_CASE("types: missing/empty messages throws RequestError", "[openai][failfast]") {
    REQUIRE_THROWS_AS(ChatCompletionRequest::parse(json{{"model", "m"}}), RequestError);
    REQUIRE_THROWS_AS(
        ChatCompletionRequest::parse(json{{"model", "m"}, {"messages", json::array()}}),
        RequestError);
}

TEST_CASE("types: non-boolean stream throws RequestError", "[openai][failfast]") {
    json body = {{"model", "m"},
                 {"messages", json::array({{{"role", "user"}, {"content", "x"}}})},
                 {"stream", "yes"}};
    REQUIRE_THROWS_AS(ChatCompletionRequest::parse(body), RequestError);
}

TEST_CASE("types: ModelCard serializes to OpenAI model object", "[openai]") {
    ModelCard m;
    m.id = "llama3.2:3b";
    m.created = 1700000000;
    json j = m;
    CHECK(j["id"] == "llama3.2:3b");
    CHECK(j["object"] == "model");
    CHECK(j["created"] == 1700000000);
    CHECK(j["owned_by"] == "kvmux");
}
