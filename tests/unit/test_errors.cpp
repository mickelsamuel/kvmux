#include "openai/errors.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace kvmux::openai;

TEST_CASE("errors: status codes match the mapping table", "[errors]") {
    CHECK(status_code(ErrorKind::InvalidRequest) == 400);
    CHECK(status_code(ErrorKind::UnknownModel) == 404);
    CHECK(status_code(ErrorKind::NotFound) == 404);
    CHECK(status_code(ErrorKind::Admission) == 429);
    CHECK(status_code(ErrorKind::UpstreamFailure) == 502);
    CHECK(status_code(ErrorKind::NoBackend) == 503);
    CHECK(status_code(ErrorKind::UpstreamTimeout) == 504);
}

TEST_CASE("errors: make_error produces OpenAI shape with param null when empty", "[errors]") {
    auto j = make_error(ErrorKind::InvalidRequest, "bad", "");
    REQUIRE(j.contains("error"));
    CHECK(j["error"]["message"] == "bad");
    CHECK(j["error"]["type"] == "invalid_request_error");
    CHECK(j["error"]["code"] == 400);
    CHECK(j["error"]["param"].is_null());
}

TEST_CASE("errors: make_error carries param when provided", "[errors]") {
    auto j = make_error(ErrorKind::InvalidRequest, "bad model", "model");
    CHECK(j["error"]["param"] == "model");
}

TEST_CASE("errors: OpenAI-shaped upstream error passes through unchanged", "[errors]") {
    // vLLM / llama.cpp / Ollama-/v1 style.
    std::string upstream =
        R"({"error":{"message":"model not found","type":"invalid_request_error","code":404}})";
    auto j = wrap_upstream_error(upstream, 404);
    CHECK(j["error"]["message"] == "model not found");
    CHECK(j["error"]["type"] == "invalid_request_error");
    CHECK(j["error"]["code"] == 404);
}

TEST_CASE("errors: Ollama plain-string error is wrapped to OpenAI shape", "[errors]") {
    // Ollama native: {"error":"<string>"}.
    std::string upstream = R"({"error":"model 'foo' not found, try pulling it first"})";
    auto j = wrap_upstream_error(upstream, 404);
    CHECK(j["error"]["message"] == "model 'foo' not found, try pulling it first");
    CHECK(j["error"]["type"] == "upstream_error");
    CHECK(j["error"]["code"] == 404);
    CHECK(j["error"]["param"].is_null());
}

TEST_CASE("errors: unparseable upstream body is wrapped, not dropped", "[errors]") {
    auto j = wrap_upstream_error("<html>502 Bad Gateway</html>", 502);
    CHECK(j["error"]["type"] == "upstream_error");
    CHECK(j["error"]["code"] == 502);
    CHECK(j["error"]["message"] == "<html>502 Bad Gateway</html>");
}

TEST_CASE("errors: empty upstream body yields a generic message", "[errors]") {
    auto j = wrap_upstream_error("", 502);
    CHECK(j["error"]["message"] == "upstream request failed");
    CHECK(j["error"]["code"] == 502);
}

TEST_CASE("errors: RequestError carries kind and renders OpenAI json", "[errors]") {
    RequestError e(ErrorKind::UnknownModel, "no such model", "model");
    CHECK(e.kind() == ErrorKind::UnknownModel);
    auto j = e.to_json();
    CHECK(j["error"]["code"] == 404);
    CHECK(j["error"]["param"] == "model");
}
