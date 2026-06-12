#pragma once

// OpenAI-shaped error model. Every error kvmux emits downstream is an OpenAI
// error object: {"error":{"message","type","param","code"}}. The mapping table
// is fixed by the plan's wire-behavior section.

#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

namespace kvmux::openai {

using json = nlohmann::json;

// The gateway-generated error categories and their HTTP status codes.
enum class ErrorKind {
    InvalidRequest,  // 400 — malformed/invalid inbound request
    UnknownModel,    // 404 — model/route not served by any backend
    NotFound,        // 404 — unknown route
    Admission,       // 429 — admission control: queue full / timed out
    UpstreamFailure, // 502 — backend returned an error / connection failed
    NoBackend,       // 503 — no eligible (healthy, closed-breaker) backend
    UpstreamTimeout, // 504 — upstream exceeded request_timeout_ms
};

// HTTP status code for an ErrorKind.
int status_code(ErrorKind kind) noexcept;

// The OpenAI `type` string for an ErrorKind.
const char* error_type(ErrorKind kind) noexcept;

// Build an OpenAI error object: {"error":{message,type,param,code}}.
// `code` is the integer HTTP status (matching vLLM/Ollama /v1 convention).
json make_error(ErrorKind kind, const std::string& message, const std::string& param = "");

// Wrap an upstream error body into OpenAI shape.
//  - If `upstream_body` already parses to an OpenAI-shaped error
//    ({"error":{...}} object), it is passed through unchanged.
//  - Ollama-style plain-string errors ({"error":"<string>"}) and any other
//    shape are wrapped as
//    {"error":{"message":<text>,"type":"upstream_error","code":<http_status>}}.
// `http_status` is the upstream HTTP status (used as the wrapped code; defaults
// to 502 when the upstream failed without a usable status).
json wrap_upstream_error(const std::string& upstream_body, int http_status = 502);

// Thrown by request parsing / validation; carries an ErrorKind so the session
// layer maps it to the right status + OpenAI error body.
class RequestError : public std::runtime_error {
  public:
    RequestError(ErrorKind kind, std::string message, std::string param = "")
        : std::runtime_error(message), kind_(kind), message_(std::move(message)),
          param_(std::move(param)) {}

    ErrorKind kind() const noexcept { return kind_; }
    const std::string& message() const noexcept { return message_; }
    const std::string& param() const noexcept { return param_; }
    json to_json() const { return make_error(kind_, message_, param_); }

  private:
    ErrorKind kind_;
    std::string message_;
    std::string param_;
};

} // namespace kvmux::openai
