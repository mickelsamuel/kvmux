#include "openai/errors.hpp"

#include "openai/types.hpp"

namespace kvmux::openai {

int status_code(ErrorKind kind) noexcept {
    switch (kind) {
    case ErrorKind::InvalidRequest:
        return 400;
    case ErrorKind::UnknownModel:
    case ErrorKind::NotFound:
        return 404;
    case ErrorKind::Admission:
        return 429;
    case ErrorKind::UpstreamFailure:
        return 502;
    case ErrorKind::NoBackend:
        return 503;
    case ErrorKind::UpstreamTimeout:
        return 504;
    }
    return 500;
}

const char* error_type(ErrorKind kind) noexcept {
    switch (kind) {
    case ErrorKind::InvalidRequest:
        return "invalid_request_error";
    case ErrorKind::UnknownModel:
        return "invalid_request_error"; // OpenAI uses this type for unknown model
    case ErrorKind::NotFound:
        return "not_found_error";
    case ErrorKind::Admission:
        return "rate_limit_error";
    case ErrorKind::UpstreamFailure:
        return "upstream_error";
    case ErrorKind::NoBackend:
        return "service_unavailable_error";
    case ErrorKind::UpstreamTimeout:
        return "timeout_error";
    }
    return "internal_error";
}

json make_error(ErrorKind kind, const std::string& message, const std::string& param) {
    json err = {
        {"message", message},
        {"type", error_type(kind)},
        {"code", status_code(kind)},
    };
    // OpenAI always carries a `param` key; null when not applicable.
    if (param.empty()) {
        err["param"] = nullptr;
    } else {
        err["param"] = param;
    }
    return json{{"error", err}};
}

json wrap_upstream_error(const std::string& upstream_body, int http_status) {
    // Try to parse; if it's already an OpenAI-shaped error object, pass through.
    json parsed = json::parse(upstream_body, nullptr, /*allow_exceptions=*/false);
    if (parsed.is_object() && parsed.contains("error")) {
        const json& e = parsed["error"];
        if (e.is_object() && e.contains("message")) {
            // Already OpenAI-shaped (vLLM, llama.cpp, Ollama /v1 all use this).
            return parsed;
        }
        if (e.is_string()) {
            // Ollama plain-string error: {"error":"<string>"}.
            return json{{"error",
                         {{"message", e.get<std::string>()},
                          {"type", "upstream_error"},
                          {"param", nullptr},
                          {"code", http_status}}}};
        }
    }

    // Unparseable or unexpected shape: wrap the raw text.
    std::string text = upstream_body.empty() ? "upstream request failed" : upstream_body;
    return json{{"error",
                 {{"message", text},
                  {"type", "upstream_error"},
                  {"param", nullptr},
                  {"code", http_status}}}};
}

// ---- types.hpp implementations that need json -----------------------------

ChatCompletionRequest ChatCompletionRequest::parse(const json& body) {
    if (!body.is_object()) {
        throw RequestError(ErrorKind::InvalidRequest, "request body must be a JSON object");
    }

    ChatCompletionRequest req;
    req.raw = body;

    auto model_it = body.find("model");
    if (model_it == body.end() || !model_it->is_string()) {
        throw RequestError(ErrorKind::InvalidRequest, "missing or invalid required field 'model'",
                           "model");
    }
    req.model = model_it->get<std::string>();

    auto messages_it = body.find("messages");
    if (messages_it == body.end() || !messages_it->is_array() || messages_it->empty()) {
        throw RequestError(ErrorKind::InvalidRequest,
                           "missing or invalid required field 'messages'", "messages");
    }
    for (const auto& m : *messages_it) {
        if (!m.is_object() || !m.contains("role") || !m["role"].is_string()) {
            throw RequestError(ErrorKind::InvalidRequest, "each message requires a string 'role'",
                               "messages");
        }
        ChatMessage msg;
        msg.role = m["role"].get<std::string>();
        msg.content = m.contains("content") ? m["content"] : json(nullptr);
        req.messages.push_back(std::move(msg));
    }

    auto stream_it = body.find("stream");
    if (stream_it != body.end()) {
        if (!stream_it->is_boolean()) {
            throw RequestError(ErrorKind::InvalidRequest, "'stream' must be a boolean", "stream");
        }
        req.stream = stream_it->get<bool>();
    }

    return req;
}

void to_json(json& j, const ModelCard& m) {
    j = json{
        {"id", m.id},
        {"object", "model"},
        {"created", m.created},
        {"owned_by", m.owned_by},
    };
}

} // namespace kvmux::openai
