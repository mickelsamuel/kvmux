#pragma once

// OpenAI-compatible request/response/chunk types with nlohmann/json serializers.
// Only the subset kvmux v1 needs (chat completions, models). Unknown fields on
// inbound requests are preserved verbatim so they can be forwarded upstream
// (vLLM/llama.cpp/Ollama each accept their own extras).

#include <cstdint>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

namespace kvmux::openai {

using json = nlohmann::json;

// A single chat message. `content` is kept as raw json because the OpenAI schema
// allows either a string or an array of content parts; kvmux does not need to
// interpret it, only to hash a canonical form and forward it.
struct ChatMessage {
    std::string role;
    json content; // string or array; preserved as-is
};

// Parsed view of an inbound /v1/chat/completions request. The original json is
// retained (`raw`) so the upstream request body is forwarded with full fidelity
// plus the stream_options injection; this struct exposes only what routing,
// admission, and streaming decisions need.
struct ChatCompletionRequest {
    std::string model;
    std::vector<ChatMessage> messages;
    bool stream = false;
    json raw; // full original request body

    // Parse from a request body. Throws kvmux::openai::RequestError (see
    // errors.hpp) on a structurally invalid request (missing model/messages,
    // wrong types). Does not validate the model against the backend map.
    static ChatCompletionRequest parse(const json& body);
};

// Aggregated /v1/models entry.
struct ModelCard {
    std::string id;
    std::string owned_by = "kvmux";
    std::int64_t created = 0;
};

void to_json(json& j, const ModelCard& m);

} // namespace kvmux::openai
