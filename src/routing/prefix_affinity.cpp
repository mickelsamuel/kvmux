#include "routing/prefix_affinity.hpp"

#include "openai/types.hpp"

#include <algorithm>
#include <array>
#include <cstring>

// Single-translation-unit inline build of xxHash: XXH_INLINE_ALL pulls the
// implementation into this TU so we need not compile/link xxhash.c.
#define XXH_INLINE_ALL
#include <xxhash.h>

namespace kvmux::routing {

std::string canonical_affinity_string(const std::string& model,
                                      const std::vector<std::pair<std::string, std::string>>& msgs,
                                      int prefix_bytes) {
    // Canonical form: model + '\x1f' + for each message in order
    //   role + ':' + content
    // concatenated, then truncated to the first prefix_bytes bytes.
    const std::size_t budget = prefix_bytes > 0 ? static_cast<std::size_t>(prefix_bytes) : 0;
    std::string out;
    out.reserve(std::min(budget, model.size() + 64));

    auto append_capped = [&](std::string_view s) {
        if (out.size() >= budget) {
            return;
        }
        const std::size_t room = budget - out.size();
        out.append(s.substr(0, room));
    };

    append_capped(model);
    append_capped(std::string_view("\x1f", 1));
    for (const auto& [role, content] : msgs) {
        if (out.size() >= budget) {
            break;
        }
        append_capped(role);
        append_capped(std::string_view(":", 1));
        append_capped(content);
    }
    return out;
}

std::uint64_t affinity_key_hash(const std::string& canonical) {
    return static_cast<std::uint64_t>(XXH64(canonical.data(), canonical.size(), /*seed=*/0));
}

namespace {
// Flatten messages[].content (string or array of parts) to a single string for
// the affinity canonicalization. For array content, concatenate the textual
// "text" fields (the only part type that affects prefix locality); anything else
// is serialized compactly so distinct structured prompts still hash distinctly.
std::string content_to_string(const nlohmann::json& content) {
    if (content.is_string()) {
        return content.get<std::string>();
    }
    if (content.is_null()) {
        return std::string();
    }
    if (content.is_array()) {
        std::string s;
        for (const auto& part : content) {
            if (part.is_object() && part.contains("text") && part["text"].is_string()) {
                s += part["text"].get<std::string>();
            } else {
                s += part.dump();
            }
        }
        return s;
    }
    return content.dump();
}
} // namespace

std::uint64_t affinity_key_for_request(const openai::ChatCompletionRequest& req, int prefix_bytes) {
    std::vector<std::pair<std::string, std::string>> msgs;
    msgs.reserve(req.messages.size());
    for (const auto& m : req.messages) {
        msgs.emplace_back(m.role, content_to_string(m.content));
    }
    return affinity_key_hash(canonical_affinity_string(req.model, msgs, prefix_bytes));
}

std::vector<std::size_t> PrefixAffinity::hrw_rank(std::uint64_t key,
                                                  const std::vector<Candidate>& candidates) {
    // HRW / rendezvous: score(backend) = H(key, backend_id); rank by descending
    // score; ties (vanishingly unlikely with a 64-bit mix) break by backend_id
    // for determinism. We mix key and the stable backend id (its config index)
    // through XXH64 over an 16-byte buffer so the score is well-distributed and
    // reproducible across processes/runs.
    struct Scored {
        std::size_t backend_index;
        std::uint64_t score;
    };
    std::vector<Scored> scored;
    scored.reserve(candidates.size());
    for (const auto& c : candidates) {
        std::array<unsigned char, 16> buf{};
        std::uint64_t id = static_cast<std::uint64_t>(c.backend_index);
        std::memcpy(buf.data(), &key, 8);
        std::memcpy(buf.data() + 8, &id, 8);
        std::uint64_t score = static_cast<std::uint64_t>(XXH64(buf.data(), buf.size(), 0));
        scored.push_back({c.backend_index, score});
    }
    std::sort(scored.begin(), scored.end(), [](const Scored& a, const Scored& b) {
        if (a.score != b.score) {
            return a.score > b.score; // higher score wins
        }
        return a.backend_index < b.backend_index; // deterministic tie-break
    });
    std::vector<std::size_t> out;
    out.reserve(scored.size());
    for (const auto& s : scored) {
        out.push_back(s.backend_index);
    }
    return out;
}

RouteOrder PrefixAffinity::order(const RouteRequest& req,
                                 const std::vector<Candidate>& candidates) const {
    RouteOrder out;
    if (candidates.empty()) {
        return out;
    }
    // Hash the (already canonical, already truncated) affinity key bytes.
    const std::uint64_t key = affinity_key_hash(req.affinity_key);

    // Full HRW ranking, best-first.
    std::vector<std::size_t> rank = hrw_rank(key, candidates);

    // Index candidates by backend_index so we can read live load during the
    // load-guard cascade.
    auto load_of = [&](std::size_t backend_index) -> std::pair<int, int> {
        for (const auto& c : candidates) {
            if (c.backend_index == backend_index) {
                return {c.active_requests, c.max_in_flight};
            }
        }
        return {0, 1};
    };

    // Load guard: skip leading HRW winners whose active_requests is at/above
    // load_threshold * max_in_flight, cascading to the next candidate. Count each
    // skipped leading winner as a spill. The skipped winners are appended after
    // the chosen head so they remain available as failover fallbacks.
    std::size_t head = 0;
    int spills = 0;
    for (; head < rank.size(); ++head) {
        auto [active, cap] = load_of(rank[head]);
        const double limit = load_threshold_ * static_cast<double>(cap);
        if (static_cast<double>(active) >= limit) {
            ++spills; // this leading winner is overloaded -> spill to the next
            continue;
        }
        break; // rank[head] is the first under-loaded candidate -> chosen head
    }

    out.spills = spills;
    if (head >= rank.size()) {
        // All candidates are at/above the load threshold: fall back to the pure
        // HRW order (best-first) so the request still goes somewhere sticky.
        out.order = std::move(rank);
        return out;
    }

    // Chosen head first, then the rest of the HRW ranking (including the spilled
    // leading winners) as failover fallbacks, in HRW order.
    out.order.reserve(rank.size());
    out.order.push_back(rank[head]);
    for (std::size_t i = 0; i < rank.size(); ++i) {
        if (i == head) {
            continue;
        }
        out.order.push_back(rank[i]);
    }
    return out;
}

} // namespace kvmux::routing
