#pragma once

// Prefix-affinity routing v1 — the differentiator (locked algorithm, 03_PLAN.md).
//
// HONEST LABEL (never claim more): this is *client-side prefix-hash affinity* —
// an approximation of KV-cache locality that needs no engine events. It does not
// read vLLM/SGLang KV-cache state; it routes requests that share a prompt prefix
// to the same backend so that backend's automatic prefix cache is the one that
// gets reused. v1.1 may consume vLLM's ZMQ KV events; v1 does not.
//
// Algorithm, exactly:
//   1. Affinity key = first `prefix_bytes` of the canonical concatenation
//        model + '\x1f' + role + ':' + content   (messages[0..], in order)
//      truncated at the byte budget; hashed with XXH64.
//   2. Backend choice = rendezvous (HRW) hashing of (key, backend-id) over the
//      eligible backends -> deterministic stickiness, minimal reshuffle on churn.
//   3. Load guard: if the HRW winner's active_requests >=
//      load_threshold * max_in_flight, cascade to the next HRW candidate. Every
//      such step is a "spill" (counted by the gateway via affinity_spills_total).
//
// All functions are pure/deterministic so the HRW behavior is unit-testable.

#include "routing/policy.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace kvmux::openai {
struct ChatCompletionRequest;
}

namespace kvmux::routing {

// Build the canonical affinity-key string for a request, truncated to
// `prefix_bytes`. Exposed for testing the canonicalization independently.
std::string canonical_affinity_string(const std::string& model,
                                      const std::vector<std::pair<std::string, std::string>>& msgs,
                                      int prefix_bytes);

// XXH64 of the canonical affinity string. The hash of the (already truncated)
// canonical bytes is the affinity key used by HRW.
std::uint64_t affinity_key_hash(const std::string& canonical);

// Convenience: derive the affinity key hash straight from a parsed request.
std::uint64_t affinity_key_for_request(const openai::ChatCompletionRequest& req, int prefix_bytes);

// Rendezvous (HRW) ordering of candidates for a given affinity key, plus the
// load-guard spill cascade. `load_threshold` is the fraction of max_in_flight at
// or above which the top HRW winner is skipped in favor of the next.
//
// The returned RouteOrder.order is the HRW ranking (best-first) AFTER the load
// guard has moved any over-loaded leading winners behind the first under-loaded
// candidate; RouteOrder.spills counts how many leading HRW winners were skipped
// by the guard. The full ranking is preserved as the failover tail.
class PrefixAffinity {
  public:
    explicit PrefixAffinity(double load_threshold) : load_threshold_(load_threshold) {}

    RouteOrder order(const RouteRequest& req, const std::vector<Candidate>& candidates) const;

    // The pure HRW ranking (best-first) for a key over candidate backend ids —
    // exposed for deterministic stickiness/churn tests.
    static std::vector<std::size_t> hrw_rank(std::uint64_t key,
                                             const std::vector<Candidate>& candidates);

  private:
    double load_threshold_;
};

} // namespace kvmux::routing
