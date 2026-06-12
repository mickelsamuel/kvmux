#pragma once

// Routing policy interface. A policy orders the eligible backends for a given
// request; the gateway then tries them in that order (failover walks the list).
//
//   * round_robin     — the benchmark baseline; spreads load evenly. Must stay
//                       selectable forever (plan, locked spec).
//   * prefix_affinity — the differentiator (M4): rendezvous/HRW hashing of the
//                       request's canonical prefix key over eligible backends,
//                       with a load-guard spill cascade.
//
// Both implementations are pure ordering functions over (request, eligible
// backend set, live load) — no I/O — so they are deterministically testable.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kvmux::routing {

// A backend as the router sees it: a stable id (index into the gateway's
// backend vector), its current active-request count, and its max_in_flight (for
// the affinity load guard). Eligibility (health, breaker, serves-model) is
// decided by the gateway before the router is consulted — the router only sees
// candidates it is allowed to choose.
struct Candidate {
    std::size_t backend_index = 0;
    int active_requests = 0;
    int max_in_flight = 0;
};

// Inputs the policies may need from the request.
struct RouteRequest {
    // The canonical affinity key bytes (model + roles + content, truncated to
    // prefix_bytes). Empty for round_robin (which ignores it).
    std::string affinity_key;
};

// Outcome of ordering: the backend indices to try, best-first. The gateway
// walks this list for failover. `spills` counts affinity load-guard cascade
// steps taken (0 for round_robin) so the gateway can bump the spill counter.
struct RouteOrder {
    std::vector<std::size_t> order;
    int spills = 0;
};

} // namespace kvmux::routing
