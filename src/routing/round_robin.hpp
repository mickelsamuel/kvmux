#pragma once

// Round-robin policy: the benchmark baseline. Spreads requests across the
// eligible candidates by an atomically-incremented cursor, then lists the
// remaining candidates as failover fallbacks in rotation order.

#include "routing/policy.hpp"

#include <atomic>

namespace kvmux::routing {

class RoundRobin {
  public:
    // Order the candidates starting from the next rotation slot. The chosen
    // backend is order[0]; the rest follow in rotation order for failover.
    RouteOrder order(const RouteRequest& req, const std::vector<Candidate>& candidates);

  private:
    std::atomic<std::uint64_t> cursor_{0};
};

} // namespace kvmux::routing
