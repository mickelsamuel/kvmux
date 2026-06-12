#include "routing/round_robin.hpp"

namespace kvmux::routing {

RouteOrder RoundRobin::order(const RouteRequest& req, const std::vector<Candidate>& candidates) {
    (void) req; // round-robin ignores the affinity key
    RouteOrder out;
    const std::size_t n = candidates.size();
    if (n == 0) {
        return out;
    }
    const std::uint64_t start = cursor_.fetch_add(1, std::memory_order_relaxed);
    out.order.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const std::size_t idx = static_cast<std::size_t>((start + i) % n);
        out.order.push_back(candidates[idx].backend_index);
    }
    return out;
}

} // namespace kvmux::routing
