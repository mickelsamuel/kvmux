#include "routing/round_robin.hpp"

#include <catch2/catch_test_macros.hpp>
#include <set>

using kvmux::routing::Candidate;
using kvmux::routing::RoundRobin;
using kvmux::routing::RouteRequest;

namespace {
std::vector<Candidate> make(std::vector<std::size_t> indices) {
    std::vector<Candidate> out;
    for (auto i : indices) {
        Candidate c;
        c.backend_index = i;
        c.max_in_flight = 8;
        out.push_back(c);
    }
    return out;
}
} // namespace

TEST_CASE("round_robin: rotates the head across calls", "[routing][rr]") {
    RoundRobin rr;
    auto cands = make({0, 1, 2});
    RouteRequest req;
    auto a = rr.order(req, cands);
    auto b = rr.order(req, cands);
    auto c = rr.order(req, cands);
    REQUIRE(a.order.size() == 3);
    // Heads advance 0 -> 1 -> 2.
    CHECK(a.order[0] == 0);
    CHECK(b.order[0] == 1);
    CHECK(c.order[0] == 2);
}

TEST_CASE("round_robin: order is a full permutation for failover", "[routing][rr]") {
    RoundRobin rr;
    auto cands = make({5, 6, 7});
    auto o = rr.order(RouteRequest{}, cands);
    std::set<std::size_t> s(o.order.begin(), o.order.end());
    CHECK(s == std::set<std::size_t>{5, 6, 7});
    CHECK(o.spills == 0); // RR never spills
}

TEST_CASE("round_robin: empty candidate set yields empty order", "[routing][rr]") {
    RoundRobin rr;
    auto o = rr.order(RouteRequest{}, {});
    CHECK(o.order.empty());
}

TEST_CASE("round_robin: single candidate is always chosen", "[routing][rr]") {
    RoundRobin rr;
    auto cands = make({3});
    for (int i = 0; i < 5; ++i) {
        auto o = rr.order(RouteRequest{}, cands);
        REQUIRE(o.order.size() == 1);
        CHECK(o.order[0] == 3);
    }
}
