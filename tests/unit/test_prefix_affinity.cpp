#include "routing/prefix_affinity.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>
#include <vector>

using kvmux::routing::Candidate;
using kvmux::routing::PrefixAffinity;
using kvmux::routing::RouteRequest;

namespace {
std::vector<Candidate> backends(const std::vector<std::size_t>& ids, int active = 0,
                                int max_in_flight = 8) {
    std::vector<Candidate> out;
    for (auto id : ids) {
        Candidate c;
        c.backend_index = id;
        c.active_requests = active;
        c.max_in_flight = max_in_flight;
        out.push_back(c);
    }
    return out;
}

std::string canon(const std::string& model,
                  const std::vector<std::pair<std::string, std::string>>& msgs, int bytes = 1024) {
    return kvmux::routing::canonical_affinity_string(model, msgs, bytes);
}
} // namespace

// --- Canonicalization + key ------------------------------------------------

TEST_CASE("affinity: canonical string is model + 0x1f + role:content joined", "[affinity][key]") {
    auto s = canon("m", {{"system", "you are helpful"}, {"user", "hi"}});
    // model, then 0x1f, then "system:you are helpful", then "user:hi"
    CHECK(s.rfind("m", 0) == 0);
    CHECK(s.find('\x1f') != std::string::npos);
    CHECK(s.find("system:you are helpful") != std::string::npos);
    CHECK(s.find("user:hi") != std::string::npos);
}

TEST_CASE("affinity: canonical string is truncated to prefix_bytes", "[affinity][key]") {
    std::string big(5000, 'x');
    auto s = canon("model", {{"user", big}}, 1024);
    CHECK(s.size() == 1024);
}

TEST_CASE("affinity: same prefix -> same key hash; different prefix -> different",
          "[affinity][key]") {
    auto k1 = kvmux::routing::affinity_key_hash(canon("m", {{"user", "shared prefix abc"}}));
    auto k2 = kvmux::routing::affinity_key_hash(canon("m", {{"user", "shared prefix abc"}}));
    auto k3 = kvmux::routing::affinity_key_hash(canon("m", {{"user", "totally different"}}));
    CHECK(k1 == k2);
    CHECK(k1 != k3);
}

TEST_CASE("affinity: model is part of the key (same content, different model)", "[affinity][key]") {
    auto k1 = kvmux::routing::affinity_key_hash(canon("model-a", {{"user", "x"}}));
    auto k2 = kvmux::routing::affinity_key_hash(canon("model-b", {{"user", "x"}}));
    CHECK(k1 != k2);
}

// --- HRW stickiness --------------------------------------------------------

TEST_CASE("affinity: HRW is deterministic and sticky for a fixed key", "[affinity][hrw]") {
    auto cands = backends({0, 1, 2, 3});
    std::uint64_t key = 0xABCDEF1234567890ULL;
    auto r1 = PrefixAffinity::hrw_rank(key, cands);
    auto r2 = PrefixAffinity::hrw_rank(key, cands);
    REQUIRE(r1 == r2);       // deterministic
    REQUIRE(r1.size() == 4); // full ranking
    std::set<std::size_t> s(r1.begin(), r1.end());
    CHECK(s == std::set<std::size_t>{0, 1, 2, 3}); // a permutation
}

TEST_CASE("affinity: distinct keys spread across backends", "[affinity][hrw]") {
    auto cands = backends({0, 1, 2, 3});
    std::set<std::size_t> winners;
    for (std::uint64_t k = 0; k < 200; ++k) {
        auto r = PrefixAffinity::hrw_rank(k * 0x9E3779B97F4A7C15ULL + 1, cands);
        winners.insert(r.front());
    }
    // With 200 distinct keys over 4 backends, every backend should win some.
    CHECK(winners.size() == 4);
}

TEST_CASE("affinity: removing a backend reshuffles minimally (HRW property)", "[affinity][hrw]") {
    // For each key, the winner over {0,1,2,3} should, after removing a backend
    // that was NOT the winner, keep the same winner. Only keys whose winner was
    // the removed backend get a new winner. That is the HRW minimal-reshuffle
    // guarantee.
    auto full = backends({0, 1, 2, 3});
    auto without2 = backends({0, 1, 3});
    int moved = 0;
    int kept = 0;
    for (std::uint64_t k = 0; k < 400; ++k) {
        std::uint64_t key = k * 0x9E3779B97F4A7C15ULL + 7;
        auto wfull = PrefixAffinity::hrw_rank(key, full).front();
        auto wless = PrefixAffinity::hrw_rank(key, without2).front();
        if (wfull == 2) {
            // its key must move to some other backend
            CHECK(wless != 2);
            ++moved;
        } else {
            // winner unaffected by removing a non-winner
            CHECK(wless == wfull);
            ++kept;
        }
    }
    // Sanity: both buckets are non-trivial.
    CHECK(moved > 0);
    CHECK(kept > 0);
}

// --- Load-guard spill cascade ----------------------------------------------

TEST_CASE("affinity: under-loaded winner is chosen with zero spills", "[affinity][spill]") {
    PrefixAffinity pa(0.8);
    auto cands = backends({0, 1, 2}, /*active=*/0, /*max=*/10);
    RouteRequest req;
    req.affinity_key = canon("m", {{"user", "hello"}});
    auto o = pa.order(req, cands);
    REQUIRE_FALSE(o.order.empty());
    CHECK(o.spills == 0);
    // The chosen head is the HRW winner.
    auto rank =
        PrefixAffinity::hrw_rank(kvmux::routing::affinity_key_hash(req.affinity_key), cands);
    CHECK(o.order.front() == rank.front());
}

TEST_CASE("affinity: overloaded winner spills to the next HRW candidate", "[affinity][spill]") {
    PrefixAffinity pa(0.8);
    RouteRequest req;
    req.affinity_key = canon("m", {{"user", "spill-test"}});
    std::uint64_t key = kvmux::routing::affinity_key_hash(req.affinity_key);

    // Build candidates where the HRW winner is at/above 0.8*max (overloaded) and
    // the rest are idle. The winner must be skipped (1 spill) and the chosen head
    // becomes the second-ranked backend.
    auto cands = backends({0, 1, 2}, 0, 10);
    auto rank = PrefixAffinity::hrw_rank(key, cands);
    // Overload only the top-ranked backend.
    for (auto& c : cands) {
        if (c.backend_index == rank.front()) {
            c.active_requests = 9; // >= 0.8*10
        }
    }
    auto o = pa.order(req, cands);
    CHECK(o.spills == 1);
    CHECK(o.order.front() == rank[1]); // spilled to the 2nd HRW candidate
    // The overloaded winner is still present as a failover fallback.
    CHECK(std::find(o.order.begin(), o.order.end(), rank.front()) != o.order.end());
}

TEST_CASE("affinity: cascade spills past multiple overloaded leaders", "[affinity][spill]") {
    PrefixAffinity pa(0.8);
    RouteRequest req;
    req.affinity_key = canon("m", {{"user", "cascade"}});
    std::uint64_t key = kvmux::routing::affinity_key_hash(req.affinity_key);
    auto cands = backends({0, 1, 2, 3}, 0, 10);
    auto rank = PrefixAffinity::hrw_rank(key, cands);
    // Overload the top two ranked backends.
    for (auto& c : cands) {
        if (c.backend_index == rank[0] || c.backend_index == rank[1]) {
            c.active_requests = 10;
        }
    }
    auto o = pa.order(req, cands);
    CHECK(o.spills == 2);
    CHECK(o.order.front() == rank[2]);
}

TEST_CASE("affinity: all overloaded -> falls back to pure HRW order (still sticky)",
          "[affinity][spill]") {
    PrefixAffinity pa(0.8);
    RouteRequest req;
    req.affinity_key = canon("m", {{"user", "all-busy"}});
    std::uint64_t key = kvmux::routing::affinity_key_hash(req.affinity_key);
    auto cands = backends({0, 1, 2}, 10, 10); // all at capacity
    auto o = pa.order(req, cands);
    auto rank = PrefixAffinity::hrw_rank(key, cands);
    // No under-loaded candidate; the order is the pure HRW ranking.
    CHECK(o.order == rank);
    CHECK(o.spills == static_cast<int>(rank.size())); // every leader was a spill
}
