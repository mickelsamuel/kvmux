#include "metrics/registry.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

using kvmux::metrics::Labels;
using kvmux::metrics::Registry;

namespace {
Labels lbl(const std::string& backend = "b0", const std::string& model = "m") {
    return Labels{"chat_completions", backend, model};
}
int count_substr(const std::string& hay, const std::string& needle) {
    int n = 0;
    for (std::size_t pos = 0; (pos = hay.find(needle, pos)) != std::string::npos;
         pos += needle.size()) {
        ++n;
    }
    return n;
}
} // namespace

TEST_CASE("metrics: exposition contains all locked metric names and TYPEs", "[metrics]") {
    Registry reg;
    reg.observe_ttft(lbl(), 0.05);
    reg.observe_itl(lbl(), 0.02);
    reg.observe_e2e(lbl(), 0.5);
    reg.observe_queue_time(lbl(), 0.001);
    reg.observe_overhead(lbl(), 0.0003);
    reg.inc_requests_total("200");
    reg.inc_affinity_spills(lbl(), 2);
    reg.inc_backend_failures(lbl(), 1);
    reg.set_requests_running(3);
    reg.set_requests_waiting(1);

    std::string out = reg.expose();

    // Locked names (colon form, per plan line 69 + findings Q6).
    CHECK(out.find("kvmux:ttft_seconds") != std::string::npos);
    CHECK(out.find("kvmux:inter_token_latency_seconds") != std::string::npos);
    CHECK(out.find("kvmux:e2e_request_latency_seconds") != std::string::npos);
    CHECK(out.find("kvmux:request_queue_time_seconds") != std::string::npos);
    CHECK(out.find("kvmux:gateway_overhead_seconds") != std::string::npos);
    CHECK(out.find("kvmux:requests_running") != std::string::npos);
    CHECK(out.find("kvmux:requests_waiting") != std::string::npos);
    CHECK(out.find("kvmux:requests_total") != std::string::npos);
    CHECK(out.find("kvmux:affinity_spills_total") != std::string::npos);
    CHECK(out.find("kvmux:backend_failures_total") != std::string::npos);

    // TYPE headers present.
    CHECK(out.find("# TYPE kvmux:ttft_seconds histogram") != std::string::npos);
    CHECK(out.find("# TYPE kvmux:requests_running gauge") != std::string::npos);
    CHECK(out.find("# TYPE kvmux:requests_total counter") != std::string::npos);

    // Labels present on a histogram series.
    CHECK(out.find("route=\"chat_completions\"") != std::string::npos);
    CHECK(out.find("backend=\"b0\"") != std::string::npos);
    CHECK(out.find("model=\"m\"") != std::string::npos);

    // Counter values.
    CHECK(out.find("kvmux:requests_total{code=\"200\"} 1") != std::string::npos);
    CHECK(out.find("kvmux:requests_running 3") != std::string::npos);
    CHECK(out.find("kvmux:requests_waiting 1") != std::string::npos);
}

TEST_CASE("metrics: histogram buckets are cumulative with +Inf, _sum, _count", "[metrics]") {
    Registry reg;
    // Three observations into the TTFT histogram.
    reg.observe_ttft(lbl(), 0.005); // <= 0.005 bucket
    reg.observe_ttft(lbl(), 0.05);  // <= 0.1 bucket
    reg.observe_ttft(lbl(), 100.0); // only +Inf
    std::string out = reg.expose();

    // +Inf bucket equals total count (3).
    CHECK(
        out.find("kvmux:ttft_seconds_bucket{route=\"chat_completions\",backend=\"b0\",model=\"m\","
                 "le=\"+Inf\"} 3") != std::string::npos);
    // _count is 3, _sum reflects the total.
    CHECK(
        out.find("kvmux:ttft_seconds_count{route=\"chat_completions\",backend=\"b0\",model=\"m\"} "
                 "3") != std::string::npos);
    CHECK(out.find("kvmux:ttft_seconds_sum") != std::string::npos);
    // The le="0.005" bucket has exactly 1 observation (cumulative).
    CHECK(out.find("le=\"0.005\"} 1") != std::string::npos);
}

TEST_CASE("metrics: distinct label sets produce distinct series", "[metrics]") {
    Registry reg;
    reg.observe_ttft(lbl("b0", "m"), 0.01);
    reg.observe_ttft(lbl("b1", "m"), 0.01);
    std::string out = reg.expose();
    CHECK(count_substr(out, "kvmux:ttft_seconds_count{") == 2); // one per backend
}

TEST_CASE("metrics: label values are escaped", "[metrics]") {
    Registry reg;
    reg.inc_backend_failures(Labels{"chat_completions", "weird\"name", "m\\x"}, 1);
    std::string out = reg.expose();
    CHECK(out.find("backend=\"weird\\\"name\"") != std::string::npos);
    CHECK(out.find("model=\"m\\\\x\"") != std::string::npos);
}
