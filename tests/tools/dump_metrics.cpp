// Dumps a fully-populated kvmux Prometheus exposition to stdout. Used by the
// `metrics-promtool-check` CTest to validate the exposition format with
// `promtool check metrics` without needing a live server.

#include "metrics/registry.hpp"

#include <iostream>

int main() {
    using kvmux::metrics::Labels;
    kvmux::metrics::Registry reg;

    // Populate every metric family with at least two label sets so the
    // exposition exercises labeled series, buckets, +Inf, _sum, _count, gauges,
    // and counters.
    for (const char* backend : {"vllm-a", "llama-1"}) {
        Labels l{"chat_completions", backend, "Qwen/Qwen2.5-3B-Instruct"};
        reg.observe_ttft(l, 0.012);
        reg.observe_ttft(l, 0.4);
        reg.observe_itl(l, 0.03);
        reg.observe_itl(l, 0.12);
        reg.observe_e2e(l, 1.5);
        reg.observe_queue_time(l, 0.002);
        reg.observe_overhead(l, 0.0004);
        reg.inc_affinity_spills(l, 1);
        reg.inc_backend_failures(l, 1);
    }
    reg.inc_requests_total("200");
    reg.inc_requests_total("429");
    reg.inc_requests_total("503");
    reg.set_requests_running(2);
    reg.set_requests_waiting(1);

    std::cout << reg.expose();
    return 0;
}
