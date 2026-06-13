#pragma once

// Metrics registry — the full kvmux Prometheus surface (locked names/buckets/
// labels, 03_PLAN.md + findings Q6). Handwritten text exposition, no dependency.
//
// Histograms (seconds, OTel GenAI-compatible buckets):
//   kvmux:ttft_seconds                  — TTFT buckets
//   kvmux:inter_token_latency_seconds   — TPOT buckets
//   kvmux:e2e_request_latency_seconds   — request-duration buckets
//   kvmux:request_queue_time_seconds    — admission queue wait
//   kvmux:gateway_overhead_seconds      — admission -> first upstream byte
//                                         forwarded (the self-overhead metric)
// Gauges:
//   kvmux:requests_running              — in-flight requests
//   kvmux:requests_waiting              — queued (admission) requests
// Counters:
//   kvmux:requests_total{code}          — completed requests by HTTP code
//   kvmux:affinity_spills_total         — prefix-affinity load-guard spills
//   kvmux:backend_failures_total        — upstream request failures
//
// Labels: {route, backend, model}. route is the served route (e.g.
// "chat_completions"); backend is the chosen backend name; model is the
// requested model. Counters/histograms are label-set keyed; gauges are global.
//
// Thread-safety: the registry runs on the single gateway io_context but is also
// scraped from the same loop; all state is plain integers guarded by one mutex
// (scrape is infrequent and off the per-token hot path).

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace kvmux::metrics {

// Label set carried on histogram/counter observations.
struct Labels {
    std::string route;
    std::string backend;
    std::string model;

    bool operator<(const Labels& o) const {
        return std::tie(route, backend, model) < std::tie(o.route, o.backend, o.model);
    }
};

// A fixed-bucket cumulative histogram (Prometheus le-bucket semantics).
class Histogram {
  public:
    explicit Histogram(std::vector<double> upper_bounds);
    void observe(double value);
    // Prometheus text for one labeled series of this histogram.
    void write(std::string& out, const std::string& name, const std::string& label_str) const;

  private:
    std::vector<double> bounds_;        // ascending upper bounds (no +Inf; implicit)
    std::vector<std::uint64_t> counts_; // per-bucket cumulative-at-write counts
    std::uint64_t total_count_ = 0;
    double sum_ = 0.0;
};

class Registry {
  public:
    Registry();

    // --- Observations (called from the request path) ----------------------
    void observe_ttft(const Labels& l, double seconds);
    void observe_itl(const Labels& l, double seconds);
    void observe_e2e(const Labels& l, double seconds);
    void observe_queue_time(const Labels& l, double seconds);
    void observe_overhead(const Labels& l, double seconds);

    void inc_requests_total(const std::string& code);
    void inc_affinity_spills(const Labels& l, int n = 1);
    void inc_backend_failures(const Labels& l, int n = 1);

    void set_requests_running(int v);
    void set_requests_waiting(int v);

    // --- Exposition -------------------------------------------------------
    // Full Prometheus text exposition (with HELP/TYPE headers).
    std::string expose() const;

  private:
    static std::string label_str(const Labels& l);

    mutable std::mutex mu_;

    // Histograms keyed by label set.
    std::map<Labels, Histogram> ttft_;
    std::map<Labels, Histogram> itl_;
    std::map<Labels, Histogram> e2e_;
    std::map<Labels, Histogram> queue_;
    std::map<Labels, Histogram> overhead_;

    // Counters.
    std::map<std::string, std::uint64_t> requests_total_; // keyed by code
    std::map<Labels, std::uint64_t> affinity_spills_;
    std::map<Labels, std::uint64_t> backend_failures_;

    // Gauges (global).
    std::int64_t requests_running_ = 0;
    std::int64_t requests_waiting_ = 0;

    // Bucket templates.
    const std::vector<double> ttft_buckets_;
    const std::vector<double> tpot_buckets_;
    const std::vector<double> duration_buckets_;

    Histogram& hist_for(std::map<Labels, Histogram>& m, const Labels& l,
                        const std::vector<double>& buckets);
};

} // namespace kvmux::metrics
