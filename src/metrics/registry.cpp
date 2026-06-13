#include "metrics/registry.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>

// DELIBERATE NAMING CONVENTION (do not "fix"): kvmux metric names are
// colon-prefixed (kvmux:ttft_seconds, ...) to mirror vLLM's / SGLang's de-facto
// serving-metric naming (vllm:..., findings Q6) so kvmux drops into existing
// GenAI dashboards. `promtool check metrics` prints a "metric names should not
// contain ':'" advisory to stderr but exits 0 — that exit-0 is the acceptance
// bar (see the metrics-promtool-check CTest); the advisory is an intentional
// ecosystem trade-off, not a defect. Architect ruling E4 (2026-06-12).

namespace kvmux::metrics {

namespace {

// OTel GenAI / vLLM TTFT buckets (identical sets — findings Q6).
const std::vector<double> kTtftBuckets = {0.001, 0.005, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1,
                                          0.25,  0.5,   0.75, 1.0,  2.5,  5.0,  7.5,  10.0};
// OTel time_per_output_token / vLLM inter_token_latency buckets.
const std::vector<double> kTpotBuckets = {0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2,
                                          0.3,  0.4,   0.5,  0.75,  1.0, 2.5};
// OTel request.duration buckets (also used for queue-time and overhead so the
// whole latency family shares an axis).
const std::vector<double> kDurationBuckets = {0.01, 0.02, 0.04, 0.08,  0.16,  0.32,  0.64,
                                              1.28, 2.56, 5.12, 10.24, 20.48, 40.96, 81.92};

// Escape a Prometheus label value (\, ", and newline).
std::string esc(const std::string& v) {
    std::string out;
    out.reserve(v.size());
    for (char c : v) {
        switch (c) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        default:
            out += c;
        }
    }
    return out;
}

std::string fmt_double(double d) {
    if (std::isinf(d)) {
        return d > 0 ? "+Inf" : "-Inf";
    }
    std::ostringstream os;
    os << d;
    return os.str();
}

} // namespace

Histogram::Histogram(std::vector<double> upper_bounds)
    : bounds_(std::move(upper_bounds)), counts_(bounds_.size(), 0) {}

void Histogram::observe(double value) {
    sum_ += value;
    ++total_count_;
    for (std::size_t i = 0; i < bounds_.size(); ++i) {
        if (value <= bounds_[i]) {
            ++counts_[i];
        }
    }
}

void Histogram::write(std::string& out, const std::string& name,
                      const std::string& label_str) const {
    // Cumulative le-buckets, then +Inf, _sum, _count.
    auto with_le = [&](const std::string& le) {
        // Insert le into the existing label set (label_str is "{a="b"}" or "").
        if (label_str.empty() || label_str == "{}") {
            return std::string("{le=\"") + le + "\"}";
        }
        std::string inner = label_str.substr(1, label_str.size() - 2); // strip { }
        return std::string("{") + inner + ",le=\"" + le + "\"}";
    };
    for (std::size_t i = 0; i < bounds_.size(); ++i) {
        out += name + "_bucket" + with_le(fmt_double(bounds_[i])) + " " +
               std::to_string(counts_[i]) + "\n";
    }
    out += name + "_bucket" + with_le("+Inf") + " " + std::to_string(total_count_) + "\n";
    out += name + "_sum" + label_str + " " + fmt_double(sum_) + "\n";
    out += name + "_count" + label_str + " " + std::to_string(total_count_) + "\n";
}

Registry::Registry()
    : ttft_buckets_(kTtftBuckets), tpot_buckets_(kTpotBuckets),
      duration_buckets_(kDurationBuckets) {}

std::string Registry::label_str(const Labels& l) {
    // Always emit the three locked labels (empty string when unset is fine, but
    // we keep them present so series are stable).
    std::string s = "{route=\"";
    s += esc(l.route);
    s += "\",backend=\"";
    s += esc(l.backend);
    s += "\",model=\"";
    s += esc(l.model);
    s += "\"}";
    return s;
}

Histogram& Registry::hist_for(std::map<Labels, Histogram>& m, const Labels& l,
                              const std::vector<double>& buckets) {
    auto it = m.find(l);
    if (it == m.end()) {
        it = m.emplace(l, Histogram(buckets)).first;
    }
    return it->second;
}

void Registry::observe_ttft(const Labels& l, double seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    hist_for(ttft_, l, ttft_buckets_).observe(seconds);
}
void Registry::observe_itl(const Labels& l, double seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    hist_for(itl_, l, tpot_buckets_).observe(seconds);
}
void Registry::observe_e2e(const Labels& l, double seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    hist_for(e2e_, l, duration_buckets_).observe(seconds);
}
void Registry::observe_queue_time(const Labels& l, double seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    hist_for(queue_, l, duration_buckets_).observe(seconds);
}
void Registry::observe_overhead(const Labels& l, double seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    hist_for(overhead_, l, duration_buckets_).observe(seconds);
}

void Registry::inc_requests_total(const std::string& code) {
    std::lock_guard<std::mutex> lk(mu_);
    ++requests_total_[code];
}
void Registry::inc_affinity_spills(const Labels& l, int n) {
    std::lock_guard<std::mutex> lk(mu_);
    affinity_spills_[l] += static_cast<std::uint64_t>(n);
}
void Registry::inc_backend_failures(const Labels& l, int n) {
    std::lock_guard<std::mutex> lk(mu_);
    backend_failures_[l] += static_cast<std::uint64_t>(n);
}
void Registry::set_requests_running(int v) {
    std::lock_guard<std::mutex> lk(mu_);
    requests_running_ = v;
}
void Registry::set_requests_waiting(int v) {
    std::lock_guard<std::mutex> lk(mu_);
    requests_waiting_ = v;
}

std::string Registry::expose() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::string out;
    out.reserve(4096);

    auto hist_block = [&](const std::string& name, const std::string& help,
                          const std::map<Labels, Histogram>& m) {
        out += "# HELP " + name + " " + help + "\n";
        out += "# TYPE " + name + " histogram\n";
        for (const auto& [labels, h] : m) {
            h.write(out, name, label_str(labels));
        }
    };

    hist_block("kvmux:ttft_seconds", "Time to first token, seconds.", ttft_);
    hist_block("kvmux:inter_token_latency_seconds",
               "Inter-token (time-per-output-token) latency, seconds.", itl_);
    hist_block("kvmux:e2e_request_latency_seconds", "End-to-end request latency, seconds.", e2e_);
    hist_block("kvmux:request_queue_time_seconds", "Admission queue wait time, seconds.", queue_);
    hist_block("kvmux:gateway_overhead_seconds",
               "Gateway self-overhead: admission to first upstream byte forwarded, seconds.",
               overhead_);

    // Gauges.
    out += "# HELP kvmux:requests_running In-flight requests.\n";
    out += "# TYPE kvmux:requests_running gauge\n";
    out += "kvmux:requests_running " + std::to_string(requests_running_) + "\n";
    out += "# HELP kvmux:requests_waiting Requests waiting in the admission queue.\n";
    out += "# TYPE kvmux:requests_waiting gauge\n";
    out += "kvmux:requests_waiting " + std::to_string(requests_waiting_) + "\n";

    // Counters.
    out += "# HELP kvmux:requests_total Completed requests by HTTP status code.\n";
    out += "# TYPE kvmux:requests_total counter\n";
    for (const auto& [code, n] : requests_total_) {
        out += "kvmux:requests_total{code=\"" + esc(code) + "\"} " + std::to_string(n) + "\n";
    }
    out += "# HELP kvmux:affinity_spills_total Prefix-affinity load-guard spill cascades.\n";
    out += "# TYPE kvmux:affinity_spills_total counter\n";
    for (const auto& [labels, n] : affinity_spills_) {
        out += "kvmux:affinity_spills_total" + label_str(labels) + " " + std::to_string(n) + "\n";
    }
    out += "# HELP kvmux:backend_failures_total Upstream request failures.\n";
    out += "# TYPE kvmux:backend_failures_total counter\n";
    for (const auto& [labels, n] : backend_failures_) {
        out += "kvmux:backend_failures_total" + label_str(labels) + " " + std::to_string(n) + "\n";
    }

    return out;
}

} // namespace kvmux::metrics
