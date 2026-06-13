#pragma once

// Per-backend circuit breaker (locked spec, 03_PLAN.md):
//   * 5 consecutive request failures -> OPEN
//   * OPEN for 10 s, then a single half-open probe is admitted
//   * the probe's success closes the breaker; its failure re-opens it
// Both the failure threshold and the open window are config-overridable
// (BackendConfig::failure_threshold / open_ms).
//
// The breaker is consulted on the request path (allow_request) and updated on
// the request outcome (on_success / on_failure). It is safe to call from
// multiple coroutines: state transitions are guarded by a small mutex so the
// "single probe in half-open" invariant holds even under concurrency.

#include <chrono>
#include <mutex>

namespace kvmux::health {

enum class BreakerState { Closed, Open, HalfOpen };

const char* to_string(BreakerState state) noexcept;

class CircuitBreaker {
  public:
    using clock = std::chrono::steady_clock;

    CircuitBreaker(int failure_threshold, std::chrono::milliseconds open_window)
        : failure_threshold_(failure_threshold), open_window_(open_window) {}

    // Outcome of an admission decision: whether the request may proceed and, if
    // so, whether it is the lone half-open probe (its outcome decides the
    // breaker's fate).
    struct Decision {
        bool allowed = false;
        bool is_probe = false;
    };

    // Decide whether a request may be sent to this backend right now. CLOSED ->
    // always allowed. OPEN -> denied until the open window elapses, after which
    // exactly one caller is promoted to HALF_OPEN and allowed as the probe.
    // HALF_OPEN -> denied (the probe is already in flight).
    Decision allow_request();

    // Report a completed request. `is_probe` must be the value returned by the
    // matching allow_request() call.
    void on_success(bool is_probe);
    void on_failure(bool is_probe);

    // Abort an admitted probe WITHOUT recording success or failure: the request
    // could not actually be dispatched (e.g. the backend's local in-flight slot
    // was full), so the probe was not a real test of the backend. If this was the
    // half-open probe, return the breaker to OPEN and restart the open window so
    // a fresh probe is admitted later; a non-probe abort is a no-op. This must
    // NOT reset the consecutive-failure run (a capacity skip is not a success).
    void abort_probe(bool is_probe);

    // Current state (advances OPEN->time-eligible lazily for observability only;
    // does not consume the probe slot).
    BreakerState state() const;

    // Test/diagnostic accessors.
    int consecutive_failures() const;

  private:
    const int failure_threshold_;
    const std::chrono::milliseconds open_window_;

    mutable std::mutex mu_;
    BreakerState state_ = BreakerState::Closed;
    int consecutive_failures_ = 0;
    clock::time_point opened_at_{};
};

} // namespace kvmux::health
