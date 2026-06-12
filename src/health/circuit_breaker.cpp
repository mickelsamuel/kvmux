#include "health/circuit_breaker.hpp"

namespace kvmux::health {

const char* to_string(BreakerState state) noexcept {
    switch (state) {
    case BreakerState::Closed:
        return "closed";
    case BreakerState::Open:
        return "open";
    case BreakerState::HalfOpen:
        return "half_open";
    }
    return "unknown";
}

CircuitBreaker::Decision CircuitBreaker::allow_request() {
    std::lock_guard<std::mutex> lk(mu_);
    switch (state_) {
    case BreakerState::Closed:
        return {true, false};
    case BreakerState::Open:
        if (clock::now() - opened_at_ >= open_window_) {
            // Open window elapsed: promote exactly this caller to the probe.
            state_ = BreakerState::HalfOpen;
            return {true, true};
        }
        return {false, false};
    case BreakerState::HalfOpen:
        // A probe is already in flight; deny everyone else until it resolves.
        return {false, false};
    }
    return {false, false};
}

void CircuitBreaker::on_success(bool is_probe) {
    std::lock_guard<std::mutex> lk(mu_);
    // Any success closes the breaker and clears the failure run. (A success from
    // the probe is the canonical close; a success that races in while CLOSED
    // simply keeps it closed.)
    (void) is_probe;
    state_ = BreakerState::Closed;
    consecutive_failures_ = 0;
}

void CircuitBreaker::on_failure(bool is_probe) {
    std::lock_guard<std::mutex> lk(mu_);
    if (is_probe) {
        // The half-open probe failed: re-open and restart the window.
        state_ = BreakerState::Open;
        opened_at_ = clock::now();
        return;
    }
    if (state_ == BreakerState::Open) {
        // Already open; a late failure from an in-flight pre-open request does
        // not move the count.
        return;
    }
    if (++consecutive_failures_ >= failure_threshold_) {
        state_ = BreakerState::Open;
        opened_at_ = clock::now();
    }
}

BreakerState CircuitBreaker::state() const {
    std::lock_guard<std::mutex> lk(mu_);
    if (state_ == BreakerState::Open && clock::now() - opened_at_ >= open_window_) {
        // Window elapsed: observably half-open-eligible. Do not consume the
        // probe slot here — allow_request() does that atomically.
        return BreakerState::HalfOpen;
    }
    return state_;
}

int CircuitBreaker::consecutive_failures() const {
    std::lock_guard<std::mutex> lk(mu_);
    return consecutive_failures_;
}

} // namespace kvmux::health
