#pragma once

// Admission control (locked spec, 03_PLAN.md).
//
//   * Global ceiling: max_concurrent_streams in-flight requests across the
//     whole gateway.
//   * Per-backend ceiling: max_in_flight (enforced by the routing/eligibility
//     path via PerBackendLimiter, which the gateway consults when picking a
//     backend).
//   * When the global ceiling is reached, new requests join a bounded FIFO
//     wait-queue of depth queue_depth; each queued request waits at most
//     queue_timeout_ms. A full queue or a wait-timeout yields a 429 with
//     Retry-After: 1. Queue time is recorded for kvmux:request_queue_time_seconds.
//
// The global gate is an async FIFO counting semaphore over the io_context: a
// waiter that cannot get a slot immediately suspends on a per-waiter timer that
// is cancelled when a slot frees up. FIFO order is preserved by a queue of
// waiters served oldest-first on release.

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>

namespace kvmux::admission {

// Per-backend in-flight counter (max_in_flight). Atomic so health/metrics can
// read it; mutation is via try_acquire/release. Also feeds the prefix-affinity
// load guard (active_requests vs load_threshold * max_in_flight) in M4.
class PerBackendLimiter {
  public:
    explicit PerBackendLimiter(int max_in_flight) : max_in_flight_(max_in_flight) {}

    // Reserve a slot if below the cap. Returns true on success.
    bool try_acquire() noexcept {
        int cur = active_.load(std::memory_order_relaxed);
        while (cur < max_in_flight_) {
            if (active_.compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    void release() noexcept { active_.fetch_sub(1, std::memory_order_acq_rel); }

    int active() const noexcept { return active_.load(std::memory_order_relaxed); }
    int max_in_flight() const noexcept { return max_in_flight_; }
    bool at_capacity() const noexcept { return active() >= max_in_flight_; }

  private:
    const int max_in_flight_;
    std::atomic<int> active_{0};
};

// RAII holder for a per-backend slot.
class BackendSlot {
  public:
    BackendSlot() = default;
    explicit BackendSlot(PerBackendLimiter& lim) : lim_(&lim) {}
    BackendSlot(BackendSlot&& o) noexcept : lim_(o.lim_) { o.lim_ = nullptr; }
    BackendSlot& operator=(BackendSlot&& o) noexcept {
        if (this != &o) {
            reset();
            lim_ = o.lim_;
            o.lim_ = nullptr;
        }
        return *this;
    }
    BackendSlot(const BackendSlot&) = delete;
    BackendSlot& operator=(const BackendSlot&) = delete;
    ~BackendSlot() { reset(); }
    void reset() {
        if (lim_) {
            lim_->release();
            lim_ = nullptr;
        }
    }
    bool held() const noexcept { return lim_ != nullptr; }

  private:
    PerBackendLimiter* lim_ = nullptr;
};

// Outcome of an admission attempt.
enum class AdmitStatus {
    Admitted,    // a global slot was acquired (immediately or after queueing)
    QueueFull,   // the wait-queue was already at queue_depth -> 429
    QueueTimeout // waited queue_timeout_ms without a slot -> 429
};

struct AdmitResult {
    AdmitStatus status = AdmitStatus::QueueFull;
    std::chrono::nanoseconds queued_for{0}; // time spent waiting for a slot
    bool admitted() const noexcept { return status == AdmitStatus::Admitted; }
};

// Global async FIFO semaphore with a bounded wait-queue. Single-threaded with
// respect to one io_context (kvmux runs one io_context); all mutation happens
// on that executor, so no internal locking is needed.
class GlobalGate {
  public:
    GlobalGate(boost::asio::io_context& ioc, int max_concurrent, int queue_depth,
               std::chrono::milliseconds queue_timeout);

    // Try to acquire a global slot. Returns immediately if one is free; else
    // queues (FIFO) up to queue_depth, waiting up to queue_timeout. The caller
    // must call release() exactly once for every Admitted result.
    boost::asio::awaitable<AdmitResult> acquire();

    // Release a previously acquired slot, waking the oldest waiter if any.
    void release();

    int in_flight() const noexcept { return in_flight_; }
    int waiting() const noexcept { return static_cast<int>(waiters_.size()); }

  private:
    struct Waiter {
        boost::asio::steady_timer timer;
        bool granted = false;
        explicit Waiter(const boost::asio::any_io_executor& ex) : timer(ex) {}
    };

    boost::asio::io_context& ioc_;
    const int max_concurrent_;
    const int queue_depth_;
    const std::chrono::milliseconds queue_timeout_;
    int in_flight_ = 0;
    std::deque<std::shared_ptr<Waiter>> waiters_;
};

// RAII ticket: releases the global slot when it goes out of scope. Move-only.
class Ticket {
  public:
    Ticket() = default;
    explicit Ticket(GlobalGate& gate) : gate_(&gate) {}
    Ticket(Ticket&& o) noexcept : gate_(o.gate_) { o.gate_ = nullptr; }
    Ticket& operator=(Ticket&& o) noexcept {
        if (this != &o) {
            reset();
            gate_ = o.gate_;
            o.gate_ = nullptr;
        }
        return *this;
    }
    Ticket(const Ticket&) = delete;
    Ticket& operator=(const Ticket&) = delete;
    ~Ticket() { reset(); }

    void reset() {
        if (gate_) {
            gate_->release();
            gate_ = nullptr;
        }
    }

  private:
    GlobalGate* gate_ = nullptr;
};

} // namespace kvmux::admission
