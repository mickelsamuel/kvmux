#include "admission/controller.hpp"

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace kvmux::admission {

namespace asio = boost::asio;

GlobalGate::GlobalGate(asio::io_context& ioc, int max_concurrent, int queue_depth,
                       std::chrono::milliseconds queue_timeout)
    : ioc_(ioc), max_concurrent_(max_concurrent), queue_depth_(queue_depth),
      queue_timeout_(queue_timeout) {}

asio::awaitable<AdmitResult> GlobalGate::acquire() {
    AdmitResult result;

    // Fast path: a slot is free and nobody is waiting (FIFO fairness — never
    // jump the queue ahead of a waiter).
    if (in_flight_ < max_concurrent_ && waiters_.empty()) {
        ++in_flight_;
        result.status = AdmitStatus::Admitted;
        co_return result;
    }

    // No immediate slot: join the wait-queue if there is room.
    if (static_cast<int>(waiters_.size()) >= queue_depth_) {
        result.status = AdmitStatus::QueueFull;
        co_return result;
    }

    auto executor = co_await asio::this_coro::executor;
    auto waiter = std::make_shared<Waiter>(executor);
    waiter->timer.expires_after(queue_timeout_);
    waiters_.push_back(waiter);

    const auto start = std::chrono::steady_clock::now();
    boost::system::error_code ec;
    co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
    result.queued_for = std::chrono::steady_clock::now() - start;

    if (waiter->granted) {
        // release() cancelled our timer and handed us the slot (already counted
        // in in_flight_ by release()).
        result.status = AdmitStatus::Admitted;
        co_return result;
    }

    // Timer fired without a grant: queue timeout. Remove ourselves from the
    // queue (we may still be in it).
    for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
        if (it->get() == waiter.get()) {
            waiters_.erase(it);
            break;
        }
    }
    result.status = AdmitStatus::QueueTimeout;
    co_return result;
}

void GlobalGate::release() {
    // Hand the freed slot to the oldest live waiter, if any; otherwise lower the
    // in-flight count. The slot count stays conserved: an admitted waiter keeps
    // the slot we are releasing.
    while (!waiters_.empty()) {
        auto waiter = waiters_.front();
        waiters_.pop_front();
        // The grant transfers our slot directly to the waiter; in_flight_ is
        // unchanged (one in, one out).
        waiter->granted = true;
        waiter->timer.cancel();
        return;
    }
    --in_flight_;
}

} // namespace kvmux::admission
