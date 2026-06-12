#include "admission/controller.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <vector>

namespace asio = boost::asio;
using namespace std::chrono_literals;
using kvmux::admission::AdmitStatus;
using kvmux::admission::BackendSlot;
using kvmux::admission::GlobalGate;
using kvmux::admission::PerBackendLimiter;

// --- PerBackendLimiter (synchronous) ---------------------------------------

TEST_CASE("admission: per-backend limiter caps in-flight at max_in_flight", "[admission]") {
    PerBackendLimiter lim(2);
    CHECK(lim.try_acquire());
    CHECK(lim.try_acquire());
    CHECK(lim.active() == 2);
    CHECK(lim.at_capacity());
    CHECK_FALSE(lim.try_acquire()); // full
    lim.release();
    CHECK(lim.active() == 1);
    CHECK(lim.try_acquire());
}

TEST_CASE("admission: BackendSlot releases on scope exit", "[admission]") {
    PerBackendLimiter lim(1);
    REQUIRE(lim.try_acquire());
    {
        BackendSlot s(lim);
        CHECK(s.held());
        CHECK(lim.active() == 1);
    }
    CHECK(lim.active() == 0);
}

// --- GlobalGate (async, FIFO, bounded queue, timeout) -----------------------

TEST_CASE("admission: global gate admits immediately when slots are free", "[admission]") {
    asio::io_context ioc;
    GlobalGate gate(ioc, /*max=*/2, /*queue=*/4, 1000ms);
    AdmitStatus s1{}, s2{};
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            s1 = (co_await gate.acquire()).status;
            s2 = (co_await gate.acquire()).status;
            co_return;
        },
        asio::detached);
    ioc.run();
    CHECK(s1 == AdmitStatus::Admitted);
    CHECK(s2 == AdmitStatus::Admitted);
    CHECK(gate.in_flight() == 2);
}

TEST_CASE("admission: queue-full request is rejected (429 path)", "[admission]") {
    asio::io_context ioc;
    GlobalGate gate(ioc, /*max=*/1, /*queue=*/1, 5000ms);
    AdmitStatus admitted{}, queued_full{};
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            admitted = (co_await gate.acquire()).status; // takes the only slot
            // queue depth 1: first waiter queues; we add it then try a second
            // that must see QueueFull. Spawn a long-lived waiter first.
            co_return;
        },
        asio::detached);

    // A waiter that occupies the single queue slot (and never gets served).
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            (void) co_await gate.acquire();
            co_return;
        },
        asio::detached);

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            // Let the first two coroutines run to occupy slot+queue.
            asio::steady_timer t(co_await asio::this_coro::executor);
            t.expires_after(50ms);
            co_await t.async_wait(asio::use_awaitable);
            queued_full = (co_await gate.acquire()).status; // queue already full
            co_return;
        },
        asio::detached);

    ioc.run_for(1s);
    CHECK(admitted == AdmitStatus::Admitted);
    CHECK(queued_full == AdmitStatus::QueueFull);
}

TEST_CASE("admission: queued request times out -> QueueTimeout", "[admission]") {
    asio::io_context ioc;
    GlobalGate gate(ioc, /*max=*/1, /*queue=*/4, /*timeout=*/80ms);
    AdmitStatus first{}, queued{};
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            first = (co_await gate.acquire()).status; // holds the slot, never releases
            co_return;
        },
        asio::detached);
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto r = co_await gate.acquire(); // must time out waiting
            queued = r.status;
            co_return;
        },
        asio::detached);
    ioc.run_for(1s);
    CHECK(first == AdmitStatus::Admitted);
    CHECK(queued == AdmitStatus::QueueTimeout);
}

TEST_CASE("admission: releasing a slot wakes the oldest waiter (FIFO)", "[admission]") {
    asio::io_context ioc;
    GlobalGate gate(ioc, /*max=*/1, /*queue=*/4, 5000ms);
    std::vector<int> served_order;

    auto worker = [&](int id, bool release_after) -> asio::awaitable<void> {
        auto r = co_await gate.acquire();
        if (r.admitted()) {
            served_order.push_back(id);
            if (release_after) {
                gate.release();
            }
        }
        co_return;
    };

    // id 0 grabs the slot and releases (waking the FIFO-oldest waiter).
    asio::co_spawn(ioc, worker(0, true), asio::detached);
    // ids 1,2,3 queue in order; FIFO means 1 is served first when 0 releases.
    asio::co_spawn(ioc, worker(1, true), asio::detached);
    asio::co_spawn(ioc, worker(2, true), asio::detached);
    asio::co_spawn(ioc, worker(3, true), asio::detached);

    ioc.run_for(1s);
    REQUIRE(served_order.size() == 4);
    CHECK(served_order[0] == 0);
    CHECK(served_order[1] == 1);
    CHECK(served_order[2] == 2);
    CHECK(served_order[3] == 3);
}
