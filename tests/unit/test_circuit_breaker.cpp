#include "health/circuit_breaker.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>

using kvmux::health::BreakerState;
using kvmux::health::CircuitBreaker;
using namespace std::chrono_literals;

TEST_CASE("breaker: closed allows requests and successes keep it closed", "[breaker]") {
    CircuitBreaker cb(5, 10s);
    for (int i = 0; i < 20; ++i) {
        auto d = cb.allow_request();
        CHECK(d.allowed);
        CHECK_FALSE(d.is_probe);
        cb.on_success(d.is_probe);
    }
    CHECK(cb.state() == BreakerState::Closed);
}

TEST_CASE("breaker: 5 consecutive failures open it; the 5th is the trip", "[breaker]") {
    CircuitBreaker cb(5, 10s);
    for (int i = 0; i < 4; ++i) {
        auto d = cb.allow_request();
        REQUIRE(d.allowed);
        cb.on_failure(d.is_probe);
        CHECK(cb.state() == BreakerState::Closed); // not yet
    }
    auto d5 = cb.allow_request();
    REQUIRE(d5.allowed);
    cb.on_failure(d5.is_probe);
    CHECK(cb.state() == BreakerState::Open);

    // While open, requests are denied.
    auto denied = cb.allow_request();
    CHECK_FALSE(denied.allowed);
}

TEST_CASE("breaker: a success resets the consecutive-failure run", "[breaker]") {
    CircuitBreaker cb(5, 10s);
    for (int i = 0; i < 4; ++i) {
        auto d = cb.allow_request();
        cb.on_failure(d.is_probe);
    }
    CHECK(cb.consecutive_failures() == 4);
    auto ok = cb.allow_request();
    cb.on_success(ok.is_probe);
    CHECK(cb.consecutive_failures() == 0);
    // Four more failures must NOT trip (run was reset).
    for (int i = 0; i < 4; ++i) {
        auto d = cb.allow_request();
        cb.on_failure(d.is_probe);
    }
    CHECK(cb.state() == BreakerState::Closed);
}

TEST_CASE("breaker: after the open window a single half-open probe is admitted", "[breaker]") {
    CircuitBreaker cb(2, 50ms); // tiny window for the test
    for (int i = 0; i < 2; ++i) {
        auto d = cb.allow_request();
        cb.on_failure(d.is_probe);
    }
    REQUIRE(cb.state() == BreakerState::Open);
    CHECK_FALSE(cb.allow_request().allowed); // still open, window not elapsed

    std::this_thread::sleep_for(70ms);
    // Exactly one caller is promoted to the probe.
    auto probe = cb.allow_request();
    CHECK(probe.allowed);
    CHECK(probe.is_probe);
    // A second concurrent caller is denied while the probe is in flight.
    CHECK_FALSE(cb.allow_request().allowed);
}

TEST_CASE("breaker: probe success closes the breaker", "[breaker]") {
    CircuitBreaker cb(2, 50ms);
    for (int i = 0; i < 2; ++i) {
        auto d = cb.allow_request();
        cb.on_failure(d.is_probe);
    }
    std::this_thread::sleep_for(70ms);
    auto probe = cb.allow_request();
    REQUIRE(probe.is_probe);
    cb.on_success(probe.is_probe);
    CHECK(cb.state() == BreakerState::Closed);
    CHECK(cb.allow_request().allowed); // fully reopened to traffic
}

TEST_CASE("breaker: probe failure re-opens the breaker and restarts the window", "[breaker]") {
    CircuitBreaker cb(2, 50ms);
    for (int i = 0; i < 2; ++i) {
        auto d = cb.allow_request();
        cb.on_failure(d.is_probe);
    }
    std::this_thread::sleep_for(70ms);
    auto probe = cb.allow_request();
    REQUIRE(probe.is_probe);
    cb.on_failure(probe.is_probe);
    CHECK(cb.state() == BreakerState::Open);
    CHECK_FALSE(cb.allow_request().allowed); // window restarted; denied again
}

TEST_CASE("breaker: config-overridable threshold respected", "[breaker]") {
    CircuitBreaker cb(1, 10s); // trips on the first failure
    auto d = cb.allow_request();
    cb.on_failure(d.is_probe);
    CHECK(cb.state() == BreakerState::Open);
}
