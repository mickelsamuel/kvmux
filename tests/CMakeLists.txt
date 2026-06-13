# Catch2 v3 via FetchContent.
include(FetchContent)
FetchContent_Declare(Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG v3.5.4
    GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(Catch2)

list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
include(Catch)

add_executable(kvmux_tests
    unit/test_smoke.cpp
    unit/test_config.cpp
    unit/test_openai_types.cpp
    unit/test_errors.cpp
    unit/test_sse_parser.cpp
    unit/test_sse_fixtures.cpp
    unit/test_circuit_breaker.cpp
    unit/test_health.cpp
    unit/test_admission.cpp
    unit/test_round_robin.cpp
    unit/test_prefix_affinity.cpp
    unit/test_metrics.cpp
)

target_link_libraries(kvmux_tests
    PRIVATE
        kvmux_core
        kvmux_warnings
        kvmux_sanitizers
        Catch2::Catch2WithMain)

# Make the recorded SSE fixtures locatable from the test binary regardless of
# the working directory.
target_compile_definitions(kvmux_tests PRIVATE
    KVMUX_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")

catch_discover_tests(kvmux_tests)

# Integration tests: in-process mock upstream + real Gateway, driven over real
# sockets. Hermetic (no Python/network deps), so they run in CI under ASan/TSan.
add_executable(kvmux_integration_tests
    integration/test_proxy.cpp
    integration/test_multibackend.cpp
    integration/test_routing.cpp
)
target_link_libraries(kvmux_integration_tests
    PRIVATE
        kvmux_core
        kvmux_warnings
        kvmux_sanitizers
        Catch2::Catch2WithMain)
catch_discover_tests(kvmux_integration_tests)

# ---------------------------------------------------------------------------
# Metrics exposition validation: dump a fully-populated Prometheus exposition
# and pipe it through `promtool check metrics`. The locked metric names use the
# vLLM-style `kvmux:` colon prefix; promtool exits 0 on them (the colon advisory
# is a stderr lint note, not a failure). The test asserts that exit-0 contract.
# Skipped cleanly when promtool is not installed on the runner.
# ---------------------------------------------------------------------------
add_executable(kvmux_dump_metrics tools/dump_metrics.cpp)
target_link_libraries(kvmux_dump_metrics
    PRIVATE kvmux_core kvmux_warnings kvmux_sanitizers)

find_program(PROMTOOL_EXE promtool)
if(PROMTOOL_EXE)
    add_test(
        NAME metrics-promtool-check
        COMMAND ${CMAKE_COMMAND}
            -DDUMP_EXE=$<TARGET_FILE:kvmux_dump_metrics>
            -DPROMTOOL=${PROMTOOL_EXE}
            -DOUT_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/promtool_check.cmake)
else()
    message(STATUS "promtool not found; metrics-promtool-check test will be skipped")
endif()
