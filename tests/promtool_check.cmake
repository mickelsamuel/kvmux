# Runs: dump_metrics > file ; promtool check metrics < file
#
# The locked kvmux metric names use the vLLM-style `kvmux:` colon prefix (plan
# line 69 + findings Q6). Prometheus reserves ':' in metric names for recording-
# rule outputs, so `promtool check metrics` emits one advisory per colon metric:
#   "<name> metric names should not contain ':'".
# Whether that advisory is fatal depends on how stderr is captured: exit 0 when
# stderr is a terminal/file, exit 3 when stderr is captured as a pipe (CI / this
# CMake execute_process). See ARCHITECT-RULINGS.md E4 — a naming ruling is open.
#
# This test therefore validates what is unambiguous and locked: the exposition
# is STRUCTURALLY VALID Prometheus text, and the ONLY promtool finding is the
# colon-name advisory (no malformed series, bad buckets, type mismatches, etc.).
# It does NOT silently rename the locked metrics. If the architect rules for
# underscore names, flip src/metrics/registry.cpp and this test passes on exit 0.

set(tmp "${OUT_DIR}/kvmux_metrics_exposition.prom")

execute_process(
    COMMAND "${DUMP_EXE}"
    OUTPUT_FILE "${tmp}"
    RESULT_VARIABLE dump_rc)
if(NOT dump_rc EQUAL 0)
    message(FATAL_ERROR "dump_metrics failed with code ${dump_rc}")
endif()

execute_process(
    COMMAND "${PROMTOOL}" check metrics
    INPUT_FILE "${tmp}"
    OUTPUT_VARIABLE pt_out
    ERROR_VARIABLE pt_err
    RESULT_VARIABLE pt_rc)

message(STATUS "promtool exit: ${pt_rc}")
message(STATUS "promtool stdout: ${pt_out}")
message(STATUS "promtool stderr: ${pt_err}")

# Clean exit -> fully accepted (happens with underscore names, or colon names
# when stderr is not pipe-captured).
if(pt_rc EQUAL 0)
    message(STATUS "promtool check metrics: PASS (exit 0)")
    return()
endif()

# Otherwise: accept iff EVERY non-empty stderr line is the known colon advisory.
# Any other finding (a real format error) fails the test.
string(REPLACE "\n" ";" pt_lines "${pt_err}")
set(unexpected "")
foreach(line IN LISTS pt_lines)
    string(STRIP "${line}" line)
    if(line STREQUAL "")
        continue()
    endif()
    if(line MATCHES "metric names should not contain ':'")
        continue()
    endif()
    list(APPEND unexpected "${line}")
endforeach()

if(unexpected)
    message(FATAL_ERROR
        "promtool reported findings beyond the known colon-name advisory:\n${unexpected}")
endif()

message(STATUS
    "promtool check metrics: structurally valid; only the locked-colon-name advisory was reported "
    "(promtool exit ${pt_rc} under pipe-captured stderr). Naming decision tracked as E4.")
