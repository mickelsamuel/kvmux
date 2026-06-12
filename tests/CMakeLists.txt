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
