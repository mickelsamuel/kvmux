cmake_minimum_required(VERSION 3.28)

project(kvmux
    VERSION 0.1.0
    DESCRIPTION "Single-binary OpenAI-compatible LLM gateway"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Standard / global settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Sanitizer toggles (CI wires ASan+UBSan and TSan jobs through these).
option(KVMUX_ASAN "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
option(KVMUX_TSAN "Build with ThreadSanitizer" OFF)
option(KVMUX_BUILD_TESTS "Build the test suite" ON)

add_library(kvmux_warnings INTERFACE)
target_compile_options(kvmux_warnings INTERFACE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion)

add_library(kvmux_sanitizers INTERFACE)
if(KVMUX_ASAN)
    target_compile_options(kvmux_sanitizers INTERFACE
        -fsanitize=address,undefined -fno-omit-frame-pointer -g)
    target_link_options(kvmux_sanitizers INTERFACE -fsanitize=address,undefined)
endif()
if(KVMUX_TSAN)
    target_compile_options(kvmux_sanitizers INTERFACE
        -fsanitize=thread -fno-omit-frame-pointer -g)
    target_link_options(kvmux_sanitizers INTERFACE -fsanitize=thread)
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
find_package(Boost 1.83 REQUIRED CONFIG COMPONENTS system)
find_package(Threads REQUIRED)

include(FetchContent)

# nlohmann/json — JSON serialization (hot-path cost measured in M5).
FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
    GIT_SHALLOW TRUE)

# toml++ — config parsing.
FetchContent_Declare(tomlplusplus
    GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
    GIT_TAG v3.4.0
    GIT_SHALLOW TRUE)

# xxHash — XXH64 for prefix-affinity keys (single-header use).
FetchContent_Declare(xxhash
    GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
    GIT_TAG v0.8.2
    GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(nlohmann_json tomlplusplus xxhash)

# xxHash ships no CMake target we want; expose its header dir.
add_library(xxhash_header INTERFACE)
target_include_directories(xxhash_header INTERFACE ${xxhash_SOURCE_DIR})

# ---------------------------------------------------------------------------
# Core library (everything but main(), so tests can link against it)
# ---------------------------------------------------------------------------
set(KVMUX_SOURCES
    src/config.cpp
    src/openai/errors.cpp
    src/upstream/sse_parser.cpp
    src/upstream/client.cpp
    src/server/sse_writer.cpp
    src/server/gateway.cpp
    src/server/listener.cpp
)

add_library(kvmux_core STATIC ${KVMUX_SOURCES})
target_include_directories(kvmux_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kvmux_core
    PUBLIC
        Boost::system
        Threads::Threads
        nlohmann_json::nlohmann_json
        tomlplusplus::tomlplusplus
        xxhash_header
    PRIVATE
        kvmux_warnings
        kvmux_sanitizers)

# ---------------------------------------------------------------------------
# Main binary
# ---------------------------------------------------------------------------
add_executable(kvmux src/main.cpp)
target_link_libraries(kvmux
    PRIVATE
        kvmux_core
        kvmux_warnings
        kvmux_sanitizers)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(KVMUX_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
