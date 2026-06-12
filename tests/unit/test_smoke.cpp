#include "config.hpp"

#include <catch2/catch_test_macros.hpp>

// M0 smoke test: proves Catch2 is wired and the core library links. The real
// behavioral tests live in the other files.
TEST_CASE("smoke: backend-type stringification", "[smoke]") {
    using kvmux::config::BackendType;
    CHECK(std::string(to_string(BackendType::Vllm)) == "vllm");
    CHECK(std::string(to_string(BackendType::Llamacpp)) == "llamacpp");
    CHECK(std::string(to_string(BackendType::Ollama)) == "ollama");
}
