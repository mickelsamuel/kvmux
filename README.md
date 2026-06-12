# kvmux

[![ci](https://github.com/mickelsamuel/kvmux/actions/workflows/ci.yml/badge.svg)](https://github.com/mickelsamuel/kvmux/actions/workflows/ci.yml)

**A single-binary, OpenAI-compatible LLM gateway in C++20.** *(Building in public — status below.)*

When you self-host inference, you rarely run one engine. A typical setup mixes a
vLLM box, a llama.cpp-server, and an Ollama instance, and the app in front of
them still needs one OpenAI-compatible endpoint, SSE streaming, failover when a
backend dies, and a way to keep multi-turn conversations landing on the backend
that already has their prefix cached. `kvmux` is that gateway: one binary, no
Kubernetes, fronting heterogeneous self-hosted backends, with prefix-affinity
routing and per-route latency instrumentation.

It is written in C++20 because that's where the author's systems depth is, and
because the proxy hot path — re-streaming tokens with per-frame flush — is worth
controlling directly. That's the honest reason; this isn't a claim that the job
*requires* C++.

## What it does

- OpenAI-compatible HTTP API (`/v1/chat/completions` streaming and non-streaming,
  `/v1/models`), SSE token streaming with per-frame flush for inter-token fidelity
- Heterogeneous backends behind one endpoint: **vLLM + llama.cpp-server + Ollama**
- Prefix-affinity routing: same-prefix requests stick to one backend so its
  engine-side prefix cache gets reused. This is **client-side prefix-hash
  affinity — an approximation of KV-cache locality that needs no engine events**;
  it does not subscribe to engine KV events (a later candidate, not v1).
- Health-checking and circuit-breaking failover across backends
- First-class latency instrumentation: TTFT and inter-token P50/P99/P99.9 per
  route, plus the gateway's own added latency, on a Prometheus `/metrics` endpoint

## Architecture

```
                +-------------------------------------------------+
   client       |                    kvmux                        |
  (OpenAI   --> |  listener -> session -> admission -> routing    |
   SDK / curl)  |     |            |          |          |        |
                |   SSE re-stream  |     max-in-flight   |        |
                |   (flush/frame)  |     + FIFO queue    |        |
                |                  |                  HRW prefix   |
                |             health + circuit         affinity   |
                |             breaker per backend         |       |
                +------------------|----------------------|-------+
                                   |                      |
                  +----------------+------+------+--------+--------+
                  |                       |               |
                  v                       v               v
              vLLM /v1            llama.cpp /v1        Ollama /v1
           (RTX, APC on)        (CPU GGUF, slots)    (small models)
```

## Build

Requires CMake >= 3.28, a C++20 compiler (GCC 13 or Clang 18), and Boost >= 1.83.
On Ubuntu 24.04:

```bash
sudo apt-get install -y build-essential g++-13 cmake ninja-build libboost-all-dev
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

## Run

```bash
./build/kvmux --config config/kvmux.example.toml
```

Point an OpenAI client at `http://127.0.0.1:8080/v1`. See
`config/kvmux.example.toml` for the configuration surface.

## Status

Building in public. Architecture and milestones land here as they're built; the
current tree is the early skeleton plus the streaming proxy spine.

**Every benchmark published in this repo will state its exact hardware,
configuration, and methodology. There are no performance numbers in this README,
and there won't be any until they trace to a committed raw log.**

## License

MIT — see [LICENSE](LICENSE).
