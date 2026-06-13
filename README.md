# kvmux

[![ci](https://github.com/mickelsamuel/kvmux/actions/workflows/ci.yml/badge.svg)](https://github.com/mickelsamuel/kvmux/actions/workflows/ci.yml)

A single-binary, zero-Kubernetes, OpenAI-compatible gateway in C++20 that fronts
heterogeneous self-hosted backends — vLLM, llama.cpp-server, and Ollama — behind
one endpoint, with SSE streaming, admission control, circuit-breaker failover,
and prefix-affinity routing.

When you self-host inference you rarely run a single engine. A real setup mixes
a vLLM box, a llama.cpp-server, and an Ollama instance, and the application in
front of them still wants one OpenAI-compatible URL, token streaming that keeps
its inter-token timing, failover when a backend dies, and a way to keep
multi-turn conversations landing on the backend that already has their prefix
cached. kvmux is that gateway: one binary, no orchestrator, with per-route
latency instrumentation built for measuring time-to-first-token and inter-token
latency.

What sets it apart is the combination, not a single trick: the form factor (one
static-ish binary, no control plane), the backend breadth (three different
engines with their quirks handled in one place), and quant-grade TTFT/ITL
instrumentation on a Prometheus endpoint. It is written in C++20 because that's
where the author's systems depth is, and because the proxy hot path —
re-streaming tokens with a per-frame flush — is worth controlling directly. That
is the honest reason for the language choice; it is not a claim that the job
*requires* C++.

> Status: building in public. The code through multi-backend routing, metrics,
> and a preliminary overhead benchmark is here. The headline affinity-vs-round-robin
> benchmark is **pending a bare-metal run** (see [Benchmarks](#benchmarks)). The
> only numbers in this README are the preliminary WSL2 overhead figures, and they
> are labeled as such.

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Quickstart (under 10 minutes against Ollama)](#quickstart-under-10-minutes-against-ollama)
- [Configuration reference](#configuration-reference)
- [Routing](#routing)
- [Metrics](#metrics)
- [Benchmarks](#benchmarks)
- [Limitations](#limitations)
- [Build from source](#build-from-source)
- [License](#license)

## What it does

- **OpenAI-compatible HTTP API.** `POST /v1/chat/completions` (streaming and
  non-streaming), `GET /v1/models` (aggregated across backends), plus `GET /healthz`
  (gateway liveness) and `GET /metrics` (Prometheus text). Unknown routes return
  an OpenAI-shaped 404.
- **One endpoint over heterogeneous backends.** vLLM, llama.cpp-server, and
  Ollama, each fronted with its own health-check and error conventions, behind a
  single URL. Each backend's quirks live in one module; the rest of the gateway
  doesn't know the difference.
- **SSE streaming with per-frame flush.** Each `data: {...}` frame is written and
  flushed as it arrives, with `TCP_NODELAY` on the upstream socket, so inter-token
  timing is preserved rather than coalesced. The trailing usage chunk and
  `data: [DONE]` are forwarded verbatim.
- **Prefix-affinity routing.** Same-prefix requests stick to one backend so its
  engine-side prefix cache gets reused. This is **client-side prefix-hash affinity
  — an approximation of KV-cache locality that needs no engine events**; it does
  not subscribe to engine KV events (a v1.1 candidate, not v1). Round-robin is
  always available as the baseline.
- **Failover and admission control.** Per-backend health checking and circuit
  breaking; a backend that fails repeatedly is taken out and probed back in.
  A global concurrency ceiling plus a bounded FIFO wait-queue protect the
  backends; overflow returns `429` with `Retry-After`.
- **Latency instrumentation.** TTFT, inter-token latency, end-to-end latency,
  queue time, and the gateway's *own* added latency, as Prometheus histograms
  with OpenTelemetry GenAI buckets.

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
           (GPU, APC on)        (CPU GGUF, slots)    (small models)
```

A request enters the listener, becomes a session, passes admission control
(global concurrency limit and per-backend in-flight limit, with a bounded
wait-queue), and is routed to a backend. The routing layer picks a healthy,
non-tripped backend that serves the requested model — by prefix affinity or
round-robin. The upstream client streams the response back, and the SSE writer
re-emits each frame downstream as it arrives. Health checks and the circuit
breaker run per backend in the background.

## Quickstart (under 10 minutes against Ollama)

This is the shortest path from nothing to a streaming response through the
gateway. It uses [Ollama](https://ollama.com) as the single backend. On a clean
Ubuntu 24.04 (or WSL2) box the whole thing — clone, build, run, stream — took
about a minute on the reference machine; most of that is the build.

**1. Install and start Ollama, pull a small model.**

```bash
curl -fsSL https://ollama.com/install.sh | sh   # or your platform's installer
ollama serve &                                   # if not already running as a service
ollama pull llama3.2:1b
```

Confirm it's up:

```bash
curl http://127.0.0.1:11434/            # -> Ollama is running
```

**2. Build kvmux** (needs CMake ≥ 3.28, GCC 13 or Clang 18, Boost ≥ 1.83; see
[Build from source](#build-from-source) for the exact apt line):

```bash
git clone https://github.com/mickelsamuel/kvmux.git
cd kvmux
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

**3. Run the gateway** against the bundled single-Ollama config:

```bash
./build/kvmux --config config/ollama-quickstart.toml
# kvmux 0.1.0 — loaded config/ollama-quickstart.toml (1 backend(s), policy=prefix_affinity)
```

`config/ollama-quickstart.toml` is one backend block pointing at local Ollama
and serving `llama3.2:1b`. To use a different model, change the `models` list
and pull that model with `ollama pull`.

**4. Stream a completion** through the gateway with `curl -N` (no buffering, so
you see tokens arrive one by one):

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama3.2:1b","stream":true,
       "messages":[{"role":"user","content":"In one sentence, what is an LLM gateway?"}]}'
```

You'll see `data: {...}` frames stream in token by token, then a usage chunk,
then `data: [DONE]`. A captured run is in [`docs/demo.txt`](docs/demo.txt).

Point any OpenAI client at `http://127.0.0.1:8080/v1` and it works the same way.
`GET /v1/models` lists the models the gateway is serving; `GET /metrics` (on port
9090) exposes the latency histograms.

## Configuration reference

Configuration is a single TOML file. This is the **complete v1 surface** —
anything not listed here is, by design, not configurable in v1. Unknown keys are
a hard error with the offending line and column, so a typo fails fast instead of
being silently ignored. The full annotated example is
[`config/kvmux.example.toml`](config/kvmux.example.toml).

### `[server]`

| key | type | default | meaning |
|-----|------|---------|---------|
| `listen` | string | `"0.0.0.0"` | address the OpenAI API binds to |
| `port` | int | `8080` | OpenAI API port |
| `max_concurrent_streams` | int | `256` | global admission ceiling on in-flight requests |
| `queue_depth` | int | `64` | bounded FIFO wait-queue size when at the ceiling |
| `queue_timeout_ms` | int | `5000` | a queued request waits at most this long, then `429` |
| `request_timeout_ms` | int | `300000` | upstream request budget (streaming path) |

### `[metrics]`

| key | type | default | meaning |
|-----|------|---------|---------|
| `port` | int | `9090` | Prometheus text exposition is served on `/metrics` here |

### `[routing]`

| key | type | default | meaning |
|-----|------|---------|---------|
| `policy` | string | `"prefix_affinity"` | `"prefix_affinity"` or `"round_robin"` |
| `prefix_bytes` | int | `1024` | bytes of the canonical request used to form the affinity key |
| `load_threshold` | float | `0.8` | spill to the next backend when the chosen one is this fraction of `max_in_flight` |

### `[[backends]]` (one block per upstream)

| key | type | default | meaning |
|-----|------|---------|---------|
| `name` | string | required | unique backend id (used in metric labels) |
| `type` | string | required | `"vllm"`, `"llamacpp"`, or `"ollama"` — selects the quirk module |
| `base_url` | string | required | upstream base URL |
| `models` | string[] | required | model ids this backend serves; for vLLM these must be the exact `--served-model-name` (vLLM returns 404 on a mismatch) |
| `max_in_flight` | int | `8` | per-backend concurrency limit |
| `health_interval_ms` | int | `2000` | health-check interval |
| `failure_threshold` | int | `5` | consecutive request failures that open the circuit breaker |
| `open_ms` | int | `10000` | how long the breaker stays open before a half-open probe |

The model map is authoritative: kvmux routes a request to a backend whose
`models` list contains the requested model id, and never rewrites model names
heuristically.

## Routing

kvmux ships two routing policies, selected by `routing.policy`. Round-robin is
the benchmark baseline and stays selectable forever.

**Prefix-affinity** is the differentiator, and it is deliberately a client-side
approximation:

> client-side prefix-hash affinity — an approximation of KV-cache locality that
> needs no engine events; v1.1 may consume vLLM's ZMQ KV events.

It never claims engine-level KV awareness. The mechanism:

1. **Affinity key.** Take the canonical concatenation of `model`, then each
   message as `role:content` in order, and truncate to the first `prefix_bytes`
   (default 1024). Hash it with XXH64. The idea: two requests that share a long
   common prefix (a shared system prompt, the same conversation so far) produce
   the same key, because the prefix dominates the first kilobyte.
2. **Backend choice.** Rendezvous (highest-random-weight) hashing of
   `(key, backend-id)` over the eligible backends. HRW gives deterministic
   stickiness with no shared session state, and reshuffles minimally when a
   backend joins or leaves.
3. **Load guard.** If the HRW winner is already at or above
   `load_threshold × max_in_flight`, the request cascades to the next HRW
   candidate instead of piling onto a saturated backend. Every such spill is
   counted (`kvmux:affinity_spills_total`).

The bet is that pinning same-prefix traffic to one backend raises that backend's
engine-side prefix-cache hit rate, which lowers TTFT — without kvmux needing to
know anything about the engine's cache internals. Whether, and by how much, that
bet pays off is exactly what the headline benchmark measures.

## Metrics

`GET /metrics` (on the metrics port) returns Prometheus text exposition. Metric
names are **colon-prefixed** (`kvmux:ttft_seconds`, …) on purpose: vLLM and
SGLang ship their serving metrics with a `vllm:` / `sglang:` colon prefix, and
matching that convention lets kvmux drop into the same GenAI dashboards.
`promtool check metrics` prints a "metric names should not contain ':'" style
advisory for these names but exits 0; the colon prefix is an intentional
ecosystem trade-off, not an oversight.

Histograms (OpenTelemetry GenAI / vLLM buckets, base unit seconds):

| metric | meaning |
|--------|---------|
| `kvmux:ttft_seconds` | time to first token |
| `kvmux:inter_token_latency_seconds` | inter-token (time-per-output-token) latency |
| `kvmux:e2e_request_latency_seconds` | end-to-end request latency |
| `kvmux:request_queue_time_seconds` | time spent in the admission queue |
| `kvmux:gateway_overhead_seconds` | gateway self-overhead: admission to first upstream byte forwarded |

Gauges: `kvmux:requests_running`, `kvmux:requests_waiting`.
Counters: `kvmux:requests_total{code}`, `kvmux:affinity_spills_total`,
`kvmux:backend_failures_total`.

Histogram and counter series carry `{route, backend, model}` labels.
`kvmux:gateway_overhead_seconds` is the differentiator metric: it isolates the
latency kvmux itself adds, separate from whatever the backend takes.

## Benchmarks

There are two benchmark questions, and only one of them has numbers yet.

**Gateway overhead — preliminary (WSL2).** How much latency does kvmux add on
top of a backend? Measured by running the same load directly against a mock
backend (zero inter-token delay, so the result is dominated by the gateway and
loopback) versus through the gateway, and taking the difference.

These figures are **`preliminary (WSL2)`** — taken on WSL2 on a Windows
workstation, not a bare-metal Ubuntu bench. They are a starting point, not a
publishable result. Environment:
[`bench/results/wsl2-preliminary/environment_preliminary_wsl2.json`](bench/results/wsl2-preliminary/environment_preliminary_wsl2.json)
(Intel Core i9-14900KF, 12 vCPUs visible to WSL2; kernel
`6.6.87.2-microsoft-standard-WSL2`; round-robin policy; concurrency 128;
warm-up excluded). Added latency = (through gateway) − (direct):

| rate (req/s) | TTFT p50 added | TTFT p99 added | E2E p50 added | E2E p99 added |
|--------------|----------------|----------------|---------------|---------------|
| 20  | +0.361 ms | +0.308 ms | +0.359 ms | +0.339 ms |
| 50  | +0.390 ms | +0.532 ms | +0.408 ms | +0.563 ms |
| 100 | +0.250 ms | +0.382 ms | +0.263 ms | +0.429 ms |
| 200 | +0.224 ms | +0.386 ms | +0.251 ms | +0.405 ms |

So roughly **0.2–0.4 ms P50 / 0.3–0.5 ms P99 added TTFT** across 20–200 req/s,
with no dropped requests at any rate. A self-measured cross-check from
`kvmux:gateway_overhead_seconds` (admission → first upstream byte, over 4004
requests) gives a **~0.51 ms mean**, consistent with the external delta. Raw
per-request CSVs, the run log, the metrics scrape, and a CPU flamegraph are under
[`bench/results/wsl2-preliminary/`](bench/results/wsl2-preliminary/) and
reproduce via `bench/run_overhead.sh`. Full write-up:
[`bench/results/wsl2-preliminary/RESULTS.md`](bench/results/wsl2-preliminary/RESULTS.md).

**Headline benchmark (affinity vs round-robin): pending bare-metal run.** The
question that actually matters for the routing claim — does prefix affinity lower
P99 TTFT versus round-robin, at a stated prefix-overlap %, on documented
hardware — has **no published number yet**. It needs two real engine replicas
with prefix caching on, which is a bare-metal GPU job, not a WSL2 one. When it
runs, the result will state hardware, overlap %, arrival rate, and the cache-hit
rate, with raw logs committed. Until then there is no affinity performance claim
here, in either direction.

Every number in this README traces to a committed raw artifact under
`bench/results/`. There are no projected or aspirational figures.

## Limitations

Being honest about the edges:

- **Preliminary, single-machine numbers only.** The overhead figures above are
  WSL2 on one workstation. They are labeled preliminary and are not a substitute
  for the bare-metal run. There is no headline affinity benchmark yet.
- **No TLS and no auth.** The gateway speaks plain HTTP and does not
  authenticate callers. Run it on a trusted network or behind something that
  terminates TLS and handles auth (a reverse proxy, a service mesh).
- **Ollama is fronted through its `/v1/` layer only.** kvmux talks to Ollama's
  OpenAI-compatible `/v1/` endpoints, not its native NDJSON API. Anything Ollama
  only exposes on the native API is out of scope.
- **A focused v1 surface.** No embeddings, no legacy completions, no model
  management, no web UI, no multi-tenancy. The served endpoints are exactly the
  four listed under [What it does](#what-it-does).
- **Prefix affinity is an approximation, by design.** It is client-side
  prefix-hash stickiness, not engine KV-cache awareness. Consuming real engine KV
  events (e.g. vLLM's ZMQ KV events) is a v1.1 candidate, not v1.
- **io_uring is not in v1.** The networking is epoll-based via Boost.Asio;
  io_uring is a labeled future exploration.

## Build from source

Verified from a clean clone on Ubuntu 24.04 (the same steps the quickstart runs).
Requirements: CMake ≥ 3.28, a C++20 compiler (GCC 13 or Clang 18), Boost ≥ 1.83.
Dependencies that aren't system packages (nlohmann/json, toml++, Catch2, xxhash)
are fetched by CMake during configure.

```bash
sudo apt-get update
sudo apt-get install -y build-essential g++-13 cmake ninja-build libboost-all-dev

git clone https://github.com/mickelsamuel/kvmux.git
cd kvmux
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Run the tests:

```bash
ctest --test-dir build --output-on-failure
```

The binary is `build/kvmux`. Run it with `--config <path.toml>`; `--version` and
`--help` are also available. CI builds the same tree on `ubuntu-24.04` under GCC
13 and Clang 18, with AddressSanitizer+UndefinedBehaviorSanitizer and
ThreadSanitizer jobs and a clang-format check.

## License

MIT — see [LICENSE](LICENSE).
