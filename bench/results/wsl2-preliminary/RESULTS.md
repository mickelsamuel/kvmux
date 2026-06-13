# kvmux preliminary results — WSL2 (NOT publishable)

**Label: `preliminary (WSL2)`.** These numbers are from WSL2 on a Windows
workstation, not the bare-metal Ubuntu bench env. They are NOT publishable and
do NOT appear in the top-level README. Publishable numbers come only from the
bare-metal env (M6). Every figure below traces to a committed raw CSV in this
directory and is reproducible via `bench/run_overhead.sh`.

## Environment
- Kernel: `6.6.87.2-microsoft-standard-WSL2`, CPU: Intel Core i9-14900KF
  (12 vCPUs visible to WSL2), ~28 GB RAM, Python 3.12.3.
- Gateway: `kvmux` (RelWithDebInfo), `policy = round_robin`, one mock backend at
  **zero inter-token delay** (so the measured latency is dominated by the
  gateway + loopback, isolating kvmux's own overhead).
- Loadgen: Poisson arrivals, concurrency 128, overlap 0.0, max_tokens 32,
  warm-up 3 s excluded, duration 12 s per cell.

## Gateway-overhead experiment (loadgen → kvmux → mock  vs  loadgen → mock direct)

`bench/run_overhead.sh` sweep over the default rates (20/50/100/200), run with
`DURATION=12 WARMUP=3` env overrides (the script's own defaults are 20 s / 5 s),
concurrency 128, overlap 0.0. Per-request latency, steady state.
Overhead Δ = (through-gateway) − (direct). Source CSVs: `*_rate{20,50,100,200}_20260613T002514Z.csv`.

| rate (req/s) | path    | TTFT p50 | TTFT p99 | E2E p50 | E2E p99 |
|--------------|---------|----------|----------|---------|---------|
| 20           | direct  | 0.753 ms | 1.015 ms | 0.828 ms| 1.100 ms|
| 20           | gateway | 1.114 ms | 1.323 ms | 1.187 ms| 1.439 ms|
| 20           | **Δ overhead** | **+0.361 ms** | **+0.308 ms** | **+0.359 ms** | **+0.339 ms** |
| 50           | direct  | 0.682 ms | 0.876 ms | 0.741 ms| 0.975 ms|
| 50           | gateway | 1.072 ms | 1.408 ms | 1.149 ms| 1.538 ms|
| 50           | **Δ overhead** | **+0.390 ms** | **+0.532 ms** | **+0.408 ms** | **+0.563 ms** |
| 100          | direct  | 0.699 ms | 0.927 ms | 0.769 ms| 1.035 ms|
| 100          | gateway | 0.949 ms | 1.309 ms | 1.032 ms| 1.464 ms|
| 100          | **Δ overhead** | **+0.250 ms** | **+0.382 ms** | **+0.263 ms** | **+0.429 ms** |
| 200          | direct  | 0.649 ms | 0.941 ms | 0.723 ms| 1.072 ms|
| 200          | gateway | 0.873 ms | 1.327 ms | 0.974 ms| 1.477 ms|
| 200          | **Δ overhead** | **+0.224 ms** | **+0.386 ms** | **+0.251 ms** | **+0.405 ms** |

**Preliminary (WSL2) added latency: ~0.2–0.4 ms P50 / ~0.3–0.5 ms P99 TTFT**
across 20–200 req/s. (No failed or open-loop-dropped requests at any rate.)

### Self-measured cross-check (`kvmux:gateway_overhead_seconds`)
admission → first upstream byte forwarded, over 4004 gateway requests:
`sum = 2.03055 s / count = 4004` → **~0.507 ms mean** self-overhead, consistent
with the external direct-vs-gateway delta above.

## CPU flamegraph
`flamegraph_gateway_preliminary_wsl2.svg` (+ `.folded` raw stacks).

WSL2 caveat: the `6.6.87.2-microsoft-standard-WSL2` kernel exposes **no hardware
PMU** (`cycles`/`instructions` are `<not supported>`), so the profile uses
**`cpu-clock` software-event sampling** (`perf record -e cpu-clock -F 499 -g`),
which works and yields a valid CPU flamegraph. Hardware-event profiling and a
final flamegraph are deferred to the bare-metal env (M6). The profile shows the
gateway's CPU time dominated by socket `sendmsg` (SSE frame forwarding), `epoll`
wait/ctl, and `Client::stream_chat` — i.e. networking syscalls, not gateway
logic. No egregious hotspot in kvmux code → no optimization pass taken.

## Raw files
- `direct_rate{20,50,100,200}_*.csv`, `gateway_rate{20,50,100,200}_*.csv` —
  per-request rows (ok, status, ttft_s, e2e_s, mean_itl_s, n_tokens, is_overlap, error).
- `overhead_summary_preliminary_wsl2.txt` — full run log + summaries.
- `environment_preliminary_wsl2.json` — host/run parameters.
- `metrics_*.prom` — a `/metrics` scrape taken at the end of the run.
- `flamegraph_gateway_preliminary_wsl2.svg` / `.folded` — the CPU flamegraph.

Reproduce: `DURATION=12 WARMUP=3 bench/run_overhead.sh` to match this run's
parameters (overhead + CSVs + scrape). A bare run uses the script's 20 s / 5 s
defaults and collects more samples per cell, so percentiles will be comparable
but not digit-identical. Flamegraph reproduction is documented in the M5 build
log (perf cpu-clock sampling).
