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
- Loadgen: Poisson arrivals, concurrency 128, overlap 0.0, max_tokens 16,
  warm-up 3 s excluded, duration 12 s per cell.

## Gateway-overhead experiment (loadgen → kvmux → mock  vs  loadgen → mock direct)

Per-request latency, steady state. Overhead = (through-gateway) − (direct).

| rate (req/s) | path    | TTFT p50 | TTFT p99 | E2E p50 | E2E p99 |
|--------------|---------|----------|----------|---------|---------|
| 20           | direct  | 0.720 ms | 0.898 ms | 0.802 ms| 0.963 ms|
| 20           | gateway | 1.091 ms | 1.312 ms | 1.162 ms| 1.399 ms|
| 20           | **Δ overhead** | **+0.371 ms** | **+0.414 ms** | **+0.360 ms** | **+0.436 ms** |
| 50           | direct  | 0.699 ms | 0.890 ms | 0.761 ms| 0.983 ms|
| 50           | gateway | 1.059 ms | 1.376 ms | 1.146 ms| 1.465 ms|
| 50           | **Δ overhead** | **+0.360 ms** | **+0.486 ms** | **+0.385 ms** | **+0.482 ms** |
| 100          | direct  | 0.646 ms | 0.883 ms | 0.702 ms| 1.012 ms|
| 100          | gateway | 0.940 ms | 1.300 ms | 1.024 ms| 1.465 ms|
| 100          | **Δ overhead** | **+0.294 ms** | **+0.417 ms** | **+0.322 ms** | **+0.453 ms** |

**Preliminary (WSL2) added latency: ~0.3–0.4 ms P50 / ~0.4–0.5 ms P99 TTFT.**

### Self-measured cross-check (`kvmux:gateway_overhead_seconds`)
admission → first upstream byte forwarded, over 1925 requests:
`sum = 1.00371 s / count = 1925` → **~0.521 ms mean** self-overhead, consistent
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
- `direct_rate{20,50,100}_*.csv`, `gateway_rate{20,50,100}_*.csv` — per-request
  rows (ok, status, ttft_s, e2e_s, mean_itl_s, n_tokens, is_overlap, error).
- `overhead_summary_preliminary_wsl2.txt` — full run log + summaries.
- `environment_preliminary_wsl2.json` — host/run parameters.
- `metrics_*.prom` — a `/metrics` scrape taken at the end of the run.
- `flamegraph_gateway_preliminary_wsl2.svg` / `.folded` — the CPU flamegraph.

Reproduce: `bench/run_overhead.sh` (overhead + CSVs + scrape). Flamegraph
reproduction is documented in the M5 build log (perf cpu-clock sampling).
