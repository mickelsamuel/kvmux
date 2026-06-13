# kvmux benchmarks

This directory holds the load generator, scenarios, and committed raw results.

**Status:** M5 populated (preliminary, WSL2). `loadgen.py` (Poisson-arrival
OpenAI client → per-request TTFT/ITL/E2E CSV + P50/P99/P99.9), `run_overhead.sh`
(the gateway-overhead experiment + rate sweep), and a CPU flamegraph live here;
preliminary results are under `results/wsl2-preliminary/`. The headline
affinity-vs-round-robin benchmark lands in M6 on the bare-metal Ubuntu env.

Quick start:

```
# Gateway-overhead experiment (direct vs through-gateway, rate sweep):
bench/run_overhead.sh [BUILD_DIR]        # writes results/wsl2-preliminary/

# Ad-hoc load against any OpenAI endpoint:
python3 bench/loadgen.py --url http://127.0.0.1:8080 --model <m> \
    --rate 50 --duration 30 --warmup 5 --concurrency 64 --overlap 0.5 --csv out.csv
```

Ground rules (non-negotiable, from the project plan):

- Every published number states hardware, configuration, arrival rate,
  concurrency, model, and prefix-overlap %.
- WSL2 results are always labeled `preliminary (WSL2)`. Publishable numbers come
  only from the bare-metal Ubuntu env.
- No number appears in the top-level README until it traces to a committed raw
  log under `bench/results/`.
