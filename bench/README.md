# kvmux benchmarks

This directory holds the load generator, scenarios, and committed raw results.

**Status:** not yet populated. The load generator (`loadgen.py`) and the
gateway-overhead methodology land in M5; the headline affinity-vs-round-robin
benchmark lands in M6 on the bare-metal Ubuntu env.

Ground rules (non-negotiable, from the project plan):

- Every published number states hardware, configuration, arrival rate,
  concurrency, model, and prefix-overlap %.
- WSL2 results are always labeled `preliminary (WSL2)`. Publishable numbers come
  only from the bare-metal Ubuntu env.
- No number appears in the top-level README until it traces to a committed raw
  log under `bench/results/`.
