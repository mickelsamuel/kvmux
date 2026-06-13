#!/usr/bin/env bash
# kvmux gateway-overhead experiment (preliminary, WSL2).
#
# Measures kvmux's own added latency by comparing, at matched load:
#   A) loadgen -> kvmux -> mock backend (zero inter-token delay)
#   B) loadgen -> mock backend directly
# The per-request TTFT/E2E delta between A and B is the gateway's self-overhead.
# kvmux also reports kvmux:gateway_overhead_seconds (admission -> first upstream
# byte forwarded) which is scraped from /metrics for cross-check.
#
# Reproducible: run this script from a built tree. It launches the mock backend,
# launches kvmux against it, sweeps a few Poisson rates through both paths, writes
# raw CSVs + a summary to bench/results/wsl2-preliminary/, and scrapes /metrics.
#
# ALL numbers from this script are PRELIMINARY (WSL2). Publishable numbers come
# only from the bare-metal Ubuntu env (M6).
#
# Usage:
#   bench/run_overhead.sh [BUILD_DIR]
# Env overrides: RATES, DURATION, WARMUP, CONCURRENCY, OVERLAP, MAX_TOKENS.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$REPO_ROOT/build}"
KVMUX_BIN="$BUILD_DIR/kvmux"
MOCK="$REPO_ROOT/tests/mock_backend/mock_server.py"
LOADGEN="$REPO_ROOT/bench/loadgen.py"
OUT_DIR="$REPO_ROOT/bench/results/wsl2-preliminary"
mkdir -p "$OUT_DIR"

RATES="${RATES:-20 50 100 200}"
DURATION="${DURATION:-20}"
WARMUP="${WARMUP:-5}"
CONCURRENCY="${CONCURRENCY:-128}"
OVERLAP="${OVERLAP:-0.0}"   # overhead test: prefix overlap is irrelevant; isolate gateway cost
MAX_TOKENS="${MAX_TOKENS:-32}"

MOCK_PORT=9711
GW_PORT=9712
METRICS_PORT=9713
MODEL="mock-model"

if [[ ! -x "$KVMUX_BIN" ]]; then
    echo "error: kvmux binary not found at $KVMUX_BIN (build first)" >&2
    exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY="$OUT_DIR/overhead_summary_${STAMP}.txt"
ENVJSON="$OUT_DIR/environment_${STAMP}.json"

# Record the environment for every run (honesty requirement).
{
    echo "{"
    echo "  \"label\": \"preliminary (WSL2)\","
    echo "  \"timestamp_utc\": \"$STAMP\","
    echo "  \"host_kernel\": \"$(uname -srm)\","
    echo "  \"cpu_model\": \"$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')\","
    echo "  \"nproc\": $(nproc),"
    echo "  \"mem_total_kb\": $(grep MemTotal /proc/meminfo | awk '{print $2}'),"
    echo "  \"python\": \"$(python3 --version 2>&1)\","
    echo "  \"rates\": \"$RATES\", \"duration_s\": $DURATION, \"warmup_s\": $WARMUP,"
    echo "  \"concurrency\": $CONCURRENCY, \"overlap\": $OVERLAP, \"max_tokens\": $MAX_TOKENS,"
    echo "  \"mock_inter_token_delay_ms\": 0"
    echo "}"
} > "$ENVJSON"

cleanup() {
    [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null || true
    [[ -n "${MOCK_PID:-}" ]] && kill "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== kvmux gateway-overhead experiment — preliminary (WSL2) ===" | tee "$SUMMARY"
cat "$ENVJSON" | tee -a "$SUMMARY"

# 1) Mock backend at zero inter-token delay (the fastest possible upstream).
python3 "$MOCK" --host 127.0.0.1 --port "$MOCK_PORT" --default-delay-ms 0 \
    > "$OUT_DIR/mock_${STAMP}.log" 2>&1 &
MOCK_PID=$!
sleep 1

# 2) kvmux config pointing at the mock (round_robin so routing cost is trivial).
GW_CFG="$(mktemp)"
cat > "$GW_CFG" <<EOF
[server]
listen = "127.0.0.1"
port = $GW_PORT
[metrics]
port = $METRICS_PORT
[routing]
policy = "round_robin"
[[backends]]
name = "mock"
type = "vllm"
base_url = "http://127.0.0.1:$MOCK_PORT"
models = ["$MODEL"]
max_in_flight = 1024
health_interval_ms = 1000
EOF

"$KVMUX_BIN" --config "$GW_CFG" > "$OUT_DIR/kvmux_${STAMP}.log" 2>&1 &
GW_PID=$!
sleep 2

run_one() {
    local tag="$1" url="$2" rate="$3"
    local csv="$OUT_DIR/${tag}_rate${rate}_${STAMP}.csv"
    echo "--- $tag @ rate=$rate/s ---" | tee -a "$SUMMARY"
    python3 "$LOADGEN" --url "$url" --model "$MODEL" \
        --rate "$rate" --duration "$DURATION" --warmup "$WARMUP" \
        --concurrency "$CONCURRENCY" --overlap "$OVERLAP" --max-tokens "$MAX_TOKENS" \
        --label "$tag rate=$rate (preliminary WSL2)" \
        --csv "$csv" 2>&1 | tee -a "$SUMMARY"
}

for rate in $RATES; do
    # B) direct to the mock (baseline)
    run_one "direct" "http://127.0.0.1:$MOCK_PORT" "$rate"
    # A) through kvmux
    run_one "gateway" "http://127.0.0.1:$GW_PORT" "$rate"
done

echo "=== /metrics scrape (kvmux:gateway_overhead_seconds cross-check) ===" | tee -a "$SUMMARY"
curl -s "http://127.0.0.1:$METRICS_PORT/metrics" > "$OUT_DIR/metrics_${STAMP}.prom" || true
grep -E '^kvmux:(gateway_overhead_seconds|ttft_seconds|e2e_request_latency_seconds)_(sum|count)' \
    "$OUT_DIR/metrics_${STAMP}.prom" | tee -a "$SUMMARY" || true

rm -f "$GW_CFG"
echo "" | tee -a "$SUMMARY"
echo "All artifacts in: $OUT_DIR  (label: preliminary (WSL2))" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
