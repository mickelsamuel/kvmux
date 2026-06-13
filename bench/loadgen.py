#!/usr/bin/env python3
"""kvmux load generator — asyncio Poisson-arrival OpenAI-compatible client.

Drives an OpenAI /v1/chat/completions endpoint (kvmux, or a backend directly)
with a Poisson arrival process at a fixed request rate and a bounded concurrency,
over a synthetic dataset with a controllable shared-prefix overlap. It records
per-request TTFT, inter-token latency (ITL), and end-to-end latency (E2E), writes
the per-request rows to CSV, and prints a P50/P99/P99.9 summary. A warm-up window
is excluded from the summary so only steady-state requests count.

This is a measurement tool, not a product surface. It honors the project's bench
ground rules: every run records rate, concurrency, model, overlap %, and target,
and (under WSL2) results are labeled preliminary by the harness around it.

Definitions (matching findings Q6):
  TTFT = wall time from sending the request to the first content token received.
  ITL  = mean gap between consecutive content tokens (a per-request scalar; the
         summary reports the distribution of per-request mean-ITL).
  E2E  = wall time from sending the request to the final [DONE]/stream close.

Usage:
  python3 loadgen.py --url http://127.0.0.1:8080 --model mock-model \\
      --rate 50 --duration 30 --concurrency 64 --overlap 0.5 \\
      --warmup 5 --csv results/run.csv [--max-tokens 64]

Notes:
  * --rate is the mean arrivals/second of the Poisson process (exponential
    inter-arrival times). Arrivals are open-loop: a request is launched on
    schedule regardless of whether prior requests finished, up to --concurrency
    in flight (excess arrivals are dropped and counted, so an overloaded target
    is visible rather than silently back-pressured into a closed loop).
  * --overlap in [0,1] is the fraction of requests that share one fixed long
    system prompt (the "hot" prefix); the rest get a unique long prefix. This is
    the prefix-overlap % every reported number must state.
"""

import argparse
import asyncio
import csv
import json
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp


# A fixed long "system prompt" that forms the shared hot prefix when a request is
# selected into the overlap set. Long enough to matter for prefix caching.
SHARED_SYSTEM_PROMPT = (
    "You are a meticulous assistant operating under a fixed, lengthy system "
    "policy that is identical across many requests in this session. " * 16
)


@dataclass
class RequestResult:
    ok: bool
    status: int
    ttft_s: Optional[float]
    e2e_s: Optional[float]
    mean_itl_s: Optional[float]
    n_tokens: int
    is_overlap: bool
    error: str = ""


@dataclass
class Stats:
    results: list = field(default_factory=list)
    dropped: int = 0  # arrivals dropped because concurrency was saturated


def make_messages(req_index: int, overlap: bool, prompt_tokens_hint: int) -> list:
    """Build a messages array. Overlap requests share SHARED_SYSTEM_PROMPT (the
    hot prefix); non-overlap requests get a unique long system prompt so their
    prefixes do not collide."""
    if overlap:
        system = SHARED_SYSTEM_PROMPT
    else:
        # Unique long prefix per request: differs in the leading bytes so the
        # affinity key (and any prefix cache) does not collide with the hot set.
        system = f"UNIQUE-{req_index}-" + (
            "This request carries its own distinct system context. " * 16
        )
    user = f"Question {req_index}: respond with a short paragraph."
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


async def one_request(
    session: aiohttp.ClientSession,
    url: str,
    model: str,
    req_index: int,
    overlap: bool,
    max_tokens: int,
    extra_body: dict,
) -> RequestResult:
    body = {
        "model": model,
        "messages": make_messages(req_index, overlap, 0),
        "stream": True,
        "max_tokens": max_tokens,
    }
    body.update(extra_body)

    t_send = time.perf_counter()
    t_first: Optional[float] = None
    token_times: list = []
    n_tokens = 0
    try:
        async with session.post(
            url.rstrip("/") + "/v1/chat/completions",
            json=body,
            headers={"Accept": "text/event-stream"},
        ) as resp:
            if resp.status != 200:
                await resp.read()
                return RequestResult(
                    False,
                    resp.status,
                    None,
                    None,
                    None,
                    0,
                    overlap,
                    error=f"http {resp.status}",
                )
            async for raw in resp.content:
                line = raw.decode("utf-8", "replace").strip()
                if not line or not line.startswith("data:"):
                    continue
                payload = line[len("data:") :].strip()
                if payload == "[DONE]":
                    break
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                choices = obj.get("choices") or []
                if not choices:
                    continue  # usage chunk (empty choices)
                delta = choices[0].get("delta") or {}
                content = delta.get("content")
                if content:
                    now = time.perf_counter()
                    if t_first is None:
                        t_first = now
                    token_times.append(now)
                    n_tokens += 1
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        return RequestResult(
            False, 0, None, None, None, n_tokens, overlap, error=str(e)
        )

    t_done = time.perf_counter()
    ttft = (t_first - t_send) if t_first is not None else None
    e2e = t_done - t_send
    mean_itl = None
    if len(token_times) >= 2:
        gaps = [token_times[i] - token_times[i - 1] for i in range(1, len(token_times))]
        mean_itl = statistics.fmean(gaps)
    return RequestResult(True, 200, ttft, e2e, mean_itl, n_tokens, overlap)


async def run_load(args) -> Stats:
    stats = Stats()
    sem = asyncio.Semaphore(args.concurrency)
    rng = random.Random(args.seed)
    timeout = aiohttp.ClientTimeout(total=args.request_timeout)
    connector = aiohttp.TCPConnector(limit=0)  # do not cap connections internally

    start = time.perf_counter()
    warmup_until = start + args.warmup
    deadline = start + args.duration
    extra_body = json.loads(args.extra_body) if args.extra_body else {}

    tasks = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        async def launch(req_index: int, overlap: bool, is_warmup: bool):
            if sem.locked():
                # Concurrency saturated: open-loop drop so overload is visible.
                stats.dropped += 1
                return
            async with sem:
                r = await one_request(
                    session,
                    args.url,
                    args.model,
                    req_index,
                    overlap,
                    args.max_tokens,
                    extra_body,
                )
                if not is_warmup:
                    stats.results.append(r)

        req_index = 0
        while True:
            now = time.perf_counter()
            if now >= deadline:
                break
            overlap = rng.random() < args.overlap
            is_warmup = now < warmup_until
            tasks.append(asyncio.create_task(launch(req_index, overlap, is_warmup)))
            req_index += 1
            # Exponential inter-arrival time for a Poisson process at mean --rate.
            sleep_for = rng.expovariate(args.rate) if args.rate > 0 else 0.0
            await asyncio.sleep(sleep_for)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return stats


def pct(values, q):
    if not values:
        return float("nan")
    s = sorted(values)
    # Rounded linear-rank percentile: index = round((q/100) * (n-1)).
    # (Not textbook nearest-rank, which would be ceil((q/100) * n) - 1.)
    k = max(0, min(len(s) - 1, int(round((q / 100.0) * (len(s) - 1)))))
    return s[k]


def summarize(name, values):
    vals = [v for v in values if v is not None]
    if not vals:
        return f"{name}: (no data)"
    return (
        f"{name}: n={len(vals)} "
        f"p50={pct(vals, 50) * 1000:.3f}ms "
        f"p99={pct(vals, 99) * 1000:.3f}ms "
        f"p99.9={pct(vals, 99.9) * 1000:.3f}ms "
        f"mean={statistics.fmean(vals) * 1000:.3f}ms"
    )


def write_csv(path, results):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "ok",
                "status",
                "ttft_s",
                "e2e_s",
                "mean_itl_s",
                "n_tokens",
                "is_overlap",
                "error",
            ]
        )
        for r in results:
            w.writerow(
                [
                    int(r.ok),
                    r.status,
                    "" if r.ttft_s is None else f"{r.ttft_s:.9f}",
                    "" if r.e2e_s is None else f"{r.e2e_s:.9f}",
                    "" if r.mean_itl_s is None else f"{r.mean_itl_s:.9f}",
                    r.n_tokens,
                    int(r.is_overlap),
                    r.error,
                ]
            )


def main():
    ap = argparse.ArgumentParser(description="kvmux Poisson-arrival load generator")
    ap.add_argument("--url", required=True, help="base URL of the OpenAI endpoint")
    ap.add_argument("--model", required=True)
    ap.add_argument(
        "--rate", type=float, default=20.0, help="mean arrivals/sec (Poisson)"
    )
    ap.add_argument("--duration", type=float, default=30.0, help="total run seconds")
    ap.add_argument(
        "--warmup", type=float, default=5.0, help="warm-up seconds (excluded)"
    )
    ap.add_argument(
        "--concurrency", type=int, default=64, help="max in-flight requests"
    )
    ap.add_argument(
        "--overlap", type=float, default=0.5, help="shared-prefix fraction [0,1]"
    )
    ap.add_argument("--max-tokens", type=int, default=64)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--request-timeout", type=float, default=120.0)
    ap.add_argument(
        "--extra-body", default="", help="JSON merged into each request body"
    )
    ap.add_argument("--csv", default="", help="write per-request rows here")
    ap.add_argument(
        "--label", default="", help="free-text label echoed into the summary"
    )
    args = ap.parse_args()

    if not (0.0 <= args.overlap <= 1.0):
        print("error: --overlap must be in [0,1]", file=sys.stderr)
        sys.exit(2)

    stats = asyncio.run(run_load(args))

    results = stats.results
    ok = [r for r in results if r.ok]
    print("=" * 72)
    print(f"kvmux loadgen summary {('— ' + args.label) if args.label else ''}")
    print(
        f"target={args.url} model={args.model} rate={args.rate}/s "
        f"concurrency={args.concurrency} overlap={args.overlap} "
        f"duration={args.duration}s warmup={args.warmup}s max_tokens={args.max_tokens}"
    )
    print(
        f"requests(steady)={len(results)} ok={len(ok)} "
        f"failed={len(results) - len(ok)} dropped(open-loop)={stats.dropped}"
    )
    print(summarize("TTFT", [r.ttft_s for r in ok]))
    print(summarize("ITL ", [r.mean_itl_s for r in ok]))
    print(summarize("E2E ", [r.e2e_s for r in ok]))
    print("=" * 72)

    if args.csv:
        write_csv(args.csv, results)
        print(f"wrote per-request CSV: {args.csv}")


if __name__ == "__main__":
    main()
