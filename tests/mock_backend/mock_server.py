#!/usr/bin/env python3
"""Deterministic OpenAI-compatible SSE mock backend for kvmux tests.

This is the contract used by every integration test and by the M5 gateway-
overhead benchmark. It emits exactly the OpenAI `chat.completion.chunk` SSE
framing (`data: {...}\n\n` ... `data: [DONE]\n\n`) that real vLLM / llama.cpp /
Ollama /v1 endpoints emit, and supports a set of fault/behavior modes selected
per-request so tests can drive each wire-behavior path deterministically.

Run:
    python3 mock_server.py --port 9000 [--default-delay-ms 5]

Mode selection (any of these, checked in order):
  * request JSON field   "mock_mode": "<mode>"
  * query string         ?mock_mode=<mode>
  * model name suffix     "<model>::<mode>"   (so a kvmux backend config can
                          point at a fixed model that triggers a mode)

Modes:
  normal        N tokens at the configured inter-token delay, then usage (if
                include_usage) then [DONE].   (default)
  slow_start    extra TTFT delay before the first token, then normal.
  abort         emit a few tokens then drop the connection mid-stream (no
                [DONE]) — exercises the gateway's mid-stream-failure path.
  http_503      respond 503 with an OpenAI-shaped error before streaming.
  malformed     emit one syntactically broken data: frame in the middle.
  usage         force-emit the trailing usage chunk regardless of
                include_usage (for capturing usage fixtures).

Tunables (request JSON or query string): n_tokens, delay_ms, ttft_ms.
"""

import argparse
import asyncio
import json
import time
import uuid

from aiohttp import web

DEFAULT_TOKENS = [
    "Hello",
    ",",
    " world",
    "!",
    " This",
    " is",
    " a",
    " mock",
    " stream",
    ".",
]


def _now():
    return int(time.time())


def _chunk(cid, model, *, role=None, content=None, finish=None, usage=None):
    choice = {"index": 0, "delta": {}, "finish_reason": finish}
    if role is not None:
        choice["delta"]["role"] = role
    if content is not None:
        choice["delta"]["content"] = content
    obj = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": _now(),
        "model": model,
        "choices": [] if usage is not None else [choice],
    }
    if usage is not None:
        obj["usage"] = usage
    return obj


def _sse(obj):
    return f"data: {json.dumps(obj)}\n\n".encode()


def _resolve_params(request, body):
    q = request.rel_url.query
    model = body.get("model", "mock-model")
    mode = body.get("mock_mode") or q.get("mock_mode")
    if mode is None and "::" in model:
        model, mode = model.split("::", 1)
    mode = mode or "normal"

    def _int(name, default):
        if name in body:
            return int(body[name])
        if name in q:
            return int(q[name])
        return default

    n_tokens = _int("n_tokens", len(DEFAULT_TOKENS))
    delay_ms = _int("delay_ms", request.app["default_delay_ms"])
    ttft_ms = _int("ttft_ms", 0)
    include_usage = bool(body.get("stream_options", {}).get("include_usage", False))
    return model, mode, n_tokens, delay_ms, ttft_ms, include_usage


def _tokens(n):
    out = []
    for i in range(n):
        out.append(DEFAULT_TOKENS[i % len(DEFAULT_TOKENS)])
    return out


async def handle_chat(request):
    try:
        body = await request.json()
    except Exception:
        body = {}

    model, mode, n_tokens, delay_ms, ttft_ms, include_usage = _resolve_params(
        request, body
    )
    stream = bool(body.get("stream", False))
    cid = "chatcmpl-" + uuid.uuid4().hex[:24]

    if mode == "http_503":
        return web.json_response(
            {
                "error": {
                    "message": "backend is loading",
                    "type": "upstream_error",
                    "code": 503,
                }
            },
            status=503,
        )

    if not stream:
        text = "".join(_tokens(n_tokens))
        return web.json_response(
            {
                "id": cid,
                "object": "chat.completion",
                "created": _now(),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": text},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": n_tokens,
                    "total_tokens": 10 + n_tokens,
                },
            }
        )

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
    await resp.prepare(request)

    if ttft_ms or mode == "slow_start":
        await asyncio.sleep((ttft_ms or 200) / 1000.0)

    # First chunk carries the role.
    await resp.write(_sse(_chunk(cid, model, role="assistant", content="")))

    tokens = _tokens(n_tokens)
    abort_after = max(1, n_tokens // 3)
    malformed_at = n_tokens // 2

    for i, tok in enumerate(tokens):
        if mode == "abort" and i >= abort_after:
            # Drop the connection mid-stream: close transport without [DONE].
            await resp.write_eof()
            request.transport.close()
            return resp
        if mode == "malformed" and i == malformed_at:
            await resp.write(b'data: {"object":"chat.completion.chunk","choices":[\n\n')
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000.0)
        await resp.write(_sse(_chunk(cid, model, content=tok)))

    # Finish chunk.
    await resp.write(_sse(_chunk(cid, model, finish="stop")))

    # Usage chunk (trailing, empty choices) when requested.
    if include_usage or mode == "usage":
        usage = {
            "prompt_tokens": 10,
            "completion_tokens": n_tokens,
            "total_tokens": 10 + n_tokens,
        }
        await resp.write(_sse(_chunk(cid, model, usage=usage)))

    await resp.write(b"data: [DONE]\n\n")
    await resp.write_eof()
    return resp


async def handle_models(request):
    return web.json_response(
        {
            "object": "list",
            "data": [
                {
                    "id": "mock-model",
                    "object": "model",
                    "created": _now(),
                    "owned_by": "mock",
                }
            ],
        }
    )


async def handle_health(request):
    # vLLM-style: 200 with empty body.
    return web.Response(status=200)


def build_app(default_delay_ms):
    app = web.Application()
    app["default_delay_ms"] = default_delay_ms
    app.router.add_post("/v1/chat/completions", handle_chat)
    app.router.add_get("/v1/models", handle_models)
    app.router.add_get("/health", handle_health)
    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--default-delay-ms", type=int, default=5)
    args = ap.parse_args()
    web.run_app(build_app(args.default_delay_ms), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
