# SSE fixtures

Recorded Server-Sent-Events streams used by the SSE-parser fixture tests
(`tests/unit/test_sse_fixtures.cpp`). These are the parser's contract against
real backend wire output.

## Provenance

| File | Source | Captured |
|---|---|---|
| `ollama_chat_stream.sse` | **Real** Ollama v0.30.8, `llama3.2:1b`, `/v1/chat/completions` `stream:true` | WSL2 Ubuntu 24.04, 2026-06-12 |
| `ollama_usage_stream.sse` | **Real** Ollama v0.30.8, same, with `stream_options.include_usage=true` | WSL2 Ubuntu 24.04, 2026-06-12 |
| `llamacpp_chat_stream.sse` | **Real** llama.cpp-server build b9616, `Qwen2.5-0.5B-Instruct` Q4_K_M, `include_usage=true` | WSL2 Ubuntu 24.04, 2026-06-12 |
| `vllm_chat_stream.synthetic.sse` | **SYNTHETIC** — documented, built from research findings Q4 (vLLM v0.22.1 chunk format). Not a real capture. | n/a |

## Why the vLLM fixture is synthetic

vLLM is CUDA/Linux-first and its Blackwell (sm_120) wheel support is the open
uncertainty U4/Q5 in the research findings. A real capture was not produced in
the M1 session; the install attempt is logged in `04_BUILD/M1-log.md` and the
real-vLLM-capture task is carried to **M6** (bare-metal Ubuntu env). Until then
this synthetic fixture mirrors the documented vLLM chunk shape: OpenAI
`chat.completion.chunk` SSE frames, a `finish_reason:"stop"` chunk, the trailing
empty-`choices` usage chunk from `include_usage`, and the `[DONE]` sentinel.

## Observed real-backend quirks captured here

- **Ollama** sets `system_fingerprint:"fp_ollama"`; usage chunk has empty
  `choices` and a `usage` object before `[DONE]`.
- **llama.cpp-server** emits a final chunk carrying **both** `usage` AND a
  top-level `timings` object (with empty `choices`) — the findings Q4.3 quirk.
  The parser forwards the `data:` payload verbatim, so `timings` passes through.

To re-capture the real fixtures, see the capture commands in `04_BUILD/M1-log.md`.
