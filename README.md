# llmux

**A low-latency LLM inference gateway in C++20.** *(Building in public — see status below.)*

Every serious LLM deployment fronts multiple model backends (vLLM, llama.cpp, Ollama, hosted APIs) and needs routing, failover, streaming, and latency control between the app and the models. Most existing gateways are Python or Node wrappers; the latency-sensitive part of the stack is underserved. `llmux` is an OpenAI-compatible gateway written in modern C++ for that hot path.

## Planned design

- OpenAI-compatible HTTP API; SSE token streaming with fan-out, backpressure, and admission control
- **KV-cache-aware routing**: same-session / same-prefix requests stick to a backend to exploit prefix caching
- Health-checking and circuit-breaking failover across backends
- First-class latency instrumentation: TTFT and inter-token P50/P99/P99.9 per route, Prometheus endpoint
- Networking on `io_uring` (epoll to start)

## Status

🚧 Building in public. Architecture and milestones land here as they're built.

**Every benchmark published in this repo will state its exact hardware, kernel configuration, and methodology — no aspirational numbers.**

## License

MIT
