# gaussflow-web

**The REST/WebSocket API and dashboard for [GaussFlow](../README.md), by Gaussian Technologies.**

`gaussflow-web` is an [Axum](https://github.com/tokio-rs/axum)-based HTTP service exposing
workflow and execution management endpoints, a WebSocket event stream, and a static dashboard.

## API surface

**Workflows**
- `GET/POST /workflows`, `GET/PUT/DELETE /workflows/:id`
- `POST /workflows/:id/execute`, `/validate`, `/duplicate`

**Executions**
- `GET /executions`, `GET/DELETE /executions/:id`
- `POST /executions/:id/{cancel,pause,resume}`
- `GET /executions/:id/logs`

**Ops**
- `GET /health`, `GET /metrics`, `GET /stats`
- `GET /ws` — WebSocket event stream
- Static dashboard served from [`static/`](static/)

## Status 🟡 Partial

- The route surface and WebSocket plumbing exist.
- **Execution is currently *simulated*** (`simulate_execution`) rather than delegated to the
  canonical [`gaussflow-runtime`](../gaussflow-runtime/README.md) engine — the dashboard shows
  fabricated progress, not real run results.
- There is **no authentication/authorization** on the API yet.

Wiring the web layer to the real engine and adding auth are tracked in Phases 4–5 of the
[Production Roadmap](../docs/PRODUCTION_ROADMAP.md).

## Run

```bash
cargo run -p gaussflow-web
# then open http://localhost:<port>/
```
</content>
