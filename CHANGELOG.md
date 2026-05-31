# Changelog

All notable changes to GaussFlow are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/) from 0.1.0 onward.

## [0.1.0] — first public release

The first coherent release: GaussFlow went from a non-compiling repository with overstated
documentation to a green, CI-gated workspace that delivers the product vision —
**prompt → DAG → confirm → deploy → run** — on a trustworthy single-machine engine.

### Added
- **Synthesis (`gaussflow-synth`).** Compile a natural-language prompt into a validated, runnable
  workflow: Plan IR → lowering → `TypeSafeDag` validation with bounded self-repair; a confirmation
  view with a cost estimate; edit / re-validate / regenerate-with-feedback; and versioned, immutable
  deployments with provenance, required-secrets, quotas, triggers, and run trace-back.
- **Node types.** All eight are implemented: `llm_call` (OpenAI / Anthropic / local Ollama / offline
  mock providers), `data_processor`, `conditional`, `router`, `ensemble`, `agent` (bounded tool
  loop), `parallel`, and `subgraph` — with conditional/router branch skipping and ensemble fan-in.
- **Durability.** Checkpoint + resume (crash recovery, idempotent / exactly-once) and a durable,
  content-addressed artifact store (`FileContentStore`).
- **Observability.** Real Prometheus metrics fed by the executor (`/metrics/prometheus`); the web
  dashboard runs the **real engine** (no more simulation) and streams live run events; run/node
  tracing correlation ids.
- **Security (`gaussflow-security`).** JWT auth (secret from env, never defaulted) + RBAC on the web
  API (unauthorized mutations rejected), a tamper-evident hash-chained audit log (`GET /api/audit`),
  and PII redaction. `gaussflow token` mints API tokens.
- **Scale-out.** A bounded-concurrent executor with backpressure (`execute_concurrent`); a
  multi-stage `Dockerfile`, `docker-compose.yml`, and a Helm chart.
- **CI.** `cargo build`/`test --workspace` gated on every PR (with `protoc`); `fmt` gated;
  `clippy -D warnings` gated on `core`/`runtime`/`synth`/`security`; `audit` informational.

### Changed
- **Consolidated the core to one model and one engine.** Removed a duplicate data model
  (`node.rs`), a buggy duplicate engine (`core::engine`), and a dead parallel engine
  (`runtime::runtime`). The single canonical executor lives in `gaussflow-runtime`; runs need **no
  database** by default (in-memory `RunStore`; SurrealDB is opt-in) and return real per-node outputs.
- Documentation was rewritten to match reality (see `docs/CODE_EVALUATION.md`).

### Security
- Purged hardcoded secrets (SurrealDB password, default JWT secret) from the source tree and from
  git history; all credentials are environment-sourced.

### Known limitations / not yet done
- Distributed (multi-node) execution, Arrow-Flight data transport, and multi-node load tests
  (single-node bounded concurrency is done; the rest needs a cluster).
- OpenTelemetry export + a packaged Grafana dashboard; a HashiCorp Vault secret backend.
- `agent`/`ensemble`/etc. advanced strategies are baseline; broader API input validation; and the
  ~398 missing-doc warnings remain to be closed.
- An external security review (planned pre-GA).

[0.1.0]: https://github.com/rismanmattotorang/gaussflow/releases/tag/v0.1.0
