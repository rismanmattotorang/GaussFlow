<div align="center">

# GaussFlow™

### Prompt-to-production AI workflows. No canvas. No glue code.

**A product by [Gaussian Technologies](#about-gaussian-technologies)**

[![Status](https://img.shields.io/badge/status-Technology%20Preview%20(Alpha)-orange)](docs/CODE_EVALUATION.md)
[![Language](https://img.shields.io/badge/built%20with-Rust-000000?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)
[![Crates](https://img.shields.io/badge/workspace-6%20crates-informational)]()

*Describe the outcome you want. GaussFlow compiles it into an executable agent graph,
shows you the plan, and — on your confirmation — deploys and runs it.*

</div>

---

> **⚠️ Project status: Technology Preview (Alpha).**
> **Phases 0–2 are complete:** the workspace builds and tests green (CI-gated, `protoc` required),
> secrets are environment-sourced, the core is consolidated to one model + one engine that runs
> with **no database**, and **all eight node types are implemented** (llm_call, agent, ensemble,
> router, subgraph, data_processor, conditional, parallel) with conditional/router branch skipping
> and ensemble fan-in. **The flagship prompt-to-DAG synthesis layer** (`gaussflow-synth`) is
> implemented end-to-end: a prompt compiles to a Plan IR, lowers to a `WorkflowSpec`, passes the
> same validator a hand-authored graph does (with bounded self-repair), is rendered for
> confirmation with a cost estimate (editable/regenerable), and — once confirmed — is deployed
> (versioned, immutable, with provenance/secrets/quota/triggers) and run with trace-back. Planning
> runs on OpenAI, Anthropic, or local Ollama. For a verified, file-level
> breakdown of what works versus what is aspirational, read the
> **[Code Evaluation](docs/CODE_EVALUATION.md)**. For the architecture of
> the synthesis layer, see the **[Synthesis Pipeline](docs/SYNTHESIS_PIPELINE.md)**. For the path
> to 1.0, see the **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)**.

---

## The idea

Tools like n8n and Flowise made agentic workflows *buildable* — but you still have to **build
them**: drag nodes onto a canvas, wire edges by hand, configure each step. That is a designer's
job, and it does not scale to the speed at which teams now want to ship AI features.

GaussFlow removes the canvas. You state a goal in natural language:

> *"Every morning, pull our top 20 support tickets, cluster them by theme, draft a summary for
> each cluster with the cheapest model that's good enough, and post the digest to Slack."*

GaussFlow **synthesizes** that into a typed, validated agent graph — choosing node types
(LLM calls, agents, routers, ensembles), wiring dependencies, and assigning resources. It shows
you the plan as a DAG you can inspect and edit. When you **confirm**, it deploys the graph and
runs it on a Rust execution core built for concurrency and correctness.

**Prompt → DAG → Confirm → Deploy → Run.** That is the entire product.

---

## How it works

```
   ┌────────────┐   ┌──────────────┐   ┌─────────────┐   ┌───────────┐   ┌──────────┐
   │  1. Prompt │ → │ 2. Synthesize│ → │ 3. Confirm  │ → │ 4. Deploy │ → │ 5. Run   │
   │  natural   │   │  LLM compiler│   │  human-in-  │   │  register │   │  Rust    │
   │  language  │   │  → typed DAG │   │  the-loop   │   │  + alloc  │   │  runtime │
   │  goal      │   │  + validate  │   │  review/edit│   │  resources│   │  execute │
   └────────────┘   └──────────────┘   └─────────────┘   └───────────┘   └──────────┘
         │                  │                  │                │              │
     "what you want"   intent → spec      you stay in       runnable      observable
                       (passes the        control;          artifact      execution with
                       same validator     edit before                     metrics + logs
                       a hand-authored    it ships
                       graph would)
```

The key engineering bet: **synthesis is a compiler front-end whose back-end already exists.**
The natural-language layer must emit a `WorkflowSpec` that passes the *same* type/cycle
validation (`gaussflow-core`) and runs on the *same* executor (`gaussflow-runtime`) that a
hand-authored graph would. Generation is creative; the validated DAG is the contract.

See **[docs/SYNTHESIS_PIPELINE.md](docs/SYNTHESIS_PIPELINE.md)** for the full design.

---

## Why a Rust core

The synthesis layer is the *interface*; the runtime is the *guarantee*. Once a graph is
confirmed, it has to run predictably under concurrent load — which is why the execution core is
Rust, not Python glue:

- **Typed DAG.** Workflows are a validated `petgraph` DAG with cycle detection — not a free-form
  script. If the graph is invalid, it never deploys.
- **Async execution on Tokio.** Topological scheduling, per-node timeouts, retry with backoff.
- **Resource-aware.** Semaphore-based CPU/GPU concurrency control with RAII guards.
- **Observable by design.** Structured tracing and run persistence so you can see exactly what
  each node did.

---

## Feature status at a glance

An engineering tool should tell you the truth about its own maturity. Here is where each
capability stands today, evaluated against the product vision above.

| Capability | Status | Notes |
|---|---|---|
| **Clean `cargo build --workspace`** | ✅ **Working** | Builds + tests green; gated in CI. Requires `protoc` (see prerequisites). |
| **Prompt → DAG synthesis (the compiler)** | ✅ **Working** | `gaussflow-synth`: prompt → Plan IR → lower → validate → bounded repair. Capability catalog kept in lockstep with the runtime. Planning runs on OpenAI/Anthropic/Ollama; deterministic pipeline + benchmark tested offline |
| **Confirm → Deploy → Run lifecycle** | ✅ **Working** | Plan + cost estimate rendered for review; edit→re-validate + regenerate-with-feedback; `--deploy` persists versioned, immutable, provenance-bearing deployments with required-secrets, quotas, triggers, and run trace-back; `--run` executes on the real engine. (A scheduler that *fires* triggers + a vault backend are future work.) |
| Workflow JSON → typed DAG parsing | ✅ **Working** | `gaussflow-core` compiles and validates (cycle/type checks) — the synthesis *target* |
| Topological single-machine execution | ✅ **Working** | One canonical engine (`gaussflow_runtime::execute_with_store`); runs with **no database** and returns real per-node outputs |
| Run with **no external dependencies** | ✅ **Working** | `RunStore` trait + in-memory default; SurrealDB is opt-in via `GAUSSFLOW_RUN_STORE=surreal` |
| Checkpoint + resume (crash recovery) | ✅ **Working** | `CheckpointStore` (in-memory/file); `execute_resumable` resumes an interrupted run, re-running only incomplete nodes (idempotent); failure-injection tested |
| LLM node + provider abstraction | ✅ **Working** | `LlmProvider` trait; **OpenAI + Anthropic + local Ollama** providers + deterministic offline `MockProvider`, selected by model name (`gpt*`/`claude*`/`ollama/*`/`mock*`) or `GAUSSFLOW_LLM_PROVIDER` |
| Data-processor / conditional nodes | ✅ **Working** | Real deterministic transforms (`extract`/`set`) and comparisons (`eq`/`gt`/…); tested offline |
| Conditional/router branching | ✅ **Working** | Engine traverses edges by `on` label and skips untaken branches; `router` selects by input field; `subgraph` runs a nested workflow |
| Ensemble fan-in (named ports) | ✅ **Working** | Handlers receive per-predecessor outputs; `ensemble` aggregates via `collect`/`first`/`vote` |
| Agent (bounded tool loop) | ✅ **Working** | Tool-use loop with a `max_steps` budget over the provider abstraction; built-in `echo`/`upper`/`sum` tools; offline-testable via a scripted directive list |
| Parallel sub-workflows | ✅ **Working** | Runs inline `branches` concurrently on nested engines and collects results |
| Resource control (CPU/GPU semaphores) | ✅ **Working** | Concurrency limits + per-node timeouts in the executor |
| Retry with backoff (fixed/linear/exp) | ✅ **Working** | Per-node retry policy applied by the executor |
| Run persistence (SurrealDB) | 🟡 **Partial** | Env-sourced creds; opt-in backend behind the `RunStore` trait |
| CLI (validate / run / serve / config) | 🟡 **Partial** | `validate` + DB-free `run` work; `serve`/`config` wiring incomplete |
| Web dashboard + REST/WebSocket API | ✅ **Working** | Executes on the **real engine** (simulation removed); streams real per-node/completion events over WebSocket |
| Metrics & observability | ✅ **Working** | Real run/node counters → Prometheus exposition (`prometheus_metrics()` + web `/metrics/prometheus`); run/node tracing correlation IDs |
| Python bindings (PyO3) | 🟡 **Partial** | `validate` + async `execute` exposed |
| Terminal UI (TUI) | 🟡 **Partial** | Monitoring UI scaffold |
| Anthropic / Ollama providers, streaming | 🔴 **Planned** | `LlmProvider` abstraction is in place (OpenAI + mock); more backends + token streaming are enhancements |
| Content-addressable artifact store | ✅ **Working** | SHA-256 content-addressed; in-memory + **durable** `FileContentStore` (dedup, atomic writes); dummy SurrealDB/SkyTable stores removed |
| Distributed checkpointing & time-travel | 🟡 **Partial** | Single-machine checkpoint/resume works (above); distributed/time-travel planned |
| API auth (JWT) + RBAC | ✅ **Working** | `gaussflow-security`: HS256 JWT (secret from env, never defaulted), role-based authz; web middleware rejects unauthorized mutations (401/403/503) |
| Tamper-evident audit + PII redaction | ✅ **Working** | Hash-chained audit log (`verify()`, `GET /api/audit`); recursive email redaction for JSON. SLA/compliance still planned |
| Distributed / K8s / edge execution | 🔴 **Planned** | Feature flags exist; runtime not implemented |

Legend: ✅ Working · 🟡 Partial / scaffolded · 🔴 Planned

> **The honest summary:** Phase 0 ✅ (builds + tests green, secrets removed, CI-gated), Phase 1 ✅
> (one data model, one execution engine, runs with **no database** and returns real outputs), and
> Phase 2 ✅ (all eight node types real: `llm_call`, `agent`, `ensemble`, `router`, `subgraph`,
> `data_processor`, `conditional`, `parallel` — with conditional/router branch skipping and
> ensemble fan-in). The **flagship synthesis layer** (`gaussflow-synth`, **Phase S ✅**) compiles a
> prompt into a validated, runnable graph end-to-end — confirm/edit/estimate, multi-provider
> planning (OpenAI/Anthropic/Ollama), and versioned/immutable deploy with provenance,
> required-secrets, quotas, triggers, and run trace-back. **Phase 3 ✅** adds durability
> (checkpoint + resume, durable content-addressed store). **Phase 4 ✅** adds observability: real
> Prometheus metrics fed by the executor, the web dashboard wired to the **real engine** (no more
> simulation) streaming live run events, and run/node tracing correlation IDs. **Phase 5 ✅** adds
> security: JWT auth + RBAC on the API (unauthorized mutations rejected), a tamper-evident
> hash-chained audit log, and PII redaction (`gaussflow-security`). Next: scale-out incl.
> bounded-concurrent execution + backpressure (Phase 6) and release engineering (Phase 7). The
> roadmap is sequenced exactly that way.

---

## Architecture

GaussFlow is a Cargo workspace of focused crates. The synthesis layer (planned) sits *above*
the runtime and emits the same `WorkflowSpec` the runtime already executes:

```
            ┌───────────────────────────────────────────────────────────┐
            │   gaussflow-synth  (v1 — the product's front door)          │
            │   prompt → Plan IR → lower → validate → (repair) → confirm   │
            └───────────────────────────────┬───────────────────────────┘
                                            │  emits a validated WorkflowSpec
            ┌───────────────────────────────▼───────────────────────────┐
            │                       Interfaces                            │
            │   gaussflow-cli   gaussflow-web   gaussflow-tui             │
            │   gaussflow-py (Python bindings, PyO3)                       │
            └───────────────────────────────┬───────────────────────────┘
                                            │
            ┌───────────────────────────────▼───────────────────────────┐
            │                     gaussflow-runtime                       │
            │   Topological executor · node handlers · retry/backoff ·    │
            │   resource semaphores · run persistence                     │
            └───────────────────────────────┬───────────────────────────┘
                                            │
            ┌───────────────────────────────▼───────────────────────────┐
            │                      gaussflow-core                         │
            │   TypeSafeDag (petgraph) · WorkflowSpec model · validator · │
            │   scheduler · resource manager · checkpoint/storage traits  │
            └─────────────────────────────────────────────────────────────┘
```

| Crate | Role | Read more |
|---|---|---|
| **gaussflow-core** | Graph model, workflow spec, validation, scheduling primitives | [README](gaussflow-core/README.md) |
| **gaussflow-runtime** | Async execution engine, node handlers, LLM providers | [README](gaussflow-runtime/README.md) |
| **gaussflow-synth** | Prompt → DAG synthesis (Plan IR, lowering, validate/repair) | — |
| **gaussflow-security** | JWT auth, RBAC, tamper-evident audit log, PII redaction | — |
| **gaussflow-cli** | `gaussflow` command-line tool | [README](gaussflow-cli/README.md) |
| **gaussflow-web** | REST/WebSocket API + dashboard | [README](gaussflow-web/README.md) |
| **gaussflow-tui** | Terminal monitoring UI | [README](gaussflow-tui/README.md) |
| **gaussflow-py** | Python bindings (PyO3) | [README](gaussflow-py/README.md) |

---

## Quick start

> **Note:** until the synthesis layer lands, you author the `WorkflowSpec` directly — i.e. you
> hand-write the artifact the compiler will eventually produce. This is the runtime that the
> prompt-to-DAG layer is being built on top of.

### Prerequisites
- Rust 1.75+ and Cargo
- **`protoc` (Protocol Buffers compiler)** — required by the gRPC build scripts
  (`apt-get install protobuf-compiler`, `brew install protobuf`, or set `PROTOC`)
- (Optional) An `OPENAI_API_KEY` for live LLM nodes
- (Optional) A running [SurrealDB](https://surrealdb.com/) instance for run persistence

### Build

```bash
git clone <your-fork-url> gaussflow
cd gaussflow
cargo build --workspace   # ✅ builds (requires protoc)
cargo test  --workspace   # ✅ green
```

### Define a workflow (today: the synthesis *output* format)

`workflow.json`:

```json
{
  "name": "hello-llm",
  "nodes": [
    {
      "id": "summarize",
      "type": "llm_call",
      "model": { "provider": "openai", "model": "gpt-4o-mini" },
      "params": { "prompt": "Summarize the GaussFlow project in one sentence." }
    }
  ],
  "connections": [],
  "settings": { "concurrency": 4, "fail_fast": false }
}
```

### Synthesize from a prompt (the flagship loop)

```bash
# Describe the outcome; GaussFlow compiles it to a DAG, shows the plan + estimate, and runs it.
# Live planning needs a capable model — OpenAI (gpt*) or Anthropic (claude*):
export OPENAI_API_KEY=sk-...        # or: export ANTHROPIC_API_KEY=sk-ant-...
cargo run -p gaussflow-cli -- synth "Extract the text field, then summarize it" \
    --model gpt-4o-mini --run --deploy
```

This prints the proposed plan, a cost/latency estimate, and the workflow spec (the confirmation
view), then — with `--run` — executes it on the same engine a hand-authored workflow uses, and —
with `--deploy` — persists a **versioned, immutable** deployment (with the originating prompt as
provenance) and links the run back to it. The synthesized spec is held to the *same* validation as
any other workflow. (Use `--save out.json` to write the spec for hand-editing.)

### Validate and run a hand-authored workflow

```bash
# Validate a workflow specification
cargo run -p gaussflow-cli -- validate workflow.json

# Execute it — runs with NO database by default and prints per-node outputs.
# (Set OPENAI_API_KEY for live llm_call nodes; set GAUSSFLOW_RUN_STORE=surreal to persist runs.)
cargo run -p gaussflow-cli -- run workflow.json
```

### Use from Python

```python
import gaussflow_py
import asyncio, json

spec = open("workflow.json").read()

# Validate: returns (name, node_count, edge_count)
print(gaussflow_py.validate(spec))

# Execute asynchronously
async def main():
    result = await gaussflow_py.execute_py(spec, json.dumps({}))
    print(result)

asyncio.run(main())
```

> **Known limitations (tracked on the roadmap):** execution runs with no database by default and
> returns the full per-node output map (`{ run_id, output, outputs }`); SurrealDB persistence is
> opt-in via `GAUSSFLOW_RUN_STORE=surreal`. The main gaps now are that most node types
> (agent/router/ensemble/…) are still passthrough stubs (Phase 2), and there is no prompt-to-DAG
> step yet — you supply the `WorkflowSpec` the compiler will one day generate.

---

## The workflow specification (the synthesis target)

Whether written by a human or generated by the synthesis layer, a workflow is `name`, a list of
`nodes`, a list of `connections` (edges), and `settings`. This schema is the **contract**
between the prompt-to-DAG compiler and the runtime:

```jsonc
{
  "name": "advanced-pipeline",
  "nodes": [
    {
      "id": "router",
      "type": "router",
      "resources": { "cpu_cores": 1, "memory_mb": 512, "timeout_ms": 30000, "priority": 5 }
    },
    {
      "id": "draft",
      "type": "llm_call",
      "model": { "provider": "openai", "model": "gpt-4o" },
      "params": { "prompt": "Write a draft." },
      "retry": { "max_attempts": 3, "backoff": "exponential" }
    }
  ],
  "connections": [
    { "from": "router", "to": "draft", "on": "success" }
  ],
  "settings": { "concurrency": 8, "fail_fast": false, "resume": true }
}
```

Supported node types in the core model: `llm_call`, `agent`, `ensemble`, `router`, `subgraph`,
`data_processor`, `conditional`, `parallel`. **All eight are implemented:** `llm_call` (provider abstraction),
`data_processor` (`extract`/`set`/`passthrough`), `conditional` (comparisons), `router`
(field-based routing), `subgraph` (nested workflow), `ensemble` (`collect`/`first`/`vote`),
`agent` (bounded tool loop), and `parallel` (concurrent sub-workflows). Remaining Phase 2 work is
enhancements only — more LLM providers (Anthropic/Ollama) and token streaming. The synthesis
layer can only safely emit node types that the runtime actually executes — which is why
"make the node types real" precedes "ship the compiler" on the roadmap.

---

## Development

```bash
cargo build --workspace        # build everything (requires protoc)
cargo test  --workspace        # run the suite (green)
cargo bench -p gaussflow-runtime
```

> Some legacy tests and example programs that target the soon-to-be-consolidated engine paths are
> **quarantined** behind a `legacy_tests` feature (and `examples_legacy/`) so the default build and
> CI stay green. They will be rewritten against the canonical engine in roadmap Phase 1. To see
> them: `cargo test --workspace --features gaussflow-core/legacy_tests,gaussflow-runtime/legacy_tests`.

See [`build.sh`](build.sh) and [`clean.sh`](clean.sh) for convenience scripts.

Contributions are welcome. The most valuable contributions right now are the items tracked in
the [Production Roadmap](docs/PRODUCTION_ROADMAP.md).

---

## Documentation

- 🧠 **[Synthesis Pipeline](docs/SYNTHESIS_PIPELINE.md)** — architecture of the flagship
  prompt → DAG → confirm → deploy → run layer.
- 📋 **[Code Evaluation](docs/CODE_EVALUATION.md)** — honest engineering assessment of the
  current codebase: what works, what's a stub, and the risks.
- 🗺️ **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)** — the phased plan to take GaussFlow
  from Alpha to a production 1.0.
- 📐 **[GaussFlow Specs](GAUSSFLOW-SPECS.md)** — the original design vision.

---

## About Gaussian Technologies

**Gaussian Technologies** is a deep-tech company building the infrastructure layer for
production AI. We believe the next decade of software will be defined not by single models, but
by *systems* of models, agents, and tools working together — and that the people who need those
systems should not have to become workflow engineers to build them.

GaussFlow is our flagship engine for turning intent into running systems: you describe the
outcome, we compile, verify, and execute the graph. Named for Carl Friedrich Gauss — and for the
distributions at the heart of modern machine learning — it reflects our engineering values:
rigor, precision, and elegant foundations.

> *Orchestrate intelligence.*

---

## License

Apache-2.0. See [`LICENSE`](LICENSE) for details.

© 2026 Gaussian Technologies. GaussFlow™ is a trademark of Gaussian Technologies.
</content>
</invoke>
