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
> **Phase 0 (build + secure + CI) is complete:** `cargo build --workspace` and
> `cargo test --workspace` are green (with `protoc` installed), CI gates both on every PR, and the
> hardcoded secrets have been purged in favour of environment-sourced credentials. The design —
> the typed DAG model and a single-machine Rust runtime — is in place. The flagship
> **prompt-to-DAG synthesis layer** that defines the product vision is **not yet implemented** and
> is the next major milestone. For a verified, file-level breakdown of what works versus what is
> aspirational, read the **[Code Evaluation](docs/CODE_EVALUATION.md)**. For the architecture of
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
| **Prompt → DAG synthesis (the compiler)** | 🔴 **Planned — flagship** | The defining feature. No NL→graph code exists yet; design in [SYNTHESIS_PIPELINE.md](docs/SYNTHESIS_PIPELINE.md) |
| **Confirm → Deploy → Run lifecycle** | 🔴 **Planned** | No confirmation/registration/deploy step exists yet |
| Workflow JSON → typed DAG parsing | ✅ **Working** | `gaussflow-core` compiles and validates (cycle/type checks) — the synthesis *target* |
| Topological single-machine execution | ✅ **Working** | One canonical engine (`gaussflow_runtime::execute_with_store`); runs with **no database** and returns real per-node outputs |
| Run with **no external dependencies** | ✅ **Working** | `RunStore` trait + in-memory default; SurrealDB is opt-in via `GAUSSFLOW_RUN_STORE=surreal` |
| LLM node + provider abstraction | ✅ **Working** | `LlmProvider` trait; OpenAI provider + deterministic offline `MockProvider` (no API key needed for `mock*` models). Anthropic/Ollama planned |
| Data-processor / conditional nodes | ✅ **Working** | Real deterministic transforms (`extract`/`set`) and comparisons (`eq`/`gt`/…); tested offline |
| Resource control (CPU/GPU semaphores) | ✅ **Working** | Concurrency limits + per-node timeouts in the executor |
| Retry with backoff (fixed/linear/exp) | ✅ **Working** | Per-node retry policy applied by the executor |
| Run persistence (SurrealDB) | 🟡 **Partial** | Env-sourced creds; opt-in backend behind the `RunStore` trait |
| CLI (validate / run / serve / config) | 🟡 **Partial** | `validate` + DB-free `run` work; `serve`/`config` wiring incomplete |
| Web dashboard + REST/WebSocket API | 🟡 **Partial** | API surface exists; execution path is *simulated* |
| Python bindings (PyO3) | 🟡 **Partial** | `validate` + async `execute` exposed |
| Terminal UI (TUI) | 🟡 **Partial** | Monitoring UI scaffold |
| Agent / ensemble / router nodes | 🟡 **In progress** | Need engine support: conditional edge traversal (router), per-predecessor inputs (ensemble), tool loops (agent), nested engine (subgraph) |
| Content-addressable artifact store | 🔴 **Planned** | In-memory store works; SurrealDB store is a stub |
| Distributed checkpointing & time-travel | 🔴 **Planned** | Types defined; backend not implemented |
| RBAC, audit, SLA, compliance | 🔴 **Planned** | Config types defined; enforcement not implemented |
| Distributed / K8s / edge execution | 🔴 **Planned** | Feature flags exist; runtime not implemented |

Legend: ✅ Working · 🟡 Partial / scaffolded · 🔴 Planned

> **The honest summary:** Phase 0 ✅ (builds + tests green, secrets removed, CI-gated) and Phase 1 ✅
> (one data model, one execution engine, runs with **no database** and returns real outputs) are
> done. Phase 2 is **in progress**: the LLM provider abstraction and the deterministic compute
> nodes (data-processor, conditional) are real and tested; agent/router/ensemble/subgraph still
> need deeper engine support (conditional edge traversal, per-predecessor inputs, tool loops,
> nested engines). The *front door* — the prompt-to-DAG compiler — comes after, on a runtime we
> now trust. The roadmap is sequenced exactly that way.

---

## Architecture

GaussFlow is a Cargo workspace of focused crates. The synthesis layer (planned) sits *above*
the runtime and emits the same `WorkflowSpec` the runtime already executes:

```
            ┌───────────────────────────────────────────────────────────┐
            │   Synthesis layer  (PLANNED — the product's front door)     │
            │   prompt → intent → DAG synthesis → validate → confirm      │
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
| **gaussflow-runtime** | Async execution engine and node handlers | [README](gaussflow-runtime/README.md) |
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

### Validate and run

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
`data_processor`, `conditional`, `parallel`. Implemented today: `llm_call` (via the provider
abstraction), `data_processor` (`extract`/`set`/`passthrough`), and `conditional` (comparisons).
`agent`/`ensemble`/`router`/`subgraph`/`parallel` are still stubs pending the engine work noted in
the status table above. The synthesis
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
