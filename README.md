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

> **⚠️ Project status: Technology Preview (Alpha) — does not currently build from a clean checkout.**
> GaussFlow's design — the typed DAG model and a single-machine Rust runtime — is in place, and
> `gaussflow-core` compiles on its own. But **the workspace as committed does not compile**: the
> `gaussflow-runtime` crate fails with feature/dependency errors, and the gRPC crates require an
> undocumented `protoc` toolchain. The flagship **prompt-to-DAG synthesis layer** that defines the
> product vision is **not yet implemented**. Fixing the build is the #1 item on our roadmap. For a
> verified, file-level breakdown of what compiles versus what is aspirational, read the
> **[Code Evaluation](docs/CODE_EVALUATION.md)**. For the architecture of the synthesis layer,
> see the **[Synthesis Pipeline](docs/SYNTHESIS_PIPELINE.md)**. For the path to 1.0, see the
> **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)**.

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
| **Clean `cargo build --workspace`** | 🔴 **Broken** | `gaussflow-runtime` fails to compile (invalid `rocksdb` feature on the core dep; `tracing`/`metrics` feature-gating bugs; `procfs` API mismatch). Requires `protoc`. Roadmap Phase 0. |
| **Prompt → DAG synthesis (the compiler)** | 🔴 **Planned — flagship** | The defining feature. No NL→graph code exists yet; design in [SYNTHESIS_PIPELINE.md](docs/SYNTHESIS_PIPELINE.md) |
| **Confirm → Deploy → Run lifecycle** | 🔴 **Planned** | No confirmation/registration/deploy step exists yet |
| Workflow JSON → typed DAG parsing | ✅ **Working** | `gaussflow-core` compiles and validates (cycle/type checks) — the synthesis *target* |
| Topological single-machine execution | 🟠 **Present, not building** | `gaussflow-runtime` (Tokio-based) — code exists and is the intended back-end, but the crate does not currently compile |
| LLM node (OpenAI Chat Completions) | 🟠 **Present, not building** | Real OpenAI call in `handler.rs`; blocked by the runtime build failure |
| Resource control (CPU/GPU semaphores) | 🟡 **Partial** | Implemented in `gaussflow-core` (compiles); runtime wiring blocked by build |
| Retry with backoff (fixed/linear/exp) | 🟡 **Partial** | Policy modeled; runtime wiring blocked by build |
| Run persistence (SurrealDB) | 🟡 **Partial** | Hardcoded creds; needs config + graceful fallback |
| CLI (validate / run / serve / config) | 🟡 **Partial** | Command scaffold present, wiring incomplete |
| Web dashboard + REST/WebSocket API | 🟡 **Partial** | API surface exists; execution path is *simulated* |
| Python bindings (PyO3) | 🟡 **Partial** | `validate` + async `execute` exposed |
| Terminal UI (TUI) | 🟡 **Partial** | Monitoring UI scaffold |
| Agent / ensemble / router node logic | 🔴 **Planned** | Currently echo/passthrough stubs |
| Content-addressable artifact store | 🔴 **Planned** | In-memory store works; SurrealDB store is a stub |
| Distributed checkpointing & time-travel | 🔴 **Planned** | Types defined; backend not implemented |
| RBAC, audit, SLA, compliance | 🔴 **Planned** | Config types defined; enforcement not implemented |
| Distributed / K8s / edge execution | 🔴 **Planned** | Feature flags exist; runtime not implemented |

Legend: ✅ Working · 🟡 Partial / scaffolded · 🟠 Code present but does not build · 🔴 Planned / Broken

> **The honest summary:** GaussFlow today is a *designed* execution substrate whose runtime crate
> does not yet compile, with the *front door* — the prompt-to-DAG compiler — not built at all.
> The core model (`gaussflow-core`) is real and compiles; the runtime is the right shape but needs
> a build fix before any execution claim can be made. The roadmap therefore starts by making the
> workspace build, then consolidates the engine, then builds the synthesis layer on top of it.

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
cargo build -p gaussflow-core   # ✅ compiles today
cargo build --workspace         # ⚠️ currently fails in gaussflow-runtime — see roadmap Phase 0
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

# Execute it (requires OPENAI_API_KEY for live LLM nodes,
# and a SurrealDB instance for run persistence)
export OPENAI_API_KEY=sk-...
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

> **Known limitations (tracked on the roadmap):** the runtime persists runs to SurrealDB using
> hardcoded credentials and returns a `run_id` rather than full node outputs; and there is no
> prompt-to-DAG step yet — you supply the `WorkflowSpec` the compiler will one day generate.

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
`data_processor`, `conditional`, `parallel`. Only `llm_call` has a full implementation today;
the others currently behave as passthrough handlers (see the status table above). The synthesis
layer can only safely emit node types that the runtime actually executes — which is why
"make the node types real" precedes "ship the compiler" on the roadmap.

---

## Development

```bash
cargo build -p gaussflow-core  # the core crate compiles today
cargo build --workspace        # ⚠️ currently fails in gaussflow-runtime (roadmap Phase 0)
cargo test --workspace         # ~69 tests exist but cannot run until the build is fixed
cargo run --example simple_workflow -p gaussflow-core
cargo bench -p gaussflow-runtime
```

> The test suite (~69 functions) is a genuine asset, but it is gated behind the runtime build
> failure: tests cannot currently execute workspace-wide. Restoring a green
> `cargo build --workspace` + `cargo test --workspace` is the first roadmap milestone.

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

Apache-2.0. See `LICENSE` (to be added) for details.

© 2026 Gaussian Technologies. GaussFlow™ is a trademark of Gaussian Technologies.
</content>
</invoke>
