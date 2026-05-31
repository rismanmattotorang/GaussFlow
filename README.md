<div align="center">

# GaussFlow™

### The orchestration engine for multi-LLM and agentic AI workflows

**A product by [Gaussian Technologies](#about-gaussian-technologies)**

[![Status](https://img.shields.io/badge/status-Technology%20Preview%20(Alpha)-orange)](docs/CODE_EVALUATION.md)
[![Language](https://img.shields.io/badge/built%20with-Rust-000000?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)
[![Crates](https://img.shields.io/badge/workspace-6%20crates-informational)]()

*Define your AI pipeline as a graph. Let GaussFlow run it — fast, safe, and observable.*

</div>

---

> **⚠️ Project status: Technology Preview (Alpha).**
> GaussFlow is under active development. The core graph model, workflow parser, and a
> single-machine execution runtime work today. Many advanced capabilities described in our
> vision (distributed execution, full enterprise governance, persistent graph storage) are
> **partially implemented or planned**. For an honest, file-level breakdown of what is real
> versus aspirational, read the **[Code Evaluation](docs/CODE_EVALUATION.md)**. For the path
> to a 1.0 production release, see the **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)**.

---

## Why GaussFlow

Modern AI products are no longer a single model call. They are *pipelines*: retrieve, route to
the cheapest capable model, run several agents in parallel, aggregate, validate, retry on
failure, and checkpoint along the way. Stitching this together with Python glue scripts is
fragile, slow, and hard to observe.

GaussFlow models the whole pipeline as a **typed Directed Acyclic Graph (DAG)** and executes it
with a Rust core built for concurrency and correctness:

- **Describe, don't script.** Author workflows as JSON (compatible with n8n-style exports) or
  via the Rust/Python API. The engine handles scheduling, dependencies, and data flow.
- **Rust core, predictable performance.** Async execution on Tokio, semaphore-based resource
  control, and zero-GC latency.
- **Built for LLMs and agents.** First-class node types for LLM calls, agents, ensembles,
  routers, and nested subgraphs.
- **Observable by design.** Structured tracing, execution metrics, and run persistence so you
  can see exactly what happened.

---

## Feature status at a glance

We believe an engineering tool should tell you the truth about its own maturity. Here is where
each capability stands today.

| Capability | Status | Notes |
|---|---|---|
| Workflow JSON → typed DAG parsing | ✅ **Working** | `gaussflow-core`, cycle/validation checks |
| Topological single-machine execution | ✅ **Working** | `gaussflow-runtime`, Tokio-based |
| LLM node (OpenAI Chat Completions) | ✅ **Working** | Reads `OPENAI_API_KEY` from env |
| Resource control (CPU/GPU semaphores) | ✅ **Working** | Concurrency limits, per-node timeouts |
| Retry with backoff (fixed/linear/exp) | ✅ **Working** | Per-node retry policy |
| Run persistence (SurrealDB) | 🟡 **Partial** | Hardcoded creds; needs config + graceful fallback |
| CLI (validate / run / serve / config) | 🟡 **Partial** | Command scaffold present, wiring incomplete |
| Web dashboard + REST/WebSocket API | 🟡 **Partial** | API surface exists; execution path is simulated |
| Python bindings (PyO3) | 🟡 **Partial** | `validate` + async `execute` exposed |
| Terminal UI (TUI) | 🟡 **Partial** | Monitoring UI scaffold |
| Agent / ensemble / router node logic | 🔴 **Planned** | Currently echo/passthrough stubs |
| Content-addressable artifact store | 🔴 **Planned** | In-memory store works; SurrealDB store is a stub |
| Distributed checkpointing & time-travel | 🔴 **Planned** | Types defined; backend not implemented |
| RBAC, audit, SLA, compliance | 🔴 **Planned** | Config types defined; enforcement not implemented |
| Distributed / K8s / edge execution | 🔴 **Planned** | Feature flags exist; runtime not implemented |

Legend: ✅ Working · 🟡 Partial / scaffolded · 🔴 Planned

---

## Architecture

GaussFlow is a Cargo workspace of focused crates:

```
                         ┌─────────────────────────────────────────────┐
                         │                Interfaces                    │
                         │  gaussflow-cli   gaussflow-web   gaussflow-tui│
                         │  gaussflow-py (Python bindings, PyO3)         │
                         └───────────────────────┬─────────────────────┘
                                                 │
                         ┌───────────────────────▼─────────────────────┐
                         │             gaussflow-runtime                │
                         │  Topological executor · node handlers ·      │
                         │  retry/backoff · resource semaphores ·       │
                         │  run persistence                             │
                         └───────────────────────┬─────────────────────┘
                                                 │
                         ┌───────────────────────▼─────────────────────┐
                         │              gaussflow-core                  │
                         │  TypeSafeDag (petgraph) · WorkflowSpec model │
                         │  validator · scheduler · resource manager ·  │
                         │  checkpoint/versioning/storage traits        │
                         └─────────────────────────────────────────────┘
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

### Prerequisites
- Rust 1.75+ and Cargo
- (Optional) An `OPENAI_API_KEY` for live LLM nodes
- (Optional) A running [SurrealDB](https://surrealdb.com/) instance for run persistence

### Build

```bash
git clone <your-fork-url> gaussflow
cd gaussflow
cargo build --workspace
```

### Define a workflow

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

> **Known limitation:** today the runtime persists runs to a SurrealDB instance using
> hardcoded credentials and returns a `run_id` rather than the full node outputs. This is one
> of the first items on the [Production Roadmap](docs/PRODUCTION_ROADMAP.md).

---

## Workflow specification

A workflow is `name`, a list of `nodes`, a list of `connections` (edges), and `settings`:

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
  "settings": {
    "concurrency": 8,
    "fail_fast": false,
    "resume": true
  }
}
```

Supported node types in the core model: `llm_call`, `agent`, `ensemble`, `router`, `subgraph`,
`data_processor`, `conditional`, `parallel`. Only `llm_call` has a full implementation today;
the others currently behave as passthrough handlers (see the status table above).

---

## Development

```bash
cargo build --workspace        # build everything
cargo test --workspace         # run the test suite (~69 tests)
cargo run --example simple_workflow -p gaussflow-core
cargo bench -p gaussflow-runtime
```

See [`build.sh`](build.sh) and [`clean.sh`](clean.sh) for convenience scripts.

Contributions are welcome. The most valuable contributions right now are the items tracked in
the [Production Roadmap](docs/PRODUCTION_ROADMAP.md).

---

## Documentation

- 📋 **[Code Evaluation](docs/CODE_EVALUATION.md)** — honest engineering assessment of the
  current codebase: what works, what's a stub, and the risks.
- 🗺️ **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)** — the phased plan to take GaussFlow
  from Alpha to a production 1.0.
- 📐 **[GaussFlow Specs](GAUSSFLOW-SPECS.md)** — the original design vision.

---

## About Gaussian Technologies

**Gaussian Technologies** is a deep-tech startup building the infrastructure layer for
production AI. We believe the next decade of software will be defined not by single models, but
by *systems* of models, agents, and tools working together — and that those systems deserve an
execution substrate that is fast, type-safe, observable, and honest about its guarantees.

GaussFlow is our flagship open engine for orchestrating those systems. Named for Carl Friedrich
Gauss — and for the distributions at the heart of modern machine learning — it reflects our
engineering values: rigor, precision, and elegant foundations.

> *Orchestrate intelligence.*

---

## License

Apache-2.0. See `LICENSE` (to be added) for details.

© 2026 Gaussian Technologies. GaussFlow™ is a trademark of Gaussian Technologies.
</content>
</invoke>
