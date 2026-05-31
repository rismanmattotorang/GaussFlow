# gaussflow-core

**The graph model and primitives at the heart of [GaussFlow](../README.md), by Gaussian Technologies.**

`gaussflow-core` defines *what a workflow is* and the building blocks used to validate and
schedule it. It is the foundational, dependency-light crate that every other GaussFlow crate
builds on.

## What's inside

| Module | Purpose | Status |
|---|---|---|
| `model` | `WorkflowSpec`, `NodeSpec`, `EdgeSpec`, `NodeConfig`, settings (serde) | ✅ Working |
| `dag` | `TypeSafeDag` over petgraph; construction, validation, cycle detection | ✅ Working (deserialization TODO) |
| `validator` | DAG validation rules | ✅ Working |
| `resource` | Semaphore-based CPU/GPU/memory `ResourceManager` with RAII guards | ✅ Working |
| `scheduler` | Priority scheduler primitives | ✅ Working |
| `error` | Structured error enums, severity levels, recovery strategies | ✅ Working |
| `storage` | `ContentStore` trait + `InMemoryStore` | ✅ (in-memory) / 🔴 (SurrealDB stub) |
| `engine` | Legacy in-crate execution engine | ⚠️ Superseded — see note |
| `checkpoint`, `versioning` | Checkpoint/version types | 🟡 Types defined, backend planned |
| `node`, `policy` | Enterprise policy/security types | 🟡 Schema only, no enforcement |

> **⚠️ Note on `engine`:** the canonical execution path lives in
> [`gaussflow-runtime`](../gaussflow-runtime/README.md). The `engine` module here contains
> earlier execution code with known dependency-ordering issues and is slated for consolidation
> (see the [Production Roadmap](../docs/PRODUCTION_ROADMAP.md), Phase 1).

## Example

```rust
use gaussflow_core::TypeSafeDag;

let json = std::fs::read_to_string("workflow.json")?;
let dag = TypeSafeDag::from_json(&json)?;
println!("Loaded '{}' with {} nodes, {} edges",
    dag.name, dag.graph.node_count(), dag.graph.edge_count());
```

## Build & test

```bash
cargo build -p gaussflow-core
cargo test  -p gaussflow-core
cargo run --example simple_workflow -p gaussflow-core
```

See [examples/](examples/) for runnable workflows.
</content>
