# gaussflow-runtime

**The execution engine for [GaussFlow](../README.md), by Gaussian Technologies.**

`gaussflow-runtime` takes a validated `TypeSafeDag` from
[`gaussflow-core`](../gaussflow-core/README.md) and *runs it*: topological scheduling,
per-node timeouts, retries with backoff, resource-aware concurrency, and run persistence.

## What's inside

| Component | Purpose | Status |
|---|---|---|
| `execute` (`lib.rs`) | **Canonical** async topological executor | ✅ Working |
| `handler` | Per-node-type handlers (`NodeHandler` trait) | 🟡 Only `llm_call` (OpenAI) is real; others echo |
| `planner` | Execution planning helpers | 🟡 Partial |
| `sys` | Platform CPU/process introspection (Linux + fallback) | ✅ Working |
| `runtime/` submodule | Optimized work-stealing scheduler, pool, batcher | 🔴 Experimental — `execute` is `todo!()`, not wired in |

> **Important:** the public `execute()` in `lib.rs` is the engine that works today. The
> `runtime/` submodule (scheduler, executor, pool, steal_queue, batcher) is an unfinished
> parallel implementation kept for future distributed work; it is **not** the active path.
> Consolidation is tracked in the [Production Roadmap](../docs/PRODUCTION_ROADMAP.md).

## Current behavior & limitations

- Executes nodes in topological order with CPU/GPU semaphores and per-node retry/backoff.
- The LLM handler calls the OpenAI Chat Completions API and requires `OPENAI_API_KEY`.
- **Run persistence requires a live SurrealDB** at `ws://127.0.0.1:8000` — currently with
  hardcoded credentials (a Phase 0 fix on the roadmap) and not yet configurable.
- `execute()` returns a `{ run_id }` today rather than full node outputs (roadmap Phase 1).

## Example

```rust
use gaussflow_core::TypeSafeDag;

let dag = TypeSafeDag::from_json(&workflow_json)?;
let result = gaussflow_runtime::execute(dag, serde_json::json!({})).await?;
```

## Build, test, bench

```bash
cargo build -p gaussflow-runtime
cargo test  -p gaussflow-runtime
cargo bench -p gaussflow-runtime
```
</content>
