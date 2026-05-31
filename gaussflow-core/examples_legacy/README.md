# Legacy examples (quarantined)

These example programs target the **pre-consolidation** executor / engine API and **no longer
compile** against the current code. They were moved out of `examples/` (where Cargo would
auto-compile them) so that `cargo build --workspace` and `cargo test --workspace` stay green.

They are kept as reference material and will be **rewritten against the canonical engine in
roadmap Phase 1** (see `../../docs/PRODUCTION_ROADMAP.md`). Do not assume they reflect the
current API.

| File | Targets |
|---|---|
| `simple_workflow.rs` | Legacy `AsyncNodeExecutor`/`ExecutionEngine` API |
| `advanced_workflow.rs` | Legacy engine + node types (incl. removed variants) |
| `error_handling.rs` | Legacy `EngineError` variants |
| `parallel_processing.rs` | Legacy engine concurrency API |
