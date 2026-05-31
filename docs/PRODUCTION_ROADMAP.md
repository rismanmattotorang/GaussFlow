# GaussFlow — Production Roadmap

**Owner:** Gaussian Technologies
**Goal:** Take GaussFlow from its current **Alpha / Technology Preview** state to a credible,
secure, well-documented **1.0 production release**.
**Companion document:** [`CODE_EVALUATION.md`](CODE_EVALUATION.md)

This plan is deliberately ordered by *risk reduction and trust*, not by feature breadth. The
guiding principle: **make the core narrow path correct, secure, and honest before widening it.**

---

## Guiding principles

1. **One source of truth.** Every concept (DAG, node, execution) should have exactly one
   implementation. Delete duplicates.
2. **Honesty over hype.** Documentation must track reality. A feature is "done" only when it is
   implemented, tested, and documented.
3. **Secure by default.** No secrets in source. No insecure defaults that ship.
4. **Vertical slices.** Ship a thin, fully-working end-to-end path, then deepen it.

---

## Phase 0 — Stop the bleeding (Week 1) ✅ trust & safety

*Objective: remove the things that are actively harmful or misleading.*

- [x] **Rewrite the README and docs to reflect actual status** (this PR).
- [ ] **Purge hardcoded secrets.** Remove `REDACTED` and `"REDACTED"` from
      `gaussflow-runtime/src/lib.rs`, `gaussflow-cli/src/database.rs`, `gaussflow-cli/src/config.rs`.
      Source credentials from environment variables / a secrets provider only.
- [ ] **Rotate any real credentials** that may have been committed; scrub git history if needed.
- [ ] **Add `.github/workflows/ci.yml`**: `cargo build`, `cargo test`, `cargo clippy -D warnings`,
      `cargo fmt --check`, and `cargo audit` on every PR.
- [ ] **Add `cargo-deny` / `cargo-audit`** for dependency and license scanning.
- [ ] **Add a `LICENSE` file** (Apache-2.0 per README) and `SECURITY.md`.

**Exit criteria:** no secrets in the tree; CI is green and gating; docs are honest.

---

## Phase 1 — Consolidate the core (Weeks 2–4) 🧱 foundation

*Objective: one data model, one execution engine, one correct path.*

- [ ] **Choose a canonical data model.** Keep `gaussflow-core/src/model.rs`; remove or merge the
      competing types in `gaussflow-core/src/node.rs`. Document the schema.
- [ ] **Choose a canonical engine.** Promote `gaussflow-runtime/src/lib.rs::execute` as the one
      true executor. **Delete or quarantine** the `runtime/runtime/` parallel engine (with the
      `todo!()`), or formally mark it `experimental` behind a feature flag with no public re-export.
- [ ] **Remove the broken core engine paths.** Either fix or remove
      `core::engine::execute` / `execute_workflow` (dependency-ordering and node-id/index bugs).
      Do not ship two more execution implementations.
- [ ] **Decouple execution from SurrealDB.** Introduce a `RunStore` trait with an in-memory
      default so workflows can run with zero external dependencies; SurrealDB becomes one
      pluggable backend.
- [ ] **Return real outputs.** `execute` should return node outputs, not just a `run_id`.
- [ ] **Implement `TypeSafeDag` deserialization** (`dag.rs:254`) or remove the API.
- [ ] **Property-test the scheduler:** generate random DAGs, assert topological correctness and
      that no node runs before its dependencies.

**Exit criteria:** one model, one engine; `cargo run -p gaussflow-cli -- run workflow.json`
executes a multi-node DAG correctly with no database required.

---

## Phase 2 — Make the node types real (Weeks 5–9) 🤖 capability

*Objective: deliver the node types the product promises.*

- [ ] **LLM provider abstraction.** Define a `Provider` trait; refactor the OpenAI handler
      behind it. Add at least one more provider (Anthropic) and a local/Ollama option.
- [ ] **Router node.** Real provider/branch selection from policy (cost, capability, edge
      conditions). Implement edge `condition` evaluation (`${input.score > 0.7}`).
- [ ] **Ensemble node.** Parallel fan-out to N children + pluggable aggregation
      (vote / concat / reduce).
- [ ] **Agent node.** A real tool-use/reasoning loop with a bounded step budget.
- [ ] **Subgraph node.** Spawn a nested engine instance and stitch results back.
- [ ] **Data/transform/conditional/parallel nodes.** Implement the deterministic compute nodes.
- [ ] **Streaming.** Token streaming for LLM nodes surfaced over the API/WebSocket.

**Exit criteria:** every node type in `NodeType` either has a real implementation or is removed
from the public enum and docs.

---

## Phase 3 — State, persistence & reliability (Weeks 10–13) 💾 durability

*Objective: workflows survive failures and can resume.*

- [ ] **Real content-addressable artifact store** backed by a durable backend (replace the
      `SurrealStore`/`SkyCache` dummies).
- [ ] **Checkpointing** that actually persists `node_results` and `WorkflowStatus`, with
      `resume` from a checkpoint working end-to-end.
- [ ] **Idempotency & exactly-once semantics** for node execution on retry/resume.
- [ ] **Backpressure & flow control** on the typed data channels.
- [ ] **Failure injection tests** (kill mid-run, resume, assert correctness).

**Exit criteria:** a long workflow can be interrupted and resumed to the correct final state.

---

## Phase 4 — Observability & operability (Weeks 12–15, overlaps P3) 🔭

*Objective: you can see and debug what the engine is doing in production.*

- [ ] **Real metrics.** Replace `prometheus_metrics() -> "Metrics not available"` with a true
      Prometheus exporter; remove the dual stubs. Standardize on the `metrics` feature.
- [ ] **Structured tracing** with OpenTelemetry export; one trace per run, one span per node.
- [ ] **Health/readiness endpoints** wired to real engine state in `gaussflow-web`.
- [ ] **Wire the web dashboard to the real engine.** Remove `simulate_execution`; stream actual
      run events over WebSocket.
- [ ] **Structured logs** with run/node correlation IDs.

**Exit criteria:** a Grafana dashboard shows real run/node metrics; the web UI reflects real
executions.

---

## Phase 5 — Security & multi-tenancy (Weeks 16–19) 🔐 enterprise-readiness

*Objective: make the "enterprise" types actually enforce something.*

- [ ] **AuthN/AuthZ on the web API** (JWT/OAuth2) — secret from a vault, never defaulted.
- [ ] **RBAC enforcement** that consumes the existing policy types.
- [ ] **Audit logging** that emits real, tamper-evident audit events.
- [ ] **Secrets management** integration (env injection + HashiCorp Vault / cloud secret managers).
- [ ] **PII detection/redaction** middleware wired into the data path (currently config-only).
- [ ] **Input validation & resource quotas** to prevent abuse / runaway workflows.
- [ ] **Threat model + external security review** of the API and execution sandbox.

**Exit criteria:** the API rejects unauthorized requests; audit trail is verifiable; a security
review has been completed and findings closed.

---

## Phase 6 — Scale-out (Weeks 20–26) 🌐 distribution

*Objective: deliver on the distributed-execution vision — only after single-node is rock solid.*

- [ ] **Distributed executor.** Move from single-process to a coordinator/worker model
      (the quarantined `runtime/runtime/` work-stealing scheduler may be salvageable here).
- [ ] **Data transport** between workers (e.g., Arrow Flight) for large payloads.
- [ ] **Kubernetes deployment.** Real Dockerfile, Helm chart, autoscaling, resource requests.
- [ ] **Horizontal scaling & load tests** with documented throughput/latency numbers
      (replace the unsubstantiated "performance targets met" claim with real benchmarks).

**Exit criteria:** a workflow runs across multiple worker nodes with published benchmark results.

---

## Phase 7 — Release engineering & GA (Weeks 24–28) 🚀

- [ ] **API stability review** and semantic-versioning commitment for `gaussflow-core`.
- [ ] **Publish crates** to crates.io and the Python package to PyPI (via `maturin`).
- [ ] **Versioned docs site**, tutorials, and a cookbook of real workflows.
- [ ] **Reference deployments** (Docker Compose for local, Helm for K8s).
- [ ] **Migration guide** and changelog discipline.
- [ ] **Close out doc-coverage warnings** (~398 noted in `TODO.md`).

**Exit criteria:** GaussFlow 1.0 tagged, published, documented, and deployable by a third party
without reading the source.

---

## Cross-cutting workstreams (continuous)

- **Test coverage** target ≥ 80% on `core` + `runtime`; coverage gate in CI.
- **Benchmark regression tracking** via Criterion in CI.
- **Dependency hygiene** via `cargo audit` / `cargo deny` on a schedule.
- **Documentation-as-contract:** a feature is not "done" until README status table, crate docs,
  and tests all reflect it.

---

## Suggested 1.0 definition of done

GaussFlow 1.0 ships when:
1. There is exactly one data model and one execution engine, both tested.
2. No secrets or insecure defaults exist in the source tree.
3. Every advertised node type works or has been removed from the public surface.
4. Workflows can checkpoint and resume correctly.
5. The web UI and API reflect real executions with real metrics and auth.
6. CI gates build, lint, test, coverage, and security audit.
7. The README's status table is **all ✅** for everything it lists — or the row is removed.

---

## Rough sizing

| Phase | Duration | Theme |
|---|---|---|
| 0 | 1 week | Trust & safety |
| 1 | 3 weeks | Consolidate core |
| 2 | 5 weeks | Real node types |
| 3 | 4 weeks | State & reliability |
| 4 | 4 weeks | Observability |
| 5 | 4 weeks | Security |
| 6 | 7 weeks | Scale-out |
| 7 | 5 weeks | Release engineering |

Phases 3–4 and 6–7 overlap. **Estimated calendar time to a defensible 1.0: ~6 months** with a
small focused team (2–4 engineers), front-loading Phases 0–2 which deliver the most credibility
per week of effort.
</content>
