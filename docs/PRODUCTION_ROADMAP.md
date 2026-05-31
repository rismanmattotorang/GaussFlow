# GaussFlow — Production Roadmap

**Owner:** Gaussian Technologies
**Goal:** Take GaussFlow from its current **Alpha / Technology Preview** state to a credible,
secure, well-documented **1.0 production release** that delivers the product vision:
**prompt → DAG → confirm → deploy → run.**
**Companion documents:** [`CODE_EVALUATION.md`](CODE_EVALUATION.md) ·
[`SYNTHESIS_PIPELINE.md`](SYNTHESIS_PIPELINE.md)

This plan is deliberately ordered by *risk reduction and trust*, not by feature breadth. The
guiding principle: **make the core narrow path correct, secure, and honest before widening it —
then build the prompt-to-DAG synthesis layer on top of a runtime we trust.**

### The north star (and why it comes last in sequence, first in priority)

The single capability that defines GaussFlow — synthesizing a DAG from a natural-language prompt
(**Phase S** below) — is the **highest-priority outcome** but is deliberately **sequenced after** core
consolidation and real node types. A compiler is only as trustworthy as the target it emits to:
the synthesis layer can only safely generate node types the runtime actually executes (Phase 2),
and it needs *one* correct, dependency-respecting engine (Phase 1) — not the three divergent
execution paths that exist today. Building synthesis on the current foundation would amount to
generating graphs that mostly do not run. So we earn the right to ship the front door by first
making the rooms behind it real.

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

## Phase S — The synthesis layer: prompt → DAG → confirm → deploy → run (Weeks 9–16) 🧠 the product

*Objective: deliver GaussFlow's defining capability — turn a natural-language prompt into a
validated, runnable agent graph that the user confirms before it ships. Full design in
[`SYNTHESIS_PIPELINE.md`](SYNTHESIS_PIPELINE.md). This phase overlaps Phases 2–4: it can begin as
soon as a trustworthy subset of node types and a single engine exist, and matures alongside them.*

This is the highest-value phase. Everything before it exists to make this phase safe to ship.

- [ ] **New `gaussflow-synth` crate.** Houses intake, the Plan IR, lowering, and the repair loop.
      Keep it cleanly separated from `core`/`runtime`.
- [ ] **Prompt intake → `SynthesisRequest`.** Capture the goal plus constraints (cost, latency,
      allowed tools, schedule, data scope) as explicit, enforceable guards.
- [ ] **Intent extraction → Plan IR.** LLM with structured output produces an abstract capability
      graph, *not* raw `WorkflowSpec`. Drive generation from a **capability catalog derived from
      actually-registered runtime handlers** — never from the `NodeType` enum (no inventing stubs).
- [ ] **Lowering: Plan IR → `WorkflowSpec`.** Map capabilities to concrete nodes, wire edges from
      data dependencies, assign resources/retry, and select models by cost/quality policy.
- [ ] **Validation gate + bounded self-repair.** Every synthesized spec must pass
      `TypeSafeDag::from_json`; on failure, feed validator errors back for ≤ N repair iterations,
      then fail loudly. Restrict emitted node types to those with real handlers (gates on Phase 2).
- [ ] **Confirmation UX (human-in-the-loop).** Render the proposed DAG (graph + JSON), show a
      per-node plain-language explanation with provenance to the prompt, and an estimated
      cost/latency/side-effect summary. Support edit / regenerate-with-feedback / confirm. Editing
      re-validates. **Nothing deploys without explicit confirmation.**
- [ ] **Deploy step.** Persist the confirmed, versioned, immutable spec; resolve providers and
      secrets from the secrets manager; reserve quotas; register triggers; emit a deployment handle.
- [ ] **Run + trace-back.** Execute on the canonical runtime; link every run to its originating
      prompt for auditability.
- [ ] **Synthesis benchmark suite.** A prompt corpus with expected capabilities; track
      first-pass validation rate, repair iterations, and run-success rate as regression gates.

**Exit criteria:** a natural-language prompt produces a `WorkflowSpec` that passes validation
unmodified ≥ 90% of the time (with bounded repair), uses only runtime-supported node types, can be
reviewed/edited/confirmed, and — once confirmed — deploys reproducibly and runs end-to-end with
real outputs. (See the acceptance criteria in [`SYNTHESIS_PIPELINE.md`](SYNTHESIS_PIPELINE.md).)

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
8. **A natural-language prompt synthesizes a validated DAG that the user confirms, deploys, and
   runs end-to-end** — i.e. the product vision works, not just the runtime.

---

## Rough sizing

| Phase | Duration | Theme |
|---|---|---|
| 0 | 1 week | Trust & safety |
| 1 | 3 weeks | Consolidate core |
| 2 | 5 weeks | Real node types |
| **S** | **7 weeks** | **Synthesis layer — the product** (overlaps 2–4) |
| 3 | 4 weeks | State & reliability |
| 4 | 4 weeks | Observability |
| 5 | 4 weeks | Security |
| 6 | 7 weeks | Scale-out |
| 7 | 5 weeks | Release engineering |

Phases S, 3–4 and 6–7 overlap. **Estimated calendar time to a defensible 1.0: ~6–7 months** with
a small focused team (2–4 engineers), front-loading Phases 0–2 (credibility per week) and then
Phase S (the capability that makes the product the product). Phase S can start mid-Phase-2 once a
trustworthy node subset and a single engine exist.
</content>
