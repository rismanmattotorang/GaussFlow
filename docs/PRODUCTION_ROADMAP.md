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

- [x] **Make `cargo build --workspace` pass — task #1, blocks everything.** ✅ **Done.** The
      `gaussflow-runtime` failures were fixed:
  - Root cause was a **misplaced `[target.'cfg(...)'.dependencies]` table header** that orphaned
    `tracing` + `gaussflow-core` onto the non-Linux target; deps moved back to `[dependencies]`.
  - Removed the invalid `features = ["rocksdb"]` on the `gaussflow-core` path dependency.
  - Made `tracing`/`tracing-subscriber` non-optional (they are imported unconditionally);
    declared the `metrics` feature; dropped two unused, conflict-prone otel crates.
  - Fixed the `procfs` 0.14 API drift in `sys/linux.rs` (`Status` is a struct, not a tuple).
  - Corrected two `[[test]]`/`[[bench]]` paths pointing at non-existent files.
  - `protoc` documented as a prerequisite and installed in CI.
- [x] **Get `cargo test --workspace` green.** ✅ **Done.** Fixed three real unit-test issues
      (cache `remove` used `demote` instead of `pop`; `format_duration` used `{:.2}` on an
      integer; a flaky sub-second uptime assertion; a malformed semver range in a versioning
      test). Stale tests/examples targeting the legacy/dead engine paths are **quarantined**
      behind a `legacy_tests` feature (off by default) with TODO notes pointing to Phase 1;
      stale example programs moved to `examples_legacy/`.
- [x] **Rewrite the README and docs to reflect actual status**, including the (now fixed) build
      story and the `protoc` prerequisite.
- [x] **Purge hardcoded secrets.** ✅ **Done.** Removed `REDACTED` and `"REDACTED"`
      from `gaussflow-runtime/src/lib.rs`, `gaussflow-cli/src/{server,database,config}.rs`. All
      credentials are now sourced from environment variables (`GAUSSFLOW_DB_PASS`,
      `GAUSSFLOW_JWT_SECRET`, …) with local-dev fallbacks; the JWT secret defaults to empty so
      `Config::validate()` rejects an unconfigured secret. See `.env.example` / `SECURITY.md`.
- [ ] **Rotate any real credentials** that may have been committed; scrub git history if needed.
      *(Operational follow-up: the strings are gone from `HEAD` but remain in history.)*
- [x] **Add `.github/workflows/ci.yml`.** ✅ **Done.** Installs `protoc`, then **gates** on
      `cargo build --workspace --locked` and `cargo test --workspace --locked`. `cargo fmt
      --check`, `cargo clippy`, and `cargo audit` run as **informational (non-gating)** for now —
      the tree has ~700 rustfmt diffs and ~166 clippy warnings (0 errors) that are a separate
      cleanup workstream; these checks become gating once that debt is paid down.
- [ ] **Add `cargo-deny`** for license scanning (`cargo-audit` is wired into CI).
- [x] **Add a `LICENSE` file** (Apache-2.0) and `SECURITY.md`. ✅ **Done.**

**Exit criteria:** ✅ `cargo build --workspace` and `cargo test --workspace` pass on a clean
checkout (with `protoc` installed via CI); ✅ no secrets in the tree; ✅ CI gates build + test;
✅ docs are honest. **Remaining:** make lint/fmt/audit gating (debt cleanup) and scrub git
history of the old secrets.

---

## Phase 1 — Consolidate the core (Weeks 2–4) 🧱 foundation ✅ core objectives done

*Objective: one data model, one execution engine, one correct path.*

- [x] **Choose a canonical data model.** ✅ `gaussflow-core/src/model.rs` is canonical; the
      competing duplicate types in `node.rs` (`NodeNodeSpec`/`NodeNodeType`/policy structs, used by
      nothing) were removed along with their re-exports.
- [x] **Choose a canonical engine.** ✅ `gaussflow_runtime::execute_with_store` is the one true
      executor. The dead `runtime/runtime/` parallel engine (the `todo!()` work-stealing scheduler,
      never declared as a module) was **deleted** along with its tests and benches — recoverable
      from git history if Phase 6 wants to salvage the work-stealing scheduler.
- [x] **Remove the broken core engine paths.** ✅ Deleted `core::engine` (the duplicate
      `execute`/`execute_workflow` with the dependency-ordering and node-id/index bugs) and the
      dead `node_executor.rs`. There is now a single execution implementation.
- [x] **Decouple execution from SurrealDB.** ✅ New `RunStore` trait (`gaussflow-runtime/src/store.rs`)
      with a dependency-free `InMemoryRunStore` **default** — workflows run with zero external
      services. `SurrealRunStore` is one pluggable backend, opt-in via `GAUSSFLOW_RUN_STORE=surreal`.
- [x] **Return real outputs.** ✅ `execute` now returns `{ run_id, output, outputs }` with the full
      per-node output map, not just a `run_id`.
- [x] **Implement `TypeSafeDag` deserialization or remove the API.** ✅ Removed the misleading
      `serialize`/`deserialize`/`partition` stubs (one panicked via `unimplemented!()`, one silently
      returned `""`); `TypeSafeDag::from_json` is the canonical entry point.
- [x] **Fix the CLI so it actually runs.** ✅ Fixed a `clap` `-c` short-option collision (global
      `--config` vs `--cache`) and invalid `null` values in `config/default.toml` that panicked the
      binary on every invocation. `validate` and `run` now work.
- [x] **Property-test the scheduler.** ✅ `tests/scheduler_proptest.rs` generates random acyclic
      DAGs (up to 8 nodes, 200 cases) and, via a recording handler injected through the new
      `execute_with` seam, asserts that every node runs exactly once and **no node runs before any
      of its dependencies**.

**Exit criteria:** ✅ one model, one engine; `cargo run -p gaussflow-cli -- run workflow.json`
executes a multi-node DAG correctly **with no database required** (verified end-to-end, by the
`no_db_execution` integration test, and by the `scheduler_proptest` property test). **Phase 1
complete.**

---

## Phase 2 — Make the node types real (Weeks 5–9) 🤖 capability ✅ exit criterion met

*Objective: deliver the node types the product promises. **All eight `NodeType` variants now have
real implementations**; the remaining items (extra providers, streaming) are enhancements.*

- [x] **LLM provider abstraction.** ✅ `gaussflow-runtime/src/provider.rs` defines an `LlmProvider`
      trait; the OpenAI handler is refactored behind it (`OpenAiProvider`), and a deterministic,
      offline `MockProvider` enables tests/local runs with no API key (`provider_for` selects by
      model name / `GAUSSFLOW_LLM_PROVIDER`). *Remaining: real Anthropic + local/Ollama providers.*
- [x] **Source-node input flow.** ✅ (engine prerequisite discovered in Phase 2) Source nodes now
      receive the workflow run input; previously they got an empty object.
- [x] **Data/transform nodes.** ✅ `DataProcessor` performs real deterministic transforms
      (`passthrough` / `extract` / `set`); `Conditional` evaluates real comparisons
      (`eq`/`ne`/`gt`/`lt`/`ge`/`le`) and emits a `matched`/`branch` decision. Covered by the
      `node_handlers` tests.
- [x] **Conditional/Router edge traversal.** ✅ The engine now evaluates each outgoing edge's `on`
      label against the source node's output and **skips** nodes on untaken branches (`edge_taken`
      in `gaussflow-runtime/src/lib.rs`): `success`/`failure` plus arbitrary labels matched against
      a node's `branch`/`route` output. Covered by the `edge_traversal` tests.
- [x] **Router node.** ✅ `RouterHandler` switches on an input field via a `routes` map (+ `default`)
      and emits a `route` the engine uses to take the matching edge. *Policy-based selection
      (cost/capability) remains future work.*
- [x] **Subgraph node.** ✅ `SubgraphHandler` runs an inline nested workflow on a fresh in-memory
      engine and surfaces its result. (Dispatch no longer aliases Subgraph to the agent stub.)
- [x] **Per-predecessor inputs (named ports).** ✅ Engine prerequisite for fan-in: the
      `NodeHandler` trait now receives `NodeInput { merged, sources }`, where `sources` is each
      taken predecessor's `(id, output)` individually (the shallow `merged` view is lossy when
      predecessors share keys).
- [x] **Ensemble node.** ✅ `EnsembleHandler` aggregates its members via `strategy`:
      `collect` (default), `first`, and `vote` (majority over a `field`, with full tally). Covered
      by the `ensemble` tests.
- [x] **Agent node.** ✅ `AgentHandler` is a bounded tool-use loop: each step takes a directive
      (`{tool,args}` → run a built-in tool and continue, or `{final}` → stop) from the LLM provider
      or, for deterministic offline runs/tests, from a `script` param. Honors a `max_steps` budget;
      built-in tools `echo`/`upper`/`sum`. Covered by the `agent_parallel` tests.
- [x] **Parallel node.** ✅ `ParallelHandler` runs an array of inline `branches` concurrently (each
      on its own in-memory engine) and collects their outputs in order.
- [x] **More providers.** ✅ Anthropic (Messages API) and a local **Ollama** provider added behind
      `LlmProvider`, alongside OpenAI and the offline mock; selected by model name (`gpt*`/`claude*`/
      `ollama/*`/`mock*`) or `GAUSSFLOW_LLM_PROVIDER`.
- [ ] **Streaming.** Token streaming for LLM nodes surfaced over the API/WebSocket. *(enhancement)*

**Exit criteria:** ✅ **met** — every `NodeType` variant has a real implementation: `llm_call`,
`agent`, `ensemble`, `router`, `subgraph`, `data_processor`, `conditional`, `parallel`. The
remaining provider/streaming items are enhancements, not blockers.

---

## Phase S — The synthesis layer: prompt → DAG → confirm → deploy → run 🧠 the product ✅ complete

*Objective: deliver GaussFlow's defining capability — turn a natural-language prompt into a
validated, runnable agent graph that the user confirms before it ships. Full design in
[`SYNTHESIS_PIPELINE.md`](SYNTHESIS_PIPELINE.md).*

This is the highest-value phase. Everything before it existed to make this phase safe to ship.
**A working first version now exists** in the `gaussflow-synth` crate, exercised end-to-end offline.

- [x] **New `gaussflow-synth` crate.** ✅ Houses intake, the Plan IR (`plan.rs`), the capability
      catalog, lowering, the synthesize/repair orchestrator, and a deploy/run entrypoint. Cleanly
      separated; depends on `core` (validator) and `runtime` (provider + executor).
- [x] **Prompt intake → `SynthesisRequest`.** ✅ Captures the goal plus `Constraints`
      (`allowed_capabilities`). (Cost/latency/schedule guards remain to be added.)
- [x] **Intent extraction → Plan IR.** ✅ The LLM (`LlmProvider`) produces an abstract capability
      graph parsed into `PlanIR` — not raw `WorkflowSpec`. Generation is driven from a
      **capability catalog kept in lockstep with the runtime handlers** (`catalog.rs`), so it
      cannot propose a node type the runtime only stubs.
- [x] **Lowering: Plan IR → `WorkflowSpec`.** ✅ Deterministic `lower()` maps each capability to a
      concrete `{ id, type, params }` node and wires edges from `depends_on` (with branch labels
      from each step's `edges`). (Resource/retry assignment + cost-based model selection remain.)
- [x] **Validation gate + bounded self-repair.** ✅ Every synthesized spec must pass
      `TypeSafeDag::from_json`; on failure (or an unsupported capability), the validator error is
      fed back for ≤ N repair iterations, then fails loudly (`SynthError::Unconverged`).
- [x] **Run + deploy entrypoint.** ✅ `gaussflow_synth::run` executes a confirmed spec on the
      canonical runtime (no database). CLI: `gaussflow synth "<prompt>" [--run]`.
- [x] **Confirmation + estimates.** ✅ `SynthesisResult::explanation` renders a per-step,
      provenance-bearing plan, and `PlanEstimate` reports node count, model-invocation upper bound
      (incl. agent budgets), and whether the workflow makes external calls. The CLI prints both
      before `--run`, and `--save <path>` writes the spec for hand-editing.
- [x] **Edit → re-validate.** ✅ `validate_plan()` re-runs the deterministic catalog + lower + DAG
      validation on a (hand-)edited `PlanIR` with no LLM; `PlanIR` has edit helpers
      (`set_param`/`add_step`/`remove_step` with reference scrubbing). `Synthesizer::regenerate`
      re-synthesizes with user feedback.
- [x] **Synthesis benchmark suite.** ✅ `gaussflow-synth/tests/benchmark.rs`: a 7-case corpus
      (one capability each + a repair case) pushed through synthesize → validate → run, reporting
      first-pass validation rate, avg repair iterations, and run-success rate with regression-gate
      asserts (currently 100% validated, 100% run, 86% first-pass). *Measuring real planning
      quality needs a live provider; the corpus uses canned plans as stand-ins.*
- [x] **Deploy hardening.** ✅ `deploy.rs`: confirmed results become **versioned, immutable**
      `Deployment` records (SHA-256 content hash; per-name version; rejects overwriting with
      different content) carrying the **originating prompt (provenance)**. `run_deployment`
      executes on the canonical runtime and links each run back to its deployment (**trace-back**).
      `DeploymentStore` has in-memory and file-backed (`FileDeploymentStore`) implementations.
      Deployments now also: record their **required secrets** (inferred from node models, recursing
      into subgraph/parallel) and validate them via a `SecretProvider` (`check_secrets`); enforce a
      resource **`Quota`** (max nodes / model invocations / external-calls ban) at deploy time; and
      register **`Trigger`s** (manual / schedule / webhook). CLI: `gaussflow synth … --deploy
      [--deploy-dir] [--schedule CRON] [--max-model-calls N]`. *Remaining: an executor that fires
      scheduled triggers, and a concrete vault/cloud secret-manager backend.*
- [x] **Production planning provider.** ✅ Planning runs on the real `LlmProvider`s — OpenAI and
      **Anthropic** (selected by model name: `gpt*`/`claude*`, or `GAUSSFLOW_LLM_PROVIDER`).
      `gaussflow synth "<prompt>" --model claude-3-5-sonnet` plans for real. A local **Ollama**
      provider is also available (`ollama/<model>`, via `OLLAMA_HOST`).
- [x] **Real planning provider.** ✅ Planning runs on OpenAI or Anthropic (by model name);
      offline tests use a scripted stub provider.

**Exit criteria ✅ met:** a prompt produces a `WorkflowSpec` that passes the same validator a
hand-authored graph does, uses only runtime-supported node types (with bounded repair), is
rendered for confirmation (with a cost estimate), can be edited/re-validated/regenerated, and —
once confirmed — is deployed (versioned, immutable, with provenance + required-secrets + quota +
triggers) and runs end-to-end with real outputs and run trace-back. Planning runs on OpenAI,
Anthropic, or local Ollama; the deterministic pipeline is proven offline by the synth test suite
and benchmark. **Remaining (deferred, smaller):** an executor that fires scheduled triggers, a
vault/cloud secret-manager backend, and a richer interactive edit UI.

---

## Phase 3 — State, persistence & reliability (Weeks 10–13) 💾 durability ✅ complete

*Objective: workflows survive failures and can resume.*

- [x] **Checkpointing.** ✅ `gaussflow-runtime/src/checkpoint.rs`: a `CheckpointStore` trait
      (`Noop`/`InMemory`/`File`, atomic write-then-rename) persisting the canonical
      `gaussflow_core::model::Checkpoint` (`node_results` + `WorkflowStatus`) keyed by run id. The
      executor writes a checkpoint after every node.
- [x] **Resume from a checkpoint, end-to-end.** ✅ `execute_resumable[_with]` resumes a run from
      its checkpoint, replaying only the not-yet-completed nodes; an already-`Completed` run
      returns its recorded result.
- [x] **Idempotency & exactly-once on resume.** ✅ Nodes already recorded in the checkpoint are not
      re-executed on resume (a failed node is *not* checkpointed, so it — and only it — is retried).
- [x] **Failure-injection tests.** ✅ `tests/resume.rs` crashes a run mid-way (node `b` fails),
      resumes, and asserts the completed node ran exactly once, the failed node was retried, and
      the run reached the correct final state; plus completed-run no-op and file-checkpoint
      durability across a fresh store.
- [x] **Real content-addressable artifact store.** ✅ `gaussflow-core/src/storage.rs`: the dummy
      `SurrealStore`/`SkyCache` are removed; `ContentStore` now has the in-memory `InMemoryStore`
      and a **durable** `FileContentStore` — SHA-256 content-addressed (automatic dedup, immutable
      keys), atomic write-then-rename, `put`/`get`/`has` + a clear `ArtifactNotFound` error. Tested
      for round-trip, dedup, and cross-store durability.
- [→] **Backpressure & flow control** — **relocated to Phase 6 (scale-out).** As originally written
      this described typed data channels in the *removed* parallel/work-stealing engine. The
      consolidated engine is topologically sequential with **no unbounded queues**; its flow-control
      primitive is the per-node concurrency semaphore (`settings.concurrency`). Meaningful
      backpressure returns only with **concurrent/distributed execution** — and, notably, bounded
      concurrency trades off against the *per-node* checkpointing that powers fine-grained resume
      (Phase 3's headline). So it correctly belongs with scale-out, not durability.

**Exit criteria:** ✅ **met** — an interrupted run resumes to the correct final state
(`tests/resume.rs`), and durable content-addressed artifacts are available
(`storage::FileContentStore`). **Phase 3 complete.**

---

## Phase 4 — Observability & operability (Weeks 12–15, overlaps P3) 🔭 — in progress

*Objective: you can see and debug what the engine is doing in production.*

- [x] **Real metrics.** ✅ `gaussflow-runtime/src/metrics.rs`: a real registry the executor feeds
      (runs started/completed/failed; node counts by type+outcome; node-execution-time summary).
      `prometheus_metrics()` now renders the **Prometheus text exposition** from live counters; the
      core-side duplicate stub was removed. Unit + integration tested.
- [x] **Wire the web dashboard to the real engine.** ✅ `gaussflow-web` no longer simulates:
      `simulate_execution` is replaced by `run_execution`, which executes the workflow on
      `gaussflow_runtime::execute` and streams real per-node + completion/failure events over the
      WebSocket. A `/metrics/prometheus` endpoint exposes the runtime exposition.
- [x] **Structured logs with run/node correlation IDs.** ✅ Each node executes inside a tracing
      span carrying `run` + `node` ids, so logs correlate to their run and node.
- [→] **Structured tracing with OpenTelemetry export** — partial: spans exist (one per node, run
      span context via ids); wiring an OTLP exporter needs a collector (deployment concern, Phase 7).
- [ ] **Health/readiness endpoints** wired to real engine state in `gaussflow-web` (a `/health`
      route exists; make `/ready` reflect real readiness).

**Exit criteria:** ✅ the web UI reflects **real** executions (no more simulation) and real run/node
metrics are exported in Prometheus format. **Remaining:** an OTLP exporter + a packaged Grafana
dashboard (both deployment/infra, deferred to release engineering).

---

## Phase 5 — Security & multi-tenancy (Weeks 16–19) 🔐 enterprise-readiness — in progress

*Objective: make the "enterprise" types actually enforce something. A new `gaussflow-security`
crate (auth / rbac / audit / pii) provides the primitives; the web API wires them in.*

- [x] **AuthN/AuthZ on the web API.** ✅ `gaussflow-security::auth` (JWT/HS256) + a web auth
      middleware: mutating requests (POST/PUT/DELETE/PATCH) require a valid `Bearer` token; reads
      stay public. The signing secret is read from `GAUSSFLOW_JWT_SECRET` and **never defaulted**
      (unset ⇒ 503 misconfigured, not silently open). Missing/invalid token ⇒ 401.
- [x] **RBAC enforcement.** ✅ `gaussflow-security::rbac` (viewer ⊂ operator ⊂ admin); the
      middleware authorizes mutations against the token's roles (403 if insufficient).
- [x] **Audit logging (tamper-evident).** ✅ `gaussflow-security::audit`: an append-only,
      SHA-256 **hash-chained** log that detects edits/deletes/reordering (`verify()`). Wired at the
      web auth boundary (every authorized mutation is recorded) and exposed at `GET /api/audit`
      with a chain-validity flag.
- [x] **PII detection/redaction.** ✅ `gaussflow-security::pii::redact_json` redacts email-like
      PII from JSON, recursively and structure-preserving. *(Available + tested; wiring it into the
      log/storage path everywhere is a follow-up.)*
- [→] **Secrets management.** Env-sourced secrets are in place (Phase S; `SecretProvider`); a
      HashiCorp Vault / cloud secret-manager backend is deferred (infra).
- [ ] **Input validation & resource quotas.** Synthesis-time quotas exist (Phase S); broader API
      input validation / runaway-workflow limits remain.
- [ ] **Threat model + external security review** (process; deferred to pre-GA).

**Exit criteria:** ✅ the API rejects unauthorized requests (401/403/503), and the audit trail is
verifiable (hash-chained, `verify()` + `GET /api/audit`). **Remaining:** a vault backend, broader
input-validation, and an external security review (pre-GA).

---

## Phase 6 — Scale-out (Weeks 20–26) 🌐 distribution — in progress

*Objective: deliver on the distributed-execution vision — only after single-node is rock solid.*

- [x] **Bounded-concurrent executor + backpressure.** ✅ `gaussflow-runtime/src/concurrent.rs`
      (`execute_concurrent[_with]`): a ready-queue scheduler that runs independent nodes
      concurrently, **bounded by `settings.concurrency`** (real backpressure — at most N node tasks
      in flight). Honors dependencies + conditional edge-skipping (reuses `edge_taken`) and per-node
      timeout/retry. Kept *separate* from `execute_resumable` (concurrency vs. fine-grained
      per-node checkpointing is an explicit trade-off). Tested for measured speedup + the bound.
- [x] **Kubernetes/containerization (build artifacts).** ✅ Multi-stage `Dockerfile` (installs
      `protoc`, release-builds web+CLI, slim runtime) + `docker-compose.yml` (web, optional
      SurrealDB profile) + `.dockerignore`. *(A Helm chart + autoscaling manifests remain.)*
- [x] **Throughput measurement.** ✅ The concurrency test measures a real speedup vs. sequential
      (replacing unsubstantiated perf claims with a measured one); per-node timings feed the
      Prometheus summary.
- [ ] **Distributed executor (coordinator/worker).** The concurrent scheduler is the foundation;
      a multi-process coordinator/worker split + node dispatch over the network remains (needs
      real infra to be meaningful).
- [ ] **Data transport** between workers (e.g., Arrow Flight) for large payloads.
- [ ] **Helm chart + multi-node load tests** with published throughput/latency numbers.

**Exit criteria (single-node ✅):** workflows run with **bounded concurrency + backpressure**
(verified) and ship as a container. **Remaining (needs a cluster):** the coordinator/worker split,
data transport, a Helm chart, and multi-node benchmarks.

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
