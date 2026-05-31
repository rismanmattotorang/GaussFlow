# GaussFlow — The Synthesis Pipeline

**Owner:** Gaussian Technologies
**Status:** Design (not yet implemented) — this is the product's defining capability.
**Companion documents:** [`CODE_EVALUATION.md`](CODE_EVALUATION.md) · [`PRODUCTION_ROADMAP.md`](PRODUCTION_ROADMAP.md)

---

## 1. What this is

GaussFlow's thesis is that you should not have to *build* an agentic workflow the way you build
one in n8n or Flowise — dragging nodes onto a canvas and wiring edges by hand. You should be able
to **describe the outcome** and have the system produce a correct, runnable graph.

This document specifies the layer that delivers that:

> **Prompt → DAG → Confirm → Deploy → Run.**

It does **not** exist in the codebase today. The static evaluation
([`CODE_EVALUATION.md`](CODE_EVALUATION.md)) found no natural-language-to-graph code, no
confirmation step, and no deploy/register step. This document is the design we will build against,
and it is the north-star phase of the [roadmap](PRODUCTION_ROADMAP.md).

---

## 2. The core insight: synthesis is a compiler

We deliberately frame this as a **compiler**, not "an LLM that writes JSON."

| Compiler concept | GaussFlow equivalent |
|---|---|
| Source language | Natural-language prompt (+ context, constraints, available tools) |
| Front-end (parse → IR) | LLM-based intent extraction → a structured **Plan IR** |
| Back-end target | `WorkflowSpec` (the existing `gaussflow-core` model) |
| Type checker | `TypeSafeDag::from_json` — cycle detection + node/edge validation |
| Linker / loader | Deploy step: register spec, resolve providers, allocate resources |
| Runtime | `gaussflow-runtime` topological executor |

The payoff of this framing: **the generative step is the only new, "soft" part.** Everything
downstream of the produced `WorkflowSpec` reuses validation and execution machinery that already
exists (or is already on the roadmap). A generated graph is held to the *exact same contract* as
a hand-authored one. If it does not type-check, it does not deploy.

---

## 3. Stage-by-stage design

### Stage 1 — Prompt intake
- **Input:** a natural-language goal, plus optional structured context: available data sources,
  allowed tools/integrations, model/cost/latency constraints, schedule/trigger, secrets scope.
- **Output:** a normalized `SynthesisRequest`.
- **Notes:** capture constraints explicitly so they become hard guards in later stages (e.g.
  "never use a model above $X/1k tokens", "must finish in < 30s", "no external network calls").

### Stage 2 — Intent extraction → Plan IR
- Use an LLM with **structured output** (JSON schema / tool-calling) to produce a **Plan IR**: an
  ordered set of *capabilities* the workflow needs ("retrieve tickets", "cluster", "summarize per
  cluster", "post to Slack") with data dependencies between them — *not yet* GaussFlow nodes.
- The IR is intentionally smaller and more abstract than `WorkflowSpec`. Keeping a distinct IR
  makes the system testable and lets us swap the final lowering strategy without re-prompting.
- **Capability catalog:** the LLM is given the set of node types the runtime *actually supports*
  today and a registry of available integrations/tools. It may not invent capabilities.

### Stage 3 — Lowering: Plan IR → WorkflowSpec (DAG synthesis)
- Deterministically (or with a constrained second LLM pass) lower each IR capability to one or
  more concrete nodes: `llm_call`, `agent`, `router`, `ensemble`, `subgraph`, `data_processor`,
  `conditional`, `parallel`.
- Wire `connections` from the IR's data dependencies. Assign `resources` and `retry` policy from
  constraints and node-type defaults.
- **Model/provider selection** is a first-class concern: a `router` node (or the lowering policy)
  picks the cheapest model that meets the capability's quality bar, honoring cost constraints.

### Stage 4 — Validation (the type checker)
- Run the produced `WorkflowSpec` through `TypeSafeDag::from_json`: schema validity, cycle
  detection, edge endpoint existence, node-type support.
- **Hard gate:** only node types with a real runtime implementation may appear. Synthesis cannot
  emit a `router`/`agent` until those handlers are real (roadmap Phase 2). Until then the
  synthesizer is restricted to the implemented subset (`llm_call`, plus deterministic compute).
- On failure, feed the validator's error back into Stage 2/3 for a bounded number of **repair
  iterations** (self-correcting compile loop), then surface a clear error if it cannot converge.

### Stage 5 — Confirmation (human-in-the-loop)
- Render the proposed DAG for review: a graph visualization, the underlying JSON, an estimated
  cost/latency, and a **plain-language explanation of each node and why it's there**
  (provenance back to the prompt).
- The user can **edit** (tweak a prompt, swap a model, add/remove a node), **regenerate** (with
  feedback), or **confirm**. Editing re-runs Stage 4 validation. Nothing deploys without explicit
  confirmation. This is a safety and trust boundary, not a formality.

### Stage 6 — Deploy (link + load)
- Persist the confirmed, immutable `WorkflowSpec` as a versioned, addressable artifact.
- Resolve providers and secrets (from the secrets manager, never inlined), reserve resource
  quotas, register triggers/schedules, and produce a runnable **deployment handle**.
- Deployment is reproducible: same spec + same inputs ⇒ same wiring.

### Stage 7 — Run + observe
- Execute via the canonical `gaussflow-runtime` executor.
- Stream node-level events (status, token streams, costs) over the existing WebSocket API to the
  dashboard. Persist run results and link them back to the synthesizing prompt for auditability.

---

## 4. Where this plugs into what exists

| Stage | Reuses | New work |
|---|---|---|
| 1–3 Synthesis | `WorkflowSpec` model (`gaussflow-core/src/model.rs`) as the lowering target | A new `gaussflow-synth` crate: intake, Plan IR, lowering, repair loop |
| 4 Validation | `TypeSafeDag::from_json` (`gaussflow-core/src/dag.rs`) | Validator-error → repair feedback loop |
| 5 Confirmation | Web dashboard + WebSocket (`gaussflow-web`) | Plan rendering, diff/edit UI, explainability |
| 6 Deploy | Run persistence, versioning types (`gaussflow-core/src/versioning.rs`) | A `RunStore`/deploy registry; secrets resolution |
| 7 Run | `gaussflow-runtime::execute` (the one correct engine) | Return full node outputs; stream real events (replace `simulate_execution`) |

This is why the roadmap sequences **"consolidate the core" and "make node types real" *before*
"ship synthesis."** The compiler can only safely target capabilities the runtime can actually
execute, and it needs *one* trustworthy executor — not the three divergent ones that exist today.

---

## 5. Hard problems we must get right

1. **Faithfulness.** The graph must do what the prompt asked — no more (no surprise network
   calls, no unbounded spend) and no less. Constraints from Stage 1 become enforced guards.
2. **Determinism of the contract.** Generation is stochastic; the *validated DAG* must be a
   stable, inspectable artifact. The IR + validator gate make the output auditable.
3. **Bounded self-repair.** The compile-fix loop must terminate and fail loudly rather than
   silently shipping a degraded graph.
4. **Capability honesty.** The synthesizer must never emit a node type the runtime only stubs.
   The capability catalog is generated from real handler registration, not from the enum.
5. **Cost & safety estimation pre-confirm.** Users confirm with eyes open: estimated spend,
   latency, external side effects, and data egress shown *before* deploy.
6. **Explainability.** Every node carries provenance back to the prompt span that motivated it.

---

## 6. Acceptance criteria for "synthesis works"

- A natural-language prompt produces a `WorkflowSpec` that passes `TypeSafeDag` validation
  unmodified ≥ 90% of the time on a benchmark prompt suite (with bounded repair).
- The produced graph uses **only** runtime-supported node types.
- The user can review, edit, and confirm a plan; confirmed plans deploy reproducibly.
- A confirmed plan runs end-to-end on the canonical runtime and returns real node outputs.
- Cost/latency estimates shown at confirm time are within a documented tolerance of actuals.
- Every deployed run is traceable back to its originating prompt.

When these hold, GaussFlow stops being "a Rust workflow runtime" and becomes the product it set
out to be: **describe it, confirm it, ship it.**
</content>
