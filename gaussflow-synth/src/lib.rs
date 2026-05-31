//! # GaussFlow Synthesis Layer
//!
//! Compiles a natural-language **prompt** into a validated, runnable **DAG**:
//! `prompt → plan → lower → validate (→ repair) → confirm → deploy → run`.
//!
//! The thesis (see `docs/SYNTHESIS_PIPELINE.md`): synthesis is a *compiler front-end* whose
//! back-end already exists. The only "soft" step is planning (an LLM call); everything downstream
//! is deterministic and reuses the existing pieces — the [`gaussflow_core`] `WorkflowSpec` model,
//! the `TypeSafeDag` validator, and the [`gaussflow_runtime`] executor. A synthesized graph is
//! held to the *exact same* validation and run path as a hand-authored one.
//!
//! Planning needs a capable LLM in production; tests drive it with a stub [`LlmProvider`] that
//! returns canned plans, so the whole pipeline is exercised offline.

pub mod catalog;
pub mod plan;

use std::collections::HashMap;

use gaussflow_runtime::provider::{CompletionRequest, LlmProvider};
use serde_json::{json, Value};

pub use plan::{PlanIR, PlanStep};

/// A natural-language synthesis request plus optional constraints.
#[derive(Debug, Clone, Default)]
pub struct SynthesisRequest {
    /// The outcome to achieve, in natural language.
    pub goal: String,
    /// Optional constraints on synthesis.
    pub constraints: Constraints,
}

impl SynthesisRequest {
    /// Convenience constructor from a goal string.
    pub fn new(goal: impl Into<String>) -> Self {
        Self {
            goal: goal.into(),
            constraints: Constraints::default(),
        }
    }
}

/// Constraints that bound what synthesis may produce.
#[derive(Debug, Clone, Default)]
pub struct Constraints {
    /// If non-empty, restrict synthesis to this subset of supported capabilities.
    pub allowed_capabilities: Vec<String>,
}

/// The result of synthesis: the plan, the validated `WorkflowSpec`, and a human-readable
/// explanation to show at the confirmation step.
#[derive(Debug, Clone)]
pub struct SynthesisResult {
    /// The abstract plan the workflow was lowered from.
    pub plan: PlanIR,
    /// The concrete workflow spec (validated `WorkflowSpec` JSON value).
    pub spec: Value,
    /// The spec serialized to a string (ready to deploy/run).
    pub spec_json: String,
    /// A plain-language description of the proposed workflow, for human review.
    pub explanation: String,
    /// A cost/latency/side-effect estimate to show before the user confirms.
    pub estimate: PlanEstimate,
}

/// A rough, deterministic estimate of a plan's cost/latency/side-effects, shown at confirm time.
///
/// Counts are static (derived from node types and budgets), not measured. Nested sub-workflows
/// inside `subgraph`/`parallel` are not recursed into for v1 (noted in `summary`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEstimate {
    /// Number of nodes in the workflow.
    pub node_count: usize,
    /// Direct `llm_call` nodes (one model invocation each).
    pub llm_call_count: usize,
    /// Upper bound on model invocations: `llm_call` nodes plus each `agent`'s `max_steps` budget.
    pub max_model_invocations: usize,
    /// Whether the workflow can make external/network calls (any `llm_call` or `agent`).
    pub makes_external_calls: bool,
    /// A human-readable one-line summary.
    pub summary: String,
}

/// Errors from the synthesis pipeline.
#[derive(Debug, thiserror::Error)]
pub enum SynthError {
    /// The planning provider (LLM) failed.
    #[error("planning provider failed: {0}")]
    Provider(String),
    /// The model's output could not be parsed into a plan.
    #[error("could not parse a plan from the model output: {0}")]
    PlanParse(String),
    /// The plan is structurally invalid (unsupported capability, empty, or fails DAG validation).
    /// This is the repairable error the synthesis loop feeds back to the planner.
    #[error("invalid plan: {0}")]
    InvalidPlan(String),
    /// Synthesis did not produce a valid workflow within the repair budget.
    #[error("synthesis did not converge after {0} repair attempt(s); last error: {1}")]
    Unconverged(usize, String),
    /// JSON (de)serialization error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// The synthesizer: turns a [`SynthesisRequest`] into a validated [`SynthesisResult`].
pub struct Synthesizer<'a> {
    provider: &'a dyn LlmProvider,
    model: String,
    max_repairs: usize,
}

impl<'a> Synthesizer<'a> {
    /// Create a synthesizer over an LLM provider.
    pub fn new(provider: &'a dyn LlmProvider) -> Self {
        Self {
            provider,
            model: "gpt-4o-mini".to_string(),
            max_repairs: 2,
        }
    }

    /// Set the planning model.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Set the maximum number of self-repair iterations.
    pub fn with_max_repairs(mut self, n: usize) -> Self {
        self.max_repairs = n;
        self
    }

    /// Run the pipeline: plan → lower → validate, with bounded self-repair on failure.
    pub async fn synthesize(&self, req: &SynthesisRequest) -> Result<SynthesisResult, SynthError> {
        self.synthesize_seeded(req, None).await
    }

    /// Re-synthesize, seeding the planner with the user's `feedback` (e.g. "use a cheaper model"
    /// or "add a validation step"). This is the "regenerate with feedback" path of the confirm UX.
    pub async fn regenerate(
        &self,
        req: &SynthesisRequest,
        feedback: &str,
    ) -> Result<SynthesisResult, SynthError> {
        self.synthesize_seeded(req, Some(feedback.to_string()))
            .await
    }

    async fn synthesize_seeded(
        &self,
        req: &SynthesisRequest,
        seed_feedback: Option<String>,
    ) -> Result<SynthesisResult, SynthError> {
        let mut feedback = seed_feedback;
        let mut last_err = String::from("no attempts were made");

        for _ in 0..=self.max_repairs {
            let plan = self.request_plan(req, feedback.as_deref()).await?;

            // Reuse the deterministic, offline validation path (catalog + lower + DAG validate).
            match validate_plan(&plan, &req.constraints, &req.goal) {
                Ok(result) => return Ok(result),
                Err(SynthError::InvalidPlan(msg)) => {
                    last_err = msg.clone();
                    feedback = Some(format!("{msg}. Fix it and try again."));
                }
                Err(other) => return Err(other),
            }
        }

        Err(SynthError::Unconverged(self.max_repairs, last_err))
    }

    async fn request_plan(
        &self,
        req: &SynthesisRequest,
        feedback: Option<&str>,
    ) -> Result<PlanIR, SynthError> {
        let prompt = build_prompt(req, feedback);
        let completion = self
            .provider
            .complete(&CompletionRequest {
                model: self.model.clone(),
                prompt,
                system: Some(SYSTEM_PROMPT.to_string()),
                temperature: Some(0.0),
            })
            .await
            .map_err(|e| SynthError::Provider(e.to_string()))?;
        parse_plan(&completion)
    }
}

const SYSTEM_PROMPT: &str = "You are GaussFlow's workflow compiler. You translate a goal into a \
    plan of capabilities. Respond with ONLY a JSON object, no prose, no code fences.";

/// Build the planning prompt, including the capability catalog and any repair feedback.
fn build_prompt(req: &SynthesisRequest, feedback: Option<&str>) -> String {
    let mut p = String::new();
    p.push_str("Produce a JSON plan of this exact shape:\n");
    p.push_str(
        "{\"name\": string, \"steps\": [{\"id\": string, \"capability\": string, \
         \"depends_on\": [string], \"edges\": {downstream_id: label}, \"params\": object}]}\n\n",
    );
    p.push_str("Available capabilities:\n");
    p.push_str(&catalog::catalog_description());
    p.push_str(
        "\n\nRules: every `capability` must be from the list above; `depends_on` ids must \
        refer to other steps; the graph must be acyclic.\n\n",
    );
    p.push_str("Goal: ");
    p.push_str(&req.goal);
    p.push('\n');
    if !req.constraints.allowed_capabilities.is_empty() {
        p.push_str("Allowed capabilities (restrict to these): ");
        p.push_str(&req.constraints.allowed_capabilities.join(", "));
        p.push('\n');
    }
    if let Some(fb) = feedback {
        p.push_str("\nYour previous attempt was rejected: ");
        p.push_str(fb);
        p.push('\n');
    }
    p
}

/// Extract a JSON object from possibly-noisy model output and parse it into a [`PlanIR`].
fn parse_plan(text: &str) -> Result<PlanIR, SynthError> {
    let start = text
        .find('{')
        .ok_or_else(|| SynthError::PlanParse("no JSON object found".to_string()))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| SynthError::PlanParse("no JSON object found".to_string()))?;
    if end < start {
        return Err(SynthError::PlanParse("malformed JSON object".to_string()));
    }
    serde_json::from_str(&text[start..=end]).map_err(|e| SynthError::PlanParse(e.to_string()))
}

/// Lower a [`PlanIR`] to a concrete `WorkflowSpec` JSON value.
///
/// Deterministic and pure: each step becomes a `{ id, type, params }` node, and each `depends_on`
/// becomes an edge whose `on` label comes from the source step's `edges` map (default `success`).
pub fn lower(plan: &PlanIR) -> Value {
    let by_id: HashMap<&str, &PlanStep> = plan.steps.iter().map(|s| (s.id.as_str(), s)).collect();

    let nodes: Vec<Value> = plan
        .steps
        .iter()
        .map(|s| json!({ "id": s.id, "type": s.capability, "params": s.params }))
        .collect();

    let mut connections: Vec<Value> = Vec::new();
    for target in &plan.steps {
        for dep in &target.depends_on {
            let label = by_id
                .get(dep.as_str())
                .and_then(|src| src.edges.get(&target.id))
                .and_then(|v| v.as_str())
                .unwrap_or("success");
            connections.push(json!({ "from": dep, "to": target.id, "on": label }));
        }
    }

    let name = if plan.name.is_empty() {
        "synthesized"
    } else {
        plan.name.as_str()
    };

    json!({
        "name": name,
        "nodes": nodes,
        "connections": connections,
        "settings": { "concurrency": 4, "fail_fast": false }
    })
}

/// Validate a (possibly hand-edited) plan and produce a confirmable [`SynthesisResult`].
///
/// Deterministic and offline — no LLM. This is the **edit → re-validate** path: edit a [`PlanIR`]
/// (see its helper methods) then call this to lower it, run it through the same `TypeSafeDag`
/// validator a hand-authored graph uses, and compute the explanation + estimate. Returns
/// [`SynthError::InvalidPlan`] for an unsupported capability, an empty plan, or a DAG that fails
/// validation (e.g. a cycle or a dangling edge).
pub fn validate_plan(
    plan: &PlanIR,
    constraints: &Constraints,
    goal: &str,
) -> Result<SynthesisResult, SynthError> {
    if plan.steps.is_empty() {
        return Err(SynthError::InvalidPlan(
            "the plan has no steps; produce at least one".to_string(),
        ));
    }

    let allowed = effective_catalog(constraints);
    if let Some(bad) = plan
        .steps
        .iter()
        .find(|s| !allowed.iter().any(|c| c == &s.capability))
    {
        return Err(SynthError::InvalidPlan(format!(
            "capability '{}' is not supported; use only: {}",
            bad.capability,
            allowed.join(", ")
        )));
    }

    let spec = lower(plan);
    let spec_json = serde_json::to_string(&spec)?;

    // The same validator a hand-authored graph passes through.
    gaussflow_core::TypeSafeDag::from_json(&spec_json)
        .map_err(|e| SynthError::InvalidPlan(format!("the workflow failed validation: {e}")))?;

    Ok(SynthesisResult {
        explanation: explain(plan, goal),
        estimate: estimate(plan),
        plan: plan.clone(),
        spec,
        spec_json,
    })
}

/// Compute a deterministic cost/latency/side-effect estimate for a plan.
pub fn estimate(plan: &PlanIR) -> PlanEstimate {
    let node_count = plan.steps.len();
    let llm_call_count = plan
        .steps
        .iter()
        .filter(|s| s.capability == "llm_call")
        .count();
    let agent_budget: usize = plan
        .steps
        .iter()
        .filter(|s| s.capability == "agent")
        .map(|s| {
            s.params
                .get("max_steps")
                .and_then(|v| v.as_u64())
                .unwrap_or(5) as usize
        })
        .sum();
    let agent_count = plan
        .steps
        .iter()
        .filter(|s| s.capability == "agent")
        .count();
    let max_model_invocations = llm_call_count + agent_budget;
    let makes_external_calls = llm_call_count > 0 || agent_count > 0;
    let has_nested = plan
        .steps
        .iter()
        .any(|s| s.capability == "subgraph" || s.capability == "parallel");

    let mut summary = format!(
        "{node_count} node(s); up to {max_model_invocations} model call(s); external calls: {}",
        if makes_external_calls { "yes" } else { "no" }
    );
    if has_nested {
        summary.push_str(" (excludes nested sub-workflows)");
    }

    PlanEstimate {
        node_count,
        llm_call_count,
        max_model_invocations,
        makes_external_calls,
        summary,
    }
}

/// Produce a plain-language description of the plan for the human confirmation step, with
/// per-step provenance back to the goal.
fn explain(plan: &PlanIR, goal: &str) -> String {
    let mut s = String::new();
    s.push_str(&format!("Goal: {}\n", goal));
    s.push_str(&format!(
        "Proposed workflow \"{}\" with {} step(s):\n",
        if plan.name.is_empty() {
            "synthesized"
        } else {
            &plan.name
        },
        plan.steps.len()
    ));
    for (i, step) in plan.steps.iter().enumerate() {
        let deps = if step.depends_on.is_empty() {
            "inputs".to_string()
        } else {
            format!("[{}]", step.depends_on.join(", "))
        };
        s.push_str(&format!(
            "  {}. {} — {} (after {})\n",
            i + 1,
            step.id,
            step.capability,
            deps
        ));
    }
    s
}

/// Returns the effective capability set: the supported capabilities, optionally narrowed by the
/// request's `allowed_capabilities`.
fn effective_catalog(constraints: &Constraints) -> Vec<String> {
    if constraints.allowed_capabilities.is_empty() {
        catalog::SUPPORTED_CAPABILITIES
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        constraints
            .allowed_capabilities
            .iter()
            .filter(|c| catalog::is_supported(c))
            .cloned()
            .collect()
    }
}

/// Deploy and run a confirmed [`SynthesisResult`] on the canonical runtime, with no database.
///
/// This is the back half of the product loop (`deploy → run`): the *same* executor that runs a
/// hand-authored workflow. Call it after a human has reviewed [`SynthesisResult::explanation`].
pub async fn run(
    result: &SynthesisResult,
    input: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let dag = gaussflow_core::TypeSafeDag::from_json(&result.spec_json)?;
    let store = gaussflow_runtime::InMemoryRunStore::new();
    gaussflow_runtime::execute_with_store(dag, input, &store).await
}
