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
        let allowed = effective_catalog(&req.constraints);
        let mut feedback: Option<String> = None;
        let mut last_err = String::from("no attempts were made");

        for _ in 0..=self.max_repairs {
            let plan = self.request_plan(req, feedback.as_deref()).await?;

            // Capability honesty: only emit node types the runtime can execute.
            if let Some(bad) = plan
                .steps
                .iter()
                .find(|s| !allowed.iter().any(|c| c == &s.capability))
            {
                last_err = format!("capability '{}' is not supported", bad.capability);
                feedback = Some(format!(
                    "{last_err}. Use only these capabilities: {}.",
                    allowed.join(", ")
                ));
                continue;
            }
            if plan.steps.is_empty() {
                last_err = "the plan had no steps".to_string();
                feedback = Some(format!("{last_err}; produce at least one step."));
                continue;
            }

            let spec = lower(&plan);
            let spec_json = serde_json::to_string(&spec)?;

            // The same validator a hand-authored graph passes through.
            match gaussflow_core::TypeSafeDag::from_json(&spec_json) {
                Ok(_) => {
                    let explanation = explain(&plan, req);
                    return Ok(SynthesisResult {
                        plan,
                        spec,
                        spec_json,
                        explanation,
                    });
                }
                Err(e) => {
                    last_err = e.to_string();
                    feedback = Some(format!(
                        "the workflow failed validation: {last_err}. Fix it."
                    ));
                }
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

/// Produce a plain-language description of the plan for the human confirmation step, with
/// per-step provenance back to the goal.
fn explain(plan: &PlanIR, req: &SynthesisRequest) -> String {
    let mut s = String::new();
    s.push_str(&format!("Goal: {}\n", req.goal));
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
