//! Phase S hardening: the offline confirm/edit API — edit a plan, re-validate deterministically,
//! and inspect the cost/latency estimate. No LLM involved.

use gaussflow_synth::{estimate, run, validate_plan, Constraints, PlanIR};
use serde_json::json;

fn base_plan() -> PlanIR {
    serde_json::from_value(json!({
        "name": "edit-demo",
        "steps": [
            { "id": "ingest", "capability": "data_processor", "depends_on": [],
              "params": { "op": "extract", "field": "text" } },
            { "id": "summarize", "capability": "llm_call", "depends_on": ["ingest"],
              "params": { "model": "mock-a", "prompt": "summarize" } }
        ]
    }))
    .unwrap()
}

#[test]
fn estimate_counts_model_calls_and_external_effects() {
    let est = estimate(&base_plan());
    assert_eq!(est.node_count, 2);
    assert_eq!(est.llm_call_count, 1);
    assert_eq!(est.max_model_invocations, 1);
    assert!(est.makes_external_calls);
}

#[test]
fn agent_budget_counts_toward_max_model_invocations() {
    let mut plan = base_plan();
    plan.add_step(
        serde_json::from_value(json!({
            "id": "act", "capability": "agent", "depends_on": ["summarize"],
            "params": { "task": "do", "max_steps": 4 }
        }))
        .unwrap(),
    );
    let est = estimate(&plan);
    // 1 llm_call + agent budget of 4.
    assert_eq!(est.max_model_invocations, 5);
}

#[tokio::test]
async fn edit_then_revalidate_then_run() {
    let mut plan = base_plan();

    // Edit: swap the model on the summarize step.
    assert!(plan.set_param("summarize", "model", json!("mock-edited")));

    // Re-validate deterministically (no LLM) — same validator a hand-authored graph uses.
    let result = validate_plan(&plan, &Constraints::default(), "edited goal")
        .expect("edited plan still validates");
    assert!(result.explanation.contains("edited goal"));

    // Run the edited workflow; the model swap is reflected in the output.
    let out = run(&result, json!({ "text": "hi" })).await.expect("runs");
    assert!(out["outputs"]["summarize"]["answer"]
        .as_str()
        .unwrap()
        .starts_with("[mock:mock-edited]"));
}

#[test]
fn remove_step_scrubs_references() {
    let mut plan = base_plan();
    // Removing "ingest" should also drop "summarize"'s dependency on it.
    assert!(plan.remove_step("ingest"));
    assert_eq!(plan.steps.len(), 1);
    assert!(plan.steps[0].depends_on.is_empty());
    // The reduced plan still validates.
    assert!(validate_plan(&plan, &Constraints::default(), "g").is_ok());
}

#[test]
fn editing_in_an_unsupported_capability_is_rejected() {
    let mut plan = base_plan();
    plan.step_mut("summarize").unwrap().capability = "quantum".to_string();
    let err = validate_plan(&plan, &Constraints::default(), "g").unwrap_err();
    assert!(matches!(err, gaussflow_synth::SynthError::InvalidPlan(_)));
}
