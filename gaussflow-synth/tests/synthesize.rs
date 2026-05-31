//! End-to-end synthesis tests, fully offline. A stub `LlmProvider` stands in for the LLM and
//! returns canned plans; we assert the synthesized workflow validates and **runs on the real
//! runtime**, proving the "compiler front-end whose back-end already exists" thesis.

use async_trait::async_trait;
use gaussflow_runtime::provider::{CompletionRequest, LlmProvider, ProviderError};
use gaussflow_synth::{run, SynthesisRequest, Synthesizer};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A provider that returns pre-scripted completions in order (simulating an LLM's plan output).
struct ScriptedProvider {
    responses: Vec<String>,
    calls: AtomicUsize,
}

impl ScriptedProvider {
    fn new(responses: Vec<&str>) -> Self {
        Self {
            responses: responses.into_iter().map(String::from).collect(),
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn complete(&self, _req: &CompletionRequest) -> Result<String, ProviderError> {
        let i = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .responses
            .get(i)
            .cloned()
            .unwrap_or_else(|| self.responses.last().cloned().unwrap_or_default()))
    }
    fn name(&self) -> &'static str {
        "scripted"
    }
}

const GOOD_PLAN: &str = r#"{
    "name": "summarize-pipeline",
    "steps": [
        { "id": "ingest", "capability": "data_processor", "depends_on": [],
          "params": { "op": "extract", "field": "text" } },
        { "id": "summarize", "capability": "llm_call", "depends_on": ["ingest"],
          "params": { "model": "mock-sum", "prompt": "summarize" } }
    ]
}"#;

#[tokio::test]
async fn synthesizes_validates_and_runs_offline() {
    let provider = ScriptedProvider::new(vec![GOOD_PLAN]);
    let synth = Synthesizer::new(&provider).with_model("mock-planner");

    let req = SynthesisRequest::new("Summarize the input text");
    let result = synth.synthesize(&req).await.expect("synthesis succeeds");

    // The plan lowered to a two-node DAG that passed the same validator as a hand-authored graph.
    assert_eq!(result.plan.steps.len(), 2);
    assert_eq!(result.spec["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(result.spec["connections"].as_array().unwrap().len(), 1);
    assert!(result.explanation.contains("summarize-pipeline"));

    // Deploy + run on the real runtime — no database, offline mock LLM.
    let out = run(&result, json!({ "text": "hello world" }))
        .await
        .expect("the synthesized workflow runs");
    // llm_call used the offline mock provider (model "mock-sum").
    assert_eq!(out["outputs"]["summarize"]["provider"], json!("mock"));
    assert!(out["outputs"]["summarize"]["answer"]
        .as_str()
        .unwrap()
        .starts_with("[mock:mock-sum]"));
}

#[tokio::test]
async fn repairs_an_invalid_plan_then_succeeds() {
    // First response uses an unsupported capability; second is valid. The repair loop should
    // feed the error back and converge.
    let bad = r#"{ "name": "x", "steps": [
        { "id": "a", "capability": "quantum_teleport", "depends_on": [], "params": {} } ] }"#;
    let provider = ScriptedProvider::new(vec![bad, GOOD_PLAN]);
    let synth = Synthesizer::new(&provider).with_max_repairs(2);

    let result = synth
        .synthesize(&SynthesisRequest::new("do the thing"))
        .await
        .expect("synthesis converges after repair");
    assert_eq!(result.plan.steps.len(), 2);
    // It took two provider calls (the bad attempt + the repair).
    assert!(result.spec_json.contains("summarize"));
}

#[tokio::test]
async fn gives_up_after_the_repair_budget() {
    let bad = r#"{ "name": "x", "steps": [
        { "id": "a", "capability": "nope", "depends_on": [], "params": {} } ] }"#;
    // Always returns an unsupported capability; should exhaust the budget and error.
    let provider = ScriptedProvider::new(vec![bad]);
    let synth = Synthesizer::new(&provider).with_max_repairs(1);
    let err = synth
        .synthesize(&SynthesisRequest::new("impossible"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        gaussflow_synth::SynthError::Unconverged(_, _)
    ));
}
