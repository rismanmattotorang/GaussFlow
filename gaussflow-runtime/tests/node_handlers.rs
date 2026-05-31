//! Phase 2: deterministic, offline tests for real node handlers and the LLM provider abstraction.

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::provider::{provider_for, CompletionRequest};
use gaussflow_runtime::{execute_with_store, InMemoryRunStore};
use serde_json::{json, Value};

/// Run a single-node workflow and return that node's output object.
async fn run_single(node_json: &str) -> Value {
    let wf = format!(
        r#"{{ "name": "t", "nodes": [{node_json}], "connections": [],
              "settings": {{ "concurrency": 1, "fail_fast": false }} }}"#
    );
    let dag = TypeSafeDag::from_json(&wf).expect("workflow parses");
    let store = InMemoryRunStore::new();
    let result = execute_with_store(dag, json!({"score": 5, "name": "ada"}), &store)
        .await
        .expect("execution succeeds");
    result["outputs"]["only"].clone()
}

#[tokio::test]
async fn mock_provider_is_deterministic_and_offline() {
    let p = provider_for("mock-test");
    assert_eq!(p.name(), "mock");
    let req = CompletionRequest {
        model: "mock-test".into(),
        prompt: "hello".into(),
        system: None,
        temperature: None,
    };
    let out = p.complete(&req).await.unwrap();
    assert_eq!(out, "[mock:mock-test] hello");
}

#[tokio::test]
async fn llm_call_uses_the_mock_provider_offline() {
    // A model named `mock*` routes to the offline provider — no network, no API key.
    let node = r#"{ "id": "only", "type": "llm_call", "model": "mock-1",
                    "params": { "prompt": "ping" } }"#;
    let out = run_single(node).await;
    assert_eq!(out["provider"], "mock");
    assert_eq!(out["answer"], "[mock:mock-1] ping");
}

#[tokio::test]
async fn data_processor_extract_and_set() {
    // extract: pulls a field out of the input.
    let node = r#"{ "id": "only", "type": "data_processor",
                    "params": { "op": "extract", "field": "name" } }"#;
    let out = run_single(node).await;
    assert_eq!(out["result"], json!("ada"));

    // set: shallow-merges a patch object over the input.
    let node = r#"{ "id": "only", "type": "data_processor",
                    "params": { "op": "set", "value": { "added": true } } }"#;
    let out = run_single(node).await;
    assert_eq!(out["result"]["added"], json!(true));
    assert_eq!(out["result"]["score"], json!(5)); // original field preserved
}

#[tokio::test]
async fn conditional_evaluates_comparisons() {
    // score (5) > 3  => matched
    let node = r#"{ "id": "only", "type": "conditional",
                    "params": { "field": "score", "op": "gt", "value": 3 } }"#;
    let out = run_single(node).await;
    assert_eq!(out["matched"], json!(true));
    assert_eq!(out["branch"], json!("true"));

    // name == "ada" => matched
    let node = r#"{ "id": "only", "type": "conditional",
                    "params": { "field": "name", "op": "eq", "value": "ada" } }"#;
    let out = run_single(node).await;
    assert_eq!(out["matched"], json!(true));

    // score (5) < 1 => not matched
    let node = r#"{ "id": "only", "type": "conditional",
                    "params": { "field": "score", "op": "lt", "value": 1 } }"#;
    let out = run_single(node).await;
    assert_eq!(out["matched"], json!(false));
    assert_eq!(out["branch"], json!("false"));
}
