//! Phase 2: the `agent` (bounded tool-use loop) and `parallel` (concurrent sub-workflows) nodes.

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::{execute_with_store, InMemoryRunStore};
use serde_json::{json, Value};

async fn run_single(node_json: &str) -> Value {
    let wf = format!(
        r#"{{ "name": "t", "nodes": [{node_json}], "connections": [],
              "settings": {{ "concurrency": 4, "fail_fast": false }} }}"#
    );
    let dag = TypeSafeDag::from_json(&wf).expect("workflow parses");
    let store = InMemoryRunStore::new();
    let result = execute_with_store(dag, json!({}), &store)
        .await
        .expect("execution succeeds");
    result["outputs"]["only"].clone()
}

#[tokio::test]
async fn agent_runs_a_scripted_tool_loop() {
    // The script stands in for the LLM's decisions: call `upper`, then finish.
    let node = r#"{ "id": "only", "type": "agent", "params": {
        "task": "shout the greeting",
        "script": [
            { "tool": "upper", "args": { "text": "hello" } },
            { "final": "done" }
        ]
    } }"#;
    let out = run_single(node).await;
    assert_eq!(out["steps"], json!(2));
    assert_eq!(out["answer"], json!("done"));
    assert_eq!(out["budget_exhausted"], json!(false));
    // The scratchpad records the tool call and its deterministic result.
    assert_eq!(out["scratchpad"][0]["tool"], json!("upper"));
    assert_eq!(out["scratchpad"][0]["result"]["text"], json!("HELLO"));
}

#[tokio::test]
async fn agent_respects_the_step_budget() {
    // Three tool calls but a budget of 2 → loop stops with budget exhausted, no final answer.
    let node = r#"{ "id": "only", "type": "agent", "params": {
        "task": "loop",
        "max_steps": 2,
        "script": [
            { "tool": "echo", "args": { "n": 1 } },
            { "tool": "echo", "args": { "n": 2 } },
            { "tool": "echo", "args": { "n": 3 } }
        ]
    } }"#;
    let out = run_single(node).await;
    assert_eq!(out["steps"], json!(2));
    assert_eq!(out["budget_exhausted"], json!(true));
    assert_eq!(out["scratchpad"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn agent_uses_the_mock_provider_offline_when_unscripted() {
    // No script → it calls the provider. A `mock*` model means no network/API key; the mock's
    // non-JSON echo is treated as the final answer, so the loop finishes in one step.
    let node =
        r#"{ "id": "only", "type": "agent", "params": { "task": "hi", "model": "mock-a" } }"#;
    let out = run_single(node).await;
    assert_eq!(out["steps"], json!(1));
    assert!(out["answer"].as_str().unwrap().starts_with("[mock:mock-a]"));
}

#[tokio::test]
async fn parallel_runs_branches_concurrently_and_collects_results() {
    // Two branches each extract a different field from the (here empty) input; we assert both ran
    // and results are collected in order.
    let node = r#"{ "id": "only", "type": "parallel", "params": { "branches": [
        { "name": "b1", "nodes": [ { "id": "x", "type": "data_processor", "params": { "op": "set", "value": { "branch": 1 } } } ], "connections": [], "settings": {} },
        { "name": "b2", "nodes": [ { "id": "y", "type": "data_processor", "params": { "op": "set", "value": { "branch": 2 } } } ], "connections": [], "settings": {} }
    ] } }"#;
    let out = run_single(node).await;
    assert_eq!(out["count"], json!(2));
    let results = out["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["result"]["branch"], json!(1));
    assert_eq!(results[1]["result"]["branch"], json!(2));
}
