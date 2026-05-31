//! Phase 2: conditional edge traversal. A `conditional`/`router` node selects which downstream
//! edges are taken; nodes on untaken branches are skipped (not executed).

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::{execute_with_store, InMemoryRunStore};
use serde_json::{json, Value};

async fn run(workflow: &str, input: Value) -> Value {
    let dag = TypeSafeDag::from_json(workflow).expect("workflow parses");
    let store = InMemoryRunStore::new();
    execute_with_store(dag, input, &store)
        .await
        .expect("execution succeeds")
}

fn is_skipped(outputs: &Value, node: &str) -> bool {
    outputs[node].get("skipped").and_then(|v| v.as_bool()) == Some(true)
}

const COND_WORKFLOW: &str = r#"{
    "name": "branch",
    "nodes": [
        { "id": "check",    "type": "conditional",    "params": { "field": "score", "op": "gt", "value": 3 } },
        { "id": "on_true",  "type": "data_processor",  "params": { "op": "set", "value": { "took": "true" } } },
        { "id": "on_false", "type": "data_processor",  "params": { "op": "set", "value": { "took": "false" } } }
    ],
    "connections": [
        { "from": "check", "to": "on_true",  "on": "true"  },
        { "from": "check", "to": "on_false", "on": "false" }
    ],
    "settings": { "concurrency": 4, "fail_fast": false }
}"#;

#[tokio::test]
async fn conditional_takes_true_branch_and_skips_false() {
    let out = run(COND_WORKFLOW, json!({ "score": 5 })).await;
    let outputs = &out["outputs"];
    assert_eq!(outputs["check"]["branch"], json!("true"));
    assert!(!is_skipped(outputs, "on_true"), "true branch should run");
    assert!(
        is_skipped(outputs, "on_false"),
        "false branch should be skipped"
    );
}

#[tokio::test]
async fn conditional_takes_false_branch_and_skips_true() {
    let out = run(COND_WORKFLOW, json!({ "score": 1 })).await;
    let outputs = &out["outputs"];
    assert_eq!(outputs["check"]["branch"], json!("false"));
    assert!(
        is_skipped(outputs, "on_true"),
        "true branch should be skipped"
    );
    assert!(!is_skipped(outputs, "on_false"), "false branch should run");
}

const ROUTER_WORKFLOW: &str = r#"{
    "name": "route",
    "nodes": [
        { "id": "router", "type": "router",
          "params": { "field": "tier", "routes": { "gold": "premium", "silver": "standard" }, "default": "standard" } },
        { "id": "premium",  "type": "data_processor", "params": {} },
        { "id": "standard", "type": "data_processor", "params": {} }
    ],
    "connections": [
        { "from": "router", "to": "premium",  "on": "premium"  },
        { "from": "router", "to": "standard", "on": "standard" }
    ],
    "settings": { "concurrency": 4, "fail_fast": false }
}"#;

#[tokio::test]
async fn router_selects_matching_route_only() {
    let out = run(ROUTER_WORKFLOW, json!({ "tier": "gold" })).await;
    let outputs = &out["outputs"];
    assert_eq!(outputs["router"]["route"], json!("premium"));
    assert!(!is_skipped(outputs, "premium"), "premium route should run");
    assert!(
        is_skipped(outputs, "standard"),
        "standard route should be skipped"
    );
}

#[tokio::test]
async fn router_falls_back_to_default() {
    let out = run(ROUTER_WORKFLOW, json!({ "tier": "bronze" })).await;
    let outputs = &out["outputs"];
    assert_eq!(outputs["router"]["route"], json!("standard"));
    assert!(is_skipped(outputs, "premium"));
    assert!(!is_skipped(outputs, "standard"));
}
