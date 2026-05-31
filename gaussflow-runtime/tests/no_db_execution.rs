//! Phase 1 acceptance: a workflow executes end-to-end with **no database** and returns real
//! per-node outputs, using the default in-memory run store.

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::{execute_with_store, InMemoryRunStore};
use serde_json::json;

const TWO_NODE_WORKFLOW: &str = r#"{
    "name": "no-db-test",
    "nodes": [
        { "id": "first",  "type": "data_processor", "params": {} },
        { "id": "second", "type": "data_processor", "params": {} }
    ],
    "connections": [
        { "from": "first", "to": "second", "on": "success" }
    ],
    "settings": { "concurrency": 4, "fail_fast": false }
}"#;

#[tokio::test]
async fn executes_without_a_database_and_returns_outputs() {
    let dag = TypeSafeDag::from_json(TWO_NODE_WORKFLOW).expect("workflow should parse");

    let store = InMemoryRunStore::new();
    let result = execute_with_store(dag, json!({"name": "world"}), &store)
        .await
        .expect("execution should succeed with no database");

    // The result carries a run id, a final output, and the full per-node output map.
    let run_id = result["run_id"].as_str().expect("run_id present");
    assert!(!run_id.is_empty());

    let outputs = result["outputs"].as_object().expect("outputs map present");
    // "input" plus the two nodes.
    assert!(outputs.contains_key("input"));
    assert!(outputs.contains_key("first"));
    assert!(outputs.contains_key("second"));

    // The run was recorded in the in-memory store and marked finished.
    let record = store.get(run_id).expect("run recorded in store");
    assert_eq!(record.status, "finished");
    assert!(record.output.is_some());
}

#[tokio::test]
async fn default_execute_needs_no_database() {
    // The public `execute` entrypoint defaults to the in-memory store (no GAUSSFLOW_RUN_STORE set),
    // so it must succeed without any SurrealDB instance running.
    let dag = TypeSafeDag::from_json(TWO_NODE_WORKFLOW).expect("workflow should parse");
    let result = gaussflow_runtime::execute(dag, json!({}))
        .await
        .expect("default execute should need no database");
    assert!(result["outputs"].is_object());
}
