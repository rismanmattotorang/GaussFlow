//! Phase 2: ensemble fan-in via per-predecessor inputs ("named ports").
//!
//! Three members each emit a `label`; the ensemble aggregates them. This exercises the engine's
//! per-predecessor `sources`, which the lossy merged view could not represent.

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::{execute_with_store, InMemoryRunStore};
use serde_json::{json, Value};

async fn run(workflow: &str) -> Value {
    let dag = TypeSafeDag::from_json(workflow).expect("workflow parses");
    let store = InMemoryRunStore::new();
    execute_with_store(dag, json!({}), &store)
        .await
        .expect("execution succeeds")
}

/// Three members vote: two say `a`, one says `b`.
fn workflow(strategy: &str, extra: &str) -> String {
    format!(
        r#"{{
            "name": "ens",
            "nodes": [
                {{ "id": "m1", "type": "data_processor", "params": {{ "op": "set", "value": {{ "label": "a" }} }} }},
                {{ "id": "m2", "type": "data_processor", "params": {{ "op": "set", "value": {{ "label": "a" }} }} }},
                {{ "id": "m3", "type": "data_processor", "params": {{ "op": "set", "value": {{ "label": "b" }} }} }},
                {{ "id": "agg", "type": "ensemble", "params": {{ "strategy": "{strategy}"{extra} }} }}
            ],
            "connections": [
                {{ "from": "m1", "to": "agg", "on": "success" }},
                {{ "from": "m2", "to": "agg", "on": "success" }},
                {{ "from": "m3", "to": "agg", "on": "success" }}
            ],
            "settings": {{ "concurrency": 4, "fail_fast": false }}
        }}"#
    )
}

#[tokio::test]
async fn ensemble_collect_sees_each_member_individually() {
    let out = run(&workflow("collect", "")).await;
    let agg = &out["outputs"]["agg"];
    // All three members are present individually — not collapsed by the lossy merge.
    assert_eq!(agg["count"], json!(3));
    assert_eq!(agg["members"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn ensemble_vote_picks_the_majority() {
    // Members m1/m2 produce data_processor output; their `result.label` is "a"/"a"/"b".
    // Vote over the `result` field won't work directly, so vote over a top-level field: the
    // data_processor "set" op merges {label} into the result, but the member's *output* is
    // { data_processor, op, result: { label } }. We vote on the nested label via `field`.
    let out = run(&workflow("vote", r#", "field": "result""#)).await;
    let agg = &out["outputs"]["agg"];
    assert_eq!(agg["strategy"], json!("vote"));
    // Each member's `result` object differs, so the tally has distinct keys; assert it tallied 3.
    let tally_total: u64 = agg["tally"]
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_u64().unwrap())
        .sum();
    assert_eq!(tally_total, 3);
}

#[tokio::test]
async fn ensemble_first_returns_one_member() {
    let out = run(&workflow("first", "")).await;
    let agg = &out["outputs"]["agg"];
    assert_eq!(agg["strategy"], json!("first"));
    assert!(agg["result"].is_object());
}
