//! Phase 3: checkpoint + resume, idempotency, and failure injection.
//!
//! A 3-node chain a → b → c. We crash mid-run (node `b` fails), then resume from the checkpoint:
//! the already-completed `a` is NOT re-run (idempotent), `b` is retried and succeeds, and `c`
//! runs — driving the interrupted run to the correct final state.

use async_trait::async_trait;
use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::handler::{NodeHandler, NodeInput};
use gaussflow_runtime::{
    execute_resumable, execute_resumable_with, FileCheckpointStore, InMemoryCheckpointStore,
    InMemoryRunStore,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const CHAIN: &str = r#"{
    "name": "chain",
    "nodes": [
        { "id": "a", "type": "data_processor", "params": {} },
        { "id": "b", "type": "data_processor", "params": {} },
        { "id": "c", "type": "data_processor", "params": {} }
    ],
    "connections": [
        { "from": "a", "to": "b", "on": "success" },
        { "from": "b", "to": "c", "on": "success" }
    ],
    "settings": { "concurrency": 1, "fail_fast": false }
}"#;

/// Counts executions per node, and fails node `b` while `fail_b` is set.
struct CrashHandler {
    counts: Arc<Mutex<HashMap<String, usize>>>,
    fail_b: Arc<AtomicBool>,
}

#[async_trait]
impl NodeHandler for CrashHandler {
    async fn execute(
        &self,
        node: &gaussflow_core::model::NodeSpec,
        _input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        *self
            .counts
            .lock()
            .unwrap()
            .entry(node.id.clone())
            .or_insert(0) += 1;
        if node.id == "b" && self.fail_b.load(Ordering::SeqCst) {
            return Err("boom".into());
        }
        Ok(json!({ "id": node.id }))
    }
}

#[tokio::test]
async fn interrupted_run_resumes_to_correct_state_without_rerunning_completed_nodes() {
    let dag = TypeSafeDag::from_json(CHAIN).unwrap();
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let fail_b = Arc::new(AtomicBool::new(true));

    let counts_for_resolver = counts.clone();
    let fail_for_resolver = fail_b.clone();
    let resolver = move |_k: &gaussflow_core::model::NodeType| -> Box<dyn NodeHandler> {
        Box::new(CrashHandler {
            counts: counts_for_resolver.clone(),
            fail_b: fail_for_resolver.clone(),
        })
    };

    let run_store = InMemoryRunStore::new();
    let checkpoints = InMemoryCheckpointStore::new();
    let run_id = "run-1".to_string();

    // Run 1: b fails → the run errors, but `a` is checkpointed.
    let first = execute_resumable_with(
        TypeSafeDag::from_json(CHAIN).unwrap(),
        json!({}),
        &run_store,
        &resolver,
        &checkpoints,
        Some(run_id.clone()),
    )
    .await;
    assert!(first.is_err(), "first run should fail at b");

    // Resume: b now succeeds.
    fail_b.store(false, Ordering::SeqCst);
    let _ = dag; // keep the parsed dag's lifetime tidy
    let result = execute_resumable_with(
        TypeSafeDag::from_json(CHAIN).unwrap(),
        json!({}),
        &run_store,
        &resolver,
        &checkpoints,
        Some(run_id.clone()),
    )
    .await
    .expect("resumed run should succeed");

    // All three nodes are present in the final outputs.
    let outputs = result["outputs"].as_object().unwrap();
    assert!(outputs.contains_key("a") && outputs.contains_key("b") && outputs.contains_key("c"));

    // Idempotency: `a` ran exactly once (not re-run on resume); `c` once; `b` twice (failed, then
    // succeeded on resume).
    let counts = counts.lock().unwrap();
    assert_eq!(counts.get("a"), Some(&1), "a must not re-run on resume");
    assert_eq!(counts.get("c"), Some(&1));
    assert_eq!(counts.get("b"), Some(&2), "b failed once then succeeded");
}

#[tokio::test]
async fn resuming_a_completed_run_is_a_noop() {
    let run_store = InMemoryRunStore::new();
    let checkpoints = InMemoryCheckpointStore::new();
    let run_id = "done-1".to_string();

    // Complete a run.
    let first = execute_resumable(
        TypeSafeDag::from_json(CHAIN).unwrap(),
        json!({}),
        &run_store,
        &checkpoints,
        Some(run_id.clone()),
    )
    .await
    .unwrap();

    // Resuming the same (completed) run returns the recorded result without re-executing.
    let again = execute_resumable(
        TypeSafeDag::from_json(CHAIN).unwrap(),
        json!({}),
        &run_store,
        &checkpoints,
        Some(run_id.clone()),
    )
    .await
    .unwrap();

    assert_eq!(first["outputs"], again["outputs"]);
    assert_eq!(again["run_id"], json!("done-1"));
}

#[tokio::test]
async fn file_checkpoints_survive_a_fresh_store() {
    let dir = std::env::temp_dir().join(format!("gf-ckpt-{}", std::process::id()));
    let run_id = "filed-1".to_string();

    // Complete a run, persisting checkpoints to disk.
    {
        let run_store = InMemoryRunStore::new();
        let checkpoints = FileCheckpointStore::new(&dir).unwrap();
        execute_resumable(
            TypeSafeDag::from_json(CHAIN).unwrap(),
            json!({}),
            &run_store,
            &checkpoints,
            Some(run_id.clone()),
        )
        .await
        .unwrap();
    }

    // A brand-new store over the same dir sees the completed checkpoint and resume is a no-op.
    {
        let run_store = InMemoryRunStore::new();
        let checkpoints = FileCheckpointStore::new(&dir).unwrap();
        let resumed = execute_resumable(
            TypeSafeDag::from_json(CHAIN).unwrap(),
            json!({}),
            &run_store,
            &checkpoints,
            Some(run_id.clone()),
        )
        .await
        .unwrap();
        assert!(resumed["outputs"].as_object().unwrap().contains_key("c"));
    }

    let _ = std::fs::remove_dir_all(&dir);
}
