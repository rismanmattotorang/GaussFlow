//! Phase 6: bounded-concurrent execution + backpressure.
//!
//! A diamond DAG (source → 4 middles → sink) with sleepy handlers. We assert that independent
//! nodes actually run concurrently (measured speedup), that the `concurrency` setting bounds the
//! number running at once (backpressure), and that dependencies/outputs stay correct.

use async_trait::async_trait;
use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::handler::{NodeHandler, NodeInput};
use gaussflow_runtime::{execute_concurrent_with, InMemoryRunStore};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Tracks how many handlers run at once (current + observed max) and sleeps to create overlap.
struct SleepyHandler {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    sleep_ms: u64,
}

#[async_trait]
impl NodeHandler for SleepyHandler {
    async fn execute(
        &self,
        node: &gaussflow_core::model::NodeSpec,
        _input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(json!({ "id": node.id }))
    }
}

fn diamond(concurrency: u32) -> String {
    format!(
        r#"{{
            "name": "diamond",
            "nodes": [
                {{ "id": "s",  "type": "data_processor", "params": {{}} }},
                {{ "id": "m1", "type": "data_processor", "params": {{}} }},
                {{ "id": "m2", "type": "data_processor", "params": {{}} }},
                {{ "id": "m3", "type": "data_processor", "params": {{}} }},
                {{ "id": "m4", "type": "data_processor", "params": {{}} }},
                {{ "id": "k",  "type": "data_processor", "params": {{}} }}
            ],
            "connections": [
                {{ "from": "s", "to": "m1", "on": "success" }},
                {{ "from": "s", "to": "m2", "on": "success" }},
                {{ "from": "s", "to": "m3", "on": "success" }},
                {{ "from": "s", "to": "m4", "on": "success" }},
                {{ "from": "m1", "to": "k", "on": "success" }},
                {{ "from": "m2", "to": "k", "on": "success" }},
                {{ "from": "m3", "to": "k", "on": "success" }},
                {{ "from": "m4", "to": "k", "on": "success" }}
            ],
            "settings": {{ "concurrency": {concurrency}, "fail_fast": false }}
        }}"#
    )
}

fn resolver(
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    sleep_ms: u64,
) -> impl Fn(&gaussflow_core::model::NodeType) -> Box<dyn NodeHandler> + Send + Sync {
    move |_k| {
        Box::new(SleepyHandler {
            current: current.clone(),
            max: max.clone(),
            sleep_ms,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_independent_nodes_concurrently_with_a_speedup() {
    let current = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let res = resolver(current.clone(), max.clone(), 50);

    let dag = TypeSafeDag::from_json(&diamond(4)).unwrap();
    let store = InMemoryRunStore::new();
    let start = Instant::now();
    let out = execute_concurrent_with(dag, json!({}), &store, &res)
        .await
        .unwrap();
    let elapsed = start.elapsed();

    // Correctness: all six nodes ran.
    let outputs = out["outputs"].as_object().unwrap();
    for id in ["s", "m1", "m2", "m3", "m4", "k"] {
        assert!(outputs.contains_key(id), "missing {id}");
    }
    // The 4 middles overlapped (≥2 at once), and the run was far faster than fully sequential
    // (which would be ~6 × 50ms = 300ms).
    assert!(max.load(Ordering::SeqCst) >= 2, "expected real concurrency");
    assert!(
        elapsed < Duration::from_millis(250),
        "expected a speedup, took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_setting_bounds_in_flight_work() {
    let current = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let res = resolver(current.clone(), max.clone(), 40);

    // 4 middles but a concurrency cap of 2 → never more than 2 running at once (backpressure).
    let dag = TypeSafeDag::from_json(&diamond(2)).unwrap();
    let store = InMemoryRunStore::new();
    execute_concurrent_with(dag, json!({}), &store, &res)
        .await
        .unwrap();

    assert!(
        max.load(Ordering::SeqCst) <= 2,
        "concurrency cap of 2 exceeded: {}",
        max.load(Ordering::SeqCst)
    );
}
