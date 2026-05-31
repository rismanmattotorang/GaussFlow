//! Phase 4: the executor feeds real metrics, and `prometheus_metrics()` renders them.

use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::{execute_with_store, prometheus_metrics, InMemoryRunStore};
use serde_json::json;

const WF: &str = r#"{
    "name": "metrics-wf",
    "nodes": [
        { "id": "a", "type": "data_processor", "params": {} },
        { "id": "b", "type": "data_processor", "params": {} }
    ],
    "connections": [ { "from": "a", "to": "b", "on": "success" } ],
    "settings": { "concurrency": 2 }
}"#;

#[tokio::test]
async fn executor_updates_global_metrics_and_exposition() {
    // Counters are process-global and monotonic, so assert a delta (robust under parallel tests).
    let before = gaussflow_runtime::metrics::global().snapshot();

    let dag = TypeSafeDag::from_json(WF).unwrap();
    let store = InMemoryRunStore::new();
    execute_with_store(dag, json!({}), &store).await.unwrap();

    let after = gaussflow_runtime::metrics::global().snapshot();
    assert!(after.runs_started > before.runs_started);
    assert!(after.runs_completed > before.runs_completed);
    // Two data_processor nodes executed.
    assert!(after.nodes_executed >= before.nodes_executed + 2);

    // The Prometheus exposition reflects real, non-stub data.
    let text = prometheus_metrics();
    assert!(text.contains("# TYPE gaussflow_runs_started_total counter"));
    assert!(
        text.contains("gaussflow_nodes_total{node_type=\"data_processor\",outcome=\"executed\"}")
    );
    assert!(!text.contains("Metrics not available"));
}
