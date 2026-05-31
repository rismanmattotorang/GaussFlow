//! Property tests for the scheduler (Phase 1).
//!
//! For randomly generated DAGs we assert the core scheduling invariant: **no node runs before all
//! of its dependencies have completed**, and every node runs exactly once. We observe the actual
//! execution order by injecting a recording handler via the [`execute_with`] seam.

use async_trait::async_trait;
use gaussflow_core::model::NodeType;
use gaussflow_core::TypeSafeDag;
use gaussflow_runtime::handler::{NodeHandler, NodeInput};
use gaussflow_runtime::{execute_with, InMemoryRunStore};
use proptest::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A handler that records the id of each node as it executes, in completion order.
struct RecordingHandler {
    order: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl NodeHandler for RecordingHandler {
    async fn execute(
        &self,
        node: &gaussflow_core::model::NodeSpec,
        _input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.order.lock().unwrap().push(node.id.clone());
        Ok(json!({ "id": node.id }))
    }
}

/// Build a workflow JSON from a node count and a set of `(from_idx, to_idx)` edges.
fn workflow_json(num_nodes: usize, edges: &[(usize, usize)]) -> String {
    let nodes: Vec<String> = (0..num_nodes)
        .map(|i| format!(r#"{{ "id": "n{i}", "type": "data_processor", "params": {{}} }}"#))
        .collect();
    let conns: Vec<String> = edges
        .iter()
        .map(|(a, b)| format!(r#"{{ "from": "n{a}", "to": "n{b}", "on": "success" }}"#))
        .collect();
    format!(
        r#"{{ "name": "proptest", "nodes": [{}], "connections": [{}],
             "settings": {{ "concurrency": 4, "fail_fast": false }} }}"#,
        nodes.join(","),
        conns.join(",")
    )
}

/// Strategy: 1..=8 nodes and an arbitrary subset of forward edges `(i, j)` with `i < j`
/// (guaranteeing an acyclic graph).
fn dag_strategy() -> impl Strategy<Value = (usize, Vec<(usize, usize)>)> {
    (1usize..=8).prop_flat_map(|n| {
        let pairs: Vec<(usize, usize)> = (0..n)
            .flat_map(|i| (i + 1..n).map(move |j| (i, j)))
            .collect();
        let len = pairs.len();
        proptest::sample::subsequence(pairs, 0..=len).prop_map(move |edges| (n, edges))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn scheduler_respects_dependencies((num_nodes, edges) in dag_strategy()) {
        let json = workflow_json(num_nodes, &edges);
        let dag = TypeSafeDag::from_json(&json).expect("generated workflow should parse");

        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let order_for_resolver = order.clone();
        let resolver = move |_kind: &NodeType| -> Box<dyn NodeHandler> {
            Box::new(RecordingHandler { order: order_for_resolver.clone() })
        };

        let store = InMemoryRunStore::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            execute_with(dag, json!({}), &store, &resolver)
                .await
                .expect("execution should succeed for any valid DAG");
        });

        let run_order = order.lock().unwrap().clone();

        // Every node ran exactly once.
        prop_assert_eq!(run_order.len(), num_nodes);
        let positions: HashMap<String, usize> = run_order
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        for i in 0..num_nodes {
            prop_assert!(positions.contains_key(&format!("n{i}")), "node n{} never ran", i);
        }

        // The invariant: for every dependency edge (u -> v), u completed before v started.
        for (a, b) in &edges {
            let pu = positions[&format!("n{a}")];
            let pv = positions[&format!("n{b}")];
            prop_assert!(pu < pv, "n{} ran before its dependency n{} ({} !< {})", b, a, pu, pv);
        }
    }
}
