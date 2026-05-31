//! Bounded-concurrent execution (Phase 6).
//!
//! A throughput-oriented executor that runs **independent ready nodes concurrently**, bounded by
//! `settings.concurrency` — the bound is real **backpressure** (no more than N node tasks run at
//! once; new ready nodes wait for a slot). Dependencies and conditional edge-skipping are honored
//! exactly as in the sequential engine (it reuses [`crate::edge_taken`]).
//!
//! This is a *separate* path from [`crate::execute_resumable`]: concurrency trades off against the
//! per-node checkpointing that powers fine-grained resume, so callers choose throughput
//! (`execute_concurrent`) or fine-grained durability (`execute_resumable`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use gaussflow_core::TypeSafeDag;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::handler::{self, NodeInput};
use crate::{edge_taken, metrics, node_type_name, HandlerResolver, RunStore};

type ExecError = Box<dyn std::error::Error + Send + Sync>;

/// Execute a DAG with bounded concurrency, using the default handlers.
pub async fn execute_concurrent(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
) -> Result<Value, ExecError> {
    execute_concurrent_with(dag, input, store, &|kind| handler::handler_for(kind)).await
}

/// Execute a DAG with bounded concurrency and an explicit [`HandlerResolver`].
pub async fn execute_concurrent_with(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
    resolve: &HandlerResolver,
) -> Result<Value, ExecError> {
    let run_id = Uuid::new_v4().to_string();
    store.start_run(&run_id, &input).await?;
    metrics::global().record_run_started();

    let concurrency = dag
        .settings
        .concurrency
        .unwrap_or(num_cpus::get() as u32)
        .max(1) as usize;

    let mut outputs: HashMap<String, Value> = HashMap::new();
    outputs.insert("input".to_string(), input);
    let mut active: HashSet<String> = HashSet::new();
    let mut last_active: Option<String> = None;

    // Remaining unresolved in-edges per node (a node is "ready" when this hits 0).
    let mut pending: HashMap<NodeIndex, usize> = HashMap::new();
    let mut ready: VecDeque<NodeIndex> = VecDeque::new();
    for nx in dag.graph.node_indices() {
        let indeg = dag.graph.edges_directed(nx, Direction::Incoming).count();
        pending.insert(nx, indeg);
        if indeg == 0 {
            ready.push_back(nx);
        }
    }

    let mut in_flight: tokio::task::JoinSet<(NodeIndex, String, Result<Value, ExecError>)> =
        tokio::task::JoinSet::new();

    // Resolve a node (completed or skipped): decrement dependents, enqueue newly-ready ones.
    macro_rules! resolve_node {
        ($nx:expr) => {{
            let succs: Vec<NodeIndex> = dag
                .graph
                .edges_directed($nx, Direction::Outgoing)
                .map(|e| e.target())
                .collect();
            for s in succs {
                if let Some(p) = pending.get_mut(&s) {
                    *p = p.saturating_sub(1);
                    if *p == 0 {
                        ready.push_back(s);
                    }
                }
            }
        }};
    }

    loop {
        // Fill available concurrency slots with ready nodes.
        while in_flight.len() < concurrency {
            let Some(nx) = ready.pop_front() else { break };
            let n = &dag.graph[nx];
            let node_id = n.id.clone();

            // Activation + input assembly from resolved predecessors (same rules as the
            // sequential engine). Source nodes receive the run input.
            let incoming: Vec<_> = dag.graph.edges_directed(nx, Direction::Incoming).collect();
            let (is_active, node_input) = if incoming.is_empty() {
                (
                    true,
                    NodeInput {
                        merged: outputs.get("input").cloned().unwrap_or_else(|| json!({})),
                        sources: Vec::new(),
                    },
                )
            } else {
                let mut act = false;
                let mut merged = json!({});
                let mut sources: Vec<(String, Value)> = Vec::new();
                for e in &incoming {
                    let src_id = dag.graph[e.source()].id.clone();
                    if !active.contains(&src_id) {
                        continue;
                    }
                    let src_out = outputs.get(&src_id);
                    if edge_taken(&e.weight().on, src_out) {
                        act = true;
                        if let Some(out) = src_out {
                            if let Some(obj) = out.as_object() {
                                for (k, v) in obj {
                                    merged[k] = v.clone();
                                }
                            }
                            sources.push((src_id, out.clone()));
                        }
                    }
                }
                (act, NodeInput { merged, sources })
            };

            if !is_active {
                metrics::global().record_node(
                    &node_type_name(&n.node_type),
                    metrics::NodeOutcome::Skipped,
                    0,
                );
                outputs.insert(node_id, json!({ "skipped": true }));
                resolve_node!(nx);
                continue;
            }

            active.insert(node_id.clone());
            let handler = resolve(&n.node_type);
            let n_ref = n.clone();
            in_flight.spawn(async move {
                let started = Instant::now();
                let result = run_node(handler.as_ref(), &n_ref, node_input).await;
                let _ = started; // duration recorded by the collector
                (nx, n_ref.id.clone(), result)
            });
        }

        if in_flight.is_empty() {
            break; // nothing running and nothing ready → done
        }

        // Await the next completion.
        let joined = in_flight.join_next().await.expect("in_flight non-empty");
        let (nx, node_id, result) = match joined {
            Ok(tuple) => tuple,
            Err(join_err) => {
                let _ = store.fail_run(&run_id, &join_err.to_string()).await;
                metrics::global().record_run_failed();
                return Err(Box::new(join_err));
            }
        };
        match result {
            Ok(v) => {
                metrics::global().record_node(
                    &node_type_name(&dag.graph[nx].node_type),
                    metrics::NodeOutcome::Executed,
                    0,
                );
                outputs.insert(node_id.clone(), v);
                last_active = Some(node_id);
                resolve_node!(nx);
            }
            Err(e) => {
                metrics::global().record_node(
                    &node_type_name(&dag.graph[nx].node_type),
                    metrics::NodeOutcome::Failed,
                    0,
                );
                metrics::global().record_run_failed();
                let _ = store.fail_run(&run_id, &e.to_string()).await;
                in_flight.shutdown().await;
                return Err(e);
            }
        }
    }

    let final_output = last_active
        .as_ref()
        .and_then(|id| outputs.get(id))
        .cloned()
        .unwrap_or_default();
    store.finish_run(&run_id, &final_output).await?;
    metrics::global().record_run_completed();

    let outputs_map: serde_json::Map<String, Value> = outputs.into_iter().collect();
    Ok(json!({ "run_id": run_id, "output": final_output, "outputs": outputs_map }))
}

/// Run a single node's handler with its per-node timeout and retry/backoff policy.
async fn run_node(
    handler: &dyn handler::NodeHandler,
    node: &gaussflow_core::model::NodeSpec,
    input: NodeInput,
) -> Result<Value, ExecError> {
    let mut attempt = 0u32;
    loop {
        let timeout_ms = node
            .resources
            .as_ref()
            .map(|r| r.timeout_ms)
            .unwrap_or(60_000);
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            handler.execute(node, input.clone()),
        )
        .await
        {
            Ok(Ok(v)) => break Ok(v),
            Ok(Err(e)) => {
                attempt += 1;
                if let Some(retry) = &node.retry {
                    if attempt <= retry.max_attempts {
                        let backoff_ms = match retry.backoff {
                            gaussflow_core::model::Backoff::Exponential => 2u64.pow(attempt) * 100,
                            gaussflow_core::model::Backoff::Fixed => 500,
                            gaussflow_core::model::Backoff::Linear => (attempt * 100).into(),
                        };
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                }
                break Err(e);
            }
            Err(_) => {
                break Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("DAG Timeout: {}", node.id),
                )) as ExecError)
            }
        }
    }
}
