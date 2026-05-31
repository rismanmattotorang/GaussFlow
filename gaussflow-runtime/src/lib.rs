//! GaussFlow Runtime – Phase 2 scheduler

use gaussflow_core::TypeSafeDag;
use petgraph::visit::{EdgeRef, Topo};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

pub mod checkpoint;
pub mod concurrent;
pub mod metrics;
pub mod provider;
pub mod store;
pub use checkpoint::{
    CheckpointStore, FileCheckpointStore, InMemoryCheckpointStore, NoopCheckpointStore,
};
pub use concurrent::{execute_concurrent, execute_concurrent_with};
pub use store::{InMemoryRunStore, RunStore, SurrealRunStore};

static INIT: Once = Once::new();

/// Initialize observability (tracing + metrics) for GaussFlow Runtime
pub fn init_observability(_service_name: &str) {
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    });
}

/// Expose the runtime metrics in Prometheus text-exposition format (what a `/metrics` endpoint
/// scrapes). Reflects real run/node counters updated by the executor.
pub fn prometheus_metrics() -> String {
    metrics::global().render_prometheus()
}

#[cfg(feature = "metrics")]
pub mod metrics_endpoint {
    use axum::{response::IntoResponse, routing::get, Router};
    use prometheus::{gather, Encoder, TextEncoder};
    use std::net::SocketAddr;
    use tokio::task;

    pub async fn serve_metrics(addr: &str) {
        let app = Router::new().route("/metrics", get(metrics_handler));
        let addr: SocketAddr = addr.parse().expect("Invalid metrics address");
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .expect("Failed to start metrics server");
    }

    async fn metrics_handler() -> impl IntoResponse {
        let metric_families = gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    pub fn spawn_metrics_server(addr: &str) {
        let addr = addr.to_string();
        task::spawn(async move {
            serve_metrics(&addr).await;
        });
    }
}

pub mod handler;
pub mod planner;
pub mod sys;

/// Resolve SurrealDB connection settings from the environment, falling back to local-dev
/// defaults. No credentials are hardcoded; set `GAUSSFLOW_DB_PASS` for any non-local deployment.
/// Returns `(url, user, password, namespace, database)`.
pub(crate) fn surreal_settings() -> (String, String, String, String, String) {
    let url = std::env::var("GAUSSFLOW_SURREAL_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:8000/rpc".to_string());
    let user = std::env::var("GAUSSFLOW_DB_USER").unwrap_or_else(|_| "root".to_string());
    let pass = std::env::var("GAUSSFLOW_DB_PASS").unwrap_or_else(|_| {
        warn!(
            "GAUSSFLOW_DB_PASS is not set; using an insecure local-dev default. \
               Set it before deploying GaussFlow anywhere non-local."
        );
        "root".to_string()
    });
    let ns = std::env::var("GAUSSFLOW_DB_NS").unwrap_or_else(|_| "gaussflow".to_string());
    let db = std::env::var("GAUSSFLOW_DB_NAME").unwrap_or_else(|_| "gaussflow".to_string());
    (url, user, pass, ns, db)
}

/// Execute a workflow DAG.
///
/// Selects a [`RunStore`] backend and delegates to [`execute_with_store`]. By default this uses
/// the dependency-free [`InMemoryRunStore`], so **no database is required**. Set
/// `GAUSSFLOW_RUN_STORE=surreal` to persist runs to SurrealDB instead.
///
/// Returns a JSON object `{ "run_id", "output", "outputs" }` where `outputs` maps each node id
/// (plus `"input"`) to its result and `output` is the result of the last node in topological order.
pub async fn execute(
    dag: TypeSafeDag,
    input: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let use_surreal = std::env::var("GAUSSFLOW_RUN_STORE")
        .map(|v| v.eq_ignore_ascii_case("surreal"))
        .unwrap_or(false);

    if use_surreal {
        let store = SurrealRunStore::connect().await?;
        execute_with_store(dag, input, &store).await
    } else {
        let store = InMemoryRunStore::new();
        execute_with_store(dag, input, &store).await
    }
}

/// Resolves a node type to the handler that executes it. The default is [`handler::handler_for`];
/// tests and embedders can supply a custom resolver (e.g. recording or mock handlers).
pub type HandlerResolver =
    dyn Fn(&gaussflow_core::model::NodeType) -> Box<dyn handler::NodeHandler> + Send + Sync;

/// Execute a workflow DAG against an explicit [`RunStore`], using the default node handlers.
///
/// This is the single canonical execution path: a topological walk that respects dependencies,
/// merges predecessor outputs as each node's input, applies per-node timeouts and retry/backoff,
/// and bounds concurrency with CPU/GPU semaphores. Run lifecycle events are written to `store`.
pub async fn execute_with_store(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    execute_with(dag, input, store, &|kind| handler::handler_for(kind)).await
}

/// Execute a workflow DAG against an explicit [`RunStore`] and an explicit [`HandlerResolver`].
///
/// Same topological, dependency-respecting walk as [`execute_with_store`], but the handler used
/// for each node is produced by `resolve`. This is the seam used for testing scheduler behavior
/// with recording handlers, and for embedding custom node implementations.
pub async fn execute_with(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
    resolve: &HandlerResolver,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    execute_core(
        dag,
        input,
        store,
        resolve,
        &checkpoint::NoopCheckpointStore,
        None,
    )
    .await
}

/// Execute — or **resume** — a workflow with checkpointing.
///
/// After each node the engine writes a checkpoint (per-node results + status) to `checkpoints`.
/// If `resume_run_id` names a run with a saved checkpoint, execution resumes from it:
/// already-completed nodes are **not** re-run (idempotent / exactly-once), only the rest execute,
/// and an already-`Completed` run returns its recorded result. This lets a run survive an
/// interruption and be driven to the correct final state.
pub async fn execute_resumable(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
    checkpoints: &dyn checkpoint::CheckpointStore,
    resume_run_id: Option<String>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    execute_core(
        dag,
        input,
        store,
        &|kind| handler::handler_for(kind),
        checkpoints,
        resume_run_id,
    )
    .await
}

/// Like [`execute_resumable`] but with an explicit [`HandlerResolver`] (for custom/test handlers).
#[allow(clippy::too_many_arguments)]
pub async fn execute_resumable_with(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
    resolve: &HandlerResolver,
    checkpoints: &dyn checkpoint::CheckpointStore,
    resume_run_id: Option<String>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    execute_core(dag, input, store, resolve, checkpoints, resume_run_id).await
}

#[allow(clippy::too_many_arguments)]
async fn execute_core(
    dag: TypeSafeDag,
    input: Value,
    store: &dyn RunStore,
    resolve: &HandlerResolver,
    checkpoints: &dyn checkpoint::CheckpointStore,
    resume_run_id: Option<String>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    use gaussflow_core::model::WorkflowStatus;

    let mut outputs: HashMap<String, Value> = HashMap::new();
    // Node ids that were activated (ran). Used for conditional edge traversal: a node only
    // activates downstream edges if it itself ran.
    let mut active_set: HashSet<String> = HashSet::new();
    let mut last_node_id: Option<String> = None;
    let run_id: String;
    let mut resuming = false;

    if let Some(id) = resume_run_id {
        if let Some(cp) = checkpoints.load(&id).await? {
            // Resume: seed state from the checkpoint.
            resuming = true;
            outputs = cp.node_results;
            // Reconstruct the active set: any real node whose recorded output is not a skip marker.
            for (nid, out) in &outputs {
                if nid != "input" && out.get("skipped").and_then(|s| s.as_bool()) != Some(true) {
                    active_set.insert(nid.clone());
                }
            }
            last_node_id = cp.last_executed_node.clone();
            // An already-completed run is idempotent: return its recorded result.
            if matches!(cp.status, WorkflowStatus::Completed) {
                let final_output = last_node_id
                    .as_ref()
                    .and_then(|i| outputs.get(i))
                    .cloned()
                    .unwrap_or_default();
                let outputs_map: serde_json::Map<String, Value> = outputs.into_iter().collect();
                return Ok(json!({ "run_id": id, "output": final_output, "outputs": outputs_map }));
            }
        }
        run_id = id;
    } else {
        run_id = Uuid::new_v4().to_string();
    }

    if !resuming {
        store.start_run(&run_id, &input).await?;
        outputs.insert("input".to_string(), input);
        metrics::global().record_run_started();
    }

    let cpu_sem = Arc::new(Semaphore::new(
        dag.settings.concurrency.unwrap_or(num_cpus::get() as u32) as usize,
    ));
    let gpu_sem = Arc::new(Semaphore::new(1)); // placeholder for single local GPU

    let mut topo = Topo::new(&dag.graph);

    while let Some(nx) = topo.next(&dag.graph) {
        if dag.settings.fail_fast && outputs.values().any(|v| v.get("error").is_some()) {
            break;
        }
        let n = &dag.graph[nx];
        let node_id = n.id.clone();

        // Idempotency / exactly-once: a node already recorded in a prior (resumed) run is not
        // re-executed. Keep `last_node_id` tracking active nodes for a deterministic final output.
        if outputs.contains_key(&node_id) {
            if active_set.contains(&node_id) {
                last_node_id = Some(node_id);
            }
            continue;
        }

        // Activation + input assembly with conditional edge traversal.
        // A source node (no incoming edges) is always active and receives the run input.
        // Otherwise the node is active iff at least one incoming edge is "taken" from an active
        // predecessor (see `edge_taken`); its input is the merge of those predecessors' outputs.
        // Nodes that are not activated are skipped, and their outgoing edges are never taken.
        let incoming: Vec<_> = dag
            .graph
            .edges_directed(nx, petgraph::Direction::Incoming)
            .collect();

        let (is_active, node_input) = if incoming.is_empty() {
            (
                true,
                handler::NodeInput {
                    merged: outputs.get("input").cloned().unwrap_or_else(|| json!({})),
                    sources: Vec::new(),
                },
            )
        } else {
            let mut active = false;
            let mut merged = json!({});
            let mut sources: Vec<(String, Value)> = Vec::new();
            for e in &incoming {
                let src_id = dag.graph[e.source()].id.clone();
                if !active_set.contains(&src_id) {
                    continue;
                }
                let src_out = outputs.get(&src_id);
                if edge_taken(&e.weight().on, src_out) {
                    active = true;
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
            (active, handler::NodeInput { merged, sources })
        };

        let node_type_name = node_type_name(&n.node_type);

        if !is_active {
            metrics::global().record_node(&node_type_name, metrics::NodeOutcome::Skipped, 0);
            outputs.insert(node_id.clone(), json!({ "skipped": true }));
            save_checkpoint(
                checkpoints,
                &run_id,
                &outputs,
                Some(node_id),
                WorkflowStatus::Running,
                None,
            )
            .await?;
            continue;
        }
        active_set.insert(node_id.clone());

        let handler = resolve(&n.node_type);

        let resources = n.resources.as_ref();
        let permit_sem = if resources.is_some_and(|r| r.gpu_count > 0) {
            gpu_sem.clone()
        } else {
            cpu_sem.clone()
        };

        let n_ref = n.clone();
        let run_id_for_span = run_id.clone();
        let node_start = std::time::Instant::now();
        let task = tokio::spawn(async move {
            // One span per node, tagged with the run id for run/node log correlation.
            let span = tracing::span!(
                tracing::Level::INFO,
                "node_execute",
                run = %run_id_for_span,
                node = %n_ref.id,
            );
            let _enter = span.enter();
            let _permit = permit_sem.acquire().await.unwrap();
            let mut attempt = 0u32;
            loop {
                let timeout_ms = n_ref
                    .resources
                    .as_ref()
                    .map(|r| r.timeout_ms)
                    .unwrap_or(60_000);
                let exec_res = tokio::time::timeout(
                    Duration::from_millis(timeout_ms),
                    handler.execute(&n_ref, node_input.clone()),
                )
                .await;
                match exec_res {
                    Ok(inner) => match inner {
                        Ok(v) => break Ok(v),
                        Err(e) => {
                            attempt += 1;
                            if let Some(retry) = &n_ref.retry {
                                if attempt <= retry.max_attempts {
                                    let backoff_ms = match retry.backoff {
                                        gaussflow_core::model::Backoff::Exponential => {
                                            2u64.pow(attempt) * 100
                                        }
                                        gaussflow_core::model::Backoff::Fixed => 500,
                                        gaussflow_core::model::Backoff::Linear => {
                                            (attempt * 100).into()
                                        }
                                    };
                                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                    continue;
                                }
                            }
                            error!("node error: {e}");
                            break Err(e);
                        }
                    },
                    Err(_) => {
                        // timeout elapsed
                        break Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("DAG Timeout: {}", n_ref.id.clone()),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
            }
        });

        // Record failures (run store + a Failed checkpoint) before propagating. The failed node is
        // NOT added to `outputs`, so a resume retries exactly that node.
        let node_output = match task.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                metrics::global().record_node(
                    &node_type_name,
                    metrics::NodeOutcome::Failed,
                    node_start.elapsed().as_millis() as u64,
                );
                metrics::global().record_run_failed();
                let _ = store.fail_run(&run_id, &e.to_string()).await;
                let _ = save_checkpoint(
                    checkpoints,
                    &run_id,
                    &outputs,
                    last_node_id.clone(),
                    WorkflowStatus::Failed,
                    Some(e.to_string()),
                )
                .await;
                return Err(e);
            }
            Err(join_err) => {
                metrics::global().record_node(
                    &node_type_name,
                    metrics::NodeOutcome::Failed,
                    node_start.elapsed().as_millis() as u64,
                );
                metrics::global().record_run_failed();
                let _ = store.fail_run(&run_id, &join_err.to_string()).await;
                let _ = save_checkpoint(
                    checkpoints,
                    &run_id,
                    &outputs,
                    last_node_id.clone(),
                    WorkflowStatus::Failed,
                    Some(join_err.to_string()),
                )
                .await;
                return Err(Box::new(join_err));
            }
        };
        metrics::global().record_node(
            &node_type_name,
            metrics::NodeOutcome::Executed,
            node_start.elapsed().as_millis() as u64,
        );
        outputs.insert(node_id.clone(), node_output);
        last_node_id = Some(node_id);
        save_checkpoint(
            checkpoints,
            &run_id,
            &outputs,
            last_node_id.clone(),
            WorkflowStatus::Running,
            None,
        )
        .await?;
    }

    // The "final" output is the last node in topological order (deterministic), with the full
    // per-node output map returned alongside so callers can pick a different sink.
    let final_output = last_node_id
        .as_ref()
        .and_then(|id| outputs.get(id))
        .cloned()
        .unwrap_or_default();

    store.finish_run(&run_id, &final_output).await?;
    metrics::global().record_run_completed();
    save_checkpoint(
        checkpoints,
        &run_id,
        &outputs,
        last_node_id.clone(),
        gaussflow_core::model::WorkflowStatus::Completed,
        None,
    )
    .await?;

    let outputs_map: serde_json::Map<String, Value> = outputs.into_iter().collect();
    Ok(json!({
        "run_id": run_id,
        "output": final_output,
        "outputs": outputs_map,
    }))
}

/// Persist the current execution state as a checkpoint for `run_id`.
async fn save_checkpoint(
    checkpoints: &dyn checkpoint::CheckpointStore,
    run_id: &str,
    outputs: &HashMap<String, Value>,
    last_executed_node: Option<String>,
    status: gaussflow_core::model::WorkflowStatus,
    error: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let cp = gaussflow_core::model::Checkpoint {
        id: run_id.to_string(),
        timestamp,
        status,
        last_executed_node,
        node_results: outputs.clone(),
        error,
    };
    checkpoints.save(run_id, &cp).await
}

/// The metric/label name for a node type (its snake_case serde name, e.g. `llm_call`).
pub(crate) fn node_type_name(node_type: &gaussflow_core::model::NodeType) -> String {
    serde_json::to_value(node_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Decide whether a graph edge is "taken", given its `on` label and the source node's output.
///
/// - `success` (the default): taken when the source completed without an `error` field.
/// - `failure`: taken when the source produced an `error` field.
/// - any other label: taken when the source's `branch` or `route` output equals the label — this
///   is how `conditional` and `router` nodes select which downstream paths execute.
pub(crate) fn edge_taken(on: &str, src_out: Option<&Value>) -> bool {
    match on {
        "success" => src_out.is_some_and(|o| o.get("error").is_none()),
        "failure" => src_out.is_some_and(|o| o.get("error").is_some()),
        label => src_out.is_some_and(|o| {
            o.get("branch").and_then(|b| b.as_str()) == Some(label)
                || o.get("route").and_then(|r| r.as_str()) == Some(label)
        }),
    }
}
