//! GaussFlow Runtime – Phase 2 scheduler

use gaussflow_core::TypeSafeDag;
use petgraph::visit::Topo;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

pub mod provider;
pub mod store;
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

/// Expose Prometheus metrics as a string
pub fn prometheus_metrics() -> String {
    "Metrics not available".to_string()
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
    let run_id = Uuid::new_v4().to_string();
    store.start_run(&run_id, &input).await?;

    let cpu_sem = Arc::new(Semaphore::new(
        dag.settings.concurrency.unwrap_or(num_cpus::get() as u32) as usize,
    ));
    let gpu_sem = Arc::new(Semaphore::new(1)); // placeholder for single local GPU

    let mut topo = Topo::new(&dag.graph);
    let mut outputs: HashMap<String, Value> = HashMap::new();
    outputs.insert("input".to_string(), input);
    let mut last_node_id: Option<String> = None;

    while let Some(nx) = topo.next(&dag.graph) {
        if dag.settings.fail_fast && outputs.values().any(|v| v.get("error").is_some()) {
            break;
        }
        let n = &dag.graph[nx];

        // Input assembly: merge the outputs of all predecessors. Source nodes (no predecessors)
        // receive the workflow's run input, so input flows into the graph at its roots.
        let predecessors = dag
            .graph
            .neighbors_directed(nx, petgraph::Direction::Incoming);
        let mut merged_input = json!({});
        let mut had_predecessor = false;
        for p_nx in predecessors {
            had_predecessor = true;
            if let Some(output) = outputs.get(&dag.graph[p_nx].id) {
                if let Some(obj) = output.as_object() {
                    for (k, v) in obj {
                        merged_input[k] = v.clone();
                    }
                }
            }
        }
        if !had_predecessor {
            merged_input = outputs.get("input").cloned().unwrap_or_else(|| json!({}));
        }

        let handler = resolve(&n.node_type);

        let resources = n.resources.as_ref();
        let permit_sem = if resources.is_some_and(|r| r.gpu_count > 0) {
            gpu_sem.clone()
        } else {
            cpu_sem.clone()
        };

        let n_ref = n.clone();
        let task = tokio::spawn(async move {
            let span = tracing::span!(tracing::Level::INFO, "node_execute", node = %n_ref.id);
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
                    handler.execute(&n_ref, merged_input.clone()),
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

        // Record failures in the store before propagating.
        let node_output = match task.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                let _ = store.fail_run(&run_id, &e.to_string()).await;
                return Err(e);
            }
            Err(join_err) => {
                let _ = store.fail_run(&run_id, &join_err.to_string()).await;
                return Err(Box::new(join_err));
            }
        };
        outputs.insert(n.id.clone(), node_output);
        last_node_id = Some(n.id.clone());
    }

    // The "final" output is the last node in topological order (deterministic), with the full
    // per-node output map returned alongside so callers can pick a different sink.
    let final_output = last_node_id
        .as_ref()
        .and_then(|id| outputs.get(id))
        .cloned()
        .unwrap_or_default();

    store.finish_run(&run_id, &final_output).await?;

    let outputs_map: serde_json::Map<String, Value> = outputs.into_iter().collect();
    Ok(json!({
        "run_id": run_id,
        "output": final_output,
        "outputs": outputs_map,
    }))
}
