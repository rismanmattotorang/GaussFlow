//! GaussFlow Runtime – Phase 2 scheduler

use gaussflow_core::TypeSafeDag;
use petgraph::visit::Topo;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

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
fn surreal_settings() -> (String, String, String, String, String) {
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

pub async fn execute(
    dag: TypeSafeDag,
    input: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    info!("Connecting to SurrealDB");
    // Connect to SurrealDB using environment-sourced credentials.
    let (surreal_url, db_user, db_pass, ns, db_name) = surreal_settings();
    let db = Surreal::new::<Ws>(surreal_url.as_str()).await?;
    db.signin(Root {
        username: db_user.as_str(),
        password: db_pass.as_str(),
    })
    .await?;
    db.use_ns(ns.as_str()).use_db(db_name.as_str()).await?;

    let run_id = Uuid::new_v4().to_string();
    db.query("CREATE run SET id = $id, status = 'running', started = time::now(), input = $input")
        .bind(("id", run_id.clone()))
        .bind(("input", input.clone()))
        .await?;

    let cpu_sem = Arc::new(Semaphore::new(
        dag.settings.concurrency.unwrap_or(num_cpus::get() as u32) as usize,
    ));
    let gpu_sem = Arc::new(Semaphore::new(1)); // placeholder for single local GPU

    let mut topo = Topo::new(&dag.graph);
    let mut outputs = HashMap::new();
    outputs.insert("input".to_string(), input);

    while let Some(nx) = topo.next(&dag.graph) {
        if dag.settings.fail_fast && outputs.values().any(|v| v.get("error").is_some()) {
            break;
        }
        let n = &dag.graph[nx];

        // Basic input assembly: merge outputs of all predecessors
        let predecessors = dag
            .graph
            .neighbors_directed(nx, petgraph::Direction::Incoming);
        let mut merged_input = json!({});
        for p_nx in predecessors {
            if let Some(output) = outputs.get(&dag.graph[p_nx].id) {
                // For simplicity, we merge outputs; real implementation needs named ports
                output.as_object().unwrap().iter().for_each(|(k, v)| {
                    merged_input[k] = v.clone();
                });
            }
        }

        let handler = handler::handler_for(&n.node_type);

        // Handle resources with proper Option handling
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
                        )));
                    }
                }
            }
        });

        outputs.insert(n.id.clone(), task.await??);
    }

    // For now, return the output of the last node processed (usually the sink)
    let output = outputs.values().last().cloned().unwrap_or_default();

    db.query(
        "UPDATE type::thing('run', $id) SET status='finished', finished=time::now(), output=$out",
    )
    .bind(("id", run_id.clone()))
    .bind(("out", output.clone()))
    .await?;

    Ok(json!({ "run_id": run_id }))
}
