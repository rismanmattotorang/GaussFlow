//! # GaussFlow Core
//!
//! This crate provides the core data structures and logic for building and validating
//! GaussFlow workflows. It defines the `TypeSafeDag` and the models that represent
//! a workflow specification, parsed from GaussFlow's extended n8n JSON format.
//!
//! The main entry point is the `TypeSafeDag::from_spec` or `TypeSafeDag::from_json`
//! function, which validates and constructs an in-memory DAG from a specification.
//! A high-performance, extensible workflow engine for building and executing
//! complex data processing pipelines with support for LLMs, agents, and more.

//! GaussFlow Core - The heart of the GaussFlow workflow engine

pub mod checkpoint;
pub mod dag;
pub mod engine;
pub mod error;
pub mod executor;
pub mod hash_impls;
pub mod metrics;
pub mod model;
pub mod node;
pub mod policy;
pub mod resource;
pub mod scheduler;
pub mod storage;
pub mod validator;
pub mod versioning;

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

use petgraph::graph::{EdgeIndex, NodeIndex};
use std::sync::Once;
use thiserror::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static INIT: Once = Once::new();

/// Initialize observability (tracing + metrics) for GaussFlow
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

// Re-export commonly used types from submodules
pub use crate::checkpoint::{CheckpointManager, CheckpointStore, FileCheckpointStore};
pub use crate::dag::{DagEdge, DagNode, DagValidationError};
pub use crate::engine::{
    ExecutionEngine, ExecutionError as EngineError, TaskQueue, TaskQueueReceiver,
};
pub use crate::error::{
    ExecutionError as ErrorExecutionError, GaussFlowError, PolicyError, ResourceError,
};
pub use crate::executor::{Middleware, NodeExecutor as ExecutorNodeExecutor, PolicyHook};
pub use crate::metrics::Metrics;
pub use crate::model::{EdgeSpec, NodeSpec, NodeType, WorkflowSettings, WorkflowSpec};
pub use crate::node::{
    AuditPolicy, MonitoringPolicy, NodeSpec as NodeNodeSpec, NodeType as NodeNodeType,
    SecurityPolicy,
};
pub use crate::policy::Policy;
pub use crate::resource::{ResourceManager, ResourceSpec, ResourceUsage};
pub use crate::scheduler::{PriorityScheduler, Scheduler, SchedulerConfig};
pub use crate::validator::{DagValidator, DefaultDagValidator};
pub use crate::versioning::{VersionManager, VersionedWorkflow, VersioningError};

// Re-export TypeSafeDag with proper generic parameters
pub type TypeSafeDag<N = crate::model::NodeSpec, E = crate::model::EdgeSpec> =
    crate::dag::TypeSafeDag<N, E>;

/// A type alias for TypeSafeDag with default node and edge types
pub type TypeSafeDagDefault = TypeSafeDag<crate::model::NodeSpec, crate::model::EdgeSpec>;
pub type NodeId = NodeIndex;
pub type EdgeId = EdgeIndex;

// ----- Core Error Types -----
#[derive(Debug, Error)]
pub enum DagError {
    #[error("A cycle was detected in the workflow graph.")]
    Cycle,

    #[error("Node with ID '{0}' was not found.")]
    NodeNotFound(String),

    #[error("Duplicate node ID '{0}' is not allowed.")]
    DuplicateNodeId(String),

    #[error("Duplicate edge from '{0}' to '{1}' is not allowed.")]
    DuplicateEdge(String, String),

    #[error("I/O error while reading workflow: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse workflow JSON: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Node '{0}' timed out")]
    Timeout(String),
}
