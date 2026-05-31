//! GaussFlow WebUI - Modern web interface for DAG workflow management
//!
//! This application provides a comprehensive web-based interface for:
//! - Workflow visualization and management
//! - Real-time execution monitoring via WebSocket
//! - Workflow creation, editing, and deletion
//! - System metrics and resource monitoring

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{broadcast, RwLock};
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
    trace::TraceLayer,
};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

mod api;
mod templates;

// ============================================================================
// Application State
// ============================================================================

#[derive(Clone)]
pub struct AppState {
    pub workflows: Arc<DashMap<String, WorkflowInfo>>,
    pub executions: Arc<DashMap<String, ExecutionInfo>>,
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub connected_clients: Arc<RwLock<usize>>,
    pub start_time: std::time::Instant,
    pub execution_counter: Arc<AtomicU64>,
}

impl AppState {
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(1000);
        Self {
            workflows: Arc::new(DashMap::new()),
            executions: Arc::new(DashMap::new()),
            event_tx,
            connected_clients: Arc::new(RwLock::new(0)),
            start_time: std::time::Instant::now(),
            execution_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Data Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub node_count: usize,
    pub edge_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: WorkflowStatus,
    pub spec: serde_json::Value,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum WorkflowStatus {
    #[default]
    Draft,
    Active,
    Paused,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionInfo {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub status: ExecutionStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub current_node: Option<String>,
    pub completed_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub progress: f32,
    pub error: Option<String>,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub node_outputs: HashMap<String, serde_json::Value>,
    pub logs: Vec<LogEntry>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ExecutionStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    pub node_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
pub enum LogLevel {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    WorkflowCreated {
        workflow: WorkflowInfo,
    },
    WorkflowUpdated {
        workflow: WorkflowInfo,
    },
    WorkflowDeleted {
        workflow_id: String,
    },
    ExecutionStarted {
        execution: ExecutionInfo,
    },
    ExecutionProgress {
        execution_id: String,
        progress: f32,
        current_node: Option<String>,
    },
    ExecutionNodeCompleted {
        execution_id: String,
        node_id: String,
        output: serde_json::Value,
    },
    ExecutionNodeFailed {
        execution_id: String,
        node_id: String,
        error: String,
    },
    ExecutionCompleted {
        execution: ExecutionInfo,
    },
    ExecutionFailed {
        execution: ExecutionInfo,
    },
    ExecutionCancelled {
        execution_id: String,
    },
    SystemMetrics {
        metrics: SystemMetrics,
    },
    LogEntry {
        execution_id: String,
        log: LogEntry,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub active_executions: usize,
    pub total_workflows: usize,
    pub connected_clients: usize,
    pub uptime_seconds: u64,
    pub total_executions: u64,
}

// ============================================================================
// API Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateWorkflowRequest {
    pub name: String,
    pub description: Option<String>,
    pub spec: serde_json::Value,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWorkflowRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub spec: Option<serde_json::Value>,
    pub status: Option<WorkflowStatus>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteWorkflowRequest {
    pub input: Option<serde_json::Value>,
    #[serde(default)]
    pub async_execution: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct ListQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub status: Option<String>,
    pub search: Option<String>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<ApiError>,
    pub meta: Option<ResponseMeta>,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ResponseMeta {
    pub total: Option<usize>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub has_more: Option<bool>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: None,
        }
    }

    pub fn success_with_meta(data: T, meta: ResponseMeta) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: Some(meta),
        }
    }
}

impl ApiResponse<()> {
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
                details: None,
            }),
            meta: None,
        }
    }

    pub fn error_with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
                details: Some(details),
            }),
            meta: None,
        }
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gaussflow_web=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(
        "Starting GaussFlow WebUI Server v{}",
        env!("CARGO_PKG_VERSION")
    );

    // Create application state
    let state = AppState::new();

    // Load sample workflows for demo
    load_sample_workflows(&state).await;

    // Spawn background task for system metrics
    let metrics_state = state.clone();
    tokio::spawn(async move {
        broadcast_system_metrics(metrics_state).await;
    });

    // Build the router
    let app = create_router(state);

    // Start the server
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("GaussFlow WebUI listening on http://{}", addr);
    info!("  Dashboard: http://{}", addr);
    info!("  API:       http://{}/api", addr);
    info!("  WebSocket: ws://{}/ws", addr);
    info!("  Health:    http://{}/api/health", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Graceful shutdown handling
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        info!("Shutdown signal received, gracefully shutting down...");
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    info!("Server shutdown complete");
    Ok(())
}

// ============================================================================
// Router Setup
// ============================================================================

fn create_router(state: AppState) -> Router {
    // API routes
    let api_routes = Router::new()
        // Workflow endpoints
        .route("/workflows", get(list_workflows).post(create_workflow))
        .route(
            "/workflows/:id",
            get(get_workflow)
                .put(update_workflow)
                .delete(delete_workflow),
        )
        .route("/workflows/:id/execute", post(execute_workflow))
        .route("/workflows/:id/validate", post(validate_workflow))
        .route("/workflows/:id/duplicate", post(duplicate_workflow))
        // Execution endpoints
        .route("/executions", get(list_executions))
        .route(
            "/executions/:id",
            get(get_execution).delete(delete_execution),
        )
        .route("/executions/:id/cancel", post(cancel_execution))
        .route("/executions/:id/pause", post(pause_execution))
        .route("/executions/:id/resume", post(resume_execution))
        .route("/executions/:id/logs", get(get_execution_logs))
        // System endpoints
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .route("/stats", get(get_stats));

    // Create static file service with fallback
    let static_service =
        ServeDir::new("gaussflow-web/static").not_found_service(ServeDir::new("static"));

    Router::new()
        // Serve the main HTML page for SPA routes
        .route("/", get(serve_index))
        .route("/dashboard", get(serve_index))
        .route("/workflows", get(serve_index))
        .route("/workflows/*path", get(serve_index))
        .route("/executions", get(serve_index))
        .route("/executions/*path", get(serve_index))
        .route("/settings", get(serve_index))
        // API routes
        .nest("/api", api_routes)
        // WebSocket endpoint
        .route("/ws", get(websocket_handler))
        // Static files
        .nest_service("/static", static_service)
        // Add middleware
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

// ============================================================================
// Page Handlers
// ============================================================================

async fn serve_index() -> impl IntoResponse {
    Html(include_str!("../static/index.html"))
}

// ============================================================================
// API Handlers - Workflows
// ============================================================================

async fn list_workflows(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(100);
    let page = query.page.unwrap_or(0);

    let mut workflows: Vec<WorkflowInfo> =
        state.workflows.iter().map(|r| r.value().clone()).collect();

    // Filter by status if provided
    if let Some(status_str) = &query.status {
        workflows.retain(|w| {
            let status_match = match status_str.to_lowercase().as_str() {
                "active" => w.status == WorkflowStatus::Active,
                "draft" => w.status == WorkflowStatus::Draft,
                "paused" => w.status == WorkflowStatus::Paused,
                "archived" => w.status == WorkflowStatus::Archived,
                _ => true,
            };
            status_match
        });
    }

    // Filter by search if provided
    if let Some(search) = &query.search {
        let search_lower = search.to_lowercase();
        workflows.retain(|w| {
            w.name.to_lowercase().contains(&search_lower)
                || w.description
                    .as_ref()
                    .is_some_and(|d| d.to_lowercase().contains(&search_lower))
        });
    }

    // Sort workflows
    let sort_order = query.sort_order.as_deref().unwrap_or("desc");
    match query.sort_by.as_deref().unwrap_or("updated_at") {
        "name" => workflows.sort_by(|a, b| {
            if sort_order == "asc" {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        "created_at" => workflows.sort_by(|a, b| {
            if sort_order == "asc" {
                a.created_at.cmp(&b.created_at)
            } else {
                b.created_at.cmp(&a.created_at)
            }
        }),
        _ => workflows.sort_by(|a, b| {
            if sort_order == "asc" {
                a.updated_at.cmp(&b.updated_at)
            } else {
                b.updated_at.cmp(&a.updated_at)
            }
        }),
    }

    let total = workflows.len();
    let workflows: Vec<WorkflowInfo> = workflows
        .into_iter()
        .skip(page * limit)
        .take(limit)
        .collect();

    let has_more = (page + 1) * limit < total;

    Json(ApiResponse::success_with_meta(
        workflows,
        ResponseMeta {
            total: Some(total),
            page: Some(page),
            limit: Some(limit),
            has_more: Some(has_more),
        },
    ))
}

async fn create_workflow(
    State(state): State<AppState>,
    Json(req): Json<CreateWorkflowRequest>,
) -> impl IntoResponse {
    // Validate name
    if req.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "INVALID_NAME", "message": "Workflow name cannot be empty"}
            })),
        );
    }

    // Parse and validate the workflow spec
    let dag_result = gaussflow_core::TypeSafeDag::from_json(
        &serde_json::to_string(&req.spec).unwrap_or_default(),
    );

    match dag_result {
        Ok(dag) => {
            let id = Uuid::new_v4().to_string();
            let now = Utc::now();

            let workflow = WorkflowInfo {
                id: id.clone(),
                name: req.name.trim().to_string(),
                description: req.description.map(|d| d.trim().to_string()),
                node_count: dag.graph.node_count(),
                edge_count: dag.graph.edge_count(),
                created_at: now,
                updated_at: now,
                status: WorkflowStatus::Active,
                spec: req.spec,
                tags: req.tags,
                version: 1,
            };

            state.workflows.insert(id, workflow.clone());

            // Broadcast event
            let _ = state.event_tx.send(ServerEvent::WorkflowCreated {
                workflow: workflow.clone(),
            });

            info!(
                "Created workflow '{}' with {} nodes",
                workflow.name, workflow.node_count
            );

            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "success": true,
                    "data": workflow
                })),
            )
        }
        Err(e) => {
            warn!("Failed to create workflow: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "success": false,
                    "error": {
                        "code": "INVALID_WORKFLOW",
                        "message": format!("Invalid workflow specification: {}", e)
                    }
                })),
            )
        }
    }
}

async fn get_workflow(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.workflows.get(&id) {
        Some(workflow) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "data": workflow.clone()
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
            })),
        ),
    }
}

async fn update_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateWorkflowRequest>,
) -> impl IntoResponse {
    match state.workflows.get_mut(&id) {
        Some(mut workflow) => {
            let now = Utc::now();

            if let Some(name) = req.name {
                if name.trim().is_empty() {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "success": false,
                            "error": {"code": "INVALID_NAME", "message": "Workflow name cannot be empty"}
                        })),
                    );
                }
                workflow.name = name.trim().to_string();
            }

            if let Some(description) = req.description {
                workflow.description = Some(description.trim().to_string());
            }

            if let Some(spec) = req.spec {
                // Validate the new spec
                match gaussflow_core::TypeSafeDag::from_json(
                    &serde_json::to_string(&spec).unwrap_or_default(),
                ) {
                    Ok(dag) => {
                        workflow.spec = spec;
                        workflow.node_count = dag.graph.node_count();
                        workflow.edge_count = dag.graph.edge_count();
                    }
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "success": false,
                                "error": {
                                    "code": "INVALID_WORKFLOW",
                                    "message": format!("Invalid workflow specification: {}", e)
                                }
                            })),
                        );
                    }
                }
            }

            if let Some(status) = req.status {
                workflow.status = status;
            }

            if let Some(tags) = req.tags {
                workflow.tags = tags;
            }

            workflow.updated_at = now;
            workflow.version += 1;

            let updated = workflow.clone();
            drop(workflow);

            // Broadcast event
            let _ = state.event_tx.send(ServerEvent::WorkflowUpdated {
                workflow: updated.clone(),
            });

            info!("Updated workflow '{}'", updated.name);

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": updated
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
            })),
        ),
    }
}

async fn delete_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // Check if there are running executions for this workflow
    let has_running = state
        .executions
        .iter()
        .any(|e| e.workflow_id == id && e.status == ExecutionStatus::Running);

    if has_running {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "success": false,
                "error": {
                    "code": "HAS_RUNNING_EXECUTIONS",
                    "message": "Cannot delete workflow with running executions"
                }
            })),
        );
    }

    match state.workflows.remove(&id) {
        Some((_, workflow)) => {
            let _ = state.event_tx.send(ServerEvent::WorkflowDeleted {
                workflow_id: id.clone(),
            });
            info!("Deleted workflow '{}'", workflow.name);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": {"deleted": true, "id": id}
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
            })),
        ),
    }
}

async fn validate_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.workflows.get(&id) {
        Some(workflow) => {
            let validation_result = gaussflow_core::TypeSafeDag::from_json(
                &serde_json::to_string(&workflow.spec).unwrap_or_default(),
            );

            match validation_result {
                Ok(dag) => {
                    // Try to validate the DAG structure
                    let errors: Vec<String> = Vec::new();
                    let warnings: Vec<String> = Vec::new();

                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "success": true,
                            "data": {
                                "valid": errors.is_empty(),
                                "errors": errors,
                                "warnings": warnings,
                                "node_count": dag.graph.node_count(),
                                "edge_count": dag.graph.edge_count()
                            }
                        })),
                    )
                }
                Err(e) => (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "success": true,
                        "data": {
                            "valid": false,
                            "errors": [format!("{}", e)],
                            "warnings": []
                        }
                    })),
                ),
            }
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
            })),
        ),
    }
}

async fn duplicate_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.workflows.get(&id) {
        Some(workflow) => {
            let new_id = Uuid::new_v4().to_string();
            let now = Utc::now();

            let new_workflow = WorkflowInfo {
                id: new_id.clone(),
                name: format!("{} (Copy)", workflow.name),
                description: workflow.description.clone(),
                node_count: workflow.node_count,
                edge_count: workflow.edge_count,
                created_at: now,
                updated_at: now,
                status: WorkflowStatus::Draft,
                spec: workflow.spec.clone(),
                tags: workflow.tags.clone(),
                version: 1,
            };

            state.workflows.insert(new_id, new_workflow.clone());

            let _ = state.event_tx.send(ServerEvent::WorkflowCreated {
                workflow: new_workflow.clone(),
            });

            info!(
                "Duplicated workflow '{}' as '{}'",
                workflow.name, new_workflow.name
            );

            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "success": true,
                    "data": new_workflow
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
            })),
        ),
    }
}

// ============================================================================
// API Handlers - Executions
// ============================================================================

async fn execute_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Json(req): Json<ExecuteWorkflowRequest>,
) -> impl IntoResponse {
    let workflow = match state.workflows.get(&workflow_id) {
        Some(w) => w.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "success": false,
                    "error": {"code": "NOT_FOUND", "message": "Workflow not found"}
                })),
            );
        }
    };

    // Check if workflow is active
    if workflow.status != WorkflowStatus::Active {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "success": false,
                "error": {
                    "code": "WORKFLOW_NOT_ACTIVE",
                    "message": "Workflow must be active to execute"
                }
            })),
        );
    }

    let execution_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    // Increment execution counter
    state.execution_counter.fetch_add(1, Ordering::SeqCst);

    let execution = ExecutionInfo {
        id: execution_id.clone(),
        workflow_id: workflow_id.clone(),
        workflow_name: workflow.name.clone(),
        status: ExecutionStatus::Running,
        started_at: now,
        finished_at: None,
        current_node: None,
        completed_nodes: vec![],
        failed_nodes: vec![],
        progress: 0.0,
        error: None,
        input: req.input.unwrap_or(serde_json::Value::Null),
        output: None,
        node_outputs: HashMap::new(),
        logs: vec![LogEntry {
            timestamp: now,
            level: LogLevel::Info,
            message: format!("Execution started for workflow '{}'", workflow.name),
            node_id: None,
            metadata: None,
        }],
        duration_ms: None,
    };

    state
        .executions
        .insert(execution_id.clone(), execution.clone());

    // Broadcast execution started
    let _ = state.event_tx.send(ServerEvent::ExecutionStarted {
        execution: execution.clone(),
    });

    info!(
        "Started execution {} for workflow '{}'",
        execution_id, workflow.name
    );

    // Spawn background task to simulate execution
    let exec_state = state.clone();
    let exec_id = execution_id.clone();
    let spec = workflow.spec.clone();

    tokio::spawn(async move {
        simulate_execution(exec_state, exec_id, spec).await;
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "success": true,
            "data": execution
        })),
    )
}

async fn list_executions(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(100);
    let page = query.page.unwrap_or(0);

    let mut executions: Vec<ExecutionInfo> =
        state.executions.iter().map(|r| r.value().clone()).collect();

    // Filter by status if provided
    if let Some(status_str) = &query.status {
        executions.retain(|e| match status_str.to_lowercase().as_str() {
            "running" => e.status == ExecutionStatus::Running,
            "completed" => e.status == ExecutionStatus::Completed,
            "failed" => e.status == ExecutionStatus::Failed,
            "cancelled" => e.status == ExecutionStatus::Cancelled,
            "pending" => e.status == ExecutionStatus::Pending,
            "paused" => e.status == ExecutionStatus::Paused,
            _ => true,
        });
    }

    // Sort by started_at descending by default
    let sort_order = query.sort_order.as_deref().unwrap_or("desc");
    executions.sort_by(|a, b| {
        if sort_order == "asc" {
            a.started_at.cmp(&b.started_at)
        } else {
            b.started_at.cmp(&a.started_at)
        }
    });

    let total = executions.len();
    let executions: Vec<ExecutionInfo> = executions
        .into_iter()
        .skip(page * limit)
        .take(limit)
        .collect();

    let has_more = (page + 1) * limit < total;

    Json(ApiResponse::success_with_meta(
        executions,
        ResponseMeta {
            total: Some(total),
            page: Some(page),
            limit: Some(limit),
            has_more: Some(has_more),
        },
    ))
}

async fn get_execution(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.executions.get(&id) {
        Some(execution) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "data": execution.clone()
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

async fn delete_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.executions.get(&id) {
        Some(exec) if exec.status == ExecutionStatus::Running => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "success": false,
                "error": {
                    "code": "EXECUTION_RUNNING",
                    "message": "Cannot delete a running execution. Cancel it first."
                }
            })),
        ),
        Some(_) => {
            state.executions.remove(&id);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": {"deleted": true, "id": id}
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

async fn cancel_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.executions.get_mut(&id) {
        Some(mut execution) => {
            if execution.status != ExecutionStatus::Running
                && execution.status != ExecutionStatus::Paused
            {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "success": false,
                        "error": {
                            "code": "INVALID_STATE",
                            "message": "Execution is not running or paused"
                        }
                    })),
                );
            }

            let now = Utc::now();
            execution.status = ExecutionStatus::Cancelled;
            execution.finished_at = Some(now);
            execution.duration_ms = Some((now - execution.started_at).num_milliseconds() as u64);
            execution.logs.push(LogEntry {
                timestamp: now,
                level: LogLevel::Warn,
                message: "Execution cancelled by user".to_string(),
                node_id: None,
                metadata: None,
            });

            let cancelled_exec = execution.clone();
            drop(execution);

            let _ = state.event_tx.send(ServerEvent::ExecutionCancelled {
                execution_id: id.clone(),
            });

            info!("Cancelled execution {}", id);

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": cancelled_exec
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

async fn pause_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.executions.get_mut(&id) {
        Some(mut execution) => {
            if execution.status != ExecutionStatus::Running {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "success": false,
                        "error": {
                            "code": "INVALID_STATE",
                            "message": "Execution is not running"
                        }
                    })),
                );
            }

            execution.status = ExecutionStatus::Paused;
            execution.logs.push(LogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                message: "Execution paused".to_string(),
                node_id: None,
                metadata: None,
            });

            let paused_exec = execution.clone();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": paused_exec
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

async fn resume_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.executions.get_mut(&id) {
        Some(mut execution) => {
            if execution.status != ExecutionStatus::Paused {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "success": false,
                        "error": {
                            "code": "INVALID_STATE",
                            "message": "Execution is not paused"
                        }
                    })),
                );
            }

            execution.status = ExecutionStatus::Running;
            execution.logs.push(LogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                message: "Execution resumed".to_string(),
                node_id: None,
                metadata: None,
            });

            let resumed_exec = execution.clone();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": resumed_exec
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

async fn get_execution_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    match state.executions.get(&id) {
        Some(execution) => {
            let limit = query.limit.unwrap_or(100).min(500);
            let page = query.page.unwrap_or(0);

            let total = execution.logs.len();
            let logs: Vec<LogEntry> = execution
                .logs
                .iter()
                .skip(page * limit)
                .take(limit)
                .cloned()
                .collect();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "data": logs,
                    "meta": {
                        "total": total,
                        "page": page,
                        "limit": limit,
                        "has_more": (page + 1) * limit < total
                    }
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": {"code": "NOT_FOUND", "message": "Execution not found"}
            })),
        ),
    }
}

// ============================================================================
// API Handlers - System
// ============================================================================

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": Utc::now().to_rfc3339(),
        "uptime_seconds": state.uptime_seconds()
    }))
}

async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let active_executions = state
        .executions
        .iter()
        .filter(|e| e.status == ExecutionStatus::Running)
        .count();

    let clients = *state.connected_clients.read().await;

    Json(ApiResponse::success(SystemMetrics {
        cpu_usage: 0.0, // Would need actual system metrics
        memory_usage: 0.0,
        active_executions,
        total_workflows: state.workflows.len(),
        connected_clients: clients,
        uptime_seconds: state.uptime_seconds(),
        total_executions: state.execution_counter.load(Ordering::SeqCst),
    }))
}

async fn get_stats(State(state): State<AppState>) -> impl IntoResponse {
    let total_executions = state.executions.len();
    let completed = state
        .executions
        .iter()
        .filter(|e| e.status == ExecutionStatus::Completed)
        .count();
    let failed = state
        .executions
        .iter()
        .filter(|e| e.status == ExecutionStatus::Failed)
        .count();
    let running = state
        .executions
        .iter()
        .filter(|e| e.status == ExecutionStatus::Running)
        .count();
    let cancelled = state
        .executions
        .iter()
        .filter(|e| e.status == ExecutionStatus::Cancelled)
        .count();

    let finished = completed + failed;
    let success_rate = if finished > 0 {
        (completed as f64 / finished as f64 * 100.0).round()
    } else {
        0.0
    };

    Json(ApiResponse::success(serde_json::json!({
        "workflows": {
            "total": state.workflows.len(),
            "active": state.workflows.iter().filter(|w| w.status == WorkflowStatus::Active).count(),
            "draft": state.workflows.iter().filter(|w| w.status == WorkflowStatus::Draft).count(),
            "paused": state.workflows.iter().filter(|w| w.status == WorkflowStatus::Paused).count(),
            "archived": state.workflows.iter().filter(|w| w.status == WorkflowStatus::Archived).count(),
        },
        "executions": {
            "total": total_executions,
            "completed": completed,
            "failed": failed,
            "running": running,
            "cancelled": cancelled,
            "success_rate": success_rate
        },
        "system": {
            "uptime_seconds": state.uptime_seconds(),
            "connected_clients": *state.connected_clients.read().await,
        }
    })))
}

// ============================================================================
// WebSocket Handler
// ============================================================================

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut event_rx = state.event_tx.subscribe();

    // Update connected clients count
    {
        let mut clients = state.connected_clients.write().await;
        *clients += 1;
        info!("WebSocket client connected. Total: {}", *clients);
    }

    // Send initial data
    let initial_data = serde_json::json!({
        "type": "initial_data",
        "workflows": state.workflows.iter().map(|r| r.value().clone()).collect::<Vec<_>>(),
        "executions": state.executions.iter().map(|r| r.value().clone()).collect::<Vec<_>>(),
        "timestamp": Utc::now().to_rfc3339()
    });

    if let Ok(msg) = serde_json::to_string(&initial_data) {
        if sender.send(Message::Text(msg)).await.is_err() {
            warn!("Failed to send initial data to WebSocket client");
        }
    }

    // Handle incoming messages and broadcast events
    loop {
        tokio::select! {
            // Handle messages from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Handle client commands
                        if let Ok(cmd) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(cmd_type) = cmd.get("type").and_then(|t| t.as_str()) {
                                match cmd_type {
                                    "ping" => {
                                        let _ = sender.send(Message::Text(
                                            serde_json::json!({
                                                "type": "pong",
                                                "timestamp": Utc::now().to_rfc3339()
                                            }).to_string()
                                        )).await;
                                    }
                                    "subscribe" => {
                                        let topic = cmd.get("topic").and_then(|t| t.as_str()).unwrap_or("all");
                                        info!("Client subscribed to topic: {}", topic);
                                        let _ = sender.send(Message::Text(
                                            serde_json::json!({
                                                "type": "subscribed",
                                                "topic": topic
                                            }).to_string()
                                        )).await;
                                    }
                                    "refresh" => {
                                        // Send fresh data
                                        let data = serde_json::json!({
                                            "type": "refresh_data",
                                            "workflows": state.workflows.iter().map(|r| r.value().clone()).collect::<Vec<_>>(),
                                            "executions": state.executions.iter().map(|r| r.value().clone()).collect::<Vec<_>>(),
                                            "timestamp": Utc::now().to_rfc3339()
                                        });
                                        if let Ok(msg) = serde_json::to_string(&data) {
                                            let _ = sender.send(Message::Text(msg)).await;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    _ => {}
                }
            }
            // Broadcast server events
            event = event_rx.recv() => {
                if let Ok(event) = event {
                    if let Ok(msg) = serde_json::to_string(&event) {
                        if sender.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    // Update connected clients count
    {
        let mut clients = state.connected_clients.write().await;
        *clients = clients.saturating_sub(1);
        info!("WebSocket client disconnected. Total: {}", *clients);
    }
}

// ============================================================================
// Background Tasks
// ============================================================================

async fn simulate_execution(state: AppState, execution_id: String, spec: serde_json::Value) {
    // Parse the spec to get nodes
    let nodes: Vec<String> = spec
        .get("nodes")
        .and_then(|n| n.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|n| n.get("id").and_then(|id| id.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let total_nodes = nodes.len().max(1);

    for (i, node_id) in nodes.iter().enumerate() {
        // Check if execution was cancelled or paused
        if let Some(exec) = state.executions.get(&execution_id) {
            match exec.status {
                ExecutionStatus::Cancelled => {
                    info!("Execution {} was cancelled", execution_id);
                    return;
                }
                ExecutionStatus::Paused => {
                    // Wait until resumed or cancelled
                    drop(exec);
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if let Some(exec) = state.executions.get(&execution_id) {
                            match exec.status {
                                ExecutionStatus::Running => break,
                                ExecutionStatus::Cancelled => return,
                                _ => continue,
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Update current node
        if let Some(mut exec) = state.executions.get_mut(&execution_id) {
            exec.current_node = Some(node_id.clone());
            exec.progress = ((i as f32 + 0.5) / total_nodes as f32) * 100.0;
            exec.logs.push(LogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                message: format!("Executing node: {}", node_id),
                node_id: Some(node_id.clone()),
                metadata: None,
            });
        }

        // Broadcast progress
        let _ = state.event_tx.send(ServerEvent::ExecutionProgress {
            execution_id: execution_id.clone(),
            progress: ((i as f32 + 0.5) / total_nodes as f32) * 100.0,
            current_node: Some(node_id.clone()),
        });

        // Simulate node execution time with proper random
        let exec_time = 500 + (fast_random() % 1000);
        tokio::time::sleep(Duration::from_millis(exec_time)).await;

        // Mark node as completed
        let output = serde_json::json!({
            "node_id": node_id,
            "result": "success",
            "data": {
                "processed": true,
                "timestamp": Utc::now().to_rfc3339(),
                "execution_time_ms": exec_time
            }
        });

        if let Some(mut exec) = state.executions.get_mut(&execution_id) {
            exec.completed_nodes.push(node_id.clone());
            exec.node_outputs.insert(node_id.clone(), output.clone());
            exec.progress = ((i + 1) as f32 / total_nodes as f32) * 100.0;
            exec.logs.push(LogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                message: format!("Node '{}' completed successfully", node_id),
                node_id: Some(node_id.clone()),
                metadata: Some(serde_json::json!({"execution_time_ms": exec_time})),
            });
        }

        let _ = state.event_tx.send(ServerEvent::ExecutionNodeCompleted {
            execution_id: execution_id.clone(),
            node_id: node_id.clone(),
            output,
        });

        // Broadcast log entry
        let _ = state.event_tx.send(ServerEvent::LogEntry {
            execution_id: execution_id.clone(),
            log: LogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                message: format!("Node '{}' completed", node_id),
                node_id: Some(node_id.clone()),
                metadata: None,
            },
        });
    }

    // Mark execution as completed
    if let Some(mut exec) = state.executions.get_mut(&execution_id) {
        let now = Utc::now();
        exec.status = ExecutionStatus::Completed;
        exec.finished_at = Some(now);
        exec.progress = 100.0;
        exec.current_node = None;
        let duration = (now - exec.started_at).num_milliseconds() as u64;
        exec.duration_ms = Some(duration);
        exec.output = Some(serde_json::json!({
            "success": true,
            "nodes_executed": exec.completed_nodes.len(),
            "duration_ms": duration
        }));
        exec.logs.push(LogEntry {
            timestamp: now,
            level: LogLevel::Info,
            message: format!("Execution completed successfully in {}ms", duration),
            node_id: None,
            metadata: None,
        });

        info!("Execution {} completed successfully", execution_id);

        let _ = state.event_tx.send(ServerEvent::ExecutionCompleted {
            execution: exec.clone(),
        });
    }
}

async fn broadcast_system_metrics(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let active_executions = state
            .executions
            .iter()
            .filter(|e| e.status == ExecutionStatus::Running)
            .count();

        let connected_clients = *state.connected_clients.read().await;

        let metrics = SystemMetrics {
            cpu_usage: (fast_random() % 50) as f32,
            memory_usage: 30.0 + (fast_random() % 40) as f32,
            active_executions,
            total_workflows: state.workflows.len(),
            connected_clients,
            uptime_seconds: state.uptime_seconds(),
            total_executions: state.execution_counter.load(Ordering::SeqCst),
        };

        let _ = state.event_tx.send(ServerEvent::SystemMetrics { metrics });
    }
}

async fn load_sample_workflows(state: &AppState) {
    // Sample workflow 1: Simple LLM Pipeline
    let sample1 = WorkflowInfo {
        id: "sample-llm-pipeline".to_string(),
        name: "Simple LLM Pipeline".to_string(),
        description: Some("A basic workflow demonstrating LLM call chaining with input processing and output formatting.".to_string()),
        node_count: 3,
        edge_count: 2,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        status: WorkflowStatus::Active,
        tags: vec!["llm".to_string(), "demo".to_string()],
        version: 1,
        spec: serde_json::json!({
            "name": "Simple LLM Pipeline",
            "nodes": [
                {"id": "input", "type": "data_processor", "name": "Input Handler"},
                {"id": "llm1", "type": "llm_call", "name": "Primary LLM", "model": "gpt-4"},
                {"id": "output", "type": "data_processor", "name": "Output Handler"}
            ],
            "connections": [
                {"from": "input", "to": "llm1"},
                {"from": "llm1", "to": "output"}
            ],
            "settings": {}
        }),
    };

    // Sample workflow 2: Multi-Agent System
    let sample2 = WorkflowInfo {
        id: "sample-multi-agent".to_string(),
        name: "Multi-Agent Ensemble".to_string(),
        description: Some("A complex workflow with multiple agents working in parallel, demonstrating ensemble coordination and result aggregation.".to_string()),
        node_count: 6,
        edge_count: 7,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        status: WorkflowStatus::Active,
        tags: vec!["agent".to_string(), "ensemble".to_string(), "parallel".to_string()],
        version: 1,
        spec: serde_json::json!({
            "name": "Multi-Agent Ensemble",
            "nodes": [
                {"id": "router", "type": "router", "name": "Task Router"},
                {"id": "agent1", "type": "agent", "name": "Research Agent"},
                {"id": "agent2", "type": "agent", "name": "Analysis Agent"},
                {"id": "agent3", "type": "agent", "name": "Writing Agent"},
                {"id": "ensemble", "type": "ensemble", "name": "Result Aggregator"},
                {"id": "output", "type": "data_processor", "name": "Final Output"}
            ],
            "connections": [
                {"from": "router", "to": "agent1"},
                {"from": "router", "to": "agent2"},
                {"from": "router", "to": "agent3"},
                {"from": "agent1", "to": "ensemble"},
                {"from": "agent2", "to": "ensemble"},
                {"from": "agent3", "to": "ensemble"},
                {"from": "ensemble", "to": "output"}
            ],
            "settings": {"concurrency": 3}
        }),
    };

    // Sample workflow 3: Data Processing Pipeline
    let sample3 = WorkflowInfo {
        id: "sample-data-pipeline".to_string(),
        name: "Data Processing Pipeline".to_string(),
        description: Some(
            "A data processing workflow with conditional branching and parallel processing stages."
                .to_string(),
        ),
        node_count: 5,
        edge_count: 5,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        status: WorkflowStatus::Active,
        tags: vec!["data".to_string(), "processing".to_string()],
        version: 1,
        spec: serde_json::json!({
            "name": "Data Processing Pipeline",
            "nodes": [
                {"id": "ingest", "type": "data_processor", "name": "Data Ingestion"},
                {"id": "validate", "type": "data_processor", "name": "Validation"},
                {"id": "transform", "type": "data_processor", "name": "Transform"},
                {"id": "enrich", "type": "llm_call", "name": "AI Enrichment"},
                {"id": "store", "type": "data_processor", "name": "Storage"}
            ],
            "connections": [
                {"from": "ingest", "to": "validate"},
                {"from": "validate", "to": "transform"},
                {"from": "transform", "to": "enrich"},
                {"from": "enrich", "to": "store"},
                {"from": "validate", "to": "store"}
            ],
            "settings": {"fail_fast": true}
        }),
    };

    state.workflows.insert(sample1.id.clone(), sample1);
    state.workflows.insert(sample2.id.clone(), sample2);
    state.workflows.insert(sample3.id.clone(), sample3);

    info!("Loaded {} sample workflows", state.workflows.len());
}

// Fast random number generator for simulation (not cryptographically secure)
fn fast_random() -> u64 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};

    thread_local! {
        static RNG_STATE: Cell<u64> = Cell::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        );
    }

    RNG_STATE.with(|state| {
        let mut s = state.get();
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        state.set(s);
        s.wrapping_mul(0x2545F4914F6CDD1D)
    })
}
