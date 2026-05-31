//! # GaussFlow Execution Engine
//!
//! This module is responsible for taking a validated `TypeSafeDag` and executing it.
//! It will contain the scheduler, task runners, and state management logic.
//!
//! This module implements the core execution engine for GaussFlow, including DAG execution,
//! task scheduling, and resource management.
// Standard library
use async_trait::async_trait;
use dashmap::DashMap;
use petgraph::prelude::NodeIndex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::dag::{DagNode, TypeSafeDag};
use crate::model::{EdgeSpec, NodeSpec};
use crate::resource::{ResourceManager, ResourceSpec};

/// Backoff strategy for retries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed { delay: Duration },

    /// Exponential backoff with jitter
    Exponential {
        /// Base of the exponential
        base: u32,
        /// Maximum delay
        max_delay: Option<Duration>,
    },

    /// Linear backoff
    Linear {
        /// Initial delay
        initial: Duration,
        /// Maximum delay
        max_delay: Option<Duration>,
    },
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::Exponential {
            base: 2,
            max_delay: Some(Duration::from_secs(60)),
        }
    }
}

/// Status of an execution attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AttemptStatus {
    /// The attempt is pending execution
    #[default]
    Pending,
    /// The attempt is currently running
    Running,
    /// The attempt completed successfully
    Succeeded,
    /// The attempt failed
    Failed,
    /// The attempt was cancelled
    Cancelled,
    /// The attempt timed out
    TimedOut,
}

/// Helper function to convert string to NodeIndex
fn str_to_node_index(s: &str) -> Option<NodeIndex> {
    s.parse::<usize>().ok().map(NodeIndex::new)
}

/// Error types that can occur during workflow execution
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// An error occurred during execution
    #[error("Execution error: {0}")]
    Execution(String),

    /// A validation error occurred
    #[error("Validation error: {0}")]
    Validation(String),

    /// A timeout occurred
    #[error("Timeout after {0:?}")]
    Timeout(Duration),

    /// A resource error occurred
    #[error("Resource error: {0}")]
    ResourceError(String),

    /// A serialization/deserialization error occurred
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A configuration error occurred
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// A node execution error occurred
    #[error("Node execution error: {0}")]
    NodeExecution(String),

    /// Insufficient resources available
    #[error("Insufficient resources: {message}")]
    InsufficientResources {
        message: String,
        available: Option<ResourceSpec>,
        requested: Option<ResourceSpec>,
    },

    /// The operation was cancelled
    #[error("Operation was cancelled")]
    Cancelled,

    /// An I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Dag(#[from] crate::DagError),
}

impl From<crate::resource::ResourceError> for ExecutionError {
    fn from(err: crate::resource::ResourceError) -> Self {
        ExecutionError::ResourceError(err.to_string())
    }
}

/// Metrics collected during workflow execution
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    pub nodes_completed: u64,
    pub nodes_failed: u64,
    pub nodes_skipped: u64,
    pub total_duration: Duration,
    pub resource_wait_time: Duration,
}

/// Context passed to each node during execution
#[derive(Debug, Clone, Default)]
pub struct ExecutionContext {
    /// The ID of the current execution
    pub execution_id: String,

    /// The ID of the current node being executed
    pub node_id: String,

    /// The attempt number (0-based) for this execution
    pub attempt: u32,

    /// The start time of the execution
    pub start_time: Option<Instant>,

    /// The timeout for the execution in seconds
    pub timeout: u64,

    /// The start time of this specific attempt
    pub attempt_start_time: Option<Instant>,

    /// The cancellation token for this execution
    pub cancel_token: CancellationToken,

    /// The maximum number of retries
    pub max_retries: u32,
    /// The backoff strategy for retries
    pub backoff_strategy: BackoffStrategy,
    /// The current attempt's timeout
    pub attempt_timeout: Duration,
    /// The current attempt's backoff
    pub attempt_backoff: Duration,
    /// The current attempt's error
    pub attempt_error: Option<String>,
    /// The current attempt's result
    pub attempt_result: Option<Value>,
    /// The current attempt's status
    pub attempt_status: AttemptStatus,
    /// The current attempt's metadata
    pub attempt_metadata: HashMap<String, Value>,
    /// The current attempt's metrics
    pub attempt_metrics: HashMap<String, Value>,
    /// The current attempt's logs
    pub attempt_logs: Vec<String>,
    /// The current attempt's start time
    pub attempt_duration: Duration,
    /// The current attempt's progress
    pub attempt_progress: f32,
    /// The current attempt's progress message
    pub attempt_progress_message: Option<String>,
    /// The current attempt's progress data
    pub attempt_progress_data: Option<Value>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with default values
    pub fn new(
        execution_id: String,
        node_id: String,
        cancel_token: CancellationToken,
        max_retries: u32,
        timeout: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            execution_id,
            node_id,
            attempt: 0,
            start_time: Some(now),
            timeout: timeout.as_secs(),
            attempt_start_time: Some(now),
            cancel_token,
            max_retries,
            backoff_strategy: BackoffStrategy::Exponential {
                base: 2,
                max_delay: Some(Duration::from_secs(60)),
            },
            attempt_timeout: timeout,
            attempt_backoff: Duration::default(),
            attempt_error: None,
            attempt_result: None,
            attempt_status: AttemptStatus::Pending,
            attempt_metadata: HashMap::new(),
            attempt_metrics: HashMap::new(),
            attempt_logs: Vec::new(),
            attempt_duration: Duration::default(),
            attempt_progress: 0.0,
            attempt_progress_message: None,
            attempt_progress_data: None,
        }
    }
}

/// Trait for executing workflow nodes
#[async_trait]
pub trait NodeExecutor: Send + Sync + 'static {
    /// Execute a node with the given input and context
    fn execute<'a>(
        &'a self,
        node: &'a NodeSpec,
        input: &'a Value,
        ctx: &'a ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<Value, ExecutionError>> + Send + 'a>>;

    /// Get the concurrency level for this executor
    fn concurrency(&self) -> usize {
        1
    }

    /// Get the resource requirements for a node
    fn resource_requirements(&self, _node: &NodeSpec) -> ResourceSpec {
        ResourceSpec::default()
    }
}

/// Default implementation of NodeExecutor
#[derive(Clone)]
pub struct DefaultNodeExecutor;

impl NodeExecutor for DefaultNodeExecutor {
    fn execute<'a>(
        &'a self,
        node: &'a NodeSpec,
        _input: &'a Value,
        _ctx: &'a ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<Value, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // In a real implementation, this would dispatch to specific executors
            // based on node_type
            match node.node_type {
                crate::model::NodeType::LlmCall => {
                    // Execute LLM call
                    // This is just a placeholder - in a real implementation, we would
                    // make an actual LLM API call
                    Ok(Value::String("llm_output".to_string()))
                }

                crate::model::NodeType::Agent => {
                    // Execute agent logic
                    // This is just a placeholder
                    Ok(Value::String("agent_output".to_string()))
                }

                crate::model::NodeType::Ensemble => {
                    // Execute ensemble logic
                    // This is just a placeholder
                    Ok(Value::String("ensemble_output".to_string()))
                }

                crate::model::NodeType::Router => {
                    // Execute routing logic
                    // This is just a placeholder
                    Ok(Value::String("routed_output".to_string()))
                }

                crate::model::NodeType::Subgraph => {
                    // Execute subgraph
                    // This is just a placeholder
                    Ok(Value::String("subgraph_output".to_string()))
                }

                _ => {
                    // Default implementation for other node types
                    Err(ExecutionError::NodeExecution(format!(
                        "Unsupported node type: {:?}",
                        node.node_type
                    )))
                }
            }
        })
    }
}

/// Task queue for node execution
/// A simple MPSC (multiple producer, single consumer) channel-based queue
#[derive(Debug)]
pub struct TaskQueue {
    sender: Sender<NodeSpec>,
    receiver: AsyncMutex<Receiver<NodeSpec>>,
}

impl TaskQueue {
    /// Create a new TaskQueue with the given buffer size
    pub fn new(buffer: usize) -> (Self, TaskQueueReceiver) {
        let (sender, receiver) = mpsc::channel(buffer);
        let task_queue = TaskQueue {
            sender: sender.clone(),
            receiver: AsyncMutex::new(receiver),
        };
        (task_queue, TaskQueueReceiver { sender })
    }

    #[tracing::instrument]
    pub async fn send(&self, node: NodeSpec) -> Result<(), SendError<NodeSpec>> {
        self.sender.send(node).await
    }

    #[tracing::instrument]
    pub async fn recv(&self) -> Option<NodeSpec> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
    }
}

/// Receiver part of the task queue
#[derive(Debug, Clone)]
pub struct TaskQueueReceiver {
    sender: Sender<NodeSpec>,
}

impl TaskQueueReceiver {
    /// Push a new task into the queue
    pub async fn send(&self, node: NodeSpec) -> Result<(), SendError<NodeSpec>> {
        self.sender.send(node).await
    }
}

/// A robust workflow execution engine that handles DAG execution with concurrency,
/// retries, resource management, and work stealing.
pub struct ExecutionEngine {
    concurrency: usize,
    node_executor: Arc<dyn NodeExecutor>,
    max_retries: u32,
    task_timeout: Duration,
    cancel_token: CancellationToken,
    metrics: Arc<ExecutionMetrics>,
    task_queue_sender: Arc<TaskQueue>,
    task_queue_receiver: std::sync::Mutex<TaskQueueReceiver>,
    running_tasks: Arc<DashMap<String, CancellationToken>>,
    resource_manager: Arc<ResourceManager>,
}

impl std::fmt::Debug for ExecutionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionEngine")
            .field("concurrency", &self.concurrency)
            .field("max_retries", &self.max_retries)
            .field("task_timeout", &self.task_timeout)
            .field("metrics", &self.metrics)
            .field("resource_manager", &self.resource_manager)
            .finish_non_exhaustive()
    }
}

impl Clone for ExecutionEngine {
    fn clone(&self) -> Self {
        // Create a new TaskQueue with the same buffer size as the original
        let buffer_size = self.task_queue_sender.sender.capacity();
        let (sender, receiver) = TaskQueue::new(buffer_size);

        Self {
            concurrency: self.concurrency,
            node_executor: self.node_executor.clone(),
            max_retries: self.max_retries,
            task_timeout: self.task_timeout,
            cancel_token: self.cancel_token.clone(),
            metrics: self.metrics.clone(),
            task_queue_sender: Arc::new(sender),
            task_queue_receiver: std::sync::Mutex::new(receiver),
            running_tasks: self.running_tasks.clone(),
            resource_manager: self.resource_manager.clone(),
        }
    }
}

/// RAII guard for resource management
pub struct ResourceGuard {
    cpu_millicores: u32,
    memory_mb: u64,
    gpu_guard: Option<tokio::sync::OwnedSemaphorePermit>,
    memory_guard: Option<tokio::sync::OwnedSemaphorePermit>,
    resource_manager: std::sync::Weak<ResourceManager>,
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        // Semaphore permits are released automatically when dropped
        // No manual cleanup needed for the new semaphore-based approach
    }
}

/// Manages the execution of tasks with resource constraints and cancellation support
#[derive(Debug)]
pub struct TaskManager {
    task_queue_sender: Arc<TaskQueue>,
    task_queue_receiver: std::sync::Mutex<TaskQueueReceiver>,
    running_tasks: Arc<DashMap<String, CancellationToken>>,
    resource_manager: Arc<ResourceManager>,
    metrics: Arc<std::sync::Mutex<ExecutionMetrics>>,
    cancel_token: CancellationToken,
}

impl Clone for TaskManager {
    fn clone(&self) -> Self {
        // Create a new task queue for the cloned instance
        let (sender, receiver) = TaskQueue::new(16); // Use a reasonable buffer size

        Self {
            task_queue_sender: Arc::new(sender),
            task_queue_receiver: std::sync::Mutex::new(receiver),
            running_tasks: Arc::clone(&self.running_tasks),
            resource_manager: Arc::clone(&self.resource_manager),
            metrics: Arc::clone(&self.metrics),
            cancel_token: self.cancel_token.clone(),
        }
    }
}

impl TaskManager {
    /// Create a new TaskManager with the given configuration
    pub fn new(concurrency: usize, cpu_limit: u32, gpu_limit: usize) -> Self {
        // Initialize the task queue with the specified concurrency level
        let (sender, receiver) = TaskQueue::new(concurrency);

        Self {
            task_queue_sender: Arc::new(sender),
            task_queue_receiver: std::sync::Mutex::new(receiver),
            running_tasks: Arc::new(DashMap::new()),
            resource_manager: ResourceManager::new(ResourceSpec {
                cpu_cores: cpu_limit,
                gpu_count: gpu_limit as u32,
                ..Default::default()
            }),
            metrics: Arc::new(std::sync::Mutex::new(ExecutionMetrics::default())),
            cancel_token: CancellationToken::new(),
        }
    }

    pub async fn submit_task<F, T, Fut>(
        &self,
        task_id: String,
        task: F,
        resources: ResourceSpec,
    ) -> Result<T, ExecutionError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ExecutionError>> + Send + 'static,
        T: Send + 'static,
    {
        // Check if task was already cancelled
        if self.cancel_token.is_cancelled() {
            return Err(ExecutionError::Cancelled);
        }

        // Acquire resources first
        let _resource_guard = self
            .resource_manager
            .acquire_resources("node", &resources)
            .await?;

        // Create a new cancellation token for this task
        let task_cancellation = self.cancel_token.child_token();

        // Create a task that will be cancelled when the manager is dropped
        let task_handle = {
            let task_cancellation = task_cancellation.clone();

            // Create a future that will be cancelled when the task is cancelled
            let task_future = async move {
                // Wait for the task to complete or be cancelled
                let result = tokio::select! {
                    _ = task_cancellation.cancelled() => {
                        return Err(ExecutionError::Cancelled);
                    }
                    result = task() => result,
                };

                result
            };

            // Spawn the task
            tokio::spawn(task_future)
        };

        // Store the task handle and cancellation token
        self.running_tasks
            .insert(task_id.clone(), task_cancellation);

        // Wait for the task to complete
        let result = match task_handle.await {
            Ok(Ok(result)) => {
                // Update metrics on success
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.nodes_completed += 1;
                }
                Ok(result)
            }
            Ok(Err(e)) => {
                // Update metrics on error
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.nodes_failed += 1;
                }
                Err(e)
            }
            Err(join_err) => {
                // If the task was aborted, return Cancelled error
                if join_err.is_cancelled() {
                    if let Ok(mut metrics) = self.metrics.lock() {
                        metrics.nodes_failed += 1;
                    }
                    Err(ExecutionError::Cancelled)
                } else {
                    if let Ok(mut metrics) = self.metrics.lock() {
                        metrics.nodes_failed += 1;
                    }
                    Err(ExecutionError::NodeExecution(format!(
                        "Task panicked: {}",
                        join_err
                    )))
                }
            }
        };

        // Remove the task from the map
        self.running_tasks.remove(&task_id);

        // The resource guard will be dropped here, releasing the resources
        result
    }

    /// Cancel a running task
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to cancel
    ///
    /// # Returns
    /// `true` if the task was found and cancelled, `false` otherwise
    pub fn cancel_task(&self, task_id: &str) -> bool {
        if let Some((_, token)) = self.running_tasks.remove(task_id) {
            token.cancel();

            // Update metrics
            if let Ok(mut metrics) = self.metrics.lock() {
                metrics.nodes_failed += 1;
            }
            true
        } else {
            false
        }
    }

    /// Cancel all running tasks
    pub fn cancel_all_tasks(&mut self) {
        // Cancel all running tasks
        for entry in self.running_tasks.iter() {
            entry.value().cancel();
        }

        // Clear the running tasks map
        self.running_tasks.clear();

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.nodes_failed += 1;
        }
    }

    /// Get current execution metrics
    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics.lock().unwrap().clone()
    }
}

impl ExecutionEngine {
    /// Creates a new ExecutionEngine with the specified concurrency level and retry policy.
    ///
    /// # Arguments
    /// * `concurrency` - Maximum number of parallel node executions (>=1)
    /// * `node_executor` - The executor to use for node execution
    /// * `max_retries` - Maximum number of retries for failed nodes
    /// * `task_timeout` - Maximum time to wait for a single node to complete
    pub fn new(
        concurrency: usize,
        node_executor: Arc<dyn NodeExecutor>,
        max_retries: u32,
        task_timeout: Duration,
    ) -> Self {
        assert!(concurrency > 0, "Concurrency must be at least 1");

        // Initialize resource manager with system CPU count
        let num_cpus = num_cpus::get() as u32 * 1000; // Convert to millicores

        let (sender, receiver) = TaskQueue::new(concurrency);

        Self {
            concurrency,
            node_executor,
            max_retries,
            task_timeout,
            cancel_token: CancellationToken::new(),
            metrics: Arc::new(ExecutionMetrics::default()),
            task_queue_sender: Arc::new(sender),
            task_queue_receiver: std::sync::Mutex::new(receiver),
            running_tasks: Arc::new(DashMap::new()),
            resource_manager: ResourceManager::new(ResourceSpec {
                cpu_cores: num_cpus,
                gpu_count: 1,
                ..Default::default()
            }),
        }
    }

    /// Cancel the current execution
    pub fn cancel(&self) {
        self.cancel_token.cancel();
        // Also cancel all running tasks
        for entry in self.running_tasks.iter() {
            entry.value().cancel();
        }
    }

    pub async fn check_resources(&self, resources: &ResourceSpec) -> Result<(), ExecutionError> {
        // For now, just check if we can acquire the resources
        // The actual acquisition will happen in submit_task
        let _guard = self
            .resource_manager
            .acquire_resources("node", resources)
            .await?;
        Ok(())
    }

    /// Calculate the total resources required for the DAG
    pub fn calculate_required_resources(
        &self,
        dag: &TypeSafeDag<NodeSpec, EdgeSpec>,
    ) -> Result<ResourceSpec, ExecutionError> {
        let mut any_gpu = false;
        let mut max_priority = 0;
        let mut max_timeout = 0;
        let mut total_memory_mb = 0;
        let mut total_cpu_millicores = 0;

        // Check if any node requires GPU and find max priority/timeout
        for node in dag.graph.node_weights() {
            if let Some(resources) = &node.resources {
                any_gpu = any_gpu || resources.gpu_count > 0;

                // Priority is not optional in ResourceSpec
                max_priority = max_priority.max(resources.priority);

                // Timeout is not optional in ResourceSpec
                max_timeout = max_timeout.max(resources.timeout_ms);

                // Memory is not optional in ResourceSpec
                total_memory_mb += resources.memory_mb;

                // CPU millicores is not optional in ResourceSpec
                total_cpu_millicores += resources.cpu_millicores;
            }
        }

        Ok(ResourceSpec {
            cpu_cores: (total_cpu_millicores / 1000) as u32,
            memory_mb: total_memory_mb,
            gpu_count: if any_gpu { 1 } else { 0 },
            memory: format!("{}MB", total_memory_mb),
            concurrency: 1,
            timeout_ms: if max_timeout > 0 { max_timeout } else { 30000 },
            priority: if max_priority > 0 { max_priority } else { 5 },
            labels: HashMap::new(),
            remote: false,
            required_executor: None,
            cpu_millicores: total_cpu_millicores,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            affinity: None,
        })
    }

    /// Get execution metrics
    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics.as_ref().clone()
    }

    pub async fn execute(
        &self,
        dag: crate::dag::TypeSafeDag<NodeSpec, EdgeSpec>,
        input: Value,
    ) -> Result<std::collections::HashMap<String, Value>, ExecutionError> {
        let order = dag.graph.node_indices().collect::<Vec<_>>();
        let results: Arc<tokio::sync::Mutex<std::collections::HashMap<String, Value>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

        // Initialize with input
        results.lock().await.insert("_input".into(), input);

        // Create a channel for task results
        let (tx, mut rx) = tokio::sync::mpsc::channel(order.len());

        // Send all tasks to the queue
        for node_idx in order {
            let node = dag.graph.node_weight(node_idx).unwrap();
            let node_id = node.id().to_string();
            info!(%node_id, "Queueing node for execution");

            // Check resource requirements
            let default_resources = ResourceSpec::default();
            let resources = node.resources.as_ref().unwrap_or(&default_resources);
            self.check_resources(resources).await?;

            // Send task to the task queue
            if let Err(e) = self.task_queue_sender.send(node.clone()).await {
                error!(%node_id, error = %e, "Failed to send task to queue");
                return Err(ExecutionError::Execution(format!(
                    "Failed to queue task: {}",
                    e
                )));
            }
        }

        // Create worker tasks
        let mut handles = vec![];
        for _ in 0..self.concurrency {
            let node_executor = self.node_executor.clone();
            let cancel_token = self.cancel_token.child_token();
            let _metrics = self.metrics.clone();
            let worker_tx = tx.clone();
            let worker_results = results.clone();
            let worker_task_queue = self.task_queue_sender.clone();

            let handle = tokio::spawn(async move {
                loop {
                    // Check for cancellation
                    if cancel_token.is_cancelled() {
                        break;
                    }

                    // Get the next node to process from the shared queue
                    let node = match worker_task_queue.recv().await {
                        Some(node) => node,
                        None => break, // Channel is closed, exit the loop
                    };

                    let node_id = node.id().to_string();

                    // Execute the node
                    let input = serde_json::Value::Object(serde_json::Map::new());
                    let result = node_executor
                        .execute(&node, &input, &Default::default())
                        .await;

                    // Handle the result
                    match result {
                        Ok(output) => {
                            worker_results.lock().await.insert(node_id, output);
                            if let Err(e) = worker_tx.send(Ok(())).await {
                                error!("Failed to send task completion: {}", e);
                            }
                        }
                        Err(e) => {
                            error!(%node_id, error = %e, "Node execution failed");
                            if let Err(e) = worker_tx.send(Err(e)).await {
                                error!("Failed to send task failure: {}", e);
                            }
                            break;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete or fail
        drop(tx); // Close the sender so the receiver can complete

        // Wait for all workers to finish
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Worker task panicked: {}", e);
            }
        }

        // Check for any errors
        let mut errors = vec![];
        while let Some(result) = rx.recv().await {
            if let Err(e) = result {
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(ExecutionError::Execution(format!(
                "{} nodes failed to execute. First error: {}",
                errors.len(),
                errors[0]
            )));
        }

        // Return the final results
        let results = Arc::try_unwrap(results)
            .map_err(|_| ExecutionError::Execution("Failed to get results".into()))?
            .into_inner();

        Ok(results)
    }

    #[tracing::instrument]
    async fn execute_with_retry(
        &self,
        node: &NodeSpec,
        context: &HashMap<String, Value>,
    ) -> Result<Value, ExecutionError> {
        let mut attempt = 0;
        let execution_id = Uuid::new_v4().to_string();

        loop {
            match self
                .execute_node(node, context, &execution_id, attempt)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let is_retryable = Self::is_retryable_error(&e);
                    error!(
                        "Error executing node {} after {} attempts: {}",
                        node.id,
                        attempt + 1,
                        e
                    );

                    if !is_retryable || attempt >= self.max_retries {
                        return Err(e);
                    }

                    // Calculate backoff
                    let backoff = match node.retry.as_ref() {
                        Some(retry) => match retry.backoff {
                            crate::model::Backoff::Exponential => {
                                Duration::from_secs(2u64.pow(attempt))
                            }
                            crate::model::Backoff::Fixed => Duration::from_secs(1),
                            crate::model::Backoff::Linear => Duration::from_secs(attempt as u64),
                        },
                        None => Duration::from_secs(1),
                    };

                    info!(
                        "Retrying node {} in {:?} (attempt {}/{})",
                        node.id,
                        backoff,
                        attempt + 1,
                        self.max_retries
                    );

                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Determines if an error is retryable
    fn is_retryable_error(error: &ExecutionError) -> bool {
        // Consider all errors retryable except Validation and ResourceUnavailable errors
        !matches!(
            error,
            ExecutionError::Validation(_) | ExecutionError::ResourceError(_)
        )
    }

    #[tracing::instrument]
    /// Execute a single node with the given context and execution metrics
    async fn execute_node(
        &self,
        node: &NodeSpec,
        context: &HashMap<String, Value>,
        execution_id: &str,
        attempt: u32,
    ) -> Result<Value, ExecutionError> {
        // Generate a unique ID for this task
        let task_id = format!("{}:{}", node.id, Uuid::new_v4());

        // Create a cancellation token for this task
        let cancel_token = CancellationToken::new();
        self.running_tasks
            .insert(task_id.clone(), cancel_token.clone());

        // Wrap the actual execution in a cancellation scope
        let result = tokio::select! {
            _ = cancel_token.cancelled() => {
                Err(ExecutionError::Cancelled)
            }
            result = async {
                // Acquire resources
                let _resource_guard = if let Some(resources) = &node.resources {
                    Some(self.resource_manager.acquire_resources("node", resources).await?)
                } else {
                    None
                };

                // Execute the node
                self.execute_node_inner(node, context, execution_id, attempt).await
            } => {
                result
            }
        };

        // Clean up
        self.running_tasks.remove(&task_id);
        result
    }

    #[tracing::instrument]
    /// Inner execution logic without cancellation handling
    async fn execute_node_inner(
        &self,
        node: &NodeSpec,
        context: &HashMap<String, Value>,
        execution_id: &str,
        _attempt: u32,
    ) -> Result<Value, ExecutionError> {
        // Check for cancellation before starting
        if self.cancel_token.is_cancelled() {
            return Err(ExecutionError::Cancelled);
        }

        let _start_time = Instant::now();
        let node_id = node.id.clone();

        let execution_ctx = ExecutionContext::new(
            execution_id.to_string(),
            node_id.clone(),
            self.cancel_token.child_token(),
            self.max_retries,
            self.task_timeout,
        );

        // Check if we have enough resources
        // For now, just check node resources since we don't have the full DAG here
        if let Some(resources) = &node.resources {
            self.check_resources(resources).await?;
        }

        // Prepare input from context
        let input = self.prepare_dependent_input(node, context).await?;

        // Execute the node
        self.node_executor
            .execute(node, &input, &execution_ctx)
            .await
    }

    pub async fn execute_workflow(
        &self,
        dag: crate::dag::TypeSafeDag<NodeSpec, EdgeSpec>,
        input: Value,
    ) -> Result<HashMap<String, Value>, ExecutionError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let mut results = HashMap::new();
        let mut pending = HashSet::new();
        let mut completed = HashSet::new();
        let mut tasks = HashMap::new();
        let total_nodes = dag.graph.node_count();
        let start_time = Instant::now();
        let execution_id = Uuid::new_v4().to_string();

        // Start with source nodes (nodes with no dependencies)
        for node_idx in dag.graph.node_indices() {
            let node = dag.graph.node_weight(node_idx).unwrap();
            if self.is_source_node(&dag, node.id()) {
                let node_id = node.id().to_string();
                let input = input.clone();
                let tx = tx.clone();
                let node_clone = node.clone();
                let executor = self.node_executor.clone();

                // Clone values that will be used in the spawned task
                let node_id_for_task = node_id.clone();
                let tx_for_task = tx.clone();
                let execution_id_clone = execution_id.clone();

                let task_handle = tokio::spawn(async move {
                    let result = async {
                        let ctx = ExecutionContext::new(
                            execution_id_clone,
                            node_id_for_task.clone(),
                            CancellationToken::new(),
                            3,                        // max_retries
                            Duration::from_secs(300), // 5 minutes timeout
                        );
                        executor.execute(&node_clone, &input, &ctx).await
                    }
                    .await;

                    match result {
                        Ok(output) => {
                            let _ = tx_for_task.send((node_id_for_task, Ok(output))).await;
                        }
                        Err(e) => {
                            let _ = tx_for_task.send((node_id_for_task, Err(e))).await;
                        }
                    }
                });
                tasks.insert(node.id().to_string(), task_handle);
                pending.insert(node.id().to_string());
            }
        }

        while completed.len() < total_nodes && !pending.is_empty() {
            // Check for timeout
            if start_time.elapsed() > self.task_timeout {
                // Cancel all running tasks
                for (_, handle) in tasks.drain() {
                    handle.abort();
                }
                return Err(ExecutionError::Timeout(self.task_timeout));
            }

            // Wait for any task to complete
            match rx.recv().await {
                Some((node_id, result)) => {
                    let result = result?;
                    completed.insert(node_id.clone());
                    pending.remove(&node_id);
                    results.insert(node_id, result);

                    // Schedule any tasks that now have their dependencies satisfied
                    for node_idx in dag.graph.node_indices() {
                        let node = dag.graph.node_weight(node_idx).unwrap();
                        let node_id = node.id().to_string();
                        if !completed.contains(&node_id)
                            && !pending.contains(&node_id)
                            && self.are_dependencies_satisfied(&dag, node, &completed)
                        {
                            let input = self.prepare_dependent_input(node, &results).await?;
                            let tx = tx.clone();
                            let node_clone = node.clone();
                            let executor = self.node_executor.clone();

                            // Clone values that will be used in the spawned task
                            let node_id_for_task = node_id.clone();
                            let tx_for_task = tx.clone();
                            let execution_id_clone = execution_id.clone();

                            let task_handle = tokio::spawn(async move {
                                let result = async {
                                    let ctx = ExecutionContext::new(
                                        execution_id_clone,
                                        node_id_for_task.clone(),
                                        CancellationToken::new(),
                                        3,                        // max_retries
                                        Duration::from_secs(300), // 5 minutes timeout
                                    );
                                    executor.execute(&node_clone, &input, &ctx).await
                                }
                                .await;

                                match result {
                                    Ok(output) => {
                                        let _ =
                                            tx_for_task.send((node_id_for_task, Ok(output))).await;
                                    }
                                    Err(e) => {
                                        let _ = tx_for_task.send((node_id_for_task, Err(e))).await;
                                    }
                                }
                            });
                            tasks.insert(node.id().to_string(), task_handle);
                            pending.insert(node.id().to_string());
                        }
                    }
                }
                None => {
                    // All senders have been dropped, which means all tasks have completed
                    // or failed. We can break out of the loop.
                    break;
                }
            }
        }

        if completed.len() < total_nodes {
            return Err(ExecutionError::NodeExecution(
                "Not all nodes were executed".into(),
            ));
        }

        Ok(results)
    }

    #[tracing::instrument]
    /// Prepare input for a dependent node
    async fn prepare_dependent_input(
        &self,
        node: &NodeSpec,
        results: &HashMap<String, Value>,
    ) -> Result<Value, ExecutionError> {
        let mut input = serde_json::Map::new();

        // Add all parent node outputs
        for (node_id, output) in results {
            input.insert(node_id.clone(), output.clone());
        }

        // Add any additional node-specific parameters from config
        if let crate::model::NodeConfig::Agent { config, .. } = &node.config {
            if let Some(params) = config.get("parameters") {
                if let Some(params_map) = params.as_object() {
                    input.extend(params_map.clone().into_iter());
                }
            }
        }

        Ok(serde_json::Value::Object(input))
    }

    /// Check if a node is a source node (has no dependencies)
    fn is_source_node(&self, dag: &TypeSafeDag<NodeSpec, EdgeSpec>, node_id: &str) -> bool {
        if let Some(idx) = str_to_node_index(node_id) {
            dag.graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .count()
                == 0
        } else {
            false
        }
    }

    /// Get all nodes that depend on the given node
    fn dependents(&self, dag: &TypeSafeDag<NodeSpec, EdgeSpec>, node_id: &str) -> Vec<NodeSpec> {
        str_to_node_index(node_id).map_or_else(Vec::new, |idx| {
            dag.graph
                .neighbors_directed(idx, petgraph::Direction::Outgoing)
                .filter_map(|node_idx| dag.graph.node_weight(node_idx).cloned())
                .collect()
        })
    }

    /// Check if all dependencies for a node are satisfied
    fn are_dependencies_satisfied(
        &self,
        dag: &TypeSafeDag<NodeSpec, EdgeSpec>,
        node: &NodeSpec,
        completed: &HashSet<String>,
    ) -> bool {
        if let Some(idx) = str_to_node_index(&node.id) {
            dag.graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .all(|dep_id| {
                    let dep_id_str = dep_id.index().to_string();
                    completed.contains(&dep_id_str)
                })
        } else {
            false
        }
    }

    /// Get the total number of nodes in the DAG
    fn node_count(&self, dag: &TypeSafeDag<NodeSpec, EdgeSpec>) -> usize {
        dag.graph.node_count()
    }
}
