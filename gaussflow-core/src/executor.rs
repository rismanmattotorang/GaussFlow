//! Core execution logic for GaussFlow nodes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use std::fmt::Debug;
use std::collections::HashMap;

use crate::checkpoint::CheckpointStore;
use crate::dag::{DagEdge, DagNode, TypeSafeDag};
use crate::metrics::Metrics;
use crate::resource::ResourceManager;
use crate::scheduler::Scheduler;
use crate::validator::DagValidator;
use crate::model::{NodeSpec};
use tokio::sync::mpsc as tokio_mpsc;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Node execution failed: {0}")]
    NodeExecution(String),
    
    #[error("Resource allocation failed: {0}")]
    Resource(String),
    
    #[error("Timeout during execution: {0}")]
    Timeout(String),
    
    #[error("Retry limit exceeded: {0}")]
    RetryLimitExceeded(String),
    
    #[error("Stream processing error: {0}")]
    Stream(String),
    
    #[error("Batch processing error: {0}")]
    Batch(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for ExecutionError {
    fn from(error: std::io::Error) -> Self {
        ExecutionError::Internal(error.to_string())
    }
}

impl From<serde_json::Error> for ExecutionError {
    fn from(error: serde_json::Error) -> Self {
        ExecutionError::Internal(error.to_string())
    }
}

impl From<crate::dag::DagValidationError> for ExecutionError {
    fn from(err: crate::dag::DagValidationError) -> Self {
        ExecutionError::Internal(err.to_string())
    }
}

/// Trait for executing nodes in a workflow
pub trait NodeExecutor: Send + Sync + 'static {
    /// Returns the type of nodes this executor can handle
    fn kind(&self) -> &'static str;
    
    /// Get the resource requirements for a node
    fn resource_requirements(&self, node: &dyn std::any::Any) -> crate::resource::ResourceSpec;
    
    /// Returns the priority of this executor (higher is more important)
    fn priority(&self) -> u8;
    
    /// Returns the maximum number of concurrent executions allowed
    fn concurrency(&self) -> usize;
}

/// Trait for asynchronously executing nodes
pub trait AsyncNodeExecutor: Send + Sync + 'static {
    /// Execute a node with the given context
    fn execute(
        &self,
        node: Arc<dyn std::any::Any + Send + Sync>,
        input: &Value,
        context: &dyn std::any::Any,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, ExecutionError>> + Send + '_>>;
}

/// Trait for type-safe asynchronous node execution
#[async_trait]
pub trait TypedAsyncNodeExecutor<N, E>: Send + Sync + 'static
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    /// Executes a typed node with the given context
    async fn execute_typed(
        &self,
        node: Arc<N>,
        context: &mut (dyn std::any::Any + Send + Sync),
    ) -> Result<(), ExecutionError>;
}

/// Default implementation of NodeExecutor that does nothing
/// This can be used as a base for implementing custom executors
#[derive(Debug, Default)]
pub struct DefaultNodeExecutor;

impl NodeExecutor for DefaultNodeExecutor {
    fn kind(&self) -> &'static str {
        "default"
    }

    fn resource_requirements(&self, _node: &dyn std::any::Any) -> crate::resource::ResourceSpec {
        crate::resource::ResourceSpec::default()
    }

    fn priority(&self) -> u8 {
        50
    }

    fn concurrency(&self) -> usize {
        1
    }
}

impl AsyncNodeExecutor for DefaultNodeExecutor {
    fn execute(
        &self,
        _node: Arc<dyn std::any::Any + Send + Sync>,
        _input: &Value,
        _context: &dyn std::any::Any,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, ExecutionError>> + Send + '_>> {
        Box::pin(async move {
            // Default implementation returns a null value
            Ok(Value::Null)
        })
    }
}

#[async_trait::async_trait]
impl<N, E> TypedAsyncNodeExecutor<N, E> for DefaultNodeExecutor 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    async fn execute_typed(
        &self,
        _node: Arc<N>,
        _context: &mut (dyn std::any::Any + Send + Sync),
    ) -> Result<(), ExecutionError> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Middleware trait
pub trait Middleware<N, E>: Send + Sync + 'static 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    fn process_input(&self, input: Value, ctx: &dyn std::any::Any) -> Result<Value, ExecutionError>;
    fn process_output(&self, output: Value, ctx: &dyn std::any::Any) -> Result<Value, ExecutionError>;
    fn process_error(&self, error: ExecutionError, ctx: &dyn std::any::Any) -> Result<Value, ExecutionError>;
    
    // Policy hooks
    fn should_execute(&self, node: &N, ctx: &dyn std::any::Any) -> bool;
    fn modify_resources(&self, spec: &mut crate::resource::ResourceSpec, ctx: &dyn std::any::Any);
    fn modify_priority(&self, priority: &mut u8, ctx: &dyn std::any::Any);
}

pub trait PolicyHook<N, E>: Send + Sync + 'static 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    fn should_execute(&self, node: &N, ctx: &dyn std::any::Any) -> bool;
    fn modify_resources(&self, spec: &mut crate::resource::ResourceSpec, ctx: &dyn std::any::Any);
    fn modify_priority(&self, priority: &mut u8, ctx: &dyn std::any::Any);
}

pub struct ExecutionContext<N, E> 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    _phantom: std::marker::PhantomData<(N, E)>,
    pub node_id: String,
    pub workflow_id: String,
    pub execution_id: String,
    pub input: Value,
    pub metadata: HashMap<String, String>,
    pub resource_manager: Arc<ResourceManager>,
    pub scheduler: Arc<dyn Scheduler<Node = N, Edge = E> + Send + Sync>,
    pub metrics: Arc<Metrics>,
    pub checkpoint_store: Arc<dyn CheckpointStore>,
}

impl<N, E> ExecutionContext<N, E> 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    pub fn new(
        execution_id: String,
        input: Value,
        metadata: HashMap<String, String>,
        resource_manager: Arc<ResourceManager>,
        scheduler: Arc<dyn Scheduler<Node = N, Edge = E> + Send + Sync>,
        metrics: Arc<Metrics>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Self {
        let _phantom = std::marker::PhantomData;
        Self {
            _phantom,
            node_id: String::new(),
            workflow_id: String::new(),
            execution_id,
            input,
            metadata,
            resource_manager,
            scheduler,
            metrics,
            checkpoint_store,
        }
    }
}

pub struct ExecutionEngine<N, E> {
    concurrency: usize,
    node_executor: Arc<dyn AsyncNodeExecutor>,
    max_retries: u32,
    task_timeout: Duration,
    cancel_token: CancellationToken,
    metrics: Arc<Metrics>,
    task_queue_sender: tokio_mpsc::Sender<N>,
    task_queue_receiver: tokio_mpsc::Receiver<N>,
    running_tasks: Arc<Mutex<HashMap<String, CancellationToken>>>,
    resource_manager: Arc<ResourceManager>,
    validator: Arc<dyn DagValidator<Node = N, Edge = E>>,
    _phantom: std::marker::PhantomData<(N, E)>,
}

impl<N, E> ExecutionEngine<N, E> 
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    pub fn new(
        concurrency: usize,
        node_executor: Arc<dyn AsyncNodeExecutor>,
        max_retries: u32,
        task_timeout: Duration,
        resource_manager: Arc<ResourceManager>,
        metrics: Arc<Metrics>,
        validator: Arc<dyn DagValidator<Node = N, Edge = E>>,
    ) -> Self {
        let (task_queue_sender, task_queue_receiver) = tokio_mpsc::channel(100);
        
        Self {
            concurrency,
            node_executor,
            max_retries,
            task_timeout,
            resource_manager,
            metrics,
            validator,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            task_queue_sender,
            task_queue_receiver,
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
            _phantom: std::marker::PhantomData,
        }
    }
    
    fn visit_node(
        &self,
        node_id: String,
        dag: &TypeSafeDag<N, E>,
        visited: &mut HashSet<String>,
        order: &mut Vec<String>,
    ) -> Result<(), ExecutionError> {
        if visited.contains(&node_id) {
            return Ok(());
        }
        visited.insert(node_id.clone());
        
        if let Some(node) = dag.get_node(&node_id) {
            for dep in node.dependencies() {
                self.visit_node(dep.to_string(), dag, visited, order)?;
            }
        }
        
        order.push(node_id);
        Ok(())
    }
    
    pub async fn execute(
        &self,
        dag: TypeSafeDag<N, E>,
        input: Value,
    ) -> Result<HashMap<String, Value>, ExecutionError> {
        self.validator.validate(&dag)?;
        
        let order = self.determine_execution_order(&dag)?;
        let mut results: HashMap<String, Value> = HashMap::new();
        
        for node_id in order {
            if let Some(node) = dag.get_node(&node_id) {
                let mut ctx = ExecutionContext {
                    _phantom: std::marker::PhantomData,
                    node_id: node.id().to_string(),
                    workflow_id: "workflow_id".to_string(),
                    execution_id: uuid::Uuid::new_v4().to_string(),
                    input: Value::Null,
                    metadata: HashMap::new(),
                    resource_manager: self.resource_manager.clone(),
                    scheduler: Arc::new(crate::scheduler::PriorityScheduler::new(
                        crate::scheduler::SchedulerConfig::default(),
                        self.resource_manager.clone()
                    )),
                    metrics: self.metrics.clone(),
                    checkpoint_store: Arc::new(crate::checkpoint::FileCheckpointStore::new("checkpoints")),
                };
                
                let deps = node.dependencies();
                let mut inputs = HashMap::new();
                
                for dep in deps {
                    if let Some(dep_result) = results.get(&dep) {
                        inputs.insert(dep, dep_result.clone());
                    } else {
                        return Err(ExecutionError::NodeExecution(
                            format!("Dependency {} not found in results", dep)
                        ));
                    }
                }
                
                ctx.input = serde_json::to_value(inputs)
                    .map_err(|e| ExecutionError::NodeExecution(format!("Failed to serialize inputs: {}", e)))?;
                
                let result = self.execute_node(node, &ctx).await?;
                results.insert(node_id, result);
            }
        }
        
        Ok(results)
    }

    async fn execute_node(
        &self,
        node: &N,
        ctx: &ExecutionContext<N, E>,
    ) -> Result<Value, ExecutionError>
    {
        let resource_spec = node.resource_requirements();
        
        let _guard = self.resource_manager
            .acquire(&resource_spec, self.task_timeout)
            .await
            .map_err(|e| ExecutionError::Resource(e.to_string()))?;

        let result = tokio::time::timeout(
            self.task_timeout,
            self.node_executor.execute(Arc::new(node.clone()), &ctx.input, ctx)
        )
        .await
        .map_err(|_| ExecutionError::Timeout("Node execution timed out".to_string()))??;

        Ok(result)
    }

    fn determine_execution_order(
        &self,
        dag: &TypeSafeDag<N, E>,
    ) -> Result<Vec<String>, ExecutionError> {
        let mut visited = HashSet::new();
        let mut order = Vec::new();

        for node in dag.nodes() {
            if !visited.contains(node.id()) {
                self.visit_node(node.id().to_string(), dag, &mut visited, &mut order)?;
            }
        }

        order.reverse();
        Ok(order)
    }
}
