use crate::dag::{DagEdge, DagNode, TypeSafeDag};
use crate::resource::ResourceSpec;
use crate::NodeId;
use async_trait::async_trait;
use dashmap::DashMap;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub concurrency: u32,
    pub max_retries: u32,
    pub retry_backoff: RetryStrategy,
    pub priority_class: String,
    pub resource_limits: ResourceSpec,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            concurrency: 10,
            max_retries: 3,
            retry_backoff: RetryStrategy::Exponential,
            priority_class: "default".to_string(),
            resource_limits: ResourceSpec::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryStrategy {
    Exponential,
    Linear,
    Fixed { delay_ms: u64 },
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Task scheduling failed: {0}")]
    Scheduling(String),

    #[error("Resource allocation failed: {0}")]
    Resource(String),

    #[error("Priority calculation failed: {0}")]
    Priority(String),
}

#[async_trait]
pub trait Scheduler: Send + Sync + 'static {
    type Node: DagNode;
    type Edge: DagEdge;

    fn optimize(
        &self,
        dag: TypeSafeDag<Self::Node, Self::Edge>,
    ) -> TypeSafeDag<Self::Node, Self::Edge>;
    fn partition(
        &self,
        dag: TypeSafeDag<Self::Node, Self::Edge>,
    ) -> Vec<TypeSafeDag<Self::Node, Self::Edge>>;
    fn schedule_task(
        &mut self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<OwnedSemaphorePermit, SchedulerError>>
                + Send
                + '_,
        >,
    >;
    fn get_task_priority(
        &self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> Result<u8, SchedulerError>;
}

pub struct PriorityScheduler<N, E> {
    config: SchedulerConfig,
    resource_manager: Arc<crate::resource::ResourceManager>,
    task_queue: PriorityQueue<NodeId, u8>,
    task_status: DashMap<NodeId, TaskStatus>,
    semaphores: DashMap<NodeId, Arc<Semaphore>>,
    _phantom: std::marker::PhantomData<(N, E)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Waiting,
}

#[derive(Debug, Clone)]
pub struct TaskStatusDetails {
    pub started_at: std::time::Instant,
    pub priority: u8,
    pub resource_spec: ResourceSpec,
}

impl<N, E> PriorityScheduler<N, E> {
    pub fn new(
        config: SchedulerConfig,
        resource_manager: Arc<crate::resource::ResourceManager>,
    ) -> Self {
        Self {
            config,
            resource_manager,
            task_queue: PriorityQueue::new(),
            task_status: DashMap::new(),
            semaphores: DashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    fn calculate_priority(&self, _node_id: &NodeId, spec: &ResourceSpec) -> u8 {
        let base_priority = 5; // Default priority if not specified
        let resource_factor = (spec.cpu_cores as f32 * 0.5 + spec.gpu_count as f32 * 0.5) as u8;

        // Use memory_mb directly instead of parsing from string
        let memory_factor = (spec.memory_mb / 1024) as u8; // Convert MB to GB for factor

        // Combine factors with weights (normalize to 1-100 range)
        let priority = (base_priority as f32 * 0.4
            + resource_factor as f32 * 0.3
            + memory_factor as f32 * 0.3) as u8;

        priority.clamp(1, 100)
    }

    fn calculate_resource_score(&self, spec: &ResourceSpec) -> u32 {
        // Simple weighted sum of resources
        // Higher score means more resource-intensive
        (spec.cpu_cores * 100) + (spec.gpu_count * 500) + (spec.memory_mb as u32 * 10)
    }
}

impl<N, E> Scheduler for PriorityScheduler<N, E>
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
    NodeId: Hash + Eq + Clone,
{
    type Node = N;
    type Edge = E;

    fn optimize(
        &self,
        dag: TypeSafeDag<Self::Node, Self::Edge>,
    ) -> TypeSafeDag<Self::Node, Self::Edge> {
        // Implement DAG optimization logic here
        // For now, just return the original DAG
        dag
    }

    fn partition(&self, dag: TypeSafeDag<N, E>) -> Vec<TypeSafeDag<N, E>> {
        // Simple partitioning - just return the DAG as a single partition
        // In a real implementation, you might want to split the DAG into multiple partitions
        // based on resource requirements or other criteria
        vec![dag]
    }

    fn schedule_task(
        &mut self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<OwnedSemaphorePermit, SchedulerError>>
                + Send
                + '_,
        >,
    > {
        let node_id = *node_id;
        let spec = spec.clone();
        let priority = self.calculate_priority(&node_id, &spec);

        // Add task to queue if not already present
        if !self.task_queue.iter().any(|(id, _)| id == &node_id) {
            self.task_queue.push(node_id, priority);
        }

        // Get or create semaphore for this task
        let semaphore = self
            .semaphores
            .entry(node_id)
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone();

        let task_status = self.task_status.clone();

        Box::pin(async move {
            // Try to acquire semaphore permit
            match semaphore.try_acquire_owned() {
                Ok(permit) => {
                    task_status.insert(node_id, TaskStatus::Running);
                    Ok(permit)
                }
                Err(_) => {
                    task_status.insert(node_id, TaskStatus::Waiting);
                    Err(SchedulerError::Resource(
                        "Failed to acquire semaphore".into(),
                    ))
                }
            }
        })
    }

    fn get_task_priority(
        &self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> Result<u8, SchedulerError> {
        // Get the current priority from the queue if it exists
        if let Some((_, &priority)) = self.task_queue.get(node_id) {
            return Ok(priority);
        }

        // Otherwise calculate a new priority
        Ok(self.calculate_priority(node_id, spec))
    }
}

/// Default scheduler implementation that wraps PriorityScheduler with default configuration
pub struct DefaultScheduler<N, E> {
    inner: PriorityScheduler<N, E>,
}

impl<N, E> DefaultScheduler<N, E> {
    pub fn new() -> Self {
        let config = SchedulerConfig::default();
        let resource_manager = crate::resource::ResourceManager::new(ResourceSpec::default());
        Self {
            inner: PriorityScheduler::new(config, resource_manager),
        }
    }
}

impl<N, E> Default for DefaultScheduler<N, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N, E> Scheduler for DefaultScheduler<N, E>
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
    NodeId: Hash + Eq + Clone,
{
    type Node = N;
    type Edge = E;

    fn optimize(
        &self,
        dag: TypeSafeDag<Self::Node, Self::Edge>,
    ) -> TypeSafeDag<Self::Node, Self::Edge> {
        self.inner.optimize(dag)
    }

    fn partition(&self, dag: TypeSafeDag<N, E>) -> Vec<TypeSafeDag<N, E>> {
        self.inner.partition(dag)
    }

    fn schedule_task(
        &mut self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<OwnedSemaphorePermit, SchedulerError>>
                + Send
                + '_,
        >,
    > {
        self.inner.schedule_task(node_id, spec)
    }

    fn get_task_priority(
        &self,
        node_id: &NodeId,
        spec: &ResourceSpec,
    ) -> Result<u8, SchedulerError> {
        self.inner.get_task_priority(node_id, spec)
    }
}
