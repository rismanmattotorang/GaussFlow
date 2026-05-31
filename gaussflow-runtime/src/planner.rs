use gaussflow_core::ResourceSpec;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use async_trait::async_trait;
use dashmap::DashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, warn};

/// Tracks available resources and manages task scheduling
#[derive(Debug, Clone)]
pub struct Planner {
    cpu_sem: Arc<Semaphore>,
    gpu_sem: Arc<Semaphore>,
    remote_sem: Arc<Semaphore>,
    metrics: Arc<ResourceMetrics>,
    remote_executors: Arc<DashMap<String, RemoteExecutorConfig>>,
    node_priorities: Arc<DashMap<String, u8>>, // Node ID -> priority (higher is more important)
}

/// Configuration for a remote executor
#[derive(Debug, Clone)]
pub struct RemoteExecutorConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub max_concurrent: usize,
    pub current_usage: usize,
    pub timeout_ms: u64,
}

/// Metrics about resource utilization
#[derive(Debug, Default)]
pub struct ResourceMetrics {
    cpu_available: AtomicUsize,
    gpu_available: AtomicUsize,
    remote_available: AtomicUsize,
    total_cpu_permits: AtomicUsize,
    total_gpu_permits: AtomicUsize,
    total_remote_permits: AtomicUsize,
}

/// Represents an acquired resource permit that will be released when dropped
pub struct ResourcePermit {
    _permit: Option<OwnedSemaphorePermit>,
    resource_type: ResourceType,
    metrics: Arc<ResourceMetrics>,
    _marker: std::marker::PhantomData<()>,
}

/// Type of resource being managed
#[derive(Debug, Clone, Copy)]
pub enum ResourceType {
    Cpu,
    Gpu,
    Remote,
}

impl Default for Planner {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ResourceMetrics {
    fn update_metrics(&self, resource_type: ResourceType, available: usize, total: usize) {
        match resource_type {
            ResourceType::Cpu => {
                self.cpu_available.store(available, Ordering::Relaxed);
                self.total_cpu_permits.store(total, Ordering::Relaxed);
            }
            ResourceType::Gpu => {
                self.gpu_available.store(available, Ordering::Relaxed);
                self.total_gpu_permits.store(total, Ordering::Relaxed);
            }
            ResourceType::Remote => {
                self.remote_available.store(available, Ordering::Relaxed);
                self.total_remote_permits.store(total, Ordering::Relaxed);
            }
        }
    }

    pub fn get_metrics(&self) -> ResourceStats {
        ResourceStats {
            cpu_available: self.cpu_available.load(Ordering::Relaxed),
            gpu_available: self.gpu_available.load(Ordering::Relaxed),
            remote_available: self.remote_available.load(Ordering::Relaxed),
            total_cpu_permits: self.total_cpu_permits.load(Ordering::Relaxed),
            total_gpu_permits: self.total_gpu_permits.load(Ordering::Relaxed),
            total_remote_permits: self.total_remote_permits.load(Ordering::Relaxed),
        }
    }
}

/// Detailed resource statistics
#[derive(Debug, Clone, Copy)]
pub struct ResourceStats {
    pub cpu_available: usize,
    pub gpu_available: usize,
    pub remote_available: usize,
    pub total_cpu_permits: usize,
    pub total_gpu_permits: usize,
    pub total_remote_permits: usize,
}

impl Planner {
    /// Create a new planner with optional concurrency overrides
    pub fn new(concurrency: Option<usize>) -> Self {
        let cpu = concurrency.unwrap_or_else(num_cpus::get);
        let metrics = Arc::new(ResourceMetrics::default());

        let planner = Self {
            cpu_sem: Arc::new(Semaphore::new(cpu)),
            gpu_sem: Arc::new(Semaphore::new(1)), // Single GPU by default
            remote_sem: Arc::new(Semaphore::new(10)), // Limit concurrent remote calls
            metrics: metrics.clone(),
            remote_executors: Arc::new(DashMap::new()),
            node_priorities: Arc::new(DashMap::new()),
        };

        // Initialize metrics
        metrics.update_metrics(ResourceType::Cpu, cpu, cpu);
        metrics.update_metrics(ResourceType::Gpu, 1, 1);
        metrics.update_metrics(ResourceType::Remote, 10, 10);

        planner
    }

    /// Add a remote executor configuration
    pub fn add_remote_executor(&self, id: String, config: RemoteExecutorConfig) {
        self.remote_executors.insert(id, config);
    }

    /// Set priority for a node
    pub fn set_node_priority(&self, node_id: &str, priority: u8) {
        self.node_priorities.insert(node_id.to_string(), priority);
    }

    /// Get priority for a node (defaults to 0)
    pub fn get_node_priority(&self, node_id: &str) -> u8 {
        self.node_priorities.get(node_id).map(|p| *p).unwrap_or(0)
    }

    /// Check if a node should be executed remotely
    fn should_execute_remotely(&self, spec: &ResourceSpec) -> bool {
        // Check if the node explicitly requests remote execution
        if spec.remote {
            return true;
        }

        // Check if node requires a specific executor that's available remotely
        if let Some(executor_id) = &spec.required_executor {
            return self.remote_executors.contains_key(executor_id);
        }

        // Default to local execution if no remote executors are available
        !self.remote_executors.is_empty() && !spec.remote
    }

    /// Get the best available remote executor for a node
    #[allow(dead_code)] // scaffolding retained for a later phase (scheduler/executor/policy/planner wiring)
    fn get_best_executor(&self, spec: &ResourceSpec) -> Option<(String, RemoteExecutorConfig)> {
        // If a specific executor is required, return it
        if let Some(executor_id) = &spec.required_executor {
            if self.remote_executors.contains_key(executor_id) {
                if let Some(executor) = self.remote_executors.get(executor_id) {
                    return Some((executor_id.clone(), executor.value().clone()));
                }
            }
            return None;
        }

        // Otherwise, find the least loaded executor
        self.remote_executors
            .iter()
            .min_by_key(|e| e.value().max_concurrent - e.value().current_usage)
            .map(|e| (e.key().clone(), e.value().clone()))
    }

    /// Acquire resources for a node, respecting timeouts and resource requirements
    pub async fn acquire_resources(
        &self,
        node_id: &str,
        spec: &ResourceSpec,
    ) -> Result<ResourcePermit, Box<dyn std::error::Error + Send + Sync>> {
        let _timeout_duration = Duration::from_secs(30); // 30 second timeout
        let (sem, resource_type) = if spec.gpu_count > 0 {
            (self.gpu_sem.clone(), ResourceType::Gpu)
        } else if self.should_execute_remotely(spec) {
            (self.remote_sem.clone(), ResourceType::Remote)
        } else {
            (self.cpu_sem.clone(), ResourceType::Cpu)
        };

        // Calculate timeout, defaulting to 30 seconds if not specified
        let timeout_duration = Duration::from_millis(spec.timeout_ms);

        info!(
            "Acquiring {:?} resources for node {}",
            resource_type, node_id
        );

        // Get available permits before acquiring
        let available = sem.available_permits();
        // Semaphore doesn't have a capacity() method, using available permits as total
        let total = available;

        // Update metrics
        self.metrics.update_metrics(resource_type, available, total);

        // Get priority for this node (higher is more important)
        let _priority = self.get_node_priority(node_id);

        // Clone semaphore for the acquire_owned call
        let sem_clone = sem.clone();

        // Acquire the semaphore with timeout
        let permit = match timeout(timeout_duration, sem_clone.acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err("Failed to acquire resource permit".into()),
            Err(_) => {
                let msg = format!(
                    "Timeout waiting for {:?} resources for node {}",
                    resource_type, node_id
                );
                warn!("{}", msg);
                return Err(msg.into());
            }
        };

        // Update metrics with the current state of the semaphore
        let available = sem.available_permits();
        let total = match resource_type {
            ResourceType::Cpu => self.cpu_sem.available_permits() + 1, // +1 because we hold one permit
            ResourceType::Gpu => 1,                                    // Assuming single GPU
            ResourceType::Remote => 10,                                // Default remote concurrency
        };

        // Update the metrics for the appropriate resource type
        match resource_type {
            ResourceType::Cpu => {
                self.metrics.update_metrics(resource_type, available, total);
            }
            ResourceType::Gpu => {
                self.metrics.update_metrics(resource_type, available, total);
            }
            ResourceType::Remote => {
                self.metrics.update_metrics(resource_type, available, total);
            }
        }

        Ok(ResourcePermit {
            _permit: Some(permit),
            resource_type,
            metrics: self.metrics.clone(),
            _marker: std::marker::PhantomData,
        })
    }

    /// Get the current resource utilization metrics
    pub fn resource_metrics(&self) -> ResourceStats {
        self.metrics.get_metrics()
    }

    /// Execute a task with resource management and monitoring
    pub async fn execute_with_resources<F, T, Fut>(
        &self,
        node_id: &str,
        spec: &ResourceSpec,
        task: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.acquire_resources(node_id, spec).await?;
        let _node_id = node_id.to_string();
        let _resource_type = permit.resource_type;

        // Spawn the task with resource tracking
        let task_handle = tokio::spawn(async move {
            let _permit = permit; // Keep the permit alive for the duration of the task
            let result = task().await;
            (result, _resource_type)
        });

        // Wait for the task to complete with timeout
        let timeout_duration = Duration::from_millis(spec.timeout_ms);

        match timeout(timeout_duration, task_handle).await {
            Ok(Ok((result, _))) => result,
            Ok(Err(join_err)) => Err(format!("Task panicked: {}", join_err).into()),
            Err(_) => Err(format!("Task timed out after {:?}", timeout_duration).into()),
        }
    }
}

// Remove duplicate ResourceMetrics struct

#[async_trait]
pub trait ResourceAwareFuture<T>: std::future::Future<Output = T> + Send + 'static {}

impl<T, F> ResourceAwareFuture<T> for F where F: std::future::Future<Output = T> + Send + 'static {}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        // When the permit is dropped, it will be automatically returned to the semaphore.
        // We just need to update our metrics.
        if self._permit.is_some() {
            // Get the current metrics
            let metrics = self.metrics.get_metrics();

            // Update the metrics for the appropriate resource type
            match self.resource_type {
                ResourceType::Cpu => {
                    let available = metrics.cpu_available + 1;
                    self.metrics
                        .cpu_available
                        .store(available, std::sync::atomic::Ordering::Relaxed);
                }
                ResourceType::Gpu => {
                    let available = metrics.gpu_available + 1;
                    self.metrics
                        .gpu_available
                        .store(available, std::sync::atomic::Ordering::Relaxed);
                }
                ResourceType::Remote => {
                    let available = metrics.remote_available + 1;
                    self.metrics
                        .remote_available
                        .store(available, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }
}

// Import cleanup - removed duplicates

/// Extension trait for futures to add resource management
pub trait ResourceExt<T> {
    /// Execute the future with resource management
    fn with_resources(
        self,
        planner: &Planner,
        node_id: &str,
        spec: &ResourceSpec,
    ) -> impl Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static;
}

impl<F, T> ResourceExt<T> for F
where
    F: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    T: Send + 'static,
{
    fn with_resources(
        self,
        planner: &Planner,
        node_id: &str,
        spec: &ResourceSpec,
    ) -> impl Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static
    {
        let node_id = node_id.to_string();
        let spec = spec.clone();
        let planner = planner.clone();

        async move {
            planner
                .execute_with_resources(&node_id, &spec, || self)
                .await
        }
    }
}
