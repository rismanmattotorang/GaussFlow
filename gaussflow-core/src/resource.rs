//! Resource management for workflow execution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Represents the specification for resources required by a node in the workflow.
/// This includes CPU, memory, GPU, and other resource constraints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceSpec {
    /// Number of CPU cores required (1 core = 1000 millicores)
    pub cpu_cores: u32,
    /// Memory required in megabytes (MB)
    pub memory_mb: u64,
    /// Number of GPUs required
    pub gpu_count: u32,
    /// Human-readable memory string (e.g., "4GB")
    pub memory: String,
    /// Maximum number of concurrent executions
    pub concurrency: u32,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Priority level (higher is more important)
    pub priority: u8,
    /// Key-value pairs for additional metadata
    pub labels: HashMap<String, String>,
    /// Whether this resource must be allocated remotely
    pub remote: bool,
    /// Optional executor requirement
    pub required_executor: Option<String>,
    /// CPU requirements in millicores (1000 = 1 core)
    pub cpu_millicores: u64,
    /// Number of retry attempts
    pub retry_attempts: u32,
    /// Delay between retries in milliseconds
    pub retry_delay_ms: u64,
    /// CPU affinity (which cores to use)
    pub affinity: Option<Vec<u32>>,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            gpu_count: 0,
            memory: "1GB".to_string(),
            concurrency: 1,
            timeout_ms: 30_000, // 30 seconds
            priority: 5,        // Medium priority
            labels: HashMap::new(),
            remote: false,
            required_executor: None,
            memory_mb: 1024,    // 1GB
            cpu_millicores: 1000, // 1 core
            retry_attempts: 3,
            retry_delay_ms: 1000, // 1 second
            affinity: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceManager {
    cpu_pool: Arc<Semaphore>,
    gpu_pool: Arc<Semaphore>,
    memory_pool: Arc<Semaphore>,
    resource_limits: ResourceSpec,
    active_allocations: DashMap<String, ResourceAllocation>,
}

impl Default for ResourceManager {
    fn default() -> Self {
        let resource_limits = ResourceSpec::default();
        let cpu_pool = Arc::new(Semaphore::new(resource_limits.cpu_cores as usize));
        let gpu_pool = Arc::new(Semaphore::new(resource_limits.gpu_count as usize));
        let memory_pool = Arc::new(Semaphore::new(1));
        
        Self {
            cpu_pool,
            gpu_pool,
            memory_pool,
            resource_limits,
            active_allocations: DashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct ResourceAllocation {
    pub node_id: String,
    pub spec: ResourceSpec,
    // These fields are not Clone, so we'll need to handle them specially
    pub cpu_permits: Vec<OwnedSemaphorePermit>,
    pub gpu_permits: Vec<OwnedSemaphorePermit>,
    pub memory_permits: Vec<OwnedSemaphorePermit>,
    pub allocated_at: Instant,
}

// Implement Clone manually since OwnedSemaphorePermit doesn't implement Clone
impl Clone for ResourceAllocation {
    fn clone(&self) -> Self {
        // Create a new allocation with empty permits since we can't clone them
        // The cloned allocation will need to be reacquired
        Self {
            node_id: self.node_id.clone(),
            spec: self.spec.clone(),
            cpu_permits: Vec::new(),
            gpu_permits: Vec::new(),
            memory_permits: Vec::new(),
            allocated_at: self.allocated_at,
        }
    }
}

impl ResourceAllocation {
    /// Creates a new resource allocation
    pub fn new(
        node_id: String,
        spec: ResourceSpec,
        cpu_permits: Vec<OwnedSemaphorePermit>,
        gpu_permits: Vec<OwnedSemaphorePermit>,
        memory_permits: Vec<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            node_id,
            spec,
            cpu_permits,
            gpu_permits,
            memory_permits,
            allocated_at: Instant::now(),
        }
    }
}

/// A guard that automatically releases resources when dropped
pub struct ResourceGuard {
    manager: Arc<ResourceManager>,
    node_id: String,
    allocation: Option<ResourceAllocation>,
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        if let Some(_allocation) = self.allocation.take() {
            // Release resources when the guard is dropped
            let manager = self.manager.clone();
            let node_id = self.node_id.clone();
            
            // Use a blocking task to release resources
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    manager.release_resources(&node_id);
                });
            });
        }
    }
}

impl ResourceManager {
    /// Allocates resources for a node
    pub async fn allocate(&self, node_id: &str, spec: &ResourceSpec) -> Result<ResourceGuard, ResourceError> {
        self.acquire_with_timeout(node_id, spec, std::time::Duration::from_secs(30)).await
    }

    pub fn new(resource_limits: ResourceSpec) -> Arc<Self> {
        let cpu_pool = Arc::new(Semaphore::new(resource_limits.cpu_cores as usize));
        let gpu_pool = Arc::new(Semaphore::new(resource_limits.gpu_count as usize));
        let memory_pool = Arc::new(Semaphore::new(1)); // Memory is handled differently
        
        Arc::new(Self {
            cpu_pool,
            gpu_pool,
            memory_pool,
            resource_limits,
            active_allocations: DashMap::new(),
        })
    }

    /// Alias for acquire_resources for backward compatibility
    pub async fn acquire(
        &self,
        spec: &ResourceSpec,
        timeout: std::time::Duration,
    ) -> Result<ResourceGuard, ResourceError> {
        self.acquire_with_timeout("default", spec, timeout).await
    }

    /// Acquire resources with a specific node ID and timeout
    pub async fn acquire_with_timeout(
        &self,
        node_id: &str,
        spec: &ResourceSpec,
        timeout: std::time::Duration,
    ) -> Result<ResourceGuard, ResourceError> {
        let allocation = tokio::time::timeout(timeout, self.acquire_resources(node_id, spec))
            .await
            .map_err(|_| ResourceError::AcquisitionTimeout)??;
        
        Ok(ResourceGuard {
            manager: Arc::new(self.clone()),
            node_id: node_id.to_string(),
            allocation: Some(allocation),
        })
    }

    /// Allocates resources based on the given spec
    pub async fn acquire_resources(
        &self,
        node_id: &str,
        spec: &ResourceSpec,
    ) -> Result<ResourceAllocation, ResourceError> {
        let allocation = self.allocate_resources(node_id, spec).await?;
        Ok(allocation)
    }

    /// Allocates resources based on the given spec
    pub async fn allocate_resources(&self, node_id: &str, spec: &ResourceSpec) -> Result<ResourceAllocation, ResourceError> {
        // Check if we have enough resources
        if spec.cpu_cores > self.resource_limits.cpu_cores {
            return Err(ResourceError::InsufficientResources("CPU cores".to_string()));
        }
        if spec.memory_mb > self.resource_limits.memory_mb {
            return Err(ResourceError::InsufficientResources("memory".to_string()));
        }
        if spec.gpu_count > self.resource_limits.gpu_count {
            return Err(ResourceError::InsufficientResources("GPU".to_string()));
        }

        // Try to acquire CPU permits
        let cpu_permits = if spec.cpu_cores > 0 {
            let permits = spec.cpu_cores as usize;
            let mut permits_vec = Vec::with_capacity(permits);
            for _ in 0..permits {
                let permit = self.cpu_pool.clone().acquire_owned().await
                    .map_err(|_| ResourceError::InsufficientResources("CPU".to_string()))?;
                permits_vec.push(permit);
            }
            permits_vec
        } else {
            Vec::new()
        };

        // Try to acquire GPU permits if needed
        let gpu_permits = if spec.gpu_count > 0 {
            let mut permits_vec = Vec::with_capacity(spec.gpu_count as usize);
            for _ in 0..spec.gpu_count {
                let permit = self.gpu_pool.clone().acquire_owned().await
                    .map_err(|_| ResourceError::InsufficientResources("GPU".to_string()))?;
                permits_vec.push(permit);
            }
            permits_vec
        } else {
            Vec::new()
        };

        // Try to acquire memory permits
        let memory_permits = if spec.memory_mb > 0 {
            let permits = spec.memory_mb as usize;
            let mut permits_vec = Vec::with_capacity(permits);
            for _ in 0..permits {
                let permit = self.memory_pool.clone().acquire_owned().await
                    .map_err(|_| ResourceError::InsufficientResources("memory".to_string()))?;
                permits_vec.push(permit);
            }
            permits_vec
        } else {
            Vec::new()
        };

        // Create the allocation
        let allocation = ResourceAllocation::new(
            node_id.to_string(),
            spec.clone(),
            cpu_permits,
            gpu_permits,
            memory_permits,
        );

        // Store the allocation
        self.active_allocations.insert(node_id.to_string(), allocation.clone());

        Ok(allocation)
    }

    pub fn release_resources(&self, node_id: &str) {
        if let Some(entry) = self.active_allocations.remove(node_id) {
            for permit in entry.1.cpu_permits {
                drop(permit);
            }
            for permit in entry.1.gpu_permits {
                drop(permit);
            }
            for permit in entry.1.memory_permits {
                drop(permit);
            }
        }
    }

    pub fn get_resource_usage(&self) -> ResourceUsage {
        ResourceUsage {
            cpu: self.cpu_pool.available_permits(),
            gpu: self.gpu_pool.available_permits(),
            memory: self.memory_pool.available_permits(),
            active_allocations: self.active_allocations.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu: usize,
    pub gpu: usize,
    pub memory: usize,
    pub active_allocations: usize,
}

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("Insufficient resources: {0}")]
    InsufficientResources(String),
    
    #[error("Resource acquisition timed out")]
    AcquisitionTimeout,
    
    #[error("Resource allocation timeout")]
    Timeout,
    
    #[error("Resource validation failed: {0}")]
    Validation(String),
}
