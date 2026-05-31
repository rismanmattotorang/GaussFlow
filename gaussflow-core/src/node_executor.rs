use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use crate::engine::ExecutionError;
use crate::model::{NodeSpec, ResourceSpec};

/// Context passed to node executors during execution
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    /// Unique ID for this execution
    pub execution_id: String,
    /// Attempt number (0-based)
    pub attempt: u32,
    /// Node-specific configuration
    pub node_config: Value,
    /// Environment variables
    pub env: HashMap<String, String>,
    /// Resource requirements
    pub resources: ResourceSpec,
    /// Metadata about the execution environment
    pub metadata: HashMap<String, Value>,
}

/// Trait for executing workflow nodes
#[async_trait]
pub trait NodeExecutor: Send + Sync + 'static {
    /// Get the concurrency level for this executor
    fn concurrency(&self) -> usize {
        1
    }

    /// Get the resource requirements for a node
    fn resource_requirements(&self, _node: &NodeSpec) -> ResourceSpec {
        ResourceSpec::default()
    }

    /// Execute a node with the given input and context
    async fn execute<'a>(
        &'a self,
        node: &'a NodeSpec,
        input: &'a Value,
        ctx: &'a ExecutionContext,
    ) -> Result<Value, ExecutionError>;
}

/// Default implementation of NodeExecutor
#[derive(Clone)]
pub struct DefaultNodeExecutor;

#[async_trait]
impl NodeExecutor for DefaultNodeExecutor {
    async fn execute<'a>(
        &'a self,
        _node: &'a NodeSpec,
        _input: &'a Value,
        _ctx: &'a ExecutionContext,
    ) -> Result<Value, ExecutionError> {
        // Default implementation just returns an empty object
        Ok(serde_json::json!({}))
    }
}
