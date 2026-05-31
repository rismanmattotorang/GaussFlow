//! Core data models for the GaussFlow workflow engine.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dag::{DagEdge, DagNode};
use uuid::Uuid;
use crate::hash_impls::hash_json_value;

/// Top-level specification for a GaussFlow workflow.
/// Matches the `GaussFlow JSON` format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowSpec {
    pub name: String,
    pub nodes: Vec<NodeSpec>,
    pub connections: Vec<EdgeSpec>,
    #[serde(default)]
    pub settings: WorkflowSettings,
}

/// Defines global settings for a workflow execution.
/// Represents the status of a workflow execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    TimedOut,
}

/// Represents a checkpoint in workflow execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Unique identifier for this checkpoint
    pub id: String,
    /// Timestamp when the checkpoint was created (milliseconds since epoch)
    pub timestamp: u128,
    /// Status of the workflow at the time of checkpoint
    pub status: WorkflowStatus,
    /// Node ID that was last executed
    pub last_executed_node: Option<String>,
    /// Results of executed nodes
    pub node_results: HashMap<String, serde_json::Value>,
    /// Any error that occurred (if status is Failed)
    pub error: Option<String>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            status: WorkflowStatus::Pending,
            last_executed_node: None,
            node_results: HashMap::new(),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowSettings {
    /// Maximum number of parallel node executions
    #[serde(default)]
    pub concurrency: Option<u32>,
    
    /// Whether to fail fast on first error
    #[serde(default)]
    pub fail_fast: bool,
    
    /// Whether to resume from last checkpoint if available
    #[serde(default)]
    pub resume: bool,
    
    /// Whether to enable checkpointing
    #[serde(default = "default_true")]
    pub enable_checkpoints: bool,
    
    /// Interval between automatic checkpoints (in milliseconds)
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_ms: u64,
    
    /// Maximum number of checkpoints to keep
    #[serde(default = "default_max_checkpoints")]
    pub max_checkpoints: usize,
    
    /// Version of the workflow specification
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_true() -> bool { true }
fn default_checkpoint_interval() -> u64 { 60_000 } // 1 minute
fn default_max_checkpoints() -> usize { 10 }
fn default_version() -> String { "1.0".to_string() }

/// The type of a node, determining its execution logic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// A node that makes LLM API calls
    LlmCall,
    
    /// An autonomous agent that can make decisions
    Agent,
    
    /// An ensemble of multiple models
    Ensemble,
    
    /// Routes data based on conditions
    Router,
    
    /// A sub-workflow
    Subgraph,
    
    /// A data processing node
    DataProcessor,
    
    /// A conditional node (if/else)
    Conditional,
    
    /// A parallel execution node
    Parallel,
}

/// Specification for a single node in the DAG.
/// Metadata for a node in the workflow
impl DagNode for NodeSpec {
    fn validate(&self) -> Result<(), crate::dag::DagValidationError> {
        // Basic validation - ensure required fields are present
        if self.id.is_empty() {
            return Err(crate::dag::DagValidationError::NodeValidation("Node ID cannot be empty".to_string()));
        }
        if self.name.as_ref().map_or(true, |n| n.is_empty()) {
            return Err(crate::dag::DagValidationError::NodeValidation(format!("Node '{}' must have a name", self.id)));
        }
        // Add more validation as needed
        Ok(())
    }

    fn dependencies(&self) -> Vec<String> {
        // For now, return an empty vector as we don't have a direct dependencies field
        // This should be updated to get dependencies from the appropriate field in NodeSpec
        // once it's added to the struct
        Vec::new()
    }

    fn resource_requirements(&self) -> crate::resource::ResourceSpec {
        self.resources.clone().unwrap_or_default()
    }

    fn priority(&self) -> u8 {
        // Get priority from the resources field or use default
        self.resources.as_ref()
            .map(|r| r.priority)
            .unwrap_or(5) // Default priority of 5 if not specified
    }
    
    fn id(&self) -> &str {
        &self.id
    }
    
    fn name(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }
    
    fn node_type(&self) -> &str {
        match &self.node_type {
            NodeType::LlmCall => "llm_call",
            NodeType::Agent => "agent",
            NodeType::Ensemble => "ensemble",
            NodeType::Router => "router",
            NodeType::Subgraph => "subgraph",
            NodeType::DataProcessor => "data_processor",
            NodeType::Conditional => "conditional",
            NodeType::Parallel => "parallel",
        }
    }
}

/// Specification for a connection between two nodes.
impl DagEdge for EdgeSpec {
    fn condition(&self) -> crate::dag::EdgeCondition {
        // For now, return a default condition since we don't have a condition field
        crate::dag::EdgeCondition::Always
    }

    fn metadata(&self) -> crate::dag::EdgeMetadata {
        crate::dag::EdgeMetadata {
            description: None,
            priority: 0,
            retry_policy: None,
            labels: HashMap::new(),
        }
    }
    
    fn validate(&self) -> Result<(), crate::dag::DagValidationError> {
        if self.from.is_empty() {
            return Err(crate::dag::DagValidationError::EdgeValidation("Edge source cannot be empty".to_string()));
        }
        if self.to.is_empty() {
            return Err(crate::dag::DagValidationError::EdgeValidation("Edge target cannot be empty".to_string()));
        }
        if self.from == self.to {
            return Err(crate::dag::DagValidationError::EdgeValidation("Edge cannot connect a node to itself".to_string()));
        }
        Ok(())
    }
    
    fn source(&self) -> String {
        self.from.clone()
    }
    
    fn target(&self) -> String {
        self.to.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    /// Unique identifier for the node
    pub id: String,
    
    /// Type of the node
    #[serde(rename = "type")]
    pub node_type: NodeType,
    
    /// Human-readable name for the node
    #[serde(default)]
    pub name: Option<String>,
    
    /// Description of what the node does
    #[serde(default)]
    pub description: Option<String>,
    
    /// Node type specific configuration
    #[serde(flatten)]
    pub config: NodeConfig,
    
    /// Custom metadata as key-value pairs
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
    
    /// Parameters for the node
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    
    /// Retry policy for the node
    #[serde(default)]
    pub retry: Option<RetrySpec>,
    
    /// Resource requirements for this node
    #[serde(default)]
    pub resources: Option<crate::resource::ResourceSpec>,
    
    /// Timeout in milliseconds
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    
    /// Maximum number of retries on failure
    #[serde(default)]
    pub max_retries: Option<u32>,
    
    /// Tags for categorization and filtering
    #[serde(default)]
    pub tags: Vec<String>,
    
    /// Whether this node is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Hash for NodeSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
        self.node_type.hash(state);
        
        // Hash the config using our custom hashing
        if let Ok(json) = serde_json::to_value(&self.config) {
            hash_json_value(&json, state);
        }
        
        // Hash metadata by serializing to JSON
        if let Ok(json) = serde_json::to_value(&self.metadata) {
            hash_json_value(&json, state);
        }
        
        // Hash params by serializing to JSON
        if let Ok(json) = serde_json::to_value(&self.params) {
            hash_json_value(&json, state);
        }
        
        // Hash retry if present
        if let Some(ref retry) = self.retry {
            retry.hash(state);
        }
        
        // Hash resources if present
        if let Some(ref resources) = self.resources {
            resources.hash(state);
        }
    }
}

impl PartialEq for NodeSpec {
    fn eq(&self, other: &Self) -> bool {
        // Compare simple fields
        if self.id != other.id || self.name != other.name || self.node_type != other.node_type {
            return false;
        }
        
        // Compare configs by serializing to JSON
        let self_config = serde_json::to_value(&self.config).ok();
        let other_config = serde_json::to_value(&other.config).ok();
        if self_config != other_config {
            return false;
        }
        
        // Compare metadata and params by serializing to JSON
        let self_meta = serde_json::to_value(&self.metadata).ok();
        let other_meta = serde_json::to_value(&other.metadata).ok();
        if self_meta != other_meta {
            return false;
        }
        
        let self_params = serde_json::to_value(&self.params).ok();
        let other_params = serde_json::to_value(&other.params).ok();
        if self_params != other_params {
            return false;
        }
        
        // Compare other fields
        self.retry == other.retry && 
        self.resources == other.resources &&
        self.timeout_ms == other.timeout_ms &&
        self.max_retries == other.max_retries &&
        self.tags == other.tags &&
        self.enabled == other.enabled
    }
}

impl Eq for NodeSpec {}



/// Node type specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum NodeConfig {
    /// Configuration for LLM nodes
    LlmCall {
        /// Model to use (e.g., "gpt-4", "claude-2")
        model: String,
        
        /// Maximum tokens to generate
        max_tokens: Option<u32>,
        
        /// Sampling temperature
        temperature: Option<f32>,
        
        /// Top-p sampling
        top_p: Option<f32>,
        
        /// Stop sequences
        stop: Option<Vec<String>>,
    },
    
    /// Configuration for agent nodes
    Agent {
        /// Agent type
        agent_type: String,
        
        /// Agent configuration
        config: HashMap<String, serde_json::Value>,
    },
    
    /// Configuration for router nodes
    Router {
        /// Routing conditions
        conditions: Vec<RouterCondition>,
        
        /// Default route if no conditions match
        default_route: Option<String>,
    },
    
    /// Configuration for subgraph nodes
    Subgraph {
        /// Reference to the sub-workflow
        workflow_id: String,
        
        /// Input mapping
        inputs: HashMap<String, String>,
    },
    
    /// Fallback for unknown/unspecified node types
    Generic(HashMap<String, serde_json::Value>),
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self::Generic(HashMap::new())
    }
}

impl Hash for NodeConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use JSON serialization for consistent hashing
        if let Ok(json) = serde_json::to_value(self) {
            hash_json_value(&json, state);
        }
    }
}

impl Eq for NodeConfig {}

/// Represents a routing condition in a router node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RouterCondition {
    /// Condition expression (e.g., "{{input.length}} > 10")
    pub condition: String,
    
    /// Target node ID if condition is true
    pub target: String,
    
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeSpec {
    pub from: String,
    pub to: String,
    #[serde(default = "default_on_success")]
    pub on: String,
}

/// Provides a default value for `EdgeSpec.on`.
fn default_on_success() -> String {
    "success".to_string()
}

/// Retry specification for nodes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetrySpec {
    pub max_attempts: u32,
    pub backoff: Backoff,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Backoff {
    Fixed,
    Exponential,
    Linear,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::Fixed
    }
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Backoff::Fixed,
            timeout: None,
        }
    }
}
