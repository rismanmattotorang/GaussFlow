use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Comprehensive error type for all GaussFlow operations
///
/// This error type provides:
/// - Detailed error categorization
/// - Context preservation with structured metadata
/// - Recovery strategy information
/// - Error severity levels
/// - Timestamp and correlation IDs
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum GaussFlowError {
    /// DAG validation errors
    #[error("DAG validation error: {message}")]
    DagValidation {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        edge_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Execution errors
    #[error("Execution error: {message}")]
    Execution {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        workflow_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Resource allocation errors
    #[error("Resource allocation error: {message}")]
    Resource {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        requested: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        available: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Checkpoint errors
    #[error("Checkpoint error: {message}")]
    Checkpoint {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        checkpoint_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Versioning errors
    #[error("Versioning error: {message}")]
    Versioning {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        workflow_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Storage errors
    #[error("Storage error: {message}")]
    Storage {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        storage_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Serialization errors
    #[error("Serialization error: {message}")]
    Serialization {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        field: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        section: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Authentication errors
    #[error("Authentication error: {message}")]
    Authentication {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        user_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Authorization errors
    #[error("Authorization error: {message}")]
    Authorization {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        user_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        permission: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Policy violation errors
    #[error("Policy violation: {message}")]
    Policy {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        policy_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        violation_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        severity: Option<PolicyViolationSeverity>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Node execution errors
    #[error("Node execution error: {message}")]
    NodeExecution {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Stream processing errors
    #[error("Stream processing error: {message}")]
    Stream {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Batch processing errors
    #[error("Batch processing error: {message}")]
    Batch {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Timeout errors
    #[error("Timeout error: {message}")]
    Timeout {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Cancellation errors
    #[error("Operation cancelled: {message}")]
    Cancelled {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },

    /// Internal errors
    #[error("Internal error: {message}")]
    Internal {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        component: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_strategy: Option<RecoveryStrategy>,
    },
}

impl GaussFlowError {
    /// Create a new DAG validation error
    pub fn dag_validation(
        message: impl Into<String>,
        node_id: Option<String>,
        edge_id: Option<String>,
    ) -> Self {
        Self::DagValidation {
            message: message.into(),
            node_id,
            edge_id,
            details: None,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }

    /// Create a new execution error
    pub fn execution(
        message: impl Into<String>,
        node_id: Option<String>,
        workflow_id: Option<String>,
    ) -> Self {
        Self::Execution {
            message: message.into(),
            node_id,
            workflow_id,
            attempt: None,
            duration: None,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }

    /// Create a new resource error
    pub fn resource(
        message: impl Into<String>,
        resource_type: Option<String>,
        requested: Option<String>,
        available: Option<String>,
    ) -> Self {
        Self::Resource {
            message: message.into(),
            resource_type,
            requested,
            available,
            recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
        }
    }

    /// Create a new timeout error
    pub fn timeout(
        message: impl Into<String>,
        duration: Option<Duration>,
        operation: Option<String>,
    ) -> Self {
        Self::Timeout {
            message: message.into(),
            duration,
            operation,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }

    /// Create a new cancellation error
    pub fn cancelled(message: impl Into<String>, reason: Option<String>) -> Self {
        Self::Cancelled {
            message: message.into(),
            reason,
            recovery_strategy: Some(RecoveryStrategy::Abort),
        }
    }

    /// Get the error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::DagValidation { .. } => ErrorSeverity::Error,
            Self::Execution { .. } => ErrorSeverity::Error,
            Self::Resource { .. } => ErrorSeverity::Warning,
            Self::Checkpoint { .. } => ErrorSeverity::Error,
            Self::Versioning { .. } => ErrorSeverity::Warning,
            Self::Storage { .. } => ErrorSeverity::Error,
            Self::Serialization { .. } => ErrorSeverity::Error,
            Self::Configuration { .. } => ErrorSeverity::Error,
            Self::Authentication { .. } => ErrorSeverity::Error,
            Self::Authorization { .. } => ErrorSeverity::Error,
            Self::Policy { severity, .. } => severity
                .as_ref()
                .map(|s| (*s).into())
                .unwrap_or(ErrorSeverity::Error),
            Self::NodeExecution { .. } => ErrorSeverity::Error,
            Self::Stream { .. } => ErrorSeverity::Error,
            Self::Batch { .. } => ErrorSeverity::Error,
            Self::Timeout { .. } => ErrorSeverity::Warning,
            Self::Cancelled { .. } => ErrorSeverity::Info,
            Self::Internal { .. } => ErrorSeverity::Error,
        }
    }

    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::DagValidation { .. } => false,
            Self::Execution { .. } => true,
            Self::Resource { .. } => true,
            Self::Checkpoint { .. } => true,
            Self::Versioning { .. } => true,
            Self::Storage { .. } => true,
            Self::Serialization { .. } => false,
            Self::Configuration { .. } => false,
            Self::Authentication { .. } => false,
            Self::Authorization { .. } => false,
            Self::Policy { .. } => false,
            Self::NodeExecution { .. } => true,
            Self::Stream { .. } => true,
            Self::Batch { .. } => true,
            Self::Timeout { .. } => true,
            Self::Cancelled { .. } => false,
            Self::Internal { .. } => false,
        }
    }

    /// Get the recovery strategy
    pub fn recovery_strategy(&self) -> Option<&RecoveryStrategy> {
        match self {
            Self::DagValidation {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Execution {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Resource {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Checkpoint {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Versioning {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Storage {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Serialization {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Configuration {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Authentication {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Authorization {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Policy {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::NodeExecution {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Stream {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Batch {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Timeout {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Cancelled {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
            Self::Internal {
                recovery_strategy, ..
            } => recovery_strategy.as_ref(),
        }
    }

    /// Add context to the error
    pub fn with_context(mut self, context: HashMap<String, String>) -> Self {
        match &mut self {
            Self::DagValidation { details, .. } => {
                *details = Some(context);
            }
            Self::Execution { .. } => {
                // Could add a context field to Execution if needed
            }
            _ => {
                // For other variants, we could add a generic context field
            }
        }
        self
    }
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARNING"),
            Self::Error => write!(f, "ERROR"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Policy violation severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyViolationSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl From<PolicyViolationSeverity> for ErrorSeverity {
    fn from(severity: PolicyViolationSeverity) -> Self {
        match severity {
            PolicyViolationSeverity::Low => ErrorSeverity::Warning,
            PolicyViolationSeverity::Medium => ErrorSeverity::Warning,
            PolicyViolationSeverity::High => ErrorSeverity::Error,
            PolicyViolationSeverity::Critical => ErrorSeverity::Critical,
        }
    }
}

/// Recovery strategies for errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    /// Retry the operation
    Retry,
    /// Wait and then retry
    WaitAndRetry,
    /// Skip the operation and continue
    Skip,
    /// Abort the entire workflow
    Abort,
    /// Use a fallback mechanism
    Fallback,
    /// Manual intervention required
    Manual,
}

impl fmt::Display for RecoveryStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry => write!(f, "RETRY"),
            Self::WaitAndRetry => write!(f, "WAIT_AND_RETRY"),
            Self::Skip => write!(f, "SKIP"),
            Self::Abort => write!(f, "ABORT"),
            Self::Fallback => write!(f, "FALLBACK"),
            Self::Manual => write!(f, "MANUAL"),
        }
    }
}

/// Enhanced error type for DAG-related operations
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum DagError {
    #[error("Cycle detected in DAG")]
    CycleDetected {
        #[serde(skip_serializing_if = "Option::is_none")]
        cycle_path: Option<Vec<String>>,
    },

    #[error("Node validation failed: {message}")]
    NodeValidation {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<HashMap<String, String>>,
    },

    #[error("Edge validation failed: {message}")]
    EdgeValidation {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        from_node: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        to_node: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<HashMap<String, String>>,
    },

    #[error("Missing dependencies for node {node_id}")]
    MissingDependencies {
        node_id: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        missing_deps: Vec<String>,
    },

    #[error("Invalid edge condition: {message}")]
    InvalidEdgeCondition {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        edge_id: Option<String>,
    },
}

/// Enhanced error type for resource management operations
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum ResourceError {
    #[error("Insufficient resources: {message}")]
    InsufficientResources {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        requested: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        available: Option<String>,
    },

    #[error("Resource allocation timeout")]
    AcquisitionTimeout {
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_duration: Option<Duration>,
    },

    #[error("Resource allocation timeout")]
    Timeout,

    #[error("Resource validation failed: {message}")]
    Validation {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource_spec: Option<String>,
    },
}

/// Enhanced error type for execution operations
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum ExecutionError {
    #[error("Node execution failed: {message}")]
    NodeExecution {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
    },

    #[error("Resource allocation failed: {message}")]
    Resource {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource_type: Option<String>,
    },

    #[error("Timeout during execution: {message}")]
    Timeout {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration: Option<Duration>,
    },

    #[error("Retry limit exceeded: {message}")]
    RetryLimitExceeded {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_retries: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        attempts: Option<u32>,
    },

    #[error("Stream processing error: {message}")]
    Stream {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
    },

    #[error("Batch processing error: {message}")]
    Batch {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_id: Option<String>,
    },
}

/// Enhanced error type for policy operations
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum PolicyError {
    #[error("Policy validation failed: {message}")]
    Validation {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        policy_id: Option<String>,
    },

    #[error("Policy enforcement failed: {message}")]
    Enforcement {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        policy_id: Option<String>,
    },

    #[error("Policy configuration error: {message}")]
    Configuration {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        policy_id: Option<String>,
    },
}

/// Error context for additional metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    /// Timestamp when the error occurred
    pub timestamp: SystemTime,
    /// Correlation ID for tracing
    pub correlation_id: Option<String>,
    /// User ID if applicable
    pub user_id: Option<String>,
    /// Session ID if applicable
    pub session_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl ErrorContext {
    /// Create a new error context
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now(),
            correlation_id: None,
            user_id: None,
            session_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new error context with correlation ID
    pub fn with_correlation_id(correlation_id: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            correlation_id: Some(correlation_id),
            user_id: None,
            session_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the context
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

impl Default for ErrorContext {
    fn default() -> Self {
        Self::new()
    }
}

// Implement From for common error types
impl From<serde_json::Error> for GaussFlowError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization {
            message: err.to_string(),
            format: Some("JSON".to_string()),
            field: None,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }
}

impl From<std::io::Error> for GaussFlowError {
    fn from(err: std::io::Error) -> Self {
        Self::Storage {
            message: err.to_string(),
            storage_type: Some("filesystem".to_string()),
            path: None,
            operation: None,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }
}

impl From<tokio::task::JoinError> for GaussFlowError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::Execution {
            message: err.to_string(),
            node_id: None,
            workflow_id: None,
            attempt: None,
            duration: None,
            recovery_strategy: Some(RecoveryStrategy::Retry),
        }
    }
}

impl From<DagError> for GaussFlowError {
    fn from(err: DagError) -> Self {
        match err {
            DagError::CycleDetected { cycle_path } => Self::DagValidation {
                message: "Cycle detected in DAG".to_string(),
                node_id: None,
                edge_id: None,
                details: cycle_path.map(|path| {
                    let mut details = HashMap::new();
                    details.insert("cycle_path".to_string(), path.join(" -> "));
                    details
                }),
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
            DagError::NodeValidation {
                message,
                node_id,
                details,
            } => Self::DagValidation {
                message,
                node_id,
                edge_id: None,
                details,
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
            DagError::EdgeValidation {
                message,
                from_node,
                to_node,
                details,
            } => Self::DagValidation {
                message,
                node_id: from_node,
                edge_id: to_node,
                details,
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
            DagError::MissingDependencies {
                node_id,
                missing_deps,
            } => Self::DagValidation {
                message: format!("Missing dependencies for node {}", node_id),
                node_id: Some(node_id),
                edge_id: None,
                details: Some({
                    let mut details = HashMap::new();
                    details.insert("missing_dependencies".to_string(), missing_deps.join(", "));
                    details
                }),
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
            DagError::InvalidEdgeCondition { message, edge_id } => Self::DagValidation {
                message,
                node_id: None,
                edge_id,
                details: None,
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
        }
    }
}

impl From<ResourceError> for GaussFlowError {
    fn from(err: ResourceError) -> Self {
        match err {
            ResourceError::InsufficientResources {
                message,
                resource_type,
                requested,
                available,
            } => Self::Resource {
                message,
                resource_type,
                requested,
                available,
                recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            },
            ResourceError::AcquisitionTimeout {
                timeout_duration: _,
            } => Self::Resource {
                message: "Resource acquisition timeout".to_string(),
                resource_type: None,
                requested: None,
                available: None,
                recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            },
            ResourceError::Timeout => Self::Resource {
                message: "Resource allocation timeout".to_string(),
                resource_type: None,
                requested: None,
                available: None,
                recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            },
            ResourceError::Validation {
                message,
                resource_spec,
            } => Self::Resource {
                message,
                resource_type: None,
                requested: resource_spec,
                available: None,
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
        }
    }
}

impl From<ExecutionError> for GaussFlowError {
    fn from(err: ExecutionError) -> Self {
        match err {
            ExecutionError::NodeExecution {
                message,
                node_id,
                attempt,
            } => Self::NodeExecution {
                message,
                node_id,
                node_type: None,
                attempt,
                duration: None,
                recovery_strategy: Some(RecoveryStrategy::Retry),
            },
            ExecutionError::Resource {
                message,
                resource_type,
            } => Self::Resource {
                message,
                resource_type,
                requested: None,
                available: None,
                recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            },
            ExecutionError::Timeout { message, duration } => Self::Timeout {
                message,
                duration,
                operation: None,
                recovery_strategy: Some(RecoveryStrategy::Retry),
            },
            ExecutionError::RetryLimitExceeded {
                message,
                max_retries: _,
                attempts,
            } => Self::Execution {
                message,
                node_id: None,
                workflow_id: None,
                attempt: attempts,
                duration: None,
                recovery_strategy: Some(RecoveryStrategy::Abort),
            },
            ExecutionError::Stream { message, stream_id } => Self::Stream {
                message,
                stream_id,
                operation: None,
                recovery_strategy: Some(RecoveryStrategy::Retry),
            },
            ExecutionError::Batch { message, batch_id } => Self::Batch {
                message,
                batch_id,
                size: None,
                recovery_strategy: Some(RecoveryStrategy::Retry),
            },
        }
    }
}

impl From<PolicyError> for GaussFlowError {
    fn from(err: PolicyError) -> Self {
        match err {
            PolicyError::Validation { message, policy_id } => Self::Policy {
                message,
                policy_id,
                violation_type: Some("validation".to_string()),
                severity: Some(PolicyViolationSeverity::Medium),
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
            PolicyError::Enforcement { message, policy_id } => Self::Policy {
                message,
                policy_id,
                violation_type: Some("enforcement".to_string()),
                severity: Some(PolicyViolationSeverity::High),
                recovery_strategy: Some(RecoveryStrategy::Abort),
            },
            PolicyError::Configuration { message, policy_id } => Self::Policy {
                message,
                policy_id,
                violation_type: Some("configuration".to_string()),
                severity: Some(PolicyViolationSeverity::Medium),
                recovery_strategy: Some(RecoveryStrategy::Manual),
            },
        }
    }
}
