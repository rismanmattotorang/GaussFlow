//! Comprehensive error types for the runtime.

use std::fmt;
use std::error::Error as StdError;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use thiserror::Error;

/// Main error type for the runtime with comprehensive error handling.
/// 
/// This error type provides:
/// - Detailed error categorization with context
/// - Structured metadata for debugging and monitoring
/// - Recovery strategy information
/// - Error severity levels
/// - Timestamp and correlation IDs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeError {
    /// The kind of error that occurred
    pub kind: ErrorKind,
    /// The error message
    pub message: String,
    /// Optional source error for error chaining
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Additional context metadata
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub context: HashMap<String, String>,
    /// Error severity level
    pub severity: ErrorSeverity,
    /// Recovery strategy for this error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_strategy: Option<RecoveryStrategy>,
    /// Timestamp when the error occurred
    pub timestamp: SystemTime,
    /// Correlation ID for tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// Whether this error is retryable
    pub retryable: bool,
    /// Maximum number of retries for this error type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    /// Suggested backoff duration for retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_backoff: Option<Duration>,
}

impl RuntimeError {
    /// Create a new runtime error.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: None,
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            max_retries: None,
            suggested_backoff: None,
        }
    }

    /// Create a new runtime error with a source error.
    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(source.into()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: None,
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            max_retries: None,
            suggested_backoff: None,
        }
    }

    /// Create a new task execution error
    pub fn task_error(
        message: impl Into<String>,
        node_id: Option<String>,
        attempt: Option<u32>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(id) = node_id {
            context.insert("node_id".to_string(), id);
        }
        if let Some(att) = attempt {
            context.insert("attempt".to_string(), att.to_string());
        }

        Self {
            kind: ErrorKind::TaskError,
            message: message.into(),
            source: None,
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(3),
            suggested_backoff: Some(Duration::from_secs(1)),
        }
    }

    /// Create a new resource error
    pub fn resource_error(
        message: impl Into<String>,
        resource_type: Option<String>,
        requested: Option<String>,
        available: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(rt) = resource_type {
            context.insert("resource_type".to_string(), rt);
        }
        if let Some(req) = requested {
            context.insert("requested".to_string(), req);
        }
        if let Some(avail) = available {
            context.insert("available".to_string(), avail);
        }

        Self {
            kind: ErrorKind::ResourceError,
            message: message.into(),
            source: None,
            context,
            severity: ErrorSeverity::Warning,
            recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(5),
            suggested_backoff: Some(Duration::from_secs(5)),
        }
    }

    /// Create a new timeout error
    pub fn timeout_error(
        message: impl Into<String>,
        duration: Option<Duration>,
        operation: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(dur) = duration {
            context.insert("timeout_duration".to_string(), format!("{:?}", dur));
        }
        if let Some(op) = operation {
            context.insert("operation".to_string(), op);
        }

        Self {
            kind: ErrorKind::TimeoutError,
            message: message.into(),
            source: None,
            context,
            severity: ErrorSeverity::Warning,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(3),
            suggested_backoff: Some(Duration::from_secs(2)),
        }
    }

    /// Create a new cancellation error
    pub fn cancelled_error(message: impl Into<String>, reason: Option<String>) -> Self {
        let mut context = HashMap::new();
        if let Some(r) = reason {
            context.insert("reason".to_string(), r);
        }

        Self {
            kind: ErrorKind::Cancelled,
            message: message.into(),
            source: None,
            context,
            severity: ErrorSeverity::Info,
            recovery_strategy: Some(RecoveryStrategy::Abort),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            max_retries: None,
            suggested_backoff: None,
        }
    }

    /// Get the error kind.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the error severity.
    pub fn severity(&self) -> ErrorSeverity {
        self.severity
    }

    /// Check if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// Get the recovery strategy.
    pub fn recovery_strategy(&self) -> Option<&RecoveryStrategy> {
        self.recovery_strategy.as_ref()
    }

    /// Add context to the error.
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.context.insert(key, value);
        self
    }

    /// Set the correlation ID for tracing.
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Set the recovery strategy.
    pub fn with_recovery_strategy(mut self, strategy: RecoveryStrategy) -> Self {
        self.recovery_strategy = Some(strategy);
        self
    }

    /// Set the error severity.
    pub fn with_severity(mut self, severity: ErrorSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set retry configuration.
    pub fn with_retry_config(mut self, retryable: bool, max_retries: Option<u32>, backoff: Option<Duration>) -> Self {
        self.retryable = retryable;
        self.max_retries = max_retries;
        self.suggested_backoff = backoff;
        self
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.severity, self.kind, self.message)?;
        
        if !self.context.is_empty() {
            write!(f, " (context: {:?})", self.context)?;
        }
        
        if let Some(ref source) = self.source {
            write!(f, " (source: {})", source)?;
        }
        
        if let Some(ref strategy) = self.recovery_strategy {
            write!(f, " (recovery: {})", strategy)?;
        }
        
        Ok(())
    }
}

impl StdError for RuntimeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None // We store source as String, not as a trait object
    }
}

/// Categories of runtime errors with detailed information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    /// An error occurred during task execution.
    TaskError,
    /// A scheduling error occurred.
    SchedulerError,
    /// A resource error occurred.
    ResourceError,
    /// A serialization/deserialization error occurred.
    SerializationError,
    /// A timeout occurred.
    TimeoutError,
    /// The operation was cancelled.
    Cancelled,
    /// An internal error occurred.
    InternalError,
    /// A configuration error occurred.
    ConfigurationError,
    /// A network error occurred.
    NetworkError,
    /// A memory allocation error occurred.
    MemoryError,
    /// A concurrency error occurred.
    ConcurrencyError,
    /// A validation error occurred.
    ValidationError,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskError => write!(f, "TaskError"),
            Self::SchedulerError => write!(f, "SchedulerError"),
            Self::ResourceError => write!(f, "ResourceError"),
            Self::SerializationError => write!(f, "SerializationError"),
            Self::TimeoutError => write!(f, "TimeoutError"),
            Self::Cancelled => write!(f, "Cancelled"),
            Self::InternalError => write!(f, "InternalError"),
            Self::ConfigurationError => write!(f, "ConfigurationError"),
            Self::NetworkError => write!(f, "NetworkError"),
            Self::MemoryError => write!(f, "MemoryError"),
            Self::ConcurrencyError => write!(f, "ConcurrencyError"),
            Self::ValidationError => write!(f, "ValidationError"),
        }
    }
}

/// Error severity levels.
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

/// Recovery strategies for errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    /// Retry the operation.
    Retry,
    /// Wait and then retry.
    WaitAndRetry,
    /// Skip the operation and continue.
    Skip,
    /// Abort the entire workflow.
    Abort,
    /// Use a fallback mechanism.
    Fallback,
    /// Manual intervention required.
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

/// Convenience type alias for results in the runtime.
pub type Result<T> = std::result::Result<T, RuntimeError>;

/// Error context for additional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    /// Timestamp when the error occurred.
    pub timestamp: SystemTime,
    /// Correlation ID for tracing.
    pub correlation_id: Option<String>,
    /// User ID if applicable.
    pub user_id: Option<String>,
    /// Session ID if applicable.
    pub session_id: Option<String>,
    /// Additional metadata.
    pub metadata: HashMap<String, String>,
}

impl ErrorContext {
    /// Create a new error context.
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now(),
            correlation_id: None,
            user_id: None,
            session_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new error context with correlation ID.
    pub fn with_correlation_id(correlation_id: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            correlation_id: Some(correlation_id),
            user_id: None,
            session_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the context.
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
impl From<serde_json::Error> for RuntimeError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::SerializationError,
            message: "Failed to serialize/deserialize data".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            max_retries: None,
            suggested_backoff: None,
        }
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::TimedOut => ErrorKind::TimeoutError,
            std::io::ErrorKind::NotFound => ErrorKind::ResourceError,
            std::io::ErrorKind::PermissionDenied => ErrorKind::ResourceError,
            _ => ErrorKind::InternalError,
        };

        let retryable = matches!(err.kind(), 
            std::io::ErrorKind::TimedOut | 
            std::io::ErrorKind::WouldBlock |
            std::io::ErrorKind::Interrupted
        );

        Self {
            kind,
            message: "I/O error occurred".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: if retryable { Some(RecoveryStrategy::Retry) } else { Some(RecoveryStrategy::Manual) },
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            max_retries: if retryable { Some(3) } else { None },
            suggested_backoff: if retryable { Some(Duration::from_secs(1)) } else { None },
        }
    }
}

impl From<tokio::task::JoinError> for RuntimeError {
    fn from(err: tokio::task::JoinError) -> Self {
        let retryable = !err.is_cancelled();
        
        Self {
            kind: if err.is_cancelled() { ErrorKind::Cancelled } else { ErrorKind::TaskError },
            message: "Task join error".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: if retryable { Some(RecoveryStrategy::Retry) } else { Some(RecoveryStrategy::Abort) },
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            max_retries: if retryable { Some(3) } else { None },
            suggested_backoff: if retryable { Some(Duration::from_secs(1)) } else { None },
        }
    }
}

impl From<tokio::time::error::Elapsed> for RuntimeError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::timeout_error(
            "Operation timed out",
            None,
            None,
        )
    }
}

impl From<tokio::sync::mpsc::error::SendError<()>> for RuntimeError {
    fn from(err: tokio::sync::mpsc::error::SendError<()>) -> Self {
        Self {
            kind: ErrorKind::ConcurrencyError,
            message: "Failed to send message through channel".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(3),
            suggested_backoff: Some(Duration::from_millis(100)),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for RuntimeError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Self {
            kind: ErrorKind::ConcurrencyError,
            message: "Failed to receive response".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(3),
            suggested_backoff: Some(Duration::from_millis(100)),
        }
    }
}

impl From<tokio::sync::AcquireError> for RuntimeError {
    fn from(err: tokio::sync::AcquireError) -> Self {
        Self {
            kind: ErrorKind::ResourceError,
            message: "Failed to acquire semaphore permit".to_string(),
            source: Some(err.to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Warning,
            recovery_strategy: Some(RecoveryStrategy::WaitAndRetry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            max_retries: Some(5),
            suggested_backoff: Some(Duration::from_secs(1)),
        }
    }
}

impl From<gaussflow_core::GaussFlowError> for RuntimeError {
    fn from(err: gaussflow_core::GaussFlowError) -> Self {
        let (kind, severity, retryable, recovery_strategy) = match &err {
            gaussflow_core::GaussFlowError::DagValidation { .. } => {
                (ErrorKind::ValidationError, ErrorSeverity::Error, false, RecoveryStrategy::Manual)
            }
            gaussflow_core::GaussFlowError::Execution { .. } => {
                (ErrorKind::TaskError, ErrorSeverity::Error, true, RecoveryStrategy::Retry)
            }
            gaussflow_core::GaussFlowError::Resource { .. } => {
                (ErrorKind::ResourceError, ErrorSeverity::Warning, true, RecoveryStrategy::WaitAndRetry)
            }
            gaussflow_core::GaussFlowError::Timeout { .. } => {
                (ErrorKind::TimeoutError, ErrorSeverity::Warning, true, RecoveryStrategy::Retry)
            }
            gaussflow_core::GaussFlowError::Cancelled { .. } => {
                (ErrorKind::Cancelled, ErrorSeverity::Info, false, RecoveryStrategy::Abort)
            }
            gaussflow_core::GaussFlowError::Serialization { .. } => {
                (ErrorKind::SerializationError, ErrorSeverity::Error, false, RecoveryStrategy::Manual)
            }
            gaussflow_core::GaussFlowError::Configuration { .. } => {
                (ErrorKind::ConfigurationError, ErrorSeverity::Error, false, RecoveryStrategy::Manual)
            }
            _ => {
                (ErrorKind::InternalError, ErrorSeverity::Error, false, RecoveryStrategy::Manual)
            }
        };

        Self {
            kind,
            message: err.to_string(),
            source: None,
            context: HashMap::new(),
            severity,
            recovery_strategy: Some(recovery_strategy),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            max_retries: if retryable { Some(3) } else { None },
            suggested_backoff: if retryable { Some(Duration::from_secs(1)) } else { None },
        }
    }
}

/// Error handling utilities
pub mod utils {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    /// Retry an operation with exponential backoff.
    pub async fn retry_with_backoff<F, Fut, T, E>(
        mut operation: F,
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
        E: Into<RuntimeError>,
    {
        let mut attempt = 0;
        let mut backoff = initial_backoff;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;
                    let runtime_error: RuntimeError = error.into();

                    if !runtime_error.is_retryable() || attempt > max_retries {
                        return Err(runtime_error);
                    }

                    // Sleep with exponential backoff
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }
    }

    /// Execute an operation with timeout.
    pub async fn with_timeout<F, Fut, T>(
        operation: F,
        timeout: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match tokio::time::timeout(timeout, operation()).await {
            Ok(result) => result,
            Err(_) => Err(RuntimeError::timeout_error(
                "Operation timed out",
                Some(timeout),
                None,
            )),
        }
    }

    /// Handle errors with custom recovery logic.
    pub async fn handle_error_with_recovery<F, Fut, T>(
        operation: F,
        error_handler: impl Fn(&RuntimeError) -> RecoveryStrategy,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match operation().await {
            Ok(result) => Ok(result),
            Err(error) => {
                let strategy = error_handler(&error);
                match strategy {
                    RecoveryStrategy::Retry => {
                        // Could implement retry logic here
                        Err(error)
                    }
                    RecoveryStrategy::WaitAndRetry => {
                        // Could implement wait and retry logic here
                        Err(error)
                    }
                    RecoveryStrategy::Skip => {
                        // Could implement skip logic here
                        Err(error)
                    }
                    RecoveryStrategy::Abort => Err(error),
                    RecoveryStrategy::Fallback => {
                        // Could implement fallback logic here
                        Err(error)
                    }
                    RecoveryStrategy::Manual => Err(error),
                }
            }
        }
    }
}
