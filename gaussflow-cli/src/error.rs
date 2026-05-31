//! Comprehensive error handling for GaussFlow CLI.

use std::fmt;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use anyhow::{Context, Result as AnyhowResult};

/// Main error type for CLI operations with comprehensive error handling.
/// 
/// This error type provides:
/// - Detailed error categorization with context
/// - Structured metadata for debugging and monitoring
/// - Recovery strategy information
/// - Error severity levels
/// - User-friendly error messages
/// - Integration with core and runtime error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliError {
    /// The kind of error that occurred
    pub kind: CliErrorKind,
    /// The error message
    pub message: String,
    /// User-friendly error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_message: Option<String>,
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
    /// Suggested action for the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_action: Option<String>,
    /// Exit code for the CLI
    pub exit_code: i32,
}

impl CliError {
    /// Create a new CLI error.
    pub fn new(kind: CliErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            user_message: None,
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: None,
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: None,
            exit_code: 1,
        }
    }

    /// Create a new configuration error
    pub fn configuration_error(
        message: impl Into<String>,
        config_file: Option<String>,
        section: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(file) = config_file {
            context.insert("config_file".to_string(), file);
        }
        if let Some(sec) = section {
            context.insert("section".to_string(), sec);
        }

        Self {
            kind: CliErrorKind::Configuration,
            message: message.into(),
            user_message: Some("Configuration error. Please check your configuration file.".to_string()),
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Run 'gaussflow config show' to view current configuration".to_string()),
            exit_code: 1,
        }
    }

    /// Create a new file system error
    pub fn file_system_error(
        message: impl Into<String>,
        path: Option<String>,
        operation: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(p) = path {
            context.insert("path".to_string(), p);
        }
        if let Some(op) = operation {
            context.insert("operation".to_string(), op);
        }

        Self {
            kind: CliErrorKind::FileSystem,
            message: message.into(),
            user_message: Some("File system error occurred.".to_string()),
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Check file permissions and ensure the file exists".to_string()),
            exit_code: 1,
        }
    }

    /// Create a new network error
    pub fn network_error(
        message: impl Into<String>,
        url: Option<String>,
        status_code: Option<u16>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(u) = url {
            context.insert("url".to_string(), u);
        }
        if let Some(status) = status_code {
            context.insert("status_code".to_string(), status.to_string());
        }

        Self {
            kind: CliErrorKind::Network,
            message: message.into(),
            user_message: Some("Network error occurred.".to_string()),
            context,
            severity: ErrorSeverity::Warning,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            suggested_action: Some("Check your internet connection and try again".to_string()),
            exit_code: 1,
        }
    }

    /// Create a new authentication error
    pub fn authentication_error(
        message: impl Into<String>,
        method: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(m) = method {
            context.insert("method".to_string(), m);
        }

        Self {
            kind: CliErrorKind::Authentication,
            message: message.into(),
            user_message: Some("Authentication failed.".to_string()),
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Check your credentials and try again".to_string()),
            exit_code: 1,
        }
    }

    /// Create a new workflow execution error
    pub fn workflow_execution_error(
        message: impl Into<String>,
        workflow_id: Option<String>,
        node_id: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(wf_id) = workflow_id {
            context.insert("workflow_id".to_string(), wf_id);
        }
        if let Some(n_id) = node_id {
            context.insert("node_id".to_string(), n_id);
        }

        Self {
            kind: CliErrorKind::WorkflowExecution,
            message: message.into(),
            user_message: Some("Workflow execution failed.".to_string()),
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Retry),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: true,
            suggested_action: Some("Check the workflow definition and try again".to_string()),
            exit_code: 1,
        }
    }

    /// Create a new validation error
    pub fn validation_error(
        message: impl Into<String>,
        field: Option<String>,
        value: Option<String>,
    ) -> Self {
        let mut context = HashMap::new();
        if let Some(f) = field {
            context.insert("field".to_string(), f);
        }
        if let Some(v) = value {
            context.insert("value".to_string(), v);
        }

        Self {
            kind: CliErrorKind::Validation,
            message: message.into(),
            user_message: Some("Validation error occurred.".to_string()),
            context,
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Check the input values and try again".to_string()),
            exit_code: 1,
        }
    }

    /// Get the error kind.
    pub fn kind(&self) -> &CliErrorKind {
        &self.kind
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the user-friendly message.
    pub fn user_message(&self) -> &str {
        self.user_message.as_deref().unwrap_or(&self.message)
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

    /// Get the suggested action.
    pub fn suggested_action(&self) -> Option<&str> {
        self.suggested_action.as_deref()
    }

    /// Get the exit code.
    pub fn exit_code(&self) -> i32 {
        self.exit_code
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

    /// Set the user message.
    pub fn with_user_message(mut self, user_message: String) -> Self {
        self.user_message = Some(user_message);
        self
    }

    /// Set the suggested action.
    pub fn with_suggested_action(mut self, action: String) -> Self {
        self.suggested_action = Some(action);
        self
    }

    /// Set the exit code.
    pub fn with_exit_code(mut self, exit_code: i32) -> Self {
        self.exit_code = exit_code;
        self
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.severity, self.kind, self.message)?;
        
        if !self.context.is_empty() {
            write!(f, " (context: {:?})", self.context)?;
        }
        
        if let Some(ref action) = self.suggested_action {
            write!(f, " (suggestion: {})", action)?;
        }
        
        Ok(())
    }
}

impl std::error::Error for CliError {}

/// Categories of CLI errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CliErrorKind {
    /// Configuration error
    Configuration,
    /// File system error
    FileSystem,
    /// Network error
    Network,
    /// Authentication error
    Authentication,
    /// Authorization error
    Authorization,
    /// Workflow execution error
    WorkflowExecution,
    /// Validation error
    Validation,
    /// Database error
    Database,
    /// Cache error
    Cache,
    /// Server error
    Server,
    /// Internal error
    Internal,
    /// User input error
    UserInput,
    /// Timeout error
    Timeout,
    /// Cancellation error
    Cancelled,
}

impl fmt::Display for CliErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration => write!(f, "ConfigurationError"),
            Self::FileSystem => write!(f, "FileSystemError"),
            Self::Network => write!(f, "NetworkError"),
            Self::Authentication => write!(f, "AuthenticationError"),
            Self::Authorization => write!(f, "AuthorizationError"),
            Self::WorkflowExecution => write!(f, "WorkflowExecutionError"),
            Self::Validation => write!(f, "ValidationError"),
            Self::Database => write!(f, "DatabaseError"),
            Self::Cache => write!(f, "CacheError"),
            Self::Server => write!(f, "ServerError"),
            Self::Internal => write!(f, "InternalError"),
            Self::UserInput => write!(f, "UserInputError"),
            Self::Timeout => write!(f, "TimeoutError"),
            Self::Cancelled => write!(f, "CancelledError"),
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
    /// Abort the entire operation.
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

/// Convenience type alias for results in the CLI.
pub type Result<T> = std::result::Result<T, CliError>;

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
impl From<gaussflow_core::GaussFlowError> for CliError {
    fn from(err: gaussflow_core::GaussFlowError) -> Self {
        let (kind, severity, retryable, recovery_strategy, exit_code) = match &err {
            gaussflow_core::GaussFlowError::DagValidation { .. } => {
                (CliErrorKind::Validation, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            gaussflow_core::GaussFlowError::Execution { .. } => {
                (CliErrorKind::WorkflowExecution, ErrorSeverity::Error, true, RecoveryStrategy::Retry, 1)
            }
            gaussflow_core::GaussFlowError::Resource { .. } => {
                (CliErrorKind::Internal, ErrorSeverity::Warning, true, RecoveryStrategy::WaitAndRetry, 1)
            }
            gaussflow_core::GaussFlowError::Timeout { .. } => {
                (CliErrorKind::Timeout, ErrorSeverity::Warning, true, RecoveryStrategy::Retry, 1)
            }
            gaussflow_core::GaussFlowError::Cancelled { .. } => {
                (CliErrorKind::Cancelled, ErrorSeverity::Info, false, RecoveryStrategy::Abort, 130)
            }
            gaussflow_core::GaussFlowError::Serialization { .. } => {
                (CliErrorKind::Validation, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            gaussflow_core::GaussFlowError::Configuration { .. } => {
                (CliErrorKind::Configuration, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            gaussflow_core::GaussFlowError::Authentication { .. } => {
                (CliErrorKind::Authentication, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            gaussflow_core::GaussFlowError::Authorization { .. } => {
                (CliErrorKind::Authorization, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            _ => {
                (CliErrorKind::Internal, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
        };

        Self {
            kind,
            message: err.to_string(),
            user_message: None,
            context: HashMap::new(),
            severity,
            recovery_strategy: Some(recovery_strategy),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            suggested_action: None,
            exit_code,
        }
    }
}

impl From<gaussflow_runtime::runtime::RuntimeError> for CliError {
    fn from(err: gaussflow_runtime::runtime::RuntimeError) -> Self {
        let (kind, severity, retryable, recovery_strategy, exit_code) = match err.kind() {
            gaussflow_runtime::runtime::error::ErrorKind::TaskError => {
                (CliErrorKind::WorkflowExecution, ErrorSeverity::Error, true, RecoveryStrategy::Retry, 1)
            }
            gaussflow_runtime::runtime::error::ErrorKind::ResourceError => {
                (CliErrorKind::Internal, ErrorSeverity::Warning, true, RecoveryStrategy::WaitAndRetry, 1)
            }
            gaussflow_runtime::runtime::error::ErrorKind::TimeoutError => {
                (CliErrorKind::Timeout, ErrorSeverity::Warning, true, RecoveryStrategy::Retry, 1)
            }
            gaussflow_runtime::runtime::error::ErrorKind::Cancelled => {
                (CliErrorKind::Cancelled, ErrorSeverity::Info, false, RecoveryStrategy::Abort, 130)
            }
            gaussflow_runtime::runtime::error::ErrorKind::SerializationError => {
                (CliErrorKind::Validation, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            gaussflow_runtime::runtime::error::ErrorKind::ConfigurationError => {
                (CliErrorKind::Configuration, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
            _ => {
                (CliErrorKind::Internal, ErrorSeverity::Error, false, RecoveryStrategy::Manual, 1)
            }
        };

        Self {
            kind,
            message: err.to_string(),
            user_message: None,
            context: HashMap::new(),
            severity,
            recovery_strategy: Some(recovery_strategy),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            suggested_action: None,
            exit_code,
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::NotFound => CliErrorKind::FileSystem,
            std::io::ErrorKind::PermissionDenied => CliErrorKind::FileSystem,
            std::io::ErrorKind::TimedOut => CliErrorKind::Timeout,
            _ => CliErrorKind::FileSystem,
        };

        let retryable = matches!(err.kind(), 
            std::io::ErrorKind::TimedOut | 
            std::io::ErrorKind::WouldBlock |
            std::io::ErrorKind::Interrupted
        );

        Self {
            kind,
            message: err.to_string(),
            user_message: Some("File system error occurred.".to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: if retryable { Some(RecoveryStrategy::Retry) } else { Some(RecoveryStrategy::Manual) },
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            suggested_action: Some("Check file permissions and ensure the file exists".to_string()),
            exit_code: 1,
        }
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            kind: CliErrorKind::Validation,
            message: err.to_string(),
            user_message: Some("Invalid JSON format.".to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Check the JSON syntax and try again".to_string()),
            exit_code: 1,
        }
    }
}

impl From<reqwest::Error> for CliError {
    fn from(err: reqwest::Error) -> Self {
        let kind = if err.is_timeout() {
            CliErrorKind::Timeout
        } else if err.is_connect() {
            CliErrorKind::Network
        } else {
            CliErrorKind::Network
        };

        let retryable = err.is_timeout() || err.is_connect();

        Self {
            kind,
            message: err.to_string(),
            user_message: Some("Network request failed.".to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Warning,
            recovery_strategy: if retryable { Some(RecoveryStrategy::Retry) } else { Some(RecoveryStrategy::Manual) },
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable,
            suggested_action: Some("Check your internet connection and try again".to_string()),
            exit_code: 1,
        }
    }
}

impl From<anyhow::Error> for CliError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            kind: CliErrorKind::Internal,
            message: err.to_string(),
            user_message: Some("An unexpected error occurred.".to_string()),
            context: HashMap::new(),
            severity: ErrorSeverity::Error,
            recovery_strategy: Some(RecoveryStrategy::Manual),
            timestamp: SystemTime::now(),
            correlation_id: None,
            retryable: false,
            suggested_action: Some("Please report this issue with the error details".to_string()),
            exit_code: 1,
        }
    }
}

/// Error handling utilities for CLI operations.
pub mod utils {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;
    use console::style;

    /// Display an error with proper formatting and styling.
    pub fn display_error(error: &CliError) {
        let severity_color = match error.severity() {
            ErrorSeverity::Info => style("ℹ").blue(),
            ErrorSeverity::Warning => style("⚠").yellow(),
            ErrorSeverity::Error => style("✗").red(),
            ErrorSeverity::Critical => style("💥").red().bold(),
        };

        eprintln!("{} {}", severity_color, style(error.user_message()).red());
        
        if let Some(action) = error.suggested_action() {
            eprintln!("{} {}", style("💡").blue(), style(action).cyan());
        }

        if !error.context.is_empty() {
            eprintln!("{} Context: {:?}", style("🔍").blue(), error.context);
        }

        if let Some(correlation_id) = &error.correlation_id {
            eprintln!("{} Correlation ID: {}", style("🆔").blue(), correlation_id);
        }
    }

    /// Retry an operation with exponential backoff.
    pub async fn retry_with_backoff<F, Fut, T>(
        mut operation: F,
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut backoff = initial_backoff;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;
                    
                    if !error.is_retryable() || attempt > max_retries {
                        return Err(error);
                    }

                    eprintln!("{} Retrying in {:?} (attempt {}/{})", 
                        style("🔄").yellow(), backoff, attempt, max_retries);

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
            Err(_) => Err(CliError::new(
                CliErrorKind::Timeout,
                format!("Operation timed out after {:?}", timeout),
            ).with_exit_code(124)),
        }
    }

    /// Handle errors with custom recovery logic.
    pub async fn handle_error_with_recovery<F, Fut, T>(
        operation: F,
        error_handler: impl Fn(&CliError) -> RecoveryStrategy,
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
                        eprintln!("{} Retrying operation...", style("🔄").yellow());
                        Err(error)
                    }
                    RecoveryStrategy::WaitAndRetry => {
                        eprintln!("{} Waiting before retry...", style("⏳").yellow());
                        Err(error)
                    }
                    RecoveryStrategy::Skip => {
                        eprintln!("{} Skipping operation...", style("⏭").yellow());
                        Err(error)
                    }
                    RecoveryStrategy::Abort => {
                        eprintln!("{} Aborting operation...", style("🛑").red());
                        Err(error)
                    }
                    RecoveryStrategy::Fallback => {
                        eprintln!("{} Using fallback...", style("🔄").yellow());
                        Err(error)
                    }
                    RecoveryStrategy::Manual => {
                        eprintln!("{} Manual intervention required...", style("👤").yellow());
                        Err(error)
                    }
                }
            }
        }
    }

    /// Convert anyhow error to CLI error with context.
    pub fn anyhow_to_cli_error(err: anyhow::Error, context: &str) -> CliError {
        CliError::new(
            CliErrorKind::Internal,
            format!("{}: {}", context, err),
        ).with_user_message(format!("Failed to {}", context.to_lowercase()))
    }

    /// Create a user-friendly error message.
    pub fn create_user_friendly_message(error: &CliError) -> String {
        match error.kind() {
            CliErrorKind::Configuration => {
                "Configuration error. Please check your configuration file.".to_string()
            }
            CliErrorKind::FileSystem => {
                "File system error. Please check file permissions and paths.".to_string()
            }
            CliErrorKind::Network => {
                "Network error. Please check your internet connection.".to_string()
            }
            CliErrorKind::Authentication => {
                "Authentication failed. Please check your credentials.".to_string()
            }
            CliErrorKind::WorkflowExecution => {
                "Workflow execution failed. Please check the workflow definition.".to_string()
            }
            CliErrorKind::Validation => {
                "Validation error. Please check the input values.".to_string()
            }
            CliErrorKind::Timeout => {
                "Operation timed out. Please try again.".to_string()
            }
            CliErrorKind::Cancelled => {
                "Operation was cancelled.".to_string()
            }
            _ => {
                "An unexpected error occurred. Please try again.".to_string()
            }
        }
    }
} 