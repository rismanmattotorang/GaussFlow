//! Comprehensive error handling example for GaussFlow.
//!
//! This example demonstrates:
//! - Error categorization and severity levels
//! - Recovery strategies and retry mechanisms
//! - Context preservation and metadata
//! - Error propagation and conversion
//! - User-friendly error messages
//! - Proper error handling patterns

use gaussflow_core::{
    GaussFlowError, DagError, ResourceError, ExecutionError, PolicyError,
    ErrorSeverity, RecoveryStrategy, ErrorContext,
    TypeSafeDag, DagNode, DagEdge,
};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

/// Example workflow node that can fail in different ways
#[derive(Debug, Clone)]
struct ExampleNode {
    id: String,
    failure_mode: FailureMode,
    retry_count: u32,
}

#[derive(Debug, Clone)]
enum FailureMode {
    None,
    ResourceExhaustion,
    Timeout,
    Validation,
    Network,
    Internal,
}

impl ExampleNode {
    fn new(id: String, failure_mode: FailureMode) -> Self {
        Self {
            id,
            failure_mode,
            retry_count: 0,
        }
    }

    async fn execute(&mut self, input: &Value) -> Result<Value, GaussFlowError> {
        self.retry_count += 1;
        
        match &self.failure_mode {
            FailureMode::None => {
                // Simulate successful execution
                Ok(serde_json::json!({
                    "node_id": self.id,
                    "status": "success",
                    "output": format!("Processed input: {}", input),
                    "attempt": self.retry_count
                }))
            }
            FailureMode::ResourceExhaustion => {
                // Simulate resource exhaustion
                Err(GaussFlowError::resource(
                    "CPU and memory resources exhausted",
                    Some("compute".to_string()),
                    Some("8 cores, 16GB RAM".to_string()),
                    Some("2 cores, 4GB RAM".to_string()),
                ))
            }
            FailureMode::Timeout => {
                // Simulate timeout
                sleep(Duration::from_millis(100)).await;
                Err(GaussFlowError::timeout(
                    "Node execution timed out",
                    Some(Duration::from_secs(30)),
                    Some("data_processing".to_string()),
                ))
            }
            FailureMode::Validation => {
                // Simulate validation error
                Err(GaussFlowError::dag_validation(
                    "Input data format is invalid",
                    Some(self.id.clone()),
                    None,
                ))
            }
            FailureMode::Network => {
                // Simulate network error
                Err(GaussFlowError::Storage {
                    message: "Network connection failed".to_string(),
                    storage_type: Some("network".to_string()),
                    path: Some("https://api.example.com".to_string()),
                    operation: Some("GET".to_string()),
                    recovery_strategy: Some(RecoveryStrategy::Retry),
                })
            }
            FailureMode::Internal => {
                // Simulate internal error
                Err(GaussFlowError::Internal {
                    message: "Unexpected internal error occurred".to_string(),
                    component: Some("example_node".to_string()),
                    recovery_strategy: Some(RecoveryStrategy::Manual),
                })
            }
        }
    }
}

/// Error handling utilities
struct ErrorHandler;

impl ErrorHandler {
    /// Handle errors with appropriate recovery strategies
    async fn handle_error(error: &GaussFlowError, context: &ErrorContext) -> Result<(), GaussFlowError> {
        println!("=== Error Handling ===");
        println!("Error: {}", error);
        println!("Severity: {}", error.severity());
        println!("Retryable: {}", error.is_retryable());
        
        if let Some(strategy) = error.recovery_strategy() {
            println!("Recovery Strategy: {}", strategy);
        }
        
        println!("Context: {:?}", context);
        println!();

        match error {
            GaussFlowError::Resource { .. } => {
                println!("🔄 Resource error detected - attempting recovery...");
                sleep(Duration::from_secs(2)).await;
                println!("✅ Resource recovered");
                Ok(())
            }
            GaussFlowError::Timeout { .. } => {
                println!("⏰ Timeout error detected - will retry with backoff...");
                Ok(())
            }
            GaussFlowError::DagValidation { .. } => {
                println!("❌ Validation error - manual intervention required");
                Err(error.clone())
            }
            GaussFlowError::Internal { .. } => {
                println!("💥 Internal error - logging for investigation");
                Err(error.clone())
            }
            _ => {
                println!("⚠️ Unknown error type - using default handling");
                Ok(())
            }
        }
    }

    /// Retry operation with exponential backoff
    async fn retry_with_backoff<F, Fut>(
        mut operation: F,
        max_retries: u32,
        initial_backoff: Duration,
    ) -> Result<Value, GaussFlowError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Value, GaussFlowError>>,
    {
        let mut attempt = 0;
        let mut backoff = initial_backoff;

        loop {
            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        println!("✅ Operation succeeded after {} retries", attempt);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    attempt += 1;
                    
                    if !error.is_retryable() || attempt > max_retries {
                        println!("❌ Operation failed after {} attempts", attempt);
                        return Err(error);
                    }

                    println!("🔄 Retrying in {:?} (attempt {}/{})", backoff, attempt, max_retries);
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                }
            }
        }
    }

    /// Create user-friendly error message
    fn create_user_message(error: &GaussFlowError) -> String {
        match error {
            GaussFlowError::Resource { message, .. } => {
                format!("Resource allocation failed: {}. Please try again later.", message)
            }
            GaussFlowError::Timeout { message, .. } => {
                format!("Operation timed out: {}. Please try again.", message)
            }
            GaussFlowError::DagValidation { message, .. } => {
                format!("Workflow validation failed: {}. Please check your workflow definition.", message)
            }
            GaussFlowError::Execution { message, .. } => {
                format!("Execution failed: {}. Please check your workflow configuration.", message)
            }
            GaussFlowError::Authentication { message, .. } => {
                format!("Authentication failed: {}. Please check your credentials.", message)
            }
            GaussFlowError::Authorization { message, .. } => {
                format!("Authorization failed: {}. Please check your permissions.", message)
            }
            _ => {
                format!("An unexpected error occurred: {}. Please contact support.", error)
            }
        }
    }
}

/// Demonstrate error conversion and propagation
async fn demonstrate_error_conversion() {
    println!("=== Error Conversion Examples ===");

    // Convert from serde_json::Error
    let json_error = serde_json::from_str::<Value>("invalid json");
    if let Err(e) = json_error {
        let gaussflow_error: GaussFlowError = e.into();
        println!("JSON Error -> GaussFlow Error: {}", gaussflow_error);
        println!("Severity: {}", gaussflow_error.severity());
        println!("Retryable: {}", gaussflow_error.is_retryable());
        println!();
    }

    // Convert from std::io::Error
    let io_error = std::fs::File::open("nonexistent_file.txt");
    if let Err(e) = io_error {
        let gaussflow_error: GaussFlowError = e.into();
        println!("IO Error -> GaussFlow Error: {}", gaussflow_error);
        println!("Severity: {}", gaussflow_error.severity());
        println!("Retryable: {}", gaussflow_error.is_retryable());
        println!();
    }

    // Convert from DagError
    let dag_error = DagError::CycleDetected {
        cycle_path: Some(vec!["A".to_string(), "B".to_string(), "A".to_string()]),
    };
    let gaussflow_error: GaussFlowError = dag_error.into();
    println!("DAG Error -> GaussFlow Error: {}", gaussflow_error);
    println!("Severity: {}", gaussflow_error.severity());
    println!("Retryable: {}", gaussflow_error.is_retryable());
    println!();
}

/// Demonstrate error context and metadata
async fn demonstrate_error_context() {
    println!("=== Error Context Examples ===");

    let mut context = ErrorContext::with_correlation_id("corr-12345".to_string());
    context = context
        .with_metadata("user_id".to_string(), "user-123".to_string())
        .with_metadata("session_id".to_string(), "session-456".to_string())
        .with_metadata("workflow_id".to_string(), "workflow-789".to_string());

    println!("Error Context: {:?}", context);
    println!();

    // Create error with context
    let mut error = GaussFlowError::execution(
        "Node processing failed",
        Some("node-1".to_string()),
        Some("workflow-1".to_string()),
    );
    
    let mut details = HashMap::new();
    details.insert("input_size".to_string(), "1024".to_string());
    details.insert("processing_time".to_string(), "5.2s".to_string());
    details.insert("memory_usage".to_string(), "512MB".to_string());
    
    error = error.with_context(details);
    
    println!("Error with context: {:?}", error);
    println!();
}

/// Demonstrate recovery strategies
async fn demonstrate_recovery_strategies() {
    println!("=== Recovery Strategy Examples ===");

    let mut node = ExampleNode::new("test-node".to_string(), FailureMode::ResourceExhaustion);
    let input = serde_json::json!({"data": "test"});

    // Demonstrate retry with backoff
    println!("Testing retry with backoff for resource error...");
    let result = ErrorHandler::retry_with_backoff(
        || async { node.execute(&input).await },
        3,
        Duration::from_millis(100),
    ).await;

    match result {
        Ok(_) => println!("✅ Retry succeeded"),
        Err(e) => println!("❌ Retry failed: {}", e),
    }
    println!();

    // Test timeout error
    let mut timeout_node = ExampleNode::new("timeout-node".to_string(), FailureMode::Timeout);
    println!("Testing timeout error handling...");
    let result = timeout_node.execute(&input).await;
    
    match result {
        Ok(_) => println!("✅ Timeout test succeeded"),
        Err(e) => {
            println!("⏰ Timeout error: {}", e);
            println!("User message: {}", ErrorHandler::create_user_message(&e));
        }
    }
    println!();

    // Test validation error (non-retryable)
    let mut validation_node = ExampleNode::new("validation-node".to_string(), FailureMode::Validation);
    println!("Testing validation error handling...");
    let result = validation_node.execute(&input).await;
    
    match result {
        Ok(_) => println!("✅ Validation test succeeded"),
        Err(e) => {
            println!("❌ Validation error: {}", e);
            println!("User message: {}", ErrorHandler::create_user_message(&e));
        }
    }
    println!();
}

/// Demonstrate error severity levels
async fn demonstrate_error_severity() {
    println!("=== Error Severity Examples ===");

    let errors = vec![
        GaussFlowError::cancelled("User cancelled operation".to_string(), Some("Ctrl+C".to_string())),
        GaussFlowError::resource(
            "Temporary resource shortage".to_string(),
            Some("memory".to_string()),
            Some("8GB".to_string()),
            Some("6GB".to_string()),
        ),
        GaussFlowError::execution(
            "Critical workflow execution failed".to_string(),
            Some("critical-node".to_string()),
            Some("production-workflow".to_string()),
        ),
        GaussFlowError::Policy {
            message: "Security policy violation".to_string(),
            policy_id: Some("security-policy-001".to_string()),
            violation_type: Some("data_access".to_string()),
            severity: Some(gaussflow_core::PolicyViolationSeverity::Critical),
            recovery_strategy: Some(RecoveryStrategy::Abort),
        },
    ];

    for error in errors {
        println!("Error: {}", error);
        println!("Severity: {}", error.severity());
        println!("Retryable: {}", error.is_retryable());
        if let Some(strategy) = error.recovery_strategy() {
            println!("Recovery: {}", strategy);
        }
        println!("User message: {}", ErrorHandler::create_user_message(&error));
        println!();
    }
}

/// Main example function
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 GaussFlow Error Handling Example");
    println!("=====================================\n");

    // Demonstrate error conversion
    demonstrate_error_conversion().await;

    // Demonstrate error context
    demonstrate_error_context().await;

    // Demonstrate recovery strategies
    demonstrate_recovery_strategies().await;

    // Demonstrate error severity levels
    demonstrate_error_severity().await;

    println!("✅ Error handling example completed successfully!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_error_conversion() {
        let json_error = serde_json::from_str::<Value>("invalid json");
        assert!(json_error.is_err());
        
        if let Err(e) = json_error {
            let gaussflow_error: GaussFlowError = e.into();
            assert_eq!(gaussflow_error.severity(), ErrorSeverity::Error);
            assert!(!gaussflow_error.is_retryable());
        }
    }

    #[tokio::test]
    async fn test_error_context() {
        let context = ErrorContext::with_correlation_id("test-correlation".to_string());
        assert_eq!(context.correlation_id, Some("test-correlation".to_string()));
    }

    #[tokio::test]
    async fn test_recovery_strategies() {
        let mut node = ExampleNode::new("test".to_string(), FailureMode::ResourceExhaustion);
        let input = serde_json::json!({"test": "data"});
        
        let result = node.execute(&input).await;
        assert!(result.is_err());
        
        if let Err(error) = result {
            assert_eq!(error.severity(), ErrorSeverity::Warning);
            assert!(error.is_retryable());
        }
    }
} 