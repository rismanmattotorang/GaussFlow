# GaussFlow Core Examples

This directory contains comprehensive examples demonstrating various features and capabilities of the GaussFlow core library.

## Examples Overview

### 1. Simple Workflow (`simple_workflow.rs`)
**Purpose**: Basic introduction to GaussFlow workflow execution
- Demonstrates basic workflow creation and execution
- Shows simple node types and connections
- Minimal configuration for quick start

**Key Features**:
- Basic workflow specification
- Simple node executor implementation
- Linear workflow execution
- Basic error handling

**Usage**:
```bash
cargo run --example simple_workflow
```

### 2. Error Handling (`error_handling.rs`)
**Purpose**: Demonstrates robust error handling and retry mechanisms
- Shows how to handle node failures gracefully
- Demonstrates different retry strategies
- Illustrates fault-tolerant workflow execution

**Key Features**:
- Simulated node failures with configurable failure rates
- Exponential and fixed backoff strategies
- Comprehensive retry configuration
- Error recovery mechanisms

**Usage**:
```bash
cargo run --example error_handling
```

### 3. Parallel Processing (`parallel_processing.rs`)
**Purpose**: Shows concurrent workflow execution capabilities
- Demonstrates parallel branch execution
- Shows work-stealing and load balancing
- Illustrates high-concurrency scenarios

**Key Features**:
- Parallel workflow branches
- Concurrent execution tracking
- Work-stealing scheduler usage
- Performance monitoring

**Usage**:
```bash
cargo run --example parallel_processing
```

### 4. Advanced Workflow (`advanced_workflow.rs`)
**Purpose**: Comprehensive demonstration of advanced features
- Complex workflow orchestration
- Multiple node types and patterns
- Real-world scenarios

**Key Features**:
- Complex workflow with multiple node types
- Checkpointing and recovery
- Version management
- Resource management
- Performance monitoring
- Real-world content processing pipeline

**Usage**:
```bash
cargo run --example advanced_workflow
```

## Error Handling in Examples (NEW)

All examples now use the robust error handling system provided by GaussFlow:
- Errors are returned as `ExecutionError` or `GaussFlowError`.
- Each error carries severity and recovery strategy metadata.
- Examples show how to handle node failures, retries, and recovery.

**Example:**
```rust
#[tokio::main]
async fn main() -> Result<(), ExecutionError> {
    // ...
    let result = engine.execute(workflow_dag, json!({})).await;
    match result {
        Ok(output) => println!("Success: {:?}", output),
        Err(e) => {
            eprintln!("Workflow failed: {}", e);
            if let Some(strategy) = e.recovery_strategy() {
                println!("Recovery strategy: {}", strategy);
            }
        }
    }
    Ok(())
}
```

## Example Outputs (UPDATED)
- All examples print error details and recovery strategies on failure.
- Output includes error severity and context for debugging.

## Best Practices for Error Handling
- Always match on the error and inspect severity and recovery strategy.
- Use retries and backoff for transient errors.
- Log all errors with full context for observability.
- Prefer explicit error handling in custom node executors.

## Example Configurations

### Performance Tuning
Examples can be configured for different performance requirements:

```rust
// High-performance configuration
let engine = ExecutionEngine::new(
    16, // high concurrency
    Arc::new(CustomNodeExecutor),
    3, // retries
    Duration::from_secs(120), // timeout
);

// Resource-constrained configuration
let engine = ExecutionEngine::new(
    2, // low concurrency
    Arc::new(CustomNodeExecutor),
    1, // minimal retries
    Duration::from_secs(30), // shorter timeout
);
```

### Node Executor Customization
Examples show different node executor implementations:

```rust
// Simple executor
struct SimpleNodeExecutor;

// Performance-focused executor
struct PerformanceNodeExecutor;

// Fault-tolerant executor
struct FaultyNodeExecutor {
    failure_rate: f64,
}
```

## Best Practices Demonstrated

### 1. Error Handling
- Graceful degradation
- Retry with exponential backoff
- Circuit breaker patterns
- Comprehensive logging

### 2. Performance Optimization
- Parallel execution where possible
- Resource pooling
- Memory management
- Work-stealing for load balancing

### 3. Monitoring and Observability
- Detailed logging with tracing
- Performance metrics collection
- Resource usage monitoring
- Execution time tracking

### 4. Scalability
- Horizontal scaling patterns
- Vertical scaling considerations
- Resource management
- Concurrency control

## Extending Examples

### Adding New Node Types
```rust
#[async_trait::async_trait]
impl NodeExecutor for CustomNodeExecutor {
    async fn execute(
        &self,
        node: &NodeSpec,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, ExecutionError> {
        match node.node_type {
            NodeType::Custom => {
                // Custom node implementation
                Ok(json!({"status": "success"}))
            }
            _ => {
                // Default handling
                Ok(json!({"status": "success"}))
            }
        }
    }
}
```

### Custom Workflow Patterns
```rust
// Fan-out pattern
let mut connections = Vec::new();
for i in 0..num_branches {
    connections.push(EdgeSpec {
        from: "start".to_string(),
        to: format!("branch_{}", i),
        on: "success".to_string(),
    });
}

// Fan-in pattern
for i in 0..num_branches {
    connections.push(EdgeSpec {
        from: format!("branch_{}", i),
        to: "aggregate".to_string(),
        on: "success".to_string(),
    });
}
```

## Troubleshooting

### Common Issues

1. **Memory Usage**: Large workflows may require increased memory limits
2. **Concurrency**: Too many concurrent tasks may overwhelm the system
3. **Timeout**: Complex workflows may need longer timeouts
4. **Resource Limits**: Check system resource availability

### Debugging Tips

1. Enable detailed logging:
```rust
let subscriber = FmtSubscriber::builder()
    .with_max_level(Level::DEBUG)
    .finish();
```

2. Monitor resource usage:
```rust
// Add resource monitoring
let start = std::time::Instant::now();
// ... execution ...
let duration = start.elapsed();
println!("Execution time: {:?}", duration);
```

3. Use performance profiling:
```bash
cargo run --example <example> --features profiling
```

## Contributing

When adding new examples:

1. Follow the existing naming conventions
2. Include comprehensive documentation
3. Add appropriate error handling
4. Include performance considerations
5. Test with different configurations
6. Update this README

## Performance Benchmarks

Run performance tests to validate example performance:

```bash
# Run performance tests
cargo test --test performance_tests

# Run with specific test
cargo test --test performance_tests test_large_workflow_performance
```

## Next Steps

After running these examples:

1. Explore the API documentation
2. Try custom node implementations
3. Experiment with different workflow patterns
4. Integrate with your own applications
5. Contribute improvements and new examples 