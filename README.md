# GaussFlow

GaussFlow is a **high-performance, type-safe DAG engine** for orchestrating multi-LLM and agentic AI workflows. It provides a robust foundation for building complex AI pipelines with advanced execution, resource management, and observability features.

**Status: Production-ready. All core, advanced, and enterprise features are implemented and tested. All tests passing. Performance targets met.**

## Key Features

### Advanced Execution Engine
- **Type-Safe DAG Management**: Compile-time type safety with runtime validation (**Implemented**)
- **Resource-Aware Scheduling**: CPU/GPU/RAM allocation with priority-based execution (**Implemented**)
- **Distributed Execution**: Support for Kubernetes, HPC, and edge computing (**Implemented**)
- **Stream Processing**: Real-time data processing capabilities (**Implemented**)
- **Batch Processing**: Efficient batch operation handling (**Implemented**)

### Advanced Resource Management
- **GPU Acceleration**: Native support for LLM inference (**Implemented**)
- **Resource Isolation**: Containerized execution with resource limits (**Implemented**)
- **Priority-Based Scheduling**: Customizable task prioritization (**Implemented**)
- **Load Balancing**: Intelligent resource allocation (**Implemented**)
- **Auto-Scaling**: Dynamic resource adjustment (**Implemented**)

### Enterprise-Grade Features
- **RBAC Security**: Role-based access control (**Implemented**)
- **Audit Logging**: Detailed execution history (**Implemented**)
- **SLA Monitoring**: Performance guarantees (**Implemented**)
- **Compliance**: Regulatory compliance hooks (**Implemented**)
- **Data Lineage**: Provenance tracking (**Implemented**)

### Advanced Node Types
- **LLM Integration**: Multi-provider support with routing (**Implemented**)
- **Agent Nodes**: Autonomous decision-making (**Implemented**)
- **Ensemble Nodes**: Parallel execution and aggregation (**Implemented**)
- **Router Nodes**: Intelligent routing strategies (**Implemented**)
- **Subgraph Nodes**: Nested workflow execution (**Implemented**)
- **Custom Executors**: Extendable node implementations (**Implemented**)

### State Management
- **Distributed Checkpointing**: Consistent state snapshots (**Implemented**)
- **Time-Travel Debugging**: Historical state access (**Implemented**)
- **State Compaction**: Efficient storage optimization (**Implemented**)
- **Recovery Strategies**: Graceful degradation (**Implemented**)
- **Version Control**: Workflow version management (**Implemented**)

## Error Handling (NEW)

GaussFlow provides a comprehensive error handling system with:
- **Structured error enums** (`GaussFlowError`, `ExecutionError`, etc.)
- **Severity levels** (`Info`, `Warning`, `Error`, `Critical`)
- **Recovery strategies** (`Retry`, `WaitAndRetry`, `Skip`, `Abort`, etc.)
- **Contextual metadata** for debugging and observability

**Example:**
```rust
use gaussflow_core::error::{GaussFlowError, ErrorSeverity, RecoveryStrategy};

fn handle_error(err: GaussFlowError) {
    match err.severity() {
        ErrorSeverity::Warning => println!("Warning: {}", err),
        ErrorSeverity::Error | ErrorSeverity::Critical => {
            if let Some(strategy) = err.recovery_strategy() {
                println!("Recovery strategy: {}", strategy);
            }
            eprintln!("Error: {}", err);
        }
        _ => {}
    }
}
```

## Example Usage (UPDATED)

```rust
use gaussflow_core::{
    engine::{ExecutionEngine, NodeExecutor, ExecutionError},
    model::{NodeSpec, WorkflowSpec, NodeType, EdgeSpec},
    TypeSafeDag
};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
struct SimpleNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for SimpleNodeExecutor {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: &serde_json::Value,
        _context: &gaussflow_core::engine::ExecutionContext,
    ) -> Result<serde_json::Value, ExecutionError> {
        Ok(json!({"status": "success", "node_id": node.id}))
    }
}

#[tokio::main]
async fn main() -> Result<(), ExecutionError> {
    let workflow_spec = WorkflowSpec { /* ... */ };
    let workflow_json = serde_json::to_string(&workflow_spec).unwrap();
    let workflow_dag = TypeSafeDag::from_json(&workflow_json).unwrap();
    let engine = ExecutionEngine::new(2, Arc::new(SimpleNodeExecutor), 3, std::time::Duration::from_secs(30));
    let result = engine.execute(workflow_dag, json!({})).await?;
    println!("Results: {:?}", result);
    Ok(())
}
```

## Running Examples, Benchmarks, and Tests

```bash
# Run an example
cargo run --example simple_workflow

# Run all tests
cargo test --all-features

# Run all benchmarks
cargo bench
```

## Troubleshooting

- **Unused warnings**: These are safe to ignore for now; they do not affect correctness.
- **Compilation errors**: Ensure you are using Rust 1.70+ and have all dependencies installed.
- **Resource errors**: Check system resource limits and adjust workflow settings as needed.
- **Timeouts**: Increase node or workflow timeouts for complex or long-running tasks.

## Best Practices
- Use structured error handling and recovery strategies for robust workflows.
- Monitor resource usage and tune concurrency for optimal performance.
- Use distributed checkpointing for fault tolerance.
- Write custom node executors for advanced use cases.
- Leverage tracing and metrics for observability.

## Getting Started

### Prerequisites
- Rust 1.70 or later
- Cargo (Rust's package manager)
- Optional dependencies:
  - CUDA for GPU acceleration
  - Kubernetes for container orchestration
  - Vault for secrets management

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/gaussflow.git
cd gaussflow

# Build core components
cargo build --release

# Build with GPU support
cargo build --release --features "gpu"

# Build with Kubernetes support
cargo build --release --features "k8s"

# Run tests
cargo test --all-features

# Run example workflow
cargo run --example advanced_workflow
```

### Python Bindings & CLI
- Python bindings are available via PyO3 (`gaussflow-py` crate).
- CLI tools are available in the `gaussflow-cli` crate for workflow management and execution.

## Core Concepts

### Advanced Workflow Definition

Workflows are defined using JSON or the Rust API. Here's an example of a complex workflow (fields updated to match implementation):

```json
{
  "metadata": {
    "name": "advanced-pipeline",
    "version": "1.0.0",
    "description": "Multi-LLM processing pipeline",
    "labels": { "environment": "production", "team": "ai" }
  },
  "nodes": [
    {
      "id": "input-processor",
      "type": "llm_call",
      "model": {
        "provider": "openai",
        "model": "gpt-4",
        "parameters": {
          "temperature": 0.2,
          "max_tokens": 2000
        }
      },
      "resources": {
        "cpu_cores": 2,
        "gpu_count": 0,
        "memory_mb": 4096,
        "memory": "4GB",
        "concurrency": 1,
        "timeout_ms": 30000,
        "priority": 5,
        "remote": false
      },
      "retry": {
        "max_attempts": 3,
        "backoff": "exponential",
        "timeout": 30000
      },
      "timeout_ms": 30000,
      "priority": 5,
      "metadata": {
        "team": "ai",
        "project": "nlp"
      }
    },
    {
      "id": "analysis",
      "type": "agent",
      "agent_type": "sentiment_analyzer",
      "resources": {
        "cpu_cores": 1,
        "gpu_count": 1,
        "memory_mb": 8192,
        "memory": "8GB",
        "concurrency": 1,
        "timeout_ms": 30000,
        "priority": 5,
        "remote": false
      }
    }
  ],
  "connections": [
    {
      "from": "input-processor",
      "to": "analysis",
      "on": "success",
      "condition": "${input.score > 0.7}",
      "metadata": {
        "type": "success_path"
      }
    }
  ],
  "settings": {
    "concurrency": 8,
    "fail_fast": false,
    "resume": true,
    "checkpoint_interval_ms": 60000,
    "resource_limits": {
      "cpu_cores": 16,
      "gpu_count": 2,
      "memory_mb": 32768,
      "memory": "32GB"
    },
    "retry_policy": {
      "global_max_attempts": 5,
      "backoff_strategy": "exponential"
    },
    "execution_strategy": "parallel",
    "priority_class": "high"
  }
}
```

### Advanced Workflow Execution

```rust
use gaussflow_core::{
    ExecutionEngine, WorkflowSpec, NodeSpec, NodeType, 
    metrics::init_tracing, checkpoint::DistributedCheckpointStore, ResourceSpec
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    init_tracing(Some("gaussflow"));
    
    // Load workflow spec
    let workflow_json = std::fs::read_to_string("workflow.json")?;
    let spec: WorkflowSpec = serde_json::from_str(&workflow_json)?;
    
    // Create distributed checkpoint store
    let checkpoint_store = Arc::new(DistributedCheckpointStore::new(
        "surreal://localhost:8000",
        "gaussflow_db"
    ));
    
    // Create execution engine with advanced settings
    let engine = ExecutionEngine::builder()
        .concurrency(16)
        .max_retries(5)
        .retry_backoff("exponential")
        .resource_limits(ResourceSpec {
            cpu_cores: 32,
            gpu_count: 4,
            memory_mb: 65536,
            memory: "64GB".to_string(),
            concurrency: 1,
            timeout_ms: 60000,
            priority: 10,
            remote: false,
            required_executor: None,
            cpu_millicores: 32000,
            retry_attempts: 5,
            retry_delay_ms: 1000,
            affinity: None,
            labels: Default::default(),
        })
        .priority_class("high")
        .build()?;
    
    // Execute workflow with monitoring
    let result = engine.execute_with_monitoring(
        spec,
        serde_json::json!({}),
        |metrics| {
            println!("Current metrics: {:#?}", metrics);
        }
    ).await?;
    
    println!("Workflow completed with result: {:#?}", result);
    Ok(())
}
```

## Advanced Features

### Distributed Checkpointing

Use distributed checkpointing for reliable state management:

```rust
let checkpoint_manager = DistributedCheckpointManager::new(
    "surreal://localhost:8000",
    "gaussflow_db",
    "workflow-123",
    true // auto-save
);

// Create checkpoint with metadata
let checkpoint = checkpoint_manager.create_checkpoint(
    WorkflowStatus::Running,
    Some("node-456".to_string()),
    node_results,
    Some("Processing phase 1".to_string()),
    Some(HashMap::from([
        ("team".to_string(), "ai".to_string()),
        ("environment".to_string(), "production".to_string())
    ]))
).await?;

// Resume from checkpoint
let result = checkpoint_manager.resume_from_checkpoint(
    &checkpoint.id,
    |progress| {
        println!("Checkpoint recovery progress: {}%", progress);
    }
).await?;
```

### Version Control

```

```
