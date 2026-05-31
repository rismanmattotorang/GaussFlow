//! Parallel processing example demonstrating concurrent workflow execution

use gaussflow_core::{
    engine::{ExecutionEngine, NodeExecutor, ExecutionError},
    model::{NodeType, NodeSpec, WorkflowSpec, EdgeSpec},
    TypeSafeDag
};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Node executor that tracks concurrent executions
#[derive(Clone)]
struct ParallelNodeExecutor {
    concurrent_executions: Arc<AtomicUsize>,
    max_concurrent: usize,
}

impl ParallelNodeExecutor {
    fn new(max_concurrent: usize) -> Self {
        Self {
            concurrent_executions: Arc::new(AtomicUsize::new(0)),
            max_concurrent,
        }
    }
    
    fn get_concurrent_count(&self) -> usize {
        self.concurrent_executions.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl NodeExecutor for ParallelNodeExecutor {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: &serde_json::Value,
    ) -> Result<serde_json::Value, ExecutionError> {
        // Increment concurrent execution counter
        let current = self.concurrent_executions.fetch_add(1, Ordering::Relaxed);
        info!("Node {} starting execution (concurrent: {})", node.id, current + 1);
        
        // Simulate work with variable duration
        let work_duration = std::time::Duration::from_millis(100 + (rand::random::<u64>() % 200));
        tokio::time::sleep(work_duration).await;
        
        // Decrement counter
        let current = self.concurrent_executions.fetch_sub(1, Ordering::Relaxed);
        info!("Node {} completed execution (concurrent: {})", node.id, current - 1);
        
        Ok(json!({
            "status": "success",
            "node_id": node.id,
            "execution_time_ms": work_duration.as_millis(),
            "max_concurrent_observed": self.max_concurrent
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), ExecutionError> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    
    info!("Starting parallel processing example");

    // Create workflow with parallel branches
    let workflow_spec = WorkflowSpec {
        name: "parallel-processing-example".to_string(),
        nodes: vec![
            // Start node
            NodeSpec {
                id: "start".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Start Node".to_string()),
                description: Some("Initial node that triggers parallel processing".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["start".to_string()],
                enabled: true,
            },
            // Parallel branch 1
            NodeSpec {
                id: "branch1_task1".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Branch 1 Task 1".to_string()),
                description: Some("First task in parallel branch 1".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["branch1".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "branch1_task2".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Branch 1 Task 2".to_string()),
                description: Some("Second task in parallel branch 1".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["branch1".to_string()],
                enabled: true,
            },
            // Parallel branch 2
            NodeSpec {
                id: "branch2_task1".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Branch 2 Task 1".to_string()),
                description: Some("First task in parallel branch 2".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["branch2".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "branch2_task2".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Branch 2 Task 2".to_string()),
                description: Some("Second task in parallel branch 2".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["branch2".to_string()],
                enabled: true,
            },
            // Aggregation node
            NodeSpec {
                id: "aggregate".to_string(),
                node_type: NodeType::Aggregator,
                name: Some("Aggregate Results".to_string()),
                description: Some("Aggregate results from parallel branches".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["aggregate".to_string()],
                enabled: true,
            },
        ],
        connections: vec![
            // Start parallel branches
            EdgeSpec {
                from: "start".to_string(),
                to: "branch1_task1".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "start".to_string(),
                to: "branch2_task1".to_string(),
                on: "success".to_string(),
            },
            // Branch 1 sequence
            EdgeSpec {
                from: "branch1_task1".to_string(),
                to: "branch1_task2".to_string(),
                on: "success".to_string(),
            },
            // Branch 2 sequence
            EdgeSpec {
                from: "branch2_task1".to_string(),
                to: "branch2_task2".to_string(),
                on: "success".to_string(),
            },
            // Aggregate results
            EdgeSpec {
                from: "branch1_task2".to_string(),
                to: "aggregate".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "branch2_task2".to_string(),
                to: "aggregate".to_string(),
                on: "success".to_string(),
            },
        ],
        settings: Default::default(),
    };
    
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec)
        .expect("Failed to create workflow DAG");
    
    // Create execution engine with high concurrency
    let max_concurrent = 8;
    let executor = ParallelNodeExecutor::new(max_concurrent);
    let engine = ExecutionEngine::new(
        max_concurrent, // high concurrency
        Arc::new(executor.clone()),
        3, // max retries
        std::time::Duration::from_secs(60), // task timeout
    );
    
    info!("Executing parallel workflow with max concurrency: {}", max_concurrent);
    let start_time = std::time::Instant::now();
    
    // Execute workflow
    let result = engine.execute(workflow_dag, json!({})).await?;
    
    let execution_time = start_time.elapsed();
    info!("Parallel workflow execution completed in {:?}", execution_time);
    
    println!("Parallel processing example completed!");
    println!("Execution time: {:?}", execution_time);
    println!("Maximum concurrent executions observed: {}", executor.get_concurrent_count());
    println!("Results: {:?}", result);
    
    Ok(())
} 