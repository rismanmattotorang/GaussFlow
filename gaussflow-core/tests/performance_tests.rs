//! Performance tests for GaussFlow core components.
//!
//! QUARANTINED (roadmap Phase 0 → Phase 1): drifted from the current API (`TypeSafeDag::from_spec`,
//! `CheckpointManager::save_checkpoint`, `FileCheckpointStore::list_checkpoints`, etc.). Gated
//! behind the `legacy_tests` feature so the default build / CI stay green; to be rewritten in
//! Phase 1.
#![cfg(feature = "legacy_tests")]

use gaussflow_core::{
    checkpoint::{CheckpointManager, FileCheckpointStore},
    engine::{ExecutionEngine, NodeExecutor, ExecutionError},
    model::{NodeType, NodeSpec, WorkflowSpec, EdgeSpec, RetrySpec, Backoff},
    versioning::{InMemoryVersionManager, VersionedWorkflow, VersionManager},
    TypeSafeDag
};
use serde_json::json;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Performance-focused node executor
#[derive(Clone)]
struct PerformanceNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for PerformanceNodeExecutor {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: &serde_json::Value,
    ) -> Result<serde_json::Value, ExecutionError> {
        // Minimal work simulation
        tokio::time::sleep(Duration::from_millis(1)).await;
        
        Ok(json!({
            "status": "success",
            "node_id": node.id,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }
}

#[tokio::test]
async fn test_large_workflow_performance() {
    let _ = init_tracing();
    
    // Create a large workflow with many nodes
    let num_nodes = 100;
    let mut nodes = Vec::with_capacity(num_nodes);
    let mut connections = Vec::with_capacity(num_nodes - 1);
    
    // Create nodes in a linear chain
    for i in 0..num_nodes {
        nodes.push(NodeSpec {
            id: format!("node_{}", i),
            node_type: NodeType::DataProcessor,
            name: Some(format!("Node {}", i)),
            description: Some(format!("Performance test node {}", i)),
            config: Default::default(),
            metadata: Default::default(),
            params: Default::default(),
            retry: None,
            resources: None,
            timeout_ms: Some(5000),
            max_retries: Some(1),
            tags: vec!["performance".to_string()],
            enabled: true,
        });
        
        if i > 0 {
            connections.push(EdgeSpec {
                from: format!("node_{}", i - 1),
                to: format!("node_{}", i),
                on: "success".to_string(),
            });
        }
    }
    
    let workflow_spec = WorkflowSpec {
        name: "large-performance-test".to_string(),
        nodes,
        connections,
        settings: Default::default(),
    };
    
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec)
        .expect("Failed to create workflow DAG");
    
    // Create execution engine
    let engine = ExecutionEngine::new(
        8, // high concurrency
        Arc::new(PerformanceNodeExecutor),
        1, // minimal retries
        Duration::from_secs(30),
    );
    
    let start_time = Instant::now();
    let result = engine.execute(workflow_dag, json!({})).await.unwrap();
    let execution_time = start_time.elapsed();
    
    info!("Large workflow execution completed in {:?}", execution_time);
    info!("Processed {} nodes", result.len());
    
    // Performance assertions
    assert_eq!(result.len(), num_nodes);
    assert!(execution_time < Duration::from_secs(10), "Execution took too long: {:?}", execution_time);
}

#[tokio::test]
async fn test_parallel_workflow_performance() {
    let _ = init_tracing();
    
    // Create a workflow with parallel branches
    let num_branches = 10;
    let nodes_per_branch = 5;
    let mut nodes = Vec::new();
    let mut connections = Vec::new();
    
    // Start node
    nodes.push(NodeSpec {
        id: "start".to_string(),
        node_type: NodeType::DataProcessor,
        name: Some("Start".to_string()),
        description: Some("Start node".to_string()),
        config: Default::default(),
        metadata: Default::default(),
        params: Default::default(),
        retry: None,
        resources: None,
        timeout_ms: Some(5000),
        max_retries: Some(1),
        tags: vec!["start".to_string()],
        enabled: true,
    });
    
    // Create parallel branches
    for branch in 0..num_branches {
        for node in 0..nodes_per_branch {
            let node_id = format!("branch_{}_node_{}", branch, node);
            nodes.push(NodeSpec {
                id: node_id.clone(),
                node_type: NodeType::DataProcessor,
                name: Some(format!("Branch {} Node {}", branch, node)),
                description: Some(format!("Node in branch {}", branch)),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(5000),
                max_retries: Some(1),
                tags: vec![format!("branch_{}", branch)],
                enabled: true,
            });
            
            // Connect nodes within branch
            if node == 0 {
                connections.push(EdgeSpec {
                    from: "start".to_string(),
                    to: node_id,
                    on: "success".to_string(),
                });
            } else {
                connections.push(EdgeSpec {
                    from: format!("branch_{}_node_{}", branch, node - 1),
                    to: node_id,
                    on: "success".to_string(),
                });
            }
        }
    }
    
    // Final aggregation node
    nodes.push(NodeSpec {
        id: "aggregate".to_string(),
        node_type: NodeType::Aggregator,
        name: Some("Aggregate".to_string()),
        description: Some("Aggregate all branches".to_string()),
        config: Default::default(),
        metadata: Default::default(),
        params: Default::default(),
        retry: None,
        resources: None,
        timeout_ms: Some(5000),
        max_retries: Some(1),
        tags: vec!["aggregate".to_string()],
        enabled: true,
    });
    
    // Connect all branch ends to aggregate
    for branch in 0..num_branches {
        connections.push(EdgeSpec {
            from: format!("branch_{}_node_{}", branch, nodes_per_branch - 1),
            to: "aggregate".to_string(),
            on: "success".to_string(),
        });
    }
    
    let workflow_spec = WorkflowSpec {
        name: "parallel-performance-test".to_string(),
        nodes,
        connections,
        settings: Default::default(),
    };
    
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec)
        .expect("Failed to create workflow DAG");
    
    // Create execution engine with high concurrency
    let engine = ExecutionEngine::new(
        16, // very high concurrency for parallel execution
        Arc::new(PerformanceNodeExecutor),
        1,
        Duration::from_secs(30),
    );
    
    let start_time = Instant::now();
    let result = engine.execute(workflow_dag, json!({})).await.unwrap();
    let execution_time = start_time.elapsed();
    
    info!("Parallel workflow execution completed in {:?}", execution_time);
    info!("Processed {} nodes", result.len());
    
    // Performance assertions
    let expected_nodes = 1 + (num_branches * nodes_per_branch) + 1; // start + branch nodes + aggregate
    assert_eq!(result.len(), expected_nodes);
    assert!(execution_time < Duration::from_secs(5), "Parallel execution took too long: {:?}", execution_time);
}

#[tokio::test]
async fn test_checkpoint_performance() {
    let _ = init_tracing();
    
    // Create temporary directory for checkpoints
    let temp_dir = tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    
    let checkpoint_store = Arc::new(FileCheckpointStore::new(checkpoint_dir));
    let mut checkpoint_manager = CheckpointManager::new(
        checkpoint_store.clone(),
        "performance-test",
        true,
    );
    
    // Create a simple workflow
    let workflow_spec = WorkflowSpec {
        name: "checkpoint-performance-test".to_string(),
        nodes: vec![
            NodeSpec {
                id: "test_node".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Test Node".to_string()),
                description: Some("Test node for checkpointing".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(5000),
                max_retries: Some(1),
                tags: vec!["test".to_string()],
                enabled: true,
            },
        ],
        connections: vec![],
        settings: Default::default(),
    };
    
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec).unwrap();
    
    // Test checkpoint creation performance
    let start_time = Instant::now();
    
    // Create multiple checkpoints
    for i in 0..100 {
        let checkpoint_data = json!({
            "workflow_state": {
                "node_id": format!("test_node_{}", i),
                "status": "completed"
            },
            "execution_context": {
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "iteration": i
            }
        });
        
        checkpoint_manager.save_checkpoint(&checkpoint_data).await.unwrap();
    }
    
    let checkpoint_time = start_time.elapsed();
    info!("Created 100 checkpoints in {:?}", checkpoint_time);
    
    // Performance assertions
    assert!(checkpoint_time < Duration::from_secs(10), "Checkpoint creation took too long: {:?}", checkpoint_time);
    
    // Test checkpoint loading performance
    let start_time = Instant::now();
    let checkpoints = checkpoint_store.list_checkpoints("performance-test").await.unwrap();
    let load_time = start_time.elapsed();
    
    info!("Loaded {} checkpoints in {:?}", checkpoints.len(), load_time);
    assert!(load_time < Duration::from_secs(5), "Checkpoint loading took too long: {:?}", load_time);
}

#[tokio::test]
async fn test_versioning_performance() {
    let _ = init_tracing();
    
    let version_manager = InMemoryVersionManager::new();
    
    // Test version creation performance
    let start_time = Instant::now();
    
    for i in 0..1000 {
        let workflow_version = VersionedWorkflow {
            id: format!("workflow_{}", i),
            version: format!("1.0.{}", i),
            content: json!({
                "name": format!("Workflow {}", i),
                "nodes": vec![
                    {
                        "id": "node_1",
                        "type": "data_processor"
                    }
                ]
            }),
            metadata: HashMap::new(),
            created_at: chrono::Utc::now().to_rfc3339(),
            created_by: Some("performance_test".to_string()),
        };
        
        version_manager.save_version(&workflow_version).await.unwrap();
    }
    
    let save_time = start_time.elapsed();
    info!("Saved 1000 versions in {:?}", save_time);
    
    // Performance assertions
    assert!(save_time < Duration::from_secs(5), "Version saving took too long: {:?}", save_time);
    
    // Test version retrieval performance
    let start_time = Instant::now();
    
    for i in 0..1000 {
        let _version = version_manager.get_version(&format!("workflow_{}", i), &format!("1.0.{}", i)).await.unwrap();
    }
    
    let load_time = start_time.elapsed();
    info!("Loaded 1000 versions in {:?}", load_time);
    
    // Performance assertions
    assert!(load_time < Duration::from_secs(5), "Version loading took too long: {:?}", load_time);
}

#[tokio::test]
async fn test_memory_usage_performance() {
    let _ = init_tracing();
    
    // Test memory usage with large workflows
    let num_nodes = 1000;
    let mut nodes = Vec::with_capacity(num_nodes);
    
    for i in 0..num_nodes {
        nodes.push(NodeSpec {
            id: format!("large_node_{}", i),
            node_type: NodeType::DataProcessor,
            name: Some(format!("Large Node {}", i)),
            description: Some(format!("Large workflow node {}", i)),
            config: Default::default(),
            metadata: Default::default(),
            params: Default::default(),
            retry: None,
            resources: None,
            timeout_ms: Some(5000),
            max_retries: Some(1),
            tags: vec!["large".to_string()],
            enabled: true,
        });
    }
    
    let workflow_spec = WorkflowSpec {
        name: "memory-test".to_string(),
        nodes,
        connections: vec![],
        settings: Default::default(),
    };
    
    let start_time = Instant::now();
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec).unwrap();
    let creation_time = start_time.elapsed();
    
    info!("Created large workflow DAG with {} nodes in {:?}", num_nodes, creation_time);
    
    // Performance assertions
    assert!(creation_time < Duration::from_secs(1), "Large workflow creation took too long: {:?}", creation_time);
    assert_eq!(workflow_dag.graph.node_count(), num_nodes);
}

fn init_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
} 