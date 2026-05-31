//! Simple workflow example demonstrating basic GaussFlow features

use gaussflow_core::{
    engine::{ExecutionEngine, NodeExecutor, ExecutionError},
    model::{NodeType, NodeSpec, WorkflowSpec, EdgeSpec},
    TypeSafeDag
};
use serde_json::json;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Simple node executor for demonstration
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
        info!("Executing node: {}", node.id);
        
        // Simulate work
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        Ok(json!({
            "status": "success",
            "node_id": node.id,
            "message": format!("Processed by {:?} node", node.node_type)
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
    
    info!("Starting simple workflow example");

    // Create a simple workflow
    let workflow_spec = WorkflowSpec {
        name: "simple-example".to_string(),
        nodes: vec![
            NodeSpec {
                id: "start".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Start Node".to_string()),
                description: Some("Initial processing node".to_string()),
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
            NodeSpec {
                id: "process".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Process Node".to_string()),
                description: Some("Main processing node".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: Default::default(),
                retry: None,
                resources: None,
                timeout_ms: Some(30000),
                max_retries: Some(3),
                tags: vec!["process".to_string()],
                enabled: true,
            },
        ],
        connections: vec![
            EdgeSpec {
                from: "start".to_string(),
                to: "process".to_string(),
                on: "success".to_string(),
            },
        ],
        settings: Default::default(),
    };
    
    // Convert workflow spec to JSON and create DAG
    let workflow_json = serde_json::to_string(&workflow_spec)
        .expect("Failed to serialize workflow spec");
    let workflow_dag = TypeSafeDag::from_json(&workflow_json)
        .expect("Failed to create workflow DAG");
    
    // Create execution engine
    let engine = ExecutionEngine::new(
        2, // concurrency
        Arc::new(SimpleNodeExecutor),
        3, // max retries
        std::time::Duration::from_secs(30), // task timeout
    );
    
    // Execute workflow
    let result = engine.execute(workflow_dag, json!({})).await?;
    
    println!("Workflow execution completed successfully!");
    println!("Results: {:?}", result);
    
    Ok(())
} 