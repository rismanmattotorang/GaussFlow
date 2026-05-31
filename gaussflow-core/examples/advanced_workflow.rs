//! Advanced workflow example demonstrating key GaussFlow features
//! 
//! This example showcases:
//! - Complex workflow orchestration
//! - Error handling and retries
//! - Checkpointing and recovery
//! - Version management
//! - Resource management
//! - Performance monitoring

use gaussflow_core::{
    checkpoint::{CheckpointManager, FileCheckpointStore},
    model::{NodeType, NodeSpec, WorkflowSpec, EdgeSpec, RetrySpec, Backoff, ResourceSpec},
    versioning::{VersionManager, VersionedWorkflow, InMemoryVersionManager},
    engine::{ExecutionEngine, NodeExecutor, ExecutionError, ExecutionContext},
    TypeSafeDag
};
use serde_json::json;
use std::sync::Arc;
use std::collections::HashMap;
use std::pin::Pin;
use std::future::Future;
use tokio::time::Duration;
use tracing::{info, error, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Custom node executor that simulates different types of nodes
#[derive(Clone)]
struct CustomNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for CustomNodeExecutor {
    fn execute<'a>(
        &'a self,
        node: &'a NodeSpec,
        input: &'a serde_json::Value,
        ctx: &'a ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let node_id = &node.id;
            info!(node_id, node_type = ?node.node_type, "Executing node");

            // Simulate work based on node type
            match node.node_type {
                NodeType::LlmCall => {
                    // Simulate LLM API call
                    let prompt = node.params.get("prompt")
                        .and_then(|p| p.as_str())
                        .unwrap_or("Default prompt");
                    
                    // Simulate API call delay with some variance
                    let delay = Duration::from_millis(50 + (rand::random::<u64>() % 100));
                    tokio::time::sleep(delay).await;
                    
                    // Simulate occasional failures
                    if rand::random::<f64>() < 0.1 {
                        return Err(ExecutionError::NodeExecutionFailed {
                            node_id: node_id.clone(),
                            error: "Simulated LLM API failure".to_string(),
                        });
                    }
                    
                    Ok(json!({ 
                        "status": "success", 
                        "output": format!("Generated response for prompt: {}", prompt),
                        "tokens_used": 42 + (rand::random::<u32>() % 100),
                        "model": node.params.get("model").and_then(|m| m.as_str()).unwrap_or("gpt-4"),
                        "latency_ms": delay.as_millis()
                    }))
                }
                NodeType::Agent => {
                    // Simulate agent processing with decision making
                    tokio::time::sleep(Duration::from_millis(100 + (rand::random::<u64>() % 200))).await;
                    
                    let sentiment = input.get("sentiment")
                        .and_then(|s| s.as_str())
                        .unwrap_or("neutral");
                    
                    let decision = match sentiment {
                        "positive" => "proceed",
                        "negative" => "review",
                        _ => "analyze_further"
                    };
                    
                    Ok(json!({
                        "status": "success",
                        "decision": decision,
                        "reason": format!("Sentiment analysis: {}", sentiment),
                        "confidence": 0.85 + (rand::random::<f64>() * 0.15)
                    }))
                }
                NodeType::Router => {
                    // Smart router based on input analysis
                    let decision = input.get("decision")
                        .and_then(|d| d.as_str())
                        .unwrap_or("default");
                    
                    let route = match decision {
                        "proceed" => "positive_processing",
                        "review" => "negative_processing", 
                        "analyze_further" => "further_analysis",
                        _ => "default_route"
                    };
                    
                    Ok(json!({
                        "status": "success",
                        "route": route,
                        "routing_logic": "sentiment_based"
                    }))
                }
                NodeType::DataProcessor => {
                    // Simulate data processing with different operations
                    let action = node.params.get("action")
                        .and_then(|a| a.as_str())
                        .unwrap_or("default");
                    
                    tokio::time::sleep(Duration::from_millis(75 + (rand::random::<u64>() % 150))).await;
                    
                    let processed_data = match action {
                        "enhance_positive" => {
                            json!({
                                "enhanced": true,
                                "sentiment_score": 0.9,
                                "recommendations": ["amplify", "share", "celebrate"]
                            })
                        }
                        "mitigate_negative" => {
                            json!({
                                "mitigated": true,
                                "sentiment_score": 0.3,
                                "actions_taken": ["review", "improve", "support"]
                            })
                        }
                        "analyze_further" => {
                            json!({
                                "analysis_complete": true,
                                "complexity_score": 0.7,
                                "requires_human_review": true
                            })
                        }
                        _ => json!({
                            "processed": true,
                            "action": action
                        })
                    };
                    
                    Ok(json!({
                        "status": "success",
                        "processed_data": processed_data,
                        "processing_time_ms": 75 + (rand::random::<u64>() % 150)
                    }))
                }
                NodeType::Conditional => {
                    // Conditional logic based on input
                    let condition = node.params.get("condition")
                        .and_then(|c| c.as_str())
                        .unwrap_or("default");
                    
                    let result = match condition {
                        "quality_check" => {
                            let quality_score = input.get("quality_score")
                                .and_then(|q| q.as_f64())
                                .unwrap_or(0.5);
                            
                            quality_score > 0.7
                        }
                        "sentiment_threshold" => {
                            let sentiment = input.get("sentiment")
                                .and_then(|s| s.as_str())
                                .unwrap_or("neutral");
                            
                            sentiment == "positive"
                        }
                        _ => true
                    };
                    
                    Ok(json!({
                        "status": "success",
                        "condition_met": result,
                        "condition_type": condition
                    }))
                }
                NodeType::Aggregator => {
                    // Aggregate results from multiple nodes
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    
                    let mut aggregated = HashMap::new();
                    aggregated.insert("total_nodes", input.as_object().map(|o| o.len()).unwrap_or(0));
                    aggregated.insert("successful_nodes", input.as_object().map(|o| 
                        o.values().filter(|v| v.get("status").and_then(|s| s.as_str()) == Some("success")).count()
                    ).unwrap_or(0));
                    aggregated.insert("aggregation_timestamp", chrono::Utc::now().to_rfc3339());
                    
                    Ok(json!({
                        "status": "success",
                        "aggregated_results": aggregated,
                        "aggregation_method": "count_based"
                    }))
                }
                _ => {
                    // Default handler for other node types
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Ok(json!({
                        "status": "success",
                        "message": format!("Processed by {:?} node", node.node_type),
                        "node_id": node_id
                    }))
                }
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), gaussflow_core::ExecutionError> {
    // Initialize tracing with more detailed configuration
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    
    info!("Starting advanced workflow example");

    // Create a checkpoint store with proper error handling
    let checkpoint_dir = "./checkpoints";
    std::fs::create_dir_all(checkpoint_dir).expect("Failed to create checkpoint directory");
    let checkpoint_store = Arc::new(FileCheckpointStore::new(checkpoint_dir));
    
    // Create a version manager
    let version_manager = Arc::new(InMemoryVersionManager::new());
    
    // Define a comprehensive workflow
    let workflow_spec = WorkflowSpec {
        name: "advanced-content-pipeline".to_string(),
        nodes: vec![
            NodeSpec {
                id: "content_generation".to_string(),
                node_type: NodeType::LlmCall,
                name: Some("Content Generation".to_string()),
                description: Some("Generate initial content using LLM".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "model": "gpt-4",
                    "prompt": "Write a comprehensive article about artificial intelligence and its impact on society",
                    "max_tokens": 1000,
                    "temperature": 0.7
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: Some(RetrySpec {
                    max_attempts: 3,
                    backoff: Backoff::Exponential,
                    timeout: Some(Duration::from_secs(60)),
                }),
                resources: Some(ResourceSpec {
                    cpu_millicores: 500,
                    memory_mb: 1024,
                    gpu: None,
                }),
                timeout_ms: Some(60000),
                max_retries: Some(3),
                tags: vec!["llm".to_string(), "content".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "sentiment_analysis".to_string(),
                node_type: NodeType::Agent,
                name: Some("Sentiment Analysis".to_string()),
                description: Some("Analyze content sentiment and tone".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "analysis_type": "sentiment",
                    "include_confidence": true,
                    "detect_emotions": true
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: Some(RetrySpec {
                    max_attempts: 2,
                    backoff: Backoff::Fixed,
                    timeout: Some(Duration::from_secs(30)),
                }),
                resources: Some(ResourceSpec {
                    cpu_millicores: 200,
                    memory_mb: 512,
                    gpu: None,
                }),
                timeout_ms: Some(30000),
                max_retries: Some(2),
                tags: vec!["analysis".to_string(), "sentiment".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "content_router".to_string(),
                node_type: NodeType::Router,
                name: Some("Content Router".to_string()),
                description: Some("Route content based on sentiment analysis".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({
                    "routing_strategy": "sentiment_based",
                    "default_route": "further_analysis"
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: None,
                resources: Some(ResourceSpec {
                    cpu_millicores: 100,
                    memory_mb: 256,
                    gpu: None,
                }),
                timeout_ms: Some(10000),
                max_retries: Some(1),
                tags: vec!["routing".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "positive_enhancement".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Positive Enhancement".to_string()),
                description: Some("Enhance positively received content".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "action": "enhance_positive",
                    "enhancement_level": "high"
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: Some(RetrySpec {
                    max_attempts: 2,
                    backoff: Backoff::Linear,
                    timeout: Some(Duration::from_secs(20)),
                }),
                resources: Some(ResourceSpec {
                    cpu_millicores: 300,
                    memory_mb: 768,
                    gpu: None,
                }),
                timeout_ms: Some(20000),
                max_retries: Some(2),
                tags: vec!["processing".to_string(), "enhancement".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "negative_mitigation".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Negative Mitigation".to_string()),
                description: Some("Mitigate negatively received content".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "action": "mitigate_negative",
                    "mitigation_strategy": "constructive"
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: Some(RetrySpec {
                    max_attempts: 2,
                    backoff: Backoff::Linear,
                    timeout: Some(Duration::from_secs(20)),
                }),
                resources: Some(ResourceSpec {
                    cpu_millicores: 300,
                    memory_mb: 768,
                    gpu: None,
                }),
                timeout_ms: Some(20000),
                max_retries: Some(2),
                tags: vec!["processing".to_string(), "mitigation".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "further_analysis".to_string(),
                node_type: NodeType::DataProcessor,
                name: Some("Further Analysis".to_string()),
                description: Some("Perform deeper analysis for neutral content".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "action": "analyze_further",
                    "analysis_depth": "comprehensive"
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: Some(RetrySpec {
                    max_attempts: 1,
                    backoff: Backoff::Fixed,
                    timeout: Some(Duration::from_secs(30)),
                }),
                resources: Some(ResourceSpec {
                    cpu_millicores: 400,
                    memory_mb: 1024,
                    gpu: None,
                }),
                timeout_ms: Some(30000),
                max_retries: Some(1),
                tags: vec!["analysis".to_string(), "deep".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "quality_check".to_string(),
                node_type: NodeType::Conditional,
                name: Some("Quality Check".to_string()),
                description: Some("Check if content meets quality standards".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "condition": "quality_check",
                    "threshold": 0.7
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: None,
                resources: Some(ResourceSpec {
                    cpu_millicores: 150,
                    memory_mb: 256,
                    gpu: None,
                }),
                timeout_ms: Some(15000),
                max_retries: Some(1),
                tags: vec!["quality".to_string(), "validation".to_string()],
                enabled: true,
            },
            NodeSpec {
                id: "final_aggregation".to_string(),
                node_type: NodeType::Aggregator,
                name: Some("Final Aggregation".to_string()),
                description: Some("Aggregate all processing results".to_string()),
                config: Default::default(),
                metadata: Default::default(),
                params: json!({ 
                    "aggregation_type": "comprehensive",
                    "include_metrics": true
                }).as_object().unwrap().clone().into_iter().collect(),
                retry: None,
                resources: Some(ResourceSpec {
                    cpu_millicores: 200,
                    memory_mb: 512,
                    gpu: None,
                }),
                timeout_ms: Some(10000),
                max_retries: Some(1),
                tags: vec!["aggregation".to_string(), "final".to_string()],
                enabled: true,
            },
        ],
        connections: vec![
            EdgeSpec {
                from: "content_generation".to_string(),
                to: "sentiment_analysis".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "sentiment_analysis".to_string(),
                to: "content_router".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "content_router".to_string(),
                to: "positive_enhancement".to_string(),
                on: "positive_processing".to_string(),
            },
            EdgeSpec {
                from: "content_router".to_string(),
                to: "negative_mitigation".to_string(),
                on: "negative_processing".to_string(),
            },
            EdgeSpec {
                from: "content_router".to_string(),
                to: "further_analysis".to_string(),
                on: "further_analysis".to_string(),
            },
            EdgeSpec {
                from: "positive_enhancement".to_string(),
                to: "quality_check".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "negative_mitigation".to_string(),
                to: "quality_check".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "further_analysis".to_string(),
                to: "quality_check".to_string(),
                on: "success".to_string(),
            },
            EdgeSpec {
                from: "quality_check".to_string(),
                to: "final_aggregation".to_string(),
                on: "condition_met".to_string(),
            },
        ],
        settings: Default::default(),
    };
    
    // Create workflow DAG
    let workflow_dag = TypeSafeDag::from_spec(&workflow_spec)
        .expect("Failed to create workflow DAG");
    
    info!("Created workflow DAG with {} nodes and {} edges", 
          workflow_dag.graph.node_count(), 
          workflow_dag.graph.edge_count());
    
    // Save workflow version
    let workflow_version = VersionedWorkflow {
        id: workflow_spec.name.clone(),
        version: "1.0.0".to_string(),
        content: serde_json::to_value(&workflow_spec)?,
        metadata: HashMap::new(),
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by: Some("advanced_example".to_string()),
    };
    
    version_manager.save_version(&workflow_version).await?;
    info!("Saved workflow version: {}", workflow_version.version);
    
    // Create checkpoint manager
    let mut checkpoint_manager = CheckpointManager::new(
        checkpoint_store.clone(),
        &workflow_spec.name,
        true,
    );
    
    // Create execution engine with comprehensive configuration
    let engine = ExecutionEngine::new(
        4, // concurrency - increased for better performance
        Arc::new(CustomNodeExecutor),
        3, // max retries
        Duration::from_secs(120), // task timeout - increased for complex workflows
    );
    
    info!("Starting workflow execution...");
    let start_time = std::time::Instant::now();
    
    // Execute workflow with initial input
    let initial_input = json!({
        "request_id": uuid::Uuid::new_v4().to_string(),
        "user_id": "example_user",
        "content_type": "article",
        "target_audience": "general",
        "priority": "high"
    });
    
    let result = engine.execute(workflow_dag.clone(), initial_input).await?;
    
    let execution_time = start_time.elapsed();
    info!("Workflow execution completed in {:?}", execution_time);
    
    // Display results
    println!("\n=== Workflow Execution Results ===");
    println!("Execution time: {:?}", execution_time);
    println!("Nodes executed: {}", result.len());
    
    for (node_id, node_result) in &result {
        println!("\nNode: {}", node_id);
        if let Some(status) = node_result.get("status").and_then(|s| s.as_str()) {
            println!("  Status: {}", status);
        }
        if let Some(message) = node_result.get("message").and_then(|m| m.as_str()) {
            println!("  Message: {}", message);
        }
    }
    
    // Verify checkpoint was created
    let checkpoints = checkpoint_store.list_checkpoints(&workflow_spec.name).await?;
    info!("Created {} checkpoints", checkpoints.len());
    
    // Test recovery from checkpoint
    if !checkpoints.is_empty() {
        info!("Testing checkpoint recovery...");
        let latest_checkpoint = checkpoints.last().unwrap();
        
        // Simulate recovery by loading checkpoint
        let checkpoint_data = checkpoint_store.load_checkpoint(&workflow_spec.name, &latest_checkpoint.id).await?;
        info!("Successfully loaded checkpoint: {}", latest_checkpoint.id);
        
        // Verify checkpoint data integrity
        assert!(checkpoint_data.contains_key("workflow_state"));
        assert!(checkpoint_data.contains_key("execution_context"));
    }
    
    println!("\n=== Example completed successfully ===");
    Ok(())
}
