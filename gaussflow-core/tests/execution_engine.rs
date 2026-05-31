//! Tests for the execution engine

use gaussflow_core::{
    engine::{ExecutionEngine, ExecutionError, NodeExecutor},
    model::{NodeSpec, NodeType, ResourceSpec, WorkflowSpec},
    TypeSafeDag,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

mod test_utils;
use test_utils::*;

struct TestNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for TestNodeExecutor {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: &serde_json::Value,
    ) -> Result<serde_json::Value, ExecutionError> {
        // Simulate some work
        if node.id == "slow_node" {
            sleep(Duration::from_millis(100)).await;
        }

        if node.id == "failing_node" {
            return Err(ExecutionError::NodeExecution("Node failed".to_string()));
        }

        Ok(json!({"node_id": node.id, "status": "success"}))
    }
}

#[tokio::test]
async fn test_linear_workflow_execution() {
    // Create a simple linear workflow: node1 -> node2 -> node3
    let workflow = create_test_workflow(
        "test-linear",
        vec![
            ("node1".to_string(), NodeType::LlmCall),
            ("node2".to_string(), NodeType::LlmCall),
            ("node3".to_string(), NodeType::LlmCall),
        ],
        vec![
            ("node1".to_string(), "node2".to_string(), "success".to_string()),
            ("node2".to_string(), "node3".to_string(), "success".to_string()),
        ],
    );

    let engine = ExecutionEngine::new(2, Arc::new(TestNodeExecutor), 3);
    let result = engine
        .execute(workflow, json!({}))
        .await
        .expect("Workflow execution failed");

    assert_eq!(result.len(), 4); // 3 nodes + _input
    assert!(result.get("node1").is_some());
    assert!(result.get("node2").is_some());
    assert!(result.get("node3").is_some());
}

#[tokio::test]
async fn test_parallel_execution() {
    // Create a workflow where two nodes can run in parallel
    //    node1
    //   /     \
    // node2  node3
    //   \     /
    //    node4
    let workflow = create_test_workflow(
        "test-parallel",
        vec![
            ("node1".to_string(), NodeType::LlmCall),
            ("node2".to_string(), NodeType::LlmCall),
            ("node3".to_string(), NodeType::LlmCall),
            ("node4".to_string(), NodeType::LlmCall),
        ],
        vec![
            ("node1".to_string(), "node2".to_string(), "success".to_string()),
            ("node1".to_string(), "node3".to_string(), "success".to_string()),
            ("node2".to_string(), "node4".to_string(), "success".to_string()),
            ("node3".to_string(), "node4".to_string(), "success".to_string()),
        ],
    );

    let start = std::time::Instant::now();
    let engine = ExecutionEngine::new(2, Arc::new(TestNodeExecutor), 3);
    engine
        .execute(workflow, json!({}))
        .await
        .expect("Workflow execution failed");
    let duration = start.elapsed();

    // The total execution time should be less than the sum of individual node times
    // if they run in parallel
    assert!(duration < std::time::Duration::from_millis(300));
}

#[tokio::test]
async fn test_resource_validation() {
    // Create a workflow with resource requirements
    let workflow = create_test_workflow(
        "test-resources",
        vec![("node1".to_string(), NodeType::LlmCall)],
        vec![],
    );

    // Create a node with resource requirements
    let mut workflow = workflow;
    if let Some(node) = workflow
        .graph
        .node_weights_mut()
        .next()
        .map(|n| n.clone())
    {
        let mut node = node;
        node.resources = Some(ResourceSpec {
            required_executor: Some("gpu".to_string()),
            ..Default::default()
        });
        let idx = workflow.graph.node_indices().next().unwrap();
        workflow.graph[idx] = node;
    }

    let engine = ExecutionEngine::new(2, Arc::new(TestNodeExecutor), 3);
    let result = engine.execute(workflow, json!({})).await;

    // Should fail because the required executor is not available
    assert!(matches!(
        result,
        Err(ExecutionError::ResourceUnavailable(_))
    ));
}

#[tokio::test]
async fn test_retry_logic() {
    // Create a workflow with a failing node that will be retried
    let workflow = create_test_workflow(
        "test-retry",
        vec![
            ("node1".to_string(), NodeType::LlmCall),
            ("failing_node".to_string(), NodeType::LlmCall),
        ],
        vec![(
            "node1".to_string(),
            "failing_node".to_string(),
            "success".to_string(),
        )],
    );

    let engine = ExecutionEngine::new(1, Arc::new(TestNodeExecutor), 2);
    let result = engine.execute(workflow, json!({})).await;

    // Should fail after retries
    assert!(matches!(
        result,
        Err(ExecutionError::NodeExecution(_))
    ));
}

#[tokio::test]
async fn test_timeout() {
    // Create a workflow with a slow node that will timeout
    let workflow = create_test_workflow(
        "test-timeout",
        vec![
            ("node1".to_string(), NodeType::LlmCall),
            ("slow_node".to_string(), NodeType::LlmCall),
        ],
        vec![(
            "node1".to_string(),
            "slow_node".to_string(),
            "success".to_string(),
        )],
    );

    // Set a very short timeout
    let mut workflow = workflow;
    if let Some(node) = workflow
        .graph
        .node_weights_mut()
        .find(|n| n.id == "slow_node")
        .map(|n| n.clone())
    {
        let mut node = node;
        node.resources = Some(ResourceSpec {
            timeout_ms: Some(10), // 10ms timeout
            ..Default::default()
        });
        let idx = workflow
            .graph
            .node_indices()
            .find(|i| workflow.graph[*i].id == "slow_node")
            .unwrap();
        workflow.graph[idx] = node;
    }

    let engine = ExecutionEngine::new(1, Arc::new(TestNodeExecutor), 1);
    let result = engine.execute(workflow, json!({})).await;

    // Should fail with timeout
    assert!(matches!(result, Err(ExecutionError::Timeout(_))));
}
