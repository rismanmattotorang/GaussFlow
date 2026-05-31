use gaussflow_core::model::{ResourceSpec, NodeSpec, NodeType};
use gaussflow_runtime::planner::{Planner, RemoteExecutorConfig};
use std::time::Duration;
use rstest::rstest;

#[tokio::test]
async fn test_basic_resource_allocation() {
    // Create a planner with 2 CPU cores
    let planner = Planner::new(Some(2));
    
    // Create a CPU-bound resource spec
    let spec = ResourceSpec {
        gpu: false,
        remote: None,
        required_executor: None,
        timeout_ms: Some(1000),
        priority: None,
        memory_mb: None,
        cpu_millicores: None,
    };
    
    // Should be able to acquire 2 CPU permits
    let permit1 = planner.acquire_resources("test1", &spec).await.unwrap();
    let permit2 = planner.acquire_resources("test2", &spec).await.unwrap();
    
    // Third acquire should time out
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        planner.acquire_resources("test3", &spec)
    ).await;
    
    assert!(result.is_err(), "Should time out waiting for resources");
    
    // Release one permit
    drop(permit1);
    
    // Now we should be able to acquire again
    let permit3 = planner.acquire_resources("test3", &spec).await.unwrap();
    
    // Clean up
    drop(permit2);
    drop(permit3);
}

#[tokio::test]
async fn test_remote_execution() {
    let planner = Planner::new(Some(2));
    
    // Add a remote executor
    planner.add_remote_executor("test-executor".to_string(), RemoteExecutorConfig {
        endpoint: "http://example.com/execute".to_string(),
        api_key: Some("test-key".to_string()),
        max_concurrent: 5,
        current_usage: 0,
        timeout_ms: 5000,
    });
    
    // Create a spec that requires remote execution
    let spec = ResourceSpec {
        gpu: false,
        remote: Some(true),
        required_executor: None,
        timeout_ms: Some(1000),
        priority: None,
        memory_mb: None,
        cpu_millicores: None,
    };
    
    // Should be able to acquire remote execution slot
    let permit = planner.acquire_resources("remote-test", &spec).await;
    assert!(permit.is_ok(), "Should be able to acquire remote execution slot");
}

#[rstest]
#[case(true)]
#[case(false)]
async fn test_gpu_resource_allocation(#[case] use_gpu: bool) {
    let planner = Planner::new(Some(2));
    
    let spec = ResourceSpec {
        gpu: use_gpu,
        remote: None,
        required_executor: None,
        timeout_ms: Some(1000),
        priority: None,
        memory_mb: None,
        cpu_millicores: None,
    };
    
    let result = planner.acquire_resources("gpu-test", &spec).await;
    
    if use_gpu {
        assert!(result.is_ok(), "Should be able to acquire GPU resource");
    } else {
        assert!(result.is_ok(), "Should be able to acquire CPU resource");
    }
}
