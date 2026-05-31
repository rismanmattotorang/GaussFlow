//! Integration tests for the runtime components.
//!
//! QUARANTINED (roadmap Phase 0 → Phase 1): targets `gaussflow_runtime::runtime` — the parallel,
//! unfinished engine (`runtime/mod.rs` has a `todo!()`) that is not even declared as a module in
//! `lib.rs`, i.e. dead code per `docs/CODE_EVALUATION.md` §3.2. Gated behind `legacy_tests` so
//! default builds / CI stay green; will be reinstated or removed when Phase 1 decides the fate of
//! that engine. Inspect with: `cargo test -p gaussflow-runtime --features legacy_tests`.
#![cfg(feature = "legacy_tests")]

use gaussflow_runtime::runtime::{
    error::RuntimeError,
    metrics::RuntimeMetrics,
    optimized_executor::{OptimizedExecutor, OptimizedExecutorConfig},
    Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[test]
fn test_runtime_integration() -> Result<()> {
    // Create a new executor with default configuration
    let config = OptimizedExecutorConfig {
        num_workers: 4,
        enable_batching: true,
        enable_string_interning: true,
        enable_work_stealing: true,
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    
    // Create a counter to track task completion
    let counter = Arc::new(AtomicUsize::new(0));
    
    // Spawn multiple tasks
    for i in 0..1000 {
        let counter = counter.clone();
        let handle = handle.clone();
        
        handle.spawn(async move {
            // Simulate some work
            let result = handle.submit_batch(i).await?;
            assert_eq!(result, i * 2);
            
            // Increment the counter
            counter.fetch_add(1, Ordering::SeqCst);
            
            Ok::<_, RuntimeError>(())
        });
    }
    
    // Run the executor in a separate thread
    let handle = std::thread::spawn(move || {
        executor.run();
    });
    
    // Wait for tasks to complete
    let start = std::time::Instant::now();
    while counter.load(Ordering::SeqCst) < 1000 && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(10));
    }
    
    // Verify all tasks completed
    assert_eq!(counter.load(Ordering::SeqCst), 1000);
    
    // Clean up
    handle.thread().unpark();
    
    Ok(())
}

#[test]
fn test_memory_pooling() -> Result<()> {
    let config = OptimizedExecutorConfig {
        num_workers: 2,
        enable_memory_pooling: true,
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    
    // Spawn tasks that use the memory pool
    let counter = Arc::new(AtomicUsize::new(0));
    
    for _ in 0..100 {
        let counter = counter.clone();
        let pool = handle.memory_pool().clone();
        
        handle.spawn(async move {
            // Get a buffer from the pool
            let mut buffer = pool.get_with_capacity(1024);
            buffer.extend_from_slice(&[0u8; 1024]);
            
            // Do some work with the buffer
            let sum: u64 = buffer.iter().map(|&b| b as u64).sum();
            assert_eq!(sum, 0);
            
            // Return the buffer to the pool
            drop(buffer);
            
            counter.fetch_add(1, Ordering::SeqCst);
        });
    }
    
    // Run the executor
    let handle = std::thread::spawn(move || {
        executor.run();
    });
    
    // Wait for tasks to complete
    let start = std::time::Instant::now();
    while counter.load(Ordering::SeqCst) < 100 && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(10));
    }
    
    assert_eq!(counter.load(Ordering::SeqCst), 100);
    handle.thread().unpark();
    
    Ok(())
}
