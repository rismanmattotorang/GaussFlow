//! Concurrent tests for GaussFlow runtime components.
//!
//! QUARANTINED (roadmap Phase 0 → Phase 1): targets `gaussflow_runtime::runtime` — the parallel,
//! unfinished engine that is not declared as a module in `lib.rs` (dead code per
//! `docs/CODE_EVALUATION.md` §3.2). Gated behind `legacy_tests` so default builds / CI stay
//! green; to be revisited when Phase 1 decides the fate of that engine.
#![cfg(feature = "legacy_tests")]

use gaussflow_runtime::runtime::{
    metrics::RuntimeMetrics,
    optimized_executor::{OptimizedExecutor, OptimizedExecutorConfig},
    scheduler::Scheduler,
    steal_queue::{StealQueue, Stealer as QueueStealer, Worker as QueueWorker},
    pool::MemoryPool,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;
use tokio::time::sleep;

/// Test concurrent task spawning and execution
#[tokio::test]
async fn test_concurrent_task_spawning() {
    let config = OptimizedExecutorConfig {
        num_workers: num_cpus::get(),
        enable_batching: true,
        enable_work_stealing: true,
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    let counter = Arc::new(AtomicUsize::new(0));
    
    // Spawn tasks concurrently from multiple threads
    let num_threads = 10;
    let tasks_per_thread = 1000;
    let mut handles = Vec::new();
    
    for _ in 0..num_threads {
        let handle = handle.clone();
        let counter = counter.clone();
        
        let thread_handle = thread::spawn(move || {
            for _ in 0..tasks_per_thread {
                let handle = handle.clone();
                let counter = counter.clone();
                
                handle.spawn(async move {
                    // Simulate work
                    sleep(Duration::from_millis(1)).await;
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
        
        handles.push(thread_handle);
    }
    
    // Wait for all threads to spawn tasks
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Run executor
    let executor_handle = thread::spawn(move || {
        executor.run();
    });
    
    // Wait for all tasks to complete
    let start = std::time::Instant::now();
    while counter.load(Ordering::Relaxed) < num_threads * tasks_per_thread 
        && start.elapsed() < Duration::from_secs(30) 
    {
        sleep(Duration::from_millis(10)).await;
    }
    
    let completed = counter.load(Ordering::Relaxed);
    assert_eq!(completed, num_threads * tasks_per_thread, 
               "Expected {} tasks, but only {} completed", 
               num_threads * tasks_per_thread, completed);
}

/// Test concurrent memory pool access
#[tokio::test]
async fn test_concurrent_memory_pool() {
    let config = OptimizedExecutorConfig {
        memory_pool_size: 1024 * 1024, // 1MB
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    let pool = handle.memory_pool();
    
    let num_threads = 8;
    let allocations_per_thread = 100;
    let mut handles = Vec::new();
    
    for _ in 0..num_threads {
        let pool = pool.clone();
        
        let handle = thread::spawn(move || {
            let mut buffers = Vec::new();
            
            for _ in 0..allocations_per_thread {
                let size = 1024 + (rand::random::<usize>() % 4096);
                let mut buf = pool.get_with_capacity(size);
                buf.extend(std::iter::repeat(0).take(size));
                buffers.push(buf);
            }
            
            // Deallocate all buffers
            drop(buffers);
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify pool is still functional
    let test_buf = pool.get_with_capacity(1024);
    assert_eq!(test_buf.capacity(), 1024);
}

/// Test concurrent scheduler access
#[tokio::test]
async fn test_concurrent_scheduler() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let scheduler = Scheduler::new(metrics);
    
    let num_workers = 4;
    let tasks_per_worker = 1000;
    let mut worker_handles = Vec::new();
    
    // Start worker threads
    for worker_id in 0..num_workers {
        let scheduler = scheduler.clone();
        
        let handle = thread::spawn(move || {
            let mut count = 0;
            while count < tasks_per_worker {
                if let Some(_) = scheduler.next_task() {
                    count += 1;
                } else {
                    thread::yield_now();
                }
            }
            count
        });
        
        worker_handles.push(handle);
    }
    
    // Schedule tasks from multiple threads
    let num_schedulers = 4;
    let mut scheduler_handles = Vec::new();
    
    for _ in 0..num_schedulers {
        let scheduler = scheduler.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..(num_workers * tasks_per_worker / num_schedulers) {
                scheduler.schedule(Box::new(|| {}), 0);
            }
        });
        
        scheduler_handles.push(handle);
    }
    
    // Wait for all schedulers to complete
    for handle in scheduler_handles {
        handle.join().unwrap();
    }
    
    // Wait for all workers to complete
    let total: usize = worker_handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, num_workers * tasks_per_worker);
}

/// Test concurrent steal queue operations
#[tokio::test]
async fn test_concurrent_steal_queue() {
    let num_workers = 8;
    let tasks_per_worker = 1000;
    
    // Create workers and stealers
    let workers: Vec<_> = (0..num_workers).map(|_| QueueWorker::new_fifo()).collect();
    let stealers: Vec<_> = workers.iter().map(|w| w.stealer()).collect();
    
    // Push tasks to first worker
    for i in 0..(num_workers * tasks_per_worker) {
        workers[0].push(i);
    }
    
    // Steal tasks in parallel
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    
    for (worker_id, stealer) in stealers.into_iter().enumerate() {
        let counter = counter.clone();
        
        let handle = thread::spawn(move || {
            let mut count = 0;
            while count < tasks_per_worker {
                if let crossbeam_deque::Steal::Success(_) = stealer.steal() {
                    count += 1;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all tasks to be stolen
    for handle in handles {
        handle.join().unwrap();
    }
    
    assert_eq!(counter.load(Ordering::Relaxed), num_workers * tasks_per_worker);
}

/// Test concurrent batch processing
#[tokio::test]
async fn test_concurrent_batch_processing() {
    let config = OptimizedExecutorConfig {
        num_workers: num_cpus::get(),
        enable_batching: true,
        batch_size: 100,
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    let counter = Arc::new(AtomicUsize::new(0));
    
    let num_batches = 10;
    let tasks_per_batch = 1000;
    let mut handles = Vec::new();
    
    // Submit batches concurrently
    for _ in 0..num_batches {
        let handle = handle.clone();
        let counter = counter.clone();
        
        let thread_handle = thread::spawn(move || {
            for _ in 0..tasks_per_batch {
                let handle = handle.clone();
                let counter = counter.clone();
                
                handle.spawn(async move {
                    let _ = handle.submit_batch(1).await;
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
        
        handles.push(thread_handle);
    }
    
    // Wait for all threads to submit tasks
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Run executor
    let executor_handle = thread::spawn(move || {
        executor.run();
    });
    
    // Wait for all tasks to complete
    let start = std::time::Instant::now();
    while counter.load(Ordering::Relaxed) < num_batches * tasks_per_batch 
        && start.elapsed() < Duration::from_secs(30) 
    {
        sleep(Duration::from_millis(10)).await;
    }
    
    let completed = counter.load(Ordering::Relaxed);
    assert_eq!(completed, num_batches * tasks_per_batch);
}

/// Test concurrent metrics collection
#[tokio::test]
async fn test_concurrent_metrics() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let num_threads = 10;
    let operations_per_thread = 1000;
    let mut handles = Vec::new();
    
    // Concurrently update metrics
    for _ in 0..num_threads {
        let metrics = metrics.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..operations_per_thread {
                metrics.increment_tasks_completed();
                metrics.record_task_duration(Duration::from_millis(1));
                metrics.increment_tasks_failed();
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify metrics
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.tasks_completed, num_threads * operations_per_thread);
    assert_eq!(snapshot.tasks_failed, num_threads * operations_per_thread);
}

/// Test stress test with mixed operations
#[tokio::test]
async fn test_stress_test() {
    let config = OptimizedExecutorConfig {
        num_workers: num_cpus::get(),
        enable_batching: true,
        enable_work_stealing: true,
        memory_pool_size: 1024 * 1024,
        ..Default::default()
    };
    
    let executor = OptimizedExecutor::new(config);
    let handle = executor.handle();
    let counter = Arc::new(AtomicUsize::new(0));
    
    let num_operations = 10000;
    let mut handles = Vec::new();
    
    // Mix of different operations
    for i in 0..num_operations {
        let handle = handle.clone();
        let counter = counter.clone();
        
        let handle = thread::spawn(move || {
            // Spawn task
            let handle = handle.clone();
            let counter = counter.clone();
            
            handle.spawn(async move {
                // Simulate work
                sleep(Duration::from_millis(rand::random::<u64>() % 10)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            });
        });
        
        handles.push(handle);
    }
    
    // Wait for all spawn operations
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Run executor
    let executor_handle = thread::spawn(move || {
        executor.run();
    });
    
    // Wait for completion
    let start = std::time::Instant::now();
    while counter.load(Ordering::Relaxed) < num_operations 
        && start.elapsed() < Duration::from_secs(60) 
    {
        sleep(Duration::from_millis(100)).await;
    }
    
    let completed = counter.load(Ordering::Relaxed);
    assert_eq!(completed, num_operations, 
               "Expected {} operations, but only {} completed", 
               num_operations, completed);
} 