//! Benchmarks for the runtime components.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use gaussflow_runtime::runtime::{
    metrics::RuntimeMetrics,
    optimized_executor::{OptimizedExecutor, OptimizedExecutorConfig},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn spawn_tasks(executor: &OptimizedExecutor, num_tasks: usize) -> Arc<AtomicUsize> {
    let handle = executor.handle();
    let counter = Arc::new(AtomicUsize::new(0));
    
    for _ in 0..num_tasks {
        let counter = counter.clone();
        let handle = handle.clone();
        
        handle.spawn(async move {
            // Simulate some work
            let _ = handle.submit_batch(1).await;
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }
    
    counter
}

fn benchmark_runtime(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime");
    
    for num_tasks in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*num_tasks as u64));
        
        group.bench_with_input(
            format!("spawn_{}_tasks", num_tasks),
            num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        num_workers: num_cpus::get(),
                        enable_batching: true,
                        enable_work_stealing: true,
                        ..Default::default()
                    };
                    
                    let executor = OptimizedExecutor::new(config);
                    let counter = spawn_tasks(&executor, num_tasks);
                    
                    // Run the executor in the current thread
                    std::thread::spawn(move || {
                        executor.run();
                    });
                    
                    // Wait for tasks to complete
                    let start = std::time::Instant::now();
                    while counter.load(Ordering::Relaxed) < num_tasks 
                        && start.elapsed() < Duration::from_secs(10) 
                    {
                        std::thread::yield_now();
                    }
                    
                    assert_eq!(counter.load(Ordering::Relaxed), num_tasks);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_memory_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool");
    
    for pool_size in [1_024, 10_240, 102_400].iter() {
        group.throughput(Throughput::Elements(*pool_size as u64));
        
        group.bench_with_input(
            format!("alloc_{}_bytes", pool_size),
            pool_size,
            |b, &size| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        memory_pool_size: *pool_size as usize,
                        ..Default::default()
                    };
                    
                    let executor = OptimizedExecutor::new(config);
                    let handle = executor.handle();
                    let pool = handle.memory_pool();
                    
                    let mut buffers = Vec::with_capacity(100);
                    
                    for _ in 0..100 {
                        let mut buf = pool.get_with_capacity(size as usize);
                        buf.extend(std::iter::repeat(0).take(size as usize));
                        buffers.push(buf);
                    }
                    
                    // Return buffers to pool
                    drop(buffers);
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_runtime, benchmark_memory_pool
);
criterion_main!(benches);
