//! Comprehensive benchmarks for GaussFlow runtime components

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use gaussflow_runtime::runtime::{
    metrics::RuntimeMetrics,
    optimized_executor::{OptimizedExecutor, OptimizedExecutorConfig},
    pool::MemoryPool,
    scheduler::Scheduler,
    steal_queue::{StealQueue, Stealer as QueueStealer, Worker as QueueWorker},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

/// Benchmark task spawning and execution
fn benchmark_task_spawning(c: &mut Criterion) {
    let mut group = c.benchmark_group("task_spawning");

    for num_tasks in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_tasks as u64));

        group.bench_with_input(
            BenchmarkId::new("spawn_tasks", num_tasks),
            &num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        num_workers: num_cpus::get(),
                        enable_batching: true,
                        enable_work_stealing: true,
                        ..Default::default()
                    };

                    let executor = OptimizedExecutor::new(config);
                    let handle = executor.handle();
                    let counter = Arc::new(AtomicUsize::new(0));

                    for _ in 0..num_tasks {
                        let counter = counter.clone();
                        let handle = handle.clone();

                        handle.spawn(async move {
                            // Simulate work
                            let _ = handle.submit_batch(1).await;
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }

                    // Run executor briefly to complete tasks
                    std::thread::spawn(move || {
                        executor.run();
                    });

                    // Wait for completion
                    let start = std::time::Instant::now();
                    while counter.load(Ordering::Relaxed) < num_tasks
                        && start.elapsed() < Duration::from_secs(5)
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

/// Benchmark memory pool operations
fn benchmark_memory_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool");

    for pool_size in [1_024, 10_240, 102_400, 1_048_576] {
        group.throughput(Throughput::Elements(pool_size as u64));

        group.bench_with_input(
            BenchmarkId::new("alloc_dealloc", pool_size),
            &pool_size,
            |b, &size| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        memory_pool_size: size as usize,
                        ..Default::default()
                    };

                    let executor = OptimizedExecutor::new(config);
                    let handle = executor.handle();
                    let pool = handle.memory_pool();

                    let mut buffers = Vec::with_capacity(100);

                    // Allocate buffers
                    for _ in 0..100 {
                        let mut buf = pool.get_with_capacity(size as usize);
                        buf.extend(std::iter::repeat(0).take(size as usize));
                        buffers.push(buf);
                    }

                    // Deallocate buffers
                    drop(buffers);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark scheduler performance
fn benchmark_scheduler(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler");

    for num_tasks in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_tasks as u64));

        group.bench_with_input(
            BenchmarkId::new("schedule_and_execute", num_tasks),
            &num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let metrics = Arc::new(RuntimeMetrics::new());
                    let scheduler = Scheduler::new(metrics);

                    // Spawn worker threads
                    let num_workers = num_cpus::get().max(1);
                    let mut handles = Vec::with_capacity(num_workers);

                    for _ in 0..num_workers {
                        let scheduler = scheduler.clone();
                        let handle = thread::spawn(move || {
                            let mut count = 0;
                            while count < num_tasks / num_workers {
                                if let Some(_) = scheduler.next_task() {
                                    count += 1;
                                } else {
                                    thread::yield_now();
                                }
                            }
                            count
                        });
                        handles.push(handle);
                    }

                    // Schedule tasks
                    for _ in 0..num_tasks {
                        scheduler.schedule(Box::new(|| {}), 0);
                    }

                    // Wait for workers to complete
                    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
                    assert_eq!(total, num_tasks);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark steal queue operations
fn benchmark_steal_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("steal_queue");

    for num_tasks in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_tasks as u64));

        group.bench_with_input(
            BenchmarkId::new("push_pop", num_tasks),
            &num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let worker = QueueWorker::new_fifo();

                    // Push tasks
                    for i in 0..num_tasks {
                        worker.push(i);
                    }

                    // Pop tasks
                    let mut count = 0;
                    while let Some(_) = worker.pop() {
                        count += 1;
                    }

                    assert_eq!(count, num_tasks);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("steal_parallel", num_tasks),
            &num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let num_workers = num_cpus::get().max(1);
                    let workers: Vec<_> =
                        (0..num_workers).map(|_| QueueWorker::new_fifo()).collect();
                    let stealers: Vec<_> = workers.iter().map(|w| w.stealer()).collect();

                    // Push tasks to first worker
                    for i in 0..num_tasks {
                        workers[0].push(i);
                    }

                    // Steal tasks in parallel
                    let counter = Arc::new(AtomicUsize::new(0));
                    let mut handles = Vec::new();

                    for stealer in stealers {
                        let counter = counter.clone();
                        let handle = thread::spawn(move || {
                            let mut count = 0;
                            while count < num_tasks / num_workers {
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

                    assert_eq!(counter.load(Ordering::Relaxed), num_tasks);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark batch processing
fn benchmark_batch_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");

    for batch_size in [10, 100, 1_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("process_batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        num_workers: num_cpus::get(),
                        enable_batching: true,
                        batch_size: size,
                        ..Default::default()
                    };

                    let executor = OptimizedExecutor::new(config);
                    let handle = executor.handle();
                    let counter = Arc::new(AtomicUsize::new(0));

                    // Submit batch
                    for _ in 0..size {
                        let counter = counter.clone();
                        let handle = handle.clone();

                        handle.spawn(async move {
                            let _ = handle.submit_batch(1).await;
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }

                    // Run executor
                    std::thread::spawn(move || {
                        executor.run();
                    });

                    // Wait for completion
                    let start = std::time::Instant::now();
                    while counter.load(Ordering::Relaxed) < size
                        && start.elapsed() < Duration::from_secs(5)
                    {
                        std::thread::yield_now();
                    }

                    assert_eq!(counter.load(Ordering::Relaxed), size);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent access patterns
fn benchmark_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_access");

    for num_threads in [2, 4, 8, 16] {
        group.throughput(Throughput::Elements(num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("concurrent_workers", num_threads),
            &num_threads,
            |b, &threads| {
                b.iter(|| {
                    let config = OptimizedExecutorConfig {
                        num_workers: threads,
                        enable_batching: true,
                        enable_work_stealing: true,
                        ..Default::default()
                    };

                    let executor = OptimizedExecutor::new(config);
                    let handle = executor.handle();
                    let counter = Arc::new(AtomicUsize::new(0));

                    let tasks_per_thread = 1000;
                    let total_tasks = threads * tasks_per_thread;

                    for _ in 0..total_tasks {
                        let counter = counter.clone();
                        let handle = handle.clone();

                        handle.spawn(async move {
                            let _ = handle.submit_batch(1).await;
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }

                    // Run executor
                    std::thread::spawn(move || {
                        executor.run();
                    });

                    // Wait for completion
                    let start = std::time::Instant::now();
                    while counter.load(Ordering::Relaxed) < total_tasks
                        && start.elapsed() < Duration::from_secs(10)
                    {
                        std::thread::yield_now();
                    }

                    assert_eq!(counter.load(Ordering::Relaxed), total_tasks);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(5));
    targets =
        benchmark_task_spawning,
        benchmark_memory_pool,
        benchmark_scheduler,
        benchmark_steal_queue,
        benchmark_batch_processing,
        benchmark_concurrent_access
);
criterion_main!(benches);
