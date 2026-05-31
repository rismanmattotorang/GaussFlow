//! Benchmarks for the work-stealing scheduler.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use gaussflow_runtime::runtime::{
    metrics::RuntimeMetrics,
    scheduler::Scheduler,
    steal_queue::{StealQueue, Stealer as QueueStealer, Worker as QueueWorker},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

fn benchmark_scheduler(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler");
    
    for num_tasks in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*num_tasks as u64));
        
        group.bench_with_input(
            format!("schedule_{}_tasks", num_tasks),
            num_tasks,
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
                            while count < num_tasks {
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

fn benchmark_steal_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("steal_queue");
    
    for num_tasks in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*num_tasks as u64));
        
        group.bench_with_input(
            format!("push_pop_{}_tasks", num_tasks),
            num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let worker = QueueWorker::new_fifo();
                    let stealer = worker.stealer();
                    
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
            format!("steal_{}_tasks", num_tasks),
            num_tasks,
            |b, &num_tasks| {
                b.iter(|| {
                    let num_workers = num_cpus::get().max(1);
                    let workers: Vec<_> = (0..num_workers).map(|_| QueueWorker::new_fifo()).collect();
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
                                if let Steal::Success(_) = stealer.steal() {
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

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_scheduler, benchmark_steal_queue
);
criterion_main!(benches);
