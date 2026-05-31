//! Task executor for the runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use parking_lot::Mutex;

use crate::runtime::error::RuntimeError;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::scheduler::{Scheduler, Task};
use crate::runtime::Result;

/// Configuration for the executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Number of worker threads to spawn.
    pub num_workers: usize,
    /// Stack size for each worker thread.
    pub stack_size: usize,
    /// Maximum number of tasks that can be queued.
    pub queue_size: usize,
    /// Maximum number of tasks that can be stolen at once.
    pub steal_batch_size: usize,
    /// Maximum number of times to retry stealing before yielding.
    pub max_steal_retries: usize,
    /// Duration to sleep when no work is available.
    pub sleep_duration: Duration,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get().max(1),
            stack_size: 2 * 1024 * 1024, // 2MB stack
            queue_size: 10_000,
            steal_batch_size: 32,
            max_steal_retries: 16,
            sleep_duration: Duration::from_micros(100),
        }
    }
}

/// A task that can be scheduled by the executor.
type BoxedTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A handle to the executor that can be used to spawn tasks.
#[derive(Clone)]
pub struct ExecutorHandle {
    /// The global task queue.
    global_queue: Arc<Injector<BoxedTask>>,
    /// The worker-local queues.
    workers: Arc<Mutex<Vec<Worker<BoxedTask>>>>,
    /// The metrics collector.
    metrics: Arc<RuntimeMetrics>,
}

impl ExecutorHandle {
    /// Spawn a new task onto the executor.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.global_queue.push(Box::pin(future));
    }

    /// Spawn a blocking task onto the executor.
    pub fn spawn_blocking<F, R>(&self, f: F) -> impl Future<Output = R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (sender, receiver) = futures::channel::oneshot::channel();
        
        self.spawn(async move {
            let result = tokio::task::block_in_place(f);
            let _ = sender.send(result);
        });
        
        async {
            receiver.await.expect("Blocking task panicked")
        }
    }
}

/// The executor that runs tasks.
pub struct Executor {
    /// The global task queue.
    global_queue: Arc<Injector<BoxedTask>>,
    /// The worker-local queues.
    workers: Arc<Mutex<Vec<Worker<BoxedTask>>>>,
    /// The metrics collector.
    metrics: Arc<RuntimeMetrics>,
    /// The configuration.
    config: ExecutorConfig,
}

impl Executor {
    /// Create a new executor with the given configuration.
    pub fn new(config: ExecutorConfig, metrics: Arc<RuntimeMetrics>) -> Self {
        let global_queue = Arc::new(Injector::new());
        let workers = Arc::new(Mutex::new(Vec::new()));
        
        Self {
            global_queue,
            workers,
            metrics,
            config,
        }
    }
    
    /// Get a handle to the executor that can be used to spawn tasks.
    pub fn handle(&self) -> ExecutorHandle {
        ExecutorHandle {
            global_queue: self.global_queue.clone(),
            workers: self.workers.clone(),
            metrics: self.metrics.clone(),
        }
    }
    
    /// Start the executor and run tasks.
    pub fn run(self) {
        let num_workers = self.config.num_workers;
        let workers = self.workers.clone();
        
        // Create worker threads
        for worker_id in 0..num_workers {
            let global_queue = self.global_queue.clone();
            let workers = workers.clone();
            let metrics = self.metrics.clone();
            let config = self.config.clone();
            
            let builder = thread::Builder::new()
                .name(format!("worker-{worker_id}"))
                .stack_size(config.stack_size);
            
            builder.spawn(move || {
                let worker = Worker::new_fifo();
                let stealer = worker.stealer();
                
                // Register this worker
                {
                    let mut workers = workers.lock();
                    workers.push(worker);
                }
                
                // Main worker loop
                loop {
                    // Try to find work to do
                    if let Some(task) = Self::find_work(&stealer, &global_queue, &workers, &config, &metrics) {
                        // Run the task
                        metrics.task_started();
                        let start = Instant::now();
                        
                        // Execute the task
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let waker = noop_waker();
                            let mut cx = Context::from_waker(&waker);
                            
                            let mut task = task;
                            while let Poll::Pending = task.as_mut().poll(&mut cx) {
                                // Keep polling until the task is ready
                                thread::yield_now();
                            }
                        }));
                        
                        // Record the task completion
                        let duration = start.elapsed();
                        metrics.task_completed(duration);
                    } else {
                        // No work available, sleep for a bit
                        thread::sleep(config.sleep_duration);
                    }
                }
            }).unwrap();
        }
    }
    
    /// Find work to do, either from the local queue, global queue, or by stealing from other workers.
    fn find_work(
        local_stealer: &Stealer<BoxedTask>,
        global_queue: &Arc<Injector<BoxedTask>>,
        workers: &Arc<Mutex<Vec<Worker<BoxedTask>>>>,
        config: &ExecutorConfig,
        metrics: &RuntimeMetrics,
    ) -> Option<BoxedTask> {
        // Try to pop from the local queue
        if let Steal::Success(task) = local_stealer.steal() {
            metrics.work_stolen();
            return Some(task);
        }
        
        // Try to pop from the global queue
        if let Some(task) = global_queue.steal() {
            return Some(task);
        }
        
        // Try to steal from other workers
        let workers = workers.lock();
        let num_workers = workers.len();
        
        if num_workers <= 1 {
            return None;
        }
        
        // Start from a random worker to avoid contention
        let start = fastrand::usize(..num_workers);
        
        for i in 0..num_workers {
            let idx = (start + i) % num_workers;
            
            // Don't try to steal from ourselves
            if idx == local_stealer as *const _ as usize % num_workers {
                continue;
            }
            
            // Try to steal a batch of tasks
            if let Steal::Success(task) = workers[idx].stealer().steal_batch(local_stealer) {
                metrics.work_stolen();
                return Some(task);
            }
        }
        
        None
    }
}

/// Create a waker that does nothing.
fn noop_waker() -> Waker {
    use std::task::{RawWaker, RawWakerVTable};
    
    unsafe fn noop(_: *const ()) {}
    unsafe fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    
    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    
    unsafe fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    
    #[test]
    fn test_executor_basics() {
        let executor = Executor::new(ExecutorConfig::default(), Arc::new(RuntimeMetrics::new()));
        let handle = executor.handle();
        
        let counter = Arc::new(AtomicUsize::new(0));
        
        // Spawn some tasks
        for _ in 0..100 {
            let counter = counter.clone();
            handle.spawn(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }
        
        // Run the executor for a short time
        std::thread::spawn(move || {
            executor.run();
        });
        
        // Wait for tasks to complete
        std::thread::sleep(Duration::from_secs(1));
        
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }
    
    #[test]
    fn test_blocking_tasks() {
        let executor = Executor::new(ExecutorConfig::default(), Arc::new(RuntimeMetrics::new()));
        let handle = executor.handle();
        
        let result = std::thread::scope(|s| {
            s.spawn(|| {
                futures::executor::block_on(async {
                    let result = handle.spawn_blocking(|| {
                        std::thread::sleep(Duration::from_millis(100));
                        42
                    }).await;
                    
                    assert_eq!(result, 42);
                    result
                })
            })
        }).unwrap();
        
        assert_eq!(result, 42);
    }
}
