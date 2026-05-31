//! Optimized task executor that combines batching, work stealing, and other optimizations.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
    mem,
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use futures::{
    future::{BoxFuture, FutureExt},
    stream::FuturesUnordered,
};
use parking_lot::{Mutex, MutexGuard};
use tracing::{debug, error, info_span, trace, warn, Instrument};

use crate::runtime::{
    batcher::Batcher,
    error::RuntimeError,
    metrics::RuntimeMetrics,
    pool::Pool,
    profiler::{MemoryProfiler, PerformanceProfiler, MEMORY_PROFILER},
    scheduler::Scheduler,
    steal_queue::{StealQueue, Stealer as QueueStealer, Worker as QueueWorker},
    string_interner::ThreadLocalInterner,
    Result,
};

// Size of the task stack for each worker
const TASK_STACK_SIZE: usize = 1024 * 1024; // 1MB per task stack

// Maximum number of tasks to process in a single batch
const MAX_BATCH_SIZE: usize = 32;

// Maximum time to spend processing tasks before yielding
const MAX_PROCESSING_TIME: Duration = Duration::from_millis(5);

// Maximum number of times to retry stealing before yielding
const MAX_STEAL_RETRIES: usize = 32;

/// Configuration for the optimized executor.
#[derive(Debug, Clone)]
pub struct OptimizedExecutorConfig {
    /// Number of worker threads.
    pub num_workers: usize,
    /// Stack size for each worker thread.
    pub stack_size: usize,
    /// Maximum number of tasks that can be queued per worker.
    pub queue_size: usize,
    /// Maximum batch size for batched operations.
    pub max_batch_size: usize,
    /// Maximum time to wait before processing a batch.
    pub max_batch_delay: Duration,
    /// Size of the memory pool for task data.
    pub memory_pool_size: usize,
    /// Whether to enable string interning.
    pub enable_string_interning: bool,
    /// Whether to enable work stealing.
    pub enable_work_stealing: bool,
    /// Whether to enable batching.
    pub enable_batching: bool,
}

impl Default for OptimizedExecutorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get().max(1),
            stack_size: 2 * 1024 * 1024, // 2MB stack
            queue_size: 10_000,
            max_batch_size: 32,
            max_batch_delay: Duration::from_micros(100),
            memory_pool_size: 1024 * 1024, // 1MB
            enable_string_interning: true,
            enable_work_stealing: true,
            enable_batching: true,
        }
    }
}

/// A task that can be scheduled by the executor.
type BoxedTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A handle to the optimized executor.
pub struct OptimizedExecutorHandle {
    /// The global task queue.
    global_queue: Arc<Injector<BoxedTask>>,
    /// The worker-local queues.
    workers: Arc<Mutex<Vec<Worker<BoxedTask>>>>,
    /// The metrics collector.
    metrics: Arc<RuntimeMetrics>,
    /// The scheduler.
    scheduler: Arc<Scheduler>,
    /// The string interner.
    string_interner: Option<Arc<ThreadLocalInterner>>,
    /// The memory pool.
    memory_pool: Arc<Pool<Vec<u8>>>,
}

impl OptimizedExecutorHandle {
    /// Spawn a new task onto the executor.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Box::pin(future);
        self.global_queue.push(task);
    }

    /// Spawn a blocking task onto the executor.
    pub fn spawn_blocking<F, R>(&self, f: F) -> impl Future<Output = R> + 'static
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
    
    /// Get a reference to the metrics collector.
    pub fn metrics(&self) -> &RuntimeMetrics {
        &self.metrics
    }
    
    /// Get a reference to the string interner, if enabled.
    pub fn string_interner(&self) -> Option<&ThreadLocalInterner> {
        self.string_interner.as_deref()
    }
    
    /// Get a reference to the memory pool.
    pub fn memory_pool(&self) -> &Pool<Vec<u8>> {
        &self.memory_pool
    }
}

/// The optimized executor that runs tasks.
pub struct OptimizedExecutor {
    /// The global task queue.
    global_queue: Arc<Injector<BoxedTask>>,
    /// The worker-local queues.
    workers: Arc<Mutex<Vec<Worker<BoxedTask>>>>,
    /// The metrics collector.
    metrics: Arc<RuntimeMetrics>,
    /// The scheduler.
    scheduler: Arc<Scheduler>,
    /// The batcher for batching operations.
    batcher: Option<Arc<Batcher<u64, Vec<u8>, Vec<u8>>>>,
    /// The string interner.
    string_interner: Option<Arc<ThreadLocalInterner>>,
    /// The memory pool.
    memory_pool: Arc<Pool<Vec<u8>>>,
    /// The configuration.
    config: OptimizedExecutorConfig,
}

impl OptimizedExecutor {
    /// Create a new optimized executor with the given configuration.
    pub fn new(config: OptimizedExecutorConfig) -> Self {
        // Initialize tracing if not already initialized
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "gaussflow_runtime=info");
            let _ = tracing_subscriber::fmt::try_init();
        }
        
        debug!("Creating optimized executor with config: {:?}", config);
        let start_time = Instant::now();
        
        // Initialize metrics with additional dimensions
        let mut metrics = RuntimeMetrics::new();
        metrics.add_dimension("executor_type", "optimized");
        metrics.add_dimension("version", env!("CARGO_PKG_VERSION"));
        metrics.add_dimension("build_profile", if cfg!(debug_assertions) { "debug" } else { "release" });
        
        let metrics = Arc::new(metrics);
        let scheduler = Arc::new(Scheduler::new(metrics.clone()));
        
        // Initialize memory pool with alignment for cache lines
        let memory_pool = if config.enable_memory_pooling {
            let pool = Pool::new(
                config.memory_pool_size,
                "task_data_pool",
            );
            
            // Pre-allocate some memory to reduce initial allocations
            if config.enable_memory_pooling {
                let mut pool_guard = pool.inner.lock();
                for _ in 0..config.initial_pool_size.min(1000) {
                    pool_guard.push(Vec::with_capacity(1024));
                }
                drop(pool_guard);
            }
            
            Arc::new(pool)
        } else {
            Arc::new(Pool::disabled())
        };
        let global_queue = Arc::new(Injector::new());
        let workers = Arc::new(Mutex::new(Vec::new()));
        let metrics = Arc::new(RuntimeMetrics::new());
        let scheduler = Arc::new(Scheduler::new(metrics.clone()));
        
        // Initialize the batcher if enabled
        let batcher = if config.enable_batching {
            let (batcher, _) = Batcher::new(
                |batch: Vec<Vec<u8>>| {
                    // Process the batch
                    Box::pin(async move {
                        let results = batch.into_iter().map(|v| Ok(v)).collect();
                        Ok(results)
                    })
                },
                config.max_batch_size,
                config.max_batch_delay,
            );
            Some(Arc::new(batcher))
        } else {
            None
        };
        
        // Initialize the string interner if enabled
        let string_interner = if config.enable_string_interning {
            Some(Arc::new(ThreadLocalInterner::new()))
        } else {
            None
        };
        
        // Initialize the memory pool
        let memory_pool = Arc::new(Pool::new(
            config.memory_pool_size,
            "task_data_pool",
        ));
        
        Self {
            global_queue,
            workers,
            metrics,
            scheduler,
            batcher,
            string_interner,
            memory_pool,
            config,
        }
    }
    
    /// Get a handle to the executor that can be used to spawn tasks.
    pub fn handle(&self) -> OptimizedExecutorHandle {
        OptimizedExecutorHandle {
            global_queue: self.global_queue.clone(),
            workers: self.workers.clone(),
            metrics: self.metrics.clone(),
            scheduler: self.scheduler.clone(),
            string_interner: self.string_interner.clone(),
            memory_pool: self.memory_pool.clone(),
        }
    }
    
    /// Start the executor and run tasks.
    pub fn run(self) {
        let num_workers = self.config.num_workers;
        
        // Create worker threads
        for worker_id in 0..num_workers {
            let global_queue = self.global_queue.clone();
            let workers = self.workers.clone();
            let metrics = self.metrics.clone();
            let scheduler = self.scheduler.clone();
            let batcher = self.batcher.clone();
            let string_interner = self.string_interner.clone();
            let memory_pool = self.memory_pool.clone();
            let config = self.config.clone();
            
            let builder = std::thread::Builder::new()
                .name(format!("worker-{worker_id}"))
                .stack_size(config.stack_size);
            
            builder.spawn(move || {
                // Set up thread-local state
                if let Some(interner) = &string_interner {
                    // Initialize thread-local string interner
                    let _ = interner.get_or_intern("");
                }
                
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
                    if let Some(task) = self.find_work(&stealer, &global_queue, &workers) {
                        // Run the task
                        self.run_task(task);
                    } else {
                        // No work available, yield
                        std::thread::yield_now();
                    }
                }
            }).unwrap();
        }
    }
    
    /// Find work to do, with improved work stealing and load balancing
    fn find_work(
        &self,
        local_stealer: &Stealer<BoxedTask>,
        global_queue: &Arc<Injector<BoxedTask>>,
        workers: &Arc<Mutex<Vec<Worker<BoxedTask>>>>,
    ) -> Option<BoxedTask> {
        let _span = info_span!("find_work").entered();
        let start = Instant::now();
        let mut stats = WorkStealingStats::default();
        
        // Try to pop from the local queue first (fast path)
        if let Steal::Success(task) = local_stealer.steal() {
            stats.local_success = true;
            stats.record_time(start.elapsed());
            self.metrics.record_work_steal_stats(&stats);
            return Some(task);
        }
        
        // Try the global queue next
        if let Some(task) = global_queue.steal() {
            stats.global_success = true;
            stats.record_time(start.elapsed());
            self.metrics.record_work_steal_stats(&stats);
            return Some(task);
        }
        
        // If work stealing is disabled, return early
        if !self.config.enable_work_stealing {
            stats.record_time(start.elapsed());
            self.metrics.record_work_steal_stats(&stats);
            return None;
        }
        
        // Try to steal work from other workers
        let workers = workers.lock();
        let num_workers = workers.len();
        
        if num_workers <= 1 {
            stats.record_time(start.elapsed());
            self.metrics.record_work_steal_stats(&stats);
            return None;
        }
        
        // Use a random starting point to avoid contention
        let start_idx = fastrand::usize(..num_workers);
        let mut steal_attempts = 0;
        
        for i in 0..num_workers {
            let idx = (start_idx + i) % num_workers;
            
            // Don't try to steal from ourselves
            if idx == local_stealer as *const _ as usize % num_workers {
                continue;
            }
            
            // Try to steal a batch of tasks
            if let Steal::Success(task) = workers[idx].stealer().steal_batch(local_stealer) {
                stats.steal_success = true;
                stats.steal_attempts = steal_attempts + 1;
                stats.record_time(start.elapsed());
                self.metrics.record_work_steal_stats(&stats);
                return Some(task);
            }
            
            steal_attempts += 1;
            
            // Don't spend too much time trying to steal
            if steal_attempts >= MAX_STEAL_RETRIES || start.elapsed() > MAX_PROCESSING_TIME {
                break;
            }
            
            // Yield to avoid CPU spinning
            if steal_attempts % 4 == 0 {
                std::thread::yield_now();
            }
        }
        
        stats.record_time(start.elapsed());
        self.metrics.record_work_steal_stats(&stats);
        None
    }
        // Try to pop from the local queue
        if let Steal::Success(task) = local_stealer.steal() {
            self.metrics.work_stolen();
            return Some(task);
        }
        
        // Try to pop from the global queue
        if let Some(task) = global_queue.steal() {
            return Some(task);
        }
        
        // Try to steal from other workers if enabled
        if self.config.enable_work_stealing {
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
                    self.metrics.work_stolen();
                    return Some(task);
                }
            }
        }
        
        None
    }
    
    /// Run a task to completion with better error handling and resource management
    fn run_task(&self, mut task: BoxedTask) {
        let _span = info_span!("run_task").entered();
        let start = Instant::now();
        let task_id = self.metrics.task_started();
        
        // Create a waker that does nothing
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        
        // Track memory usage before and after task execution
        let mem_before = MEMORY_PROFILER.current_usage();
        
        // Run the task to completion with a timeout
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut task_complete = false;
            let mut poll_count = 0;
            let start_time = Instant::now();
            
            while !task_complete && start_time.elapsed() < Duration::from_secs(30) {
                match task.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        task_complete = true;
                    }
                    Poll::Pending => {
                        poll_count += 1;
                        if poll_count % 100 == 0 {
                            // Periodically yield to the scheduler
                            std::thread::yield_now();
                        }
                    }
                }
            }
            
            if !task_complete {
                warn!("Task {} timed out after {:?}", task_id, start_time.elapsed());
                self.metrics.task_failed("timeout");
            }
        }));
        
        // Handle task completion or panic
        match result {
            Ok(_) => {
                let duration = start.elapsed();
                let mem_after = MEMORY_PROFILER.current_usage();
                let mem_used = mem_after.saturating_sub(mem_before);
                
                trace!(
                    task_id = task_id,
                    duration_ms = duration.as_millis(),
                    memory_used = mem_used,
                    "Task completed"
                );
                
                self.metrics.record_metric("task_memory_used", mem_used as f64);
                self.metrics.task_completed(duration);
            }
            Err(e) => {
                error!("Task {} panicked: {:?}", task_id, e);
                self.metrics.task_failed("panic");
                
                // Log the panic backtrace if available
                if let Some(panic_info) = e.downcast_ref::<&str>() {
                    error!("Task panic: {}", panic_info);
                }
            }
        }
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
    fn test_optimized_executor() {
        let config = OptimizedExecutorConfig {
            num_workers: 2,
            enable_batching: false,
            enable_string_interning: false,
            enable_work_stealing: true,
            ..Default::default()
        };
        
        let executor = OptimizedExecutor::new(config);
        let handle = executor.handle();
        
        let counter = Arc::new(AtomicUsize::new(0));
        
        // Spawn some tasks
        for _ in 0..100 {
            let counter = counter.clone();
            handle.spawn(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }
        
        // Run the executor in a separate thread
        std::thread::spawn(move || {
            executor.run();
        });
        
        // Wait for tasks to complete
        std::thread::sleep(Duration::from_secs(1));
        
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }
    
    #[test]
    fn test_string_interning() {
        let config = OptimizedExecutorConfig {
            num_workers: 1,
            enable_string_interning: true,
            ..Default::default()
        };
        
        let executor = OptimizedExecutor::new(config);
        let handle = executor.handle();
        
        let interner = handle.string_interner().unwrap();
        
        let s1 = interner.get_or_intern("hello");
        let s2 = interner.get_or_intern("hello");
        let s3 = interner.get_or_intern("world");
        
        assert_eq!(s1.as_ptr(), s2.as_ptr());
        assert_ne!(s1.as_ptr(), s3.as_ptr());
    }
}
