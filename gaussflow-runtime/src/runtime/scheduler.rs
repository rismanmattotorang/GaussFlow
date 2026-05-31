//! Task scheduling and work distribution.

use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use uuid::Uuid;

use super::error::RuntimeError;
use super::metrics::RuntimeMetrics;
use super::Result;

/// Priority level for tasks.
type Priority = u32;

/// A task to be executed by the runtime.
#[derive(Debug)]
pub struct Task {
    /// Unique identifier for the task.
    pub id: Uuid,
    /// Priority of the task (higher is more important).
    pub priority: Priority,
    /// The actual work to be performed.
    pub work: Box<dyn FnOnce() + Send + 'static>,
    /// When the task was created.
    pub created_at: Instant,
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.id == other.id
    }
}

impl Eq for Task {}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority comes first
        self.priority.cmp(&other.priority).then_with(|| {
            // For equal priorities, older tasks come first
            other.created_at.cmp(&self.created_at)
        })
    }
}

/// A work-stealing scheduler for distributing tasks across workers.
pub struct Scheduler {
    /// Global task queue for high-priority tasks.
    global_queue: Mutex<BinaryHeap<Task>>,
    /// Per-worker task queues.
    worker_queues: Vec<Mutex<VecDeque<Task>>>,
    /// Metrics collector.
    metrics: Arc<RuntimeMetrics>,
    /// Number of workers.
    num_workers: usize,
    /// Index of the next worker to assign a task to.
    next_worker: AtomicUsize,
}

impl Scheduler {
    /// Create a new scheduler with the given number of workers.
    pub fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        let num_workers = num_cpus::get().max(1);
        Self::with_workers(num_workers, metrics)
    }

    /// Create a new scheduler with a specific number of workers.
    pub fn with_workers(num_workers: usize, metrics: Arc<RuntimeMetrics>) -> Self {
        let worker_queues = (0..num_workers)
            .map(|_| Mutex::new(VecDeque::new()))
            .collect();

        Self {
            global_queue: Mutex::new(BinaryHeap::with_capacity(1024)),
            worker_queues,
            metrics,
            num_workers,
            next_worker: AtomicUsize::new(0),
        }
    }

    /// Schedule a new task.
    pub fn schedule<F>(&self, priority: Priority, work: F) -> Uuid
    where
        F: FnOnce() + Send + 'static,
    {
        self.schedule_with_id(Uuid::new_v4(), priority, work)
    }

    /// Schedule a new task with a specific ID.
    pub fn schedule_with_id<F>(&self, id: Uuid, priority: Priority, work: F) -> Uuid
    where
        F: FnOnce() + Send + 'static,
    {
        let task = Task {
            id,
            priority,
            work: Box::new(work),
            created_at: Instant::now(),
        };

        // For now, just add to the global queue
        // In a real implementation, we might use work-stealing
        self.global_queue.lock().push(task);
        
        // Record the scheduling
        self.metrics.task_scheduled();
        
        id
    }

    /// Get the next task for a worker to execute.
    pub fn next_task(&self, worker_id: usize) -> Option<Task> {
        // First try the worker's own queue
        if let Some(task) = self.worker_queues[worker_id].lock().pop_front() {
            return Some(task);
        }

        // Then try the global queue
        if let Some(task) = self.global_queue.lock().pop() {
            return Some(task);
        }

        // Then try to steal from other workers
        self.steal_work(worker_id)
    }

    /// Try to steal work from other workers.
    fn steal_work(&self, thief_id: usize) -> Option<Task> {
        let num_workers = self.num_workers;
        
        // Try to steal from each other worker once
        for i in 1..num_workers {
            let victim_id = (thief_id + i) % num_workers;
            
            // Try to steal half of the victim's tasks
            if let Some(task) = self.worker_queues[victim_id].lock().pop_back() {
                self.metrics.work_stolen();
                return Some(task);
            }
        }
        
        None
    }

    /// Get the number of pending tasks.
    pub fn pending_tasks(&self) -> usize {
        let global = self.global_queue.lock().len();
        let worker: usize = self.worker_queues
            .iter()
            .map(|q| q.lock().len())
            .sum();
        
        global + worker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_task_priority() {
        let now = Instant::now();
        
        let low_prio = Task {
            id: Uuid::new_v4(),
            priority: 1,
            work: Box::new(|| {}),
            created_at: now,
        };
        
        let high_prio = Task {
            id: Uuid::new_v4(),
            priority: 10,
            work: Box::new(|| {}),
            created_at: now + Duration::from_secs(1), // Newer but higher priority
        };
        
        assert!(high_prio > low_prio);
        
        // For same priority, older task comes first
        let older = Task {
            id: Uuid::new_v4(),
            priority: 5,
            work: Box::new(|| {}),
            created_at: now,
        };
        
        let newer = Task {
            id: Uuid::new_v4(),
            priority: 5,
            work: Box::new(|| {}),
            created_at: now + Duration::from_secs(1),
        };
        
        assert!(older < newer);
    }
    
    #[test]
    fn test_scheduler_basics() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let scheduler = Scheduler::with_workers(2, metrics.clone());
        
        let counter = Arc::new(AtomicUsize::new(0));
        
        // Schedule some tasks
        for _ in 0..10 {
            let counter = counter.clone();
            scheduler.schedule(1, move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }
        
        // Process tasks in two worker threads
        let handles: Vec<_> = (0..2)
            .map(|worker_id| {
                let scheduler = &scheduler;
                thread::spawn(move || {
                    while let Some(task) = scheduler.next_task(worker_id) {
                        (task.work)();
                    }
                })
            })
            .collect();
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}
