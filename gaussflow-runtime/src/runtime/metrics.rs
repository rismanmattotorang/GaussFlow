//! Runtime metrics collection and reporting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::Serialize;
use opentelemetry::{global, metrics::{Counter, Histogram, Meter, Unit}};
use tracing::{info, instrument};

/// Tracks various metrics about the runtime's performance.
#[derive(Debug, Default, Clone)]
pub struct RuntimeMetrics {
    /// Total number of tasks executed.
    pub tasks_completed: Arc<AtomicU64>,
    /// Total number of tasks that failed.
    pub tasks_failed: Arc<AtomicU64>,
    /// Total time spent executing tasks.
    pub total_execution_time: Arc<AtomicU64>, // in microseconds
    /// Maximum execution time for a single task.
    pub max_execution_time: Arc<AtomicU64>, // in microseconds
    /// Current number of active tasks.
    pub active_tasks: Arc<AtomicUsize>,
    /// Peak number of active tasks.
    pub peak_active_tasks: Arc<AtomicUsize>,
    /// Per-node type metrics.
    pub node_metrics: Arc<RwLock<HashMap<String, NodeMetrics>>>,
    // OpenTelemetry
    pub otel_tasks_completed: Counter<u64>,
    pub otel_tasks_failed: Counter<u64>,
    pub otel_total_execution_time: Histogram<u64>,
    pub otel_active_tasks: Counter<u64>,
}

/// Metrics for a specific node type.
#[derive(Debug, Default, Clone, Serialize)]
pub struct NodeMetrics {
    /// Number of times this node type was executed.
    pub count: u64,
    /// Total time spent in this node type.
    pub total_time: Duration,
    /// Maximum time spent in a single execution.
    pub max_time: Duration,
    /// Number of failures.
    pub failures: u64,
}

impl RuntimeMetrics {
    /// Create a new metrics collector.
    #[instrument]
    pub fn new() -> Self {
        let meter = global::meter("gaussflow-runtime");
        let otel_tasks_completed = meter.u64_counter("tasks_completed").with_description("Total tasks completed").init();
        let otel_tasks_failed = meter.u64_counter("tasks_failed").with_description("Total tasks failed").init();
        let otel_total_execution_time = meter.u64_histogram("total_execution_time_us").with_unit(Unit::new("us")).with_description("Total execution time in us").init();
        let otel_active_tasks = meter.u64_counter("active_tasks").with_description("Active tasks").init();
        Self {
            tasks_completed: Arc::new(AtomicU64::new(0)),
            tasks_failed: Arc::new(AtomicU64::new(0)),
            total_execution_time: Arc::new(AtomicU64::new(0)),
            max_execution_time: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            node_metrics: Arc::new(RwLock::new(HashMap::new())),
            otel_tasks_completed,
            otel_tasks_failed,
            otel_total_execution_time,
            otel_active_tasks,
        }
    }

    /// Record the start of a task execution.
    #[instrument]
    pub fn task_started(&self) -> TaskTimer {
        self.active_tasks.fetch_add(1, Ordering::SeqCst);
        self.otel_active_tasks.add(1, &[]);
        self.update_peak();
        TaskTimer {
            metrics: self.clone(),
            start: Instant::now(),
        }
    }

    /// Record a task completion.
    #[instrument]
    pub fn task_completed(&self, node_type: &str, success: bool, duration: Duration) {
        let micros = duration.as_micros();
        
        // Update global metrics
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.otel_tasks_completed.add(1, &[]);
        if !success {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
            self.otel_tasks_failed.add(1, &[]);
        }
        
        self.total_execution_time.fetch_add(micros as u64, Ordering::Relaxed);
        self.otel_total_execution_time.record(micros as u64, &[]);
        
        // Update max execution time
        let mut current_max = self.max_execution_time.load(Ordering::Relaxed);
        while micros as u64 > current_max {
            match self.max_execution_time.compare_exchange_weak(
                current_max,
                micros as u64,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
        
        // Update node-specific metrics
        let mut node_metrics = self.node_metrics.write();
        let metrics = node_metrics.entry(node_type.to_string()).or_default();
        metrics.count += 1;
        metrics.total_time += duration;
        if duration > metrics.max_time {
            metrics.max_time = duration;
        }
        if !success {
            metrics.failures += 1;
        }
        
        // Update active tasks count
        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get a snapshot of the current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let active = self.active_tasks.load(Ordering::Relaxed);
        let peak = self.peak_active_tasks.load(Ordering::Relaxed);
        let completed = self.tasks_completed.load(Ordering::Relaxed);
        let failed = self.tasks_failed.load(Ordering::Relaxed);
        let total_time = Duration::from_micros(self.total_execution_time.load(Ordering::Relaxed));
        let max_time = Duration::from_micros(self.max_execution_time.load(Ordering::Relaxed));
        
        let node_metrics = self.node_metrics.read().clone();
        
        MetricsSnapshot {
            active_tasks: active,
            peak_active_tasks: peak,
            tasks_completed: completed,
            tasks_failed: failed,
            total_execution_time: total_time,
            avg_execution_time: if completed > 0 {
                total_time / completed as u32
            } else {
                Duration::from_secs(0)
            },
            max_execution_time: max_time,
            node_metrics,
        }
    }
    
    /// Update the peak active tasks counter if needed.
    fn update_peak(&self) {
        let current = self.active_tasks.load(Ordering::Relaxed);
        let mut peak = self.peak_active_tasks.load(Ordering::Relaxed);
        
        while current > peak {
            match self.peak_active_tasks.compare_exchange_weak(
                peak,
                current,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}

/// A snapshot of metrics at a point in time.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// Current number of active tasks.
    pub active_tasks: usize,
    /// Peak number of active tasks.
    pub peak_active_tasks: usize,
    /// Total number of tasks completed.
    pub tasks_completed: u64,
    /// Total number of tasks that failed.
    pub tasks_failed: u64,
    /// Total time spent executing tasks.
    pub total_execution_time: Duration,
    /// Average execution time per task.
    pub avg_execution_time: Duration,
    /// Maximum execution time for a single task.
    pub max_execution_time: Duration,
    /// Per-node type metrics.
    pub node_metrics: std::collections::HashMap<String, NodeMetrics>,
}

/// A timer that records metrics when dropped.
pub struct TaskTimer {
    metrics: RuntimeMetrics,
    start: Instant,
}

impl Drop for TaskTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        // The node type is not known here, so we use a placeholder
        // The actual implementation should provide the node type
        self.metrics.task_completed("unknown", true, duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metrics_collection() {
        let metrics = RuntimeMetrics::new();
        
        // Simulate some task executions
        for _ in 0..10 {
            let timer = metrics.task_started();
            thread::sleep(Duration::from_millis(1));
            drop(timer); // Records completion
        }
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.tasks_completed, 10);
        assert!(snapshot.avg_execution_time > Duration::from_millis(0));
        assert_eq!(snapshot.active_tasks, 0);
    }
    
    #[test]
    fn test_concurrent_metrics() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let mut handles = vec![];
        
        // Spawn multiple threads to update metrics concurrently
        for _ in 0..10 {
            let metrics = metrics.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let timer = metrics.task_started();
                    thread::sleep(Duration::from_micros(10));
                    drop(timer);
                }
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.tasks_completed, 1000);
        assert!(snapshot.peak_active_tasks > 1); // Should have had concurrent tasks
        assert_eq!(snapshot.active_tasks, 0);
    }
}
