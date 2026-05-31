//! Metrics and observability for GaussFlow

use lazy_static::lazy_static;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn, Level};

lazy_static! {
    /// Global metrics registry
    pub static ref METRICS: Metrics = Metrics::new();
}

/// Tracks various metrics for the workflow execution
#[derive(Debug)]
pub struct Metrics {
    /// Total number of nodes executed
    pub nodes_executed: AtomicU64,
    /// Total number of failed nodes
    pub nodes_failed: AtomicU64,
    /// Total execution time in milliseconds
    pub total_execution_time_ms: AtomicU64,
    /// Number of active workflows
    pub active_workflows: AtomicU64,
}

impl Metrics {
    /// Creates a new metrics instance
    pub fn new() -> Self {
        Self {
            nodes_executed: AtomicU64::new(0),
            nodes_failed: AtomicU64::new(0),
            total_execution_time_ms: AtomicU64::new(0),
            active_workflows: AtomicU64::new(0),
        }
    }
    /// Records a node execution
    #[instrument]
    pub fn record_node_execution(&self, duration: std::time::Duration) {
        self.nodes_executed.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }
    /// Records a node failure
    #[instrument]
    pub fn record_node_failure(&self) {
        self.nodes_failed.fetch_add(1, Ordering::Relaxed);
    }
    /// Records workflow start
    #[instrument]
    pub fn record_workflow_start(&self) {
        self.active_workflows.fetch_add(1, Ordering::Relaxed);
    }
    /// Records workflow completion
    #[instrument]
    pub fn record_workflow_end(&self) {
        self.active_workflows.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A guard that records the duration of a scope when dropped
pub struct ScopeTimer {
    name: &'static str,
    start: Instant,
    level: Level,
}

impl ScopeTimer {
    /// Creates a new scope timer
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
            level: Level::DEBUG,
        }
    }

    /// Sets the log level for this timer
    pub fn with_level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Drop for ScopeTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let msg = format!("{} took {:?}", self.name, elapsed);

        match self.level {
            Level::ERROR => error!("{}", msg),
            Level::WARN => warn!("{}", msg),
            Level::INFO => info!("{}", msg),
            Level::DEBUG => debug!("{}", msg),
            Level::TRACE => debug!("{} (trace)", msg),
        }
    }
}

/// Initializes the tracing subscriber
pub fn init_tracing(level: Option<&str>) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = match level {
        Some(level) => EnvFilter::new(level),
        None => EnvFilter::from_default_env()
            .add_directive("gaussflow_core=info".parse().unwrap())
            .add_directive("gaussflow_runtime=info".parse().unwrap()),
    };

    fmt()
        .with_env_filter(filter)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
}

/// Returns a string with the current metrics
pub fn get_metrics() -> String {
    format!(
        "Nodes executed: {}, Failed: {}, Active workflows: {}",
        METRICS.nodes_executed.load(Ordering::Relaxed),
        METRICS.nodes_failed.load(Ordering::Relaxed),
        METRICS.active_workflows.load(Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics() {
        let metrics = Metrics::new();

        metrics.record_node_execution(Duration::from_millis(100));
        metrics.record_node_failure();
        metrics.record_workflow_start();

        assert_eq!(metrics.nodes_executed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.nodes_failed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.active_workflows.load(Ordering::Relaxed), 1);

        metrics.record_workflow_end();
        assert_eq!(metrics.active_workflows.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_scope_timer() {
        let _timer = ScopeTimer::new("test_timer");
        std::thread::sleep(Duration::from_millis(10));
        // Timer will log when it goes out of scope
    }
}
