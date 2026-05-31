//! Runtime metrics (Phase 4).
//!
//! A small, dependency-free metrics registry the executor updates as runs and nodes execute, plus
//! a Prometheus text-exposition renderer. A process-global instance backs [`crate::prometheus_metrics`]
//! (what a `/metrics` endpoint scrapes); the type can also be used standalone (e.g. in tests).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// The outcome of a node execution, used as a metric label.
#[derive(Debug, Clone, Copy)]
pub enum NodeOutcome {
    /// The node ran successfully.
    Executed,
    /// The node was skipped (an untaken conditional/router branch).
    Skipped,
    /// The node failed.
    Failed,
}

impl NodeOutcome {
    fn label(self) -> &'static str {
        match self {
            NodeOutcome::Executed => "executed",
            NodeOutcome::Skipped => "skipped",
            NodeOutcome::Failed => "failed",
        }
    }
}

/// Counters and timings for workflow execution.
#[derive(Default)]
pub struct RuntimeMetrics {
    runs_started: AtomicU64,
    runs_completed: AtomicU64,
    runs_failed: AtomicU64,
    /// (node_type, outcome) -> count
    nodes: Mutex<BTreeMap<(String, String), u64>>,
    node_exec_millis_total: AtomicU64,
    node_exec_count: AtomicU64,
}

/// A point-in-time copy of the counters (for tests/inspection).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub runs_started: u64,
    pub runs_completed: u64,
    pub runs_failed: u64,
    pub nodes_executed: u64,
    pub nodes_skipped: u64,
    pub nodes_failed: u64,
}

impl RuntimeMetrics {
    /// Record that a run has started.
    pub fn record_run_started(&self) {
        self.runs_started.fetch_add(1, Ordering::Relaxed);
    }
    /// Record that a run completed successfully.
    pub fn record_run_completed(&self) {
        self.runs_completed.fetch_add(1, Ordering::Relaxed);
    }
    /// Record that a run failed.
    pub fn record_run_failed(&self) {
        self.runs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a node execution: its type, outcome, and (for executed/failed) wall-clock duration.
    pub fn record_node(&self, node_type: &str, outcome: NodeOutcome, duration_ms: u64) {
        *self
            .nodes
            .lock()
            .unwrap()
            .entry((node_type.to_string(), outcome.label().to_string()))
            .or_insert(0) += 1;
        if matches!(outcome, NodeOutcome::Executed | NodeOutcome::Failed) {
            self.node_exec_millis_total
                .fetch_add(duration_ms, Ordering::Relaxed);
            self.node_exec_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Sum node counts for a given outcome label.
    fn nodes_with_outcome(&self, outcome: &str) -> u64 {
        self.nodes
            .lock()
            .unwrap()
            .iter()
            .filter(|((_, o), _)| o == outcome)
            .map(|(_, c)| *c)
            .sum()
    }

    /// Take a snapshot of the aggregate counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            runs_started: self.runs_started.load(Ordering::Relaxed),
            runs_completed: self.runs_completed.load(Ordering::Relaxed),
            runs_failed: self.runs_failed.load(Ordering::Relaxed),
            nodes_executed: self.nodes_with_outcome("executed"),
            nodes_skipped: self.nodes_with_outcome("skipped"),
            nodes_failed: self.nodes_with_outcome("failed"),
        }
    }

    /// Render the metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        let counter = |out: &mut String, name: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
            ));
        };
        counter(
            &mut out,
            "gaussflow_runs_started_total",
            "Workflow runs started.",
            self.runs_started.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "gaussflow_runs_completed_total",
            "Workflow runs completed successfully.",
            self.runs_completed.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "gaussflow_runs_failed_total",
            "Workflow runs that failed.",
            self.runs_failed.load(Ordering::Relaxed),
        );

        out.push_str("# HELP gaussflow_nodes_total Node executions by type and outcome.\n");
        out.push_str("# TYPE gaussflow_nodes_total counter\n");
        for ((node_type, outcome), count) in self.nodes.lock().unwrap().iter() {
            out.push_str(&format!(
                "gaussflow_nodes_total{{node_type=\"{node_type}\",outcome=\"{outcome}\"}} {count}\n"
            ));
        }

        let sum_secs = self.node_exec_millis_total.load(Ordering::Relaxed) as f64 / 1000.0;
        let count = self.node_exec_count.load(Ordering::Relaxed);
        out.push_str("# HELP gaussflow_node_execution_seconds Node execution wall-clock time.\n");
        out.push_str("# TYPE gaussflow_node_execution_seconds summary\n");
        out.push_str(&format!(
            "gaussflow_node_execution_seconds_sum {sum_secs}\n"
        ));
        out.push_str(&format!("gaussflow_node_execution_seconds_count {count}\n"));
        out
    }
}

/// The process-global metrics registry (what `/metrics` exposes).
pub fn global() -> &'static RuntimeMetrics {
    static METRICS: OnceLock<RuntimeMetrics> = OnceLock::new();
    METRICS.get_or_init(RuntimeMetrics::default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_renders() {
        let m = RuntimeMetrics::default();
        m.record_run_started();
        m.record_node("llm_call", NodeOutcome::Executed, 12);
        m.record_node("router", NodeOutcome::Skipped, 0);
        m.record_node("llm_call", NodeOutcome::Executed, 8);
        m.record_run_completed();

        let snap = m.snapshot();
        assert_eq!(snap.runs_started, 1);
        assert_eq!(snap.runs_completed, 1);
        assert_eq!(snap.nodes_executed, 2);
        assert_eq!(snap.nodes_skipped, 1);

        let text = m.render_prometheus();
        assert!(text.contains("gaussflow_runs_started_total 1"));
        assert!(
            text.contains("gaussflow_nodes_total{node_type=\"llm_call\",outcome=\"executed\"} 2")
        );
        assert!(text.contains("gaussflow_node_execution_seconds_count 2"));
        // Prometheus exposition lines are well-formed (name + value or HELP/TYPE).
        assert!(text
            .lines()
            .any(|l| l.starts_with("# TYPE gaussflow_runs_started_total counter")));
    }
}
