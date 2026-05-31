use anyhow::{Context, Result};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use sysinfo::{SystemExt, CpuExt};

use crate::config::MetricsConfig;

/// Metric types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// Custom metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMetric {
    pub name: String,
    pub metric_type: MetricType,
    pub description: String,
    pub labels: Vec<String>,
}

/// Performance metrics
#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    pub cpu_usage: f64,
    pub memory_usage: u64,
    pub disk_usage: u64,
    pub network_io: NetworkMetrics,
    pub workflow_metrics: WorkflowMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct NetworkMetrics {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections: u32,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowMetrics {
    pub total_workflows: u64,
    pub running_workflows: u32,
    pub completed_workflows: u64,
    pub failed_workflows: u64,
    pub avg_execution_time: Duration,
    pub throughput: f64, // workflows per second
}

/// Advanced metrics manager with Prometheus integration
pub struct MetricsManager {
    /// Prometheus handle
    prometheus: Option<PrometheusHandle>,
    
    /// Configuration
    config: MetricsConfig,
    
    /// Custom metrics registry
    custom_metrics: Arc<RwLock<HashMap<String, CustomMetric>>>,
    
    /// Performance metrics
    performance: Arc<RwLock<PerformanceMetrics>>,
    
    /// Startup time
    startup_time: Instant,
    
    /// Metrics collection interval
    collection_interval: Duration,
}

impl MetricsManager {
    /// Create a new metrics manager
    pub async fn new(config: &MetricsConfig) -> Result<Self> {
        let manager = Self {
            prometheus: None,
            config: config.clone(),
            custom_metrics: Arc::new(RwLock::new(HashMap::new())),
            performance: Arc::new(RwLock::new(PerformanceMetrics::default())),
            startup_time: Instant::now(),
            collection_interval: Duration::from_secs(config.export_interval),
        };
        
        Ok(manager)
    }
    
    /// Initialize metrics collection
    pub async fn init(&mut self) -> Result<()> {
        if !self.config.enabled {
            info!("Metrics collection disabled");
            return Ok(());
        }
        
        // Initialize Prometheus exporter
        let prometheus = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("gaussflow_workflow_duration".to_string()),
                &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
            )?
            .set_buckets_for_metric(
                Matcher::Full("gaussflow_cache_operation_duration".to_string()),
                &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
            )?
            .install_recorder()?;
        
        self.prometheus = Some(prometheus);
        
        // Register default metrics
        self.register_default_metrics().await?;
        
        // Start metrics collection
        self.start_metrics_collection().await;
        
        info!("Metrics manager initialized successfully");
        Ok(())
    }
    
    /// Register default metrics
    async fn register_default_metrics(&self) -> Result<()> {
        let default_metrics = vec![
            CustomMetric {
                name: "gaussflow_workflows_total".to_string(),
                metric_type: MetricType::Counter,
                description: "Total number of workflows processed".to_string(),
                labels: vec!["status".to_string(), "type".to_string()],
            },
            CustomMetric {
                name: "gaussflow_workflow_duration".to_string(),
                metric_type: MetricType::Histogram,
                description: "Workflow execution duration in seconds".to_string(),
                labels: vec!["workflow_type".to_string()],
            },
            CustomMetric {
                name: "gaussflow_cache_hits".to_string(),
                metric_type: MetricType::Counter,
                description: "Number of cache hits".to_string(),
                labels: vec!["cache_layer".to_string()],
            },
            CustomMetric {
                name: "gaussflow_cache_misses".to_string(),
                metric_type: MetricType::Counter,
                description: "Number of cache misses".to_string(),
                labels: vec!["cache_layer".to_string()],
            },
            CustomMetric {
                name: "gaussflow_memory_usage_bytes".to_string(),
                metric_type: MetricType::Gauge,
                description: "Memory usage in bytes".to_string(),
                labels: vec!["component".to_string()],
            },
            CustomMetric {
                name: "gaussflow_cpu_usage_percent".to_string(),
                metric_type: MetricType::Gauge,
                description: "CPU usage percentage".to_string(),
                labels: vec!["component".to_string()],
            },
            CustomMetric {
                name: "gaussflow_active_connections".to_string(),
                metric_type: MetricType::Gauge,
                description: "Number of active connections".to_string(),
                labels: vec!["connection_type".to_string()],
            },
        ];
        
        for metric in default_metrics {
            self.register_custom_metric(metric).await?;
        }
        
        Ok(())
    }
    
    /// Register a custom metric
    pub async fn register_custom_metric(&self, metric: CustomMetric) -> Result<()> {
        let metric_name = metric.name.clone();
        let mut metrics = self.custom_metrics.write().await;
        metrics.insert(metric_name.clone(), metric.clone());
        debug!("Registered custom metric: {}", metric_name);
        Ok(())
    }
    
    /// Record workflow metrics
    pub async fn record_workflow_start(&self, workflow_type: &str) -> Result<()> {
        counter!("gaussflow_workflows_total", 1, "status" => "started", "type" => workflow_type.to_string());
        
        let mut performance = self.performance.write().await;
        performance.workflow_metrics.total_workflows += 1;
        performance.workflow_metrics.running_workflows += 1;
        
        Ok(())
    }
    
    /// Record workflow completion
    pub async fn record_workflow_completion(
        &self,
        workflow_type: &str,
        duration: Duration,
        success: bool,
    ) -> Result<()> {
        let status = if success { "completed" } else { "failed" };
        counter!("gaussflow_workflows_total", 1, "status" => status, "type" => workflow_type.to_string());
        
        histogram!("gaussflow_workflow_duration", duration.as_secs_f64(), "workflow_type" => workflow_type.to_string());
        
        let mut performance = self.performance.write().await;
        performance.workflow_metrics.running_workflows -= 1;
        
        if success {
            performance.workflow_metrics.completed_workflows += 1;
        } else {
            performance.workflow_metrics.failed_workflows += 1;
        }
        
        // Update average execution time
        let total_completed = performance.workflow_metrics.completed_workflows + performance.workflow_metrics.failed_workflows;
        if total_completed > 0 {
            let current_avg = performance.workflow_metrics.avg_execution_time;
            let new_avg = (current_avg * (total_completed - 1) as u32 + duration) / total_completed as u32;
            performance.workflow_metrics.avg_execution_time = new_avg;
        }
        
        Ok(())
    }
    
    /// Record cache metrics
    pub async fn record_cache_hit(&self, cache_layer: &str) -> Result<()> {
        counter!("gaussflow_cache_hits", 1, "cache_layer" => cache_layer.to_string());
        Ok(())
    }
    
    /// Record cache miss
    pub async fn record_cache_miss(&self, cache_layer: &str) -> Result<()> {
        counter!("gaussflow_cache_misses", 1, "cache_layer" => cache_layer.to_string());
        Ok(())
    }
    
    /// Record cache operation duration
    pub async fn record_cache_operation_duration(&self, operation: &str, duration: Duration) -> Result<()> {
        histogram!("gaussflow_cache_operation_duration", duration.as_secs_f64(), "operation" => operation.to_string());
        Ok(())
    }
    
    /// Record memory usage
    pub async fn record_memory_usage(&self, component: &str, bytes: u64) -> Result<()> {
        gauge!("gaussflow_memory_usage_bytes", bytes as f64, "component" => component.to_string());
        
        let mut performance = self.performance.write().await;
        if component == "total" {
            performance.memory_usage = bytes;
        }
        
        Ok(())
    }
    
    /// Record CPU usage
    pub async fn record_cpu_usage(&self, component: &str, percentage: f64) -> Result<()> {
        gauge!("gaussflow_cpu_usage_percent", percentage, "component" => component.to_string());
        
        let mut performance = self.performance.write().await;
        if component == "total" {
            performance.cpu_usage = percentage;
        }
        
        Ok(())
    }
    
    /// Record network metrics
    pub async fn record_network_metrics(&self, bytes_sent: u64, bytes_received: u64, connections: u32) -> Result<()> {
        gauge!("gaussflow_network_bytes_sent", bytes_sent as f64);
        gauge!("gaussflow_network_bytes_received", bytes_received as f64);
        gauge!("gaussflow_active_connections", connections as f64, "connection_type" => "total");
        
        let mut performance = self.performance.write().await;
        performance.network_io.bytes_sent = bytes_sent;
        performance.network_io.bytes_received = bytes_received;
        performance.network_io.connections = connections;
        
        Ok(())
    }
    
    /// Record startup metrics
    pub async fn record_startup(&self) {
        let uptime = self.startup_time.elapsed();
        gauge!("gaussflow_uptime_seconds", uptime.as_secs_f64());
        
        // Record system information
        if let Ok(hostname) = std::env::var("HOSTNAME") {
            gauge!("gaussflow_host_info", 1.0, "hostname" => hostname);
        }
        
        if let Ok(version) = std::env::var("CARGO_PKG_VERSION") {
            gauge!("gaussflow_version_info", 1.0, "version" => version);
        }
    }
    
    /// Get current performance metrics
    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance.read().await.clone()
    }
    
    /// Get Prometheus metrics
    pub async fn get_prometheus_metrics(&self) -> Result<String> {
        if let Some(prometheus) = &self.prometheus {
            Ok(prometheus.render())
        } else {
            anyhow::bail!("Prometheus metrics not initialized");
        }
    }
    
    /// Export metrics to file
    pub async fn export_metrics(&self, path: &str) -> Result<()> {
        let metrics = self.get_prometheus_metrics().await?;
        tokio::fs::write(path, metrics).await
            .context("Failed to write metrics to file")?;
        
        info!("Metrics exported to: {}", path);
        Ok(())
    }
    
    /// Start metrics collection
    async fn start_metrics_collection(&self) {
        let metrics_manager = self.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(metrics_manager.collection_interval);
            
            loop {
                interval.tick().await;
                
                // Collect system metrics
                if let Err(e) = metrics_manager.collect_system_metrics().await {
                    error!("Failed to collect system metrics: {}", e);
                }
                
                // Update throughput calculation
                if let Err(e) = metrics_manager.update_throughput().await {
                    error!("Failed to update throughput: {}", e);
                }
                
                // Export metrics if configured
                if let Err(e) = metrics_manager.export_metrics_if_needed().await {
                    error!("Failed to export metrics: {}", e);
                }
            }
        });
    }
    
    /// Collect system metrics
    async fn collect_system_metrics(&self) -> Result<()> {
        // Memory usage
        let memory_info = sysinfo::System::new_all();
        let total_memory = memory_info.total_memory() * 1024; // Convert to bytes
        let used_memory = memory_info.used_memory() * 1024;
        self.record_memory_usage("system", used_memory).await?;

        // CPU usage - using a simplified approach for now
        let cpu_usage = 0.0; // TODO: Implement proper CPU usage measurement
        self.record_cpu_usage("system", cpu_usage).await?;

        // Disk usage (skipped, see previous note)

        Ok(())
    }
    
    /// Update throughput calculation
    async fn update_throughput(&self) -> Result<()> {
        let mut performance = self.performance.write().await;
        let uptime = self.startup_time.elapsed();
        
        if uptime.as_secs() > 0 {
            performance.workflow_metrics.throughput = 
                (performance.workflow_metrics.completed_workflows + performance.workflow_metrics.failed_workflows) as f64 
                / uptime.as_secs() as f64;
        }
        
        Ok(())
    }
    
    /// Export metrics if needed
    async fn export_metrics_if_needed(&self) -> Result<()> {
        // Check if export is configured and time has passed
        if self.config.export_interval > 0 {
            // Export to a default location
            let export_path = "metrics_export.prom";
            self.export_metrics(export_path).await?;
        }
        
        Ok(())
    }
    
    /// Get metrics summary
    pub async fn get_metrics_summary(&self) -> MetricsSummary {
        let performance = self.get_performance_metrics().await;
        let custom_metrics = self.custom_metrics.read().await;
        
        MetricsSummary {
            uptime: self.startup_time.elapsed(),
            performance,
            custom_metrics_count: custom_metrics.len(),
            prometheus_enabled: self.prometheus.is_some(),
        }
    }
}

impl Clone for MetricsManager {
    fn clone(&self) -> Self {
        Self {
            prometheus: self.prometheus.clone(),
            config: self.config.clone(),
            custom_metrics: self.custom_metrics.clone(),
            performance: self.performance.clone(),
            startup_time: self.startup_time,
            collection_interval: self.collection_interval,
        }
    }
}

/// Metrics summary for reporting
pub struct MetricsSummary {
    pub uptime: Duration,
    pub performance: PerformanceMetrics,
    pub custom_metrics_count: usize,
    pub prometheus_enabled: bool,
}

impl std::fmt::Display for MetricsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== GaussFlow Metrics Summary ===")?;
        writeln!(f, "Uptime: {:.2} seconds", self.uptime.as_secs_f64())?;
        writeln!(f, "CPU Usage: {:.2}%", self.performance.cpu_usage)?;
        writeln!(f, "Memory Usage: {} MB", self.performance.memory_usage / 1024 / 1024)?;
        writeln!(f, "Total Workflows: {}", self.performance.workflow_metrics.total_workflows)?;
        writeln!(f, "Running Workflows: {}", self.performance.workflow_metrics.running_workflows)?;
        writeln!(f, "Completed Workflows: {}", self.performance.workflow_metrics.completed_workflows)?;
        writeln!(f, "Failed Workflows: {}", self.performance.workflow_metrics.failed_workflows)?;
        writeln!(f, "Throughput: {:.2} workflows/sec", self.performance.workflow_metrics.throughput)?;
        writeln!(f, "Custom Metrics: {}", self.custom_metrics_count)?;
        writeln!(f, "Prometheus Enabled: {}", self.prometheus_enabled)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MetricsConfig;

    #[tokio::test]
    async fn test_metrics_manager_basic() {
        let config = MetricsConfig {
            enabled: true,
            port: 9090,
            path: "/metrics".to_string(),
            retention_days: 7,
            export_interval: 60,
        };
        
        let mut manager = MetricsManager::new(&config).await.unwrap();
        manager.init().await.unwrap();
        
        // Test basic metrics recording
        manager.record_workflow_start("test_workflow").await.unwrap();
        manager.record_workflow_completion("test_workflow", Duration::from_secs(5), true).await.unwrap();
        
        // Test performance metrics
        let performance = manager.get_performance_metrics().await;
        assert_eq!(performance.workflow_metrics.total_workflows, 1);
        assert_eq!(performance.workflow_metrics.completed_workflows, 1);
        assert_eq!(performance.workflow_metrics.running_workflows, 0);
        
        // Test metrics summary
        let summary = manager.get_metrics_summary().await;
        assert!(summary.uptime.as_secs() > 0);
        assert_eq!(summary.performance.workflow_metrics.total_workflows, 1);
    }

    #[tokio::test]
    async fn test_metrics_manager_disabled() {
        let config = MetricsConfig {
            enabled: false,
            port: 9090,
            path: "/metrics".to_string(),
            retention_days: 7,
            export_interval: 60,
        };
        
        let mut manager = MetricsManager::new(&config).await.unwrap();
        manager.init().await.unwrap();
        
        // Should not fail even when disabled
        manager.record_workflow_start("test_workflow").await.unwrap();
        
        let performance = manager.get_performance_metrics().await;
        assert_eq!(performance.workflow_metrics.total_workflows, 1);
    }
} 