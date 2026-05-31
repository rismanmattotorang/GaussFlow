use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use crate::ResourceSpec;

/// Authentication policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationPolicy {
    pub required: bool,
    pub methods: Vec<String>,
    pub timeout_secs: u64,
}

/// Authorization policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationPolicy {
    pub required: bool,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
}

/// Encryption policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionPolicy {
    pub required: bool,
    pub algorithm: String,
    pub key_size: u32,
}

/// PII protection policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiProtectionPolicy {
    pub enabled: bool,
    pub detection_rules: Vec<String>,
    pub masking_strategy: String,
}
impl Default for PiiProtectionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            detection_rules: Vec::new(),
            masking_strategy: "redact".to_string(),
        }
    }
}

/// Core node types supported by GaussFlow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeType {
    LlmCall,
    Agent,
    Ensemble,
    Router,
    Subgraph,
    Transform,
    Filter,
    Join,
    Split,
    Custom(String),
}

/// Node specification containing all configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: String,
    pub name: String,
    pub node_type: NodeType,
    pub inputs: HashMap<String, Value>,
    pub outputs: HashMap<String, Value>,
    pub metadata: HashMap<String, Value>,
    pub resource_spec: ResourceSpec,
    pub priority: u8,
    pub retry_policy: RetryPolicy,
    pub timeout_ms: u64,
    pub policies: Vec<Policy>,
}

/// Retry policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff_strategy: BackoffStrategy,
    pub conditions: Vec<RetryCondition>,
}

/// Backoff strategy for retry policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed { delay_ms: u64 },
    Exponential { base_ms: u64, factor: f64 },
    Linear { delay_ms: u64 },
}

/// Conditions that trigger a retry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryCondition {
    Always,
    OnError,
    OnTimeout,
    OnSpecificError(String),
    OnResourceExhaustion,
}

/// Policies that can be attached to nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Policy {
    ResourceLimit(ResourceLimitPolicy),
    Security(SecurityPolicy),
    Monitoring(MonitoringPolicy),
    Audit(AuditPolicy),
    Custom(String, Value),
}

/// Resource limit policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitPolicy {
    pub cpu: Option<u32>,
    pub gpu: Option<u32>,
    pub memory: Option<String>,
    pub concurrency: Option<u32>,
    pub labels: Option<HashMap<String, String>>,
}

/// Security policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityPolicy {
    /// Authentication settings
    pub authentication: Option<AuthenticationPolicy>,
    /// Authorization settings
    pub authorization: Option<AuthorizationPolicy>,
    /// Data encryption settings
    pub encryption: Option<EncryptionPolicy>,
    /// PII protection settings
    pub pii_protection: Option<PiiProtectionPolicy>,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            authentication: None,
            authorization: None,
            encryption: None,
            pii_protection: Some(PiiProtectionPolicy::default()),
        }
    }
}

/// Monitoring policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringPolicy {
    pub metrics: Vec<Metric>,
    pub tracing: Option<TracingPolicy>,
    pub alerts: Vec<Alert>,
}

/// Audit policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditPolicy {
    pub log_level: LogLevel,
    pub retention_days: u32,
    pub pii_redaction: bool,
    pub audit_trail: bool,
}

/// Metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub kind: MetricKind,
    pub labels: HashMap<String, String>,
    pub aggregation: Option<Aggregation>,
}

/// Tracing policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingPolicy {
    pub sampling_rate: f64,
    pub span_attributes: HashMap<String, String>,
    pub export_config: ExportConfig,
}

/// Alert definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub notification: Notification,
}

/// Various enums for policy definitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregation {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    GreaterThan(f64),
    LessThan(f64),
    EqualTo(f64),
    RateOfChange(f64),
    AnomalyDetection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Notification {
    Email,
    Slack,
    PagerDuty,
    Webhook,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportConfig {
    Console,
    File,
    Http,
    Custom(String),
}

/// Error type for node operations
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("Node validation failed: {0}")]
    Validation(String),
    
    #[error("Node execution failed: {0}")]
    Execution(String),
    
    #[error("Node configuration error: {0}")]
    Configuration(String),
    
    #[error("Resource allocation failed: {0}")]
    Resource(String),
    
    #[error("Policy enforcement failed: {0}")]
    Policy(String),
}
