use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

// Re-export types that are used in the public API
pub use crate::model::WorkflowSpec;
pub use crate::resource::ResourceUsage;

/// Log level for audit logging
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::Info
    }
}

/// Configuration for exporting audit logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    pub enabled: bool,
    pub format: String,  // e.g., "json", "csv"
    pub destination: String,  // e.g., "file:///path", "http://endpoint"
    pub batch_size: usize,
    pub batch_timeout_secs: u64,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            format: "json".to_string(),
            destination: "".to_string(),
            batch_size: 100,
            batch_timeout_secs: 5,
        }
    }
}

/// Policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    pub description: String,
    pub rules: Vec<Rule>,
    pub priority: u8,
    pub enabled: bool,
}

/// Rule definition for policies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub name: String,
    pub description: String,
    pub condition: String,
    pub action: Action,
    pub priority: u8,
}

/// Action to take when a rule is triggered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Allow,
    Deny,
    Log,
    Notify,
    Custom(String),
}

/// Policy engine that enforces policies across the workflow
pub struct PolicyEngine {
    policies: Vec<Policy>,
    validators: Vec<Box<dyn PolicyValidator>>,
    enforcers: Vec<Box<dyn PolicyEnforcer>>,
    audit_trail: AuditTrail,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
            validators: Vec::new(),
            enforcers: Vec::new(),
            audit_trail: AuditTrail::new(),
        }
    }

    pub fn add_policy(&mut self, policy: Policy) {
        self.policies.push(policy);
    }

    pub fn add_validator(&mut self, validator: Box<dyn PolicyValidator>) {
        self.validators.push(validator);
    }

    pub fn add_enforcer(&mut self, enforcer: Box<dyn PolicyEnforcer>) {
        self.enforcers.push(enforcer);
    }

    pub fn validate(&self, spec: &WorkflowSpec) -> Result<(), PolicyError> {
        for validator in &self.validators {
            validator.validate(spec)?;
        }
        Ok(())
    }

    pub fn enforce(&self, ctx: &PolicyContext) -> Result<(), PolicyError> {
        for enforcer in &self.enforcers {
            enforcer.enforce(ctx)?;
        }
        Ok(())
    }

    pub fn audit(&mut self, event: PolicyEvent) {
        self.audit_trail.record(event);
    }
}

/// Policy validation trait
pub trait PolicyValidator: Send + Sync {
    fn validate(&self, spec: &WorkflowSpec) -> Result<(), PolicyError>;
}

/// Policy enforcement trait
pub trait PolicyEnforcer: Send + Sync {
    fn enforce(&self, ctx: &PolicyContext) -> Result<(), PolicyError>;
}

/// Audit trail for policy events
pub struct AuditTrail {
    events: Vec<PolicyEvent>,
    config: AuditConfig,
}

impl AuditTrail {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            config: AuditConfig::default(),
        }
    }

    pub fn record(&mut self, event: PolicyEvent) {
        self.events.push(event);
    }

    pub fn get_events(&self) -> &[PolicyEvent] {
        &self.events
    }
}

/// Policy event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyEvent {
    ValidationPassed { policy: String, timestamp: u64 },
    ValidationFailed { policy: String, error: String, timestamp: u64 },
    EnforcementPassed { policy: String, timestamp: u64 },
    EnforcementFailed { policy: String, error: String, timestamp: u64 },
    AuditRecorded { event: String, timestamp: u64 },
}

/// Policy validation context
pub struct PolicyContext {
    pub workflow_id: String,
    pub node_id: String,
    pub spec: WorkflowSpec,
    pub metadata: HashMap<String, Value>,
    pub resource_usage: ResourceUsage,
    pub security_context: SecurityContext,
}

/// Audit configuration
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    pub enabled: bool,
    pub log_level: LogLevel,
    pub pii_redaction: bool,
    pub retention_days: u32,
    pub export_config: ExportConfig,
}

/// Security context for policy enforcement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityContext {
    pub user_id: String,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
    pub authentication: AuthenticationContext,
    pub authorization: AuthorizationContext,
}

/// Authentication context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationContext {
    pub method: String,
    pub token_type: String,
    pub token_expiry: u64,
    pub claims: HashMap<String, Value>,
}

/// Authorization context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationContext {
    pub policies: Vec<String>,
    pub resource_access: HashMap<String, Vec<String>>,
    pub audit_trail: bool,
}

/// Policy error type
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("Policy validation failed: {0}")]
    Validation(String),
    
    #[error("Policy enforcement failed: {0}")]
    Enforcement(String),
    
    #[error("Policy configuration error: {0}")]
    Configuration(String),
    
    #[error("Policy execution error: {0}")]
    Execution(String),
    
    #[error("Policy audit error: {0}")]
    Audit(String),
}
