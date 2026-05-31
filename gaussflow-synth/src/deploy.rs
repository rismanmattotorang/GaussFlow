//! Deploy hardening (Phase S).
//!
//! Turning a *confirmed* synthesis result into a durable, versioned, **immutable** deployment with
//! provenance back to the originating prompt, and running it with **run trace-back** (each run is
//! linked to the deployment that produced it). Backends implement [`DeploymentStore`]; an
//! in-memory and a file-backed store are provided.
//!
//! Deployments also record their **required secrets** (resolved at run time via a
//! [`crate::secrets::SecretProvider`]), can be gated by a resource [`Quota`] at deploy time, and
//! carry registered [`Trigger`]s. Still out of scope: an executor that actually *fires* scheduled
//! triggers, and a concrete vault/cloud secret-manager backend (the env-backed provider is built in).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::SynthesisResult;

/// Error type for deployment operations.
pub type DeployError = Box<dyn std::error::Error + Send + Sync>;

/// A durable, immutable record of a deployed workflow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Deployment {
    /// Stable id: `<workflow-name>-v<version>`.
    pub id: String,
    /// Workflow name (from the spec).
    pub name: String,
    /// Monotonic version per workflow name.
    pub version: u32,
    /// The natural-language prompt that produced this workflow (provenance).
    pub prompt: String,
    /// The frozen workflow spec. Never mutated after deployment.
    pub spec_json: String,
    /// SHA-256 of `spec_json` (content address / immutability check).
    pub spec_hash: String,
    /// Creation time, Unix seconds.
    pub created_at_unix: u64,
    /// Names of the secrets this workflow needs at run time (resolved via a `SecretProvider`).
    #[serde(default)]
    pub required_secrets: Vec<String>,
    /// Triggers registered for this deployment (declarations; an executor that fires schedules is
    /// future work).
    #[serde(default)]
    pub triggers: Vec<Trigger>,
}

impl Deployment {
    /// Verify that every required secret is resolvable via `provider`. On failure, returns the
    /// list of missing secret names — call this before running, to fail fast with a clear message.
    pub fn check_secrets(
        &self,
        provider: &dyn crate::secrets::SecretProvider,
    ) -> Result<(), Vec<String>> {
        let missing: Vec<String> = self
            .required_secrets
            .iter()
            .filter(|k| provider.get(k).is_none())
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }
}

/// How a deployment can be invoked. Persisted with the deployment; firing schedules/webhooks is a
/// separate executor concern (not implemented here).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Trigger {
    /// Run on demand.
    Manual,
    /// Run on a cron schedule (the cron expression is stored verbatim).
    Schedule(String),
    /// Run when a webhook at the given path is called.
    Webhook(String),
}

/// Resource limits checked against a plan's [`crate::PlanEstimate`] before deployment.
#[derive(Debug, Clone)]
pub struct Quota {
    /// Maximum number of nodes.
    pub max_nodes: Option<usize>,
    /// Maximum upper-bound model invocations (llm_call + agent budgets).
    pub max_model_invocations: Option<usize>,
    /// Whether external/network calls are permitted at all.
    pub allow_external_calls: bool,
}

impl Default for Quota {
    fn default() -> Self {
        Self {
            max_nodes: None,
            max_model_invocations: None,
            allow_external_calls: true,
        }
    }
}

/// Options for [`deploy_with`].
#[derive(Debug, Clone, Default)]
pub struct DeployOptions {
    /// Optional resource quota to enforce at deploy time.
    pub quota: Option<Quota>,
    /// Triggers to register with the deployment.
    pub triggers: Vec<Trigger>,
}

/// Persistence backend for deployments and their run history.
#[async_trait]
pub trait DeploymentStore: Send + Sync {
    /// Persist a deployment. Implementations MUST reject overwriting an existing id with different
    /// content (immutability).
    async fn put(&self, deployment: &Deployment) -> Result<(), DeployError>;
    /// Fetch a deployment by id.
    async fn get(&self, id: &str) -> Result<Option<Deployment>, DeployError>;
    /// List all deployments.
    async fn list(&self) -> Result<Vec<Deployment>, DeployError>;
    /// Link a run to the deployment that produced it (trace-back).
    async fn record_run(&self, deployment_id: &str, run_id: &str) -> Result<(), DeployError>;
    /// The run ids recorded for a deployment.
    async fn runs_of(&self, deployment_id: &str) -> Result<Vec<String>, DeployError>;
}

/// Hex SHA-256 of a string.
fn sha256_hex(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Collect the secret names a workflow spec needs, by inspecting `llm_call`/`agent` node models
/// (recursing into `subgraph`/`parallel` inline workflows). Deterministic.
pub fn required_secrets(spec: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_required_secrets(spec, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_required_secrets(spec: &Value, out: &mut Vec<String>) {
    let Some(nodes) = spec.get("nodes").and_then(|n| n.as_array()) else {
        return;
    };
    for node in nodes {
        let node_type = node.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let params = node.get("params");
        match node_type {
            "llm_call" | "agent" => {
                let model = params
                    .and_then(|p| p.get("model"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("gpt-3.5-turbo");
                if let Some(secret) = crate::secrets::secret_for_model(model) {
                    out.push(secret.to_string());
                }
            }
            "subgraph" => {
                if let Some(wf) = params.and_then(|p| p.get("workflow")) {
                    collect_required_secrets(wf, out);
                }
            }
            "parallel" => {
                if let Some(branches) = params
                    .and_then(|p| p.get("branches"))
                    .and_then(|b| b.as_array())
                {
                    for branch in branches {
                        collect_required_secrets(branch, out);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Check a plan estimate against a quota. Returns a human-readable reason on violation.
pub fn check_quota(estimate: &crate::PlanEstimate, quota: &Quota) -> Result<(), String> {
    if let Some(max) = quota.max_nodes {
        if estimate.node_count > max {
            return Err(format!(
                "node count {} exceeds quota of {max}",
                estimate.node_count
            ));
        }
    }
    if let Some(max) = quota.max_model_invocations {
        if estimate.max_model_invocations > max {
            return Err(format!(
                "model-invocation upper bound {} exceeds quota of {max}",
                estimate.max_model_invocations
            ));
        }
    }
    if !quota.allow_external_calls && estimate.makes_external_calls {
        return Err("external calls are not permitted by quota".to_string());
    }
    Ok(())
}

/// Deploy a confirmed synthesis result with default options (no quota, no triggers).
pub async fn deploy(
    result: &SynthesisResult,
    prompt: &str,
    store: &dyn DeploymentStore,
) -> Result<Deployment, DeployError> {
    deploy_with(result, prompt, DeployOptions::default(), store).await
}

/// Deploy a confirmed synthesis result: enforce any quota, assign the next version for its
/// workflow name, freeze the spec with a content hash, record the originating `prompt`, the
/// required secrets, and any triggers, then persist it.
pub async fn deploy_with(
    result: &SynthesisResult,
    prompt: &str,
    options: DeployOptions,
    store: &dyn DeploymentStore,
) -> Result<Deployment, DeployError> {
    if let Some(quota) = &options.quota {
        check_quota(&result.estimate, quota)
            .map_err(|reason| -> DeployError { format!("quota exceeded: {reason}").into() })?;
    }

    let name = result
        .spec
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("synthesized")
        .to_string();

    let version = store
        .list()
        .await?
        .iter()
        .filter(|d| d.name == name)
        .map(|d| d.version)
        .max()
        .unwrap_or(0)
        + 1;

    let deployment = Deployment {
        id: format!("{name}-v{version}"),
        name,
        version,
        prompt: prompt.to_string(),
        spec_hash: sha256_hex(&result.spec_json),
        required_secrets: required_secrets(&result.spec),
        triggers: options.triggers,
        spec_json: result.spec_json.clone(),
        created_at_unix: now_unix(),
    };
    store.put(&deployment).await?;
    Ok(deployment)
}

/// Run a deployment on the canonical runtime (no database) and link the run back to the deployment
/// (and thus to its originating prompt) for auditability.
pub async fn run_deployment(
    deployment: &Deployment,
    input: Value,
    store: &dyn DeploymentStore,
) -> Result<Value, DeployError> {
    let dag = gaussflow_core::TypeSafeDag::from_json(&deployment.spec_json)?;
    let run_store = gaussflow_runtime::InMemoryRunStore::new();
    let result = gaussflow_runtime::execute_with_store(dag, input, &run_store).await?;
    if let Some(run_id) = result.get("run_id").and_then(|v| v.as_str()) {
        store.record_run(&deployment.id, run_id).await?;
    }
    Ok(result)
}

/// In-memory deployment store (no external services).
#[derive(Debug, Default)]
pub struct InMemoryDeploymentStore {
    deployments: Mutex<HashMap<String, Deployment>>,
    runs: Mutex<HashMap<String, Vec<String>>>,
}

impl InMemoryDeploymentStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DeploymentStore for InMemoryDeploymentStore {
    async fn put(&self, deployment: &Deployment) -> Result<(), DeployError> {
        let mut map = self.deployments.lock().unwrap();
        if let Some(existing) = map.get(&deployment.id) {
            if existing.spec_hash != deployment.spec_hash {
                return Err(format!(
                    "deployment '{}' already exists with different content (immutable)",
                    deployment.id
                )
                .into());
            }
            return Ok(()); // identical re-deploy is a no-op
        }
        map.insert(deployment.id.clone(), deployment.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Deployment>, DeployError> {
        Ok(self.deployments.lock().unwrap().get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<Deployment>, DeployError> {
        Ok(self.deployments.lock().unwrap().values().cloned().collect())
    }

    async fn record_run(&self, deployment_id: &str, run_id: &str) -> Result<(), DeployError> {
        self.runs
            .lock()
            .unwrap()
            .entry(deployment_id.to_string())
            .or_default()
            .push(run_id.to_string());
        Ok(())
    }

    async fn runs_of(&self, deployment_id: &str) -> Result<Vec<String>, DeployError> {
        Ok(self
            .runs
            .lock()
            .unwrap()
            .get(deployment_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// File-backed deployment store. Each deployment is a JSON file `<dir>/<id>.json`; run trace-back
/// is appended to `<dir>/<id>.runs`. Durable across processes (e.g. for the CLI).
#[derive(Debug, Clone)]
pub struct FileDeploymentStore {
    dir: PathBuf,
}

impl FileDeploymentStore {
    /// Create a store rooted at `dir`, creating the directory if needed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, DeployError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
}

#[async_trait]
impl DeploymentStore for FileDeploymentStore {
    async fn put(&self, deployment: &Deployment) -> Result<(), DeployError> {
        let path = self.dir.join(format!("{}.json", deployment.id));
        if path.exists() {
            let existing: Deployment = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
            if existing.spec_hash != deployment.spec_hash {
                return Err(format!(
                    "deployment '{}' already exists with different content (immutable)",
                    deployment.id
                )
                .into());
            }
            return Ok(());
        }
        std::fs::write(&path, serde_json::to_string_pretty(deployment)?)?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Deployment>, DeployError> {
        let path = self.dir.join(format!("{id}.json"));
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&std::fs::read_to_string(
            &path,
        )?)?))
    }

    async fn list(&self) -> Result<Vec<Deployment>, DeployError> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    if let Ok(dep) = serde_json::from_str::<Deployment>(&text) {
                        out.push(dep);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn record_run(&self, deployment_id: &str, run_id: &str) -> Result<(), DeployError> {
        use std::io::Write;
        let path = self.dir.join(format!("{deployment_id}.runs"));
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{run_id}")?;
        Ok(())
    }

    async fn runs_of(&self, deployment_id: &str) -> Result<Vec<String>, DeployError> {
        let path = self.dir.join(format!("{deployment_id}.runs"));
        if !path.exists() {
            return Ok(Vec::new());
        }
        Ok(std::fs::read_to_string(&path)?
            .lines()
            .map(|l| l.to_string())
            .collect())
    }
}
