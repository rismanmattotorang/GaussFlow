//! Deploy hardening (Phase S).
//!
//! Turning a *confirmed* synthesis result into a durable, versioned, **immutable** deployment with
//! provenance back to the originating prompt, and running it with **run trace-back** (each run is
//! linked to the deployment that produced it). Backends implement [`DeploymentStore`]; an
//! in-memory and a file-backed store are provided.
//!
//! Still out of scope for this version (larger ops concerns): secrets-manager resolution, resource
//! quotas, and trigger/schedule registration.

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

/// Deploy a confirmed synthesis result: assign the next version for its workflow name, freeze the
/// spec with a content hash, record the originating `prompt`, and persist it.
pub async fn deploy(
    result: &SynthesisResult,
    prompt: &str,
    store: &dyn DeploymentStore,
) -> Result<Deployment, DeployError> {
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
