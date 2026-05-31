//! Versioning support for workflows

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Represents a versioned workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedWorkflow {
    /// The workflow ID
    pub id: String,

    /// The workflow version
    pub version: String,

    /// The workflow content
    pub content: serde_json::Value,

    /// Metadata about the version
    pub metadata: HashMap<String, String>,

    /// When this version was created (ISO 8601 timestamp)
    pub created_at: String,

    /// Who created this version
    pub created_by: Option<String>,
}

/// Error type for versioning operations
#[derive(Debug, Error)]
pub enum VersioningError {
    #[error("Invalid version format: {0}")]
    InvalidVersion(String),

    #[error("Version conflict: {0}")]
    VersionConflict(String),

    #[error("Version not found: {0}")]
    VersionNotFound(String),

    #[error(transparent)]
    SemverError(#[from] semver::Error),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Manages workflow versions
#[async_trait::async_trait]
pub trait VersionManager: Send + Sync {
    /// Saves a new version of a workflow
    async fn save_version(&self, workflow: &VersionedWorkflow) -> Result<(), VersioningError>;

    /// Gets a specific version of a workflow
    async fn get_version(
        &self,
        id: &str,
        version: &str,
    ) -> Result<VersionedWorkflow, VersioningError>;

    /// Lists all versions of a workflow
    async fn list_versions(&self, id: &str) -> Result<Vec<VersionedWorkflow>, VersioningError>;

    /// Deletes a specific version of a workflow
    async fn delete_version(&self, id: &str, version: &str) -> Result<(), VersioningError>;

    /// Gets the latest version of a workflow that matches the version requirement
    async fn get_latest_matching_version(
        &self,
        id: &str,
        version_req: &str,
    ) -> Result<VersionedWorkflow, VersioningError> {
        let versions = self.list_versions(id).await?;
        let req = VersionReq::parse(version_req)?;

        let mut latest: Option<VersionedWorkflow> = None;

        for version in versions {
            if let Ok(ver) = Version::parse(&version.version) {
                if req.matches(&ver) {
                    match &latest {
                        Some(latest_ver) => {
                            let latest_ver_num = Version::parse(&latest_ver.version).unwrap();
                            if ver > latest_ver_num {
                                latest = Some(version);
                            }
                        }
                        None => latest = Some(version),
                    }
                }
            }
        }

        latest.ok_or_else(|| VersioningError::VersionNotFound(version_req.to_string()))
    }
}

/// In-memory implementation of VersionManager for testing
pub struct InMemoryVersionManager {
    versions: dashmap::DashMap<String, Vec<VersionedWorkflow>>,
}

impl InMemoryVersionManager {
    /// Creates a new in-memory version manager
    pub fn new() -> Self {
        Self {
            versions: dashmap::DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl VersionManager for InMemoryVersionManager {
    async fn save_version(&self, workflow: &VersionedWorkflow) -> Result<(), VersioningError> {
        let mut versions = self.versions.entry(workflow.id.clone()).or_default();

        // Check for duplicate version
        if versions.iter().any(|v| v.version == workflow.version) {
            return Err(VersioningError::VersionConflict(format!(
                "Version {} already exists for workflow {}",
                workflow.version, workflow.id
            )));
        }

        versions.push(workflow.clone());
        Ok(())
    }

    async fn get_version(
        &self,
        id: &str,
        version: &str,
    ) -> Result<VersionedWorkflow, VersioningError> {
        self.versions
            .get(id)
            .and_then(|versions| versions.iter().find(|v| v.version == version).cloned())
            .ok_or_else(|| VersioningError::VersionNotFound(version.to_string()))
    }

    async fn list_versions(&self, id: &str) -> Result<Vec<VersionedWorkflow>, VersioningError> {
        Ok(self.versions.get(id).map(|v| v.clone()).unwrap_or_default())
    }

    async fn delete_version(&self, id: &str, version: &str) -> Result<(), VersioningError> {
        if let Some(mut versions) = self.versions.get_mut(id) {
            versions.retain(|v| v.version != version);
            if versions.is_empty() {
                self.versions.remove(id);
            }
        }
        Ok(())
    }
}

impl Default for InMemoryVersionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_test_workflow(id: &str, version: &str) -> VersionedWorkflow {
        VersionedWorkflow {
            id: id.to_string(),
            version: version.to_string(),
            content: serde_json::json!({}),
            metadata: HashMap::new(),
            created_at: Utc::now().to_rfc3339(),
            created_by: Some("test".to_string()),
        }
    }

    #[tokio::test]
    async fn test_version_manager() {
        let manager = InMemoryVersionManager::new();
        let workflow = create_test_workflow("test", "1.0.0");

        // Test saving a version
        manager.save_version(&workflow).await.unwrap();

        // Test getting a version
        let retrieved = manager.get_version("test", "1.0.0").await.unwrap();
        assert_eq!(retrieved.version, "1.0.0");

        // Test duplicate version
        let result = manager.save_version(&workflow).await;
        assert!(matches!(result, Err(VersioningError::VersionConflict(_))));

        // Test listing versions
        let versions = manager.list_versions("test").await.unwrap();
        assert_eq!(versions.len(), 1);

        // Test getting latest matching version
        let workflow2 = create_test_workflow("test", "2.0.0");
        manager.save_version(&workflow2).await.unwrap();

        let latest = manager
            .get_latest_matching_version("test", ">=1.0.0, <3.0.0")
            .await
            .unwrap();
        assert_eq!(latest.version, "2.0.0");

        // Test deleting a version
        manager.delete_version("test", "1.0.0").await.unwrap();
        let versions = manager.list_versions("test").await.unwrap();
        assert_eq!(versions.len(), 1);
    }
}
