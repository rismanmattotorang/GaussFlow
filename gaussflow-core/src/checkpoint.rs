//! Checkpointing functionality for workflow execution

use crate::{
    model::{Checkpoint, WorkflowStatus},
    DagError,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Trait for checkpoint storage
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Saves a checkpoint
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), DagError>;

    /// Loads the latest checkpoint for a workflow
    async fn load_latest_checkpoint(
        &self,
        workflow_id: &str,
    ) -> Result<Option<Checkpoint>, DagError>;

    /// Lists all checkpoints for a workflow
    async fn list_checkpoints(&self, workflow_id: &str) -> Result<Vec<Checkpoint>, DagError>;

    /// Deletes a checkpoint
    async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<(), DagError>;
}

/// File-based checkpoint storage
pub struct FileCheckpointStore {
    base_path: std::path::PathBuf,
}

impl FileCheckpointStore {
    /// Creates a new file-based checkpoint store
    pub fn new<P: AsRef<Path>>(base_path: P) -> Self {
        // Ensure the directory exists
        std::fs::create_dir_all(&base_path).expect("Failed to create checkpoint directory");
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    fn get_checkpoint_path(&self, workflow_id: &str, checkpoint_id: &str) -> std::path::PathBuf {
        let mut path = self.base_path.join(workflow_id);
        std::fs::create_dir_all(&path).expect("Failed to create workflow checkpoint directory");
        path.push(format!("{}.json", checkpoint_id));
        path
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), DagError> {
        let path = self.get_checkpoint_path("workflow_id", &checkpoint.id); // TODO: Add workflow_id to Checkpoint
        let json = serde_json::to_vec(checkpoint)?;
        tokio::fs::write(path, json).await?;
        Ok(())
    }

    async fn load_latest_checkpoint(
        &self,
        workflow_id: &str,
    ) -> Result<Option<Checkpoint>, DagError> {
        let dir = self.base_path.join(workflow_id);
        if !dir.exists() {
            return Ok(None);
        }

        let mut latest_checkpoint: Option<Checkpoint> = None;
        let mut latest_timestamp = 0;

        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(metadata) = entry.metadata().await {
                if metadata.is_file() {
                    if let Some(ext) = entry.path().extension() {
                        if ext == "json" {
                            if let Ok(content) = tokio::fs::read(entry.path()).await {
                                if let Ok(checkpoint) =
                                    serde_json::from_slice::<Checkpoint>(&content)
                                {
                                    if checkpoint.timestamp > latest_timestamp {
                                        latest_timestamp = checkpoint.timestamp;
                                        latest_checkpoint = Some(checkpoint);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(latest_checkpoint)
    }

    async fn list_checkpoints(&self, workflow_id: &str) -> Result<Vec<Checkpoint>, DagError> {
        let dir = self.base_path.join(workflow_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut checkpoints = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if let Ok(metadata) = entry.metadata().await {
                if metadata.is_file() {
                    if let Some(ext) = entry.path().extension() {
                        if ext == "json" {
                            if let Ok(content) = tokio::fs::read(entry.path()).await {
                                if let Ok(checkpoint) =
                                    serde_json::from_slice::<Checkpoint>(&content)
                                {
                                    checkpoints.push(checkpoint);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by timestamp, newest first
        checkpoints.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(checkpoints)
    }

    async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<(), DagError> {
        // This is a simplified implementation
        // In a real implementation, you'd need to know the workflow_id
        let path = self
            .base_path
            .join("workflow_id")
            .join(format!("{}.json", checkpoint_id));
        if path.exists() {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }
}

/// Manages checkpoints for workflow execution
pub struct CheckpointManager {
    store: Arc<dyn CheckpointStore>,
    current_checkpoint: Option<Checkpoint>,
    workflow_id: String,
    auto_save: bool,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager
    pub fn new(store: Arc<dyn CheckpointStore>, workflow_id: &str, auto_save: bool) -> Self {
        Self {
            store,
            current_checkpoint: None,
            workflow_id: workflow_id.to_string(),
            auto_save,
        }
    }

    /// Loads the latest checkpoint for the workflow
    pub async fn load_latest(&mut self) -> Result<Option<&Checkpoint>, DagError> {
        self.current_checkpoint = self.store.load_latest_checkpoint(&self.workflow_id).await?;
        Ok(self.current_checkpoint.as_ref())
    }

    /// Creates a new checkpoint
    pub async fn create_checkpoint(
        &mut self,
        status: WorkflowStatus,
        last_executed_node: Option<String>,
        node_results: HashMap<String, Value>,
        error: Option<String>,
    ) -> Result<&Checkpoint, DagError> {
        let checkpoint = Checkpoint {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            status,
            last_executed_node,
            node_results,
            error,
        };

        if self.auto_save {
            self.store.save_checkpoint(&checkpoint).await?;
        }

        self.current_checkpoint = Some(checkpoint);
        Ok(self.current_checkpoint.as_ref().unwrap())
    }

    /// Gets the current checkpoint
    pub fn current_checkpoint(&self) -> Option<&Checkpoint> {
        self.current_checkpoint.as_ref()
    }

    /// Saves the current checkpoint
    pub async fn save(&self) -> Result<(), DagError> {
        if let Some(checkpoint) = &self.current_checkpoint {
            self.store.save_checkpoint(checkpoint).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_checkpoint_store() {
        let temp_dir = tempdir().unwrap();
        let store = FileCheckpointStore::new(temp_dir.path());

        let checkpoint = Checkpoint {
            id: "test-checkpoint".to_string(),
            timestamp: 1234567890,
            status: WorkflowStatus::Running,
            last_executed_node: Some("test-node".to_string()),
            node_results: HashMap::new(),
            error: None,
        };

        // Test save and load
        store.save_checkpoint(&checkpoint).await.unwrap();
        let loaded = store
            .load_latest_checkpoint("workflow_id")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.id, checkpoint.id);

        // Test list checkpoints
        let checkpoints = store.list_checkpoints("workflow_id").await.unwrap();
        assert_eq!(checkpoints.len(), 1);

        // Test delete
        store.delete_checkpoint(&checkpoint.id).await.unwrap();
        assert!(store
            .load_latest_checkpoint("workflow_id")
            .await
            .unwrap()
            .is_none());
    }
}
