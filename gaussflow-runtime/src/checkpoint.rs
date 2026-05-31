//! Checkpoint persistence for resumable execution (Phase 3).
//!
//! The executor writes a [`Checkpoint`] after each node so that an interrupted run can be
//! **resumed** to the correct final state. We reuse the canonical `gaussflow_core::model::Checkpoint`
//! (status + per-node results) and key stores by `run_id`. A [`NoopCheckpointStore`] (the default)
//! disables checkpointing; in-memory and file-backed stores are provided.

use async_trait::async_trait;
use gaussflow_core::model::Checkpoint;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

/// Error type for checkpoint backends.
pub type CheckpointError = Box<dyn std::error::Error + Send + Sync>;

/// Persistence backend for execution checkpoints, keyed by run id.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Persist the latest checkpoint for a run (overwrites the previous one).
    async fn save(&self, run_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError>;
    /// Load the checkpoint for a run, if any.
    async fn load(&self, run_id: &str) -> Result<Option<Checkpoint>, CheckpointError>;
    /// Remove the checkpoint for a run.
    async fn clear(&self, run_id: &str) -> Result<(), CheckpointError>;
}

/// No-op store: checkpointing disabled (the default for non-resumable execution).
#[derive(Debug, Default)]
pub struct NoopCheckpointStore;

#[async_trait]
impl CheckpointStore for NoopCheckpointStore {
    async fn save(&self, _run_id: &str, _checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        Ok(())
    }
    async fn load(&self, _run_id: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(None)
    }
    async fn clear(&self, _run_id: &str) -> Result<(), CheckpointError> {
        Ok(())
    }
}

/// In-memory checkpoint store (no external services).
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    map: Mutex<HashMap<String, Checkpoint>>,
}

impl InMemoryCheckpointStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save(&self, run_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        self.map
            .lock()
            .unwrap()
            .insert(run_id.to_string(), checkpoint.clone());
        Ok(())
    }
    async fn load(&self, run_id: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.map.lock().unwrap().get(run_id).cloned())
    }
    async fn clear(&self, run_id: &str) -> Result<(), CheckpointError> {
        self.map.lock().unwrap().remove(run_id);
        Ok(())
    }
}

/// File-backed checkpoint store: one JSON file `<dir>/<run_id>.json` per run. Durable across
/// processes, so a run interrupted by a crash can be resumed by a fresh process.
#[derive(Debug, Clone)]
pub struct FileCheckpointStore {
    dir: PathBuf,
}

impl FileCheckpointStore {
    /// Create a store rooted at `dir`, creating the directory if needed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, CheckpointError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.json"))
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn save(&self, run_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        // Write to a temp file then rename, so a crash mid-write can't corrupt the checkpoint.
        let final_path = self.path(run_id);
        let tmp_path = self.dir.join(format!("{run_id}.json.tmp"));
        std::fs::write(&tmp_path, serde_json::to_vec(checkpoint)?)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }
    async fn load(&self, run_id: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let path = self.path(run_id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&std::fs::read(&path)?)?))
    }
    async fn clear(&self, run_id: &str) -> Result<(), CheckpointError> {
        let path = self.path(run_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}
