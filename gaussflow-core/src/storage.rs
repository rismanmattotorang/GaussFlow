//! # GaussFlow Storage Layer
//!
//! Content-addressed artifact storage: blobs are keyed by the SHA-256 of their bytes, so identical
//! content is automatically deduplicated and keys are immutable. Two backends are provided: an
//! in-memory store ([`InMemoryStore`]) and a **durable** file-backed store ([`FileContentStore`]).

use crate::DagError;
use async_trait::async_trait;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;

/// Compute the content address (hex SHA-256) of some bytes.
pub fn content_key(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// A generic trait for a content-addressed store.
#[async_trait]
pub trait ContentStore: Send + Sync {
    /// Stores a blob of data and returns its content key (SHA-256 hex). Storing identical bytes is
    /// idempotent and returns the same key.
    async fn put(&self, bytes: &[u8]) -> Result<String, DagError>;
    /// Retrieves a blob of data by its key. Returns [`DagError::ArtifactNotFound`] if absent.
    async fn get(&self, key: &str) -> Result<Vec<u8>, DagError>;
    /// Whether a blob with this key exists.
    async fn has(&self, key: &str) -> Result<bool, DagError>;
}

/// A robust in-memory content store backed by a thread-safe `DashMap`.
#[derive(Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<DashMap<String, Vec<u8>>>,
}

impl InMemoryStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentStore for InMemoryStore {
    async fn put(&self, bytes: &[u8]) -> Result<String, DagError> {
        let key = content_key(bytes);
        self.inner.insert(key.clone(), bytes.to_vec());
        Ok(key)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, DagError> {
        self.inner
            .get(key)
            .map(|v| v.value().clone())
            .ok_or_else(|| DagError::ArtifactNotFound(key.to_string()))
    }

    async fn has(&self, key: &str) -> Result<bool, DagError> {
        Ok(self.inner.contains_key(key))
    }
}

/// A durable, file-backed content-addressed store.
///
/// Each blob is written to `<dir>/<key>` where `<key>` is its SHA-256. Because the filename *is*
/// the content hash, writes are idempotent and naturally deduplicated; existing blobs are never
/// rewritten. Writes go to a temp file then rename, so a crash mid-write can't leave a corrupt blob
/// under a valid key.
#[derive(Debug, Clone)]
pub struct FileContentStore {
    dir: PathBuf,
}

impl FileContentStore {
    /// Create a store rooted at `dir`, creating the directory if needed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, DagError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
}

#[async_trait]
impl ContentStore for FileContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<String, DagError> {
        let key = content_key(bytes);
        let path = self.dir.join(&key);
        if !path.exists() {
            // Atomic publish: write to a unique temp file, then rename into place.
            let tmp = self.dir.join(format!("{key}.{}.tmp", std::process::id()));
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, &path)?;
        }
        Ok(key)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, DagError> {
        let path = self.dir.join(key);
        if !path.exists() {
            return Err(DagError::ArtifactNotFound(key.to_string()));
        }
        Ok(std::fs::read(&path)?)
    }

    async fn has(&self, key: &str) -> Result<bool, DagError> {
        Ok(self.dir.join(key).exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn roundtrip_and_dedup(store: &dyn ContentStore) {
        let key = store.put(b"hello gaussflow").await.unwrap();
        // Content-addressed: the key is the SHA-256 of the bytes.
        assert_eq!(key, content_key(b"hello gaussflow"));
        assert!(store.has(&key).await.unwrap());
        assert_eq!(store.get(&key).await.unwrap(), b"hello gaussflow");

        // Storing identical bytes is idempotent (same key, dedup).
        let key2 = store.put(b"hello gaussflow").await.unwrap();
        assert_eq!(key, key2);

        // Different content → different key.
        let other = store.put(b"different").await.unwrap();
        assert_ne!(key, other);

        // Missing key → ArtifactNotFound.
        assert!(!store.has("deadbeef").await.unwrap());
        assert!(matches!(
            store.get("deadbeef").await,
            Err(DagError::ArtifactNotFound(_))
        ));
    }

    #[tokio::test]
    async fn in_memory_store_roundtrips() {
        roundtrip_and_dedup(&InMemoryStore::new()).await;
    }

    #[tokio::test]
    async fn file_store_roundtrips_and_is_durable() {
        let dir = std::env::temp_dir().join(format!("gf-cas-{}", std::process::id()));
        let store = FileContentStore::new(&dir).unwrap();
        roundtrip_and_dedup(&store).await;

        // Durability: a fresh store over the same dir sees previously-written blobs.
        let key = store.put(b"persist me").await.unwrap();
        let reopened = FileContentStore::new(&dir).unwrap();
        assert_eq!(reopened.get(&key).await.unwrap(), b"persist me");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
