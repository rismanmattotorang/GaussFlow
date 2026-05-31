//! # GaussFlow Storage Layer
//!
//! This module provides abstractions for persistent and in-memory storage.
//! It defines traits for content-addressed storage and caches, with
//! placeholder implementations for SurrealDB and SkyTable.

use crate::DagError;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use dashmap::DashMap;
use std::sync::Arc;

/// A generic trait for a content-addressed store.
#[async_trait]
pub trait ContentStore: Send + Sync {
    /// Stores a blob of data and returns its unique key (e.g., a hash).
    async fn put(&self, bytes: &[u8]) -> Result<String, DagError>;
    /// Retrieves a blob of data by its key.
    async fn get(&self, key: &str) -> Result<Vec<u8>, DagError>;
}

/// A placeholder implementation for a SurrealDB-backed content store.
pub struct SurrealStore;

impl SurrealStore {
    pub async fn new() -> Self {
        // In a real implementation, this would establish a connection
        // to a SurrealDB instance.
        Self
    }
}

#[async_trait]
impl ContentStore for SurrealStore {
    async fn put(&self, _bytes: &[u8]) -> Result<String, DagError> {
        // Dummy implementation for now.
        Ok("dummy-surreal-key".into())
    }

    async fn get(&self, _key: &str) -> Result<Vec<u8>, DagError> {
        // Dummy implementation for now.
        Ok(vec![])
    }
}

/// A robust In-Memory content store backed by a thread-safe `DashMap`.
#[derive(Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<DashMap<String, Vec<u8>>>,
}

#[async_trait]
impl ContentStore for InMemoryStore {
    async fn put(&self, bytes: &[u8]) -> Result<String, DagError> {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let key = format!("{:x}", hasher.finalize());
        self.inner.insert(key.clone(), bytes.to_vec());
        Ok(key)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, DagError> {
        self.inner
            .get(key)
            .map(|v| v.value().clone())
            .ok_or_else(|| DagError::NodeNotFound(key.to_string())) // reuse error for simplicity
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A placeholder for the SkyTable in-memory cache.
pub struct SkyCache;

impl SkyCache {
    pub async fn new() -> Result<Self, DagError> {
        // In a real implementation, this would connect to the SkyTable service.
        Ok(Self)
    }
}
