use anyhow::{Context, Result};
use dashmap::DashMap;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use lru::LruCache;
use moka::sync::Cache as MokaCache;
use parking_lot::RwLock;
use serde::{
    de::{DeserializeOwned, MapAccess},
    Deserialize, Serialize,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tracing::{debug, error, info, warn};

use crate::config::CacheConfig;

/// Cache entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    pub value: T,
    pub created_at: Instant,
    pub accessed_at: Instant,
    pub access_count: u64,
    pub size: usize,
}

impl<T> Serialize for CacheEntry<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("CacheEntry", 5)?;
        state.serialize_field("value", &self.value)?;
        state.serialize_field("created_at", &self.created_at.elapsed().as_secs())?;
        state.serialize_field("accessed_at", &self.accessed_at.elapsed().as_secs())?;
        state.serialize_field("access_count", &self.access_count)?;
        state.serialize_field("size", &self.size)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for CacheEntry<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Value,
            CreatedAt,
            AccessedAt,
            AccessCount,
            Size,
        }

        struct CacheEntryVisitor<T> {
            _phantom: std::marker::PhantomData<T>,
        }

        impl<'de, T> serde::de::Visitor<'de> for CacheEntryVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = CacheEntry<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct CacheEntry")
            }

            fn visit_map<V>(self, mut map: V) -> Result<CacheEntry<T>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut value = None;
                let mut created_at = None;
                let mut accessed_at = None;
                let mut access_count = None;
                let mut size = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Value => {
                            if value.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value()?);
                        }
                        Field::CreatedAt => {
                            if created_at.is_some() {
                                return Err(serde::de::Error::duplicate_field("created_at"));
                            }
                            let secs: u64 = map.next_value()?;
                            created_at = Some(Instant::now() - Duration::from_secs(secs));
                        }
                        Field::AccessedAt => {
                            if accessed_at.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessed_at"));
                            }
                            let secs: u64 = map.next_value()?;
                            accessed_at = Some(Instant::now() - Duration::from_secs(secs));
                        }
                        Field::AccessCount => {
                            if access_count.is_some() {
                                return Err(serde::de::Error::duplicate_field("access_count"));
                            }
                            access_count = Some(map.next_value()?);
                        }
                        Field::Size => {
                            if size.is_some() {
                                return Err(serde::de::Error::duplicate_field("size"));
                            }
                            size = Some(map.next_value()?);
                        }
                    }
                }

                let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;
                let created_at =
                    created_at.ok_or_else(|| serde::de::Error::missing_field("created_at"))?;
                let accessed_at =
                    accessed_at.ok_or_else(|| serde::de::Error::missing_field("accessed_at"))?;
                let access_count =
                    access_count.ok_or_else(|| serde::de::Error::missing_field("access_count"))?;
                let size = size.ok_or_else(|| serde::de::Error::missing_field("size"))?;

                Ok(CacheEntry {
                    value,
                    created_at,
                    accessed_at,
                    access_count,
                    size,
                })
            }
        }

        const FIELDS: &[&str] = &["value", "created_at", "accessed_at", "access_count", "size"];
        deserializer.deserialize_struct(
            "CacheEntry",
            FIELDS,
            CacheEntryVisitor {
                _phantom: std::marker::PhantomData,
            },
        )
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub memory_usage: usize,
}

/// Advanced multi-layer cache manager
pub struct CacheManager {
    /// L1: In-memory LRU cache for hot data
    l1_cache: Arc<RwLock<LruCache<String, CacheEntry<Vec<u8>>>>>,

    /// L2: Concurrent cache for frequently accessed data
    l2_cache: Arc<MokaCache<String, CacheEntry<Vec<u8>>>>,

    /// L3: Persistent cache for cold data
    l3_cache: Arc<DashMap<String, CacheEntry<Vec<u8>>>>,

    /// Configuration
    config: CacheConfig,

    /// Statistics
    stats: Arc<RwLock<CacheStats>>,

    /// Compression enabled
    compression: bool,

    /// Persistence enabled
    persistence: bool,

    /// Persistence path
    persistence_path: Option<PathBuf>,
}

impl CacheManager {
    /// Create a new cache manager
    pub async fn new(config: &CacheConfig) -> Result<Self> {
        let l1_cache = Arc::new(RwLock::new(LruCache::new(
            NonZeroUsize::new(config.max_size / 10).unwrap_or(NonZeroUsize::new(100).unwrap()),
        )));

        let l2_cache = Arc::new(
            MokaCache::builder()
                .max_capacity(config.max_size as u64)
                .time_to_live(Duration::from_secs(config.ttl_seconds))
                .build(),
        );

        let l3_cache = Arc::new(DashMap::new());

        let mut manager = Self {
            l1_cache,
            l2_cache,
            l3_cache,
            config: config.clone(),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            compression: config.compression,
            persistence: config.persistence,
            persistence_path: config.persistence_path.clone(),
        };

        // Load persistent cache if enabled
        if config.persistence {
            if let Err(e) = manager.load_persistent_cache().await {
                warn!("Failed to load persistent cache: {}", e);
            }
        }

        // Start background tasks
        manager.start_background_tasks();

        Ok(manager)
    }

    /// Get a value from cache
    pub async fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send + Sync,
    {
        // Try L1 cache first
        if let Some(entry) = self.get_from_l1(key).await {
            self.update_stats(true, entry.size);
            debug!("Cache hit in L1: {}", key);
            return Ok(Some(self.deserialize_value(&entry.value)?));
        }

        // Try L2 cache
        if let Some(entry) = self.l2_cache.get(key) {
            self.update_stats(true, entry.size);
            debug!("Cache hit in L2: {}", key);

            // Promote to L1
            self.promote_to_l1(key, entry.clone()).await;

            return Ok(Some(self.deserialize_value(&entry.value)?));
        }

        // Try L3 cache
        if let Some(entry) = self.l3_cache.get(key) {
            self.update_stats(true, entry.size);
            debug!("Cache hit in L3: {}", key);

            // Promote to L2
            self.promote_to_l2(key, entry.clone()).await;

            return Ok(Some(self.deserialize_value(&entry.value)?));
        }

        self.update_stats(false, 0);
        debug!("Cache miss: {}", key);
        Ok(None)
    }

    /// Set a value in cache
    pub async fn set<T>(&self, key: &str, value: T, _ttl: Option<Duration>) -> Result<()>
    where
        T: Serialize + Send + Sync,
    {
        let serialized = self.serialize_value(&value)?;
        let size = serialized.len();

        let entry = CacheEntry {
            value: serialized,
            created_at: Instant::now(),
            accessed_at: Instant::now(),
            access_count: 1,
            size,
        };

        // Store in all cache layers
        self.set_in_l1(key, entry.clone()).await;
        self.set_in_l2(key, entry.clone()).await;
        self.set_in_l3(key, entry).await;

        debug!("Cached value for key: {} (size: {} bytes)", key, size);
        Ok(())
    }

    /// Remove a value from cache
    pub async fn remove(&self, key: &str) -> Result<bool> {
        // Remove from all layers. NOTE: use `pop` (not `demote`) — `demote` only moves the LRU
        // entry to the back, leaving it in the cache, so the value would still be served.
        let l1_removed = {
            let mut cache = self.l1_cache.write();
            cache.pop(key).is_some()
        };

        let l2_removed = self.l2_cache.remove(key).is_some();
        let l3_removed = self.l3_cache.remove(key).is_some();

        let removed = l1_removed || l2_removed || l3_removed;

        if removed {
            debug!("Removed cache entry: {}", key);
        }

        Ok(removed)
    }

    /// Clear all caches
    pub async fn clear(&self) -> Result<()> {
        self.l1_cache.write().clear();
        self.l2_cache.invalidate_all();
        self.l3_cache.clear();

        // Reset statistics
        let mut stats = self.stats.write();
        *stats = CacheStats::default();

        info!("All caches cleared");
        Ok(())
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let stats = self.stats.read();
        let mut result = stats.clone();

        // Update current size
        result.size =
            self.l1_cache.read().len() + self.l2_cache.entry_count() as usize + self.l3_cache.len();

        result
    }

    /// Get value from L1 cache
    async fn get_from_l1(&self, key: &str) -> Option<CacheEntry<Vec<u8>>> {
        let mut cache = self.l1_cache.write();
        if let Some(entry) = cache.get(key) {
            let mut updated_entry = entry.clone();
            updated_entry.accessed_at = Instant::now();
            updated_entry.access_count += 1;
            cache.put(key.to_string(), updated_entry.clone());
            Some(updated_entry)
        } else {
            None
        }
    }

    /// Set value in L1 cache
    async fn set_in_l1(&self, key: &str, entry: CacheEntry<Vec<u8>>) {
        let mut cache = self.l1_cache.write();
        cache.put(key.to_string(), entry);
    }

    /// Set value in L2 cache
    async fn set_in_l2(&self, key: &str, entry: CacheEntry<Vec<u8>>) {
        self.l2_cache.insert(key.to_string(), entry);
    }

    /// Set value in L3 cache
    async fn set_in_l3(&self, key: &str, entry: CacheEntry<Vec<u8>>) {
        self.l3_cache.insert(key.to_string(), entry);
    }

    /// Promote entry to L1 cache
    async fn promote_to_l1(&self, key: &str, entry: CacheEntry<Vec<u8>>) {
        self.set_in_l1(key, entry).await;
    }

    /// Promote entry to L2 cache
    async fn promote_to_l2(&self, key: &str, entry: CacheEntry<Vec<u8>>) {
        self.set_in_l2(key, entry).await;
    }

    /// Serialize value with optional compression
    fn serialize_value<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        let serialized = bincode::serialize(value).context("Failed to serialize value")?;

        if self.compression {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            std::io::copy(&mut std::io::Cursor::new(&serialized), &mut encoder)
                .context("Failed to compress value")?;
            Ok(encoder.finish().context("Failed to finish compression")?)
        } else {
            Ok(serialized)
        }
    }

    /// Deserialize value with optional decompression
    fn deserialize_value<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T> {
        let decompressed = if self.compression {
            let mut decoder = GzDecoder::new(data);
            let mut result = Vec::new();
            std::io::copy(&mut decoder, &mut result).context("Failed to decompress value")?;
            result
        } else {
            data.to_vec()
        };

        bincode::deserialize(&decompressed).context("Failed to deserialize value")
    }

    /// Update cache statistics
    fn update_stats(&self, hit: bool, size: usize) {
        let mut stats = self.stats.write();
        if hit {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
        stats.memory_usage = stats.memory_usage.saturating_add(size);
    }

    /// Load persistent cache from disk
    async fn load_persistent_cache(&mut self) -> Result<()> {
        if let Some(path) = &self.persistence_path {
            if path.exists() {
                info!("Loading persistent cache from: {}", path.display());

                let data = fs::read(path)
                    .await
                    .context("Failed to read persistent cache file")?;

                let cache_data: HashMap<String, CacheEntry<Vec<u8>>> = bincode::deserialize(&data)
                    .context("Failed to deserialize persistent cache")?;

                let count = cache_data.len();
                for (key, entry) in cache_data {
                    self.l3_cache.insert(key, entry);
                }

                info!("Loaded {} entries from persistent cache", count);
            }
        }
        Ok(())
    }

    /// Save persistent cache to disk
    async fn save_persistent_cache(&self) -> Result<()> {
        if let Some(path) = &self.persistence_path {
            let cache_data: HashMap<String, CacheEntry<Vec<u8>>> = self
                .l3_cache
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            let data =
                bincode::serialize(&cache_data).context("Failed to serialize persistent cache")?;

            fs::write(path, data)
                .await
                .context("Failed to write persistent cache file")?;

            debug!("Saved {} entries to persistent cache", cache_data.len());
        }
        Ok(())
    }

    /// Start background tasks for cache maintenance
    fn start_background_tasks(&self) {
        let _l2_cache = self.l2_cache.clone();

        // Background task for periodic cache cleanup
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
            loop {
                interval.tick().await;
                // MokaCache handles TTL automatically
            }
        });
    }
}

impl Clone for CacheManager {
    fn clone(&self) -> Self {
        Self {
            l1_cache: self.l1_cache.clone(),
            l2_cache: self.l2_cache.clone(),
            l3_cache: self.l3_cache.clone(),
            config: self.config.clone(),
            stats: self.stats.clone(),
            compression: self.compression,
            persistence: self.persistence,
            persistence_path: self.persistence_path.clone(),
        }
    }
}

impl Drop for CacheManager {
    fn drop(&mut self) {
        if self.persistence {
            // Save persistent cache on drop
            if let Some(path) = &self.persistence_path {
                let cache_data: HashMap<String, CacheEntry<Vec<u8>>> = self
                    .l3_cache
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                if let Ok(data) = bincode::serialize(&cache_data) {
                    if let Err(e) = std::fs::write(path, data) {
                        error!("Failed to save persistent cache on drop: {}", e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
        data: Vec<u8>,
    }

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let config = CacheConfig {
            enabled: true,
            max_size: 1000,
            ttl_seconds: 3600,
            eviction_policy: "lru".to_string(),
            compression: false,
            persistence: false,
            persistence_path: None,
        };

        let cache = CacheManager::new(&config).await.unwrap();

        let test_data = TestData {
            id: 1,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        // Test set and get
        cache.set("test_key", &test_data, None).await.unwrap();
        let retrieved: Option<TestData> = cache.get("test_key").await.unwrap();
        assert_eq!(retrieved, Some(test_data));

        // Test remove
        cache.remove("test_key").await.unwrap();
        let retrieved: Option<TestData> = cache.get("test_key").await.unwrap();
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn test_cache_compression() {
        let config = CacheConfig {
            enabled: true,
            max_size: 1000,
            ttl_seconds: 3600,
            eviction_policy: "lru".to_string(),
            compression: true,
            persistence: false,
            persistence_path: None,
        };

        let cache = CacheManager::new(&config).await.unwrap();

        let test_data = TestData {
            id: 1,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        // Test set and get with compression
        cache.set("test_key", &test_data, None).await.unwrap();
        let retrieved: Option<TestData> = cache.get("test_key").await.unwrap();
        assert_eq!(retrieved, Some(test_data));
    }
}
