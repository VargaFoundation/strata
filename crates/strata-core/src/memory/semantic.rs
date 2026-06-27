//! Semantic memory store — HNSW vector index with metadata.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// A semantic memory entry with vector embedding and metadata (used for upsert).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEntry {
    pub id: Uuid,
    pub content: String,
    pub embedding: Vec<f32>,
    pub metadata: serde_json::Value,
}

/// Lightweight metadata stored in the DashMap (no embedding vector).
///
/// This avoids duplicating the embedding between USearch and DashMap,
/// halving memory usage per entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub id: Uuid,
    pub content: String,
    pub metadata: serde_json::Value,
}

/// Search result with similarity score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub entry: EntryMetadata,
    pub score: f32,
}

/// Vector storage backed by USearch HNSW index.
pub struct SemanticStore {
    index: Mutex<usearch::Index>,
    /// Maps USearch u64 key → EntryMetadata (no embedding — stored only in USearch)
    entries: DashMap<u64, EntryMetadata>,
    /// Maps UUID → USearch u64 key
    uuid_to_key: DashMap<Uuid, u64>,
    /// Auto-incrementing key counter
    next_key: AtomicU64,
    dimension: usize,
}

impl std::fmt::Debug for SemanticStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticStore")
            .field("dimension", &self.dimension)
            .field("size", &self.entries.len())
            .finish()
    }
}

impl SemanticStore {
    /// Create a new semantic store with the given vector dimension.
    pub fn with_dimension(dimension: usize) -> crate::Result<Self> {
        let options = usearch::ffi::IndexOptions {
            dimensions: dimension,
            metric: usearch::ffi::MetricKind::Cos,
            quantization: usearch::ffi::ScalarKind::F32,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            multi: false,
        };

        let index = usearch::Index::new(&options)
            .map_err(|e| crate::Error::Storage(format!("failed to create index: {e}")))?;

        index
            .reserve(1024)
            .map_err(|e| crate::Error::Storage(format!("failed to reserve: {e}")))?;

        Ok(Self {
            index: Mutex::new(index),
            entries: DashMap::new(),
            uuid_to_key: DashMap::new(),
            next_key: AtomicU64::new(1),
            dimension,
        })
    }

    /// Create a new semantic store with default dimension (768).
    pub fn new() -> Self {
        Self::with_dimension(768).expect("failed to create semantic store")
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return up to `limit` entries (for SQL-based listing, no vector query needed).
    pub async fn search_all(&self, limit: usize) -> crate::Result<Vec<EntryMetadata>> {
        let mut results = Vec::with_capacity(limit.min(self.entries.len()));
        for entry in self.entries.iter() {
            if results.len() >= limit {
                break;
            }
            results.push(entry.value().clone());
        }
        Ok(results)
    }

    /// The vector dimension.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Upsert an entry into the semantic store.
    pub async fn upsert(&self, entry: &SemanticEntry) -> crate::Result<()> {
        if entry.embedding.len() != self.dimension {
            return Err(crate::Error::Storage(format!(
                "embedding dimension mismatch: expected {}, got {}",
                self.dimension,
                entry.embedding.len()
            )));
        }

        // Remove old entry if exists
        if let Some((_, old_key)) = self.uuid_to_key.remove(&entry.id) {
            let index = self.index.lock();
            let _ = index.remove(old_key);
            self.entries.remove(&old_key);
        }

        // Assign new key
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);

        // Add to USearch index
        {
            let index = self.index.lock();
            // Ensure capacity
            if index.size() >= index.capacity() {
                index
                    .reserve(index.capacity() * 2 + 1024)
                    .map_err(|e| crate::Error::Storage(format!("reserve failed: {e}")))?;
            }
            index
                .add(key, &entry.embedding)
                .map_err(|e| crate::Error::Storage(format!("add to index failed: {e}")))?;
        }

        // Store metadata only (embedding lives in USearch only)
        self.entries.insert(
            key,
            EntryMetadata {
                id: entry.id,
                content: entry.content.clone(),
                metadata: entry.metadata.clone(),
            },
        );
        self.uuid_to_key.insert(entry.id, key);

        Ok(())
    }

    /// Search for the k nearest neighbors to the given vector.
    pub async fn search(&self, vector: &[f32], k: usize) -> crate::Result<Vec<SearchResult>> {
        if k == 0 || self.entries.is_empty() {
            return Ok(vec![]);
        }

        if vector.len() != self.dimension {
            return Err(crate::Error::Query(format!(
                "query dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            )));
        }

        let matches = {
            let index = self.index.lock();
            index
                .search(vector, k)
                .map_err(|e| crate::Error::Query(format!("search failed: {e}")))?
        };

        let mut results = Vec::with_capacity(matches.keys.len());
        for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            if let Some(entry) = self.entries.get(key) {
                // USearch returns distance — convert to similarity score
                // For cosine: similarity = 1 - distance
                let score = 1.0 - distance;
                results.push(SearchResult {
                    entry: entry.value().clone(),
                    score,
                });
            }
        }

        Ok(results)
    }

    /// Search with metadata filters (post-filter approach).
    ///
    /// Fetches k * oversample_factor candidates from the index, then filters by
    /// the provided predicate, returning at most k results.
    pub async fn search_filtered<F>(
        &self,
        vector: &[f32],
        k: usize,
        filter: F,
    ) -> crate::Result<Vec<SearchResult>>
    where
        F: Fn(&EntryMetadata) -> bool,
    {
        if k == 0 || self.entries.is_empty() {
            return Ok(vec![]);
        }

        if vector.len() != self.dimension {
            return Err(crate::Error::Query(format!(
                "query dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            )));
        }

        // Oversample to compensate for filtered-out results
        let oversample = (k * 4).min(self.entries.len());

        let matches = {
            let index = self.index.lock();
            index
                .search(vector, oversample)
                .map_err(|e| crate::Error::Query(format!("search failed: {e}")))?
        };

        let mut results = Vec::with_capacity(k);
        for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            if results.len() >= k {
                break;
            }
            if let Some(entry) = self.entries.get(key) {
                if filter(entry.value()) {
                    let score = 1.0 - distance;
                    results.push(SearchResult {
                        entry: entry.value().clone(),
                        score,
                    });
                }
            }
        }

        Ok(results)
    }

    /// Delete an entry by UUID.
    pub async fn delete(&self, id: Uuid) -> crate::Result<()> {
        if let Some((_, key)) = self.uuid_to_key.remove(&id) {
            let index = self.index.lock();
            let _ = index.remove(key);
            self.entries.remove(&key);
        }
        Ok(())
    }

    /// Delete every vector whose metadata `tenant_id` matches (GDPR tenant erasure). Returns count.
    pub async fn delete_by_tenant(&self, tenant: &str) -> crate::Result<u64> {
        let ids: Vec<Uuid> = self
            .entries
            .iter()
            .filter(|e| {
                e.value().metadata.get("tenant_id").and_then(|v| v.as_str()) == Some(tenant)
            })
            .map(|e| e.value().id)
            .collect();
        let n = ids.len() as u64;
        for id in ids {
            self.delete(id).await?;
        }
        Ok(n)
    }

    /// Save the index and metadata to disk for persistence.
    ///
    /// Saves the USearch index to `{dir}/index.usearch` and metadata to `{dir}/metadata.json`.
    pub fn save(&self, dir: &Path) -> crate::Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;

        // Save USearch index
        let index_path = dir.join("index.usearch");
        let index_str = index_path.to_string_lossy();
        {
            let index = self.index.lock();
            index
                .save(&index_str)
                .map_err(|e| crate::Error::Storage(format!("save index: {e}")))?;
        }

        // Save metadata (entries + uuid_to_key + next_key + dimension)
        let meta = SerializedMetadata {
            entries: self
                .entries
                .iter()
                .map(|e| (*e.key(), e.value().clone()))
                .collect(),
            uuid_to_key: self
                .uuid_to_key
                .iter()
                .map(|e| (*e.key(), *e.value()))
                .collect(),
            next_key: self.next_key.load(Ordering::Relaxed),
            dimension: self.dimension,
        };
        let meta_path = dir.join("metadata.json");
        let meta_json = serde_json::to_string(&meta)
            .map_err(|e| crate::Error::Storage(format!("serialize metadata: {e}")))?;
        std::fs::write(&meta_path, meta_json)
            .map_err(|e| crate::Error::Storage(format!("write metadata: {e}")))?;

        tracing::info!(entries = self.entries.len(), path = %dir.display(), "semantic store saved");
        Ok(())
    }

    /// Load a previously saved semantic store from disk.
    pub fn load(dir: &Path) -> crate::Result<Self> {
        let meta_path = dir.join("metadata.json");
        let meta_json = std::fs::read_to_string(&meta_path)
            .map_err(|e| crate::Error::Storage(format!("read metadata: {e}")))?;
        let meta: SerializedMetadata = serde_json::from_str(&meta_json)
            .map_err(|e| crate::Error::Storage(format!("parse metadata: {e}")))?;

        let index_path = dir.join("index.usearch");
        let index_str = index_path.to_string_lossy();
        let options = usearch::ffi::IndexOptions {
            dimensions: meta.dimension,
            metric: usearch::ffi::MetricKind::Cos,
            quantization: usearch::ffi::ScalarKind::F32,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            multi: false,
        };
        let index = usearch::Index::new(&options)
            .map_err(|e| crate::Error::Storage(format!("create index: {e}")))?;
        index
            .load(&index_str)
            .map_err(|e| crate::Error::Storage(format!("load index: {e}")))?;

        let entries = DashMap::new();
        for (k, v) in meta.entries {
            entries.insert(k, v);
        }
        let uuid_to_key = DashMap::new();
        for (k, v) in meta.uuid_to_key {
            uuid_to_key.insert(k, v);
        }

        tracing::info!(entries = entries.len(), path = %dir.display(), "semantic store loaded");
        Ok(Self {
            index: Mutex::new(index),
            entries,
            uuid_to_key,
            next_key: AtomicU64::new(meta.next_key),
            dimension: meta.dimension,
        })
    }

    /// Reload this store's contents from a saved directory on disk.
    ///
    /// This replaces the current in-memory index and metadata with the
    /// contents from disk, used for Raft snapshot restore.
    pub fn load_from(&self, dir: &Path) -> crate::Result<()> {
        let meta_path = dir.join("metadata.json");
        let meta_json = std::fs::read_to_string(&meta_path)
            .map_err(|e| crate::Error::Storage(format!("read metadata: {e}")))?;
        let meta: SerializedMetadata = serde_json::from_str(&meta_json)
            .map_err(|e| crate::Error::Storage(format!("parse metadata: {e}")))?;

        let index_path = dir.join("index.usearch");
        let index_str = index_path.to_string_lossy();
        let options = usearch::ffi::IndexOptions {
            dimensions: meta.dimension,
            metric: usearch::ffi::MetricKind::Cos,
            quantization: usearch::ffi::ScalarKind::F32,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            multi: false,
        };
        let new_index = usearch::Index::new(&options)
            .map_err(|e| crate::Error::Storage(format!("create index: {e}")))?;
        new_index
            .load(&index_str)
            .map_err(|e| crate::Error::Storage(format!("load index: {e}")))?;

        // Replace the current index
        {
            let mut index = self.index.lock();
            *index = new_index;
        }

        // Replace metadata
        self.entries.clear();
        for (k, v) in meta.entries {
            self.entries.insert(k, v);
        }
        self.uuid_to_key.clear();
        for (k, v) in meta.uuid_to_key {
            self.uuid_to_key.insert(k, v);
        }
        self.next_key.store(meta.next_key, Ordering::Relaxed);

        tracing::info!(entries = self.entries.len(), path = %dir.display(), "semantic store reloaded from snapshot");
        Ok(())
    }
}

/// Serialization format for semantic store metadata.
#[derive(Serialize, Deserialize)]
struct SerializedMetadata {
    entries: Vec<(u64, EntryMetadata)>,
    uuid_to_key: Vec<(Uuid, u64)>,
    next_key: u64,
    dimension: usize,
}

impl Default for SemanticStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Multi-modal vector store: one HNSW index **per modality**, each with its own dimension fixed on
/// first upsert. This lets different modalities (e.g. 768-d text + 512-d CLIP image vectors) coexist
/// — they can't share a single index because HNSW requires a uniform dimension.
#[derive(Debug, Default)]
pub struct MultiModalStore {
    indexes: DashMap<String, std::sync::Arc<SemanticStore>>,
}

impl MultiModalStore {
    pub fn new() -> Self {
        Self {
            indexes: DashMap::new(),
        }
    }

    /// Get (or lazily create, with `dim`) the index for a modality.
    fn index_for(&self, modality: &str, dim: usize) -> std::sync::Arc<SemanticStore> {
        self.indexes
            .entry(modality.to_string())
            .or_insert_with(|| {
                std::sync::Arc::new(
                    SemanticStore::with_dimension(dim).unwrap_or_else(|_| SemanticStore::new()),
                )
            })
            .clone()
    }

    /// Upsert into the modality's index (created with this vector's dimension if new).
    pub async fn upsert(&self, modality: &str, entry: &SemanticEntry) -> crate::Result<()> {
        let idx = self.index_for(modality, entry.embedding.len());
        idx.upsert(entry).await
    }

    /// Search one modality's index. Empty if the modality has no entries yet.
    pub async fn search(
        &self,
        modality: &str,
        vector: &[f32],
        k: usize,
    ) -> crate::Result<Vec<SearchResult>> {
        match self.indexes.get(modality) {
            Some(idx) => idx.search(vector, k).await,
            None => Ok(vec![]),
        }
    }

    /// Search across every modality whose dimension matches the query vector, merged by score.
    pub async fn search_all(&self, vector: &[f32], k: usize) -> crate::Result<Vec<SearchResult>> {
        let stores: Vec<_> = self.indexes.iter().map(|e| e.value().clone()).collect();
        let mut merged = Vec::new();
        for store in stores {
            if store.dimension() == vector.len() {
                if let Ok(hits) = store.search(vector, k).await {
                    merged.extend(hits);
                }
            }
        }
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(k);
        Ok(merged)
    }

    /// Known modalities.
    pub fn modalities(&self) -> Vec<String> {
        self.indexes.iter().map(|e| e.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(content: &str, embedding: Vec<f32>) -> SemanticEntry {
        SemanticEntry {
            id: Uuid::new_v4(),
            content: content.into(),
            embedding,
            metadata: serde_json::json!({"source": "test"}),
        }
    }

    #[tokio::test]
    async fn new_store_is_empty() {
        let store = SemanticStore::with_dimension(4).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.dimension(), 4);
    }

    #[tokio::test]
    async fn upsert_and_search() {
        let store = SemanticStore::with_dimension(4).unwrap();

        // Insert 3 vectors
        store
            .upsert(&make_entry("cat", vec![1.0, 0.0, 0.0, 0.0]))
            .await
            .unwrap();
        store
            .upsert(&make_entry("dog", vec![0.9, 0.1, 0.0, 0.0]))
            .await
            .unwrap();
        store
            .upsert(&make_entry("fish", vec![0.0, 0.0, 1.0, 0.0]))
            .await
            .unwrap();

        assert_eq!(store.len(), 3);

        // Search for something similar to "cat"
        let results = store.search(&[1.0, 0.0, 0.0, 0.0], 2).await.unwrap();
        assert_eq!(results.len(), 2);
        // "cat" should be the closest match
        assert_eq!(results[0].entry.content, "cat");
        assert!(results[0].score > 0.9);
    }

    #[tokio::test]
    async fn search_empty_store() {
        let store = SemanticStore::with_dimension(4).unwrap();
        let results = store.search(&[1.0, 0.0, 0.0, 0.0], 5).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn search_with_zero_k() {
        let store = SemanticStore::with_dimension(4).unwrap();
        store
            .upsert(&make_entry("a", vec![1.0, 0.0, 0.0, 0.0]))
            .await
            .unwrap();
        let results = store.search(&[1.0, 0.0, 0.0, 0.0], 0).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn upsert_overwrites() {
        let store = SemanticStore::with_dimension(4).unwrap();
        let id = Uuid::new_v4();

        let entry1 = SemanticEntry {
            id,
            content: "version 1".into(),
            embedding: vec![1.0, 0.0, 0.0, 0.0],
            metadata: serde_json::json!({}),
        };
        store.upsert(&entry1).await.unwrap();

        let entry2 = SemanticEntry {
            id,
            content: "version 2".into(),
            embedding: vec![0.0, 1.0, 0.0, 0.0],
            metadata: serde_json::json!({}),
        };
        store.upsert(&entry2).await.unwrap();

        // Should still have 1 entry
        assert_eq!(store.len(), 1);

        // Search for the new vector
        let results = store.search(&[0.0, 1.0, 0.0, 0.0], 1).await.unwrap();
        assert_eq!(results[0].entry.content, "version 2");
    }

    #[tokio::test]
    async fn delete_entry() {
        let store = SemanticStore::with_dimension(4).unwrap();
        let entry = make_entry("to delete", vec![1.0, 0.0, 0.0, 0.0]);
        let id = entry.id;
        store.upsert(&entry).await.unwrap();
        assert_eq!(store.len(), 1);

        store.delete(id).await.unwrap();
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let store = SemanticStore::with_dimension(4).unwrap();
        store.delete(Uuid::new_v4()).await.unwrap();
    }

    #[tokio::test]
    async fn dimension_mismatch_on_upsert() {
        let store = SemanticStore::with_dimension(4).unwrap();
        let entry = make_entry("wrong dim", vec![1.0, 0.0]); // 2 dims, expect 4
        let result = store.upsert(&entry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn dimension_mismatch_on_search() {
        let store = SemanticStore::with_dimension(4).unwrap();
        store
            .upsert(&make_entry("a", vec![1.0, 0.0, 0.0, 0.0]))
            .await
            .unwrap();
        let result = store.search(&[1.0, 0.0], 1).await;
        assert!(result.is_err());
    }

    #[test]
    fn entry_serialization_roundtrip() {
        let entry = make_entry("hello", vec![0.1, 0.2, 0.3, 0.4]);
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: SemanticEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.content, "hello");
        assert_eq!(deserialized.embedding.len(), 4);
    }

    #[test]
    fn search_result_serialization() {
        let result = SearchResult {
            entry: EntryMetadata {
                id: Uuid::new_v4(),
                content: "result".into(),
                metadata: serde_json::json!({"source": "test"}),
            },
            score: 0.95,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SearchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.score, 0.95);
    }

    #[tokio::test]
    async fn many_vectors() {
        let store = SemanticStore::with_dimension(4).unwrap();
        for i in 0..100 {
            let v = vec![i as f32 / 100.0, 0.0, 0.0, 1.0 - i as f32 / 100.0];
            store
                .upsert(&make_entry(&format!("entry-{i}"), v))
                .await
                .unwrap();
        }
        assert_eq!(store.len(), 100);

        let results = store.search(&[1.0, 0.0, 0.0, 0.0], 3).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn save_and_load_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("semantic");

        // Create store, add entries, save
        {
            let store = SemanticStore::with_dimension(4).unwrap();
            store
                .upsert(&make_entry("cat", vec![1.0, 0.0, 0.0, 0.0]))
                .await
                .unwrap();
            store
                .upsert(&make_entry("dog", vec![0.9, 0.1, 0.0, 0.0]))
                .await
                .unwrap();
            assert_eq!(store.len(), 2);
            store.save(&save_path).unwrap();
        }

        // Load and verify
        {
            let store = SemanticStore::load(&save_path).unwrap();
            assert_eq!(store.len(), 2);
            assert_eq!(store.dimension(), 4);

            let results = store.search(&[1.0, 0.0, 0.0, 0.0], 1).await.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].entry.content, "cat");
        }
    }
}
