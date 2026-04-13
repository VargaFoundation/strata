//! Semantic response cache — caches LLM responses keyed by prompt similarity.
//!
//! Uses an in-memory USearch vector index for approximate matching.
//! When a query comes in, it is embedded and compared against cached query vectors.
//! If a cached entry has similarity above the threshold, the cached response is returned.
//! Falls back to exact-match (normalized key) when no embedding provider is available.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cached LLM response with its embedding vector.
struct CachedResponse {
    response: String,
    created_at: Instant,
    /// USearch key for this entry's vector.
    vector_key: Option<u64>,
}

/// Cache for LLM responses with both exact-match and vector similarity lookup.
pub struct SemanticCache {
    /// Exact-match entries keyed by normalized prompt text.
    entries: DashMap<String, CachedResponse>,
    /// Vector index for similarity-based lookup.
    index: Mutex<usearch::Index>,
    /// Maps USearch keys back to normalized prompt keys.
    key_to_prompt: DashMap<u64, String>,
    /// Next key for USearch insertion.
    next_key: AtomicU64,
    /// Minimum similarity score to consider a cache hit (0.0–1.0).
    similarity_threshold: f32,
    ttl: Duration,
    max_entries: usize,
}

impl SemanticCache {
    /// Create a new cache with default settings.
    pub fn new() -> Self {
        Self::with_config(Duration::from_secs(3600), 10_000, 0.95)
    }

    /// Create with custom TTL.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self::with_config(ttl, 10_000, 0.95)
    }

    /// Create with full configuration.
    pub fn with_config(ttl: Duration, max_entries: usize, similarity_threshold: f32) -> Self {
        let options = usearch::ffi::IndexOptions {
            dimensions: 768, // Will be adjusted on first embedding
            metric: usearch::ffi::MetricKind::Cos,
            quantization: usearch::ffi::ScalarKind::F32,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            multi: false,
        };
        let index = usearch::Index::new(&options).expect("failed to create cache index");
        index.reserve(max_entries).ok();

        Self {
            entries: DashMap::new(),
            index: Mutex::new(index),
            key_to_prompt: DashMap::new(),
            next_key: AtomicU64::new(1),
            similarity_threshold,
            ttl,
            max_entries,
        }
    }

    /// Look up a cached response by exact prompt match.
    /// Returns None on cache miss or expired entry.
    pub async fn get(&self, query: &str) -> Option<String> {
        let key = normalize_key(query);

        if let Some(entry) = self.entries.get(&key) {
            if entry.created_at.elapsed() < self.ttl {
                return Some(entry.response.clone());
            }
            // Expired — remove it
            let vector_key = entry.vector_key;
            drop(entry);
            self.entries.remove(&key);
            // Clean up vector index mapping
            if let Some(vk) = vector_key {
                self.key_to_prompt.remove(&vk);
            }
        }

        None
    }

    /// Look up a cached response by vector similarity.
    ///
    /// Searches the vector index for the nearest cached query. If the similarity
    /// score exceeds the threshold, returns the cached response.
    pub async fn get_by_vector(&self, embedding: &[f32]) -> Option<String> {
        if embedding.is_empty() {
            return None;
        }

        let index = self.index.lock();
        if index.size() == 0 {
            return None;
        }

        let results = index.search(embedding, 1).ok()?;
        if results.keys.is_empty() {
            return None;
        }

        let best_key = results.keys[0];
        let best_distance = results.distances[0];
        // USearch cosine returns distance (1 - similarity), convert to similarity
        let similarity = 1.0 - best_distance;

        if similarity < self.similarity_threshold {
            return None;
        }

        // Look up the prompt key from the vector key
        let prompt_key = self.key_to_prompt.get(&best_key)?;
        let prompt = prompt_key.value().clone();
        drop(prompt_key);

        // Check if the entry is still valid
        if let Some(entry) = self.entries.get(&prompt) {
            if entry.created_at.elapsed() < self.ttl {
                return Some(entry.response.clone());
            }
        }

        None
    }

    /// Store a response in the cache with an optional embedding vector.
    pub async fn put(&self, query: &str, response: &str) {
        self.put_with_vector(query, response, None).await;
    }

    /// Store a response with its embedding vector for similarity-based retrieval.
    pub async fn put_with_vector(&self, query: &str, response: &str, embedding: Option<&[f32]>) {
        // Evict oldest entries if at capacity
        if self.entries.len() >= self.max_entries {
            self.evict_oldest();
        }

        let key = normalize_key(query);
        let vector_key = if let Some(emb) = embedding {
            let vk = self.next_key.fetch_add(1, Ordering::Relaxed);
            let index = self.index.lock();
            if index.add(vk, emb).is_ok() {
                self.key_to_prompt.insert(vk, key.clone());
                Some(vk)
            } else {
                None
            }
        } else {
            None
        };

        self.entries.insert(
            key,
            CachedResponse {
                response: response.to_string(),
                created_at: Instant::now(),
                vector_key,
            },
        );
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove expired entries.
    pub fn evict_expired(&self) {
        self.entries
            .retain(|_, v| v.created_at.elapsed() < self.ttl);
    }

    fn evict_oldest(&self) {
        // Simple eviction: remove entries that are past 75% of TTL
        let threshold = self.ttl * 3 / 4;
        self.entries
            .retain(|_, v| v.created_at.elapsed() < threshold);
    }
}

impl Default for SemanticCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize a prompt for cache key matching.
/// Lowercases, trims whitespace, collapses multiple spaces.
fn normalize_key(query: &str) -> String {
    query
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_miss() {
        let cache = SemanticCache::new();
        assert!(cache.get("hello").await.is_none());
    }

    #[tokio::test]
    async fn cache_hit() {
        let cache = SemanticCache::new();
        cache.put("hello", "world").await;
        assert_eq!(cache.get("hello").await.unwrap(), "world");
    }

    #[tokio::test]
    async fn cache_normalized_key() {
        let cache = SemanticCache::new();
        cache.put("Hello  World", "response").await;
        // Different whitespace/case should match
        assert_eq!(cache.get("hello world").await.unwrap(), "response");
    }

    #[tokio::test]
    async fn cache_expiry() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("key", "value").await;
        assert!(cache.get("key").await.is_some());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(cache.get("key").await.is_none());
    }

    #[tokio::test]
    async fn cache_overwrite() {
        let cache = SemanticCache::new();
        cache.put("key", "v1").await;
        cache.put("key", "v2").await;
        assert_eq!(cache.get("key").await.unwrap(), "v2");
    }

    #[tokio::test]
    async fn cache_len() {
        let cache = SemanticCache::new();
        assert!(cache.is_empty());
        cache.put("a", "1").await;
        cache.put("b", "2").await;
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn evict_expired() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("old", "stale").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.put("new", "fresh").await;

        cache.evict_expired();
        assert_eq!(cache.len(), 1);
        assert!(cache.get("new").await.is_some());
    }

    #[tokio::test]
    async fn vector_cache_miss_when_empty() {
        let cache = SemanticCache::new();
        let emb = vec![0.1_f32; 768];
        assert!(cache.get_by_vector(&emb).await.is_none());
    }

    #[tokio::test]
    async fn put_with_vector_and_exact_get() {
        let cache = SemanticCache::new();
        let emb = vec![0.1_f32; 768];
        cache
            .put_with_vector("test query", "test response", Some(&emb))
            .await;
        // Exact match should still work
        assert_eq!(cache.get("test query").await.unwrap(), "test response");
    }

    #[tokio::test]
    async fn vector_similarity_hit() {
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.90);
        let emb = vec![0.5_f32; 768];
        cache
            .put_with_vector("billing question", "billing answer", Some(&emb))
            .await;

        // Same vector should be a hit
        assert_eq!(cache.get_by_vector(&emb).await.unwrap(), "billing answer");
    }

    #[tokio::test]
    async fn vector_similarity_miss_below_threshold() {
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.99);
        let emb1 = vec![1.0_f32; 768];
        cache
            .put_with_vector("query1", "response1", Some(&emb1))
            .await;

        // Very different vector should miss
        let mut emb2 = vec![0.0_f32; 768];
        emb2[0] = 1.0; // Only first element set
        assert!(cache.get_by_vector(&emb2).await.is_none());
    }

    #[test]
    fn normalize_key_fn() {
        assert_eq!(normalize_key("Hello  World"), "hello world");
        assert_eq!(normalize_key("  a  b  c  "), "a b c");
        assert_eq!(normalize_key("ABC"), "abc");
    }
}
