//! Semantic response cache — caches LLM responses keyed by prompt (+ scope).
//!
//! Two lookup modes:
//! - **Exact match** (default): normalized prompt text, namespaced by the caller's scope.
//! - **Vector similarity** (opt-in, `gateway.llm_cache_similarity`): the query is embedded and
//!   compared against cached query vectors; a hit still requires the entry's scope to match.
//!   Similarity caching of factual answers is inherently risky ("balance of cust_42" vs
//!   "balance of cust_43" can exceed the threshold) — hence opt-in.
//!
//! The **scope** is an opaque namespace string provided by the caller. The LLM proxy derives it
//! from `(tenant, user, fingerprint-of-injected-RAG-context)`, so a cached answer can only ever
//! be replayed to the same tenant AND user AND for the same retrieved context — a response
//! augmented with user A's memories is never served to user B.
//!
//! Cluster note: this cache is in-memory and **per node**. Nodes may briefly serve answers built
//! from different memory states; entries expire by TTL. Disable caching (or accept the staleness
//! window) if node-coherent responses matter to you.

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
    /// Scope this response was produced under (`None` = unscoped / auth disabled). The vector
    /// index is shared across scopes, so a similarity hit MUST be re-checked against this to
    /// avoid serving one scope's (RAG-augmented) answer to another.
    scope: Option<String>,
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

    /// Look up a cached response by exact prompt match within `scope`.
    /// Returns None on cache miss or expired entry.
    pub async fn get(&self, query: &str, scope: Option<&str>) -> Option<String> {
        let key = cache_key(scope, query);

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

    /// Look up a cached response by vector similarity within `scope`.
    ///
    /// Searches the vector index for the nearest cached queries and returns the first whose
    /// similarity exceeds the threshold **and** whose scope matches the caller. Because the vector
    /// index is shared across scopes, the scope re-check is what prevents a cross-scope leak: a
    /// nearest neighbour belonging to another tenant/user/context is skipped rather than served.
    pub async fn get_by_vector(&self, embedding: &[f32], scope: Option<&str>) -> Option<String> {
        if embedding.is_empty() {
            return None;
        }

        let results = {
            let index = self.index.lock();
            if index.size() == 0 {
                return None;
            }
            index.search(embedding, 5).ok()?
        };

        // Results are sorted by ascending distance (descending similarity), so once we drop below
        // the threshold every later candidate is worse — stop.
        for (&vk, &dist) in results.keys.iter().zip(results.distances.iter()) {
            let similarity = 1.0 - dist;
            if similarity < self.similarity_threshold {
                break;
            }
            let Some(prompt_key) = self.key_to_prompt.get(&vk) else {
                continue;
            };
            let prompt = prompt_key.value().clone();
            drop(prompt_key);
            if let Some(entry) = self.entries.get(&prompt) {
                if entry.created_at.elapsed() < self.ttl && entry.scope.as_deref() == scope {
                    return Some(entry.response.clone());
                }
            }
        }

        None
    }

    /// Store a response in the cache (no vector) within `scope`.
    pub async fn put(&self, query: &str, response: &str, scope: Option<&str>) {
        self.put_with_vector(query, response, None, scope).await;
    }

    /// Store a response (with its embedding vector) within `scope`. The entries-map key is
    /// namespaced by scope so two scopes asking the same question don't collide, and the scope
    /// is recorded on the entry so the shared vector index can be re-checked on lookup.
    pub async fn put_with_vector(
        &self,
        query: &str,
        response: &str,
        embedding: Option<&[f32]>,
        scope: Option<&str>,
    ) {
        // Evict oldest entries if at capacity
        if self.entries.len() >= self.max_entries {
            self.evict_oldest();
        }

        let key = cache_key(scope, query);
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
                scope: scope.map(|s| s.to_string()),
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

/// Namespace a cache key by scope so one scope's cached response is never keyed identically to
/// another's. `None` (unscoped / auth disabled) uses the bare normalized prompt. Uses the ASCII
/// unit-separator, which cannot appear in a normalized (whitespace-collapsed) prompt.
fn cache_key(scope: Option<&str>, query: &str) -> String {
    match scope {
        Some(s) => format!("{}\u{1f}{}", s, normalize_key(query)),
        None => normalize_key(query),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_miss() {
        let cache = SemanticCache::new();
        assert!(cache.get("hello", None).await.is_none());
    }

    #[tokio::test]
    async fn cache_hit() {
        let cache = SemanticCache::new();
        cache.put("hello", "world", None).await;
        assert_eq!(cache.get("hello", None).await.unwrap(), "world");
    }

    #[tokio::test]
    async fn cache_normalized_key() {
        let cache = SemanticCache::new();
        cache.put("Hello  World", "response", None).await;
        // Different whitespace/case should match
        assert_eq!(cache.get("hello world", None).await.unwrap(), "response");
    }

    #[tokio::test]
    async fn cache_expiry() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("key", "value", None).await;
        assert!(cache.get("key", None).await.is_some());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(cache.get("key", None).await.is_none());
    }

    #[tokio::test]
    async fn cache_overwrite() {
        let cache = SemanticCache::new();
        cache.put("key", "v1", None).await;
        cache.put("key", "v2", None).await;
        assert_eq!(cache.get("key", None).await.unwrap(), "v2");
    }

    #[tokio::test]
    async fn cache_len() {
        let cache = SemanticCache::new();
        assert!(cache.is_empty());
        cache.put("a", "1", None).await;
        cache.put("b", "2", None).await;
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn evict_expired() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("old", "stale", None).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.put("new", "fresh", None).await;

        cache.evict_expired();
        assert_eq!(cache.len(), 1);
        assert!(cache.get("new", None).await.is_some());
    }

    #[tokio::test]
    async fn vector_cache_miss_when_empty() {
        let cache = SemanticCache::new();
        let emb = vec![0.1_f32; 768];
        assert!(cache.get_by_vector(&emb, None).await.is_none());
    }

    #[tokio::test]
    async fn put_with_vector_and_exact_get() {
        let cache = SemanticCache::new();
        let emb = vec![0.1_f32; 768];
        cache
            .put_with_vector("test query", "test response", Some(&emb), None)
            .await;
        // Exact match should still work
        assert_eq!(
            cache.get("test query", None).await.unwrap(),
            "test response"
        );
    }

    #[tokio::test]
    async fn vector_similarity_hit() {
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.90);
        let emb = vec![0.5_f32; 768];
        cache
            .put_with_vector("billing question", "billing answer", Some(&emb), None)
            .await;

        // Same vector should be a hit
        assert_eq!(
            cache.get_by_vector(&emb, None).await.unwrap(),
            "billing answer"
        );
    }

    #[tokio::test]
    async fn vector_similarity_miss_below_threshold() {
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.99);
        let emb1 = vec![1.0_f32; 768];
        cache
            .put_with_vector("query1", "response1", Some(&emb1), None)
            .await;

        // Very different vector should miss
        let mut emb2 = vec![0.0_f32; 768];
        emb2[0] = 1.0; // Only first element set
        assert!(cache.get_by_vector(&emb2, None).await.is_none());
    }

    #[tokio::test]
    async fn vector_cache_is_tenant_isolated() {
        // Regression: the shared vector index must not serve one tenant's cached (RAG-augmented)
        // answer to another tenant on a similar/identical prompt.
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.90);
        let emb = vec![0.5_f32; 768];
        cache
            .put_with_vector("q", "answer-for-a", Some(&emb), Some("tenant-a"))
            .await;
        // Same tenant, same vector → hit.
        assert_eq!(
            cache.get_by_vector(&emb, Some("tenant-a")).await.unwrap(),
            "answer-for-a"
        );
        // Different tenant, identical vector → MUST miss (no cross-tenant leak).
        assert!(cache.get_by_vector(&emb, Some("tenant-b")).await.is_none());
        // The no-tenant namespace is also distinct from tenant A's.
        assert!(cache.get_by_vector(&emb, None).await.is_none());
    }

    #[test]
    fn normalize_key_fn() {
        assert_eq!(normalize_key("Hello  World"), "hello world");
        assert_eq!(normalize_key("  a  b  c  "), "a b c");
        assert_eq!(normalize_key("ABC"), "abc");
    }

    #[tokio::test]
    async fn cache_is_user_isolated_within_a_tenant() {
        // Regression: a RAG-augmented answer produced for user A must never be replayed to
        // user B of the SAME tenant. The proxy encodes (tenant, user, context) in the scope.
        let cache = SemanticCache::with_config(Duration::from_secs(3600), 1000, 0.90);
        let emb = vec![0.5_f32; 768];
        let scope_a = "acme\u{1f}user-a\u{1f}ctx123";
        let scope_b = "acme\u{1f}user-b\u{1f}ctx456";
        cache
            .put_with_vector(
                "what is my balance?",
                "A's balance is 42",
                Some(&emb),
                Some(scope_a),
            )
            .await;
        // Same user + context → hit (exact and vector).
        assert!(cache
            .get("what is my balance?", Some(scope_a))
            .await
            .is_some());
        assert!(cache.get_by_vector(&emb, Some(scope_a)).await.is_some());
        // Same tenant, different user → MUST miss on both paths.
        assert!(cache
            .get("what is my balance?", Some(scope_b))
            .await
            .is_none());
        assert!(cache.get_by_vector(&emb, Some(scope_b)).await.is_none());
        // Same user but the retrieved context changed → miss (stale-context guard).
        let scope_a_new_ctx = "acme\u{1f}user-a\u{1f}ctx999";
        assert!(cache
            .get("what is my balance?", Some(scope_a_new_ctx))
            .await
            .is_none());
    }
}
