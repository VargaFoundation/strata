//! Semantic response cache — caches LLM responses by semantic similarity.

/// Cache for LLM responses keyed by semantic similarity of the prompt.
pub struct SemanticCache {
    // TODO: vector index for cache keys, response store
}

impl SemanticCache {
    pub fn new() -> Self {
        Self {}
    }

    /// Look up a cached response by semantic similarity.
    pub async fn get(&self, _query: &str) -> Option<String> {
        None
    }

    /// Store a response in the cache.
    pub async fn put(&self, _query: &str, _response: &str) {
        // TODO: embed query, store in vector index
    }
}

impl Default for SemanticCache {
    fn default() -> Self {
        Self::new()
    }
}
