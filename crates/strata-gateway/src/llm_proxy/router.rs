//! OpenAI-compatible /v1/chat/completions proxy with automatic RAG.

/// LLM proxy router — intercepts requests, enriches with context, forwards to LLM.
pub struct LlmProxyRouter {
    // TODO: provider registry, semantic search handle, cache
}

impl LlmProxyRouter {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for LlmProxyRouter {
    fn default() -> Self {
        Self::new()
    }
}
