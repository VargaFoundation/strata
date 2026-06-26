//! Chat-completion provider abstraction used by the cognition layer.
//!
//! Lives in `strata-core` (not the gateway) so the memory-cognition pipeline can call an
//! LLM for opt-in tasks (fact extraction, consolidation) without core depending on the
//! protocol layer — mirroring [`crate::embedding::EmbeddingProvider`].

use async_trait::async_trait;

/// A single-turn chat-completion provider.
#[async_trait]
pub trait CompletionProvider: Send + Sync {
    /// Run a completion: a system instruction + a user message → assistant text.
    async fn complete(&self, system: &str, user: &str) -> crate::Result<String>;

    /// Provider/model label (for logs/metrics).
    fn model_name(&self) -> &str;
}
