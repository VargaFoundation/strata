//! Reranking provider trait — an optional second-stage relevance model.
//!
//! A reranker re-scores the candidate documents surfaced by hybrid (BM25 + vector) retrieval,
//! using a stronger but more expensive model (an LLM judge or a cross-encoder). It runs **only
//! on the read path** (`memory_search`), so it has zero Raft/determinism impact and is fully
//! optional. Mirrors [`crate::embedding::EmbeddingProvider`] and [`crate::llm::CompletionProvider`].

/// A second-stage reranker: given a query and candidate documents, returns one relevance
/// score per document (higher = more relevant), in the **same order** as `docs`.
#[async_trait::async_trait]
pub trait Reranker: Send + Sync {
    /// Score each document's relevance to `query`. Must return exactly one score per input
    /// document, in input order. Higher is more relevant.
    async fn rerank(&self, query: &str, docs: &[String]) -> crate::Result<Vec<f32>>;

    /// Provider/model label (for logs/metrics).
    fn model_name(&self) -> &str;
}
