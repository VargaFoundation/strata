//! Embedding provider trait — pluggable backends for computing vector embeddings.

/// A provider that computes vector embeddings from text.
#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Compute embeddings for a batch of texts.
    async fn embed(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>>;

    /// The dimensionality of the output vectors.
    fn dimension(&self) -> usize;

    /// The model name used for embedding.
    fn model_name(&self) -> &str;
}
