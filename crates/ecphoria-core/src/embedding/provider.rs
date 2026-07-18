//! Embedding provider trait — pluggable backends for computing vector embeddings.

/// A provider that computes vector embeddings from text.
///
/// **Asymmetric retrieval prefixes.** Modern retrieval embedding models are trained with a
/// *task instruction* prepended to the text, and crucially use a **different** instruction for the
/// search query than for the indexed document (e.g. nomic-embed-text expects `search_query: ` vs
/// `search_document: `; the e5 family uses `query: ` vs `passage: `). Omitting these — or using the
/// same text for both sides — is a well-known, measurable retrieval-quality regression. The
/// read/search path must therefore call [`EmbeddingProvider::embed_query`] and the write/index path
/// [`EmbeddingProvider::embed_documents`]; the raw [`EmbeddingProvider::embed`] applies no prefix
/// and is only for callers that manage prefixing themselves.
#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Compute embeddings for a batch of texts **without** applying any task prefix.
    async fn embed(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>>;

    /// The dimensionality of the output vectors.
    fn dimension(&self) -> usize;

    /// The model name used for embedding.
    fn model_name(&self) -> &str;

    /// Task-instruction prefix prepended to a **search query** before embedding (e.g.
    /// `"search_query: "`). Empty when the model uses no asymmetric prefixes. Default: none.
    fn query_prefix(&self) -> &str {
        ""
    }

    /// Task-instruction prefix prepended to an **indexed document** before embedding (e.g.
    /// `"search_document: "`). Default: none.
    fn document_prefix(&self) -> &str {
        ""
    }

    /// Embed search queries — applies [`EmbeddingProvider::query_prefix`]. Use on the read path.
    async fn embed_query(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
        match apply_prefix(self.query_prefix(), texts) {
            Some(prefixed) => self.embed(&prefixed).await,
            None => self.embed(texts).await,
        }
    }

    /// Embed documents to be indexed — applies [`EmbeddingProvider::document_prefix`]. Use on the
    /// write/ingest path.
    async fn embed_documents(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
        match apply_prefix(self.document_prefix(), texts) {
            Some(prefixed) => self.embed(&prefixed).await,
            None => self.embed(texts).await,
        }
    }
}

/// Prepend `prefix` to each text, or `None` when the prefix is empty (so the caller can pass the
/// original slice through with zero allocation).
fn apply_prefix(prefix: &str, texts: &[String]) -> Option<Vec<String>> {
    if prefix.is_empty() {
        None
    } else {
        Some(texts.iter().map(|t| format!("{prefix}{t}")).collect())
    }
}
