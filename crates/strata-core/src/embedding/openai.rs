//! OpenAI embedding provider.

/// OpenAI-based embedding provider.
pub struct OpenAiProvider {
    _api_key: String,
    _model: String,
    _dimension: usize,
}

impl OpenAiProvider {
    pub fn new(api_key: String, model: String, dimension: usize) -> Self {
        Self {
            _api_key: api_key,
            _model: model,
            _dimension: dimension,
        }
    }
}

#[async_trait::async_trait]
impl super::EmbeddingProvider for OpenAiProvider {
    async fn embed(&self, _texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
        // TODO: POST to OpenAI /v1/embeddings
        Ok(vec![])
    }

    fn dimension(&self) -> usize {
        self._dimension
    }

    fn model_name(&self) -> &str {
        &self._model
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::EmbeddingProvider;

    #[test]
    fn provider_metadata() {
        let provider = OpenAiProvider::new("sk-test".into(), "text-embedding-3-small".into(), 1536);
        assert_eq!(provider.dimension(), 1536);
    }
}
