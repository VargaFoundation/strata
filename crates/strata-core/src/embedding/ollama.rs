//! Ollama embedding provider.

/// Ollama-based embedding provider.
pub struct OllamaProvider {
    _url: String,
    _model: String,
    _dimension: usize,
}

impl OllamaProvider {
    pub fn new(url: String, model: String, dimension: usize) -> Self {
        Self {
            _url: url,
            _model: model,
            _dimension: dimension,
        }
    }
}

#[async_trait::async_trait]
impl super::EmbeddingProvider for OllamaProvider {
    async fn embed(&self, _texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
        // TODO: POST to Ollama /api/embeddings
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
        let provider = OllamaProvider::new(
            "http://localhost:11434".into(),
            "nomic-embed-text".into(),
            768,
        );
        assert_eq!(provider.dimension(), 768);
        assert_eq!(provider.model_name(), "nomic-embed-text");
    }
}
