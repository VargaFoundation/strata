//! In-process ONNX text-embedding provider (fastembed) behind the **`embed-local`** feature.
//!
//! Honors the single-binary promise: no Ollama/OpenAI sidecar needed. The model runs on CPU via
//! ONNX Runtime and downloads its weights to the fastembed cache on first use. Off by default (the
//! feature pulls a heavy native `onnxruntime` dependency), mirroring `rerank-local`.
//!
//! Enable with `--features embed-local` and set `embedding.provider = "local"` (+ `embedding.model`,
//! e.g. `bge-small-en`, `bge-base-en`, `all-minilm`, `multilingual-e5-small`, `nomic-embed`).

#[cfg(feature = "embed-local")]
pub use imp::FastEmbedProvider;

#[cfg(feature = "embed-local")]
mod imp {
    use std::sync::Arc;

    use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
    use parking_lot::Mutex;

    use crate::embedding::EmbeddingProvider;

    /// A local ONNX text-embedding model. The model is synchronous + CPU-bound, so
    /// [`EmbeddingProvider::embed`] runs it inside `spawn_blocking`.
    pub struct FastEmbedProvider {
        model: Arc<Mutex<TextEmbedding>>,
        model_name: String,
        dimension: usize,
        query_prefix: String,
        document_prefix: String,
    }

    /// Map a config `embedding.model` string to a fastembed model + its output dimension. Defaults
    /// to bge-small-en-v1.5 (384-d) for an unknown name.
    fn resolve_model(name: &str) -> (EmbeddingModel, usize) {
        let n = name.to_ascii_lowercase();
        if n.contains("bge-base") {
            (EmbeddingModel::BGEBaseENV15, 768)
        } else if n.contains("nomic") {
            (EmbeddingModel::NomicEmbedTextV15, 768)
        } else if n.contains("e5") {
            (EmbeddingModel::MultilingualE5Small, 384)
        } else if n.contains("minilm") || n.contains("all-mini") {
            (EmbeddingModel::AllMiniLML6V2, 384)
        } else {
            (EmbeddingModel::BGESmallENV15, 384)
        }
    }

    impl FastEmbedProvider {
        /// Load the model (downloads weights to the fastembed cache on first use). The reported
        /// [`EmbeddingProvider::dimension`] is the model's fixed output size — set
        /// `memory.semantic.default_dimension` to match (or run `ecphoria doctor`).
        pub fn new(
            model_name: &str,
            query_prefix: String,
            document_prefix: String,
        ) -> crate::Result<Self> {
            let (em, dimension) = resolve_model(model_name);
            let model =
                TextEmbedding::try_new(InitOptions::new(em).with_show_download_progress(false))
                    .map_err(|e| crate::Error::Embedding(format!("load fastembed model: {e}")))?;
            Ok(Self {
                model: Arc::new(Mutex::new(model)),
                model_name: model_name.to_string(),
                dimension,
                query_prefix,
                document_prefix,
            })
        }
    }

    #[async_trait::async_trait]
    impl EmbeddingProvider for FastEmbedProvider {
        async fn embed(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
            if texts.is_empty() {
                return Ok(vec![]);
            }
            let model = self.model.clone();
            let owned = texts.to_vec();
            tokio::task::spawn_blocking(move || {
                model
                    .lock()
                    .embed(owned, None)
                    .map_err(|e| crate::Error::Embedding(format!("fastembed embed: {e}")))
            })
            .await
            .map_err(|e| crate::Error::Embedding(format!("fastembed task join: {e}")))?
        }

        fn dimension(&self) -> usize {
            self.dimension
        }

        fn model_name(&self) -> &str {
            &self.model_name
        }

        fn query_prefix(&self) -> &str {
            &self.query_prefix
        }

        fn document_prefix(&self) -> &str {
            &self.document_prefix
        }
    }
}
