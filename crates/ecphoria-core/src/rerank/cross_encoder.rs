//! Local cross-encoder reranker — the low-latency production alternative to the LLM reranker.
//!
//! A cross-encoder scores each `(query, document)` pair with a small dedicated ONNX model in
//! milliseconds, on CPU, offline — no LLM round-trips. Built on `fastembed` (bge-reranker) behind the
//! **`rerank-local`** Cargo feature, so the default build stays lean (the feature pulls a native
//! `onnxruntime` dependency and downloads model weights on first use).
//!
//! Enable with `--features rerank-local` and set `rerank.provider = "cross_encoder"`.

#[cfg(feature = "rerank-local")]
pub use imp::CrossEncoderReranker;

#[cfg(all(test, feature = "rerank-local"))]
mod tests {
    use super::CrossEncoderReranker;
    use crate::rerank::Reranker;

    // Downloads the bge-reranker model on first run (network). Only compiled with `rerank-local`.
    #[tokio::test]
    async fn scores_more_relevant_doc_higher() {
        let r = CrossEncoderReranker::new().expect("load cross-encoder model");
        let docs = vec![
            "The Eiffel Tower is a landmark in Paris, France.".to_string(),
            "Bananas are a yellow tropical fruit.".to_string(),
        ];
        let scores = r
            .rerank("Where is the Eiffel Tower located?", &docs)
            .await
            .unwrap();
        assert_eq!(scores.len(), 2);
        assert!(
            scores[0] > scores[1],
            "the relevant document must score higher: {scores:?}"
        );
    }
}

#[cfg(feature = "rerank-local")]
mod imp {
    use std::sync::Arc;

    use fastembed::{RerankInitOptions, RerankerModel, TextRerank};
    use parking_lot::Mutex;

    use crate::rerank::Reranker;

    /// A local ONNX cross-encoder (bge-reranker-base). The model is synchronous + CPU-bound, so
    /// [`Reranker::rerank`] runs it inside `spawn_blocking`.
    pub struct CrossEncoderReranker {
        model: Arc<Mutex<TextRerank>>,
        name: &'static str,
    }

    impl CrossEncoderReranker {
        /// Load the model (downloads weights to the fastembed cache on first use).
        pub fn new() -> crate::Result<Self> {
            let model = TextRerank::try_new(
                RerankInitOptions::new(RerankerModel::BGERerankerBase)
                    .with_show_download_progress(false),
            )
            .map_err(|e| crate::Error::Llm(format!("load cross-encoder: {e}")))?;
            Ok(Self {
                model: Arc::new(Mutex::new(model)),
                name: "bge-reranker-base",
            })
        }
    }

    #[async_trait::async_trait]
    impl Reranker for CrossEncoderReranker {
        async fn rerank(&self, query: &str, docs: &[String]) -> crate::Result<Vec<f32>> {
            if docs.is_empty() {
                return Ok(vec![]);
            }
            let model = self.model.clone();
            let query = query.to_string();
            let docs = docs.to_vec();
            let n = docs.len();
            tokio::task::spawn_blocking(move || {
                let refs: Vec<&str> = docs.iter().map(String::as_str).collect();
                let results = model
                    .lock()
                    .rerank(query.as_str(), refs, false, None)
                    .map_err(|e| crate::Error::Llm(format!("cross-encoder rerank: {e}")))?;
                // fastembed returns results sorted by score; scatter back into input order.
                let mut scores = vec![0.0f32; n];
                for r in results {
                    if r.index < n {
                        scores[r.index] = r.score;
                    }
                }
                Ok(scores)
            })
            .await
            .map_err(|e| crate::Error::Llm(format!("cross-encoder task join: {e}")))?
        }

        fn model_name(&self) -> &str {
            self.name
        }
    }
}
