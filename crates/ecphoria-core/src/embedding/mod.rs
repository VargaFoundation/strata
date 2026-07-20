pub mod local;
pub mod ollama;
pub mod openai;
pub mod provider;

#[cfg(feature = "embed-image")]
pub mod image_basic;

pub use provider::EmbeddingProvider;

/// Embeds an **image** into a vector for multimodal retrieval — the pluggable hook for CLIP-style or
/// any image encoder. Wire one into the engine with
/// [`EcphoriaEngine::set_image_embedding`](crate::EcphoriaEngine::set_image_embedding); attachment
/// uploads with an `image/*` content-type are then embedded + indexed and become searchable by image.
#[async_trait::async_trait]
pub trait ImageEmbeddingProvider: Send + Sync {
    /// Embed raw image bytes (PNG/JPEG/…) into a fixed-dimension vector.
    async fn embed_image(&self, bytes: &[u8]) -> crate::Result<Vec<f32>>;
    /// The vector dimension this provider produces.
    fn dimension(&self) -> usize;
    /// A short model identifier.
    fn model_name(&self) -> &str;
}

/// Default `(query_prefix, document_prefix)` for a given embedding model, based on the instruction
/// format the model was trained with. Returns `("", "")` for models that need no prefix
/// (OpenAI `text-embedding-3-*`, `bge-m3`, …). Explicit config always overrides this
/// (see `EmbeddingConfig::resolved_prefixes`).
///
/// References: Nomic's `nomic-embed-text` is trained with `search_query:` / `search_document:`
/// task prefixes; the `intfloat/e5` family with `query:` / `passage:`.
pub fn default_prefixes(model: &str) -> (&'static str, &'static str) {
    let m = model.to_ascii_lowercase();
    if m.contains("nomic-embed") {
        ("search_query: ", "search_document: ")
    } else if m.contains("e5") {
        // intfloat e5 / multilingual-e5 / e5-mistral.
        ("query: ", "passage: ")
    } else {
        ("", "")
    }
}

#[cfg(test)]
mod tests {
    use super::default_prefixes;

    #[test]
    fn nomic_gets_asymmetric_prefixes() {
        assert_eq!(
            default_prefixes("nomic-embed-text"),
            ("search_query: ", "search_document: ")
        );
    }

    #[test]
    fn e5_family_gets_query_passage() {
        assert_eq!(
            default_prefixes("multilingual-e5-large"),
            ("query: ", "passage: ")
        );
    }

    #[test]
    fn unknown_and_openai_models_get_none() {
        assert_eq!(default_prefixes("text-embedding-3-large"), ("", ""));
        assert_eq!(default_prefixes("bge-m3"), ("", ""));
    }
}
