//! Reranking: optional second-stage relevance scoring applied after hybrid fusion.
//!
//! Read-path only (no Raft/determinism impact). Off by default; enabled via `[rerank]` config.

pub mod cross_encoder;
pub mod llm;
pub mod provider;

#[cfg(feature = "rerank-local")]
pub use cross_encoder::CrossEncoderReranker;
pub use llm::LlmReranker;
pub use provider::Reranker;
