//! Reranking: optional second-stage relevance scoring applied after hybrid fusion.
//!
//! Read-path only (no Raft/determinism impact). Off by default; enabled via `[rerank]` config.

pub mod cross_encoder;
pub mod llm;
pub mod provider;

pub use llm::LlmReranker;
pub use provider::Reranker;
