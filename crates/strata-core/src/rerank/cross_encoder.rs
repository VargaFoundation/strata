//! Cross-encoder reranker — the production answer to the LLM reranker's latency.
//!
//! The LLM reranker ([`super::llm::LlmReranker`]) works but is ~140 s/query on a local 7B model
//! (it generates over the whole candidate list). A **cross-encoder** scores each `(query, document)`
//! pair with a small, dedicated model in milliseconds, on CPU, offline — the right production
//! reranker.
//!
//! **Status: documented design, NOT bundled.** It needs a local ONNX runtime (`ort`) + a
//! `bge-reranker` model download — heavy native dependencies that are deliberately kept out of the
//! default build and could not be built/verified in this environment, so this is intentionally not
//! a fake implementation.
//!
//! To add it (behind a `rerank-local` Cargo feature, so the default build stays lean):
//!
//! ```toml
//! # crates/strata-core/Cargo.toml
//! [dependencies]
//! fastembed = { version = "4", optional = true }
//!
//! [features]
//! rerank-local = ["dep:fastembed"]
//! ```
//!
//! then implement [`super::Reranker`] over `fastembed::TextRerank` (e.g. `RerankerModel::BGERerankerBase`):
//! call `model.rerank(query, docs, false, None)` inside `tokio::task::spawn_blocking` (the model is
//! synchronous) and scatter each returned `(index, score)` back into per-document order. Wire it in
//! `StrataEngine::new` under `rerank.provider = "cross_encoder"` (that arm is already recognized and
//! currently degrades to a no-op).
//!
//! Until then, set `rerank.provider = "llm"` for the working (if slow) reranker.
