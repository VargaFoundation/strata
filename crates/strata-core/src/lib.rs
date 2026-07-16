pub mod config;
pub mod embedding;
pub mod engine;
pub mod error;
pub mod ingest;
pub mod llm;
pub mod materialized;
pub mod memory;
pub mod query;
pub mod rerank;
pub mod runtime;
pub mod storage;

pub use config::CoreConfig;
pub use engine::{FeedbackAction, MemoryChange, MemoryFeedback, MemoryProvenance, StrataEngine};
pub use error::{Error, Result};
