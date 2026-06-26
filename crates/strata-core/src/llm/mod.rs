//! Chat-completion providers for the cognition layer's opt-in LLM tasks.

pub mod ollama;
pub mod openai;
pub mod provider;

pub use provider::CompletionProvider;
