//! Chat-completion providers for the cognition layer's opt-in LLM tasks.

pub mod anthropic;
pub mod claude_cli;
pub mod ollama;
pub mod openai;
pub mod provider;

pub use provider::CompletionProvider;
