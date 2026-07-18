//! LLM backend providers — dispatches to Anthropic, OpenAI, Ollama, etc.

/// Supported LLM providers.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LlmProvider {
    Anthropic,
    OpenAi,
    Ollama,
}

/// Configuration for an LLM provider.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LlmProviderConfig {
    pub provider: LlmProvider,
    pub api_key: Option<String>,
    pub url: Option<String>,
    pub model: Option<String>,
}
