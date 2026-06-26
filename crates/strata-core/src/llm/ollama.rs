//! Ollama chat-completion provider (local LLM).

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Completion provider backed by a local Ollama server (`/api/chat`).
pub struct OllamaCompletion {
    client: Client,
    url: String,
    model: String,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    stream: bool,
}

#[derive(Deserialize)]
struct ChatResponse {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

impl OllamaCompletion {
    pub fn new(url: String, model: String) -> Self {
        Self {
            client: Client::new(),
            url,
            model,
        }
    }
}

#[async_trait::async_trait]
impl super::CompletionProvider for OllamaCompletion {
    async fn complete(&self, system: &str, user: &str) -> crate::Result<String> {
        let request = ChatRequest {
            model: &self.model,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: system,
                },
                ChatMessage {
                    role: "user",
                    content: user,
                },
            ],
            stream: false,
        };

        let response = self
            .client
            .post(format!("{}/api/chat", self.url.trim_end_matches('/')))
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Llm(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Llm(format!(
                "Ollama returned {status}: {body}"
            )));
        }

        let parsed: ChatResponse = response
            .json()
            .await
            .map_err(|e| crate::Error::Llm(format!("failed to parse response: {e}")))?;
        Ok(parsed.message.content)
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}
