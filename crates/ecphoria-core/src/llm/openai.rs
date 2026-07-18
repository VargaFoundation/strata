//! OpenAI chat-completion provider.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Completion provider backed by the OpenAI `/v1/chat/completions` API.
pub struct OpenAiCompletion {
    client: Client,
    api_key: String,
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
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

impl OpenAiCompletion {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            model,
        }
    }
}

#[async_trait::async_trait]
impl super::CompletionProvider for OpenAiCompletion {
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
        };

        let response = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Llm(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Llm(format!(
                "OpenAI returned {status}: {body}"
            )));
        }

        let parsed: ChatResponse = response
            .json()
            .await
            .map_err(|e| crate::Error::Llm(format!("failed to parse response: {e}")))?;
        parsed
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| crate::Error::Llm("OpenAI returned no choices".into()))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}
