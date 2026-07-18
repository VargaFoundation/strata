//! Anthropic (Claude) chat-completion provider — the Messages API.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Completion provider backed by the Anthropic `/v1/messages` API (Claude models).
pub struct AnthropicCompletion {
    client: Client,
    api_key: String,
    model: String,
    max_tokens: u32,
}

#[derive(Serialize)]
struct Message<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<Message<'a>>,
}

#[derive(Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: String,
}

impl AnthropicCompletion {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            model,
            max_tokens: 1024,
        }
    }
}

#[async_trait::async_trait]
impl super::CompletionProvider for AnthropicCompletion {
    async fn complete(&self, system: &str, user: &str) -> crate::Result<String> {
        let request = MessagesRequest {
            model: &self.model,
            max_tokens: self.max_tokens,
            system,
            messages: vec![Message {
                role: "user",
                content: user,
            }],
        };

        let response = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Llm(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Llm(format!(
                "Anthropic returned {status}: {body}"
            )));
        }

        let parsed: MessagesResponse = response
            .json()
            .await
            .map_err(|e| crate::Error::Llm(format!("failed to parse response: {e}")))?;
        // Concatenate all text blocks (usually one).
        let text: String = parsed
            .content
            .into_iter()
            .filter(|b| b.kind == "text")
            .map(|b| b.text)
            .collect();
        if text.is_empty() {
            return Err(crate::Error::Llm(
                "Anthropic returned no text content".into(),
            ));
        }
        Ok(text)
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_messages_response_text() {
        // Shape of a real Anthropic /v1/messages response.
        let json = r#"{
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-sonnet-5",
            "content": [{"type": "text", "text": "May 7th"}],
            "stop_reason": "end_turn"
        }"#;
        let parsed: MessagesResponse = serde_json::from_str(json).unwrap();
        let text: String = parsed
            .content
            .into_iter()
            .filter(|b| b.kind == "text")
            .map(|b| b.text)
            .collect();
        assert_eq!(text, "May 7th");
    }
}
