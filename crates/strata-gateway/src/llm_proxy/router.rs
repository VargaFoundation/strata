//! OpenAI-compatible /v1/chat/completions proxy with automatic RAG.
//!
//! Flow:
//! 1. Receive OpenAI-format chat completion request
//! 2. Extract the last user message
//! 3. Search semantic memory for relevant context
//! 4. Prepend context to system message
//! 5. Forward enriched request to configured LLM provider
//! 6. Return the provider's response

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use reqwest::Client;
use strata_core::StrataEngine;

use super::providers::LlmProvider;

/// OpenAI-compatible chat completion request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// OpenAI-compatible chat completion response.
#[derive(Debug, serde::Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, serde::Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: ChatMessage,
    pub finish_reason: String,
}

#[derive(Debug, serde::Serialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

/// Handle /v1/chat/completions — OpenAI-compatible endpoint with auto-RAG.
pub async fn chat_completions(
    State(engine): State<Arc<StrataEngine>>,
    Json(mut req): Json<ChatCompletionRequest>,
) -> Json<serde_json::Value> {
    // 1. Extract last user message for context search
    let user_query = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.clone())
        .unwrap_or_default();

    // 2. Build context from both semantic and episodic memory
    let mut context_sections: Vec<String> = Vec::new();

    // 2a. Semantic search: embed the user query and find relevant knowledge
    if engine.semantic_count() > 0 && !user_query.is_empty() {
        if let Ok(results) = engine.embed_and_search(&user_query, 5, None, None).await {
            let semantic_lines: Vec<String> = results
                .iter()
                .filter(|r| r.score >= 0.3)
                .map(|r| {
                    let source = r
                        .entry
                        .metadata
                        .get("source")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    format!(
                        "- [{}] (score: {:.2}) {}",
                        source,
                        r.score,
                        r.entry.content.chars().take(300).collect::<String>()
                    )
                })
                .collect();
            if !semantic_lines.is_empty() {
                context_sections.push(format!(
                    "### Relevant knowledge (semantic search)\n{}",
                    semantic_lines.join("\n")
                ));
            }
        }
    }

    // 2b. Episodic memory: recent events for temporal context
    let recent_events = engine
        .query_sql("SELECT source, event_type, payload, ts FROM episodic ORDER BY ts DESC LIMIT 5")
        .await
        .unwrap_or_default();

    if !recent_events.is_empty() {
        let event_lines: Vec<String> = recent_events
            .iter()
            .filter_map(|row| {
                let source = row.get("source")?.as_str()?;
                let event_type = row.get("event_type")?.as_str()?;
                let ts = row.get("ts").and_then(|v| v.as_str()).unwrap_or("unknown");
                Some(format!("- [{source}] {event_type} (at {ts})"))
            })
            .collect();
        if !event_lines.is_empty() {
            context_sections.push(format!(
                "### Recent events (episodic memory)\n{}",
                event_lines.join("\n")
            ));
        }
    }

    // 2c. Inject combined context into the conversation
    if !context_sections.is_empty() {
        let context_block = format!(
            "## Context from Strata\nThe following context was automatically retrieved from Strata's memory stores.\n\n{}",
            context_sections.join("\n\n")
        );

        if let Some(sys_msg) = req.messages.iter_mut().find(|m| m.role == "system") {
            sys_msg.content = format!("{}\n\n{}", context_block, sys_msg.content);
        } else {
            req.messages.insert(
                0,
                ChatMessage {
                    role: "system".into(),
                    content: context_block,
                },
            );
        }
    }

    // 4. Determine provider and forward (shared client for connection reuse)
    let config = engine.config();
    let provider = determine_provider(&req.model);
    // Use a thread-local shared client to avoid per-request allocation
    static HTTP_CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
    let http = HTTP_CLIENT.get_or_init(Client::new);

    match provider {
        LlmProvider::OpenAi => {
            forward_to_openai(http, &config.embedding.openai_api_key, &req).await
        }
        LlmProvider::Ollama => forward_to_ollama(http, &config.embedding.ollama_url, &req).await,
        LlmProvider::Anthropic => forward_to_anthropic(http, &req).await,
    }
}

fn determine_provider(model: &str) -> LlmProvider {
    if model.starts_with("claude") {
        LlmProvider::Anthropic
    } else if model.starts_with("gpt") || model.starts_with("o1") || model.starts_with("o3") {
        LlmProvider::OpenAi
    } else {
        // Default to Ollama for local models
        LlmProvider::Ollama
    }
}

async fn forward_to_openai(
    http: &Client,
    api_key: &str,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    match http
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        .json(req)
        .send()
        .await
    {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => Json(body),
            Err(e) => error_response(&format!("failed to parse OpenAI response: {e}")),
        },
        Err(e) => error_response(&format!("OpenAI request failed: {e}")),
    }
}

async fn forward_to_ollama(
    http: &Client,
    base_url: &str,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    match http.post(&url).json(req).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => Json(body),
            Err(e) => error_response(&format!("failed to parse Ollama response: {e}")),
        },
        Err(e) => error_response(&format!("Ollama request failed: {e}")),
    }
}

async fn forward_to_anthropic(
    http: &Client,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    // Translate OpenAI format to Anthropic Messages API format
    let api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_default();
    if api_key.is_empty() {
        return error_response("ANTHROPIC_API_KEY environment variable not set");
    }

    // Separate system message from conversation messages
    let system = req
        .messages
        .iter()
        .find(|m| m.role == "system")
        .map(|m| m.content.clone());

    let messages: Vec<serde_json::Value> = req
        .messages
        .iter()
        .filter(|m| m.role != "system")
        .map(|m| {
            serde_json::json!({
                "role": m.role,
                "content": m.content,
            })
        })
        .collect();

    let mut body = serde_json::json!({
        "model": req.model,
        "messages": messages,
        "max_tokens": req.max_tokens.unwrap_or(4096),
    });

    if let Some(sys) = system {
        body["system"] = serde_json::Value::String(sys);
    }
    if let Some(temp) = req.temperature {
        body["temperature"] = serde_json::json!(temp);
    }

    match http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(anthropic_resp) => {
                // Translate Anthropic response back to OpenAI format
                let content = anthropic_resp
                    .get("content")
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|block| block.get("text"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("");

                let usage_in = anthropic_resp
                    .get("usage")
                    .and_then(|u| u.get("input_tokens"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let usage_out = anthropic_resp
                    .get("usage")
                    .and_then(|u| u.get("output_tokens"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Check for API error
                if let Some(err) = anthropic_resp.get("error") {
                    return error_response(
                        err.get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Anthropic API error"),
                    );
                }

                Json(serde_json::json!({
                    "id": anthropic_resp.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                    "object": "chat.completion",
                    "created": chrono::Utc::now().timestamp(),
                    "model": req.model,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": content,
                        },
                        "finish_reason": anthropic_resp
                            .get("stop_reason")
                            .and_then(|v| v.as_str())
                            .unwrap_or("stop"),
                    }],
                    "usage": {
                        "prompt_tokens": usage_in,
                        "completion_tokens": usage_out,
                        "total_tokens": usage_in + usage_out,
                    }
                }))
            }
            Err(e) => error_response(&format!("failed to parse Anthropic response: {e}")),
        },
        Err(e) => error_response(&format!("Anthropic request failed: {e}")),
    }
}

fn error_response(message: &str) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "error": {
            "message": message,
            "type": "proxy_error",
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determine_openai_provider() {
        assert!(matches!(determine_provider("gpt-4"), LlmProvider::OpenAi));
        assert!(matches!(
            determine_provider("gpt-3.5-turbo"),
            LlmProvider::OpenAi
        ));
    }

    #[test]
    fn determine_anthropic_provider() {
        assert!(matches!(
            determine_provider("claude-sonnet-4-20250514"),
            LlmProvider::Anthropic
        ));
    }

    #[test]
    fn determine_ollama_provider() {
        assert!(matches!(determine_provider("llama3"), LlmProvider::Ollama));
        assert!(matches!(determine_provider("mistral"), LlmProvider::Ollama));
    }

    #[test]
    fn chat_message_serialization() {
        let msg = ChatMessage {
            role: "user".into(),
            content: "Hello".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["role"], "user");
        assert_eq!(json["content"], "Hello");
    }

    #[test]
    fn chat_request_deserialization() {
        let json = serde_json::json!({
            "model": "gpt-4",
            "messages": [
                {"role": "user", "content": "Hi"}
            ]
        });
        let req: ChatCompletionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert_eq!(req.messages.len(), 1);
    }

    #[test]
    fn chat_request_with_optional_fields() {
        let json = serde_json::json!({
            "model": "llama3",
            "messages": [{"role": "user", "content": "test"}],
            "temperature": 0.7,
            "max_tokens": 1000,
            "stream": false
        });
        let req: ChatCompletionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(1000));
        assert_eq!(req.stream, Some(false));
    }
}
