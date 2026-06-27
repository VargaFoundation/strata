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

use std::convert::Infallible;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::StreamExt;
use reqwest::Client;
use strata_core::memory::cognition::MemoryScope;
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
    /// Standard OpenAI `user` field — used to scope auto-RAG to that user's memories.
    #[serde(default)]
    pub user: Option<String>,
    /// OpenAI-style tool definitions, passed through (translated) to the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<serde_json::Value>,
    /// OpenAI-style tool_choice, passed through (translated) to the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub role: String,
    /// Optional: assistant tool-call turns carry `content: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// OpenAI tool calls on an assistant message (multi-turn tool use).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<serde_json::Value>,
    /// For `role: "tool"` messages: the id of the tool call this result answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl ChatMessage {
    /// Convenience: a plain text message.
    fn text(role: &str, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: Some(content.into()),
            tool_calls: None,
            tool_call_id: None,
        }
    }
    fn content_str(&self) -> &str {
        self.content.as_deref().unwrap_or("")
    }
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
    auth: Option<axum::Extension<crate::auth::middleware::AuthContext>>,
    Json(mut req): Json<ChatCompletionRequest>,
) -> Response {
    // Tenant for memory-RAG scoping (so the proxy can't leak one tenant's memories to another).
    let req_tenant = auth
        .as_ref()
        .and_then(|axum::Extension(c)| c.tenant_id.clone());
    // 1. Extract last user message for context search
    let user_query = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content_str().to_string())
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

    // 2c. User memories: hybrid (BM25 + vector) search over the user's distilled memories,
    // scoped by the standard OpenAI `user` field. This feeds the cognition layer into RAG.
    if let Some(user) = req.user.as_deref().filter(|u| !u.is_empty()) {
        if !user_query.is_empty() {
            let scope = MemoryScope {
                tenant_id: req_tenant.clone().unwrap_or_else(|| "default".to_string()),
                user_id: Some(user.to_string()),
                agent_id: None,
                session_id: None,
            };
            if let Ok(hits) = engine.memory_search(&user_query, &scope, 5).await {
                let memory_lines: Vec<String> = hits
                    .iter()
                    .map(|h| {
                        format!(
                            "- {}",
                            h.memory.content.chars().take(300).collect::<String>()
                        )
                    })
                    .collect();
                if !memory_lines.is_empty() {
                    context_sections.push(format!(
                        "### What we remember about this user\n{}",
                        memory_lines.join("\n")
                    ));
                }
            }
        }
    }

    // 2d. Inject combined context into the conversation
    if !context_sections.is_empty() {
        let context_block = format!(
            "## Context from Strata\nThe following context was automatically retrieved from Strata's memory stores.\n\n{}",
            context_sections.join("\n\n")
        );

        if let Some(sys_msg) = req.messages.iter_mut().find(|m| m.role == "system") {
            sys_msg.content = Some(format!("{}\n\n{}", context_block, sys_msg.content_str()));
        } else {
            req.messages
                .insert(0, ChatMessage::text("system", context_block));
        }
    }

    // 3. Streaming bypasses the (non-streaming) semantic cache and relays the provider's SSE.
    if req.stream == Some(true) {
        static STREAM_CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
        let http = STREAM_CLIENT.get_or_init(Client::new);
        let config = engine.config();
        return stream_chat(http, config, &req).await;
    }

    // 3b. Check semantic cache — skip LLM call if we have a cached response
    static CACHE: std::sync::OnceLock<super::cache::SemanticCache> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(super::cache::SemanticCache::new);

    // Try to get a cached response by embedding the user query
    let query_embedding = if !user_query.is_empty() {
        engine.embed_text(&user_query).await.ok()
    } else {
        None
    };

    if let Some(ref emb) = query_embedding {
        if let Some(cached) = cache.get_by_vector(emb).await {
            metrics::counter!("strata_llm_cache_hits_total").increment(1);
            // Return cached response in OpenAI format
            return Json(serde_json::json!({
                "id": format!("cache-{}", uuid::Uuid::new_v4()),
                "object": "chat.completion",
                "created": chrono::Utc::now().timestamp(),
                "model": req.model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": cached},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "_cached": true,
            }))
            .into_response();
        }
    }
    metrics::counter!("strata_llm_cache_misses_total").increment(1);

    // 4. Determine provider and forward (shared client for connection reuse)
    let config = engine.config();
    let provider = determine_provider(&req.model);
    static HTTP_CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
    let http = HTTP_CLIENT.get_or_init(Client::new);

    let response = match provider {
        LlmProvider::OpenAi => {
            forward_to_openai(http, &config.embedding.openai_api_key, &req).await
        }
        LlmProvider::Ollama => forward_to_ollama(http, &config.embedding.ollama_url, &req).await,
        LlmProvider::Anthropic => forward_to_anthropic(http, &req).await,
    };

    // 5. Cache the response for future similar queries
    if let Some(ref emb) = query_embedding {
        if let Some(content) = response
            .0
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
        {
            cache.put_with_vector(&user_query, content, Some(emb)).await;
        }
    }

    response.into_response()
}

/// Dispatch a streaming chat completion to the right provider, returning an SSE response.
async fn stream_chat(
    http: &Client,
    config: &strata_core::CoreConfig,
    req: &ChatCompletionRequest,
) -> Response {
    match determine_provider(&req.model) {
        // OpenAI and Ollama already speak the OpenAI SSE wire format → relay bytes verbatim.
        LlmProvider::OpenAi => {
            stream_passthrough(
                http,
                "https://api.openai.com/v1/chat/completions",
                Some(&config.embedding.openai_api_key),
                req,
            )
            .await
        }
        LlmProvider::Ollama => {
            let url = format!(
                "{}/v1/chat/completions",
                config.embedding.ollama_url.trim_end_matches('/')
            );
            stream_passthrough(http, &url, None, req).await
        }
        // Anthropic speaks its own SSE format → translate each event to an OpenAI chunk.
        LlmProvider::Anthropic => stream_anthropic(http, req).await,
    }
}

/// Relay an upstream OpenAI-compatible SSE byte stream verbatim.
async fn stream_passthrough(
    http: &Client,
    url: &str,
    api_key: Option<&str>,
    req: &ChatCompletionRequest,
) -> Response {
    let mut rb = http.post(url).json(req);
    if let Some(k) = api_key {
        rb = rb.bearer_auth(k);
    }
    match rb.send().await {
        Ok(resp) => axum::response::Response::builder()
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .body(axum::body::Body::from_stream(resp.bytes_stream()))
            .unwrap_or_else(|_| error_response("failed to build stream").into_response()),
        Err(e) => error_response(&format!("upstream stream failed: {e}")).into_response(),
    }
}

/// Stream from Anthropic, translating its SSE events into OpenAI `chat.completion.chunk`s.
async fn stream_anthropic(http: &Client, req: &ChatCompletionRequest) -> Response {
    let api_key = strata_core::config::resolve_secret("ANTHROPIC_API_KEY");
    if api_key.is_empty() {
        return error_response("ANTHROPIC_API_KEY environment variable not set").into_response();
    }
    let mut body = build_anthropic_body(req);
    body["stream"] = serde_json::Value::Bool(true);

    let resp = match http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return error_response(&format!("Anthropic stream failed: {e}")).into_response(),
    };

    let id = format!("chatcmpl-{}", uuid::Uuid::new_v4());
    let model = req.model.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);

    // Reader task: parse Anthropic SSE frames and forward translated OpenAI chunks.
    tokio::spawn(async move {
        let mut upstream = resp.bytes_stream();
        // Buffer raw BYTES (not a String): a multi-byte UTF-8 char can be split across two network
        // chunks; decoding each chunk independently would corrupt it. We only decode complete lines.
        let mut buf: Vec<u8> = Vec::new();
        let mut state = StreamState::default();
        while let Some(chunk) = upstream.next().await {
            let Ok(bytes) = chunk else { break };
            buf.extend_from_slice(&bytes);
            // Anthropic emits one JSON object per `data:` line, frames separated by newlines.
            while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = buf.drain(..=nl).collect();
                let line = String::from_utf8_lossy(&line_bytes);
                let line = line.trim();
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data.is_empty() {
                    continue;
                }
                if let Ok(ev) = serde_json::from_str::<serde_json::Value>(data) {
                    for chunk_json in anthropic_event_to_openai_chunks(&ev, &id, &model, &mut state)
                    {
                        if tx
                            .send(Ok(Event::default().data(chunk_json)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        }
        let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Translate one Anthropic streaming event into zero or more OpenAI chunk payloads (pure).
/// Streaming-translation state: maps an Anthropic content-block index (which counts text AND
/// tool_use blocks) to a sequential OpenAI tool_call index.
#[derive(Default)]
struct StreamState {
    tool_index_by_block: std::collections::HashMap<u64, usize>,
    next_tool_index: usize,
}

fn anthropic_event_to_openai_chunks(
    ev: &serde_json::Value,
    id: &str,
    model: &str,
    state: &mut StreamState,
) -> Vec<String> {
    let typ = ev.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let chunk = |delta: serde_json::Value, finish: serde_json::Value| {
        serde_json::json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": 0,
            "model": model,
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
        })
        .to_string()
    };
    match typ {
        "message_start" => vec![chunk(
            serde_json::json!({ "role": "assistant" }),
            serde_json::Value::Null,
        )],
        // A tool_use block begins → open a new OpenAI tool_call (id + name, empty args).
        "content_block_start" => {
            let block = ev.get("content_block");
            if block.and_then(|b| b.get("type")).and_then(|v| v.as_str()) == Some("tool_use") {
                let a_idx = ev.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let o_idx = state.next_tool_index;
                state.next_tool_index += 1;
                state.tool_index_by_block.insert(a_idx, o_idx);
                let tool_id = block
                    .and_then(|b| b.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let name = block
                    .and_then(|b| b.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                vec![chunk(
                    serde_json::json!({ "tool_calls": [{
                        "index": o_idx, "id": tool_id, "type": "function",
                        "function": { "name": name, "arguments": "" }
                    }]}),
                    serde_json::Value::Null,
                )]
            } else {
                vec![]
            }
        }
        "content_block_delta" => {
            let delta = ev.get("delta");
            // Text delta → assistant content.
            if let Some(text) = delta.and_then(|d| d.get("text")).and_then(|v| v.as_str()) {
                return vec![chunk(
                    serde_json::json!({ "content": text }),
                    serde_json::Value::Null,
                )];
            }
            // Tool-input JSON delta → tool_call arguments fragment.
            if let Some(partial) = delta
                .and_then(|d| d.get("partial_json"))
                .and_then(|v| v.as_str())
            {
                let a_idx = ev.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let o_idx = *state.tool_index_by_block.get(&a_idx).unwrap_or(&0);
                return vec![chunk(
                    serde_json::json!({ "tool_calls": [{
                        "index": o_idx, "function": { "arguments": partial }
                    }]}),
                    serde_json::Value::Null,
                )];
            }
            vec![]
        }
        "message_delta" => {
            let stop = ev
                .get("delta")
                .and_then(|d| d.get("stop_reason"))
                .and_then(|v| v.as_str())
                .unwrap_or("end_turn");
            let finish = match stop {
                "tool_use" => "tool_calls",
                "max_tokens" => "length",
                _ => "stop",
            };
            vec![chunk(serde_json::json!({}), serde_json::json!(finish))]
        }
        // content_block_stop, ping, message_stop carry no OpenAI-visible delta.
        _ => vec![],
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

/// Translate one OpenAI tool-call object into an Anthropic `tool_use` content block.
fn openai_tool_call_to_anthropic(tc: &serde_json::Value) -> serde_json::Value {
    let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let func = tc.get("function");
    let name = func
        .and_then(|f| f.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    // OpenAI passes tool arguments as a JSON *string*; Anthropic wants a JSON object.
    let input = func
        .and_then(|f| f.get("arguments"))
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({ "type": "tool_use", "id": id, "name": name, "input": input })
}

/// Translate OpenAI-style messages into Anthropic Messages, handling multi-turn tool use:
/// assistant `tool_calls` → `tool_use` blocks; consecutive `role:"tool"` results → a single user
/// message of `tool_result` blocks (Anthropic requires tool results grouped in one user turn).
fn build_anthropic_messages(req: &ChatCompletionRequest) -> Vec<serde_json::Value> {
    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut pending_tool_results: Vec<serde_json::Value> = Vec::new();

    let flush_tool_results = |out: &mut Vec<serde_json::Value>,
                              pending: &mut Vec<serde_json::Value>| {
        if !pending.is_empty() {
            out.push(serde_json::json!({
                "role": "user",
                "content": std::mem::take(pending),
            }));
        }
    };

    for m in req.messages.iter().filter(|m| m.role != "system") {
        if m.role == "tool" {
            pending_tool_results.push(serde_json::json!({
                "type": "tool_result",
                "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                "content": m.content_str(),
            }));
            continue;
        }
        // Any non-tool message ends a run of tool results.
        flush_tool_results(&mut out, &mut pending_tool_results);

        if m.role == "assistant" {
            if let Some(tool_calls) = m.tool_calls.as_ref().and_then(|v| v.as_array()) {
                let mut blocks: Vec<serde_json::Value> = Vec::new();
                if !m.content_str().is_empty() {
                    blocks.push(serde_json::json!({ "type": "text", "text": m.content_str() }));
                }
                for tc in tool_calls {
                    blocks.push(openai_tool_call_to_anthropic(tc));
                }
                out.push(serde_json::json!({ "role": "assistant", "content": blocks }));
                continue;
            }
        }
        out.push(serde_json::json!({ "role": m.role, "content": m.content_str() }));
    }
    flush_tool_results(&mut out, &mut pending_tool_results);
    out
}

/// Build the Anthropic Messages API request body from an OpenAI-style request (system split,
/// multi-turn message + tool-use translation, optional temperature + tools).
fn build_anthropic_body(req: &ChatCompletionRequest) -> serde_json::Value {
    let system = req
        .messages
        .iter()
        .find(|m| m.role == "system")
        .map(|m| m.content_str().to_string());

    let messages = build_anthropic_messages(req);

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
    if let Some(ref tools) = req.tools {
        body["tools"] = openai_tools_to_anthropic(tools);
        if let Some(ref tc) = req.tool_choice {
            body["tool_choice"] = openai_tool_choice_to_anthropic(tc);
        }
    }
    body
}

async fn forward_to_anthropic(
    http: &Client,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    // Translate OpenAI format to Anthropic Messages API format
    let api_key = strata_core::config::resolve_secret("ANTHROPIC_API_KEY");
    if api_key.is_empty() {
        return error_response("ANTHROPIC_API_KEY environment variable not set");
    }

    let body = build_anthropic_body(req);

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
                // Check for API error first.
                if let Some(err) = anthropic_resp.get("error") {
                    return error_response(
                        err.get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Anthropic API error"),
                    );
                }

                // Translate Anthropic content blocks → OpenAI text + tool_calls.
                let (content_text, tool_calls) = anthropic_content_to_openai(
                    anthropic_resp
                        .get("content")
                        .unwrap_or(&serde_json::Value::Null),
                );

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

                let finish_reason = match anthropic_resp
                    .get("stop_reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("end_turn")
                {
                    "tool_use" => "tool_calls",
                    "max_tokens" => "length",
                    "end_turn" | "stop_sequence" => "stop",
                    other => other,
                };

                let mut message = serde_json::json!({
                    "role": "assistant",
                    "content": content_text,
                });
                if !tool_calls.is_empty() {
                    message["tool_calls"] = serde_json::Value::Array(tool_calls);
                }

                Json(serde_json::json!({
                    "id": anthropic_resp.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                    "object": "chat.completion",
                    "created": chrono::Utc::now().timestamp(),
                    "model": req.model,
                    "choices": [{
                        "index": 0,
                        "message": message,
                        "finish_reason": finish_reason,
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

/// Translate OpenAI `tools` (`[{type:function, function:{name,description,parameters}}]`)
/// into Anthropic `tools` (`[{name,description,input_schema}]`).
fn openai_tools_to_anthropic(tools: &serde_json::Value) -> serde_json::Value {
    let out: Vec<serde_json::Value> = tools
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let f = t.get("function").unwrap_or(t);
                    let name = f.get("name")?.as_str()?;
                    let mut obj = serde_json::json!({ "name": name });
                    if let Some(d) = f.get("description").and_then(|v| v.as_str()) {
                        obj["description"] = serde_json::json!(d);
                    }
                    obj["input_schema"] = f
                        .get("parameters")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({"type": "object"}));
                    Some(obj)
                })
                .collect()
        })
        .unwrap_or_default();
    serde_json::Value::Array(out)
}

/// Translate OpenAI `tool_choice` into Anthropic's `tool_choice`.
fn openai_tool_choice_to_anthropic(tc: &serde_json::Value) -> serde_json::Value {
    match tc {
        serde_json::Value::String(s) if s == "required" || s == "any" => {
            serde_json::json!({"type": "any"})
        }
        serde_json::Value::Object(_) => tc
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|v| v.as_str())
            .map(|name| serde_json::json!({"type": "tool", "name": name}))
            .unwrap_or_else(|| serde_json::json!({"type": "auto"})),
        // "auto", "none", or anything else → let the model decide.
        _ => serde_json::json!({"type": "auto"}),
    }
}

/// Translate an Anthropic response `content` array into OpenAI `(text, tool_calls)`.
fn anthropic_content_to_openai(content: &serde_json::Value) -> (String, Vec<serde_json::Value>) {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    if let Some(arr) = content.as_array() {
        for block in arr {
            match block.get("type").and_then(|v| v.as_str()) {
                Some("text") => {
                    if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                        text.push_str(t);
                    }
                }
                Some("tool_use") => {
                    let input = block
                        .get("input")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    tool_calls.push(serde_json::json!({
                        "id": block.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                        "type": "function",
                        "function": {
                            "name": block.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                            "arguments": input.to_string(),
                        }
                    }));
                }
                _ => {}
            }
        }
    }
    (text, tool_calls)
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
    fn anthropic_text_delta_becomes_content_chunk() {
        let ev = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "text_delta", "text": "Hello" }
        });
        let chunks =
            anthropic_event_to_openai_chunks(&ev, "id1", "claude-x", &mut StreamState::default());
        assert_eq!(chunks.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&chunks[0]).unwrap();
        assert_eq!(parsed["object"], "chat.completion.chunk");
        assert_eq!(parsed["choices"][0]["delta"]["content"], "Hello");
    }

    #[test]
    fn anthropic_message_start_emits_role() {
        let ev = serde_json::json!({ "type": "message_start", "message": {} });
        let chunks = anthropic_event_to_openai_chunks(&ev, "id", "m", &mut StreamState::default());
        let parsed: serde_json::Value = serde_json::from_str(&chunks[0]).unwrap();
        assert_eq!(parsed["choices"][0]["delta"]["role"], "assistant");
    }

    #[test]
    fn anthropic_message_delta_maps_finish_reason() {
        let ev = serde_json::json!({ "type": "message_delta", "delta": { "stop_reason": "max_tokens" } });
        let chunks = anthropic_event_to_openai_chunks(&ev, "id", "m", &mut StreamState::default());
        let parsed: serde_json::Value = serde_json::from_str(&chunks[0]).unwrap();
        assert_eq!(parsed["choices"][0]["finish_reason"], "length");
    }

    #[test]
    fn anthropic_ping_emits_nothing() {
        let ev = serde_json::json!({ "type": "ping" });
        assert!(
            anthropic_event_to_openai_chunks(&ev, "id", "m", &mut StreamState::default())
                .is_empty()
        );
    }

    #[test]
    fn build_anthropic_body_splits_system_and_messages() {
        let req = ChatCompletionRequest {
            model: "claude-x".into(),
            messages: vec![
                ChatMessage::text("system", "be nice"),
                ChatMessage::text("user", "hi"),
            ],
            temperature: Some(0.5),
            max_tokens: Some(100),
            stream: None,
            user: None,
            tools: None,
            tool_choice: None,
        };
        let body = build_anthropic_body(&req);
        assert_eq!(body["system"], "be nice");
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["max_tokens"], 100);
        assert_eq!(body["temperature"], 0.5);
    }

    #[test]
    fn translate_openai_tools_to_anthropic_shape() {
        let tools = serde_json::json!([{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get weather",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}
            }
        }]);
        let out = openai_tools_to_anthropic(&tools);
        let arr = out.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "get_weather");
        assert_eq!(arr[0]["description"], "Get weather");
        assert!(arr[0]["input_schema"]["properties"]["city"].is_object());
    }

    #[test]
    fn translate_tool_choice_variants() {
        assert_eq!(
            openai_tool_choice_to_anthropic(&serde_json::json!("auto")),
            serde_json::json!({"type": "auto"})
        );
        assert_eq!(
            openai_tool_choice_to_anthropic(&serde_json::json!("required")),
            serde_json::json!({"type": "any"})
        );
        assert_eq!(
            openai_tool_choice_to_anthropic(
                &serde_json::json!({"type": "function", "function": {"name": "foo"}})
            ),
            serde_json::json!({"type": "tool", "name": "foo"})
        );
    }

    #[test]
    fn translate_anthropic_tool_use_response() {
        let content = serde_json::json!([
            {"type": "text", "text": "Let me check."},
            {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
        ]);
        let (text, calls) = anthropic_content_to_openai(&content);
        assert_eq!(text, "Let me check.");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "toolu_1");
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        let args: serde_json::Value =
            serde_json::from_str(calls[0]["function"]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(args["city"], "Paris");
    }

    #[test]
    fn chat_message_serialization() {
        let msg = ChatMessage::text("user", "Hello");
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["role"], "user");
        assert_eq!(json["content"], "Hello");
    }

    #[test]
    fn multi_turn_tool_use_translates_to_anthropic_blocks() {
        // assistant tool_call → user tool_result → assistant final.
        let req = ChatCompletionRequest {
            model: "claude-x".into(),
            messages: vec![
                ChatMessage::text("user", "weather in Paris?"),
                ChatMessage {
                    role: "assistant".into(),
                    content: None,
                    tool_calls: Some(serde_json::json!([{
                        "id": "call_1", "type": "function",
                        "function": { "name": "get_weather", "arguments": "{\"city\":\"Paris\"}" }
                    }])),
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "tool".into(),
                    content: Some("18°C".into()),
                    tool_calls: None,
                    tool_call_id: Some("call_1".into()),
                },
            ],
            temperature: None,
            max_tokens: None,
            stream: None,
            user: None,
            tools: None,
            tool_choice: None,
        };
        let msgs = build_anthropic_messages(&req);
        assert_eq!(msgs.len(), 3);
        // assistant tool_use block
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["content"][0]["type"], "tool_use");
        assert_eq!(msgs[1]["content"][0]["id"], "call_1");
        assert_eq!(msgs[1]["content"][0]["input"]["city"], "Paris");
        // tool result becomes a user message with a tool_result block
        assert_eq!(msgs[2]["role"], "user");
        assert_eq!(msgs[2]["content"][0]["type"], "tool_result");
        assert_eq!(msgs[2]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(msgs[2]["content"][0]["content"], "18°C");
    }

    #[test]
    fn streaming_tool_use_emits_openai_tool_call_deltas() {
        let mut st = StreamState::default();
        // Tool block starts (Anthropic block index 1, after a text block at 0).
        let start = serde_json::json!({
            "type": "content_block_start", "index": 1,
            "content_block": { "type": "tool_use", "id": "toolu_9", "name": "get_weather" }
        });
        let c0 = anthropic_event_to_openai_chunks(&start, "id", "m", &mut st);
        let p0: serde_json::Value = serde_json::from_str(&c0[0]).unwrap();
        let tc0 = &p0["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(tc0["index"], 0);
        assert_eq!(tc0["id"], "toolu_9");
        assert_eq!(tc0["function"]["name"], "get_weather");

        // Argument fragment for the same block.
        let delta = serde_json::json!({
            "type": "content_block_delta", "index": 1,
            "delta": { "type": "input_json_delta", "partial_json": "{\"city\":" }
        });
        let c1 = anthropic_event_to_openai_chunks(&delta, "id", "m", &mut st);
        let p1: serde_json::Value = serde_json::from_str(&c1[0]).unwrap();
        let tc1 = &p1["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(tc1["index"], 0);
        assert_eq!(tc1["function"]["arguments"], "{\"city\":");
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
