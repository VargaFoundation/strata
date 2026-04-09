//! MCP Streamable HTTP transport — JSON-RPC over HTTP with SSE.
//!
//! Implements the Model Context Protocol (MCP) server endpoint.
//! Clients POST JSON-RPC requests to /mcp, server responds with JSON-RPC responses.

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use strata_core::StrataEngine;

/// MCP JSON-RPC request envelope.
#[derive(Debug, serde::Deserialize)]
pub struct McpRequest {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// MCP JSON-RPC response envelope.
#[derive(Debug, serde::Serialize)]
pub struct McpResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpError>,
}

#[derive(Debug, serde::Serialize)]
pub struct McpError {
    pub code: i32,
    pub message: String,
}

impl McpResponse {
    fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Option<serde_json::Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(McpError { code, message }),
        }
    }
}

/// Handle MCP JSON-RPC requests.
pub async fn handle_mcp(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<McpRequest>,
) -> Json<McpResponse> {
    let id = req.id.clone();

    let response = match req.method.as_str() {
        "initialize" => McpResponse::success(
            id,
            serde_json::json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {
                    "tools": { "listChanged": false },
                    "resources": { "listChanged": false },
                },
                "serverInfo": {
                    "name": "strata",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
        ),

        "tools/list" => {
            let tools = super::tools::list_tools();
            let tool_defs: Vec<serde_json::Value> = tools
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "name": t.name,
                        "description": t.description,
                        "inputSchema": tool_input_schema(&t.name),
                    })
                })
                .collect();
            McpResponse::success(id, serde_json::json!({ "tools": tool_defs }))
        }

        "tools/call" => {
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let args = req.params.get("arguments").cloned().unwrap_or_default();
            match call_tool(&engine, tool_name, &args).await {
                Ok(result) => McpResponse::success(
                    id,
                    serde_json::json!({
                        "content": [{"type": "text", "text": result.to_string()}]
                    }),
                ),
                Err(e) => McpResponse::error(id, -32000, e),
            }
        }

        "resources/list" => {
            let resources = super::resources::list_resources();
            let res_defs: Vec<serde_json::Value> = resources
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "uri": r.uri,
                        "name": r.name,
                        "description": r.description,
                    })
                })
                .collect();
            McpResponse::success(id, serde_json::json!({ "resources": res_defs }))
        }

        "prompts/list" => {
            let prompts = super::prompts::list_prompts();
            let prompt_defs: Vec<serde_json::Value> = prompts
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "description": p.description,
                    })
                })
                .collect();
            McpResponse::success(id, serde_json::json!({ "prompts": prompt_defs }))
        }

        "ping" => McpResponse::success(id, serde_json::json!({})),

        _ => McpResponse::error(id, -32601, format!("method not found: {}", req.method)),
    };

    Json(response)
}

fn tool_input_schema(name: &str) -> serde_json::Value {
    match name {
        "query" => serde_json::json!({
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL query to execute"}
            },
            "required": ["sql"]
        }),
        "ingest" => serde_json::json!({
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Event source name"},
                "events": {"type": "array", "description": "Array of event objects"}
            },
            "required": ["source", "events"]
        }),
        "search" => serde_json::json!({
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query text"},
                "k": {"type": "number", "description": "Number of results"}
            },
            "required": ["query"]
        }),
        "get_state" => serde_json::json!({
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "key": {"type": "string"}
            },
            "required": ["agent_id", "key"]
        }),
        "set_state" => serde_json::json!({
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "key": {"type": "string"},
                "value": {"description": "Value to set"}
            },
            "required": ["agent_id", "key", "value"]
        }),
        "embed" => serde_json::json!({
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Text to embed"}
            },
            "required": ["text"]
        }),
        _ => serde_json::json!({"type": "object"}),
    }
}

async fn call_tool(
    engine: &StrataEngine,
    name: &str,
    args: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    match name {
        "query" => {
            let sql = args
                .get("sql")
                .and_then(|v| v.as_str())
                .ok_or("missing 'sql' parameter")?;
            engine
                .query_sql(sql)
                .map(|rows| serde_json::json!({"rows": rows, "count": rows.len()}))
                .map_err(|e| e.to_string())
        }

        "ingest" => {
            let source = args
                .get("source")
                .and_then(|v| v.as_str())
                .ok_or("missing 'source' parameter")?;
            let events_json = args
                .get("events")
                .and_then(|v| v.as_array())
                .ok_or("missing 'events' parameter")?;

            let events: Vec<strata_core::memory::episodic::Event> = events_json
                .iter()
                .map(|payload| strata_core::memory::episodic::Event {
                    id: uuid::Uuid::new_v4(),
                    source: source.to_string(),
                    event_type: payload
                        .get("event_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    payload: payload.clone(),
                    timestamp: chrono::Utc::now(),
                })
                .collect();

            engine
                .ingest(events)
                .await
                .map(|count| serde_json::json!({"ingested": count}))
                .map_err(|e| e.to_string())
        }

        "get_state" => {
            let agent_id = args
                .get("agent_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'agent_id'")?;
            let key = args
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("missing 'key'")?;
            engine
                .state_get(agent_id, key)
                .await
                .map(|entry| match entry {
                    Some(e) => serde_json::json!({
                        "agent_id": e.agent_id,
                        "key": e.key,
                        "value": e.value,
                        "version": e.version,
                    }),
                    None => serde_json::json!({"error": "not found"}),
                })
                .map_err(|e| e.to_string())
        }

        "set_state" => {
            let agent_id = args
                .get("agent_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'agent_id'")?;
            let key = args
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("missing 'key'")?;
            let value = args.get("value").cloned().ok_or("missing 'value'")?;
            engine
                .state_set(agent_id, key, value)
                .await
                .map(|version| serde_json::json!({"version": version}))
                .map_err(|e| e.to_string())
        }

        "search" | "embed" => Ok(serde_json::json!({
            "message": "not yet implemented — requires embedding provider"
        })),

        _ => Err(format!("unknown tool: {name}")),
    }
}
