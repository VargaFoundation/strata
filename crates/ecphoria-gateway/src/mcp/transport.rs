//! MCP transport — Streamable HTTP (JSON-RPC 2.0) at `/mcp`.
//!
//! Implements the Model Context Protocol (MCP) server endpoint:
//! - **POST `/mcp`** — clients send JSON-RPC requests, receive JSON-RPC responses. The
//!   `initialize` response carries an `Mcp-Session-Id` header (Streamable HTTP session).
//! - **GET `/mcp`** — opens a server→client SSE stream. Ecphoria is a stateless tool server
//!   (no server-initiated messages), so this is an idle keep-alive stream; its presence lets
//!   Streamable-HTTP clients (e.g. Claude Desktop) connect natively rather than via `mcp-remote`.
//!
//! See `docs/connect-claude.md`.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures::stream::Stream;

use crate::auth::middleware::AuthContext;
use ecphoria_cluster::raft::types::{AppRequest, AppResponse};
use ecphoria_cluster::ClusterCoordinator;
use ecphoria_core::memory::cognition::{MemoryInput, MemoryScope};
use ecphoria_core::EcphoriaEngine;
use tokio::sync::RwLock;

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

/// Open a server→client SSE stream (Streamable HTTP `GET /mcp`).
///
/// Ecphoria sends no server-initiated messages, so this is an idle keep-alive stream — it exists
/// so Streamable-HTTP clients that probe `GET` get a valid `text/event-stream` instead of 405.
pub async fn handle_mcp_sse() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = futures::stream::pending::<Result<Event, Infallible>>();
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Handle MCP JSON-RPC requests (Streamable HTTP `POST /mcp`).
pub async fn handle_mcp(
    State(engine): State<Arc<EcphoriaEngine>>,
    cluster: Option<Extension<Arc<RwLock<ClusterCoordinator>>>>,
    auth: Option<Extension<AuthContext>>,
    Json(req): Json<McpRequest>,
) -> Response {
    let id = req.id.clone();
    let is_initialize = req.method == "initialize";

    // Bind every tool call to the AUTHENTICATED tenant (from the Bearer token). A client can NOT
    // widen its scope with a `tenant_id` argument — the token's tenant is authoritative. When auth
    // is disabled (or a legacy bare key carries no tenant) this is `None` → unscoped/"default",
    // matching the REST handlers' behaviour.
    let tenant: Option<String> = auth
        .as_ref()
        .and_then(|Extension(ctx)| ctx.tenant_id.clone());

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
                    "name": "ecphoria",
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
                        "inputSchema": t.input_schema,
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
            let coord = cluster.as_ref().map(|Extension(c)| c.clone());
            match call_tool(&engine, coord, tenant.as_deref(), tool_name, &args).await {
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

    if is_initialize {
        // Hand the client a session id to echo on subsequent requests (Streamable HTTP).
        let session_id = uuid::Uuid::new_v4().to_string();
        ([("Mcp-Session-Id", session_id)], Json(response)).into_response()
    } else {
        Json(response).into_response()
    }
}

/// Build a memory scope from MCP tool arguments (tenant/user/agent/session).
fn scope_from_args(args: &serde_json::Value) -> MemoryScope {
    let s = |k: &str| args.get(k).and_then(|v| v.as_str()).map(|x| x.to_string());
    MemoryScope {
        tenant_id: s("tenant_id").unwrap_or_else(|| "default".into()),
        user_id: s("user_id"),
        agent_id: s("agent_id"),
        session_id: s("session_id"),
    }
}

/// Like [`scope_from_args`] but forces the tenant to the authenticated one when present, so a
/// client-supplied `tenant_id` argument can never widen the scope to another tenant's memories.
fn scoped_for(args: &serde_json::Value, tenant: Option<&str>) -> MemoryScope {
    let mut scope = scope_from_args(args);
    if let Some(t) = tenant {
        scope.tenant_id = t.to_string();
    }
    scope
}

/// Replicate a write through the Raft log via the leader. MCP isn't leader-forwarded, so a write
/// that reaches a follower surfaces a clear "retry on the leader" message.
async fn mcp_cluster_write(
    coord: &Arc<RwLock<ClusterCoordinator>>,
    ar: AppRequest,
) -> Result<AppResponse, String> {
    coord.read().await.client_write(ar).await.map_err(|e| {
        let msg = e.to_string();
        if msg.contains("ForwardToLeader") || msg.to_lowercase().contains("not leader") {
            format!("not the cluster leader — retry on the leader node ({msg})")
        } else {
            msg
        }
    })
}

async fn call_tool(
    engine: &EcphoriaEngine,
    cluster: Option<Arc<RwLock<ClusterCoordinator>>>,
    tenant: Option<&str>,
    name: &str,
    args: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    match name {
        "query" => {
            let sql = args
                .get("sql")
                .and_then(|v| v.as_str())
                .ok_or("missing 'sql' parameter")?;
            let rows = match tenant {
                Some(t) => engine.query_sql_for_tenant(sql, t).await,
                None => engine.query_sql(sql).await,
            }
            .map_err(|e| e.to_string())?;
            let count = rows.len();
            Ok(serde_json::json!({"rows": rows, "count": count}))
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

            let events: Vec<ecphoria_core::memory::episodic::Event> = events_json
                .iter()
                .map(|payload| ecphoria_core::memory::episodic::Event {
                    id: uuid::Uuid::new_v4(),
                    source: source.to_string(),
                    event_type: payload
                        .get("event_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    payload: payload.clone(),
                    timestamp: chrono::Utc::now(),
                    parent_id: None,
                    trace_id: None,
                    tags: vec![],
                    idempotency_key: None,
                })
                .collect();

            if let Some(coord) = &cluster {
                let n = match mcp_cluster_write(
                    coord,
                    AppRequest::Ingest {
                        events,
                        tenant: tenant.map(|t| t.to_string()),
                    },
                )
                .await?
                {
                    AppResponse::Ingested(n) => n,
                    _ => 0,
                };
                return Ok(serde_json::json!({ "ingested": n }));
            }
            match tenant {
                Some(t) => {
                    let tc = ecphoria_core::config::TenantContext::new(t.to_string());
                    engine.ingest_for_tenant(events, &tc).await
                }
                None => engine.ingest(events).await,
            }
            .map(|count| serde_json::json!({ "ingested": count }))
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
            let entry = match tenant {
                Some(t) => engine.state_get_for_tenant(t, agent_id, key).await,
                None => engine.state_get(agent_id, key).await,
            }
            .map_err(|e| e.to_string())?;
            Ok(match entry {
                Some(e) => serde_json::json!({
                    "agent_id": e.agent_id,
                    "key": e.key,
                    "value": e.value,
                    "version": e.version,
                }),
                None => serde_json::json!({"error": "not found"}),
            })
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
            if let Some(coord) = &cluster {
                let v = match mcp_cluster_write(
                    coord,
                    AppRequest::StateSet {
                        agent_id: agent_id.to_string(),
                        key: key.to_string(),
                        value,
                        tenant: tenant.map(|t| t.to_string()),
                    },
                )
                .await?
                {
                    AppResponse::StateVersion(v) => v,
                    _ => 0,
                };
                return Ok(serde_json::json!({ "version": v }));
            }
            match tenant {
                Some(t) => engine.state_set_for_tenant(t, agent_id, key, value).await,
                None => engine.state_set(agent_id, key, value).await,
            }
            .map(|version| serde_json::json!({ "version": version }))
            .map_err(|e| e.to_string())
        }

        "search" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or("missing 'text' parameter")?;
            let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            let results = match tenant {
                Some(t) => {
                    engine
                        .embed_and_search_for_tenant(text, k, t, None, None)
                        .await
                }
                None => engine.embed_and_search(text, k, None, None).await,
            }
            .map_err(|e| e.to_string())?;
            let items: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.entry.id.to_string(),
                        "content": r.entry.content,
                        "metadata": r.entry.metadata,
                        "score": r.score,
                    })
                })
                .collect();
            Ok(serde_json::json!({"results": items, "count": items.len()}))
        }

        "embed" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or("missing 'text' parameter")?;
            engine
                .embed_text(text)
                .await
                .map(|vector| serde_json::json!({"embedding": vector, "dimension": vector.len()}))
                .map_err(|e| e.to_string())
        }

        "start_session" => {
            let session_id = args
                .get("session_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'session_id'")?;
            let agent_id = args
                .get("agent_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'agent_id'")?;
            let parent = args.get("parent_session_id").and_then(|v| v.as_str());
            match tenant {
                Some(t) => {
                    engine
                        .session_start_for_tenant(session_id, agent_id, parent, None, t)
                        .await
                }
                None => {
                    engine
                        .session_start(session_id, agent_id, parent, None)
                        .await
                }
            }
            .map(|()| serde_json::json!({"session_id": session_id, "status": "started"}))
            .map_err(|e| e.to_string())
        }

        "end_session" => {
            let session_id = args
                .get("session_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'session_id'")?;
            let summary = args.get("summary").and_then(|v| v.as_str());
            match tenant {
                Some(t) => engine
                    .session_end_for_tenant(session_id, summary, t)
                    .await
                    .map(|_| ()),
                None => engine.session_end(session_id, summary).await,
            }
            .map(|()| serde_json::json!({"session_id": session_id, "status": "ended"}))
            .map_err(|e| e.to_string())
        }

        "recall_session" => {
            let session_id = args
                .get("session_id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'session_id'")?;
            let events = match tenant {
                Some(t) => engine.session_recall_for_tenant(session_id, t).await,
                None => engine.session_recall(session_id).await,
            }
            .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({
                "session_id": session_id,
                "events": events,
                "count": events.len(),
            }))
        }

        "add_memory" => {
            let content = args
                .get("content")
                .and_then(|v| v.as_str())
                .ok_or("missing 'content' parameter")?;
            let input = MemoryInput {
                scope: scoped_for(args, tenant),
                subject: args
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                content: content.to_string(),
                importance: args
                    .get("importance")
                    .and_then(|v| v.as_f64())
                    .map(|f| f as f32),
                source_event_ids: vec![],
                metadata: serde_json::json!({}),
                mem_type: args
                    .get("mem_type")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            };
            if let Some(coord) = &cluster {
                // Run cognition on the leader, replicate the materialized rows through the log.
                let (result, rows) = engine.memory_plan(input).await.map_err(|e| e.to_string())?;
                mcp_cluster_write(coord, AppRequest::MemoryUpsert { rows }).await?;
                return Ok(serde_json::to_value(result).unwrap_or_default());
            }
            engine
                .memory_add(input)
                .await
                .map(|added| serde_json::to_value(added).unwrap_or_default())
                .map_err(|e| e.to_string())
        }

        "search_memory" => {
            let query = args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or("missing 'query' parameter")?;
            let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            engine
                .memory_search(query, &scoped_for(args, tenant), k)
                .await
                .map(|hits| serde_json::json!({"results": hits, "count": hits.len()}))
                .map_err(|e| e.to_string())
        }

        "get_memories" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
            engine
                .memory_all(&scoped_for(args, tenant), limit)
                .await
                .map(|mems| serde_json::json!({"memories": mems, "count": mems.len()}))
                .map_err(|e| e.to_string())
        }

        "memory_history" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'id' parameter")?;
            let uuid = uuid::Uuid::parse_str(id).map_err(|_| "invalid 'id'".to_string())?;
            // Fetch tenant-scoped so a cross-tenant id reads as "not found".
            let mem = match tenant {
                Some(t) => engine.memory_get_scoped(uuid, t).await,
                None => engine.memory_get(uuid).await,
            }
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "memory not found".to_string())?;
            match mem.subject.clone() {
                Some(subject) => engine
                    .memory_history(&mem.scope, &subject)
                    .await
                    .map(
                        |h| serde_json::json!({"subject": subject, "history": h, "count": h.len()}),
                    )
                    .map_err(|e| e.to_string()),
                None => Ok(serde_json::json!({"history": [mem], "count": 1})),
            }
        }

        "delete_memory" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing 'id' parameter")?;
            let uuid = uuid::Uuid::parse_str(id).map_err(|_| "invalid 'id'".to_string())?;
            if let Some(coord) = &cluster {
                // Only replicate a delete for a memory the tenant actually owns.
                if let Some(t) = tenant {
                    if engine
                        .memory_get_scoped(uuid, t)
                        .await
                        .map_err(|e| e.to_string())?
                        .is_none()
                    {
                        return Err("memory not found".to_string());
                    }
                }
                mcp_cluster_write(coord, AppRequest::MemoryDelete { id: uuid }).await?;
                return Ok(serde_json::json!({ "id": id, "deleted": true }));
            }
            let deleted = match tenant {
                Some(t) => engine.memory_delete_scoped(uuid, t).await,
                None => engine.memory_delete(uuid).await.map(|()| true),
            }
            .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"id": id, "deleted": deleted}))
        }

        "remember" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or("missing 'text' parameter")?;
            let scope = scoped_for(args, tenant);
            // Cluster mode: run extraction+cognition on the leader, replicate each distilled fact's
            // rows through the Raft log so followers converge (was previously a local-only write).
            if let Some(coord) = &cluster {
                let plans = engine
                    .memory_remember_plan(text, &scope)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut added = Vec::with_capacity(plans.len());
                for (result, rows) in plans {
                    mcp_cluster_write(coord, AppRequest::MemoryUpsert { rows }).await?;
                    added.push(result);
                }
                return Ok(serde_json::json!({"remembered": added.len(), "memories": added}));
            }
            engine
                .memory_remember(text, &scope)
                .await
                .map(|added| serde_json::json!({"remembered": added.len(), "memories": added}))
                .map_err(|e| e.to_string())
        }

        "link_memory" => {
            let src = args
                .get("src")
                .and_then(|v| v.as_str())
                .ok_or("missing 'src' parameter")?;
            let relation = args
                .get("relation")
                .and_then(|v| v.as_str())
                .ok_or("missing 'relation' parameter")?;
            let dst = args
                .get("dst")
                .and_then(|v| v.as_str())
                .ok_or("missing 'dst' parameter")?;
            let supersede = args
                .get("supersede")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let t = tenant.unwrap_or("default");
            // Cluster mode: mint the edge id on the leader and replicate it (and the functional
            // supersede) through the Raft log, mirroring the REST handler so followers converge.
            if let Some(coord) = &cluster {
                let at = chrono::Utc::now();
                let id = uuid::Uuid::new_v4();
                if supersede {
                    mcp_cluster_write(
                        coord,
                        AppRequest::GraphSupersede {
                            tenant: Some(t.to_string()),
                            src: src.to_string(),
                            relation: relation.to_string(),
                            at,
                            by: Some(id),
                        },
                    )
                    .await?;
                }
                let edge = ecphoria_core::memory::cognition::Edge {
                    id,
                    src: src.to_string(),
                    relation: relation.to_string(),
                    dst: dst.to_string(),
                    weight: 1.0,
                    source_memory_id: None,
                    valid_from: Some(at),
                    ..Default::default()
                };
                mcp_cluster_write(
                    coord,
                    AppRequest::GraphAddEdge {
                        tenant: Some(t.to_string()),
                        edge,
                    },
                )
                .await?;
                return Ok(serde_json::json!({ "status": "ok" }));
            }
            let result = if supersede {
                engine
                    .memory_link_functional(t, src, relation, dst, None)
                    .await
            } else {
                engine.memory_link(t, src, relation, dst, None).await
            };
            result
                .map(|()| serde_json::json!({ "status": "ok" }))
                .map_err(|e| e.to_string())
        }

        "graph_neighbors" => {
            let entity = args
                .get("entity")
                .and_then(|v| v.as_str())
                .ok_or("missing 'entity' parameter")?;
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
            let depth = args.get("depth").and_then(|v| v.as_u64()).unwrap_or(1);
            let t = tenant.unwrap_or("default");
            let edges = if depth > 1 {
                engine
                    .memory_subgraph(t, entity, depth as usize, limit)
                    .await
            } else {
                engine.memory_neighbors(t, entity, limit).await
            }
            .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({ "entity": entity, "edges": edges, "count": edges.len() }))
        }

        _ => Err(format!("unknown tool: {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn engine() -> Arc<EcphoriaEngine> {
        let mut c = ecphoria_core::CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        Arc::new(EcphoriaEngine::new(c).await.unwrap())
    }

    #[tokio::test]
    async fn initialize_issues_session_id() {
        let req = McpRequest {
            jsonrpc: "2.0".into(),
            id: Some(serde_json::json!(1)),
            method: "initialize".into(),
            params: serde_json::json!({}),
        };
        let resp = handle_mcp(State(engine().await), None, None, Json(req)).await;
        assert!(resp.headers().get("Mcp-Session-Id").is_some());
    }

    #[tokio::test]
    async fn non_initialize_has_no_session_id() {
        let req = McpRequest {
            jsonrpc: "2.0".into(),
            id: Some(serde_json::json!(2)),
            method: "ping".into(),
            params: serde_json::json!({}),
        };
        let resp = handle_mcp(State(engine().await), None, None, Json(req)).await;
        assert!(resp.headers().get("Mcp-Session-Id").is_none());
    }

    #[tokio::test]
    async fn mcp_tool_calls_are_tenant_scoped() {
        // Regression test: MCP tool calls must be bound to the authenticated tenant, so one tenant
        // cannot read another's data via the `query` tool (or by naming a `tenant_id` in args).
        let e = engine().await;
        let ingest_args = serde_json::json!({
            "source": "test",
            "events": [{"event_type": "note", "msg": "secret-of-a"}],
        });
        // Tenant A ingests one event.
        call_tool(&e, None, Some("tenant-a"), "ingest", &ingest_args)
            .await
            .unwrap();

        let query = serde_json::json!({"sql": "SELECT * FROM episodic"});
        // Tenant B must see zero rows (isolation) …
        let res_b = call_tool(&e, None, Some("tenant-b"), "query", &query)
            .await
            .unwrap();
        assert_eq!(res_b["count"], 0, "tenant B must not see tenant A's events");
        // … while tenant A sees its own event.
        let res_a = call_tool(&e, None, Some("tenant-a"), "query", &query)
            .await
            .unwrap();
        assert_eq!(res_a["count"], 1, "tenant A must see its own event");

        // A client-supplied tenant_id argument on a memory write cannot widen scope: the memory is
        // stored under the authenticated tenant, so the other tenant's search can't find it.
        let add = serde_json::json!({"content": "b likes green", "tenant_id": "tenant-a"});
        call_tool(&e, None, Some("tenant-b"), "add_memory", &add)
            .await
            .unwrap();
        let search = serde_json::json!({"query": "green"});
        let seen_by_a = call_tool(&e, None, Some("tenant-a"), "search_memory", &search)
            .await
            .unwrap();
        assert_eq!(
            seen_by_a["count"], 0,
            "tenant A must not see tenant B's memory even though the arg named tenant-a"
        );
    }

    #[tokio::test]
    async fn sse_stream_is_event_stream() {
        let resp = handle_mcp_sse().await.into_response();
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("text/event-stream"));
    }
}
