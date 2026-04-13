//! REST API handler functions with proper HTTP status codes and request IDs.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use strata_core::StrataEngine;

use super::models::*;

// ── Error response helper ──────────────────────────────────────────

/// Structured API error response with proper HTTP status codes.
fn api_error(status: StatusCode, code: &str, message: String) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
        }
    });
    (status, Json(body)).into_response()
}

fn api_ok(body: serde_json::Value) -> Response {
    (StatusCode::OK, Json(body)).into_response()
}

// ── Health (liveness) ───────────────────────────────────────────────

/// Health check endpoint — probes DuckDB, SQLite, and Raft (if cluster mode).
///
/// Returns `ok` if all subsystems are healthy, `degraded` if any are down.
pub async fn health(
    State(engine): State<Arc<StrataEngine>>,
    cluster: Option<Extension<super::routes::ClusterHandle>>,
) -> Json<HealthResponse> {
    let episodic_ok = engine.check_episodic().await;
    let state_ok = engine.check_state().await;

    let raft_status = if let Some(Extension(handle)) = cluster {
        let coord = handle.0.read().await;
        let has_leader = coord.leader_id().is_some();
        Some(SubsystemStatus {
            status: if has_leader { "ok" } else { "no_leader" }.into(),
        })
    } else {
        None
    };

    let all_ok = episodic_ok && state_ok && raft_status.as_ref().is_none_or(|r| r.status == "ok");

    let status = if all_ok { "ok" } else { "degraded" };

    Json(HealthResponse {
        status: status.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        subsystems: SubsystemHealth {
            episodic: SubsystemStatus {
                status: if episodic_ok { "ok" } else { "down" }.into(),
            },
            state: SubsystemStatus {
                status: if state_ok { "ok" } else { "down" }.into(),
            },
            raft: raft_status,
        },
    })
}

// ── Readiness probe ────────────────────────────────────────────────

/// Readiness probe — returns 200 only if all subsystems are operational.
///
/// Kubernetes should use this for readiness checks.
pub async fn ready(
    State(engine): State<Arc<StrataEngine>>,
    cluster: Option<Extension<super::routes::ClusterHandle>>,
) -> StatusCode {
    let episodic_ok = engine.check_episodic().await;
    let state_ok = engine.check_state().await;

    let raft_ok = if let Some(Extension(handle)) = cluster {
        let coord = handle.0.read().await;
        coord.leader_id().is_some()
    } else {
        true
    };

    if episodic_ok && state_ok && raft_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

// ── Stub handlers (no engine — for testing router shape) ────────────

pub async fn query_no_engine(Json(_req): Json<QueryRequest>) -> Response {
    api_ok(serde_json::json!({ "rows": [], "count": 0 }))
}

pub async fn ingest_no_engine(Json(_req): Json<IngestRequest>) -> Response {
    api_ok(serde_json::json!({ "ingested": 0 }))
}

pub async fn search_no_engine(Json(_req): Json<SearchRequest>) -> Response {
    api_ok(serde_json::json!({ "results": [] }))
}

// ── Engine-backed handlers ──────────────────────────────────────────

/// Execute a SQL query against the engine.
pub async fn query(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "query").increment(1);
    let start = std::time::Instant::now();

    let result = match engine.query_sql(&req.sql).await {
        Ok(rows) => {
            let count = rows.len();
            api_ok(serde_json::json!({ "rows": rows, "count": count }))
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("only SELECT") || msg.contains("SQL parse") {
                api_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_QUERY", msg)
            } else if msg.contains("timed out") {
                api_error(StatusCode::REQUEST_TIMEOUT, "QUERY_TIMEOUT", msg)
            } else {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "QUERY_ERROR", msg)
            }
        }
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "query")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Maximum number of events per ingest batch.
const MAX_INGEST_EVENTS: usize = 10_000;

/// Ingest events into the engine.
pub async fn ingest(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<IngestRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "ingest").increment(1);
    let start = std::time::Instant::now();

    if req.events.len() > MAX_INGEST_EVENTS {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "BATCH_TOO_LARGE",
            format!(
                "batch contains {} events, maximum is {}",
                req.events.len(),
                MAX_INGEST_EVENTS
            ),
        );
    }

    let events: Vec<strata_core::memory::episodic::Event> = req
        .events
        .into_iter()
        .map(|payload| {
            let idempotency_key = payload
                .get("idempotency_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            strata_core::memory::episodic::Event {
                id: uuid::Uuid::new_v4(),
                source: req.source.clone(),
                event_type: payload
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                payload,
                timestamp: chrono::Utc::now(),
                parent_id: None,
                trace_id: None,
                tags: vec![],
                idempotency_key,
            }
        })
        .collect();

    let result = match engine.ingest(events).await {
        Ok(count) => api_ok(serde_json::json!({ "ingested": count })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INGEST_ERROR",
            e.to_string(),
        ),
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "ingest")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Webhook ingestion — normalizes vendor payloads into Strata events.
pub async fn webhook(
    State(engine): State<Arc<StrataEngine>>,
    Path(source): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "webhook").increment(1);

    match strata_core::ingest::webhook::normalize_webhook(&source, &payload) {
        Ok(events) => {
            let count = events.len();
            match engine.ingest(events).await {
                Ok(ingested) => api_ok(serde_json::json!({
                    "source": source,
                    "normalized": count,
                    "ingested": ingested,
                })),
                Err(e) => api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INGEST_ERROR",
                    e.to_string(),
                ),
            }
        }
        Err(e) => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "WEBHOOK_NORMALIZE_ERROR",
            e.to_string(),
        ),
    }
}

/// Semantic search against the engine.
pub async fn search(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "search").increment(1);
    let start = std::time::Instant::now();

    let result = if let Some(vector) = req.vector {
        let search_result = if let Some(ref filters) = req.filters {
            engine
                .semantic_search_filtered(
                    &vector,
                    req.k,
                    filters.source.as_deref(),
                    filters.event_type.as_deref(),
                )
                .await
        } else {
            engine.semantic_search(&vector, req.k).await
        };
        match search_result {
            Ok(results) => {
                let items: Vec<serde_json::Value> = results
                    .iter()
                    .filter(|r| req.min_score.is_none_or(|ms| r.score >= ms))
                    .map(|r| {
                        serde_json::json!({
                            "id": r.entry.id.to_string(),
                            "content": r.entry.content,
                            "metadata": r.entry.metadata,
                            "score": r.score,
                        })
                    })
                    .collect();
                api_ok(serde_json::json!({ "results": items }))
            }
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SEARCH_ERROR",
                e.to_string(),
            ),
        }
    } else {
        api_ok(serde_json::json!({ "results": [] }))
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "search")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Get agent state.
pub async fn state_get(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_get").increment(1);

    match engine.state_get(&agent_id, &key).await {
        Ok(Some(entry)) => api_ok(serde_json::json!({
            "agent_id": entry.agent_id,
            "key": entry.key,
            "value": entry.value,
            "version": entry.version,
        })),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("state key '{key}' not found for agent '{agent_id}'"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "STATE_ERROR",
            e.to_string(),
        ),
    }
}

/// Set agent state.
pub async fn state_set(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_set").increment(1);

    match engine.state_set(&agent_id, &key, body).await {
        Ok(version) => api_ok(serde_json::json!({ "version": version })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "STATE_ERROR",
            e.to_string(),
        ),
    }
}

// ── Admin endpoints ─────────────────────────────────────────────────

/// Enforce data retention policy — delete events older than configured retention period.
pub async fn enforce_retention(State(engine): State<Arc<StrataEngine>>) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "retention").increment(1);

    match engine.enforce_retention().await {
        Ok(deleted) => api_ok(serde_json::json!({
            "deleted": deleted,
            "retention_days": engine.config().memory.episodic.default_retention_days,
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RETENTION_ERROR",
            e.to_string(),
        ),
    }
}

/// Trigger a backup of all stores to the configured data directory.
pub async fn backup(State(engine): State<Arc<StrataEngine>>) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "backup").increment(1);

    let backup_dir = std::path::PathBuf::from(&engine.config().storage.data_dir).join("backups");
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let target = backup_dir.join(&timestamp);

    match engine.backup(&target).await {
        Ok(()) => api_ok(serde_json::json!({
            "status": "ok",
            "path": target.to_string_lossy(),
            "timestamp": timestamp,
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "BACKUP_ERROR",
            e.to_string(),
        ),
    }
}

/// Query audit log entries.
///
/// GET /api/v1/admin/audit?since=2026-01-01
pub async fn audit_query(
    audit_log: Option<Extension<crate::auth::audit::AuditLog>>,
    axum::extract::Query(params): axum::extract::Query<super::models::AuditQueryParams>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "audit").increment(1);

    let Some(Extension(log)) = audit_log else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "AUDIT_DISABLED",
            "Audit logging is not enabled (auth must be enabled)".into(),
        );
    };

    match log.query_since(&params.since) {
        Ok(entries) => {
            let count = entries.len();
            api_ok(serde_json::json!({ "entries": entries, "count": count, "since": params.since }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "AUDIT_ERROR",
            e.to_string(),
        ),
    }
}

// ── WebSocket state watcher ─────────────────────────────────────────

/// WebSocket endpoint for real-time state change notifications.
///
/// GET /api/v1/state/{agent_id}/watch → upgrades to WebSocket.
/// Sends JSON messages for each state change matching the agent_id.
pub async fn state_watch(
    State(engine): State<Arc<StrataEngine>>,
    Path(agent_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_state_ws(socket, engine, agent_id))
}

// ── Embed & Search (DX killer feature) ──────────────────────────────

/// Embed text and search semantic memory in a single call.
///
/// POST /api/v1/embed-and-search { "text": "billing issue", "k": 5 }
pub async fn embed_and_search(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<EmbedAndSearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "embed_and_search").increment(1);
    let start = std::time::Instant::now();

    let source = req.filters.as_ref().and_then(|f| f.source.as_deref());
    let event_type = req.filters.as_ref().and_then(|f| f.event_type.as_deref());

    let min_score = req.min_score;
    let result = match engine
        .embed_and_search(&req.text, req.k, source, event_type)
        .await
    {
        Ok(results) => {
            let items: Vec<serde_json::Value> = results
                .iter()
                .filter(|r| min_score.is_none_or(|ms| r.score >= ms))
                .map(|r| {
                    serde_json::json!({
                        "id": r.entry.id.to_string(),
                        "content": r.entry.content,
                        "metadata": r.entry.metadata,
                        "score": r.score,
                    })
                })
                .collect();
            api_ok(serde_json::json!({ "results": items, "count": items.len() }))
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("no embedding provider") {
                api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "NO_EMBEDDING_PROVIDER",
                    "No embedding provider configured. Set STRATA_EMBEDDING__PROVIDER=ollama"
                        .into(),
                )
            } else {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "SEARCH_ERROR", msg)
            }
        }
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "embed_and_search")
        .record(start.elapsed().as_secs_f64());
    result
}

// ── Session Management ─────────────────────────────────────────────

/// Start a new conversation session.
///
/// POST /api/v1/sessions { "session_id": "...", "agent_id": "...", "parent_session_id": "..." }
pub async fn session_start(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "session_start").increment(1);

    let session_id = req
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let agent_id = req
        .get("agent_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if session_id.is_empty() || agent_id.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "MISSING_FIELD",
            "session_id and agent_id are required".into(),
        );
    }

    let parent = req.get("parent_session_id").and_then(|v| v.as_str());
    let metadata = req.get("metadata").cloned();

    match engine
        .session_start(session_id, agent_id, parent, metadata)
        .await
    {
        Ok(()) => api_ok(serde_json::json!({
            "session_id": session_id,
            "agent_id": agent_id,
            "status": "started"
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SESSION_ERROR",
            e.to_string(),
        ),
    }
}

/// End a session.
///
/// POST /api/v1/sessions/{session_id}/end { "summary": "..." }
pub async fn session_end(
    State(engine): State<Arc<StrataEngine>>,
    Path(session_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "session_end").increment(1);

    let summary = req.get("summary").and_then(|v| v.as_str());
    match engine.session_end(&session_id, summary).await {
        Ok(()) => api_ok(serde_json::json!({
            "session_id": session_id,
            "status": "ended"
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SESSION_ERROR",
            e.to_string(),
        ),
    }
}

/// Recall all events in a session.
///
/// GET /api/v1/sessions/{session_id}/recall
pub async fn session_recall(
    State(engine): State<Arc<StrataEngine>>,
    Path(session_id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "session_recall").increment(1);

    match engine.session_recall(&session_id).await {
        Ok(events) => {
            let count = events.len();
            api_ok(serde_json::json!({
                "session_id": session_id,
                "events": events,
                "count": count
            }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SESSION_ERROR",
            e.to_string(),
        ),
    }
}

// ── Schema Introspection ────────────────────────────────────────────

/// List all distinct event sources.
pub async fn schema_sources(State(engine): State<Arc<StrataEngine>>) -> Response {
    match engine.list_sources().await {
        Ok(sources) => api_ok(serde_json::json!({ "sources": sources })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SCHEMA_ERROR",
            e.to_string(),
        ),
    }
}

/// List all distinct agent IDs.
pub async fn schema_agents(State(engine): State<Arc<StrataEngine>>) -> Response {
    match engine.list_agents().await {
        Ok(agents) => api_ok(serde_json::json!({ "agents": agents })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SCHEMA_ERROR",
            e.to_string(),
        ),
    }
}

async fn handle_state_ws(mut socket: WebSocket, engine: Arc<StrataEngine>, agent_id: String) {
    let mut rx = engine.state_subscribe();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(change) => {
                        // Only send changes for the requested agent
                        if change.agent_id == agent_id {
                            let msg = serde_json::to_string(&change).unwrap_or_default();
                            if socket.send(Message::Text(msg.into())).await.is_err() {
                                break; // Client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, agent_id = %agent_id, "state watcher lagged");
                    }
                    Err(_) => break, // Channel closed
                }
            }
            // Check for client close
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // Ignore other messages
                }
            }
        }
    }
}
