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

/// Map a cluster (Raft) write error to an HTTP response. A leadership change is **retryable**
/// (503) — leader-forwarding will route the retry to the new leader; anything else is a 500.
fn cluster_write_error(e: strata_cluster::Error) -> Response {
    let msg = e.to_string();
    if msg.contains("ForwardToLeader") || msg.to_lowercase().contains("not leader") {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "NOT_LEADER",
            format!("not the current leader — retry; {msg}"),
        )
    } else {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "CLUSTER_WRITE_ERROR",
            msg,
        )
    }
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
///
/// If the request was authenticated with a tenant-scoped JWT, the query is
/// automatically filtered to that tenant's data.
pub async fn query(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "query").increment(1);
    let start = std::time::Instant::now();

    // Tenant-scoped users only ever see their own rows (row-level isolation).
    let tenant = auth
        .as_ref()
        .and_then(|Extension(ctx)| ctx.tenant_id.clone());
    let query_result = match tenant {
        Some(t) => engine.query_sql_for_tenant(&req.sql, &t).await,
        None => engine.query_sql(&req.sql).await,
    };

    let result = match query_result {
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
///
/// If the request was authenticated with a tenant-scoped JWT, events are
/// automatically tagged with the tenant ID for row-level isolation.
pub async fn ingest(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
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

    // Route to tenant-scoped ingest if tenant context is present
    let tenant_id = auth
        .as_ref()
        .and_then(|Extension(ctx)| ctx.tenant_id.clone());

    // Cluster mode: ALWAYS replicate through the Raft log — never apply directly on a node
    // that isn't the leader (that would write un-replicated state and diverge the cluster).
    // `client_write` requires leadership; a NotLeader result is surfaced as a retryable 503.
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = strata_cluster::raft::types::AppRequest::Ingest {
            events,
            tenant: tenant_id,
        };
        let result = match coord.client_write(ar).await {
            Ok(strata_cluster::raft::types::AppResponse::Ingested(n)) => {
                api_ok(serde_json::json!({ "ingested": n }))
            }
            Ok(_) => api_ok(serde_json::json!({ "ingested": 0 })),
            Err(e) => cluster_write_error(e),
        };
        metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "ingest")
            .record(start.elapsed().as_secs_f64());
        return result;
    }

    let ingest_result = if let Some(tid) = tenant_id {
        let tenant = strata_core::config::TenantContext::new(tid);
        engine.ingest_for_tenant(events, &tenant).await
    } else {
        engine.ingest(events).await
    };

    let result = match ingest_result {
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

/// Webhook ingestion — normalizes vendor payloads into Strata events (tenant-scoped).
pub async fn webhook(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(source): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "webhook").increment(1);

    match strata_core::ingest::webhook::normalize_webhook(&source, &payload) {
        Ok(events) => {
            let count = events.len();
            // Tag with the caller's tenant so webhook data is isolated like everything else.
            let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
            let ingest_result = match tenant {
                Some(t) => {
                    let tc = strata_core::config::TenantContext::new(t);
                    engine.ingest_for_tenant(events, &tc).await
                }
                None => engine.ingest(events).await,
            };
            match ingest_result {
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
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "search").increment(1);
    let start = std::time::Instant::now();

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let src = req.filters.as_ref().and_then(|f| f.source.as_deref());
    let et = req.filters.as_ref().and_then(|f| f.event_type.as_deref());

    let result = if let Some(vector) = req.vector {
        let search_result = match &tenant {
            // Tenant-scoped users only ever see their own event embeddings.
            Some(t) => {
                engine
                    .semantic_search_for_tenant(&vector, req.k, t, src, et)
                    .await
            }
            None if req.filters.is_some() => {
                engine
                    .semantic_search_filtered(&vector, req.k, src, et)
                    .await
            }
            None => engine.semantic_search(&vector, req.k).await,
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

/// Get agent state (tenant-scoped).
pub async fn state_get(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path((agent_id, key)): Path<(String, String)>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_get").increment(1);

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let got = match tenant {
        Some(t) => engine.state_get_for_tenant(&t, &agent_id, &key).await,
        None => engine.state_get(&agent_id, &key).await,
    };
    match got {
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

/// Set agent state (tenant-scoped).
pub async fn state_set(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    Path((agent_id, key)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_set").increment(1);

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    // Cluster mode: replicate through the Raft log (never apply directly off-leader).
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = strata_cluster::raft::types::AppRequest::StateSet {
            agent_id,
            key,
            value: body,
            tenant,
        };
        return match coord.client_write(ar).await {
            Ok(strata_cluster::raft::types::AppResponse::StateVersion(v)) => {
                api_ok(serde_json::json!({ "version": v }))
            }
            Ok(_) => api_ok(serde_json::json!({ "version": 0 })),
            Err(e) => cluster_write_error(e),
        };
    }

    let set = match tenant {
        Some(t) => engine.state_set_for_tenant(&t, &agent_id, &key, body).await,
        None => engine.state_set(&agent_id, &key, body).await,
    };
    match set {
        Ok(version) => api_ok(serde_json::json!({ "version": version })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "STATE_ERROR",
            e.to_string(),
        ),
    }
}

// ── Admin endpoints ─────────────────────────────────────────────────

/// Get or set per-source retention policies.
///
/// GET /api/v1/admin/retention/policies — list all policies
/// PUT /api/v1/admin/retention/policies — set a policy { "source": "...", "retention_days": N }
pub async fn retention_policies(
    State(engine): State<Arc<StrataEngine>>,
    method: axum::http::Method,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "retention_policies")
        .increment(1);

    if method == axum::http::Method::GET {
        match engine.retention_policies().await {
            Ok(policies) => {
                let items: Vec<serde_json::Value> = policies
                    .iter()
                    .map(|(source, days)| {
                        serde_json::json!({"source": source, "retention_days": days})
                    })
                    .collect();
                api_ok(serde_json::json!({
                    "policies": items,
                    "default_retention_days": engine.config().memory.episodic.default_retention_days,
                }))
            }
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RETENTION_ERROR",
                e.to_string(),
            ),
        }
    } else if let Some(Json(body)) = body {
        let source = body
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let days = body
            .get("retention_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        if source.is_empty() {
            return api_error(
                StatusCode::BAD_REQUEST,
                "MISSING_FIELD",
                "source is required".into(),
            );
        }

        match engine.set_retention_policy(source, days).await {
            Ok(()) => api_ok(serde_json::json!({
                "source": source,
                "retention_days": days,
                "status": "set"
            })),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RETENTION_ERROR",
                e.to_string(),
            ),
        }
    } else {
        api_error(
            StatusCode::BAD_REQUEST,
            "MISSING_BODY",
            "request body required for PUT".into(),
        )
    }
}

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
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(agent_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    // Namespace the watched agent id by tenant (mirrors core's `\u{1f}` separator) so a tenant
    // only receives its own state-change notifications.
    let effective = match auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone()) {
        Some(t) => format!("{t}\u{1f}{agent_id}"),
        None => agent_id,
    };
    ws.on_upgrade(move |socket| handle_state_ws(socket, engine, effective))
}

// ── Embed & Search (DX killer feature) ──────────────────────────────

/// Embed text and search semantic memory in a single call.
///
/// POST /api/v1/embed-and-search { "text": "billing issue", "k": 5 }
pub async fn embed_and_search(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<EmbedAndSearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "embed_and_search").increment(1);
    let start = std::time::Instant::now();

    let source = req.filters.as_ref().and_then(|f| f.source.as_deref());
    let event_type = req.filters.as_ref().and_then(|f| f.event_type.as_deref());
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    let min_score = req.min_score;
    let search = match &tenant {
        Some(t) => {
            engine
                .embed_and_search_for_tenant(&req.text, req.k, t, source, event_type)
                .await
        }
        None => {
            engine
                .embed_and_search(&req.text, req.k, source, event_type)
                .await
        }
    };
    let result = match search {
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
                    "No embedding provider configured. To enable semantic search, set STRATA_EMBEDDING__PROVIDER=ollama (local, requires Ollama) or STRATA_EMBEDDING__PROVIDER=openai (cloud, requires OPENAI_API_KEY)".into(),
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
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
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

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let started = match tenant {
        Some(t) => {
            engine
                .session_start_for_tenant(session_id, agent_id, parent, metadata, &t)
                .await
        }
        None => {
            engine
                .session_start(session_id, agent_id, parent, metadata)
                .await
        }
    };
    match started {
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
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(session_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "session_end").increment(1);

    let summary = req.get("summary").and_then(|v| v.as_str());
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    match tenant {
        Some(t) => match engine
            .session_end_for_tenant(&session_id, summary, &t)
            .await
        {
            Ok(true) => api_ok(serde_json::json!({"session_id": session_id, "status": "ended"})),
            Ok(false) => api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("session '{session_id}' not found"),
            ),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SESSION_ERROR",
                e.to_string(),
            ),
        },
        None => match engine.session_end(&session_id, summary).await {
            Ok(()) => api_ok(serde_json::json!({"session_id": session_id, "status": "ended"})),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SESSION_ERROR",
                e.to_string(),
            ),
        },
    }
}

/// Recall all events in a session.
///
/// GET /api/v1/sessions/{session_id}/recall
pub async fn session_recall(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(session_id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "session_recall").increment(1);

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let recalled = match tenant {
        Some(t) => engine.session_recall_for_tenant(&session_id, &t).await,
        None => engine.session_recall(&session_id).await,
    };
    match recalled {
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

// ── Memory Cognition ────────────────────────────────────────────────

/// Resolve a memory scope, preferring the authenticated tenant for isolation.
fn scope_from(
    auth: &Option<Extension<crate::auth::middleware::AuthContext>>,
    tenant_id: Option<&str>,
    user_id: Option<&str>,
    agent_id: Option<&str>,
    session_id: Option<&str>,
) -> strata_core::memory::cognition::MemoryScope {
    let tenant = auth
        .as_ref()
        .and_then(|Extension(ctx)| ctx.tenant_id.clone())
        .or_else(|| tenant_id.map(|s| s.to_string()))
        .unwrap_or_else(|| "default".to_string());
    strata_core::memory::cognition::MemoryScope {
        tenant_id: tenant,
        user_id: user_id.map(|s| s.to_string()),
        agent_id: agent_id.map(|s| s.to_string()),
        session_id: session_id.map(|s| s.to_string()),
    }
}

/// Add a memory through the cognition pipeline (dedup / contradiction / importance).
///
/// POST /api/v1/memories { "content": "...", "subject": "...", "user_id": "..." }
pub async fn memory_add(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryAddRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_add").increment(1);

    if req.content.trim().is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "MISSING_FIELD",
            "content is required".into(),
        );
    }

    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let input = strata_core::memory::cognition::MemoryInput {
        scope,
        subject: req.subject,
        content: req.content,
        importance: req.importance,
        source_event_ids: vec![],
        metadata: req.metadata.unwrap_or_else(|| serde_json::json!({})),
    };

    // Cluster mode: run cognition on the leader to materialize the change-set, then replicate it
    // through the Raft log (never apply directly off-leader). Followers replay identical rows.
    if let Some(Extension(coord)) = cluster {
        let (result, rows) = match engine.memory_plan(input).await {
            Ok(pair) => pair,
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let coord = coord.read().await;
        let ar = strata_cluster::raft::types::AppRequest::MemoryUpsert { rows };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::to_value(result).unwrap_or_default()),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_add(input).await {
        Ok(added) => api_ok(serde_json::to_value(added).unwrap_or_default()),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Search memories within a scope (semantic when embeddings exist, else recency).
///
/// POST /api/v1/memories/search { "query": "...", "user_id": "...", "k": 5 }
pub async fn memory_search(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<MemorySearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_search").increment(1);

    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    match engine.memory_search(&req.query, &scope, req.k).await {
        Ok(hits) => api_ok(serde_json::json!({ "results": hits, "count": hits.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// List active memories in a scope.
///
/// GET /api/v1/memories?user_id=alice&limit=50
pub async fn memory_list(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryListParams>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_list").increment(1);

    let scope = scope_from(
        &auth,
        params.tenant_id.as_deref(),
        params.user_id.as_deref(),
        params.agent_id.as_deref(),
        params.session_id.as_deref(),
    );
    match engine.memory_all(&scope, params.limit).await {
        Ok(mems) => api_ok(serde_json::json!({ "memories": mems, "count": mems.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get a single memory by id (scoped to the caller's tenant).
pub async fn memory_get(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_get").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let got = match tenant {
        Some(t) => engine.memory_get_scoped(uuid, &t).await,
        None => engine.memory_get(uuid).await,
    };
    match got {
        Ok(Some(m)) => api_ok(serde_json::to_value(m).unwrap_or_default()),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Delete a memory by id (scoped to the caller's tenant).
pub async fn memory_delete(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_delete").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let outcome = match tenant {
        Some(t) => engine.memory_delete_scoped(uuid, &t).await,
        None => engine.memory_delete(uuid).await.map(|()| true),
    };
    match outcome {
        Ok(true) => api_ok(serde_json::json!({ "id": id, "deleted": true })),
        Ok(false) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get the full temporal history of a memory (every superseded version).
///
/// GET /api/v1/memories/{id}/history
pub async fn memory_history(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_history").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let fetched = match tenant {
        Some(t) => engine.memory_get_scoped(uuid, &t).await,
        None => engine.memory_get(uuid).await,
    };
    let mem = match fetched {
        Ok(Some(m)) => m,
        Ok(None) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("memory '{id}' not found"),
            )
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            )
        }
    };

    match mem.subject.clone() {
        Some(subject) => match engine.memory_history(&mem.scope, &subject).await {
            Ok(history) => api_ok(serde_json::json!({
                "subject": subject,
                "history": history,
                "count": history.len(),
            })),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            ),
        },
        // No subject → no supersession chain; the memory is its own history.
        None => api_ok(serde_json::json!({ "history": [mem], "count": 1 })),
    }
}

/// Forget low-value memories via time-decay of importance (admin).
///
/// POST /api/v1/admin/memory/decay
pub async fn memory_decay(State(engine): State<Arc<StrataEngine>>) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_decay").increment(1);
    match engine.memory_enforce_decay().await {
        Ok(forgotten) => api_ok(serde_json::json!({ "forgotten": forgotten })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

// ── Schema Introspection ────────────────────────────────────────────

/// List distinct event sources (tenant-scoped).
pub async fn schema_sources(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
) -> Response {
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let listed = match tenant {
        Some(t) => engine.list_sources_for_tenant(&t).await,
        None => engine.list_sources().await,
    };
    match listed {
        Ok(sources) => api_ok(serde_json::json!({ "sources": sources })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SCHEMA_ERROR",
            e.to_string(),
        ),
    }
}

/// List distinct agent IDs (tenant-scoped).
pub async fn schema_agents(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
) -> Response {
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let listed = match tenant {
        Some(t) => engine.list_agents_for_tenant(&t).await,
        None => engine.list_agents().await,
    };
    match listed {
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
