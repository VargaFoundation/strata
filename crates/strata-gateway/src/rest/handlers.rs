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

/// Configured HMAC secret for verifying incoming webhook signatures (layered as an Extension).
#[derive(Clone)]
pub struct WebhookSecret(pub Option<String>);

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

/// Verify a GitHub-style `sha256=<hex>` HMAC-SHA256 signature over the raw body (constant-time).
fn verify_webhook_signature(secret: &str, signature_header: Option<&str>, body: &[u8]) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let Some(sig) = signature_header else {
        return false;
    };
    let hex = sig.strip_prefix("sha256=").unwrap_or(sig);
    let Ok(expected) = hex_decode(hex) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
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
///
/// When `webhook_secret` is configured, the raw body must carry a valid GitHub-style
/// `X-Hub-Signature-256: sha256=<hmac>` (HMAC-SHA256 over the body), else the request is rejected.
pub async fn webhook(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    webhook_secret: Option<Extension<WebhookSecret>>,
    Path(source): Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "webhook").increment(1);

    // Verify the HMAC signature over the RAW body if a secret is configured.
    if let Some(Extension(WebhookSecret(Some(secret)))) = &webhook_secret {
        let sig = headers
            .get("x-hub-signature-256")
            .or_else(|| headers.get("x-signature-256"))
            .and_then(|v| v.to_str().ok());
        if !verify_webhook_signature(secret, sig, &body) {
            return api_error(
                StatusCode::UNAUTHORIZED,
                "INVALID_SIGNATURE",
                "webhook signature verification failed".into(),
            );
        }
    }

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, "INVALID_JSON", e.to_string()),
    };

    match strata_core::ingest::webhook::normalize_webhook(&source, &payload) {
        Ok(events) => {
            let count = events.len();
            // Tag with the caller's tenant so webhook data is isolated like everything else.
            let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
            // Capture (source, event_type, payload) so we can evaluate event triggers after ingest.
            let trigger_inputs: Vec<(String, String, serde_json::Value)> = events
                .iter()
                .map(|e| (e.source.clone(), e.event_type.clone(), e.payload.clone()))
                .collect();
            let ingest_result = match &tenant {
                Some(t) => {
                    let tc = strata_core::config::TenantContext::new(t.clone());
                    engine.ingest_for_tenant(events, &tc).await
                }
                None => engine.ingest(events).await,
            };
            match ingest_result {
                Ok(ingested) => {
                    // Event-driven agents: fire any matching triggers → start agent runs.
                    let tenant_str = tenant.as_deref().unwrap_or("default");
                    let mut triggered_runs = Vec::new();
                    for (src, evt, payload) in trigger_inputs {
                        if let Ok(ids) = engine.fire_triggers(tenant_str, &src, &evt, payload).await
                        {
                            triggered_runs.extend(ids.into_iter().map(|i| i.to_string()));
                        }
                    }
                    api_ok(serde_json::json!({
                        "source": source,
                        "normalized": count,
                        "ingested": ingested,
                        "triggered_runs": triggered_runs,
                    }))
                }
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
/// GDPR erasure — delete ALL data for a tenant across every store. Admin only (under /admin/).
///
/// DELETE /api/v1/admin/tenants/{tenant_id}
pub async fn delete_tenant(
    State(engine): State<Arc<StrataEngine>>,
    Path(tenant_id): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "delete_tenant").increment(1);
    match engine.delete_tenant(&tenant_id).await {
        Ok(summary) => api_ok(summary),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "DELETE_TENANT_ERROR",
            e.to_string(),
        ),
    }
}

/// Export a tenant's full data (events + memories + state) as a JSON snapshot. Admin.
///
/// GET /api/v1/admin/tenants/{tenant}/export
pub async fn export_tenant(
    State(engine): State<Arc<StrataEngine>>,
    Path(tenant): Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "export_tenant").increment(1);
    match engine.export_tenant(&tenant).await {
        Ok(snapshot) => api_ok(snapshot),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "EXPORT_ERROR",
            e.to_string(),
        ),
    }
}

/// Import a tenant snapshot (from `export`). Admin.
///
/// POST /api/v1/admin/tenants/{tenant}/import
pub async fn import_tenant(
    State(engine): State<Arc<StrataEngine>>,
    Path(tenant): Path<String>,
    Json(snapshot): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "import_tenant").increment(1);
    match engine.import_tenant(&tenant, &snapshot).await {
        Ok(counts) => api_ok(counts),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "IMPORT_ERROR",
            e.to_string(),
        ),
    }
}

/// Rebalance: move a tenant's full data from THIS shard to `target_shard`. Admin. Runs on the source
/// shard's pod: export locally → POST to the target shard's import endpoint → erase locally.
///
/// POST /api/v1/admin/rebalance  { "tenant": "...", "target_shard": N }
pub async fn rebalance(
    State(engine): State<Arc<StrataEngine>>,
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<super::models::RebalanceRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "rebalance").increment(1);
    let Some(Extension(s)) = shard else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "NOT_SHARDED",
            "rebalance requires sharded mode (cluster.shards > 1)".into(),
        );
    };
    let Some(base) = s.base_urls.get(req.target_shard) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "BAD_SHARD",
            format!("no base URL for shard {}", req.target_shard),
        );
    };
    // Export locally.
    let snapshot = match engine.export_tenant(&req.tenant).await {
        Ok(s) => s,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "EXPORT_ERROR",
                e.to_string(),
            )
        }
    };
    // Push to the target shard's import endpoint.
    let url = format!(
        "{}/api/v1/admin/tenants/{}/import",
        base.trim_end_matches('/'),
        urlencoding(&req.tenant)
    );
    let mut rb = s
        .http
        .post(&url)
        .header("x-strata-shard-forwarded", "1")
        .json(&snapshot);
    if let Some(auth) = headers.get("authorization") {
        rb = rb.header("authorization", auth.clone());
    }
    match rb.send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            return api_error(
                StatusCode::BAD_GATEWAY,
                "IMPORT_FAILED",
                format!("target shard returned {}", resp.status()),
            )
        }
        Err(e) => return api_error(StatusCode::BAD_GATEWAY, "IMPORT_UNREACHABLE", e.to_string()),
    }
    // Import succeeded → erase the tenant locally (the move is complete).
    match engine.delete_tenant(&req.tenant).await {
        Ok(_) => api_ok(
            serde_json::json!({ "status": "ok", "tenant": req.tenant, "target_shard": req.target_shard }),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "DELETE_ERROR",
            e.to_string(),
        ),
    }
}

/// Minimal percent-encoding for a path segment (UTF-8-byte correct; tenant ids are usually plain).
fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Re-embed events left unembedded (e.g. provider was down at ingest). Admin; closes cross-store gap.
pub async fn reindex(State(engine): State<Arc<StrataEngine>>) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "reindex").increment(1);
    match engine.reindex_unembedded(10_000).await {
        Ok(reindexed) => {
            let pending = engine.unembedded_count().await.unwrap_or(0);
            api_ok(serde_json::json!({ "reindexed": reindexed, "pending": pending }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "REINDEX_ERROR",
            e.to_string(),
        ),
    }
}

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
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
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

    // Local entries first.
    let mut entries = match log.query_since(&params.since) {
        Ok(e) => serde_json::to_value(e).unwrap_or_else(|_| serde_json::json!([])),
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "AUDIT_ERROR",
                e.to_string(),
            )
        }
    };
    let mut entries_vec: Vec<serde_json::Value> = entries.as_array().cloned().unwrap_or_default();

    // Cross-shard fan-out: audit is per-pod, so aggregate every shard's log into one cluster-wide
    // view. Skip self (already local) and skip when this call is itself a forwarded sub-request
    // (the `x-strata-shard-forwarded` marker), which would otherwise recurse.
    if let Some(Extension(s)) = shard {
        if s.router.shards() > 1 && !headers.contains_key("x-strata-shard-forwarded") {
            for (i, base) in s.base_urls.iter().enumerate() {
                if i == s.my_shard {
                    continue;
                }
                let url = format!("{}/api/v1/admin/audit", base.trim_end_matches('/'));
                let mut rb = s
                    .http
                    .get(url)
                    .query(&[("since", params.since.as_str())])
                    .header("x-strata-shard-forwarded", "1");
                if let Some(auth) = headers.get("authorization") {
                    rb = rb.header("authorization", auth.clone());
                }
                if let Ok(resp) = rb.send().await {
                    if let Ok(v) = resp.json::<serde_json::Value>().await {
                        if let Some(arr) = v.get("entries").and_then(|e| e.as_array()) {
                            entries_vec.extend(arr.iter().cloned());
                        }
                    }
                }
            }
            entries = serde_json::Value::Array(entries_vec.clone());
        }
    }

    let count = entries_vec.len();
    api_ok(serde_json::json!({ "entries": entries, "count": count, "since": params.since }))
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
        mem_type: req.mem_type,
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
pub async fn memory_consolidate(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryConsolidateRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_consolidate")
        .increment(1);
    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let keep = req.keep.unwrap_or(20);

    // Cluster mode: plan on the leader (summary + originals to expire), then replicate both through
    // the Raft log (summary as MemoryUpsert, originals as MemoryExpire) so followers converge.
    if let Some(Extension(coord)) = cluster {
        let plan = match engine.memory_consolidate_plan(&scope, keep).await {
            Ok(Some(p)) => p,
            Ok(None) => return api_ok(serde_json::json!({ "consolidated": null })),
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let (input, expired) = plan;
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
        if let Err(e) = coord
            .client_write(strata_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            return cluster_write_error(e);
        }
        return match coord
            .client_write(strata_cluster::raft::types::AppRequest::MemoryExpire { ids: expired })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "consolidated": result })),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_consolidate(&scope, keep).await {
        Ok(Some(m)) => api_ok(serde_json::json!({ "consolidated": m })),
        Ok(None) => api_ok(serde_json::json!({ "consolidated": null })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Upsert a pre-computed multi-modal embedding (text/image/audio/…).
pub async fn semantic_upsert(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<SemanticUpsertRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "semantic_upsert").increment(1);
    let id = req
        .id
        .and_then(|s| uuid::Uuid::parse_str(&s).ok())
        .unwrap_or_else(uuid::Uuid::new_v4);
    match engine
        .semantic_upsert_modal(
            id,
            &req.modality,
            req.content,
            req.embedding,
            req.metadata.unwrap_or_else(|| serde_json::json!({})),
        )
        .await
    {
        Ok(()) => api_ok(serde_json::json!({ "id": id.to_string() })),
        Err(e) => api_error(StatusCode::BAD_REQUEST, "SEMANTIC_ERROR", e.to_string()),
    }
}

/// Vector search optionally restricted to one modality.
pub async fn semantic_modal_search(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<ModalSearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "semantic_modal_search")
        .increment(1);
    match engine
        .semantic_search_modal(&req.vector, req.k.unwrap_or(5), req.modality.as_deref())
        .await
    {
        Ok(results) => api_ok(serde_json::json!({ "results": results })),
        Err(e) => api_error(StatusCode::BAD_REQUEST, "SEMANTIC_ERROR", e.to_string()),
    }
}

/// Add a graph edge between two entities (tenant-scoped).
pub async fn memory_link(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryLinkRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_link").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());

    // Cluster mode: generate the edge (id) on the leader and replicate it through the Raft log so
    // followers apply the identical row (was previously snapshot-only).
    if let Some(Extension(coord)) = cluster {
        let at = chrono::Utc::now();
        let id = uuid::Uuid::new_v4();
        let coord = coord.read().await;
        // Functional relation: close the prior active (src, relation) edge first, replicated with
        // the leader-supplied `at`/`by` so every node applies the identical close.
        if req.supersede {
            let sup = strata_cluster::raft::types::AppRequest::GraphSupersede {
                tenant: Some(tenant.clone()),
                src: req.src.clone(),
                relation: req.relation.clone(),
                at,
                by: Some(id),
            };
            if let Err(e) = coord.client_write(sup).await {
                return cluster_write_error(e);
            }
        }
        let edge = strata_core::memory::cognition::Edge {
            id,
            src: req.src,
            relation: req.relation,
            dst: req.dst,
            weight: 1.0,
            source_memory_id: None,
            valid_from: Some(at),
            ..Default::default()
        };
        let ar = strata_cluster::raft::types::AppRequest::GraphAddEdge {
            tenant: Some(tenant),
            edge,
        };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "status": "ok" })),
            Err(e) => cluster_write_error(e),
        };
    }

    let result = if req.supersede {
        engine
            .memory_link_functional(&tenant, &req.src, &req.relation, &req.dst, None)
            .await
    } else {
        engine
            .memory_link(&tenant, &req.src, &req.relation, &req.dst, None)
            .await
    };
    match result {
        Ok(()) => api_ok(serde_json::json!({ "status": "ok" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get an entity's 1-hop neighborhood in the memory graph (tenant-scoped).
pub async fn memory_graph(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryGraphQuery>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "memory_graph").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let limit = params.limit.unwrap_or(50);
    let result = match params.depth {
        Some(d) if d > 1 => {
            engine
                .memory_subgraph(&tenant, &params.entity, d, limit)
                .await
        }
        _ => {
            engine
                .memory_neighbors(&tenant, &params.entity, limit)
                .await
        }
    };
    match result {
        Ok(edges) => api_ok(serde_json::json!({ "entity": params.entity, "edges": edges })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Create an agent/workflow run (tenant-scoped). Cluster-replicated through Raft.
pub async fn run_create(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<CreateRunRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_create").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    // Materialize the run (id + timestamps) once; cluster mode replicates the identical row.
    let now = chrono::Utc::now();
    let run = strata_core::runtime::Run {
        id: uuid::Uuid::new_v4(),
        tenant_id: tenant,
        agent_id: req.agent_id,
        parent_run_id: req.parent_run_id,
        status: strata_core::runtime::RunStatus::Pending,
        input: req.input,
        result: serde_json::Value::Null,
        error: None,
        cursor: serde_json::Value::Null,
        created_at: now,
        updated_at: now,
        started_at: None,
        ended_at: None,
    };
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = strata_cluster::raft::types::AppRequest::RunCreate { run: run.clone() };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "run": run })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.run_apply_create(&run).await {
        Ok(()) => api_ok(serde_json::json!({ "run": run })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// List a tenant's runs (newest first), optionally `?status=` filtered.
pub async fn run_list(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<ListRunsQuery>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_list").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let status = match params.status.as_deref() {
        Some("pending") => Some(strata_core::runtime::RunStatus::Pending),
        Some("running") => Some(strata_core::runtime::RunStatus::Running),
        Some("succeeded") => Some(strata_core::runtime::RunStatus::Succeeded),
        Some("failed") => Some(strata_core::runtime::RunStatus::Failed),
        Some("cancelled") => Some(strata_core::runtime::RunStatus::Cancelled),
        Some("waiting_approval") => Some(strata_core::runtime::RunStatus::WaitingApproval),
        _ => None,
    };
    match engine
        .run_list(&tenant, status, params.limit.unwrap_or(50))
        .await
    {
        Ok(runs) => api_ok(serde_json::json!({ "runs": runs })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Get a run by id (tenant-scoped).
pub async fn run_get(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_get").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    match engine.run_get(id).await {
        Ok(Some(run)) if run.tenant_id == tenant => api_ok(serde_json::json!({ "run": run })),
        Ok(_) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "run not found".to_string(),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Full step trace of a run (episodic events tagged with the run id), tenant-scoped.
pub async fn run_trace(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_trace").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    match engine.run_get(id).await {
        Ok(Some(run)) if run.tenant_id == tenant => match engine.run_trace(id).await {
            Ok(steps) => api_ok(serde_json::json!({ "run_id": id, "steps": steps })),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RUN_ERROR",
                e.to_string(),
            ),
        },
        Ok(_) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "run not found".to_string(),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Cancel a run (tenant-scoped). Cluster-replicated through Raft.
pub async fn run_cancel(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<strata_cluster::ClusterCoordinator>>>,
    >,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_cancel").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    // Ownership check — never cancel another tenant's run.
    match engine.run_get(id).await {
        Ok(Some(run)) if run.tenant_id == tenant => {}
        Ok(_) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "run not found".to_string(),
            )
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RUN_ERROR",
                e.to_string(),
            )
        }
    }
    let now = chrono::Utc::now();
    let patch = strata_core::runtime::RunPatch {
        status: Some(strata_core::runtime::RunStatus::Cancelled),
        ended_at: Some(now),
        ..Default::default()
    };
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = strata_cluster::raft::types::AppRequest::RunUpdate {
            id,
            patch,
            updated_at: now,
        };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "status": "cancelled" })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.run_apply_update(id, &patch, now).await {
        Ok(_) => api_ok(serde_json::json!({ "status": "cancelled" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Run an agent end-to-end (durable LLM↔tool loop) and return the resulting run. Requires a
/// completion provider; the run + its step trace are persisted (see `/runs/{id}/trace`).
pub async fn run_agent_endpoint(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<RunAgentRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_agent").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .run_agent(
            &tenant,
            &req.agent_id,
            &req.question,
            req.max_turns.unwrap_or(8),
        )
        .await
    {
        Ok(run) => api_ok(serde_json::json!({ "run": run })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "AGENT_ERROR",
            e.to_string(),
        ),
    }
}

/// Register a downstream MCP tool server (governed by the existing auth layer).
pub async fn register_tool(
    gateway: Option<Extension<std::sync::Arc<crate::rest::tool_gateway::ToolGateway>>>,
    Json(req): Json<RegisterToolServer>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "register_tool").increment(1);
    match gateway {
        Some(Extension(gw)) => {
            gw.register(req.name.clone(), req.url);
            api_ok(serde_json::json!({ "status": "registered", "name": req.name }))
        }
        None => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "NO_TOOL_GATEWAY",
            "tool gateway not enabled".to_string(),
        ),
    }
}

/// List the registered downstream MCP tool servers.
pub async fn list_tools(
    gateway: Option<Extension<std::sync::Arc<crate::rest::tool_gateway::ToolGateway>>>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "list_tools").increment(1);
    match gateway {
        Some(Extension(gw)) => api_ok(serde_json::json!({ "servers": gw.list() })),
        None => api_ok(serde_json::json!({ "servers": [] })),
    }
}

/// Invoke a tool on a registered downstream MCP server (the leader-side tool side effect).
pub async fn call_tool(
    gateway: Option<Extension<std::sync::Arc<crate::rest::tool_gateway::ToolGateway>>>,
    axum::extract::Path(server): axum::extract::Path<String>,
    Json(req): Json<CallToolRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "call_tool").increment(1);
    let Some(Extension(gw)) = gateway else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "NO_TOOL_GATEWAY",
            "tool gateway not enabled".to_string(),
        );
    };
    match gw.call(&server, &req.tool, req.arguments).await {
        Ok(result) => api_ok(serde_json::json!({ "result": result })),
        Err(e) => api_error(StatusCode::BAD_GATEWAY, "TOOL_CALL_FAILED", e),
    }
}

/// Pause a run awaiting human approval (HITL).
pub async fn run_request_approval(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<RequestApprovalRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_request_approval")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    match engine.run_request_approval(id, &tenant, &req.prompt).await {
        Ok(()) => api_ok(serde_json::json!({ "status": "waiting_approval" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Approve or reject a run awaiting approval (HITL).
pub async fn run_approve(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<ApproveRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_approve").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    match engine.run_resolve_approval(id, &tenant, req.approve).await {
        Ok(()) => api_ok(serde_json::json!({
            "status": if req.approve { "approved" } else { "rejected" }
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Register an event trigger (source/event_type `*` = any) → starts a run of `agent_id`.
pub async fn trigger_register(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<RegisterTriggerRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "trigger_register").increment(1);
    match engine
        .trigger_register(&req.name, &req.source, &req.event_type, &req.agent_id)
        .await
    {
        Ok(()) => api_ok(serde_json::json!({ "status": "registered", "name": req.name })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// List the registered event triggers.
pub async fn trigger_list(State(engine): State<Arc<StrataEngine>>) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "trigger_list").increment(1);
    match engine.trigger_list().await {
        Ok(triggers) => api_ok(serde_json::json!({ "triggers": triggers })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

/// Resume a run paused for human approval (after it has been approved).
pub async fn run_resume(
    State(engine): State<Arc<StrataEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "run_resume").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(i) => i,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "run id must be a UUID".to_string(),
            )
        }
    };
    match engine.run_resume(id, &tenant).await {
        Ok(run) => api_ok(serde_json::json!({ "run": run })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn webhook_signature_verifies_and_rejects() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let secret = "supersecret-webhook-key";
        let body = br#"{"hello":"world"}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex_encode(&mac.finalize().into_bytes()));

        assert!(
            verify_webhook_signature(secret, Some(&sig), body),
            "valid sig accepted"
        );
        assert!(
            !verify_webhook_signature("wrong", Some(&sig), body),
            "wrong secret rejected"
        );
        assert!(
            !verify_webhook_signature(secret, Some(&sig), b"tampered"),
            "tampered body rejected"
        );
        assert!(
            !verify_webhook_signature(secret, None, body),
            "missing signature rejected"
        );
        assert!(
            !verify_webhook_signature(secret, Some("garbage"), body),
            "malformed sig rejected"
        );
    }
}
