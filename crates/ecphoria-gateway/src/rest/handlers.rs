//! REST API handler functions with proper HTTP status codes and request IDs.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use ecphoria_core::EcphoriaEngine;

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

/// Header marking a forwarded admin sub-request in the cluster-wide scatter-gather (prevents fan-out
/// recursion). Matches the marker the audit fan-out uses.
const SHARD_FWD_HEADER: &str = "x-ecphoria-shard-forwarded";

/// Run an admin write **cluster-wide**. Each shard owns a disjoint slice of tenants, and the
/// shard-routing middleware serves `/admin/*` locally — so a bare `backup`/`reindex`/`retention` only
/// touches the receiving shard. `local` already holds THIS shard's result; in sharded mode (and
/// unless this call is itself a forwarded sub-request) this re-invokes the same admin endpoint on
/// every OTHER shard — marked so they run locally and don't recurse — and returns a per-shard
/// breakdown. Single-shard or forwarded → returns `local` unchanged (backward compatible). A shard
/// that fails yields HTTP 207 (Multi-Status) with `partial: true`, never a silent 200 that would
/// hide an un-backed-up / un-pruned shard.
async fn scatter_admin(
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: &axum::http::HeaderMap,
    endpoint: &str,
    local: serde_json::Value,
) -> Response {
    let Some(Extension(s)) = shard else {
        return api_ok(local);
    };
    // Not sharded, or this is already a forwarded sub-request → single-shard result (back-compat).
    if s.router.shards() <= 1 || headers.contains_key(SHARD_FWD_HEADER) {
        return api_ok(local);
    }

    let mut shards = vec![serde_json::json!({
        "shard": s.my_shard, "status": "ok", "result": local
    })];
    let mut partial = false;
    for (i, base) in s.base_urls.iter().enumerate() {
        if i == s.my_shard {
            continue;
        }
        let url = format!("{}{}", base.trim_end_matches('/'), endpoint);
        let mut rb = s.http.post(&url).header(SHARD_FWD_HEADER, "1");
        if let Some(auth) = headers.get("authorization") {
            rb = rb.header("authorization", auth.clone());
        }
        let entry = match rb.send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<serde_json::Value>().await {
                    Ok(v) => serde_json::json!({ "shard": i, "status": "ok", "result": v }),
                    Err(e) => {
                        partial = true;
                        serde_json::json!({ "shard": i, "status": "error", "error": e.to_string() })
                    }
                }
            }
            Ok(resp) => {
                partial = true;
                serde_json::json!({
                    "shard": i, "status": "error", "error": format!("HTTP {}", resp.status().as_u16())
                })
            }
            Err(e) => {
                partial = true;
                serde_json::json!({ "shard": i, "status": "error", "error": e.to_string() })
            }
        };
        shards.push(entry);
    }

    let body = serde_json::json!({ "cluster": true, "partial": partial, "shards": shards });
    if partial {
        (StatusCode::MULTI_STATUS, Json(body)).into_response()
    } else {
        api_ok(body)
    }
}

/// Configured webhook signature verification (layered as an Extension).
///
/// Holds a per-source secret map plus an optional global default, and a fail-closed toggle.
/// The signature *scheme* is chosen per vendor (GitHub, Slack, Sentry, PagerDuty) — see
/// [`verify_vendor_signature`].
#[derive(Clone, Default)]
pub struct WebhookVerifier {
    /// `source` → secret (e.g. `"github"`, `"slack"`).
    per_source: std::collections::HashMap<String, String>,
    /// Fallback secret applied to any source without a specific entry.
    default: Option<String>,
    /// When true, a webhook to a source with NO configured secret is rejected (fail-closed).
    /// When false (default), such a source is accepted unverified (legacy/dev behavior).
    require: bool,
}

impl WebhookVerifier {
    /// Build from config: a global default secret, `source=secret` per-source entries, and the
    /// fail-closed toggle.
    pub fn from_config(
        default: Option<String>,
        per_source_entries: &[String],
        require: bool,
    ) -> Self {
        let mut per_source = std::collections::HashMap::new();
        for entry in per_source_entries {
            // `source=secret` — the source (a vendor slug) never contains '='.
            if let Some((src, secret)) = entry.split_once('=') {
                let src = src.trim();
                if !src.is_empty() && !secret.is_empty() {
                    per_source.insert(src.to_string(), secret.to_string());
                }
            }
        }
        Self {
            per_source,
            default,
            require,
        }
    }

    /// The secret to use for `source` (specific entry wins over the global default).
    fn secret_for(&self, source: &str) -> Option<&str> {
        self.per_source
            .get(source)
            .map(|s| s.as_str())
            .or(self.default.as_deref())
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

/// Compute HMAC-SHA256(secret, msg), or `None` if the key is unusable. Used by tests to construct
/// signatures; the verify path uses [`verify_hex_hmac`] (constant-time) directly.
#[cfg(test)]
fn hmac_sha256(secret: &str, msg: &[u8]) -> Option<Vec<u8>> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(msg);
    Some(mac.finalize().into_bytes().to_vec())
}

/// Constant-time comparison of an expected HMAC against a hex-encoded signature.
fn verify_hex_hmac(secret: &str, msg: &[u8], sig_hex: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let Ok(expected) = hex_decode(sig_hex.trim()) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(msg);
    mac.verify_slice(&expected).is_ok()
}

/// Verify a GitHub-style `sha256=<hex>` HMAC-SHA256 signature over the raw body (constant-time).
fn verify_webhook_signature(secret: &str, signature_header: Option<&str>, body: &[u8]) -> bool {
    let Some(sig) = signature_header else {
        return false;
    };
    let hex = sig.strip_prefix("sha256=").unwrap_or(sig);
    verify_hex_hmac(secret, body, hex)
}

/// Verify a Slack request signature: `v0=HMAC-SHA256(secret, "v0:{ts}:{body}")`, with the
/// timestamp taken from `X-Slack-Request-Timestamp`. Also rejects stale timestamps (±5 min) to
/// blunt replay. `now` is injected for testability.
fn verify_slack_signature(
    secret: &str,
    headers: &axum::http::HeaderMap,
    body: &[u8],
    now: i64,
) -> bool {
    let Some(ts) = headers
        .get("x-slack-request-timestamp")
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let Ok(ts_num) = ts.parse::<i64>() else {
        return false;
    };
    if (now - ts_num).abs() > 300 {
        return false; // stale → likely a replay
    }
    let Some(sig) = headers
        .get("x-slack-signature")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("v0="))
    else {
        return false;
    };
    let mut basestring = format!("v0:{ts}:").into_bytes();
    basestring.extend_from_slice(body);
    verify_hex_hmac(secret, &basestring, sig)
}

/// Verify a signature for `source` using that vendor's scheme. Unknown sources fall back to the
/// GitHub-style scheme (`X-Hub-Signature-256` / `X-Signature-256`).
fn verify_vendor_signature(
    source: &str,
    secret: &str,
    headers: &axum::http::HeaderMap,
    body: &[u8],
    now: i64,
) -> bool {
    let hdr = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    match source {
        "slack" => verify_slack_signature(secret, headers, body, now),
        // Sentry: raw hex HMAC-SHA256 over the body, no prefix.
        "sentry" => hdr("sentry-hook-signature")
            .map(|s| verify_hex_hmac(secret, body, s))
            .unwrap_or(false),
        // PagerDuty: `X-PagerDuty-Signature: v1=<hex>[,v1=<hex>...]` — accept if any matches.
        "pagerduty" => hdr("x-pagerduty-signature")
            .map(|h| {
                h.split(',')
                    .filter_map(|p| p.trim().strip_prefix("v1="))
                    .any(|sig| verify_hex_hmac(secret, body, sig))
            })
            .unwrap_or(false),
        // GitHub and any other vendor: `sha256=<hex>` over the raw body.
        _ => verify_webhook_signature(
            secret,
            hdr("x-hub-signature-256").or_else(|| hdr("x-signature-256")),
            body,
        ),
    }
}

/// Map a cluster (Raft) write error to an HTTP response. A leadership change is **retryable**
/// (503) — leader-forwarding will route the retry to the new leader; anything else is a 500.
fn cluster_write_error(e: ecphoria_cluster::Error) -> Response {
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

// ── Admin UI (static, self-contained) ───────────────────────────────

/// Serve the embedded single-page admin console (no auth — it talks to the API with the key the
/// operator enters). Bundled into the binary via `include_str!`, so it ships with the server.
pub async fn admin_ui() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("admin_ui.html"),
    )
        .into_response()
}

// ── Health (liveness) ───────────────────────────────────────────────

/// Health check endpoint — probes DuckDB, SQLite, and Raft (if cluster mode).
///
/// Returns `ok` if all subsystems are healthy, `degraded` if any are down.
pub async fn health(
    State(engine): State<Arc<EcphoriaEngine>>,
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
    State(engine): State<Arc<EcphoriaEngine>>,
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "query").increment(1);
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

    metrics::histogram!("ecphoria_rest_request_duration_seconds", "endpoint" => "query")
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<IngestRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "ingest").increment(1);
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

    let events: Vec<ecphoria_core::memory::episodic::Event> = req
        .events
        .into_iter()
        .map(|payload| {
            let idempotency_key = payload
                .get("idempotency_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            ecphoria_core::memory::episodic::Event {
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
        let ar = ecphoria_cluster::raft::types::AppRequest::Ingest {
            events,
            tenant: tenant_id,
        };
        let result = match coord.client_write(ar).await {
            Ok(ecphoria_cluster::raft::types::AppResponse::Ingested(n)) => {
                api_ok(serde_json::json!({ "ingested": n }))
            }
            Ok(_) => api_ok(serde_json::json!({ "ingested": 0 })),
            Err(e) => cluster_write_error(e),
        };
        metrics::histogram!("ecphoria_rest_request_duration_seconds", "endpoint" => "ingest")
            .record(start.elapsed().as_secs_f64());
        return result;
    }

    let ingest_result = if let Some(tid) = tenant_id {
        let tenant = ecphoria_core::config::TenantContext::new(tid);
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

    metrics::histogram!("ecphoria_rest_request_duration_seconds", "endpoint" => "ingest")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Webhook ingestion — normalizes vendor payloads into Ecphoria events (tenant-scoped).
///
/// Signature verification is per vendor (see [`verify_vendor_signature`]): GitHub
/// `X-Hub-Signature-256`, Slack `v0=` signing (with replay window), Sentry
/// `Sentry-Hook-Signature`, PagerDuty `X-PagerDuty-Signature`. The secret is resolved per source
/// (specific entry → global default). When a source has no secret configured, the request is
/// accepted only if fail-closed mode is off; otherwise it is rejected.
pub async fn webhook(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    verifier: Option<Extension<WebhookVerifier>>,
    Path(source): Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "webhook").increment(1);

    // Verify the vendor signature over the RAW body against the source's configured secret.
    if let Some(Extension(v)) = &verifier {
        match v.secret_for(&source) {
            Some(secret) => {
                let now = chrono::Utc::now().timestamp();
                if !verify_vendor_signature(&source, secret, &headers, &body, now) {
                    metrics::counter!("ecphoria_webhook_rejected_total", "reason" => "bad_signature")
                        .increment(1);
                    return api_error(
                        StatusCode::UNAUTHORIZED,
                        "INVALID_SIGNATURE",
                        "webhook signature verification failed".into(),
                    );
                }
            }
            None if v.require => {
                // Fail-closed: an unsigned source must not be able to forge events (which would
                // feed episodic → memory distillation → auto-RAG: a poisoning vector).
                metrics::counter!("ecphoria_webhook_rejected_total", "reason" => "no_secret")
                    .increment(1);
                return api_error(
                    StatusCode::UNAUTHORIZED,
                    "SIGNATURE_REQUIRED",
                    format!(
                        "no webhook secret configured for source '{source}' and signatures are \
                         required; configure gateway.webhook_secrets = [\"{source}=<secret>\"]"
                    ),
                );
            }
            None => {}
        }
    }

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, "INVALID_JSON", e.to_string()),
    };

    match ecphoria_core::ingest::webhook::normalize_webhook(&source, &payload) {
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
                    let tc = ecphoria_core::config::TenantContext::new(t.clone());
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "search").increment(1);
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

    metrics::histogram!("ecphoria_rest_request_duration_seconds", "endpoint" => "search")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Get agent state (tenant-scoped).
pub async fn state_get(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path((agent_id, key)): Path<(String, String)>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "state_get").increment(1);

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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Path((agent_id, key)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "state_set").increment(1);

    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    // Cluster mode: replicate through the Raft log (never apply directly off-leader).
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = ecphoria_cluster::raft::types::AppRequest::StateSet {
            agent_id,
            key,
            value: body,
            tenant,
        };
        return match coord.client_write(ar).await {
            Ok(ecphoria_cluster::raft::types::AppResponse::StateVersion(v)) => {
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
    State(engine): State<Arc<EcphoriaEngine>>,
    method: axum::http::Method,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "retention_policies")
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
pub async fn enforce_retention(
    State(engine): State<Arc<EcphoriaEngine>>,
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "retention").increment(1);

    let local = match engine.enforce_retention().await {
        Ok(deleted) => serde_json::json!({
            "deleted": deleted,
            "retention_days": engine.config().memory.episodic.default_retention_days,
        }),
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RETENTION_ERROR",
                e.to_string(),
            )
        }
    };
    scatter_admin(shard, &headers, "/api/v1/admin/retention", local).await
}

/// Trigger a backup of all stores to the configured data directory.
/// GDPR erasure — delete ALL data for a tenant across every store. Admin only (under /admin/).
///
/// DELETE /api/v1/admin/tenants/{tenant_id}
pub async fn delete_tenant(
    State(engine): State<Arc<EcphoriaEngine>>,
    Path(tenant_id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "delete_tenant").increment(1);
    match engine.delete_tenant(&tenant_id).await {
        Ok(summary) => api_ok(summary),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "DELETE_TENANT_ERROR",
            e.to_string(),
        ),
    }
}

/// GDPR erasure at the person level: delete a user's memories (and vectors) within the
/// authenticated tenant. Admin. The tenant comes from the caller's token — an admin can only
/// erase users of their own tenant (multi-tenant deployments), never another's.
///
/// DELETE /api/v1/admin/users/{user_id}
pub async fn delete_user(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(user_id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "delete_user").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine.delete_user(&tenant, &user_id).await {
        Ok(summary) => api_ok(summary),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "DELETE_USER_ERROR",
            e.to_string(),
        ),
    }
}

/// Export a tenant's full data (events + memories + state) as a JSON snapshot. Admin.
///
/// GET /api/v1/admin/tenants/{tenant}/export
pub async fn export_tenant(
    State(engine): State<Arc<EcphoriaEngine>>,
    Path(tenant): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "export_tenant").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    Path(tenant): Path<String>,
    Json(snapshot): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "import_tenant").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<super::models::RebalanceRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "rebalance").increment(1);
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
        .header("x-ecphoria-shard-forwarded", "1")
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
pub async fn reindex(
    State(engine): State<Arc<EcphoriaEngine>>,
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "reindex").increment(1);
    let local = match engine.reindex_unembedded(10_000).await {
        Ok(reindexed) => {
            let pending = engine.unembedded_count().await.unwrap_or(0);
            serde_json::json!({ "reindexed": reindexed, "pending": pending })
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "REINDEX_ERROR",
                e.to_string(),
            )
        }
    };
    scatter_admin(shard, &headers, "/api/v1/admin/reindex", local).await
}

pub async fn backup(
    State(engine): State<Arc<EcphoriaEngine>>,
    shard: Option<Extension<crate::cluster::shard_route::ShardRoutingState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "backup").increment(1);

    let backup_dir = std::path::PathBuf::from(&engine.config().storage.data_dir).join("backups");
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let target = backup_dir.join(&timestamp);

    let local = match engine.backup(&target).await {
        Ok(()) => serde_json::json!({
            "status": "ok",
            "path": target.to_string_lossy(),
            "timestamp": timestamp,
        }),
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "BACKUP_ERROR",
                e.to_string(),
            )
        }
    };
    scatter_admin(shard, &headers, "/api/v1/admin/backup", local).await
}

/// Restore all stores from a backup directory. **Destructive** — overwrites current data. Admin-only
/// (the `/admin/` middleware check), and a cluster should be quiesced first (restore is node-local).
pub async fn restore(
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<RestoreRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "restore").increment(1);
    let dir = std::path::PathBuf::from(&req.path);
    match engine.restore_from_backup(&dir).await {
        Ok(()) => api_ok(serde_json::json!({ "status": "ok", "restored_from": req.path })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RESTORE_ERROR",
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
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "audit").increment(1);

    let Some(Extension(log)) = audit_log else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "AUDIT_DISABLED",
            "Audit logging is not enabled (auth must be enabled)".into(),
        );
    };

    // Local entries first.
    let mut entries = match log.query_since(&params.since, params.tenant.as_deref()) {
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
    // (the `x-ecphoria-shard-forwarded` marker), which would otherwise recurse.
    if let Some(Extension(s)) = shard {
        if s.router.shards() > 1 && !headers.contains_key("x-ecphoria-shard-forwarded") {
            for (i, base) in s.base_urls.iter().enumerate() {
                if i == s.my_shard {
                    continue;
                }
                let url = format!("{}/api/v1/admin/audit", base.trim_end_matches('/'));
                let mut q: Vec<(&str, &str)> = vec![("since", params.since.as_str())];
                if let Some(t) = params.tenant.as_deref() {
                    q.push(("tenant", t));
                }
                let mut rb = s
                    .http
                    .get(url)
                    .query(&q)
                    .header("x-ecphoria-shard-forwarded", "1");
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
    State(engine): State<Arc<EcphoriaEngine>>,
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<EmbedAndSearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "embed_and_search")
        .increment(1);
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
                    "No embedding provider configured. To enable semantic search, set ECPHORIA_EMBEDDING__PROVIDER=ollama (local, requires Ollama) or ECPHORIA_EMBEDDING__PROVIDER=openai (cloud, requires OPENAI_API_KEY)".into(),
                )
            } else {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "SEARCH_ERROR", msg)
            }
        }
    };

    metrics::histogram!("ecphoria_rest_request_duration_seconds", "endpoint" => "embed_and_search")
        .record(start.elapsed().as_secs_f64());
    result
}

// ── Session Management ─────────────────────────────────────────────

/// Start a new conversation session.
///
/// POST /api/v1/sessions { "session_id": "...", "agent_id": "...", "parent_session_id": "..." }
pub async fn session_start(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "session_start").increment(1);

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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(session_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "session_end").increment(1);

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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(session_id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "session_recall").increment(1);

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

/// POST /api/v1/sessions/{id}/distill — consolidate a closed session's events into memory.
///
/// Recalls the session's episodic events and distills them into memory (LLM-extracted atomic facts
/// when `extraction=llm`, else a single memory), scoped to the session. Cluster-replicated.
pub async fn session_distill(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Path(session_id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "session_distill").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let scope = ecphoria_core::memory::cognition::MemoryScope {
        tenant_id: tenant,
        user_id: None,
        agent_id: None,
        session_id: None,
    };

    let plans = match engine.session_distill_plan(&session_id, &scope).await {
        Ok(p) => p,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SESSION_ERROR",
                e.to_string(),
            )
        }
    };

    // Cluster mode: replicate each distilled memory's rows through the Raft log.
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        for (_result, rows) in &plans {
            if let Err(e) = coord
                .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert {
                    rows: rows.clone(),
                })
                .await
            {
                return cluster_write_error(e);
            }
        }
        let memories: Vec<_> = plans.into_iter().map(|(r, _)| r).collect();
        return api_ok(serde_json::json!({
            "session_id": session_id,
            "distilled": memories.len(),
            "memories": memories,
        }));
    }

    let mut memories = Vec::with_capacity(plans.len());
    for (result, rows) in plans {
        if let Err(e) = engine.memory_apply_rows(rows).await {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            );
        }
        memories.push(result);
    }
    api_ok(serde_json::json!({
        "session_id": session_id,
        "distilled": memories.len(),
        "memories": memories,
    }))
}

// ── Memory Cognition ────────────────────────────────────────────────

/// Resolve a memory scope, preferring the authenticated tenant for isolation.
fn scope_from(
    auth: &Option<Extension<crate::auth::middleware::AuthContext>>,
    tenant_id: Option<&str>,
    user_id: Option<&str>,
    agent_id: Option<&str>,
    session_id: Option<&str>,
) -> ecphoria_core::memory::cognition::MemoryScope {
    let tenant = auth
        .as_ref()
        .and_then(|Extension(ctx)| ctx.tenant_id.clone())
        .or_else(|| tenant_id.map(|s| s.to_string()))
        .unwrap_or_else(|| "default".to_string());
    ecphoria_core::memory::cognition::MemoryScope {
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryAddRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_add").increment(1);

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
    let input = ecphoria_core::memory::cognition::MemoryInput {
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
        let ar = ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows };
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<MemorySearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_search").increment(1);

    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let result = if req.shared {
        engine.memory_search_shared(&req.query, &scope, req.k).await
    } else {
        engine.memory_search(&req.query, &scope, req.k).await
    };
    match result {
        Ok(hits) => api_ok(serde_json::json!({ "results": hits, "count": hits.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/memories/grants — grant a user read access to another user's memories (tenant from
/// the token). GET (?grantee=U) lists a user's grants; DELETE /grants/{id} revokes one.
pub async fn grant_create(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<MemoryGrantRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_create").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .grant_share(&tenant, &req.grantee_user_id, &req.grantor_user_id)
        .await
    {
        Ok(id) => api_ok(serde_json::json!({ "id": id.to_string(), "status": "granted" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn grant_list(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<GrantListParams>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_list").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine.list_grants(&tenant, &params.grantee).await {
        Ok(grants) => api_ok(serde_json::json!({ "grants": grants, "count": grants.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn grant_revoke(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_revoke").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "grant id must be a UUID".into(),
            )
        }
    };
    match engine.revoke_grant(&tenant, uuid).await {
        Ok(removed) => api_ok(serde_json::json!({ "id": id, "revoked": removed })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

/// List active memories in a scope.
///
/// GET /api/v1/memories?user_id=alice&limit=50
pub async fn memory_list(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryListParams>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_list").increment(1);

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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_get").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_delete").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_history").increment(1);
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

/// GET /api/v1/memories/{id}/provenance — "why do you believe this?".
///
/// Returns the memory, the episodic events it was distilled from, and its bi-temporal
/// supersession chain — the audit trail behind a distilled fact. Tenant-scoped (404 on mismatch).
pub async fn memory_provenance(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_provenance")
        .increment(1);
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
    match engine.memory_provenance(uuid, tenant.as_deref()).await {
        Ok(Some(prov)) => api_ok(serde_json::json!({
            "memory": prov.memory,
            "source_events": prov.source_events,
            "source_event_count": prov.source_events.len(),
            "history": prov.history,
            "history_count": prov.history.len(),
        })),
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

/// POST /api/v1/memories/{id}/feedback — close the RAG loop.
///
/// Body `{"verdict": "helpful" | "wrong" | "obsolete"}`. `helpful` reinforces the memory
/// (importance up); `wrong`/`obsolete` retire it (bi-temporal expire + drop its vector). Lets
/// ranking learn from usage without an LLM. Tenant-scoped; replicated in cluster mode.
pub async fn memory_feedback(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Path(id): Path<String>,
    Json(req): Json<MemoryFeedbackRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_feedback").increment(1);
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
    let verdict = match ecphoria_core::MemoryFeedback::from_str_loose(&req.verdict) {
        Some(v) => v,
        None => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_VERDICT",
                "verdict must be one of: helpful, wrong, obsolete".into(),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    let (memory, action) = match engine
        .memory_feedback_plan(uuid, tenant.as_deref(), verdict)
        .await
    {
        Ok(Some(pair)) => pair,
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

    // Cluster mode: replicate the materialized change through the Raft log so followers converge.
    if let Some(Extension(coord)) = cluster {
        let ar = match &action {
            ecphoria_core::FeedbackAction::Reinforce(rows) => {
                ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows: rows.clone() }
            }
            ecphoria_core::FeedbackAction::Retire(ids) => {
                ecphoria_cluster::raft::types::AppRequest::MemoryExpire { ids: ids.clone() }
            }
        };
        return match coord.read().await.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "verdict": req.verdict, "memory": memory })),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_feedback_apply(action).await {
        Ok(()) => api_ok(serde_json::json!({ "verdict": req.verdict, "memory": memory })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// GET /api/v1/memories/contradictions — the HITL review queue.
///
/// Lists subjects with more than one active memory (only possible under
/// `cognition.contradiction_review`), each group awaiting resolution. Tenant/scope from the token.
pub async fn memory_contradictions(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<ContradictionsQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_contradictions")
        .increment(1);
    let scope = scope_from(
        &auth,
        None,
        q.user_id.as_deref(),
        q.agent_id.as_deref(),
        q.session_id.as_deref(),
    );
    match engine.memory_contradictions(&scope).await {
        Ok(groups) => {
            api_ok(serde_json::json!({ "contradictions": groups, "count": groups.len() }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/memories/contradictions/resolve — resolve a contradiction by keeping one memory and
/// superseding the others for that subject. Replicated in cluster mode.
pub async fn memory_resolve_contradiction(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<ResolveContradictionRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_resolve_contradiction")
        .increment(1);
    let scope = scope_from(
        &auth,
        None,
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let rows = match engine
        .memory_resolve_plan(&scope, &req.subject, req.keep_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, "RESOLVE_ERROR", e.to_string()),
    };
    let superseded = rows.len();

    if let Some(Extension(coord)) = cluster {
        return match coord
            .read()
            .await
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "kept": req.keep_id, "superseded": superseded })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.memory_apply_rows(rows).await {
        Ok(_) => api_ok(serde_json::json!({ "kept": req.keep_id, "superseded": superseded })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Re-embed active memories with the currently-configured provider (admin).
///
/// Run after switching embedding model/dimension so existing memories are searchable under the new
/// vectors. Recomputes up to `limit` memories' vectors (oldest-updated first, so repeated calls page
/// forward through the corpus) and re-indexes them. In cluster mode the leader computes the fresh
/// vectors and replicates the rows via Raft so every node re-indexes identically.
///
/// POST /api/v1/admin/memory/reembed  { "limit": 1000 }
pub async fn memory_reembed(
    State(engine): State<Arc<EcphoriaEngine>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryReembedRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_reembed").increment(1);
    let limit = req.limit.unwrap_or(1000);

    let rows = match engine.memory_reembed_plan(limit).await {
        Ok(rows) => rows,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "REEMBED_ERROR",
                e.to_string(),
            )
        }
    };
    let reembedded = rows.len();
    if reembedded == 0 {
        return api_ok(serde_json::json!({ "reembedded": 0 }));
    }

    if let Some(Extension(coord)) = cluster {
        return match coord
            .read()
            .await
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "reembedded": reembedded })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.memory_apply_rows(rows).await {
        Ok(_) => api_ok(serde_json::json!({ "reembedded": reembedded })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Forget low-value memories via time-decay of importance (admin).
///
/// POST /api/v1/admin/memory/decay
pub async fn memory_consolidate(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryConsolidateRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_consolidate")
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
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            return cluster_write_error(e);
        }
        return match coord
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryExpire { ids: expired })
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

/// POST /api/v1/admin/memory/consolidate-similar — fold semantically-similar memory clusters into
/// abstractions (the "near-duplicate cluster" consolidation). Cluster-replicated.
pub async fn memory_consolidate_similar(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryConsolidateSimilarRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_consolidate_similar")
        .increment(1);
    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let threshold = req.threshold.unwrap_or(0.92);

    let plans = match engine
        .memory_consolidate_similar_plan(&scope, threshold)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            )
        }
    };
    let clusters = plans.len();

    // Cluster mode: for each fold, replicate the summary (MemoryUpsert) + the originals (MemoryExpire).
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        for (input, expired) in plans {
            let (_result, rows) = match engine.memory_plan(input).await {
                Ok(p) => p,
                Err(e) => {
                    return api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "MEMORY_ERROR",
                        e.to_string(),
                    )
                }
            };
            if let Err(e) = coord
                .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
                .await
            {
                return cluster_write_error(e);
            }
            if let Err(e) = coord
                .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryExpire {
                    ids: expired,
                })
                .await
            {
                return cluster_write_error(e);
            }
        }
        return api_ok(serde_json::json!({ "clusters_folded": clusters }));
    }

    for (input, expired) in plans {
        if let Err(e) = engine.memory_add(input).await {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            );
        }
        if let Err(e) = engine.memory_expire(&expired).await {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            );
        }
    }
    api_ok(serde_json::json!({ "clusters_folded": clusters }))
}

/// Upsert a pre-computed multi-modal embedding (text/image/audio/…).
pub async fn semantic_upsert(
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<SemanticUpsertRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "semantic_upsert").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<ModalSearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "semantic_modal_search")
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryLinkRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_link").increment(1);
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
            let sup = ecphoria_cluster::raft::types::AppRequest::GraphSupersede {
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
        let edge = ecphoria_core::memory::cognition::Edge {
            id,
            src: req.src,
            relation: req.relation,
            dst: req.dst,
            weight: 1.0,
            source_memory_id: None,
            valid_from: Some(at),
            ..Default::default()
        };
        let ar = ecphoria_cluster::raft::types::AppRequest::GraphAddEdge {
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
/// List all knowledge-graph edges for the tenant (bulk graph view / export).
///
/// GET /api/v1/memories/edges?limit=N
pub async fn memory_edges(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryEdgesQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_edges").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .memory_edges(&tenant, params.limit.unwrap_or(10_000))
        .await
    {
        Ok(edges) => api_ok(serde_json::json!({ "edges": edges, "count": edges.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

// ── Public read-only publish (opt-in, unauthenticated) ─────────────────────

/// State for the public publish endpoints — the tenant whose published memories are exposed.
/// Only layered when `gateway.publish_enabled`, so the routes 404 otherwise.
#[derive(Clone)]
pub struct PublishState {
    pub tenant: String,
}

/// The published memories as `{subject, content, updated_at}` (never scope/metadata) — the read set
/// shared by the JSON and HTML public views.
async fn published_items(
    engine: &EcphoriaEngine,
    tenant: &str,
) -> Result<Vec<serde_json::Value>, Response> {
    match engine.memory_published(tenant, 500).await {
        Ok(mems) => Ok(mems
            .iter()
            .map(|m| {
                serde_json::json!({
                    "subject": m.subject,
                    "content": m.content,
                    "updated_at": m.updated_at,
                })
            })
            .collect()),
        Err(e) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "PUBLISH_ERROR",
            e.to_string(),
        )),
    }
}

/// Public read-only list of published memories (JSON). No auth.
///
/// GET /public/memories
pub async fn public_memories(
    State(engine): State<Arc<EcphoriaEngine>>,
    publish: Option<Extension<PublishState>>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "public_memories").increment(1);
    let Some(Extension(p)) = publish else {
        return api_error(
            StatusCode::NOT_FOUND,
            "PUBLISH_DISABLED",
            "publishing is disabled".into(),
        );
    };
    match published_items(&engine, &p.tenant).await {
        Ok(items) => api_ok(serde_json::json!({ "memories": items, "count": items.len() })),
        Err(resp) => resp,
    }
}

/// Public read-only HTML view of published memories (like Obsidian Publish). No auth.
///
/// GET /public
pub async fn public_index(
    State(engine): State<Arc<EcphoriaEngine>>,
    publish: Option<Extension<PublishState>>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "public_index").increment(1);
    let Some(Extension(p)) = publish else {
        return (StatusCode::NOT_FOUND, "publishing is disabled").into_response();
    };
    let items = match published_items(&engine, &p.tenant).await {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    let cards: String = items
        .iter()
        .map(|m| {
            let subject = m.get("subject").and_then(|v| v.as_str()).unwrap_or("");
            let content = m.get("content").and_then(|v| v.as_str()).unwrap_or("");
            let head = if subject.is_empty() {
                String::new()
            } else {
                format!("<h3>{}</h3>", esc(subject))
            };
            format!("<article>{head}<p>{}</p></article>", esc(content))
        })
        .collect();
    let html = format!(
        "<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>\
         <title>Published memories</title><style>body{{max-width:720px;margin:2rem auto;padding:0 1rem;font:16px/1.6 system-ui,sans-serif;color:#222}}\
         article{{border-bottom:1px solid #eee;padding:1rem 0}}h3{{margin:0 0 .3rem}}p{{margin:0;white-space:pre-wrap}}\
         footer{{color:#888;font-size:13px;margin-top:2rem}}</style></head><body>\
         <h1>Published memories</h1>{}<footer>{} memories · powered by Ecphoria</footer></body></html>",
        if cards.is_empty() { "<p>Nothing published yet.</p>".into() } else { cards },
        items.len()
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

// ── Memory templates (structured memory creation) ──────────────────────────

/// Built-in memory templates: `(name, description, [field, …])`.
fn template_catalog() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({ "name": "preference", "description": "A user preference", "fields": ["subject", "value"] }),
        serde_json::json!({ "name": "person", "description": "A fact about a person", "fields": ["name", "detail"] }),
        serde_json::json!({ "name": "decision", "description": "A decision made", "fields": ["topic", "decision"] }),
        serde_json::json!({ "name": "task", "description": "A task / procedure", "fields": ["task"] }),
    ]
}

/// Render a template into `(content, subject, mem_type)`. None if the template is unknown or a
/// required field is missing. Pure — unit-tested.
fn render_template(name: &str, fields: &serde_json::Value) -> Option<(String, String, String)> {
    let f = |k: &str| {
        fields
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    match name {
        "preference" => {
            let (s, v) = (f("subject"), f("value"));
            (!s.is_empty() && !v.is_empty()).then(|| (format!("{s}: {v}"), s, "semantic".into()))
        }
        "person" => {
            let (n, d) = (f("name"), f("detail"));
            (!n.is_empty()).then(|| {
                let content = if d.is_empty() {
                    n.clone()
                } else {
                    format!("{n} — {d}")
                };
                (content, n, "semantic".into())
            })
        }
        "decision" => {
            let (t, d) = (f("topic"), f("decision"));
            (!t.is_empty() && !d.is_empty())
                .then(|| (format!("Decision on {t}: {d}"), t, "semantic".into()))
        }
        "task" => {
            let t = f("task");
            (!t.is_empty()).then(|| (t.clone(), t, "procedural".into()))
        }
        _ => None,
    }
}

/// List the built-in memory templates.
///
/// GET /api/v1/memory-templates
pub async fn memory_templates() -> Response {
    api_ok(serde_json::json!({ "templates": template_catalog() }))
}

/// Create a memory from a template + fields.
///
/// POST /api/v1/memories/from-template  { "template": "preference", "fields": {...} }
pub async fn memory_from_template(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<FromTemplateRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_from_template")
        .increment(1);
    let Some((content, subject, mem_type)) = render_template(&req.template, &req.fields) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "TEMPLATE_ERROR",
            format!(
                "unknown template '{}' or missing required fields",
                req.template
            ),
        );
    };
    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let mut input = ecphoria_core::memory::cognition::MemoryInput::new(scope, content);
    input.subject = Some(subject);
    input.mem_type = Some(mem_type);
    match engine.memory_add(input).await {
        Ok(add) => api_ok(serde_json::to_value(&add).unwrap_or_default()),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

fn parse_as_of(s: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    s.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&chrono::Utc))
}

/// Node centrality (degree + PageRank) over the knowledge graph, optionally **as-of** a time.
///
/// GET /api/v1/memories/graph/centrality?as_of=<rfc3339>&limit=N
pub async fn graph_centrality(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<GraphAnalyticsQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "graph_centrality")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .graph_centrality(&tenant, parse_as_of(q.as_of.as_deref()))
        .await
    {
        Ok(mut nodes) => {
            if let Some(l) = q.limit {
                nodes.truncate(l);
            }
            api_ok(serde_json::json!({ "nodes": nodes, "count": nodes.len() }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRAPH_ERROR",
            e.to_string(),
        ),
    }
}

/// Shortest directed path between two entities, optionally as-of.
///
/// GET /api/v1/memories/graph/path?src=A&dst=B&as_of=<rfc3339>
pub async fn graph_path(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<GraphPathQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "graph_path").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .graph_path(&tenant, &q.src, &q.dst, parse_as_of(q.as_of.as_deref()))
        .await
    {
        Ok(path) => api_ok(serde_json::json!({
            "src": q.src, "dst": q.dst,
            "path": path, "reachable": path.is_some(),
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRAPH_ERROR",
            e.to_string(),
        ),
    }
}

/// Community detection (connected components), optionally as-of.
///
/// GET /api/v1/memories/graph/communities?as_of=<rfc3339>
pub async fn graph_communities(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<GraphAnalyticsQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "graph_communities")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .graph_communities(&tenant, parse_as_of(q.as_of.as_deref()))
        .await
    {
        Ok(comms) => api_ok(serde_json::json!({ "communities": comms, "count": comms.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRAPH_ERROR",
            e.to_string(),
        ),
    }
}

/// Upload a multimodal attachment — the raw request body is the blob, the `Content-Type` header its
/// type. Optional query: `memory_id` (link to a memory), `filename`, and `caption` (also stores a
/// searchable memory citing the attachment, so its content is retrievable via hybrid search).
///
/// POST /api/v1/attachments
pub async fn attachment_upload(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<AttachmentUploadQuery>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "attachment_upload")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    if body.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "EMPTY",
            "attachment body is empty".into(),
        );
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let memory_id = q
        .memory_id
        .as_deref()
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

    let meta = match engine
        .attachment_put(&tenant, memory_id, &content_type, q.filename.clone(), body)
        .await
    {
        Ok(m) => m,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ATTACHMENT_ERROR",
                e.to_string(),
            )
        }
    };

    // Optional caption → a searchable memory citing the attachment (the pragmatic "multimodal
    // memory": the blob is stored, its caption/OCR text is recalled via the normal hybrid search).
    if let Some(caption) = q.caption.as_deref().filter(|c| !c.trim().is_empty()) {
        let mut input = ecphoria_core::memory::cognition::MemoryInput::new(
            ecphoria_core::memory::cognition::MemoryScope::tenant(&tenant),
            caption.to_string(),
        );
        input.metadata = serde_json::json!({
            "attachment_id": meta.id.to_string(),
            "content_type": meta.content_type,
        });
        let _ = engine.memory_add(input).await;
    }

    (
        StatusCode::CREATED,
        Json(serde_json::to_value(&meta).unwrap_or_default()),
    )
        .into_response()
}

/// Search image attachments by an example image (raw body = query image bytes). Requires an image
/// embedding backend; returns `[]` otherwise. Tenant-scoped.
///
/// POST /api/v1/attachments/search-image?k=N
pub async fn attachment_search_image(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<AttachmentListQuery>,
    body: axum::body::Bytes,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "attachment_search_image")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    if body.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "EMPTY",
            "query image body is empty".into(),
        );
    }
    match engine
        .attachment_search_image(&tenant, &body, q.limit.unwrap_or(10))
        .await
    {
        Ok(items) => api_ok(serde_json::json!({ "attachments": items, "count": items.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ATTACHMENT_ERROR",
            e.to_string(),
        ),
    }
}

/// Download an attachment's bytes with its stored `Content-Type`.
///
/// GET /api/v1/attachments/{id}
pub async fn attachment_download(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "attachment_download")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "BAD_ID",
            "invalid attachment id".into(),
        );
    };
    match engine.attachment_get(&tenant, uuid).await {
        Ok(Some((meta, bytes))) => {
            let ct = axum::http::HeaderValue::from_str(&meta.content_type).unwrap_or_else(|_| {
                axum::http::HeaderValue::from_static("application/octet-stream")
            });
            let mut resp = (StatusCode::OK, bytes).into_response();
            resp.headers_mut()
                .insert(axum::http::header::CONTENT_TYPE, ct);
            resp
        }
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "attachment not found".into(),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ATTACHMENT_ERROR",
            e.to_string(),
        ),
    }
}

/// List the tenant's attachments (optionally only those linked to `?memory_id=`).
///
/// GET /api/v1/attachments
pub async fn attachment_list(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<AttachmentListQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "attachment_list").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let memory_id = q
        .memory_id
        .as_deref()
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    match engine
        .attachment_list(&tenant, memory_id, q.limit.unwrap_or(100))
        .await
    {
        Ok(items) => api_ok(serde_json::json!({ "attachments": items, "count": items.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ATTACHMENT_ERROR",
            e.to_string(),
        ),
    }
}

/// Delete an attachment (metadata + blob).
///
/// DELETE /api/v1/attachments/{id}
pub async fn attachment_delete(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "attachment_delete")
        .increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "BAD_ID",
            "invalid attachment id".into(),
        );
    };
    match engine.attachment_delete(&tenant, uuid).await {
        Ok(true) => api_ok(serde_json::json!({ "deleted": id })),
        Ok(false) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "attachment not found".into(),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ATTACHMENT_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn memory_graph(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryGraphQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_graph").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<CreateRunRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_create").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    // Materialize the run (id + timestamps) once; cluster mode replicates the identical row.
    let now = chrono::Utc::now();
    let run = ecphoria_core::runtime::Run {
        id: uuid::Uuid::new_v4(),
        tenant_id: tenant,
        agent_id: req.agent_id,
        parent_run_id: req.parent_run_id,
        status: ecphoria_core::runtime::RunStatus::Pending,
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
        let ar = ecphoria_cluster::raft::types::AppRequest::RunCreate { run: run.clone() };
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<ListRunsQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_list").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let status = match params.status.as_deref() {
        Some("pending") => Some(ecphoria_core::runtime::RunStatus::Pending),
        Some("running") => Some(ecphoria_core::runtime::RunStatus::Running),
        Some("succeeded") => Some(ecphoria_core::runtime::RunStatus::Succeeded),
        Some("failed") => Some(ecphoria_core::runtime::RunStatus::Failed),
        Some("cancelled") => Some(ecphoria_core::runtime::RunStatus::Cancelled),
        Some("waiting_approval") => Some(ecphoria_core::runtime::RunStatus::WaitingApproval),
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_get").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_trace").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_cancel").increment(1);
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
    let patch = ecphoria_core::runtime::RunPatch {
        status: Some(ecphoria_core::runtime::RunStatus::Cancelled),
        ended_at: Some(now),
        ..Default::default()
    };
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        let ar = ecphoria_cluster::raft::types::AppRequest::RunUpdate {
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<RunAgentRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_agent").increment(1);
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
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "register_tool").increment(1);
    match gateway {
        Some(Extension(gw)) => match gw.register(req.name.clone(), req.url) {
            Ok(()) => api_ok(serde_json::json!({ "status": "registered", "name": req.name })),
            Err(e) => api_error(StatusCode::BAD_REQUEST, "INVALID_TOOL_URL", e),
        },
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
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "list_tools").increment(1);
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
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "call_tool").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<RequestApprovalRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_request_approval")
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
    // Ownership check — never act on another tenant's run (mirrors run_cancel).
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<ApproveRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_approve").increment(1);
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
    // Ownership check — never resolve approval on another tenant's run (mirrors run_cancel).
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
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<RegisterTriggerRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "trigger_register")
        .increment(1);
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
pub async fn trigger_list(State(engine): State<Arc<EcphoriaEngine>>) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "trigger_list").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "run_resume").increment(1);
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
    // Ownership check — never resume another tenant's run (mirrors run_cancel).
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
    match engine.run_resume(id, &tenant).await {
        Ok(run) => api_ok(serde_json::json!({ "run": run })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RUN_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn memory_decay(State(engine): State<Arc<EcphoriaEngine>>) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_decay").increment(1);
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
    State(engine): State<Arc<EcphoriaEngine>>,
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
    State(engine): State<Arc<EcphoriaEngine>>,
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

/// GET /api/v1/memories/watch → upgrades to WebSocket.
/// Streams a JSON message for each memory lifecycle change (upserted/superseded/expired) in the
/// caller's tenant — the memory CDC stream, for reactive UIs / integrations without polling.
pub async fn memory_watch(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    ws: WebSocketUpgrade,
) -> Response {
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    ws.on_upgrade(move |socket| handle_memory_ws(socket, engine, tenant))
}

async fn handle_memory_ws(
    mut socket: WebSocket,
    engine: Arc<EcphoriaEngine>,
    tenant: Option<String>,
) {
    let mut rx = engine.memory_subscribe();
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(change) => {
                        // Tenant isolation: only forward this tenant's changes (all, when unscoped).
                        if tenant.as_deref().is_none_or(|t| t == change.tenant_id) {
                            let msg = serde_json::to_string(&change).unwrap_or_default();
                            if socket.send(Message::Text(msg.into())).await.is_err() {
                                break; // client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, "memory watcher lagged");
                    }
                    Err(_) => break, // channel closed
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

async fn handle_state_ws(mut socket: WebSocket, engine: Arc<EcphoriaEngine>, agent_id: String) {
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

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn shard_state(base_urls: Vec<String>) -> crate::cluster::shard_route::ShardRoutingState {
        crate::cluster::shard_route::ShardRoutingState {
            router: Arc::new(ecphoria_cluster::ShardRouter::new(base_urls.len(), 128)),
            my_shard: 0,
            base_urls: Arc::new(base_urls),
            http: reqwest::Client::new(),
            forward_secret: None,
        }
    }

    /// Cluster-wide admin: the receiving shard's result plus each peer shard's result are aggregated
    /// into one per-shard breakdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scatter_admin_aggregates_all_shards() {
        use axum::{routing::post, Json, Router};

        // Mock peer "shard 1": responds to the forwarded admin call with its own local result, and
        // asserts it received the recursion-guard marker.
        let app = Router::new().route(
            "/api/v1/admin/retention",
            post(|headers: axum::http::HeaderMap| async move {
                assert!(
                    headers.contains_key(SHARD_FWD_HEADER),
                    "peer must get the marker"
                );
                Json(serde_json::json!({ "deleted": 5 }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let state = shard_state(vec!["http://127.0.0.1:1".into(), format!("http://{addr}")]);
        let resp = scatter_admin(
            Some(Extension(state)),
            &axum::http::HeaderMap::new(),
            "/api/v1/admin/retention",
            serde_json::json!({ "deleted": 3 }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["partial"], false);
        let shards = body["shards"].as_array().unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0]["result"]["deleted"], 3); // this shard (0)
        assert_eq!(shards[1]["result"]["deleted"], 5); // peer shard (1)
    }

    /// A shard that is unreachable yields HTTP 207 with `partial: true` — never a silent 200 that
    /// would hide an un-backed-up / un-pruned shard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scatter_admin_partial_failure_is_207() {
        // base_urls[1] points at a closed port → the peer call fails.
        let state = shard_state(vec![
            "http://127.0.0.1:1".into(),
            "http://127.0.0.1:2".into(),
        ]);
        let resp = scatter_admin(
            Some(Extension(state)),
            &axum::http::HeaderMap::new(),
            "/api/v1/admin/backup",
            serde_json::json!({ "status": "ok" }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::MULTI_STATUS);
        let body = body_json(resp).await;
        assert_eq!(body["partial"], true);
        assert_eq!(body["shards"][0]["status"], "ok"); // local shard succeeded
        assert_eq!(body["shards"][1]["status"], "error"); // peer unreachable
    }

    /// A forwarded sub-request (marker present) returns its single-shard result unchanged — this is
    /// what prevents fan-out recursion and preserves the single-shard response shape.
    #[tokio::test]
    async fn scatter_admin_forwarded_returns_local() {
        let state = shard_state(vec!["http://a".into(), "http://b".into()]);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(SHARD_FWD_HEADER, "1".parse().unwrap());
        let resp = scatter_admin(
            Some(Extension(state)),
            &headers,
            "/api/v1/admin/retention",
            serde_json::json!({ "deleted": 7 }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["deleted"], 7); // raw single-shard result, no "shards" wrapper
        assert!(body.get("shards").is_none());
    }

    #[test]
    fn render_template_builds_or_rejects() {
        let (c, s, t) = render_template(
            "preference",
            &serde_json::json!({"subject":"coffee","value":"espresso"}),
        )
        .unwrap();
        assert_eq!(
            (c.as_str(), s.as_str(), t.as_str()),
            ("coffee: espresso", "coffee", "semantic")
        );

        let (c, s, _) = render_template(
            "person",
            &serde_json::json!({"name":"Alice","detail":"engineer"}),
        )
        .unwrap();
        assert_eq!((c.as_str(), s.as_str()), ("Alice — engineer", "Alice"));

        let (c, _, t) = render_template("task", &serde_json::json!({"task":"deploy"})).unwrap();
        assert_eq!((c.as_str(), t.as_str()), ("deploy", "procedural"));

        // Missing required field → None; unknown template → None.
        assert!(render_template("preference", &serde_json::json!({"subject":"x"})).is_none());
        assert!(render_template("nope", &serde_json::json!({})).is_none());
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

    #[test]
    fn slack_signature_scheme_and_replay_window() {
        let secret = "slack-signing-secret";
        let body = br#"{"event":"x"}"#;
        let ts = 1_700_000_000_i64;
        let basestring = format!("v0:{ts}:{}", String::from_utf8_lossy(body));
        let sig = format!(
            "v0={}",
            hex_encode(&hmac_sha256(secret, basestring.as_bytes()).unwrap())
        );

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.to_string().parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        // Valid within the ±5-min window.
        assert!(verify_slack_signature(secret, &headers, body, ts + 10));
        // Same signature but a stale timestamp → rejected (replay guard).
        assert!(!verify_slack_signature(secret, &headers, body, ts + 1_000));
        // Wrong secret → rejected.
        assert!(!verify_slack_signature("nope", &headers, body, ts + 10));
        // Tampered body → rejected.
        assert!(!verify_slack_signature(secret, &headers, b"other", ts + 10));
    }

    #[test]
    fn vendor_dispatch_picks_the_right_scheme() {
        let secret = "s3cr3t";
        let body = br#"{"a":1}"#;
        let raw_hex = hex_encode(&hmac_sha256(secret, body).unwrap());

        // Sentry: raw hex, no prefix, in Sentry-Hook-Signature.
        let mut h = axum::http::HeaderMap::new();
        h.insert("sentry-hook-signature", raw_hex.parse().unwrap());
        assert!(verify_vendor_signature("sentry", secret, &h, body, 0));
        assert!(!verify_vendor_signature("sentry", "wrong", &h, body, 0));

        // PagerDuty: v1=<hex>, possibly multiple comma-separated.
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            "x-pagerduty-signature",
            format!("v1=deadbeef,v1={raw_hex}").parse().unwrap(),
        );
        assert!(verify_vendor_signature("pagerduty", secret, &h, body, 0));

        // GitHub / unknown: sha256=<hex> in X-Hub-Signature-256.
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            "x-hub-signature-256",
            format!("sha256={raw_hex}").parse().unwrap(),
        );
        assert!(verify_vendor_signature("github", secret, &h, body, 0));
        assert!(verify_vendor_signature(
            "some-custom-source",
            secret,
            &h,
            body,
            0
        ));
    }

    #[test]
    fn verifier_resolves_per_source_then_default() {
        let v = WebhookVerifier::from_config(
            Some("global".into()),
            &["github=gh-secret".into(), "slack=sk-secret".into()],
            false,
        );
        assert_eq!(v.secret_for("github"), Some("gh-secret"));
        assert_eq!(v.secret_for("slack"), Some("sk-secret"));
        // Unlisted source falls back to the global default.
        assert_eq!(v.secret_for("sentry"), Some("global"));
    }

    #[test]
    fn verifier_fail_closed_has_no_default() {
        let v = WebhookVerifier::from_config(None, &["github=gh".into()], true);
        assert!(v.require);
        assert_eq!(v.secret_for("github"), Some("gh"));
        // No secret + require=true → the handler rejects (secret_for returns None here).
        assert_eq!(v.secret_for("unknown"), None);
    }
}
