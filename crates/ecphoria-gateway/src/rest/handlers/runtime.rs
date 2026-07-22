//! Agent-runtime REST handlers — runs, tools, triggers, approvals (split from handlers.rs).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::{Extension, Json};
use ecphoria_core::EcphoriaEngine;

use crate::rest::models::*;

use super::{api_error, api_ok, cluster_write_error};

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
