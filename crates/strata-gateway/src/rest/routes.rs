//! REST API route definitions.

use std::sync::Arc;
use std::time::Duration;

use super::handlers;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::header::HeaderName;
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
use strata_cluster::ClusterCoordinator;
use strata_core::StrataEngine;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Maximum request body size (16 MB).
const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Shared handle for cluster coordinator access from health/ready endpoints.
#[derive(Clone)]
pub struct ClusterHandle(pub Arc<RwLock<ClusterCoordinator>>);

/// Build a minimal REST API router (no engine state — for basic testing).
pub fn router() -> Router {
    // Minimal router needs a default engine for health checks
    Router::new()
        .route(
            "/api/v1/query",
            axum::routing::post(handlers::query_no_engine),
        )
        .route(
            "/api/v1/ingest",
            axum::routing::post(handlers::ingest_no_engine),
        )
        .route(
            "/api/v1/search",
            axum::routing::post(handlers::search_no_engine),
        )
}

/// Build the full REST API router with engine state and production middleware.
///
/// If `auth_state` is provided, all `/api/v1/*` routes require authentication.
pub fn router_with_engine(engine: Arc<StrataEngine>) -> Router {
    router_with_engine_and_auth(engine, None, None, &crate::server::GatewayConfig::default())
}

/// Build the full REST API router with optional auth and cluster middleware.
pub fn router_with_engine_and_auth(
    engine: Arc<StrataEngine>,
    auth_state: Option<crate::auth::middleware::AuthState>,
    cluster_state: Option<crate::cluster::leader_forward::ClusterState>,
    config: &crate::server::GatewayConfig,
) -> Router {
    // Public routes (no auth required) — health/ready need engine state
    let cluster_handle = cluster_state
        .as_ref()
        .map(|cs| ClusterHandle(cs.coordinator.clone()));

    let mut health_routes = Router::new()
        .route("/health", axum::routing::get(handlers::health))
        .route("/ready", axum::routing::get(handlers::ready))
        .with_state(engine.clone());

    // If cluster mode, provide the coordinator as an optional extension
    if let Some(handle) = cluster_handle {
        health_routes = health_routes.layer(axum::Extension(handle));
    }

    let mut app = health_routes;

    // Protected API routes
    let mut api_routes = Router::new()
        .route("/query", axum::routing::post(handlers::query))
        .route("/ingest", axum::routing::post(handlers::ingest))
        .route("/webhook/{source}", axum::routing::post(handlers::webhook))
        .route("/search", axum::routing::post(handlers::search))
        .route(
            "/state/{agent_id}/{key}",
            axum::routing::get(handlers::state_get).put(handlers::state_set),
        )
        .route(
            "/embed-and-search",
            axum::routing::post(handlers::embed_and_search),
        )
        .route(
            "/schema/sources",
            axum::routing::get(handlers::schema_sources),
        )
        .route(
            "/schema/agents",
            axum::routing::get(handlers::schema_agents),
        )
        .route(
            "/admin/retention",
            axum::routing::post(handlers::enforce_retention),
        )
        .route(
            "/admin/retention/policies",
            axum::routing::get(handlers::retention_policies).put(handlers::retention_policies),
        )
        .route("/admin/backup", axum::routing::post(handlers::backup))
        .route(
            "/admin/tenants/{tenant_id}",
            axum::routing::delete(handlers::delete_tenant),
        )
        .route("/admin/audit", axum::routing::get(handlers::audit_query))
        .route(
            "/state/{agent_id}/watch",
            axum::routing::get(handlers::state_watch),
        )
        .route(
            "/memories",
            axum::routing::post(handlers::memory_add).get(handlers::memory_list),
        )
        .route(
            "/memories/search",
            axum::routing::post(handlers::memory_search),
        )
        .route(
            "/memories/{id}",
            axum::routing::get(handlers::memory_get).delete(handlers::memory_delete),
        )
        .route(
            "/memories/{id}/history",
            axum::routing::get(handlers::memory_history),
        )
        .route(
            "/admin/memory/decay",
            axum::routing::post(handlers::memory_decay),
        )
        .route("/sessions", axum::routing::post(handlers::session_start))
        .route(
            "/sessions/{session_id}/end",
            axum::routing::post(handlers::session_end),
        )
        .route(
            "/sessions/{session_id}/recall",
            axum::routing::get(handlers::session_recall),
        )
        .with_state(engine.clone());

    // Keep a handle so MCP + LLM-proxy routes can be authenticated too.
    let protocol_auth = auth_state.clone();

    // Expose the configured webhook HMAC secret to the webhook handler.
    api_routes = api_routes.layer(axum::Extension(handlers::WebhookSecret(
        config.webhook_secret.clone(),
    )));

    // Apply auth middleware if configured
    if let Some(state) = auth_state {
        // Expose audit log to the admin audit handler
        if let Some(audit_log) = state.audit_log() {
            api_routes = api_routes.layer(axum::Extension(audit_log.clone()));
        }
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            state,
            crate::auth::middleware::require_auth,
        ));
    }

    // Coordinator handle for write replication (also handed to the MCP protocol routes below).
    let coordinator = cluster_state.as_ref().map(|cs| cs.coordinator.clone());

    // Apply leader-forwarding middleware if cluster mode is active
    if let Some(cluster_state) = cluster_state {
        // Expose the coordinator to write handlers so they replicate through the Raft log.
        api_routes = api_routes.layer(axum::Extension(cluster_state.coordinator.clone()));
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            cluster_state,
            crate::cluster::leader_forward::require_leader_for_writes,
        ));
    }

    app = app.nest("/api/v1", api_routes);

    // MCP & LLM proxy (use engine state, resolved separately)
    let mut protocol_routes = Router::new()
        .route(
            "/mcp",
            axum::routing::post(crate::mcp::transport::handle_mcp)
                .get(crate::mcp::transport::handle_mcp_sse),
        )
        .route(
            "/v1/chat/completions",
            axum::routing::post(crate::llm_proxy::router::chat_completions),
        )
        .with_state(engine);

    // MCP write tools replicate through Raft in cluster mode (MCP isn't leader-forwarded, so the
    // handler checks leadership itself).
    if let Some(coord) = coordinator {
        protocol_routes = protocol_routes.layer(axum::Extension(coord));
    }

    // When auth is enabled, MCP and the LLM proxy require a Bearer token too
    // (clients like Claude Code / OpenAI SDKs send `Authorization: Bearer <token>`).
    if let Some(state) = protocol_auth {
        protocol_routes = protocol_routes.route_layer(axum::middleware::from_fn_with_state(
            state,
            crate::auth::middleware::require_auth,
        ));
    }

    app = app.merge(protocol_routes);

    // CORS: explicit origins if configured, restrictive when auth enabled, otherwise permissive (dev only)
    let cors = if !config.cors_origins.is_empty() {
        let origins: Vec<axum::http::HeaderValue> = config
            .cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::DELETE,
                axum::http::Method::OPTIONS,
            ])
            .allow_headers([
                axum::http::header::AUTHORIZATION,
                axum::http::header::CONTENT_TYPE,
                axum::http::header::ACCEPT,
            ])
            .allow_credentials(true)
    } else if config.auth_enabled {
        // Auth enabled but no explicit origins: restrict to same-origin for safety
        tracing::warn!(
            "auth_enabled=true but no cors_origins configured — CORS restricted to same-origin"
        );
        CorsLayer::new()
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::DELETE,
                axum::http::Method::OPTIONS,
            ])
            .allow_headers([
                axum::http::header::AUTHORIZATION,
                axum::http::header::CONTENT_TYPE,
                axum::http::header::ACCEPT,
            ])
    } else {
        CorsLayer::permissive()
    };

    // Global middleware stack (applied bottom-up)
    app.layer(axum::middleware::from_fn(request_id_middleware))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
}

/// Middleware that reads or generates an `X-Request-Id` header and includes it in the response.
///
/// If the client sends an `X-Request-Id`, it is preserved. Otherwise a UUID v4 is generated.
/// The request ID is also injected into the current tracing span for log correlation.
async fn request_id_middleware(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get(&X_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    tracing::Span::current().record("request_id", &request_id);

    let mut response = next.run(req).await;
    if let Ok(val) = request_id.parse() {
        response.headers_mut().insert(X_REQUEST_ID.clone(), val);
    }
    response
}
