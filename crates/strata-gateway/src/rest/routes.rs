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
    router_with_engine_and_auth(
        engine,
        None,
        None,
        None,
        &crate::server::GatewayConfig::default(),
    )
}

/// Build the full REST API router with optional auth, cluster, and shard-routing middleware.
pub fn router_with_engine_and_auth(
    engine: Arc<StrataEngine>,
    auth_state: Option<crate::auth::middleware::AuthState>,
    cluster_state: Option<crate::cluster::leader_forward::ClusterState>,
    shard_state: Option<crate::cluster::shard_route::ShardRoutingState>,
    config: &crate::server::GatewayConfig,
) -> Router {
    // Public routes (no auth required) — health/ready need engine state
    let cluster_handle = cluster_state
        .as_ref()
        .map(|cs| ClusterHandle(cs.coordinator.clone()));

    let mut health_routes = Router::new()
        .route("/health", axum::routing::get(handlers::health))
        .route("/ready", axum::routing::get(handlers::ready))
        // Self-contained admin console (public; authenticates to the API with an operator-supplied key).
        .route("/ui", axum::routing::get(handlers::admin_ui))
        .route(
            "/",
            axum::routing::get(|| async { axum::response::Redirect::permanent("/ui") }),
        )
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
        .route("/admin/restore", axum::routing::post(handlers::restore))
        .route("/admin/reindex", axum::routing::post(handlers::reindex))
        .route(
            "/admin/tenants/{tenant}/export",
            axum::routing::get(handlers::export_tenant),
        )
        .route(
            "/admin/tenants/{tenant}/import",
            axum::routing::post(handlers::import_tenant),
        )
        .route("/admin/rebalance", axum::routing::post(handlers::rebalance))
        .route(
            "/admin/tenants/{tenant_id}",
            axum::routing::delete(handlers::delete_tenant),
        )
        .route(
            "/admin/users/{user_id}",
            axum::routing::delete(handlers::delete_user),
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
            "/memories/{id}/provenance",
            axum::routing::get(handlers::memory_provenance),
        )
        .route(
            "/memories/{id}/feedback",
            axum::routing::post(handlers::memory_feedback),
        )
        .route(
            "/memories/watch",
            axum::routing::get(handlers::memory_watch),
        )
        .route(
            "/memories/contradictions",
            axum::routing::get(handlers::memory_contradictions),
        )
        .route(
            "/memories/contradictions/resolve",
            axum::routing::post(handlers::memory_resolve_contradiction),
        )
        .route(
            "/admin/memory/decay",
            axum::routing::post(handlers::memory_decay),
        )
        .route(
            "/admin/memory/consolidate",
            axum::routing::post(handlers::memory_consolidate),
        )
        .route(
            "/admin/memory/consolidate-similar",
            axum::routing::post(handlers::memory_consolidate_similar),
        )
        .route(
            "/semantic/upsert",
            axum::routing::post(handlers::semantic_upsert),
        )
        .route(
            "/semantic/search",
            axum::routing::post(handlers::semantic_modal_search),
        )
        .route("/memories/link", axum::routing::post(handlers::memory_link))
        .route(
            "/memories/graph",
            axum::routing::get(handlers::memory_graph),
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
        .route(
            "/sessions/{session_id}/distill",
            axum::routing::post(handlers::session_distill),
        )
        .route(
            "/runs",
            axum::routing::post(handlers::run_create).get(handlers::run_list),
        )
        .route("/runs/{id}", axum::routing::get(handlers::run_get))
        .route("/runs/{id}/trace", axum::routing::get(handlers::run_trace))
        .route(
            "/runs/{id}/cancel",
            axum::routing::post(handlers::run_cancel),
        )
        .route(
            "/runs/{id}/request-approval",
            axum::routing::post(handlers::run_request_approval),
        )
        .route(
            "/runs/{id}/approve",
            axum::routing::post(handlers::run_approve),
        )
        .route(
            "/runs/{id}/resume",
            axum::routing::post(handlers::run_resume),
        )
        .route(
            "/agents/run",
            axum::routing::post(handlers::run_agent_endpoint),
        )
        .route(
            "/tools",
            axum::routing::post(handlers::register_tool).get(handlers::list_tools),
        )
        .route(
            "/tools/{server}/call",
            axum::routing::post(handlers::call_tool),
        )
        .route(
            "/triggers",
            axum::routing::post(handlers::trigger_register).get(handlers::trigger_list),
        )
        .with_state(engine.clone());

    // Keep a handle so MCP + LLM-proxy routes can be authenticated too.
    let protocol_auth = auth_state.clone();

    // Expose the webhook signature verifier (per-source secrets + vendor schemes) to the handler.
    api_routes = api_routes.layer(axum::Extension(handlers::WebhookVerifier::from_config(
        config.webhook_secret.clone(),
        &config.webhook_secrets,
        config.webhook_require_signature,
    )));

    // MCP tool-gateway: a governed registry of downstream MCP servers agents can call. Also injected
    // into the engine so the agent loop can invoke registered tools via `TOOL call`.
    let tool_gateway = std::sync::Arc::new(crate::rest::tool_gateway::ToolGateway::new(
        config.tool_gateway_allow_private_networks,
    ));
    engine.set_tool_executor(tool_gateway.clone());
    api_routes = api_routes.layer(axum::Extension(tool_gateway));

    // Coordinator handle for write replication (also handed to the MCP protocol routes below).
    let coordinator = cluster_state.as_ref().map(|cs| cs.coordinator.clone());

    // Keep a copy of the shard-routing state for the MCP/LLM protocol routes (below).
    let shard_state_protocol = shard_state.clone();

    // Middleware execution order (request flows outer→inner): auth → shard-route → leader-forward →
    // handler. In axum the LAST-applied route_layer is the OUTERMOST (runs first), so apply them in
    // reverse: leader-forward first (innermost), then shard-route, then auth last (outermost).
    // Auth must run before shard-route because shard-route reads the tenant from `AuthContext`.

    // 1. Leader-forwarding (innermost) — applied first.
    if let Some(cluster_state) = cluster_state {
        api_routes = api_routes.layer(axum::Extension(cluster_state.coordinator.clone()));
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            cluster_state,
            crate::cluster::leader_forward::require_leader_for_writes,
        ));
    }

    // 2. Shard routing (middle) — only when actually sharded; routes each request to its tenant's shard.
    if let Some(shard_state) = shard_state {
        if shard_state.router.shards() > 1 {
            // Also expose the state to handlers that scatter-gather across shards (e.g. admin audit).
            api_routes = api_routes.layer(axum::Extension(shard_state.clone()));
            api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
                shard_state,
                crate::cluster::shard_route::route_to_owning_shard,
            ));
        }
    }

    // 3. Auth (outermost) — applied last, so it runs first and populates AuthContext for shard routing.
    if let Some(state) = auth_state {
        if let Some(audit_log) = state.audit_log() {
            api_routes = api_routes.layer(axum::Extension(audit_log.clone()));
        }
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            state,
            crate::auth::middleware::require_auth,
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
        .route(
            "/v1/embeddings",
            axum::routing::post(crate::llm_proxy::router::embeddings),
        )
        .route(
            "/v1/messages",
            axum::routing::post(crate::llm_proxy::router::messages),
        )
        .with_state(engine)
        // Response-cache mode for the proxy (exact-match by default; similarity is opt-in).
        .layer(axum::Extension(
            crate::llm_proxy::router::LlmCacheSimilarity(config.llm_cache_similarity),
        ));

    // MCP write tools replicate through Raft in cluster mode (MCP isn't leader-forwarded, so the
    // handler checks leadership itself).
    if let Some(coord) = coordinator {
        protocol_routes = protocol_routes.layer(axum::Extension(coord));
    }

    // Shard routing for MCP/LLM (inner) — register before auth so execution is auth → shard-route →
    // handler. A tenant's MCP writes route to its shard (same Raft group as its REST writes).
    if let Some(shard_state) = shard_state_protocol {
        if shard_state.router.shards() > 1 {
            protocol_routes = protocol_routes.route_layer(axum::middleware::from_fn_with_state(
                shard_state,
                crate::cluster::shard_route::route_to_owning_shard,
            ));
        }
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
