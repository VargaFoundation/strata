//! REST API route definitions.

use std::sync::Arc;

use axum::Router;
use strata_core::StrataEngine;

use super::handlers;

/// Build a minimal REST API router (no engine state — for basic testing).
pub fn router() -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
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

/// Build the full REST API router with engine state.
pub fn router_with_engine(engine: Arc<StrataEngine>) -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
        .route("/api/v1/query", axum::routing::post(handlers::query))
        .route("/api/v1/ingest", axum::routing::post(handlers::ingest))
        .route("/api/v1/search", axum::routing::post(handlers::search))
        .route(
            "/api/v1/state/{agent_id}/{key}",
            axum::routing::get(handlers::state_get).put(handlers::state_set),
        )
        .route(
            "/mcp",
            axum::routing::post(crate::mcp::transport::handle_mcp),
        )
        .with_state(engine)
}
