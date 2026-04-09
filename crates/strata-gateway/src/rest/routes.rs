//! REST API route definitions.

use axum::Router;

/// Build the REST API router.
pub fn router() -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
        .route("/api/v1/query", axum::routing::post(handlers::query))
        .route("/api/v1/ingest", axum::routing::post(handlers::ingest))
        .route("/api/v1/search", axum::routing::post(handlers::search))
}

mod handlers {
    pub use super::super::handlers::*;
}
