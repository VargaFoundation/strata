//! REST API handler functions.

use axum::Json;

use super::models::{HealthResponse, IngestRequest, IngestResponse, QueryRequest, SearchRequest};

/// Health check endpoint.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

/// Execute a SQL query.
pub async fn query(Json(_req): Json<QueryRequest>) -> Json<serde_json::Value> {
    // TODO: route to query engine
    Json(serde_json::json!({ "rows": [], "count": 0 }))
}

/// Ingest events.
pub async fn ingest(Json(_req): Json<IngestRequest>) -> Json<IngestResponse> {
    // TODO: route to ingest pipeline
    Json(IngestResponse { ingested: 0 })
}

/// Semantic search.
pub async fn search(Json(_req): Json<SearchRequest>) -> Json<serde_json::Value> {
    // TODO: route to semantic store
    Json(serde_json::json!({ "results": [] }))
}
