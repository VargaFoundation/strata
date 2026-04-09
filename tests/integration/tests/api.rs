//! Integration tests: REST API routing via axum's test utilities.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt;

fn app() -> axum::Router {
    strata_gateway::rest::router()
}

#[tokio::test]
async fn health_returns_ok() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn query_accepts_valid_sql() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"sql": "SELECT 1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["rows"].is_array());
    assert_eq!(json["count"], 0);
}

#[tokio::test]
async fn ingest_accepts_events() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"source": "test", "events": [{"type": "click"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ingested"], 0);
}

#[tokio::test]
async fn search_accepts_query() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/search")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"query": "billing issue", "k": 3}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["results"].is_array());
}

#[tokio::test]
async fn rejects_invalid_json() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Axum returns 400 for JSON parse errors
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn query_without_content_type_returns_415() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .body(Body::from(r#"{"sql": "SELECT 1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Without Content-Type header, axum rejects with 415
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}
