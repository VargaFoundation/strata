//! Security tests: a tenant must never read another tenant's data, and the protocol
//! endpoints (/mcp) require authentication when auth is enabled.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use strata_core::{CoreConfig, StrataEngine};
use tower::ServiceExt;

const SECRET: &str = "test-secret-key-256-bits-long!!!";

/// Mint an HS256 JWT scoped to a tenant (writer role).
fn jwt(tenant: &str) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let claims = serde_json::json!({
        "sub": format!("user-{tenant}"),
        "role": "writer",
        "exp": exp,
        "tenant_id": tenant,
    });
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// Build an auth-enabled router over fully in-memory stores.
async fn authed_router() -> axum::Router {
    let mut config = CoreConfig::default();
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    let engine = Arc::new(StrataEngine::new(config).await.unwrap());

    let auth = strata_gateway::auth::middleware::AuthState::new(vec![], Some(SECRET.into()), 0);
    let gw = strata_gateway::server::GatewayConfig {
        auth_enabled: true,
        ..Default::default()
    };
    strata_gateway::rest::router_with_engine_and_auth(engine, Some(auth), None, &gw)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let req = match body {
        Some(b) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn sql_query_isolates_tenants() {
    let app = authed_router().await;
    let (ta, tb) = (jwt("tenant-a"), jwt("tenant-b"));

    // tenant-a ingests one event.
    let (s, _) = send(
        &app,
        "POST",
        "/api/v1/ingest",
        Some(&ta),
        Some(r#"{"source":"sa","events":[{"event_type":"e","note":"alpha"}]}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // tenant-b runs `SELECT *`-style count → sees NOTHING of tenant-a.
    let (s, body) = send(
        &app,
        "POST",
        "/api/v1/query",
        Some(&tb),
        Some(r#"{"sql":"SELECT count(*)::VARCHAR AS c FROM episodic"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["rows"][0]["c"], "0", "tenant-b leaked tenant-a rows!");

    // tenant-a sees its own row.
    let (_, body) = send(
        &app,
        "POST",
        "/api/v1/query",
        Some(&ta),
        Some(r#"{"sql":"SELECT count(*)::VARCHAR AS c FROM episodic"}"#),
    )
    .await;
    assert_eq!(body["rows"][0]["c"], "1");
}

#[tokio::test]
async fn memories_isolate_tenants() {
    let app = authed_router().await;
    let (ta, tb) = (jwt("tenant-a"), jwt("tenant-b"));

    // tenant-a stores a memory.
    let (s, body) = send(
        &app,
        "POST",
        "/api/v1/memories",
        Some(&ta),
        Some(r#"{"content":"alpha secret","user_id":"u1"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let id = body["memory"]["id"].as_str().unwrap().to_string();

    // tenant-b lists the same user scope → empty (different tenant).
    let (_, body) = send(&app, "GET", "/api/v1/memories?user_id=u1", Some(&tb), None).await;
    assert_eq!(body["count"], 0, "tenant-b leaked tenant-a memories!");

    // tenant-b cannot fetch tenant-a's memory by id → 404.
    let (s, _) = send(
        &app,
        "GET",
        &format!("/api/v1/memories/{id}"),
        Some(&tb),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // tenant-a can.
    let (s, _) = send(
        &app,
        "GET",
        &format!("/api/v1/memories/{id}"),
        Some(&ta),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn mcp_and_proxy_require_auth_when_enabled() {
    let app = authed_router().await;

    // No token → 401 on /mcp.
    let (s, _) = send(
        &app,
        "POST",
        "/mcp",
        None,
        Some(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    // With a valid token → 200.
    let (s, _) = send(
        &app,
        "POST",
        "/mcp",
        Some(&jwt("tenant-a")),
        Some(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn state_isolates_tenants() {
    let app = authed_router().await;
    let (ta, tb) = (jwt("tenant-a"), jwt("tenant-b"));

    // tenant-a writes state for agent "bot".
    let (s, _) = send(
        &app,
        "PUT",
        "/api/v1/state/bot/mood",
        Some(&ta),
        Some(r#""happy""#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // tenant-b reads the SAME agent/key → not found (separate tenant namespace).
    let (s, _) = send(&app, "GET", "/api/v1/state/bot/mood", Some(&tb), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "tenant-b read tenant-a state!");

    // tenant-a reads its own → ok, and the agent_id is returned un-prefixed.
    let (s, body) = send(&app, "GET", "/api/v1/state/bot/mood", Some(&ta), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["value"], "happy");
    assert_eq!(body["agent_id"], "bot");
}

#[tokio::test]
async fn schema_introspection_isolates_tenants() {
    let app = authed_router().await;
    let (ta, tb) = (jwt("tenant-a"), jwt("tenant-b"));

    send(
        &app,
        "POST",
        "/api/v1/ingest",
        Some(&ta),
        Some(r#"{"source":"sa","events":[{"event_type":"e"}]}"#),
    )
    .await;
    send(&app, "PUT", "/api/v1/state/bot-a/k", Some(&ta), Some("1")).await;

    // tenant-b sees neither the source nor the agent.
    let (_, srcs) = send(&app, "GET", "/api/v1/schema/sources", Some(&tb), None).await;
    assert_eq!(srcs["sources"].as_array().unwrap().len(), 0);
    let (_, ags) = send(&app, "GET", "/api/v1/schema/agents", Some(&tb), None).await;
    assert_eq!(ags["agents"].as_array().unwrap().len(), 0);

    // tenant-a sees its own.
    let (_, srcs) = send(&app, "GET", "/api/v1/schema/sources", Some(&ta), None).await;
    assert!(srcs["sources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s == "sa"));
    let (_, ags) = send(&app, "GET", "/api/v1/schema/agents", Some(&ta), None).await;
    assert!(ags["agents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a == "bot-a"));
}

#[tokio::test]
async fn webhook_is_tenant_scoped() {
    let app = authed_router().await;
    let (ta, tb) = (jwt("tenant-a"), jwt("tenant-b"));

    let (s, _) = send(
        &app,
        "POST",
        "/api/v1/webhook/github",
        Some(&ta),
        Some(r#"{"action":"completed","commits":[{"id":"abc"}],"repository":{"full_name":"o/r"},"sender":{"login":"d"}}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // tenant-b sees nothing from tenant-a's webhook.
    let (_, body) = send(
        &app,
        "POST",
        "/api/v1/query",
        Some(&tb),
        Some(r#"{"sql":"SELECT count(*)::VARCHAR AS c FROM episodic"}"#),
    )
    .await;
    assert_eq!(
        body["rows"][0]["c"], "0",
        "tenant-b saw tenant-a webhook data!"
    );

    // tenant-a sees its own webhook event.
    let (_, body) = send(
        &app,
        "POST",
        "/api/v1/query",
        Some(&ta),
        Some(r#"{"sql":"SELECT count(*)::VARCHAR AS c FROM episodic"}"#),
    )
    .await;
    assert_eq!(body["rows"][0]["c"], "1");
}
