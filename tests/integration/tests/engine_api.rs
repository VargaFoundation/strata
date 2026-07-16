//! Integration tests: REST API backed by a real engine.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use strata_core::{CoreConfig, StrataEngine};
use tower::ServiceExt;

async fn engine_router() -> axum::Router {
    let mut config = CoreConfig::default();
    // All stores in-memory so tests are isolated and don't persist state (e.g. the sessions
    // table) into a shared ./data file across runs.
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    config.runtime.db_path = ":memory:".into();
    let engine = Arc::new(StrataEngine::new(config).await.unwrap());
    strata_gateway::rest::router_with_engine(engine)
}

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn run_lifecycle_via_rest() {
    let app = engine_router().await;

    // Create a run.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"agent_id":"a1","input":{"q":1}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let created = json_body(resp).await;
    assert_eq!(created["run"]["status"], "pending");
    assert_eq!(created["run"]["agent_id"], "a1");
    let id = created["run"]["id"].as_str().unwrap().to_string();

    // Get it back.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/runs/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["run"]["id"], id);

    // List runs.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/runs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["runs"].as_array().unwrap().len(), 1);

    // Cancel it.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/runs/{id}/cancel"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // It is now cancelled, and the trace endpoint works (empty — no steps appended).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/runs/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(json_body(resp).await["run"]["status"], "cancelled");

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/runs/{id}/trace"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["steps"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn tool_gateway_register_and_list_via_rest() {
    let app = engine_router().await;

    // Register a downstream MCP server.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tools")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"github","url":"http://localhost:9001"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["status"], "registered");

    // List it back.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/tools")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let servers = json_body(resp).await;
    let servers = servers["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0]["name"], "github");
}

#[tokio::test]
async fn webhook_fires_trigger_into_run() {
    let app = engine_router().await;

    // Register a catch-all event trigger.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/triggers")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"any","source":"*","event_type":"*","agent_id":"catch"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // A webhook ingests an event, which fires the trigger → starts a run.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhook/myapp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"event_type":"thing","data":{"x":1}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert!(
        !body["triggered_runs"].as_array().unwrap().is_empty(),
        "webhook should have fired the catch-all trigger: {body}"
    );

    // The fired run exists.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/runs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(!json_body(resp).await["runs"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn ingest_then_query() {
    let app = engine_router().await;

    // Ingest events
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"source":"test","events":[{"event_type":"click"},{"event_type":"view"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ingested"], 2);

    // Query events back
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT count(*)::VARCHAR as cnt FROM episodic WHERE source='test'"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["rows"][0]["cnt"], "2");
}

#[tokio::test]
async fn state_set_then_get() {
    let app = engine_router().await;

    // Set state
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/state/bot-1/mood")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#""happy""#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["version"], 1);

    // Get state back
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/state/bot-1/mood")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["value"], "happy");
    assert_eq!(json["version"], 1);
}

#[tokio::test]
async fn mcp_initialize() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["jsonrpc"], "2.0");
    assert!(json["result"]["serverInfo"]["name"].as_str().is_some());
}

#[tokio::test]
async fn mcp_tools_list() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tools = json["result"]["tools"].as_array().unwrap();
    assert!(tools.len() >= 5);
}

#[tokio::test]
async fn mcp_tools_call_query() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 42::VARCHAR as answer"}}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("42"));
}

#[tokio::test]
async fn webhook_github_push() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhook/github")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"action":"completed","commits":[{"id":"abc"}],"repository":{"full_name":"org/repo"},"sender":{"login":"dev"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["source"], "github");
    assert_eq!(json["ingested"], 1);
}

// ── Session lifecycle ───────────────────────────────────────────────

#[tokio::test]
async fn session_lifecycle() {
    let app = engine_router().await;

    // Start session
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"session_id":"s1","agent_id":"bot-1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "started");

    // End session
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions/s1/end")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"summary":"test conversation"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Recall session
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions/s1/recall")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 0);
}

// ── Schema introspection ────────────────────────────────────────────

#[tokio::test]
async fn schema_sources_after_ingest() {
    let app = engine_router().await;

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"source":"schema-test","events":[{"event_type":"ping"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/schema/sources")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sources = json["sources"].as_array().unwrap();
    assert!(sources.iter().any(|s| s.as_str() == Some("schema-test")));
}

// ── MCP tools ───────────────────────────────────────────────────────

#[tokio::test]
async fn mcp_tools_list_has_all_tools() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tools = json["result"]["tools"].as_array().unwrap();
    assert_eq!(
        tools.len(),
        17,
        "expected 17 tools (6 core + 3 session + 6 memory + 2 graph)"
    );

    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"add_memory"));
    assert!(names.contains(&"search_memory"));
    assert!(names.contains(&"link_memory"));
    assert!(names.contains(&"graph_neighbors"));

    for tool in tools {
        assert!(
            tool["inputSchema"].is_object(),
            "tool {} missing inputSchema",
            tool["name"]
        );
    }
}

/// Fully in-memory router so cognition tests are isolated from `./data`.
async fn memory_router() -> axum::Router {
    let mut config = CoreConfig::default();
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    let engine = Arc::new(StrataEngine::new(config).await.unwrap());
    strata_gateway::rest::router_with_engine(engine)
}

async fn post_json(app: &axum::Router, uri: &str, body: &str) -> serde_json::Value {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "POST {uri}");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn memory_lifecycle_via_rest() {
    let app = memory_router().await;

    // Add an initial memory with a subject (enables contradiction resolution).
    let added = post_json(
        &app,
        "/api/v1/memories",
        r#"{"content":"favorite color is blue","subject":"favorite_color","user_id":"alice"}"#,
    )
    .await;
    assert_eq!(added["outcome"], "inserted");
    let first_id = added["memory"]["id"].as_str().unwrap().to_string();

    // A contradicting fact supersedes the old one.
    let superseded = post_json(
        &app,
        "/api/v1/memories",
        r#"{"content":"favorite color is green","subject":"favorite_color","user_id":"alice"}"#,
    )
    .await;
    assert_eq!(superseded["outcome"], "superseded");

    // Only the latest is active.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/memories?user_id=alice")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(list["count"], 1);
    assert_eq!(list["memories"][0]["content"], "favorite color is green");

    // History of the original memory shows both versions.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/memories/{first_id}/history"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let history: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(history["count"], 2);

    // Scoped search (recency fallback, no embedding provider) returns the active memory.
    let search = post_json(
        &app,
        "/api/v1/memories/search",
        r#"{"query":"colour","user_id":"alice"}"#,
    )
    .await;
    assert_eq!(search["count"], 1);
    assert_eq!(
        search["results"][0]["memory"]["content"],
        "favorite color is green"
    );
}

#[tokio::test]
async fn mcp_add_memory_tool_dispatch() {
    let app = memory_router().await;
    let result = post_json(
        &app,
        "/mcp",
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_memory","arguments":{"content":"alice likes espresso","user_id":"alice"}}}"#,
    )
    .await;
    let text = result["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("inserted"), "unexpected tool result: {text}");
}

#[tokio::test]
async fn mcp_session_tool_dispatch() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"start_session","arguments":{"session_id":"mcp-s1","agent_id":"mcp-bot"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let text = json["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("started"), "expected 'started' in: {text}");
}

// ── Batch limit ─────────────────────────────────────────────────────

#[tokio::test]
async fn ingest_batch_limit_rejects_oversize() {
    let app = engine_router().await;

    let events: Vec<serde_json::Value> = (0..10_001)
        .map(|i| serde_json::json!({"event_type": "test", "i": i}))
        .collect();
    let body_json = serde_json::json!({"source": "test", "events": events});

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&body_json).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "BATCH_TOO_LARGE");
}

// ── Retention policies ──────────────────────────────────────────────

#[tokio::test]
async fn retention_policies_crud() {
    let app = engine_router().await;

    // Set a policy
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/admin/retention/policies")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"source":"logs","retention_days":30}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Read policies
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/admin/retention/policies")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let policies = json["policies"].as_array().unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0]["source"], "logs");
    assert_eq!(policies[0]["retention_days"], 30);
}
