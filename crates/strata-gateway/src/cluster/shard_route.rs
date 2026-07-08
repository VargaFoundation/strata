//! Shard-routing middleware — routes each `/api/v1/*` request to its tenant's owning shard.
//!
//! When the fleet runs in sharded mode (`cluster.shards > 1`), each gateway pod belongs to one shard
//! and knows the base URL of every shard's HTTP gateway. A request is routed by **tenant** (a
//! tenant's data lives entirely on one shard — aligned with the per-tenant rebalancing primitive):
//! if this pod owns the tenant the request is served locally; otherwise it is reverse-proxied to the
//! owning shard. Reverse-proxy (not a redirect) because the existing leader-forward returns an
//! info-only 307 with no usable `Location`, so a cross-shard hop must actually reach the other shard.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use strata_cluster::ShardRouter;

use crate::auth::middleware::AuthContext;

/// Header marking a request as already cross-shard-forwarded (loop guard).
const FORWARD_MARKER: &str = "x-strata-shard-forwarded";

/// Where a request should be handled.
#[derive(Debug, PartialEq, Eq)]
pub enum ShardTarget {
    /// This pod owns the tenant — serve locally.
    Local,
    /// Reverse-proxy to this shard's base URL (no trailing slash).
    Forward(String),
    /// The owning shard has no configured base URL — fail safe (never serve the wrong shard's data).
    Unroutable,
}

/// Pure routing decision (no I/O) — the unit-testable core.
pub fn route_decision(
    tenant: &str,
    router: &ShardRouter,
    my_shard: usize,
    base_urls: &[String],
) -> ShardTarget {
    let owner = router.shard_for(tenant);
    if owner == my_shard {
        ShardTarget::Local
    } else {
        match base_urls.get(owner) {
            Some(url) => ShardTarget::Forward(url.trim_end_matches('/').to_string()),
            None => ShardTarget::Unroutable,
        }
    }
}

/// The key a request routes on. Normally the authenticated tenant, but tenant-deletion must route by
/// the **path** tenant (`DELETE /…/admin/tenants/{id}`) — otherwise it would delete on the caller's
/// shard, not the target tenant's (a correctness bug).
fn routing_key(req: &Request) -> String {
    if req.method() == Method::DELETE {
        let path = req.uri().path();
        if let Some(idx) = path.find("/admin/tenants/") {
            let rest = &path[idx + "/admin/tenants/".len()..];
            let id = rest.split('/').next().unwrap_or("");
            if !id.is_empty() {
                return id.to_string();
            }
        }
    }
    req.extensions()
        .get::<AuthContext>()
        .and_then(|c| c.tenant_id.clone())
        .unwrap_or_else(|| "default".to_string())
}

/// State for the shard-routing middleware (built once at startup).
#[derive(Clone)]
pub struct ShardRoutingState {
    pub router: Arc<ShardRouter>,
    pub my_shard: usize,
    pub base_urls: Arc<Vec<String>>,
    pub http: reqwest::Client,
    /// Value stamped into the forward marker so the destination can AUTHENTICATE it (the fleet's
    /// shared cluster secret). `None` → the destination re-counts the request (rate-limit not
    /// skipped), which is safe: only an authenticated marker may bypass rate-limiting.
    pub forward_secret: Option<Arc<String>>,
}

/// Hop-by-hop headers that must not be forwarded across a proxy.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

/// Middleware: serve locally if this pod owns the request's tenant, else reverse-proxy to the owner.
pub async fn route_to_owning_shard(
    State(state): State<ShardRoutingState>,
    req: Request,
    next: Next,
) -> Response {
    // Admin endpoints are cluster-wide concerns, not tenant data — serve them locally rather than
    // mis-routing to the caller's tenant shard. The one exception is tenant-deletion, which must
    // reach the target tenant's shard (handled by routing_key's path-tenant rule below).
    let path = req.uri().path();
    let is_delete_tenant = req.method() == Method::DELETE && path.contains("/admin/tenants/");
    if path.contains("/admin/") && !is_delete_tenant {
        return next.run(req).await;
    }

    let tenant = routing_key(&req);
    match route_decision(&tenant, &state.router, state.my_shard, &state.base_urls) {
        ShardTarget::Local => next.run(req).await,
        ShardTarget::Unroutable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "shard_unroutable", "tenant": tenant })),
        )
            .into_response(),
        ShardTarget::Forward(base) => {
            // A forwarded request that still isn't local here means shard_index/base_urls are
            // misconfigured across pods — refuse to re-proxy rather than loop.
            if req.headers().contains_key(FORWARD_MARKER) {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "error": "shard_proxy_loop" })),
                )
                    .into_response();
            }
            let marker = state.forward_secret.as_ref().map(|s| s.as_str());
            proxy(&state.http, &base, req, marker).await
        }
    }
}

/// Reverse-proxy `req` to `base` + its path/query, returning the upstream response. `marker` is the
/// value stamped into the forward marker (the shared secret) so the destination can authenticate it.
async fn proxy(
    client: &reqwest::Client,
    base: &str,
    req: Request,
    marker: Option<&str>,
) -> Response {
    let (parts, body) = req.into_parts();
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let url = format!("{base}{pq}");

    // Body bounded by the global 16 MB DefaultBodyLimit (applied outside this middleware).
    let bytes = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let method = match reqwest::Method::from_bytes(parts.method.as_str().as_bytes()) {
        Ok(m) => m,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Forward headers minus hop-by-hop / host / content-length; keep authorization (the destination
    // re-authenticates) and content-type.
    let mut fwd_headers = HeaderMap::new();
    for (name, value) in parts.headers.iter() {
        let n = name.as_str();
        if is_hop_by_hop(n)
            || n.eq_ignore_ascii_case("host")
            || n.eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        fwd_headers.insert(name.clone(), value.clone());
    }
    // Stamp the marker with the shared secret (falls back to "1" for the loop-guard when no secret
    // is configured or the secret isn't a valid header value — the destination then re-counts).
    let marker_val = HeaderValue::from_str(marker.unwrap_or("1"))
        .unwrap_or_else(|_| HeaderValue::from_static("1"));
    fwd_headers.insert(HeaderName::from_static(FORWARD_MARKER), marker_val);

    // The destination's intra-shard leader-forward may 307 a write that lands on a follower; retry a
    // few times so a subsequent connection (via the shard Service) reaches the leader.
    let mut last: Option<reqwest::Response> = None;
    for _ in 0..4 {
        let resp = match client
            .request(method.clone(), &url)
            .headers(fwd_headers.clone())
            .body(bytes.clone())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "error": "shard_proxy_failed", "detail": e.to_string() })),
                )
                    .into_response();
            }
        };
        if resp.status().as_u16() == 307 {
            last = Some(resp);
            continue;
        }
        return reqwest_to_axum(resp).await;
    }
    // Exhausted retries on 307 — surface the last one.
    match last {
        Some(resp) => reqwest_to_axum(resp).await,
        None => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn reqwest_to_axum(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = HeaderMap::new();
    for (name, value) in resp.headers().iter() {
        if is_hop_by_hop(name.as_str()) || name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            headers.insert(n, v);
        }
    }
    let body: Bytes = resp.bytes().await.unwrap_or_default();
    (status, headers, body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant_owned_by(router: &ShardRouter, shard: usize) -> String {
        (0..)
            .map(|i| format!("tenant-{i}"))
            .find(|t| router.shard_for(t) == shard)
            .unwrap()
    }

    #[test]
    fn local_when_owner_is_self_else_forward() {
        let router = ShardRouter::new(2, 128);
        let urls = vec!["http://s0:8432".to_string(), "http://s1:8432/".to_string()];
        let t0 = tenant_owned_by(&router, 0);
        assert_eq!(route_decision(&t0, &router, 0, &urls), ShardTarget::Local);
        // Same tenant, viewed from shard 1 → forward to shard 0's URL (trailing slash trimmed).
        assert_eq!(
            route_decision(&t0, &router, 1, &urls),
            ShardTarget::Forward("http://s0:8432".into())
        );
        let t1 = tenant_owned_by(&router, 1);
        assert_eq!(
            route_decision(&t1, &router, 0, &urls),
            ShardTarget::Forward("http://s1:8432".into())
        );
    }

    #[test]
    fn single_shard_is_always_local() {
        let router = ShardRouter::new(1, 128);
        assert_eq!(
            route_decision("anything", &router, 0, &["http://s0".into()]),
            ShardTarget::Local
        );
    }

    #[test]
    fn unroutable_when_owner_url_missing() {
        let router = ShardRouter::new(3, 128);
        let t2 = tenant_owned_by(&router, 2);
        // Only shard 0 has a URL configured → owner shard 2 is unroutable.
        assert_eq!(
            route_decision(&t2, &router, 0, &["http://s0".into()]),
            ShardTarget::Unroutable
        );
    }

    #[test]
    fn routing_key_uses_path_tenant_for_delete_tenant() {
        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/v1/admin/tenants/tenant-xyz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(routing_key(&req), "tenant-xyz");
    }

    #[test]
    fn routing_key_defaults_to_default_without_auth() {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/memories")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(routing_key(&req), "default");
    }

    #[test]
    fn hop_by_hop_detection() {
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("transfer-encoding"));
        assert!(!is_hop_by_hop("authorization"));
        assert!(!is_hop_by_hop("content-type"));
    }

    #[tokio::test]
    async fn admin_paths_are_served_locally_not_proxied() {
        use axum::body::Body;
        use tower::ServiceExt;

        // base_urls point at an unreachable shard-1 — if admin were proxied this would fail/hang.
        let state = ShardRoutingState {
            router: Arc::new(ShardRouter::new(2, 128)),
            my_shard: 0,
            base_urls: Arc::new(vec![
                "http://unused".into(),
                "http://127.0.0.1:9".into(), // discard port — would fail if proxied
            ]),
            http: reqwest::Client::new(),
            forward_secret: None,
        };
        let app = axum::Router::new()
            .route(
                "/admin/audit",
                axum::routing::get(|| async { "LOCAL_ADMIN" }),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                state,
                route_to_owning_shard,
            ));
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            &body[..],
            b"LOCAL_ADMIN",
            "admin path must be served locally"
        );
    }

    async fn inmem_engine() -> std::sync::Arc<strata_core::StrataEngine> {
        let mut c = strata_core::CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        std::sync::Arc::new(strata_core::StrataEngine::new(c).await.unwrap())
    }

    /// End-to-end cross-pod rebalance (single process, real socket): seed a tenant on shard A, POST
    /// `/admin/rebalance` to A, and verify the tenant's events + memory + state landed on shard B and
    /// were erased from A. Exercises export → HTTP import → delete across two real gateways.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebalance_moves_tenant_across_pods() {
        use strata_core::memory::cognition::{MemoryInput, MemoryScope};
        use tower::ServiceExt;

        // Shard B: a real gateway on a real port.
        let engine_b = inmem_engine().await;
        let router_b = crate::rest::router_with_engine(engine_b.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router_b).await.unwrap() });

        // Shard A: router with shard routing, base_urls[1] → shard B.
        let engine_a = inmem_engine().await;
        let shard_state = ShardRoutingState {
            router: Arc::new(ShardRouter::new(2, 128)),
            my_shard: 0,
            base_urls: Arc::new(vec![
                "http://127.0.0.1:1".into(),
                format!("http://{addr_b}"),
            ]),
            http: reqwest::Client::new(),
            forward_secret: None,
        };
        let router_a = crate::rest::router_with_engine_and_auth(
            engine_a.clone(),
            None,
            None,
            Some(shard_state),
            &crate::server::GatewayConfig::default(),
        );

        // Seed tenant "t" on shard A: an event, a memory, a state key.
        let ta = strata_core::config::TenantContext::new("t");
        engine_a
            .ingest_for_tenant(
                vec![strata_core::memory::episodic::Event::new(
                    "s",
                    "e",
                    serde_json::json!({"x": 1}),
                )],
                &ta,
            )
            .await
            .unwrap();
        engine_a
            .memory_add(MemoryInput::new(MemoryScope::tenant("t"), "fact"))
            .await
            .unwrap();
        engine_a
            .state_set_for_tenant("t", "bot", "k", serde_json::json!("v"))
            .await
            .unwrap();

        // Rebalance tenant "t" from shard 0 → shard 1.
        let resp = router_a
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/admin/rebalance")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&serde_json::json!({"tenant": "t", "target_shard": 1}))
                            .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "rebalance should succeed");

        // Tenant data is now on shard B …
        assert_eq!(
            engine_b
                .memory_all(&MemoryScope::tenant("t"), 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine_b
                .state_get_for_tenant("t", "bot", "k")
                .await
                .unwrap()
                .map(|e| e.value),
            Some(serde_json::json!("v"))
        );
        let ev_b = engine_b
            .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "t")
            .await
            .unwrap();
        assert_eq!(ev_b[0]["c"], "1");
        // … and erased from shard A.
        assert_eq!(
            engine_a
                .memory_all(&MemoryScope::tenant("t"), 10)
                .await
                .unwrap()
                .len(),
            0
        );
    }

    /// End-to-end (single process, real socket): a request for a tenant owned by another shard is
    /// reverse-proxied to that shard's URL; a request for a local tenant is served locally. Proves
    /// the proxy plumbing without a k8s cluster. Uses the `DELETE /admin/tenants/{id}` path so the
    /// routing key is controllable (the path tenant) without auth.
    #[tokio::test]
    async fn proxies_cross_shard_and_serves_local() {
        use axum::body::Body;
        use tower::ServiceExt;

        // Stand up a downstream "shard 1" server that marks every response.
        let downstream = axum::Router::new().fallback(|| async { "FROM_SHARD_1" });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, downstream).await.unwrap() });

        let router = ShardRouter::new(2, 128);
        let t0 = tenant_owned_by(&router, 0);
        let t1 = tenant_owned_by(&router, 1);

        let state = ShardRoutingState {
            router: Arc::new(router),
            my_shard: 0, // we are shard 0
            base_urls: Arc::new(vec![
                "http://unused-shard-0".into(),
                format!("http://{addr}"), // shard 1 = the downstream
            ]),
            http: reqwest::Client::new(),
            forward_secret: None,
        };

        let app = axum::Router::new()
            .route(
                "/admin/tenants/{id}",
                axum::routing::delete(|| async { "LOCAL" }),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                state,
                route_to_owning_shard,
            ));

        // Tenant owned by shard 1 → reverse-proxied to the downstream.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/admin/tenants/{t1}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            &body[..],
            b"FROM_SHARD_1",
            "cross-shard request was not proxied"
        );

        // Tenant owned by shard 0 → served locally.
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/admin/tenants/{t0}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], b"LOCAL", "local tenant should not be proxied");
    }
}
