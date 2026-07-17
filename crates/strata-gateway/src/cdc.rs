//! Outbound CDC sink — mirror memory lifecycle changes to a downstream system.
//!
//! When `gateway.cdc_sink_url` is set, every memory change (upserted / superseded / expired) emitted
//! on [`StrataEngine::memory_subscribe`](strata_core::StrataEngine::memory_subscribe) is POSTed as
//! JSON to that URL. This is the stream to feed a downstream search index, warehouse, or event bus.
//!
//! Delivery semantics:
//! - **Leader-gated in cluster mode**: the change stream fires on *every* node's Raft apply, so
//!   without gating N nodes would each deliver the same event. The sink only ships when this node is
//!   the leader (or when running single-node), giving at-least-once delivery from one emitter.
//! - **Best-effort with bounded retry**: each POST is retried a few times with backoff; a change
//!   that still fails is logged and dropped rather than blocking the stream (a slow sink must not
//!   stall memory writes). A lagging broadcast receiver likewise drops the oldest events.
//! - **Trusted endpoint**: the URL is operator configuration, not user input, so no SSRF guard is
//!   applied (unlike the MCP tool-gateway, whose targets are agent-controlled).

use std::sync::Arc;
use std::time::Duration;

use strata_core::StrataEngine;
use tokio::sync::RwLock;

use strata_cluster::ClusterCoordinator;

/// Spawn the outbound CDC sink task. No-op if `url` is empty. Returns immediately; the task runs
/// until the process exits.
pub fn spawn(
    engine: Arc<StrataEngine>,
    url: String,
    coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let mut rx = engine.memory_subscribe();
        tracing::info!(%url, "outbound CDC sink enabled");
        loop {
            let change = match rx.recv().await {
                Ok(c) => c,
                // Lagged: the sink fell behind the write rate; skip the dropped window and continue.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(dropped = n, "CDC sink lagged; some changes not delivered");
                    continue;
                }
                // Sender gone → engine shutting down.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };

            // Deliver from a single emitter: only the leader ships (every node sees the change).
            let is_leader = match &coordinator {
                Some(c) => c.read().await.is_leader(),
                None => true,
            };
            if !is_leader {
                continue;
            }

            deliver(&client, &url, &change).await;
        }
    });
}

/// POST one change with bounded retry + backoff. Best-effort: gives up after the last attempt.
async fn deliver(client: &reqwest::Client, url: &str, change: &strata_core::MemoryChange) {
    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        match client.post(url).json(change).send().await {
            Ok(resp) if resp.status().is_success() => return,
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    attempt,
                    id = %change.id,
                    "CDC sink returned non-success"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, id = %change.id, "CDC sink POST failed");
            }
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
    }
    tracing::warn!(id = %change.id, event = change.event, "CDC change dropped after retries");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    async fn inmem_engine() -> Arc<StrataEngine> {
        let mut c = strata_core::CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        Arc::new(StrataEngine::new(c).await.unwrap())
    }

    /// End-to-end: a memory_add on the engine is POSTed to the sink URL as a JSON change (single
    /// node → no coordinator → always "leader", so it ships).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sink_delivers_memory_change() {
        use axum::{extract::State, routing::post, Json, Router};

        // Mock sink: record every received change body.
        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/cdc",
                post(
                    |State(store): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                     Json(body): Json<serde_json::Value>| async move {
                        store.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(received.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let engine = inmem_engine().await;
        spawn(engine.clone(), format!("http://{addr}/cdc"), None);
        // Give the subscriber task a moment to attach before we emit.
        tokio::time::sleep(Duration::from_millis(100)).await;

        engine
            .memory_add(strata_core::memory::cognition::MemoryInput::new(
                strata_core::memory::cognition::MemoryScope::user("alice"),
                "alice likes tea",
            ))
            .await
            .unwrap();

        // Poll for delivery.
        let mut got = None;
        for _ in 0..50 {
            if let Some(v) = received.lock().unwrap().first().cloned() {
                got = Some(v);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let change = got.expect("CDC sink should have received a change");
        assert_eq!(change["event"], "upserted");
        assert_eq!(change["user_id"], "alice");
    }
}
