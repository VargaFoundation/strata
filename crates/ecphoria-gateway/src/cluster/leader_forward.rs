//! Leader forwarding middleware — redirects writes to the Raft leader.
//!
//! In a Raft cluster, only the leader can accept writes. This middleware
//! checks leadership status and returns a redirect if this node is a follower.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ecphoria_cluster::ClusterCoordinator;
use tokio::sync::RwLock;

/// Shared cluster state for the leader-forwarding middleware.
#[derive(Clone)]
pub struct ClusterState {
    pub coordinator: Arc<RwLock<ClusterCoordinator>>,
}

/// Axum middleware that checks if this node is the Raft leader.
///
/// - For read requests (GET), passes through to the local engine (follower reads).
/// - For write requests (POST, PUT, DELETE), checks leadership:
///   - If leader: passes through.
///   - If follower: returns 307 Temporary Redirect with the leader's address.
///   - If no leader known: returns 503 Service Unavailable.
pub async fn require_leader_for_writes(
    State(state): State<ClusterState>,
    req: Request,
    next: Next,
) -> Response {
    // Reads are always served locally (C6: follower reads)
    if req.method() == axum::http::Method::GET {
        return next.run(req).await;
    }

    // Writes need to go to the leader
    let coordinator = state.coordinator.read().await;

    if coordinator.is_leader() {
        drop(coordinator);
        return next.run(req).await;
    }

    // Not the leader — redirect to leader if known
    match coordinator.leader_id() {
        Some(leader_id) => {
            let body = serde_json::json!({
                "error": "not_leader",
                "leader_id": leader_id,
                "message": "This node is not the leader. Retry on the leader node.",
            });
            (StatusCode::TEMPORARY_REDIRECT, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "error": "no_leader",
                "message": "No leader elected yet. Retry later.",
            });
            (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ClusterState>();
    }
}
