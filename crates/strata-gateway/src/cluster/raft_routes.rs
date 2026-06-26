//! Cluster admin HTTP endpoints (status + membership changes).
//!
//! The hot-path Raft RPCs (AppendEntries / Vote / InstallSnapshot) are served over **gRPC** by
//! `strata_cluster::raft::server::RaftGrpcServer` on the Raft port — see that module. These HTTP
//! routes are low-traffic admin operations only.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use strata_cluster::coordinator::StrataRaft;

/// Shared state for cluster admin handlers.
#[derive(Clone)]
pub struct RaftState {
    pub raft: Arc<StrataRaft>,
}

/// Request body for POST /cluster/add-learner.
#[derive(serde::Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: strata_cluster::raft::types::NodeId,
    pub addr: String,
}

/// POST /cluster/add-learner — Add a learner node to the Raft cluster.
pub async fn add_learner(
    State(state): State<RaftState>,
    Json(req): Json<AddLearnerRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let node_info = strata_cluster::raft::types::NodeInfo { addr: req.addr };
    state
        .raft
        .add_learner(req.node_id, node_info, true)
        .await
        .map(|resp| {
            Json(serde_json::json!({
                "log_id": resp.log_id().index,
                "membership": format!("{:?}", resp.membership()),
            }))
        })
        .map_err(|e| {
            tracing::warn!(error = %e, "add_learner failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// Request body for POST /cluster/change-membership.
#[derive(serde::Deserialize)]
pub struct ChangeMembershipRequest {
    pub members: std::collections::BTreeSet<strata_cluster::raft::types::NodeId>,
}

/// POST /cluster/change-membership — Promote learners to voters or update membership.
pub async fn change_membership(
    State(state): State<RaftState>,
    Json(req): Json<ChangeMembershipRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .raft
        .change_membership(req.members, false)
        .await
        .map(|resp| {
            Json(serde_json::json!({
                "log_id": resp.log_id().index,
                "membership": format!("{:?}", resp.membership()),
            }))
        })
        .map_err(|e| {
            tracing::warn!(error = %e, "change_membership failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// GET /cluster/status — Cluster health and Raft metrics
pub async fn cluster_status(State(state): State<RaftState>) -> Json<serde_json::Value> {
    let metrics = state.raft.metrics().borrow().clone();
    Json(serde_json::json!({
        "node_id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_leader": metrics.current_leader,
        "current_term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied.map(|id| id.index),
        "membership": format!("{:?}", metrics.membership_config),
    }))
}

/// Build the cluster admin router (status + membership). The hot-path Raft RPCs are served over
/// gRPC, not here.
pub fn raft_router(raft: Arc<StrataRaft>) -> axum::Router {
    let state = RaftState { raft };

    axum::Router::new()
        .route("/cluster/status", axum::routing::get(cluster_status))
        .route("/cluster/add-learner", axum::routing::post(add_learner))
        .route(
            "/cluster/change-membership",
            axum::routing::post(change_membership),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_state_is_clone() {
        // Can't test without a Raft instance, but verify the struct is Clone
        fn assert_clone<T: Clone>() {}
        assert_clone::<RaftState>();
    }
}
