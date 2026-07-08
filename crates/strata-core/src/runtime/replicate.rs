//! Replication hook for the agent-run ledger.
//!
//! The in-process agent driver (`run_agent`) writes run records and step events. On a single node
//! those go straight to the local stores. In a cluster they must go through the Raft log so a run
//! started via `/agents/run` — and its full step trace — replicate and survive leader failover,
//! exactly like the REST `POST /runs` path already does.
//!
//! `strata-core` cannot depend on the cluster layer, so it defines this trait and the cluster
//! implements it (mapping each call to `coordinator.client_write`). The committed apply performs the
//! actual local store write on every node. The gateway injects an implementation via
//! [`crate::StrataEngine::set_run_replicator`]; absent it, the engine writes locally.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{Run, RunPatch};
use crate::memory::episodic::Event;

/// Routes agent-run ledger writes through consensus. All values are materialized by the caller
/// (ids, timestamps already fixed) so the committed apply is deterministic on every node.
#[async_trait]
pub trait RunReplicator: Send + Sync {
    /// Replicate a run creation (→ `AppRequest::RunCreate`).
    async fn replicate_run_create(&self, run: &Run) -> crate::Result<()>;
    /// Replicate a run patch with a leader-supplied `updated_at` (→ `AppRequest::RunUpdate`).
    async fn replicate_run_update(
        &self,
        id: Uuid,
        patch: &RunPatch,
        updated_at: DateTime<Utc>,
    ) -> crate::Result<()>;
    /// Replicate a fully-formed step event (→ `AppRequest::Ingest`).
    async fn replicate_step(&self, event: Event) -> crate::Result<()>;
    /// Replicate a state write made by the driver, e.g. a HITL approval key (→ `AppRequest::StateSet`)
    /// so it survives failover. Uses the non-replicating `state_set` at apply time (no loop).
    async fn replicate_state_set(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> crate::Result<()>;

    /// Whether this node is currently the cluster leader — a cheap LOCAL metric read, no consensus
    /// round-trip. Default `true` (single-node / no replicator). Lets the driver stop a stale
    /// ex-leader before it executes a side-effecting tool during a partition.
    async fn is_leader(&self) -> bool {
        true
    }
}
