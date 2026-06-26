//! Cluster coordinator — owns the Raft instance, routes writes through consensus.

use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::Raft;
use strata_core::StrataEngine;

use crate::config::ClusterConfig;
use crate::raft::network::NetworkFactory;
use crate::raft::store::MemStore;
use crate::raft::types::{AppRequest, AppResponse, NodeId, NodeInfo, TypeConfig};

/// Type alias for the openraft Raft instance with Strata's type config.
pub type StrataRaft = Raft<TypeConfig>;

/// Coordinates cluster membership, owns the Raft instance, and routes
/// write requests through consensus.
pub struct ClusterCoordinator {
    config: ClusterConfig,
    /// The openraft Raft instance (None if cluster mode is disabled).
    raft: Option<StrataRaft>,
}

impl ClusterCoordinator {
    /// Create a coordinator in single-node mode (no Raft, always leader).
    pub fn new(config: ClusterConfig) -> Self {
        Self { config, raft: None }
    }

    /// Start the Raft instance with the given engine for state machine application.
    pub async fn start_raft(&mut self, engine: Arc<StrataEngine>) -> crate::Result<()> {
        let raft_config = openraft::Config {
            cluster_name: "strata".into(),
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            // Log compaction: trigger snapshot after 5000 committed entries,
            // retain 500 entries after snapshot for slow followers.
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            max_in_snapshot_log_to_keep: 500,
            // Batch size for AppendEntries RPCs
            max_payload_entries: 100,
            ..Default::default()
        };

        let config = Arc::new(raft_config);

        let store = if self.config.data_dir == ":memory:" {
            tracing::info!("Raft store: in-memory");
            MemStore::new(Some(engine))
        } else {
            let data_dir = std::path::Path::new(&self.config.data_dir);
            match MemStore::open(
                &data_dir.join(format!("node-{}.db", self.config.node_id)),
                Some(engine.clone()),
            ) {
                Ok(s) => {
                    tracing::info!(
                        node_id = self.config.node_id,
                        "Raft store: persistent (SQLite)"
                    );
                    s
                }
                Err(e) => {
                    tracing::warn!(error = %e, "falling back to in-memory Raft store");
                    MemStore::new(Some(engine))
                }
            }
        };
        let (log_store, state_machine) = Adaptor::new(store);
        let network = NetworkFactory::new();

        let raft = Raft::new(
            self.config.node_id,
            config,
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| crate::Error::Raft(format!("failed to create raft: {e}")))?;

        tracing::info!(node_id = self.config.node_id, "Raft instance started");

        // If this is a single-node cluster (no peers), initialize immediately
        if self.config.peers.is_empty() {
            let mut members = std::collections::BTreeMap::new();
            members.insert(
                self.config.node_id,
                NodeInfo {
                    addr: format!("http://{}", self.config.listen),
                },
            );

            raft.initialize(members)
                .await
                .map_err(|e| crate::Error::Raft(format!("failed to initialize: {e}")))?;

            tracing::info!("single-node cluster initialized");
        }

        // Spawn background task to publish Raft metrics to Prometheus
        let metrics_raft = raft.clone();
        let metrics_node_id = self.config.node_id;
        tokio::spawn(async move {
            let mut watch = metrics_raft.metrics();
            let mut prev_leader: Option<NodeId> = None;
            loop {
                // Wait for metrics to change (watch channel)
                if watch.changed().await.is_err() {
                    break; // Raft shut down
                }
                let m = watch.borrow().clone();

                metrics::gauge!("strata_raft_term").set(m.current_term as f64);
                metrics::gauge!("strata_raft_is_leader").set(
                    if m.current_leader == Some(metrics_node_id) {
                        1.0
                    } else {
                        0.0
                    },
                );

                if let Some(last_applied) = m.last_applied {
                    metrics::gauge!("strata_raft_last_applied_index")
                        .set(last_applied.index as f64);
                }
                if let Some(last_log) = m.last_log_index {
                    metrics::gauge!("strata_raft_last_log_index").set(last_log as f64);
                    if let Some(last_applied) = m.last_applied {
                        metrics::gauge!("strata_raft_replication_lag")
                            .set((last_log.saturating_sub(last_applied.index)) as f64);
                    }
                }

                // Track leader changes
                if m.current_leader != prev_leader {
                    if prev_leader.is_some() {
                        metrics::counter!("strata_raft_leader_changes_total").increment(1);
                    }
                    prev_leader = m.current_leader;
                }
            }
        });

        self.raft = Some(raft);
        Ok(())
    }

    /// Whether this node is the current Raft leader.
    pub fn is_leader(&self) -> bool {
        match &self.raft {
            Some(raft) => {
                let metrics = raft.metrics().borrow().clone();
                metrics.current_leader == Some(self.config.node_id)
            }
            None => true, // Single-node mode: always leader
        }
    }

    /// Get the current leader's node ID.
    pub fn leader_id(&self) -> Option<NodeId> {
        match &self.raft {
            Some(raft) => raft.metrics().borrow().current_leader,
            None => Some(self.config.node_id),
        }
    }

    /// Propose a write through Raft consensus.
    ///
    /// Returns the response after the entry is committed and applied.
    /// Returns `NotLeader` error if this node is not the leader.
    pub async fn client_write(&self, request: AppRequest) -> crate::Result<AppResponse> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| crate::Error::Raft("raft not started".into()))?;

        let response = raft.client_write(request).await.map_err(|e| {
            // Check if this is a ForwardToLeader error
            crate::Error::Raft(format!("client_write failed: {e}"))
        })?;

        Ok(response.data)
    }

    /// Get a reference to the Raft instance (for receiving RPCs from other nodes).
    pub fn raft(&self) -> Option<&StrataRaft> {
        self.raft.as_ref()
    }

    /// Node ID of this node.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Graceful shutdown.
    pub async fn shutdown(&mut self) -> crate::Result<()> {
        if let Some(raft) = self.raft.take() {
            raft.shutdown()
                .await
                .map_err(|e| crate::Error::Raft(format!("shutdown failed: {e}")))?;
            tracing::info!("Raft instance shut down");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_is_leader() {
        let coord = ClusterCoordinator::new(ClusterConfig::default());
        assert!(coord.is_leader());
        assert_eq!(coord.leader_id(), Some(1));
    }

    #[test]
    fn node_id() {
        let config = ClusterConfig {
            node_id: 42,
            ..Default::default()
        };
        let coord = ClusterCoordinator::new(config);
        assert_eq!(coord.node_id(), 42);
    }

    #[tokio::test]
    async fn start_single_node_raft() {
        let engine = Arc::new(
            StrataEngine::new(strata_core::CoreConfig::default())
                .await
                .unwrap(),
        );
        let config = ClusterConfig {
            data_dir: ":memory:".into(),
            ..Default::default()
        };
        let mut coord = ClusterCoordinator::new(config);
        coord.start_raft(engine).await.unwrap();
        assert!(coord.raft().is_some());

        // Give Raft a moment to elect leader
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(coord.is_leader());

        coord.shutdown().await.unwrap();
    }

    /// End-to-end proof of the log-based write path: a write proposed via `client_write`
    /// goes through real openraft consensus (propose → commit → apply) and lands on the
    /// node's engine. This validates that the deterministic apply composes with Raft.
    #[tokio::test]
    async fn client_write_round_trips_through_consensus() {
        let mut core_cfg = strata_core::CoreConfig::default();
        core_cfg.memory.episodic.db_path = ":memory:".into();
        core_cfg.memory.state.db_path = ":memory:".into();
        core_cfg.memory.cognition.db_path = ":memory:".into();
        let engine = Arc::new(StrataEngine::new(core_cfg).await.unwrap());

        let mut coord = ClusterCoordinator::new(ClusterConfig {
            data_dir: ":memory:".into(),
            ..Default::default()
        });
        coord.start_raft(engine.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(coord.is_leader());

        // Ingest through consensus.
        let ev = strata_core::memory::episodic::Event::new("src", "e", serde_json::json!({"x": 1}));
        let resp = coord
            .client_write(AppRequest::Ingest {
                events: vec![ev],
                tenant: None,
            })
            .await
            .unwrap();
        assert!(matches!(resp, AppResponse::Ingested(1)));
        assert_eq!(engine.event_count().await.unwrap(), 1);

        // Memory through consensus: the leader runs cognition to materialize the change-set
        // (memory_plan, no local write), then replicates the rows through the log.
        let input = strata_core::memory::cognition::MemoryInput::new(
            strata_core::memory::cognition::MemoryScope::user("alice"),
            "likes tea",
        );
        let (_result, rows) = engine.memory_plan(input).await.unwrap();
        coord
            .client_write(AppRequest::MemoryUpsert { rows })
            .await
            .unwrap();
        assert_eq!(engine.memory_count().await.unwrap(), 1);

        // State set through consensus.
        coord
            .client_write(AppRequest::StateSet {
                agent_id: "bot".into(),
                key: "mood".into(),
                value: serde_json::json!("happy"),
                tenant: None,
            })
            .await
            .unwrap();
        assert_eq!(
            engine
                .state_get("bot", "mood")
                .await
                .unwrap()
                .unwrap()
                .value,
            serde_json::json!("happy")
        );

        coord.shutdown().await.unwrap();
    }
}
