//! Cluster coordinator — owns the Raft instance, routes writes through consensus.

use std::sync::Arc;

use ecphoria_core::EcphoriaEngine;
use openraft::storage::Adaptor;
use openraft::Raft;

use crate::config::ClusterConfig;
use crate::raft::network::GrpcRaftNetworkFactory;
use crate::raft::server::RaftGrpcServer;
use crate::raft::store::MemStore;
use crate::raft::types::{AppRequest, AppResponse, NodeId, NodeInfo, TypeConfig};

/// Type alias for the openraft Raft instance with Ecphoria's type config.
pub type EcphoriaRaft = Raft<TypeConfig>;

/// Coordinates cluster membership, owns the Raft instance, and routes
/// write requests through consensus.
pub struct ClusterCoordinator {
    config: ClusterConfig,
    /// The openraft Raft instance (None if cluster mode is disabled).
    raft: Option<EcphoriaRaft>,
    /// Shutdown signal for the inter-node Raft gRPC server (multi-node only).
    raft_grpc_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ClusterCoordinator {
    /// Create a coordinator in single-node mode (no Raft, always leader).
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            raft: None,
            raft_grpc_shutdown: None,
        }
    }

    /// Consistent-hash router for this cluster's write shards (see [`crate::shard`]). Single-group
    /// clusters (`shards = 1`) route everything to shard 0; the accessor lets call sites compute a
    /// key's target shard now, ahead of multi-group wiring.
    pub fn shard_router(&self) -> crate::ShardRouter {
        crate::ShardRouter::new(self.config.shards, 128)
    }

    /// Number of write shards in this fleet.
    pub fn shards(&self) -> usize {
        self.config.shards
    }

    /// This pod's 0-based shard index.
    pub fn shard_index(&self) -> usize {
        self.config.shard_index
    }

    /// The fleet-shared cluster secret (also used to authenticate the internal shard-forward marker).
    pub fn secret(&self) -> Option<String> {
        self.config.secret.clone()
    }

    /// Base URLs of every shard's HTTP gateway, indexed by shard (empty/whitespace entries dropped).
    pub fn shard_base_urls(&self) -> Vec<String> {
        self.config
            .shard_base_urls
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    /// Start the Raft instance with the production gRPC network, and (in multi-node mode) serve
    /// this node's Raft instance to peers over gRPC on `cluster.listen`.
    pub async fn start_raft(&mut self, engine: Arc<EcphoriaEngine>) -> crate::Result<()> {
        let tls = match &self.config.tls {
            Some(t) => Some(crate::raft::tls::client_tls(t)?),
            None => None,
        };
        self.start_raft_with_network(
            engine,
            GrpcRaftNetworkFactory {
                secret: self.config.secret.clone(),
                tls,
            },
        )
        .await?;
        if !self.config.peers.is_empty() {
            self.start_raft_grpc_server()?;
        }
        Ok(())
    }

    /// Spawn the inter-node Raft gRPC server bound to `cluster.listen` (e.g. :9433 — the address
    /// peers actually dial). Single-node mode skips this (no peers contact this node).
    fn start_raft_grpc_server(&mut self) -> crate::Result<()> {
        let raft = self.raft.as_ref().expect("raft started").clone();
        let addr: std::net::SocketAddr = self.config.listen.parse().map_err(|e| {
            crate::Error::Coordination(format!(
                "invalid cluster listen '{}': {e}",
                self.config.listen
            ))
        })?;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let service =
            RaftGrpcServer::new(Arc::new(raft), self.config.secret.clone()).into_service();
        let mut server = tonic::transport::Server::builder();
        if let Some(t) = &self.config.tls {
            server = server
                .tls_config(crate::raft::tls::server_tls(t)?)
                .map_err(|e| crate::Error::Coordination(format!("Raft server TLS: {e}")))?;
        }
        tokio::spawn(async move {
            tracing::info!(%addr, "Raft gRPC server listening");
            let result = server
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
            if let Err(e) = result {
                tracing::error!(error = %e, "Raft gRPC server stopped with error");
            }
        });
        self.raft_grpc_shutdown = Some(tx);
        Ok(())
    }

    /// Start the Raft instance with a custom network factory (tests inject an in-process network).
    ///
    /// Forms the cluster from configuration:
    /// - **no peers** → single-node, initialized immediately;
    /// - **peers** (`id@addr` voter membership, including this node) → the lowest-id node is the
    ///   designated bootstrapper: it `initialize`s the full membership **once**, idempotently
    ///   (skips if already initialized — e.g. on restart) and retries until peers are reachable.
    ///   Every other node simply starts and is brought into the cluster by the leader.
    pub async fn start_raft_with_network<N>(
        &mut self,
        engine: Arc<EcphoriaEngine>,
        network: N,
    ) -> crate::Result<()>
    where
        N: openraft::RaftNetworkFactory<TypeConfig>,
    {
        let raft_config = openraft::Config {
            cluster_name: "ecphoria".into(),
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

        // Form the cluster from configuration.
        let members = self.parse_members()?;
        if self.config.peers.is_empty() {
            raft.initialize(members)
                .await
                .map_err(|e| crate::Error::Raft(format!("failed to initialize: {e}")))?;
            tracing::info!("single-node cluster initialized");
        } else {
            let min_id = *members.keys().next().expect("membership is non-empty");
            if self.config.node_id == min_id {
                tracing::info!(
                    node_id = min_id,
                    members = members.len(),
                    "designated bootstrap node — forming cluster"
                );
                let raft_bs = raft.clone();
                tokio::spawn(async move { bootstrap_cluster(raft_bs, members).await });
            } else {
                tracing::info!(
                    node_id = self.config.node_id,
                    "joining cluster — awaiting leader replication"
                );
            }
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

                metrics::gauge!("ecphoria_raft_term").set(m.current_term as f64);
                metrics::gauge!("ecphoria_raft_is_leader").set(
                    if m.current_leader == Some(metrics_node_id) {
                        1.0
                    } else {
                        0.0
                    },
                );

                if let Some(last_applied) = m.last_applied {
                    metrics::gauge!("ecphoria_raft_last_applied_index")
                        .set(last_applied.index as f64);
                }
                if let Some(last_log) = m.last_log_index {
                    metrics::gauge!("ecphoria_raft_last_log_index").set(last_log as f64);
                    if let Some(last_applied) = m.last_applied {
                        metrics::gauge!("ecphoria_raft_replication_lag")
                            .set((last_log.saturating_sub(last_applied.index)) as f64);
                    }
                }

                // Track leader changes
                if m.current_leader != prev_leader {
                    if prev_leader.is_some() {
                        metrics::counter!("ecphoria_raft_leader_changes_total").increment(1);
                    }
                    prev_leader = m.current_leader;
                }
            }
        });

        self.raft = Some(raft);
        Ok(())
    }

    /// Build the full voter membership from configuration.
    ///
    /// Empty peers → just this node (single-node). Otherwise each `peers` entry is `id@addr`
    /// (the complete voter set, including this node), e.g. `"2@http://ecphoria-1:9433"`.
    fn parse_members(&self) -> crate::Result<std::collections::BTreeMap<NodeId, NodeInfo>> {
        use std::collections::BTreeMap;
        let mut members = BTreeMap::new();
        if self.config.peers.is_empty() {
            members.insert(
                self.config.node_id,
                NodeInfo {
                    addr: normalize_addr(&self.config.listen),
                },
            );
            return Ok(members);
        }
        for entry in &self.config.peers {
            let (id_str, addr) = entry.split_once('@').ok_or_else(|| {
                crate::Error::Coordination(format!(
                    "cluster peer '{entry}' must be in 'id@addr' form (e.g. '2@http://ecphoria-1:9433')"
                ))
            })?;
            let id: NodeId = id_str.trim().parse().map_err(|_| {
                crate::Error::Coordination(format!("invalid node id in cluster peer '{entry}'"))
            })?;
            members.insert(
                id,
                NodeInfo {
                    addr: normalize_addr(addr.trim()),
                },
            );
        }
        if !members.contains_key(&self.config.node_id) {
            return Err(crate::Error::Coordination(format!(
                "cluster peers must include this node (id {}); peers={:?}",
                self.config.node_id, self.config.peers
            )));
        }
        Ok(members)
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
    pub fn raft(&self) -> Option<&EcphoriaRaft> {
        self.raft.as_ref()
    }

    /// Node ID of this node.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Graceful shutdown.
    pub async fn shutdown(&mut self) -> crate::Result<()> {
        // Stop accepting inter-node RPCs first.
        if let Some(tx) = self.raft_grpc_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(raft) = self.raft.take() {
            raft.shutdown()
                .await
                .map_err(|e| crate::Error::Raft(format!("shutdown failed: {e}")))?;
            tracing::info!("Raft instance shut down");
        }
        Ok(())
    }
}

/// Prefix a bare `host:port` with `http://` so the HTTP Raft transport can POST to it.
fn normalize_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Wraps the coordinator as a [`ecphoria_core::runtime::RunReplicator`] so the core agent-run driver
/// replicates its run/step writes through the Raft log (durable across failover) instead of writing
/// only on the leader. The committed apply performs the local store write on every node.
pub struct CoordinatorRunReplicator {
    coord: Arc<tokio::sync::RwLock<ClusterCoordinator>>,
}

impl CoordinatorRunReplicator {
    pub fn new(coord: Arc<tokio::sync::RwLock<ClusterCoordinator>>) -> Self {
        Self { coord }
    }
}

fn replicate_err(e: crate::Error) -> ecphoria_core::Error {
    ecphoria_core::Error::Ingest(format!("cluster replicate: {e}"))
}

#[async_trait::async_trait]
impl ecphoria_core::runtime::RunReplicator for CoordinatorRunReplicator {
    async fn is_leader(&self) -> bool {
        self.coord.read().await.is_leader()
    }

    async fn replicate_run_create(
        &self,
        run: &ecphoria_core::runtime::Run,
    ) -> ecphoria_core::Result<()> {
        self.coord
            .read()
            .await
            .client_write(crate::raft::types::AppRequest::RunCreate { run: run.clone() })
            .await
            .map(|_| ())
            .map_err(replicate_err)
    }

    async fn replicate_run_update(
        &self,
        id: uuid::Uuid,
        patch: &ecphoria_core::runtime::RunPatch,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> ecphoria_core::Result<()> {
        self.coord
            .read()
            .await
            .client_write(crate::raft::types::AppRequest::RunUpdate {
                id,
                patch: patch.clone(),
                updated_at,
            })
            .await
            .map(|_| ())
            .map_err(replicate_err)
    }

    async fn replicate_step(
        &self,
        event: ecphoria_core::memory::episodic::Event,
    ) -> ecphoria_core::Result<()> {
        self.coord
            .read()
            .await
            .client_write(crate::raft::types::AppRequest::Ingest {
                events: vec![event],
                tenant: None,
            })
            .await
            .map(|_| ())
            .map_err(replicate_err)
    }

    async fn replicate_state_set(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> ecphoria_core::Result<()> {
        self.coord
            .read()
            .await
            .client_write(crate::raft::types::AppRequest::StateSet {
                agent_id: agent_id.to_string(),
                key: key.to_string(),
                value,
                tenant: None,
            })
            .await
            .map(|_| ())
            .map_err(replicate_err)
    }
}

/// Bootstrap a multi-node cluster: `initialize` once, idempotently, retrying until a quorum of
/// peers is reachable. Safe to run on every start — a restart finds the cluster already
/// initialized (membership restored from the persisted log) and skips.
async fn bootstrap_cluster(
    raft: EcphoriaRaft,
    members: std::collections::BTreeMap<NodeId, NodeInfo>,
) {
    for attempt in 0..120u32 {
        if matches!(raft.is_initialized().await, Ok(true)) {
            tracing::info!("cluster already initialized — skipping bootstrap");
            return;
        }
        match raft.initialize(members.clone()).await {
            Ok(()) => {
                tracing::info!(members = members.len(), "multi-node cluster bootstrapped");
                return;
            }
            Err(e) => {
                let msg = e.to_string();
                // Another node (or a previous boot) already formed the cluster — done.
                if msg.contains("already") || msg.contains("initialized") {
                    tracing::info!("cluster already initialized (concurrent) — skipping");
                    return;
                }
                // Peers not reachable yet (quorum unavailable) — back off and retry.
                tracing::debug!(attempt, error = %msg, "cluster bootstrap retry — peers not ready");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
    tracing::error!("cluster bootstrap timed out — cluster not formed");
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
            EcphoriaEngine::new(ecphoria_core::CoreConfig::default())
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
        let mut core_cfg = ecphoria_core::CoreConfig::default();
        core_cfg.memory.episodic.db_path = ":memory:".into();
        core_cfg.memory.state.db_path = ":memory:".into();
        core_cfg.memory.cognition.db_path = ":memory:".into();
        let engine = Arc::new(EcphoriaEngine::new(core_cfg).await.unwrap());

        let mut coord = ClusterCoordinator::new(ClusterConfig {
            data_dir: ":memory:".into(),
            ..Default::default()
        });
        coord.start_raft(engine.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(coord.is_leader());

        // Ingest through consensus.
        let ev =
            ecphoria_core::memory::episodic::Event::new("src", "e", serde_json::json!({"x": 1}));
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
        let input = ecphoria_core::memory::cognition::MemoryInput::new(
            ecphoria_core::memory::cognition::MemoryScope::user("alice"),
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
