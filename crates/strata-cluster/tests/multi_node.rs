//! Multi-node Raft integration test.
//!
//! Stands up a **real 3-node openraft cluster** — each node with its own `StrataEngine` +
//! `MemStore` state machine — wired through an in-process network (so the test is deterministic
//! and port-free; the HTTP `NetworkClient` transport is covered by the single-node + unit tests).
//! Proves that a write proposed on the leader is committed via quorum and **converges on every
//! node's engine**, which is the property real HA depends on.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::{Config, Raft, RaftNetwork, RaftNetworkFactory};
use parking_lot::Mutex;

use strata_cluster::raft::store::MemStore;
use strata_cluster::raft::types::{AppRequest, NodeId, NodeInfo, TypeConfig};
use strata_core::StrataEngine;

type RaftHandle = Raft<TypeConfig>;

/// Shared registry mapping node id → its Raft handle, used by the in-process network to deliver
/// RPCs directly to the target node (no sockets).
#[derive(Clone, Default)]
struct Router {
    nodes: Arc<Mutex<BTreeMap<NodeId, RaftHandle>>>,
}

impl Router {
    fn register(&self, id: NodeId, raft: RaftHandle) {
        self.nodes.lock().insert(id, raft);
    }
    /// Look up a target node. `None` if it isn't registered yet — during bootstrap a node may
    /// dial a peer that hasn't started; the caller turns that into a retryable `Unreachable`.
    fn get(&self, id: NodeId) -> Option<RaftHandle> {
        self.nodes.lock().get(&id).cloned()
    }
}

/// A network connection to one target node — dispatches each RPC straight to its Raft handler.
struct Conn {
    router: Router,
    target: NodeId,
}

fn not_registered(target: NodeId) -> Unreachable {
    Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!("node {target} not registered yet"),
    ))
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        let raft = self
            .router
            .get(self.target)
            .ok_or_else(|| RPCError::Unreachable(not_registered(self.target)))?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self
            .router
            .get(self.target)
            .ok_or_else(|| RPCError::Unreachable(not_registered(self.target)))?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _o: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        let raft = self
            .router
            .get(self.target)
            .ok_or_else(|| RPCError::Unreachable(not_registered(self.target)))?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

struct Factory {
    router: Router,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Conn;
    async fn new_client(&mut self, target: NodeId, _node: &NodeInfo) -> Conn {
        Conn {
            router: self.router.clone(),
            target,
        }
    }
}

async fn inmem_engine() -> Arc<StrataEngine> {
    let mut c = strata_core::CoreConfig::default();
    c.memory.episodic.db_path = ":memory:".into();
    c.memory.state.db_path = ":memory:".into();
    c.memory.cognition.db_path = ":memory:".into();
    Arc::new(StrataEngine::new(c).await.unwrap())
}

fn raft_config() -> Arc<Config> {
    Arc::new(Config {
        cluster_name: "strata-test".into(),
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    })
}

#[tokio::test]
async fn three_node_cluster_replicates_and_converges() {
    let router = Router::default();
    let mut engines: BTreeMap<NodeId, Arc<StrataEngine>> = BTreeMap::new();
    let mut rafts: BTreeMap<NodeId, RaftHandle> = BTreeMap::new();

    // Bring up 3 nodes, each with its own engine + state machine, sharing the in-process network.
    for id in 1..=3u64 {
        let engine = inmem_engine().await;
        let (log, sm) = Adaptor::new(MemStore::new(Some(engine.clone())));
        let raft = Raft::new(
            id,
            raft_config(),
            Factory {
                router: router.clone(),
            },
            log,
            sm,
        )
        .await
        .unwrap();
        router.register(id, raft.clone());
        engines.insert(id, engine);
        rafts.insert(id, raft);
    }

    // Bootstrap the cluster from node 1 with all three voters.
    let members: BTreeMap<NodeId, NodeInfo> = (1..=3u64)
        .map(|id| {
            (
                id,
                NodeInfo {
                    addr: format!("mem://{id}"),
                },
            )
        })
        .collect();
    rafts[&1].initialize(members).await.unwrap();

    // Wait for a leader to emerge.
    let mut leader = None;
    for _ in 0..100 {
        if let Some(l) = rafts[&1].metrics().borrow().current_leader {
            leader = Some(l);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let leader = leader.expect("a leader should be elected");
    assert!((1..=3).contains(&leader));

    // Propose an ingest + a state write on the leader (goes through real quorum commit).
    let ev = strata_core::memory::episodic::Event::new("src", "e", serde_json::json!({"x": 1}));
    let ev_id = ev.id;
    rafts[&leader]
        .client_write(AppRequest::Ingest {
            events: vec![ev],
            tenant: None,
        })
        .await
        .unwrap();
    rafts[&leader]
        .client_write(AppRequest::StateSet {
            agent_id: "bot".into(),
            key: "mood".into(),
            value: serde_json::json!("happy"),
            tenant: None,
        })
        .await
        .unwrap();

    // Every node's engine must converge to the committed state.
    for id in 1..=3u64 {
        let engine = &engines[&id];
        let mut converged = false;
        for _ in 0..100 {
            let ingested = engine.event_count().await.unwrap() == 1;
            let stated = engine
                .state_get("bot", "mood")
                .await
                .unwrap()
                .map(|e| e.value == serde_json::json!("happy"))
                .unwrap_or(false);
            if ingested && stated {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            converged,
            "node {id} did not converge to the committed state"
        );

        // And it's the SAME event id everywhere (deterministic apply).
        let rows = engine.query_sql("SELECT id FROM episodic").await.unwrap();
        assert_eq!(rows[0]["id"].as_str().unwrap(), ev_id.to_string());
    }

    for raft in rafts.into_values() {
        let _ = raft.shutdown().await;
    }
}

/// End-to-end deployment path: bring up 3 `ClusterCoordinator`s from **configuration only**
/// (`peers` = `id@addr` membership) and assert the cluster forms ITSELF — the lowest-id node
/// bootstraps via `start_raft_with_network`, no manual `initialize` — then a leader write
/// converges on every node. This is what a real 3-replica deployment does.
#[tokio::test]
async fn cluster_forms_from_config_via_coordinator() {
    use strata_cluster::{ClusterConfig, ClusterCoordinator};

    let router = Router::default();
    let peers: Vec<String> = (1..=3u64).map(|id| format!("{id}@mem://{id}")).collect();
    let mut engines: BTreeMap<NodeId, Arc<StrataEngine>> = BTreeMap::new();
    let mut coords: Vec<ClusterCoordinator> = Vec::new();

    for id in 1..=3u64 {
        let engine = inmem_engine().await;
        let config = ClusterConfig {
            enabled: true,
            node_id: id,
            listen: "0.0.0.0:9433".into(),
            peers: peers.clone(),
            data_dir: ":memory:".into(),
        };
        let mut coord = ClusterCoordinator::new(config);
        coord
            .start_raft_with_network(
                engine.clone(),
                Factory {
                    router: router.clone(),
                },
            )
            .await
            .unwrap();
        // Register this node so peers' bootstrap/election RPCs can reach it.
        router.register(id, coord.raft().unwrap().clone());
        engines.insert(id, engine);
        coords.push(coord);
    }

    // No manual initialize: the designated coordinator forms the cluster on its own. Wait for a
    // leader to emerge (bootstrap retries until all peers are registered/reachable).
    let mut elected = false;
    for _ in 0..200 {
        if coords.iter().any(|c| c.is_leader()) {
            elected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(elected, "cluster did not elect a leader from config");

    // Propose a write on whichever node is leader.
    let leader = coords
        .iter()
        .find(|c| c.is_leader())
        .expect("a leader coordinator");
    let ev = strata_core::memory::episodic::Event::new("src", "e", serde_json::json!({"y": 2}));
    leader
        .client_write(AppRequest::Ingest {
            events: vec![ev],
            tenant: None,
        })
        .await
        .unwrap();

    // Converges on every node's engine.
    for id in 1..=3u64 {
        let engine = &engines[&id];
        let mut converged = false;
        for _ in 0..100 {
            if engine.event_count().await.unwrap() == 1 {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            converged,
            "node {id} did not converge after config-driven formation"
        );
    }

    for mut c in coords {
        let _ = c.shutdown().await;
    }
}
