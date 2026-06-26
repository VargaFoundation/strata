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

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
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
    fn get(&self, id: NodeId) -> RaftHandle {
        self.nodes.lock().get(&id).expect("node registered").clone()
    }
}

/// A network connection to one target node — dispatches each RPC straight to its Raft handler.
struct Conn {
    router: Router,
    target: NodeId,
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.router
            .get(self.target)
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.router
            .get(self.target)
            .install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _o: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.router
            .get(self.target)
            .vote(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
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
