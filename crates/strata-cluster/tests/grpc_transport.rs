//! Real-socket gRPC transport test.
//!
//! Brings up a 3-node cluster on real `127.0.0.1:<port>` addresses using the **production**
//! path — `ClusterCoordinator::start_raft`, which wires the gRPC network factory AND serves each
//! node's Raft instance over gRPC on `cluster.listen`. Proves the cluster forms from config and a
//! leader write converges on every node **over real HTTP/2 sockets** (and that the Raft server
//! binds the address peers actually dial — the port the old HTTP transport got wrong).

use std::sync::Arc;
use std::time::Duration;

use strata_cluster::raft::types::AppRequest;
use strata_cluster::{ClusterConfig, ClusterCoordinator};
use strata_core::StrataEngine;

async fn inmem_engine() -> Arc<StrataEngine> {
    let mut c = strata_core::CoreConfig::default();
    c.memory.episodic.db_path = ":memory:".into();
    c.memory.state.db_path = ":memory:".into();
    c.memory.cognition.db_path = ":memory:".into();
    Arc::new(StrataEngine::new(c).await.unwrap())
}

/// Grab an ephemeral localhost port, then release it for the coordinator to bind.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_grpc_cluster_replicates_over_sockets() {
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    // Full voter membership as `id@http://addr` (incl. self), exactly like the Helm config.
    let peers: Vec<String> = ports
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{}@http://127.0.0.1:{}", i + 1, p))
        .collect();

    let mut engines = Vec::new();
    let mut coords: Vec<ClusterCoordinator> = Vec::new();
    for (i, port) in ports.iter().enumerate() {
        let engine = inmem_engine().await;
        let config = ClusterConfig {
            enabled: true,
            node_id: (i + 1) as u64,
            listen: format!("127.0.0.1:{port}"),
            peers: peers.clone(),
            data_dir: ":memory:".into(),
            // Exercise inter-node auth end-to-end: every node presents this Bearer token over gRPC.
            secret: Some("test-cluster-secret".into()),
            tls: None,
            shards: 1,
        };
        let mut coord = ClusterCoordinator::new(config);
        // Production path: gRPC network factory + gRPC server bound to cluster.listen.
        coord.start_raft(engine.clone()).await.unwrap();
        engines.push(engine);
        coords.push(coord);
    }

    // Wait for a leader to emerge over real gRPC (production timings: election ≤3s + bootstrap retry).
    let mut elected = false;
    for _ in 0..200 {
        if coords.iter().any(|c| c.is_leader()) {
            elected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(elected, "cluster did not elect a leader over gRPC");

    // Propose a write on the leader.
    let leader = coords
        .iter()
        .find(|c| c.is_leader())
        .expect("a leader coordinator");
    let ev = strata_core::memory::episodic::Event::new("grpc", "e", serde_json::json!({"z": 9}));
    leader
        .client_write(AppRequest::Ingest {
            events: vec![ev],
            tenant: None,
        })
        .await
        .unwrap();

    // The committed write must converge on every node's engine — over real sockets.
    for (i, engine) in engines.iter().enumerate() {
        let mut converged = false;
        for _ in 0..100 {
            if engine.event_count().await.unwrap() == 1 {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(converged, "node {} did not converge over gRPC", i + 1);
    }

    for mut c in coords {
        let _ = c.shutdown().await;
    }
}
