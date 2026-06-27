//! Multi-group sharding integration test.
//!
//! Builds a `ShardedCluster` from 2 independent single-node Raft groups (each its own engine) and
//! proves that writes are routed by key to the owning shard's Raft group — the foundation for
//! horizontal write scaling (each shard has its own leader, so throughput scales with shard count).

use std::sync::Arc;
use std::time::Duration;

use strata_cluster::raft::types::AppRequest;
use strata_cluster::{ClusterConfig, ClusterCoordinator, ShardedCluster};
use strata_core::StrataEngine;

async fn inmem_engine() -> Arc<StrataEngine> {
    let mut c = strata_core::CoreConfig::default();
    c.memory.episodic.db_path = ":memory:".into();
    c.memory.state.db_path = ":memory:".into();
    c.memory.cognition.db_path = ":memory:".into();
    Arc::new(StrataEngine::new(c).await.unwrap())
}

/// A single-node Raft group (no peers → initializes immediately).
async fn single_node(engine: Arc<StrataEngine>) -> ClusterCoordinator {
    let config = ClusterConfig {
        enabled: true,
        node_id: 1,
        listen: "127.0.0.1:0".into(),
        peers: vec![],
        data_dir: ":memory:".into(),
        secret: None,
        tls: None,
        shards: 1,
    };
    let mut coord = ClusterCoordinator::new(config);
    coord.start_raft(engine).await.unwrap();
    coord
}

async fn await_leader(coord: &ClusterCoordinator) {
    for _ in 0..100 {
        if coord.is_leader() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("single-node shard did not become leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_route_to_owning_shard() {
    let e0 = inmem_engine().await;
    let e1 = inmem_engine().await;
    let c0 = single_node(e0.clone()).await;
    let c1 = single_node(e1.clone()).await;
    await_leader(&c0).await;
    await_leader(&c1).await;

    let cluster = ShardedCluster::new(vec![c0, c1]);
    assert_eq!(cluster.shards(), 2);

    // Write one event per key; track which shard each routes to.
    let mut expected = [0u64, 0u64];
    let keys: Vec<String> = (0..12).map(|i| format!("tenant-{i}")).collect();
    for k in &keys {
        expected[cluster.shard_for(k)] += 1;
        let ev = strata_core::memory::episodic::Event::new("s", "e", serde_json::json!({}));
        cluster
            .client_write(
                k,
                AppRequest::Ingest {
                    events: vec![ev],
                    tenant: None,
                },
            )
            .await
            .unwrap();
    }

    // Each shard's engine holds exactly the events routed to it (routing + isolation).
    assert_eq!(e0.event_count().await.unwrap(), expected[0]);
    assert_eq!(e1.event_count().await.unwrap(), expected[1]);
    assert_eq!(expected[0] + expected[1], 12);
    // With 12 keys over a balanced ring, both shards should carry load (proves real partitioning).
    assert!(
        expected[0] > 0 && expected[1] > 0,
        "load did not split across shards"
    );

    cluster.shutdown().await;
}
