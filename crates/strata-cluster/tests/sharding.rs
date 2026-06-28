//! Multi-group sharding integration test.
//!
//! Builds a `ShardedCluster` from 2 independent single-node Raft groups (each its own engine) and
//! proves that writes are routed by key to the owning shard's Raft group — the foundation for
//! horizontal write scaling (each shard has its own leader, so throughput scales with shard count).

use std::sync::Arc;
use std::time::Duration;

use strata_cluster::raft::types::AppRequest;
use strata_cluster::shard::ShardMove;
use strata_cluster::{ClusterConfig, ClusterCoordinator, ShardedCluster};
use strata_core::memory::cognition::{MemoryInput, MemoryScope};
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

    let cluster = ShardedCluster::new(vec![(c0, e0.clone()), (c1, e1.clone())]);
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

    // Cross-shard read: scatter-gather over both shards' engines returns every event.
    let all_rows = cluster.query_all("SELECT * FROM episodic").await.unwrap();
    assert_eq!(
        all_rows.len(),
        12,
        "cross-shard query must aggregate all shards"
    );

    // engine_for routes a key's reads to its owning shard.
    let key = "tenant-3";
    let owner = cluster.shard_for(key);
    let owner_engine = if owner == 0 { &e0 } else { &e1 };
    assert!(std::sync::Arc::ptr_eq(
        cluster.engine_for(key),
        owner_engine
    ));

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_moves_tenant_data_between_shards() {
    let e0 = inmem_engine().await;
    let e1 = inmem_engine().await;
    // Seed tenant-a's memories directly on shard 0.
    for i in 0..3 {
        e0.memory_add(MemoryInput::new(
            MemoryScope::tenant("tenant-a"),
            format!("fact {i}"),
        ))
        .await
        .unwrap();
    }
    let c0 = single_node(e0.clone()).await;
    let c1 = single_node(e1.clone()).await;
    await_leader(&c0).await;
    await_leader(&c1).await;
    let cluster = ShardedCluster::new(vec![(c0, e0.clone()), (c1, e1.clone())]);

    // Operator computed that tenant-a now belongs on shard 1 → execute the move.
    let moved = cluster
        .apply_moves(&[ShardMove {
            key: "tenant-a".into(),
            from: 0,
            to: 1,
        }])
        .await
        .unwrap();
    assert_eq!(moved, 3);
    assert_eq!(
        e0.export_tenant_memories("tenant-a").await.unwrap().len(),
        0
    );
    assert_eq!(
        e1.export_tenant_memories("tenant-a").await.unwrap().len(),
        3
    );

    cluster.shutdown().await;
}
