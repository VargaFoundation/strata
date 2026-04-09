# strata-cluster

## Responsibility

Distributed mode. Implements Raft consensus (via openraft), data replication,
and node coordination. Phase 3 deliverable (M6-M9) — stub implementation initially.

## Internal Architecture

```
src/
  lib.rs           Re-exports
  error.rs         ClusterError
  config.rs        ClusterConfig
  coordinator.rs   ClusterCoordinator: leader election, request routing
  raft/
    types.rs       TypeConfig, NodeId, LogEntry, SnapshotData
    network.rs     RaftNetwork (HTTP-based)
    store.rs       RaftLogStore + RaftStateMachine
  replication/
    log_shipper.rs WAL segment shipping
    snapshot.rs    Snapshot transfer
```

## Testing

- `cargo test -p strata-cluster`
- In-memory Raft network for unit tests (no real TCP)
