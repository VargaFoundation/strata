# strata-cluster

## Responsibility

Distributed mode. Implements Raft consensus via openraft v0.9, routes writes through
the leader, and provides cluster coordination. Each write (ingest, state_set, etc.)
is proposed as an `AppRequest` through Raft and applied to the local `StrataEngine`
upon commit.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| `TypeConfig` | **Working** | `declare_raft_types!` macro, AppRequest/AppResponse enums, NodeInfo, MessagePack serde |
| `MemStore` | **Working** | Full `RaftStorage` trait impl: log (BTreeMap), vote, state machine (applies to StrataEngine), snapshots |
| `NetworkClient` | **Working** | HTTP JSON POST for AppendEntries, Vote, InstallSnapshot RPCs |
| `NetworkFactory` | **Working** | Creates `NetworkClient` per target node with shared reqwest::Client |
| `ClusterCoordinator` | **Working** | Owns the `openraft::Raft` instance, `client_write()`, `is_leader()`, `leader_id()`, single-node init, graceful shutdown |
| `ClusterConfig` | **Working** | TOML deserialization, node_id, listen, peers |
| `ClusterCoordinator` (metrics) | **Working** | Background task publishing Raft metrics to Prometheus (term, is_leader, replication_lag, leader_changes) |
| `LogShipper` | Stub | WAL segment shipping between peers (not needed for basic Raft, uses AppendEntries) |
| `SnapshotManager` | **Working** | Binary pack/unpack of all 3 stores (DuckDB export + USearch index + metadata), build/install wired into RaftSnapshotBuilder |

## Internal Architecture

```
src/
  lib.rs           Re-exports (ClusterConfig, ClusterCoordinator, Error, Result)
  error.rs         ClusterError (Raft, Replication, Coordination, NotLeader, Core, Internal)
  config.rs        ClusterConfig (enabled, node_id, listen, peers)
  coordinator.rs   ClusterCoordinator: Raft lifecycle, client_write, leader detection
  raft/
    types.rs       TypeConfig, AppRequest, AppResponse, NodeInfo (openraft types)
    network.rs     NetworkClient + NetworkFactory (HTTP JSON transport)
    store.rs       MemStore: RaftStorage + RaftLogReader + RaftSnapshotBuilder
  replication/
    log_shipper.rs WAL segment shipping (stub)
    snapshot.rs    SnapshotManager: build/restore with binary pack/unpack format
```

## AppRequest Variants

All mutating operations are serialized as `AppRequest` through Raft:

| Variant | Description | Response |
|---------|-------------|----------|
| `Ingest { source, events }` | Ingest events into episodic + semantic | `Ingested(count)` |
| `StateSet { agent_id, key, value }` | Set agent state | `StateVersion(version)` |
| `StateDelete { agent_id, key }` | Delete agent state | `Deleted` |
| `SemanticUpsert { id, content, embedding, metadata }` | Upsert semantic entry | `Ok` |
| `SemanticDelete { id }` | Delete semantic entry | `Ok` |

## Testing

- `cargo test -p strata-cluster` (18 tests)
- Config deserialization tests (TOML, defaults, clone)
- MemStore tests (create, save/read vote, log state)
- AppRequest/AppResponse serialization roundtrip (MessagePack + JSON)
- NetworkClient URL construction
- ClusterCoordinator single-node Raft lifecycle (start → is_leader → shutdown)
- In-memory Raft network for unit tests (no real TCP)
