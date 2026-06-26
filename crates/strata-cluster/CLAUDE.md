# strata-cluster

## Responsibility

Distributed mode. Implements Raft consensus via openraft v0.9, routes writes through
the leader, and provides cluster coordination.

In cluster mode, **ingest and state writes** are proposed as an `AppRequest` through Raft
(`coordinator.client_write`) and applied deterministically to every node's `StrataEngine`
on commit — committed writes survive leader failover. **Memory writes** currently apply
directly on the leader and replicate to followers via (complete) snapshots, not the log.
`apply` MUST be deterministic: requests carry fully-materialized values (ids, timestamps,
cognition results) computed once on the leader — never re-run non-deterministic logic
(uuid/now/LLM) at apply time.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| `TypeConfig` | **Working** | `declare_raft_types!` macro, AppRequest/AppResponse enums, NodeInfo, MessagePack serde |
| `MemStore` | **Working** | Full `RaftStorage` trait impl: log (BTreeMap), vote, state machine (applies to StrataEngine), snapshots |
| `NetworkClient` | **Working** | HTTP JSON POST for AppendEntries, Vote, InstallSnapshot RPCs |
| `NetworkFactory` | **Working** | Creates `NetworkClient` per target node with shared reqwest::Client |
| `ClusterCoordinator` | **Working** | Owns the `openraft::Raft` instance, `client_write()`, `is_leader()`, `leader_id()`, graceful shutdown. **Cluster formation**: single-node inits immediately; multi-node parses `peers` as `id@addr` voter membership and the lowest-id node bootstraps via `initialize` once — idempotent on restart, retries until peers are reachable. `start_raft_with_network` allows injecting a network (tests). |
| `ClusterConfig` | **Working** | TOML deserialization, node_id, listen, peers |
| `ClusterCoordinator` (metrics) | **Working** | Background task publishing Raft metrics to Prometheus (term, is_leader, replication_lag, leader_changes) |
| `LogShipper` | Stub | WAL segment shipping between peers (not needed for basic Raft, uses AppendEntries) |
| `SnapshotManager` | **Working** | Binary pack/unpack of **all 4 stores** (episodic + memories DuckDB exports, state SQLite, USearch index); restore via `engine.restore_from_backup` (atomic stage-then-swap), build/install wired into RaftSnapshotBuilder |

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

All requests carry **materialized** values so apply is deterministic on every node.

| Variant | Description | Response |
|---------|-------------|----------|
| `Ingest { events, tenant }` | Append fully-formed events (ids/timestamps fixed by leader), tenant-scoped | `Ingested(count)` |
| `StateSet { agent_id, key, value, tenant }` | Set agent state (tenant-scoped) | `StateVersion(version)` |
| `StateDelete { agent_id, key, tenant }` | Delete agent state (tenant-scoped) | `Deleted` |
| `SemanticUpsert { id, content, embedding, metadata }` | Upsert semantic entry | `Ok` |
| `SemanticDelete { id }` | Delete semantic entry | `Ok` |
| `MemoryUpsert { memories }` | Replace materialized memory rows (leader ran cognition) | `MemoryCount(n)` |
| `MemoryDelete { id }` | Delete a memory by id | `MemoryCount(n)` |

## Testing

- `cargo test -p strata-cluster` (22 tests, incl. deterministic-apply + consensus round-trip)
- Config deserialization tests (TOML, defaults, clone)
- MemStore tests (create, save/read vote, log state)
- AppRequest/AppResponse serialization roundtrip (MessagePack + JSON)
- NetworkClient URL construction
- ClusterCoordinator single-node Raft lifecycle (start → is_leader → shutdown)
- Consensus round-trip: `client_write` → commit → apply lands on the engine (Ingest/State/Memory)
- Deterministic apply: the same committed entry yields identical state on two independent engines
- **Multi-node** (`tests/multi_node.rs`, 2 tests): (1) a real 3-node openraft cluster over an
  in-process network proves a leader write commits via quorum and **converges on every node's
  engine** (same event id); (2) **config-driven formation** — 3 `ClusterCoordinator`s built from
  `peers` config form the cluster THEMSELVES (lowest-id node bootstraps, no manual `initialize`),
  elect a leader, and a write converges. Port-free, non-flaky. The HTTP `NetworkClient` transport
  is covered by the single-node + unit tests.

## Cluster deployment

`STRATA_CLUSTER__PEERS` is the **full voter membership** as comma-separated `id@addr`
(including this node), e.g. `1@http://strata-0:9433,2@http://strata-1:9433,3@http://strata-2:9433`;
`STRATA_CLUSTER__NODE_ID` is this node's id. The Helm StatefulSet derives both from the pod
ordinal automatically. The lowest-id node forms the cluster; the rest are pulled in by the leader.
