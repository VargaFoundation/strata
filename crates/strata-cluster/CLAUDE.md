# strata-cluster

## Responsibility

Distributed mode. Implements Raft consensus via openraft v0.9, routes writes through
the leader, and provides cluster coordination.

In cluster mode, **ingest, state, and memory writes** are proposed as an `AppRequest` through Raft
(`coordinator.client_write`) and applied deterministically to every node's `StrataEngine` on
commit — committed writes survive leader failover. This covers the REST write handlers AND the MCP
write tools (`ingest`/`set_state`/`add_memory`/`delete_memory`); for `add_memory` the leader runs
cognition (`memory_plan`) and replicates the materialized rows. The one exception is the MCP
`remember` tool (LLM extraction) which stays direct + snapshot-replicated. `apply` MUST be
deterministic: requests carry fully-materialized values (ids, timestamps, cognition results)
computed once on the leader — never re-run non-deterministic logic (uuid/now/LLM) at apply time.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| `TypeConfig` | **Working** | `declare_raft_types!` macro, AppRequest/AppResponse enums, NodeInfo, MessagePack serde |
| `MemStore` | **Working** | Full `RaftStorage` trait impl: log (BTreeMap), vote, state machine (applies to StrataEngine), snapshots |
| `GrpcRaftNetwork` (client) | **Working** | **gRPC (tonic, HTTP/2)** transport for AppendEntries/Vote/InstallSnapshot. openraft RPCs are MessagePack-encoded into an opaque `RaftBytes` proto (~1.8× smaller than JSON on embedding-heavy batches). Lazy per-peer `Channel` (auto-reconnect, multiplexed). 512 MB message cap for snapshots. |
| `RaftGrpcServer` | **Working** | tonic service exposing this node's Raft to peers; mirror of the client. Bound to `cluster.listen` (the port peers dial). Started by the coordinator in multi-node mode. |
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
  proto/raft.proto Thin gRPC service (RaftBytes envelope); compiled by build.rs
  raft/
    types.rs       TypeConfig, AppRequest, AppResponse, NodeInfo (openraft types)
    network.rs     GrpcRaftNetwork + GrpcRaftNetworkFactory (tonic gRPC client)
    server.rs      RaftGrpcServer (tonic gRPC service — receives peer RPCs)
    pb (in mod.rs) tonic::include_proto! generated types
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

## Inter-node transport (gRPC)

Hot-path Raft RPCs use **gRPC (tonic, HTTP/2)** — `GrpcRaftNetwork` (client) ↔ `RaftGrpcServer`
(server), via a thin `RaftBytes` proto carrying MessagePack-encoded openraft RPCs. The server binds
`cluster.listen` (e.g. :9433 — the address peers actually dial). The Raft log is also persisted in
MessagePack (`store.rs`). Low-traffic admin ops (`/cluster/status`, add-learner, change-membership)
stay on the HTTP gateway.

**Inter-node auth:** set `STRATA_CLUSTER__SECRET` to require a shared Bearer token on every Raft
RPC — the server rejects RPCs without it (constant-time check), so an unauthorized node can't inject
AppendEntries/Vote and corrupt the cluster. `None` = no auth (single-node / trusted network).

**TLS (encryption + optional mTLS):** set `cluster.tls` (`cert_path`, `key_path`, optional `ca_path`,
`domain`) to run the Raft gRPC transport over TLS. With `ca_path` the server requires + verifies peer
certs (mutual TLS) and clients trust that CA. When TLS is on, peer addresses must use the `https://`
scheme. The shared secret and TLS compose (TLS = confidentiality + peer identity; secret = cheap
app-level check). `raft/tls.rs` builds the tonic configs from PEM files. A mesh/mTLS sidecar remains
a valid alternative at the infra layer.

**Migration caveat:** the wire format AND on-disk log format are binary (MessagePack) — a breaking
change from the previous JSON. All nodes must run the same version, and on upgrade each node's
**Raft data dir must be wiped** (the old JSON log is unreadable). Low blast radius: the log is
rebuildable from the leader/snapshot.

## Testing

- `cargo test -p strata-cluster` (lib + integration; deterministic-apply, consensus round-trip,
  formation, transport)
- Config deserialization tests (TOML, defaults, clone); MemStore tests (vote, log state)
- AppRequest/AppResponse serialization roundtrip; **MessagePack-vs-JSON size** (embedding-heavy
  AppendEntries ≈1.8× smaller — justifies the migration)
- ClusterCoordinator single-node lifecycle; consensus round-trip (`client_write` → commit → apply);
  deterministic apply across two engines
- **Multi-node, in-process** (`tests/multi_node.rs`, 2 tests): real 3-node openraft convergence +
  config-driven self-formation (lowest-id bootstraps, no manual `initialize`). Port-free.
- **Multi-node, real sockets** (`tests/grpc_transport.rs`): 3 `ClusterCoordinator`s via the
  production path (`start_raft` → gRPC factory + server on real `127.0.0.1:<port>`) form the cluster
  and a leader write converges on every node **over real HTTP/2** — proves the transport + that the
  Raft server binds the address peers dial.

## Cluster deployment

`STRATA_CLUSTER__PEERS` is the **full voter membership** as comma-separated `id@addr`
(including this node), e.g. `1@http://strata-0:9433,2@http://strata-1:9433,3@http://strata-2:9433`;
`STRATA_CLUSTER__NODE_ID` is this node's id. The Helm StatefulSet derives both from the pod
ordinal automatically. The lowest-id node forms the cluster; the rest are pulled in by the leader.
Each node serves the Raft gRPC transport on `STRATA_CLUSTER__LISTEN` (:9433).

**Sharded deployment (horizontal write scaling):** set `sharding.enabled=true` in the Helm chart to
deploy N **independent Raft groups** (one StatefulSet `…-shard-<i>` + headless service each,
`replicasPerShard` nodes), with `STRATA_CLUSTER__SHARDS` uniform across the fleet so every node hashes
keys identically (`ShardRouter`). Each shard has its own leader, so write throughput scales with shard
count. Single-group (`sharding.enabled=false`, default) is unchanged.

**Runtime routing (gateway):** in sharded mode each pod knows its `STRATA_CLUSTER__SHARD_INDEX` and the
HTTP base URL of every shard (`STRATA_CLUSTER__SHARD_BASE_URLS`). The gateway middleware
`cluster/shard_route.rs` routes each `/api/v1/*` request **by tenant** to the owning shard
(`route_decision`): served locally if owned, else **reverse-proxied** to the owning shard (not a 307 —
the leader-forward 307 has no usable Location). Middleware order: `auth → shard-route → leader-forward`.
Tenant-deletion routes by the path tenant. Verified by unit tests + a single-process reverse-proxy
e2e test (`crates/strata-gateway/src/cluster/shard_route.rs`).

**Known limits of the routing increment (documented, not bugs):** (1) cross-tenant **admin** reads
(`/admin/audit|backup|reindex|retention|memory/*`) route by the caller's tenant → see only one shard;
true cross-tenant aggregation needs scatter-gather (`ShardedCluster::query_all`/`memory_search_all`
exist as the model). (2) **MCP/LLM-proxy/gRPC/PG** are outside `/api/v1` → not yet shard-routed
(REST-first). (3) A cross-shard write landing on a destination follower relies on the proxy's bounded
307-retry + Service balancing to reach the leader. (4) The token-bucket rate limiter runs on both hops
of a cross-shard request (minor double-decrement). Cross-shard 307 convergence + Raft-group isolation
need a live multi-pod cluster to validate.
