# ADR-003: Raft for Clustering

**Status**: Accepted  
**Date**: 2024-07-10  
**Author**: Ecphoria Core Team

## Context

Ecphoria must support high-availability deployments where agents depend on continuous access to their memory. A single node is a single point of failure — if it goes down, all connected agents lose their memory layer.

We need a distributed consensus mechanism that:

- Provides strong consistency for writes (agents must not see stale or conflicting state)
- Supports automatic leader election and failover
- Integrates with our Rust/Tokio stack
- Doesn't add external service dependencies

### Alternatives Considered

**Paxos**: The foundational consensus algorithm. Theoretically proven but notoriously difficult to implement correctly. No production-ready embeddable Rust implementation available. Multi-Paxos (the practical variant) adds significant complexity.

**Gossip protocols (SWIM, Serf)**: Lightweight and scalable for membership and failure detection. But they provide only eventual consistency — two nodes can temporarily disagree about state. Unacceptable for agent state where a stale value could cause an agent to repeat an action or miss context.

**External coordination (etcd/ZooKeeper)**: Battle-tested distributed consensus as a service. But adding an external coordination service contradicts Ecphoria's single-binary philosophy. Operators would need to deploy and maintain a separate etcd/ZK cluster alongside Ecphoria.

**Primary-replica replication**: Simple and well-understood. But without consensus, there's no automatic failover — someone (or something) must decide when the primary is dead and promote a replica. Adds an external dependency (sentinel, load balancer health checks) or risks split-brain.

## Decision

Use **Raft** consensus via the `openraft` crate for distributed coordination.

Raft provides linearizable writes, automatic leader election, and a straightforward mental model (leader, follower, candidate). The `openraft` crate is a well-maintained async-first Rust implementation that aligns with our Tokio runtime.

Implementation details:

- **openraft v0.9** with `serde` feature for snapshot serialization
- **TypeConfig trait**: Custom `NodeId = u64`, `Node = EcphoriaNode`, `Entry = EcphoriaEntry`
- **HTTP network transport**: Raft RPC messages (AppendEntries, RequestVote, InstallSnapshot) sent over HTTP via our existing axum stack — no separate RPC port needed (though we use port 9433 for clarity)
- **In-memory MemStore**: Log entries and state machine stored in memory. Durability comes from the replicated log across nodes, not from local disk persistence of the Raft log itself
- **Leader forwarding**: A middleware layer redirects write requests (POST/PUT/DELETE) to the current leader. Read requests are served locally from any node (follower reads)
- **Single-node init**: In standalone mode, the node initializes a single-member Raft cluster and immediately becomes leader — no ceremony needed

## Consequences

### Positive

- **Strong consistency**: Linearizable writes ensure all nodes agree on the order of operations. No stale reads of agent state, no conflicting event sequences.
- **Automatic failover**: If the leader crashes, followers elect a new leader within seconds. Agents reconnect transparently via the load balancer.
- **Proven algorithm**: Raft is widely deployed (etcd, CockroachDB, TiKV). The algorithm is well-understood and extensively verified.
- **Clean Rust integration**: `openraft` is async-native, works with Tokio, and uses traits/generics for customization. No C FFI or bridge code.
- **No external dependencies**: Consensus runs inside the Ecphoria binary. No etcd, no ZooKeeper, no additional operational burden.

### Negative

- **Write throughput**: All writes go through the leader and require majority acknowledgment. Write latency is bounded by the slowest majority node's network RTT. For Ecphoria's workload (agent events, not financial transactions), this is acceptable.
- **Odd-number clusters**: Raft requires a majority quorum, so 3 or 5 nodes are optimal. 2-node clusters cannot tolerate any failure. Operators must understand this constraint.
- **Snapshot complexity**: As the Raft log grows, snapshots must be taken and transferred to lagging followers. Our in-memory MemStore means snapshots include the full state, which grows with event count. Mitigated by periodic compaction.
- **Leader bottleneck**: All writes route to a single node. Horizontal write scaling requires sharding (not yet implemented), not just adding Raft members.
