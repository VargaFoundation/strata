# Local Strata cluster (no Docker required)

Bring up a real N-node Raft cluster as background processes on one host — useful for validating
HA, leader election, replication, and failover without containers. Every listener is on its own
**configurable** port, so nodes never collide on `localhost`.

## Quick start

```bash
cargo build --release --bin strata-server      # once

ops/cluster-local/run-cluster.sh               # start 3 nodes
ops/cluster-local/status.sh                    # watch them elect a leader
ops/cluster-local/failover-test.sh             # create a run, kill the leader, prove it survives
ops/cluster-local/stop-cluster.sh              # stop
```

## Ports (all overridable)

Node `i` (1-based) binds four ports, taken from four well-separated bases so nothing overlaps:

| Listener | Env base (default) | Node 1 / 2 / 3 |
|----------|--------------------|----------------|
| HTTP/REST/MCP/metrics | `HTTP_BASE` (18001) | 18001 / 18002 / 18003 |
| PostgreSQL wire | `PG_BASE` (15001) | 15001 / 15002 / 15003 |
| gRPC | `GRPC_BASE` (19001) | 19001 / 19002 / 19003 |
| Raft (inter-node) | `RAFT_BASE` (19101) | 19101 / 19102 / 19103 |

Override any base to dodge a conflict, e.g. `HTTP_BASE=28001 ops/cluster-local/run-cluster.sh`.
`NODES` sets the cluster size, `RUN_DIR` the data/log/pid directory (default `/tmp/strata-cluster`).

Each node also gets isolated `STRATA_STORAGE__DATA_DIR` and `STRATA_CLUSTER__DATA_DIR` under
`$RUN_DIR/node-<i>/`, so state never mixes.

## How formation works

The launcher builds the **full voter membership** as `id@addr` (every node, including itself) and
passes the identical list to all nodes via `STRATA_CLUSTER__PEERS`
(e.g. `1@http://127.0.0.1:19101,2@http://127.0.0.1:19102,3@http://127.0.0.1:19103`). The lowest-id
node bootstraps the cluster; the others are pulled in by the leader. (This is the same `id@addr`
format the Helm chart and `deploy/docker-compose.cluster.yml` use.)

## What `failover-test.sh` proves

1. Finds the current leader via `/cluster/status`.
2. Creates a durable agent **run** on the leader (`POST /api/v1/runs` → replicated as a Raft
   `RunCreate`).
3. Reads it back from a **follower** — proves replication.
4. **Kills the leader process**, waits for re-election.
5. Reads the run from the new leader — proves it **survived leader loss**.

## Scope / honest note

This proves the **durable run ledger** (create/update) is replicated and survives failover — the HA
substrate of the agentic platform. The in-process agent **loop driver** (`run_agent`) currently
executes on the leader and writes its steps locally; replicating each step through the log so a
failover *mid-loop* resumes exactly where it left off is a separate, tracked increment. Run records,
state, ingest, and memories all already replicate (verified here and by the in-process multi-node
tests in `crates/strata-cluster/tests/`).
