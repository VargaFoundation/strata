#!/usr/bin/env bash
# Bring up a local N-node Strata Raft cluster (default 3) as background processes on one host.
#
# Every listener is on its own configurable port so nodes never collide on localhost:
#   node i (1-based):  HTTP = HTTP_BASE+(i-1)   PG = PG_BASE+(i-1)
#                      gRPC = GRPC_BASE+(i-1)    Raft = RAFT_BASE+(i-1)
# The four bases are far apart by default so no two listeners ever overlap, and each is
# overridable via the environment (HTTP_BASE, PG_BASE, GRPC_BASE, RAFT_BASE) to dodge anything
# already running (e.g. a dev server on 8432 or Postgres on 5432).
#
# Usage:
#   ops/cluster-local/run-cluster.sh           # 3 nodes
#   NODES=5 ops/cluster-local/run-cluster.sh   # 5 nodes
#   HTTP_BASE=28001 ops/cluster-local/run-cluster.sh
set -euo pipefail

BIN="${STRATA_BIN:-./target/release/strata-server}"
# Resolve to an absolute path: each node runs from its own working dir for data isolation.
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
RUN_DIR="${RUN_DIR:-/tmp/strata-cluster}"
NODES="${NODES:-3}"
HOST="${HOST:-127.0.0.1}"

# Disjoint port blocks (override any to avoid conflicts).
HTTP_BASE="${HTTP_BASE:-18001}"
PG_BASE="${PG_BASE:-15001}"
GRPC_BASE="${GRPC_BASE:-19001}"
RAFT_BASE="${RAFT_BASE:-19101}"

if [[ ! -x "$BIN" ]]; then
  echo "strata-server binary not found at '$BIN'." >&2
  echo "Build it first: cargo build --release --bin strata-server" >&2
  exit 1
fi

# Build the full id@addr voter membership (every node, including itself).
PEERS=""
for i in $(seq 1 "$NODES"); do
  raft=$((RAFT_BASE + i - 1))
  PEERS+="${i}@http://${HOST}:${raft},"
done
PEERS="${PEERS%,}"

mkdir -p "$RUN_DIR"
echo "Starting $NODES-node cluster (membership: $PEERS)"
for i in $(seq 1 "$NODES"); do
  http=$((HTTP_BASE + i - 1))
  pg=$((PG_BASE + i - 1))
  grpc=$((GRPC_BASE + i - 1))
  raft=$((RAFT_BASE + i - 1))
  nodedir="$RUN_DIR/node-$i"
  mkdir -p "$nodedir"

  # Run each node from its own working dir so every relative ./data/* path (episodic, state,
  # semantic index, raft log) is isolated per node — no shared files, no lock conflicts.
  (
    cd "$nodedir"
    STRATA_CLUSTER__ENABLED=true \
    STRATA_CLUSTER__NODE_ID="$i" \
    STRATA_CLUSTER__LISTEN="0.0.0.0:${raft}" \
    STRATA_CLUSTER__PEERS="$PEERS" \
    STRATA_GATEWAY__LISTEN="0.0.0.0:${http}" \
    STRATA_GATEWAY__PG_LISTEN="0.0.0.0:${pg}" \
    STRATA_GATEWAY__GRPC_LISTEN="0.0.0.0:${grpc}" \
    exec "$BIN" >"$nodedir/server.log" 2>&1
  ) &
  echo "$!" >"$RUN_DIR/node-$i.pid"
  printf '  node %d  http=%-5s pg=%-5s grpc=%-5s raft=%-5s  pid=%s\n' \
    "$i" "$http" "$pg" "$grpc" "$raft" "$!"
done

echo
echo "Logs:   $RUN_DIR/node-*/server.log"
echo "Status: ops/cluster-local/status.sh    Stop: ops/cluster-local/stop-cluster.sh"
echo "Node 1 HTTP: http://${HOST}:${HTTP_BASE}"
