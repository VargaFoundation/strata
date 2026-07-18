#!/usr/bin/env bash
# Bring up a local **sharded** Ecphoria cluster: SHARDS independent Raft groups, REPLICAS nodes each
# (SHARDS*REPLICAS processes on one host). Each shard forms its own group (its own PEERS + leader);
# every node knows the whole fleet's shard layout (SHARDS + SHARD_BASE_URLS) so the gateway routes
# each request to its tenant's owning shard. Auth is ON with per-tenant API keys — with auth off the
# router would key everything on "default" and never cross a shard boundary.
#
# Global process index p = shard*REPLICAS + replica (0-based); every listener derives from p so no
# two processes collide. Shard s's HTTP entrypoint (advertised in SHARD_BASE_URLS) is its replica 0.
#
# Usage:
#   ops/cluster-local/run-sharded.sh                 # 2 shards × 3 replicas
#   SHARDS=3 REPLICAS=3 ops/cluster-local/run-sharded.sh
set -euo pipefail

BIN="${ECPHORIA_BIN:-./target/release/ecphoria-server}"
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
RUN_DIR="${RUN_DIR:-/tmp/ecphoria-sharded}"
SHARDS="${SHARDS:-2}"
REPLICAS="${REPLICAS:-3}"
HOST="${HOST:-127.0.0.1}"

# Disjoint port blocks (override any to avoid conflicts).
HTTP_BASE="${HTTP_BASE:-28001}"
PG_BASE="${PG_BASE:-25001}"
GRPC_BASE="${GRPC_BASE:-29001}"
RAFT_BASE="${RAFT_BASE:-29101}"

# Per-tenant API keys shared by every node. `<secret>@<tenant>:<role>` → a request bearing <secret>
# is scoped to <tenant>, which is exactly what the shard router keys on. Kept in sync with
# sharding-test.sh.
API_KEYS="${API_KEYS:-alpha-key@alpha:admin,beta-key@beta:admin,gamma-key@gamma:admin,delta-key@delta:admin}"

if [[ ! -x "$BIN" ]]; then
  echo "ecphoria-server binary not found at '$BIN'." >&2
  echo "Build it first: cargo build --release --bin ecphoria-server" >&2
  exit 1
fi

http_of() { echo $(( HTTP_BASE + $1 )); }   # arg = global process index p

# SHARD_BASE_URLS: one HTTP entrypoint per shard (its replica 0), indexed by shard.
BASE_URLS=""
for s in $(seq 0 $((SHARDS - 1))); do
  p=$((s * REPLICAS))                        # replica 0 of shard s
  BASE_URLS+="http://${HOST}:$(http_of "$p"),"
done
BASE_URLS="${BASE_URLS%,}"

mkdir -p "$RUN_DIR"
echo "Starting ${SHARDS}×${REPLICAS} sharded cluster"
echo "  shard base urls: $BASE_URLS"

for s in $(seq 0 $((SHARDS - 1))); do
  # Membership WITHIN this shard: replicas 1..REPLICAS, raft addrs in this shard's block.
  peers=""
  for r in $(seq 0 $((REPLICAS - 1))); do
    p=$((s * REPLICAS + r))
    peers+="$((r + 1))@http://${HOST}:$((RAFT_BASE + p)),"
  done
  peers="${peers%,}"

  for r in $(seq 0 $((REPLICAS - 1))); do
    p=$((s * REPLICAS + r))
    http=$((HTTP_BASE + p)); pg=$((PG_BASE + p)); grpc=$((GRPC_BASE + p)); raft=$((RAFT_BASE + p))
    nodedir="$RUN_DIR/shard-$s-node-$r"
    mkdir -p "$nodedir"
    (
      cd "$nodedir"
      ECPHORIA_CLUSTER__ENABLED=true \
      ECPHORIA_CLUSTER__NODE_ID="$((r + 1))" \
      ECPHORIA_CLUSTER__LISTEN="0.0.0.0:${raft}" \
      ECPHORIA_CLUSTER__PEERS="$peers" \
      ECPHORIA_CLUSTER__SHARDS="$SHARDS" \
      ECPHORIA_CLUSTER__SHARD_INDEX="$s" \
      ECPHORIA_CLUSTER__SHARD_BASE_URLS="$BASE_URLS" \
      ECPHORIA_GATEWAY__LISTEN="0.0.0.0:${http}" \
      ECPHORIA_GATEWAY__PG_LISTEN="0.0.0.0:${pg}" \
      ECPHORIA_GATEWAY__GRPC_LISTEN="0.0.0.0:${grpc}" \
      ECPHORIA_GATEWAY__AUTH_ENABLED=true \
      ECPHORIA_GATEWAY__API_KEYS="$API_KEYS" \
      ECPHORIA_CLUSTER__SECRET="${ECPHORIA_CLUSTER__SECRET:-sharded-dev-secret}" \
      exec "$BIN" >"$nodedir/server.log" 2>&1
    ) &
    echo "$!" >"$RUN_DIR/shard-$s-node-$r.pid"
    printf '  shard %d replica %d (node_id %d)  http=%-5s raft=%-5s  pid=%s\n' \
      "$s" "$r" "$((r + 1))" "$http" "$raft" "$!"
  done
done

echo
echo "Logs:   $RUN_DIR/shard-*/server.log"
echo "Test:   ops/cluster-local/sharding-test.sh    Stop: RUN_DIR=$RUN_DIR ops/cluster-local/stop-cluster.sh"
