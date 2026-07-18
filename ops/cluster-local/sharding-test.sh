#!/usr/bin/env bash
# Validate the sharded cluster started by run-sharded.sh. Proves the three properties a multi-Raft
# deployment must have, which single-group tests cannot cover:
#
#   1. Raft-group isolation at formation — each shard elects its OWN leader independently
#      (SHARDS distinct groups, each with a leader).
#   2. Cross-shard tenant routing — a tenant written via one shard's gateway is readable via every
#      OTHER shard's gateway (the reverse-proxy path: non-owning gateways forward to the owner).
#   3. Failover isolation — killing shard 0's leader re-elects within shard 0 while shard 1's
#      leader/term are untouched (a group failure stays contained to its group).
set -euo pipefail
HOST="${HOST:-127.0.0.1}"
HTTP_BASE="${HTTP_BASE:-28001}"
SHARDS="${SHARDS:-2}"
REPLICAS="${REPLICAS:-3}"
RUN_DIR="${RUN_DIR:-/tmp/ecphoria-sharded}"

# tenant → api key (kept in sync with run-sharded.sh API_KEYS).
declare -A KEY=( [alpha]=alpha-key [beta]=beta-key [gamma]=gamma-key [delta]=delta-key )
TENANTS=(alpha beta gamma delta)

pidx() { echo $(( $1 * REPLICAS + $2 )); }                 # (shard, replica) → global process index
http_of() { echo $(( HTTP_BASE + $1 )); }                 # global process index → HTTP port
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d$1)" 2>/dev/null; }
# /cluster/status requires auth when auth is enabled; any valid key works (not tenant-specific).
status_at() { curl -fsS --max-time 2 -H "authorization: Bearer ${KEY[alpha]}" \
  "http://${HOST}:$(http_of "$1")/cluster/status" 2>/dev/null || true; }

# Leader node_id for a shard (poll its replica 0..R-1 until one reports a leader). Echoes "" if none.
shard_leader() {
  local s="$1" r p st l
  for r in $(seq 0 $((REPLICAS - 1))); do
    p="$(pidx "$s" "$r")"; st="$(status_at "$p")"; [[ -z "$st" ]] && continue
    l="$(echo "$st" | jget "['current_leader']" || true)"
    [[ -n "$l" && "$l" != "None" && "$l" != "null" ]] && { echo "$l"; return 0; }
  done
  echo ""; return 0
}
shard_term() {                                            # current term as seen by shard s replica 0
  local st; st="$(status_at "$(pidx "$1" 0)")"
  echo "$st" | jget "['current_term']" 2>/dev/null || echo ""
}

echo "== 1. wait for every shard to elect its own leader (group isolation at formation) =="
for s in $(seq 0 $((SHARDS - 1))); do
  leader=""
  for _ in $(seq 1 40); do leader="$(shard_leader "$s")"; [[ -n "$leader" ]] && break; sleep 1; done
  [[ -z "$leader" ]] && { echo "  ✗ shard $s elected no leader"; exit 1; }
  echo "  ✓ shard $s leader = node_id $leader (term $(shard_term "$s"))"
done

# Gateway entrypoint (replica 0) of a shard.
gw() { echo "http://${HOST}:$(http_of "$(pidx "$1" 0)")"; }

echo "== 2. write each tenant via shard 0, read back via EVERY shard (cross-shard routing) =="
for t in "${TENANTS[@]}"; do
  # Write via shard 0's gateway. The router keys on the token's tenant and forwards to the owner.
  code="$(curl -fsS -o /dev/null -w '%{http_code}' -X POST "$(gw 0)/api/v1/memories" \
    -H "authorization: Bearer ${KEY[$t]}" -H 'content-type: application/json' \
    -d "{\"content\":\"${t} likes tea\",\"subject\":\"drink\"}" || true)"
  [[ "$code" == "200" || "$code" == "201" ]] || { echo "  ✗ write for $t via shard 0 failed (HTTP $code)"; exit 1; }
done
sleep 1
# Read every tenant from every shard's gateway; for a tenant not owned by that shard this is a
# reverse-proxy hop, so success across all shards proves both the local and proxy paths.
for t in "${TENANTS[@]}"; do
  for s in $(seq 0 $((SHARDS - 1))); do
    body="$(curl -fsS --max-time 5 "$(gw "$s")/api/v1/memories/search" \
      -H "authorization: Bearer ${KEY[$t]}" -H 'content-type: application/json' \
      -d '{"query":"tea","k":5}' 2>/dev/null || true)"
    echo "$body" | grep -q "${t} likes tea" \
      || { echo "  ✗ tenant $t not readable via shard $s gateway (routing broken)"; echo "     got: $body"; exit 1; }
  done
  echo "  ✓ tenant $t readable via all $SHARDS shard gateways"
done

echo "== 3. kill shard 0's leader; shard 0 re-elects, shard 1 untouched (failover isolation) =="
s1_leader_before="$(shard_leader 1)"; s1_term_before="$(shard_term 1)"
old="$(shard_leader 0)"
kill "$(cat "$RUN_DIR/shard-0-node-$((old - 1)).pid")" && rm -f "$RUN_DIR/shard-0-node-$((old - 1)).pid"
echo "  killed shard 0 leader (node_id $old)"

new=""
for _ in $(seq 1 40); do new="$(shard_leader 0)"; [[ -n "$new" && "$new" != "$old" ]] && break; sleep 1; done
[[ -z "$new" || "$new" == "$old" ]] && { echo "  ✗ shard 0 did not re-elect"; exit 1; }
echo "  ✓ shard 0 re-elected → node_id $new"

s1_leader_after="$(shard_leader 1)"; s1_term_after="$(shard_term 1)"
if [[ "$s1_leader_after" == "$s1_leader_before" && "$s1_term_after" == "$s1_term_before" ]]; then
  echo "  ✓ shard 1 unaffected (leader $s1_leader_after, term $s1_term_after) — failure contained to shard 0"
else
  echo "  ✗ shard 1 disturbed by shard 0 failover: leader $s1_leader_before→$s1_leader_after term $s1_term_before→$s1_term_after"
  exit 1
fi

echo
echo "ALL SHARDING ASSERTIONS PASSED ✓ (group isolation, cross-shard routing, contained failover)"
