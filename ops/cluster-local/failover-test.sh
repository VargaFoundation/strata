#!/usr/bin/env bash
# End-to-end HA proof on the local cluster: create a durable run on the leader, confirm it
# replicated to a follower, KILL the leader, wait for re-election, and confirm the run survived.
#
# Run after run-cluster.sh has formed a cluster (a leader is elected).
set -euo pipefail
HOST="${HOST:-127.0.0.1}"
HTTP_BASE="${HTTP_BASE:-18001}"
NODES="${NODES:-3}"
RUN_DIR="${RUN_DIR:-/tmp/ecphoria-cluster}"

port_of() { echo $((HTTP_BASE + $1 - 1)); }
status_of() { curl -fsS --max-time 2 "http://${HOST}:$(port_of "$1")/cluster/status" 2>/dev/null || true; }
# Extract a JSON field with python (no jq dependency).
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d$1)" 2>/dev/null; }

find_leader() {
  for i in $(seq 1 "$NODES"); do
    [[ -f "$RUN_DIR/node-$i.pid" ]] && kill -0 "$(cat "$RUN_DIR/node-$i.pid")" 2>/dev/null || continue
    local s; s="$(status_of "$i")"; [[ -z "$s" ]] && continue
    local l; l="$(echo "$s" | jget "['current_leader']" || true)"
    if [[ -n "$l" && "$l" != "None" && "$l" != "null" ]]; then echo "$l"; return 0; fi
  done
  return 1
}

echo "== waiting for a leader =="
leader=""
for _ in $(seq 1 30); do leader="$(find_leader || true)"; [[ -n "$leader" ]] && break; sleep 1; done
[[ -z "$leader" ]] && { echo "no leader elected — is the cluster up?"; exit 1; }
echo "leader = node $leader (:$(port_of "$leader"))"

echo "== create a durable run on the leader (re-resolve + retry on leadership change) =="
run_id=""
for _ in $(seq 1 10); do
  leader="$(find_leader || true)"; [[ -z "$leader" ]] && { sleep 1; continue; }
  run="$(curl -fsS -X POST "http://${HOST}:$(port_of "$leader")/api/v1/runs" \
    -H 'content-type: application/json' \
    -d '{"agent_id":"failover-demo","input":{"proof":"survives-leader-kill"}}' 2>/dev/null || true)"
  run_id="$(echo "$run" | jget "['run']['id']" 2>/dev/null || true)"
  [[ -n "$run_id" && "$run_id" != "None" ]] && break
  run_id=""; sleep 1
done
[[ -z "$run_id" ]] && { echo "run not created after retries"; exit 1; }
echo "run id = $run_id  (created on leader node $leader)"

# Pick a follower (any live node that isn't the leader).
follower=""
for i in $(seq 1 "$NODES"); do
  [[ "$i" == "$leader" ]] && continue
  [[ -f "$RUN_DIR/node-$i.pid" ]] && kill -0 "$(cat "$RUN_DIR/node-$i.pid")" 2>/dev/null && { follower="$i"; break; }
done
echo "== read the run back from follower node $follower (proves replication) =="
sleep 1
got="$(curl -fsS "http://${HOST}:$(port_of "$follower")/api/v1/runs/${run_id}" | jget "['run']['id']" || true)"
[[ "$got" == "$run_id" ]] && echo "  ✓ replicated to follower" || { echo "  ✗ NOT on follower (got: $got)"; exit 1; }

echo "== KILL the leader (node $leader) =="
kill "$(cat "$RUN_DIR/node-$leader.pid")" && rm -f "$RUN_DIR/node-$leader.pid"

echo "== wait for re-election among survivors =="
newleader=""
for _ in $(seq 1 30); do newleader="$(find_leader || true)"; [[ -n "$newleader" && "$newleader" != "$leader" ]] && break; sleep 1; done
[[ -z "$newleader" ]] && { echo "no new leader — quorum lost?"; exit 1; }
echo "new leader = node $newleader (:$(port_of "$newleader"))"

echo "== read the run after failover (proves durability across leader loss) =="
survived="$(curl -fsS "http://${HOST}:$(port_of "$newleader")/api/v1/runs/${run_id}" | jget "['run']['id']" || true)"
if [[ "$survived" == "$run_id" ]]; then
  echo "  ✓ run $run_id SURVIVED the leader kill — HA proven"
else
  echo "  ✗ run lost after failover (got: $survived)"; exit 1
fi
