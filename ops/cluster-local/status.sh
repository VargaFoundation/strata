#!/usr/bin/env bash
# Print each node's /cluster/status (state + current leader) so you can see the cluster converge.
set -uo pipefail
HOST="${HOST:-127.0.0.1}"
HTTP_BASE="${HTTP_BASE:-18001}"
NODES="${NODES:-3}"

for i in $(seq 1 "$NODES"); do
  port=$((HTTP_BASE + i - 1))
  body="$(curl -fsS --max-time 2 "http://${HOST}:${port}/cluster/status" 2>/dev/null || echo '{"error":"unreachable"}')"
  printf 'node %d (:%d)  %s\n' "$i" "$port" "$body"
done
