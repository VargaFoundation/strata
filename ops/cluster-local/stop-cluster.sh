#!/usr/bin/env bash
# Stop the local cluster started by run-cluster.sh (kills each recorded PID).
set -uo pipefail
RUN_DIR="${RUN_DIR:-/tmp/strata-cluster}"

shopt -s nullglob
pids=("$RUN_DIR"/node-*.pid)
if [[ ${#pids[@]} -eq 0 ]]; then
  echo "No cluster PIDs found under $RUN_DIR"
  exit 0
fi

for pidfile in "${pids[@]}"; do
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null && echo "stopped pid $pid ($(basename "$pidfile"))"
  fi
  rm -f "$pidfile"
done
echo "Cluster stopped. (Data kept under $RUN_DIR — remove it to reset.)"
