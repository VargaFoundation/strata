#!/usr/bin/env bash
# target-guard.sh — keep Cargo's build artifacts from saturating the disk.
#
# Measures the combined size of every `target/` dir in the repo (the main workspace + the standalone
# `ops/operator` workspace) and, if it exceeds a cap, reclaims space — preferring `cargo sweep`
# (removes only STALE artifacts, if installed) and falling back to a full `cargo clean`.
#
# Usage:
#   scripts/target-guard.sh            # check + auto-clean if over the cap (default 40 GB)
#   scripts/target-guard.sh --check    # report sizes only, never clean (exit 1 if over cap)
#   CAP_GB=25 scripts/target-guard.sh  # custom cap
#
# Wire it before heavy builds (the Makefile does this) or run it from cron.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CAP_GB="${CAP_GB:-40}"
CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

targets=("$ROOT/target" "$ROOT/ops/operator/target")
total_kb=0
for t in "${targets[@]}"; do
  if [ -d "$t" ]; then
    kb=$(du -sk "$t" 2>/dev/null | cut -f1)
    total_kb=$((total_kb + kb))
  fi
done
total_gb=$((total_kb / 1024 / 1024))

echo "target/ artifacts: ${total_gb} GB (cap ${CAP_GB} GB)"

if [ "$total_gb" -lt "$CAP_GB" ]; then
  exit 0
fi

if [ "$CHECK_ONLY" = "1" ]; then
  echo "OVER CAP — run 'make clean' (or scripts/target-guard.sh) to reclaim space." >&2
  exit 1
fi

echo "over cap → reclaiming space…"
if command -v cargo-sweep >/dev/null 2>&1; then
  # Remove artifacts not touched in the last 7 days + those from other toolchains.
  cargo sweep --time 7 "$ROOT" || true
  cargo sweep --installed "$ROOT" || true
  # Re-check; if still over cap, fall through to a full clean.
  kb=$(du -sk "$ROOT/target" 2>/dev/null | cut -f1 || echo 0)
  [ $((kb / 1024 / 1024)) -lt "$CAP_GB" ] && { echo "reclaimed via cargo-sweep."; exit 0; }
fi

( cd "$ROOT" && cargo clean )
[ -d "$ROOT/ops/operator" ] && ( cd "$ROOT/ops/operator" && cargo clean )
echo "reclaimed via cargo clean."
