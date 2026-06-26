#!/usr/bin/env bash
# "Claude remembers across sessions" — a self-contained demo of Strata's cognition layer.
#
# It uses only curl against a running Strata server, so it needs no API key. It shows:
#   1. remembering facts about a user,
#   2. a contradiction superseding an old fact (bi-temporal),
#   3. recalling memories in a *new* session — the whole point.
#
# To put Claude in the loop, have your Anthropic tool-use loop call these same endpoints
# (see ../../docs/connect-claude.md). Memory survives process restarts (file-backed stores).
#
# Usage:  STRATA=http://localhost:8432 ./demo.sh
set -euo pipefail
STRATA="${STRATA:-http://localhost:8432}"
USER_ID="cust_42"
say() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

say "Session 1 — Claude learns facts about $USER_ID"
curl -s -X POST "$STRATA/api/v1/memories" -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER_ID\",\"subject\":\"plan\",\"content\":\"On the Pro plan\"}" | jq -c .
curl -s -X POST "$STRATA/api/v1/memories" -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER_ID\",\"content\":\"Prefers email over phone support\"}" | jq -c .

say "A contradiction arrives — the old fact is superseded, not overwritten"
SUP=$(curl -s -X POST "$STRATA/api/v1/memories" -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER_ID\",\"subject\":\"plan\",\"content\":\"Upgraded to Enterprise\"}")
echo "$SUP" | jq -c '{outcome, content: .memory.content}'
MEM_ID=$(echo "$SUP" | jq -r '.memory.id')

say "Session 2 (imagine a fresh conversation / restarted agent) — Claude recalls"
curl -s -X POST "$STRATA/api/v1/memories/search" -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER_ID\",\"query\":\"what plan are they on and how to contact them?\"}" \
  | jq -c '.results[] | {score, content: .memory.content}'

say "Bi-temporal history — what we believed, and when"
curl -s "$STRATA/api/v1/memories/$MEM_ID/history" | jq -c '.history[] | {state, content}'

printf '\n\033[1mDone.\033[0m The Enterprise fact is active; "On the Pro plan" is retained as history.\n'
