#!/usr/bin/env bash
#
# Seed Ecphoria with sample events so the Grafana dashboard isn't empty.
# Waits for Ecphoria to be healthy, then ingests 50 events across multiple sources.

set -euo pipefail

ECPHORIA_URL="${ECPHORIA_URL:-http://ecphoria:8432}"

echo "Waiting for Ecphoria to be ready..."
for i in $(seq 1 30); do
  if curl -sf "${ECPHORIA_URL}/health" > /dev/null 2>&1; then
    echo "Ecphoria is ready."
    break
  fi
  sleep 2
done

ingest() {
  local source="$1"
  local events="$2"
  curl -sf -X POST "${ECPHORIA_URL}/api/v1/ingest" \
    -H 'Content-Type: application/json' \
    -d "{\"source\": \"${source}\", \"events\": ${events}}" > /dev/null
}

echo "Ingesting sample events..."

# Web app events
for i in $(seq 1 15); do
  ingest "web-app" "[
    {\"event_type\": \"user.signup\", \"payload\": {\"user_id\": \"user-${i}\", \"plan\": \"free\"}},
    {\"event_type\": \"user.login\", \"payload\": {\"user_id\": \"user-${i}\", \"method\": \"password\"}}
  ]"
done

# Mobile app events
for i in $(seq 1 8); do
  ingest "mobile-app" "[
    {\"event_type\": \"user.login\", \"payload\": {\"user_id\": \"mobile-${i}\", \"platform\": \"ios\"}},
    {\"event_type\": \"search.query\", \"payload\": {\"query\": \"how to reset password\", \"results\": ${i}}}
  ]"
done

# API gateway events
for i in $(seq 1 5); do
  ingest "api-gateway" "[
    {\"event_type\": \"order.created\", \"payload\": {\"order_id\": \"ord-${i}\", \"amount\": $((i * 29))}},
    {\"event_type\": \"error.500\", \"payload\": {\"path\": \"/api/v1/checkout\", \"status\": 500}}
  ]"
done

# Agent events
for i in $(seq 1 4); do
  ingest "support-agent" "[
    {\"event_type\": \"ticket.created\", \"payload\": {\"ticket_id\": \"tkt-${i}\", \"priority\": \"medium\"}},
    {\"event_type\": \"ticket.resolved\", \"payload\": {\"ticket_id\": \"tkt-${i}\", \"resolution\": \"auto\"}}
  ]"
done

echo "Seeded 50 events across 4 sources."
