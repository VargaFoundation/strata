# Multi-Agent Customer Support with Ecphoria

Three autonomous agents collaborate through shared memory to triage, resolve, and escalate customer support tickets — all backed by Ecphoria.

## Architecture

```
                         ┌──────────────────────────────────────┐
  Support Tickets        │              Ecphoria                  │
  ──────────────►        │  ┌──────────┬──────────┬──────────┐  │
                         │  │ Episodic │ Semantic │  State   │  │
┌──────────┐  ingest     │  │ (events) │ (search) │ (ticket  │  │
│  Triage  │────────────►│  │          │          │  state)  │  │
│  Agent   │◄────────────│  │ ticket   │ past     │ priority │  │
└────┬─────┘  read state │  │ history  │ resolu-  │ status   │  │
     │                   │  │          │ tions    │ context  │  │
     ▼ low/med           │  └──────────┴──────────┴──────────┘  │
┌──────────┐  search     │                                      │
│ L1 Agent │────────────►│  REST API :8432                      │
│ (auto-   │◄────────────│  PG wire  :5432                      │
│ resolve) │  results    └──────────────────────────────────────┘
└────┬─────┘
     │ no match
     ▼ high/critical
┌──────────┐
│ L2 Agent │  Gathers full context → marks pending_human
│ (escala- │
│  tion)   │
└──────────┘
```

## How It Works

1. **Triage Agent** — Polls for new tickets, classifies priority (critical/high/medium/low) and category (billing/account/technical/general), routes to L1 or L2
2. **L1 Support Agent** — Searches Ecphoria's semantic memory for similar past resolutions. Auto-resolves if similarity > 0.8, otherwise escalates
3. **L2 Escalation Agent** — Gathers full context (original ticket + triage info + L1 attempt), packages everything for human review

All agents share state through Ecphoria's key-value store and communicate by ingesting events into the episodic timeline.

## Quick Start

### With Docker Compose

```bash
cd examples/multi-agent-support
docker compose up
```

This starts Ecphoria, Ollama (for embeddings), and all three agents.

### Without Docker

```bash
# Start Ecphoria
docker run -d --name ecphoria -p 8432:8432 -p 5432:5432 \
  ghcr.io/varga-foundation/ecphoria:latest

# Install deps
pip install -r requirements.txt

# Run the simulation
python simulate.py
```

## Example Output

```
2024-01-15 10:00:01 [simulate] INFO Created ticket a1b2c3d4: Cannot login to my account
2024-01-15 10:00:01 [simulate] INFO Created ticket e5f6g7h8: URGENT: Production outage
...
2024-01-15 10:00:03 [triage] INFO Ticket a1b2c3d4 → priority=high category=account → l2
2024-01-15 10:00:03 [triage] INFO Ticket e5f6g7h8 → priority=critical category=technical → l2
2024-01-15 10:00:03 [triage] INFO Ticket i9j0k1l2 → priority=medium category=billing → l1
...
2024-01-15 10:00:05 [support-l1] INFO Ticket i9j0k1l2 — auto-resolved (score=0.87): Previous billing...
2024-01-15 10:00:06 [escalation-l2] INFO Ticket e5f6g7h8 — full context gathered, marking pending_human

======================================================================
SIMULATION SUMMARY
======================================================================
  [OK] a1b2c3d4  high      account      → resolved
  [L2] e5f6g7h8  critical  technical    → pending_human
  [OK] i9j0k1l2  medium    billing      → resolved
  ...
----------------------------------------------------------------------
  Total: 10  |  Auto-resolved: 4  |  Pending human: 3  |  Escalated: 2  |  In progress: 1
======================================================================
```

## Inspect the Data

```bash
# Connect via psql
psql -h localhost -p 5432 -U postgres

# View all triage decisions
SELECT ts, payload->>'ticket_id' as ticket,
       payload->>'priority' as priority,
       payload->>'assigned_to' as assigned
FROM episodic
WHERE source = 'triage-agent'
ORDER BY ts DESC;

# View auto-resolutions
SELECT ts, payload->>'ticket_id' as ticket,
       payload->>'similarity_score' as score,
       payload->>'resolution' as resolution
FROM episodic
WHERE source = 'l1-agent' AND event_type = 'ticket.resolved';

# Count by agent
SELECT source, event_type, COUNT(*)
FROM episodic
GROUP BY source, event_type
ORDER BY source;
```

## Customization

- **Add an LLM**: Replace keyword-based triage with Claude or GPT classification
- **WebSocket state watching**: Use `GET /api/v1/state/{agent_id}/watch` for real-time ticket updates
- **More agents**: Add an SLA monitor, customer sentiment tracker, or auto-responder
- **Webhook ingest**: Connect real ticketing systems via `POST /api/v1/webhook/{source}`
