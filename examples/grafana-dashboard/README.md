# Grafana Dashboard for AI Agents

A ready-to-import Grafana dashboard that connects to Ecphoria via its PostgreSQL wire protocol and Prometheus metrics. Visualize agent activity, event timelines, and API performance in real time.

## What You'll See

```
┌────────────────┬────────────────┬────────────────┬────────────────┐
│  Events        │  Active        │  Events        │  Avg Events    │
│  Ingested      │  Sources       │  Today         │  / Hour        │
│     1,247      │       4        │      183       │     42.3       │
├────────────────┴────────────────┼────────────────┴────────────────┤
│  Events Over Time (stacked bar) │  Events by Source (donut)       │
│  ████                           │      ┌─────┐                   │
│  ██████████                     │  web ─┤     ├─ api             │
│  ████████████████               │      └─────┘                   │
├─────────────────────────────────┼─────────────────────────────────┤
│  Top Event Types (horizontal)   │  Recent Events (table)          │
│  user.login     ████████████    │  ts     source  event_type      │
│  user.signup    ████████        │  12:01  web-app user.signup     │
│  order.created  ████            │  12:00  mobile  search.query    │
├─────────────────────────────────┼─────────────────────────────────┤
│  Request Rate (timeseries)      │  Request Duration p99           │
│  Prometheus metrics             │  Prometheus metrics             │
└─────────────────────────────────┴─────────────────────────────────┘
```

Overview stats, event timeline with source breakdown, agent activity details, and API performance
from Prometheus — plus an **Agent runtime & reliability** row: agent runs & steps per minute,
**embedding failures** (each degrades search to BM25-only), LLM cache hit ratio, and Raft
leader/term/replication-lag for cluster health.

## Quick Start

```bash
cd examples/grafana-dashboard
docker compose up -d
```

Then open [http://localhost:3000](http://localhost:3000) — login with `admin` / `ecphoria`.

The dashboard auto-provisions and the seed container injects 50 sample events so you see data immediately. Auto-refresh is set to 30s.

## Stack

| Service | Port | Purpose |
|---------|------|---------|
| Ecphoria | 8432 (HTTP), 5432 (PG wire) | Context lake |
| Grafana | 3000 | Dashboard UI |
| Prometheus | 9090 | Metrics scraping |

## How It Works

- **SQL panels** connect to Ecphoria's PG wire interface (port 5432) using the PostgreSQL datasource. Ecphoria routes SQL to DuckDB, which supports full analytical queries.
- **Prometheus panels** scrape Ecphoria's `/metrics` endpoint for request rates and latency histograms.
- The dashboard is provisioned automatically via Grafana's file-based provisioning.

## Import into Existing Grafana

If you already have Grafana running:

1. Add a PostgreSQL datasource pointing to your Ecphoria instance (port 5432)
2. Import `dashboards/ai-agents.json` via Grafana UI (Dashboards > Import)
3. Select the Ecphoria datasource when prompted

For Prometheus panels, also add a Prometheus datasource scraping Ecphoria's `:8432/metrics`.

## Customization

Edit `dashboards/ai-agents.json` to:

- Add panels for specific agents or event types
- Change time ranges or refresh intervals
- Add template variables for source/event_type filtering
- Create alerts on event rates or error counts

The dashboard auto-reloads from disk every 30 seconds (configurable in `provisioning/dashboards.yml`).
