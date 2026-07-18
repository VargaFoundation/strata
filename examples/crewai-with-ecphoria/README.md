# CrewAI with Ecphoria Memory Backend

This example shows how to use [Ecphoria](https://github.com/VargaFoundation/ecphoria) as a persistent memory backend for [CrewAI](https://github.com/crewAIInc/crewAI) agents.

Ecphoria provides episodic, semantic, and state memory — giving your crew long-term recall across runs.

## Prerequisites

1. A running Ecphoria server:
   ```bash
   docker run -p 8432:8432 ghcr.io/vargafoundation/ecphoria:latest
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## What This Example Does

- **Ingest phase**: Loads sample incident reports into Ecphoria's episodic memory
- **Research agent**: Queries Ecphoria to find relevant past incidents using semantic search
- **Analyst agent**: Analyzes patterns across incidents using SQL queries
- **Reporter agent**: Writes a summary report using state memory to track progress

## Run

```bash
python main.py
```

## Architecture

```
CrewAI Agents
    │
    ├── ecphoria.find()       → semantic search over past events
    ├── ecphoria.query()      → SQL analytics on episodic store
    ├── ecphoria.state_set()  → persist agent state across runs
    └── ecphoria.ingest()     → log new findings back to memory
```

## Key Concepts

- **Episodic memory** (events): Raw incident reports, log entries, and agent observations
- **Semantic memory** (vectors): Ecphoria auto-embeds ingested events for similarity search
- **State memory** (KV): Agent-specific state (e.g., last analysis timestamp, running tallies)
