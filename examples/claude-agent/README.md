# 5-Minute Agent with Claude + Ecphoria

A conversational AI agent that **remembers everything** — powered by Claude for reasoning and Ecphoria for persistent memory. Every message is stored, searchable, and shapes future conversations.

## Architecture

```
┌─────────┐       ┌──────────────────────────────────────────┐
│         │       │               Ecphoria                     │
│  User   │◄─────►│  ┌───────────┬───────────┬────────────┐  │
│         │       │  │ Episodic  │ Semantic  │   State    │  │
└────┬────┘       │  │ (DuckDB)  │ (USearch) │ (SQLite)   │  │
     │            │  │           │           │            │  │
     │            │  │ events    │ vector    │ mood,      │  │
     ▼            │  │ timeline  │ search    │ context,   │  │
┌─────────┐       │  │           │           │ decisions  │  │
│  Claude │       │  └───────────┴───────────┴────────────┘  │
│  Agent  │◄─────►│                                          │
│ (agent  │       │  REST API :8432                          │
│  .py)   │       │  MCP Server /mcp                         │
└─────────┘       └──────────────────────────────────────────┘
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (to run Ecphoria)
- Python 3.10+
- An [Anthropic API key](https://console.anthropic.com/)

## Quick Start

**1. Start Ecphoria**

```bash
docker run -d --name ecphoria \
  -p 8432:8432 -p 5432:5432 \
  ghcr.io/varga-foundation/ecphoria:latest
```

**2. Install dependencies**

```bash
cd examples/claude-agent
pip install -r requirements.txt
```

**3. Set your API key**

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

**4. Run the agent**

```bash
python agent.py
```

```
Connected to Ecphoria at http://localhost:8432
Type a message (or 'quit' to exit).

You: I'm working on a billing integration with Stripe
Agent: Got it! What aspect of the Stripe billing integration are you working on?

You: We discussed webhook handling last time, remember?
Agent: Yes, you mentioned working on the Stripe billing integration.
       Based on our past conversation, you were specifically looking at
       webhook handling. Want to pick up where we left off?
```

## How It Works

Each conversation turn follows a six-step loop:

1. **Ingest** — The user message is stored as an episodic event in Ecphoria
2. **Search** — Semantic memory is searched for related past interactions
3. **Read state** — The agent's current context (mood, topic, decisions) is loaded
4. **Reason** — Claude receives the user message enriched with memories and state
5. **Update state** — The agent's context is updated with new understanding
6. **Store response** — The assistant's reply is ingested as another event

Over time, the agent builds a rich history that it can draw on naturally.

## Using with Claude Code (MCP)

Ecphoria is MCP-native. Add it to your Claude Code configuration:

```bash
# Use the included config
claude mcp add-from-file mcp_config.json

# Or add manually
claude mcp add ecphoria --url http://localhost:8432/mcp
```

Claude Code can then directly use Ecphoria tools: `query`, `ingest`, `search`, `get_state`, `set_state`.

## What's Happening in Ecphoria

**Inspect events via psql:**

```bash
psql -h localhost -p 5432 -U postgres -c \
  "SELECT ts, event_type, payload->>'content' as content
   FROM episodic
   WHERE source = 'claude-agent'
   ORDER BY ts DESC LIMIT 5;"
```

**Search for related memories:**

```bash
curl -s http://localhost:8432/api/v1/embed-and-search \
  -H 'Content-Type: application/json' \
  -d '{"text": "billing integration", "k": 3}' | jq '.results[].content'
```

**Check agent state:**

```bash
curl -s http://localhost:8432/api/v1/state/claude-agent/context | jq
```

```json
{
  "mood": "engaged",
  "topic": "Stripe webhook handling",
  "decision_count": 7,
  "last_summary": "Discussed retry logic for failed webhook deliveries..."
}
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ECPHORIA_URL` | `http://localhost:8432` | Ecphoria server address |
| `ANTHROPIC_API_KEY` | (required) | Your Anthropic API key |
| `CLAUDE_MODEL` | `claude-sonnet-4-20250514` | Claude model to use |

## Next Steps

- Add [Ollama](https://ollama.ai/) for local embeddings: `ECPHORIA_EMBEDDING__PROVIDER=ollama`
- Connect a [Grafana dashboard](../grafana-dashboard/) to visualize agent activity
- Scale to [multiple agents](../multi-agent-support/) sharing context through Ecphoria
