# Migrating from Mem0 to Ecphoria

Ecphoria's memory layer is **API-compatible in spirit** with Mem0 (add / search / get / history /
delete, scoped by user/agent/session) — but it's a single self-hostable Rust binary, your data never
leaves your servers, and the "intelligence" (bi-temporal facts, contradiction resolution, dedup,
hybrid + graph retrieval, decay) runs locally. This guide maps the common Mem0 calls to Ecphoria.

## Why switch

| | Mem0 | Ecphoria |
|---|---|---|
| Hosting | Cloud-first | Self-hosted single binary (Docker/K8s), data stays local |
| Graph memory | Paywalled | Built-in (bi-temporal edges, `graph_expansion`) |
| Query | API only | API **+ SQL** (PostgreSQL wire) over your memory |
| HA | Managed | Raft clustering built in |
| Beyond memory | — | Durable **agent runtime** (runs, HITL, workflows, triggers, tools) |

## Mapping

| Mem0 | Ecphoria REST | Ecphoria Python SDK |
|------|-------------|-------------------|
| `m.add(text, user_id=…)` | `POST /api/v1/memories` | `await s.memory_add(text, user_id=…)` |
| `m.search(q, user_id=…)` | `POST /api/v1/memories/search` | `await s.memory_search(q, user_id=…)` |
| `m.get_all(user_id=…)` | `GET /api/v1/memories?user_id=…` | `await s.memory_list(user_id=…)` |
| `m.history(memory_id)` | `GET /api/v1/memories/{id}/history` | `await s.memory_history(id)` |
| `m.delete(memory_id)` | `DELETE /api/v1/memories/{id}` | `await s.memory_delete(id)` |

### Before (Mem0)

```python
from mem0 import Memory
m = Memory()
m.add("Alice is on the Pro plan", user_id="alice")
hits = m.search("what plan is alice on?", user_id="alice")
```

### After (Ecphoria)

```python
from ecphoria import EcphoriaClient
async with EcphoriaClient("http://localhost:8432") as s:
    await s.memory_add("Alice is on the Pro plan", user_id="alice")
    hits = await s.memory_search("what plan is alice on?", user_id="alice")
```

That's the whole migration for basic usage — same scoping (`user_id` / `agent_id` / `session_id`),
same add/search/history/delete semantics. Ecphoria adds, for free:

- **Contradiction resolution**: a newer fact about the same `subject` supersedes the old one (kept
  for history) — deterministic, no LLM required.
- **Bi-temporal history**: ask *"what did we believe at time T?"* in plain SQL.
- **Hybrid + graph retrieval**: BM25 + vector + (optional) knowledge-graph expansion.
- **SQL access**: `SELECT … FROM memories` / `episodic` via the PostgreSQL wire protocol.

## Reproducible quality

Run the LoCoMo harness on your own data and see the numbers — we don't publish leaderboard claims we
can't reproduce. See [`benchmarks-locomo.md`](./benchmarks-locomo.md).

## Beyond memory

Once migrated, you also get the [agentic platform](./agentic-platform.md): durable agent runs, HITL,
workflows, event triggers, and an MCP tool-gateway — in the same binary.
