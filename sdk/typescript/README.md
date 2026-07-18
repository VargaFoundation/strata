# @ecphoria/client

TypeScript/JavaScript client for the [Ecphoria](https://github.com/VargaFoundation/ecphoria) context lake.

Zero runtime dependencies — uses the native `fetch` API (Node 18+, Deno, Bun, browsers).

## Install

```bash
npm install @ecphoria/client
```

## Quick Start

```ts
import { EcphoriaClient } from "@ecphoria/client";

const client = new EcphoriaClient({ url: "http://localhost:8432" });

// Ingest events
await client.ingest("my-app", [
  { event_type: "user.signup", user_id: "u1", email: "alice@example.com" },
  { event_type: "page.view", user_id: "u1", path: "/dashboard" },
]);

// Query with SQL
const rows = await client.query("SELECT * FROM episodic ORDER BY ts DESC LIMIT 10");

// Semantic search (text → embed → search in one call)
const results = await client.find("frustrated customer billing issue", { k: 5 });

// Vector search (pre-computed embedding)
const hits = await client.search([0.1, 0.2, 0.3], { k: 5 });

// Agent state
await client.stateSet("bot-1", "mood", "happy");
const entry = await client.stateGet("bot-1", "mood");
await client.stateDelete("bot-1", "mood");

// Schema introspection
const sources = await client.sources();
const agents = await client.agents();

// Health & cluster
const health = await client.health();
const cluster = await client.clusterStatus();
```

## Authentication

```ts
const client = new EcphoriaClient({
  url: "http://localhost:8432",
  apiKey: "your-api-key",
});
```

## API Reference

| Method | Description |
|--------|-------------|
| `health()` | Check server health |
| `query(sql)` | Execute SQL against episodic store |
| `ingest(source, events)` | Ingest events into episodic memory |
| `search(vector, opts?)` | Semantic search by vector |
| `find(text, opts?)` | Semantic search by text (auto-embed) |
| `stateGet(agentId, key)` | Get agent state (null if not found) |
| `stateSet(agentId, key, value)` | Set agent state |
| `stateDelete(agentId, key)` | Delete agent state |
| `sources()` | List all event sources |
| `agents()` | List all agent IDs |
| `backup()` | Trigger backup |
| `enforceRetention()` | Enforce retention policy |
| `clusterStatus()` | Get Raft cluster status |

## License

Apache-2.0
