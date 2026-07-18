# Ecphoria Go SDK

Go client for the [Ecphoria](https://github.com/VargaFoundation/ecphoria) context lake.

Zero external dependencies — uses only the Go standard library (`net/http`, `encoding/json`).

## Install

```bash
go get github.com/VargaFoundation/ecphoria/sdk/go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/VargaFoundation/ecphoria/sdk/go/ecphoria"
)

func main() {
	ctx := context.Background()
	client := ecphoria.NewClient("http://localhost:8432", nil)

	// Ingest events
	n, err := client.Ingest(ctx, "my-app", []ecphoria.Event{
		{"event_type": "user.signup", "user_id": "u1"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Ingested %d events\n", n)

	// Query with SQL
	rows, err := client.Query(ctx, "SELECT * FROM episodic LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got %d rows\n", len(rows))

	// Semantic search by text
	results, err := client.Find(ctx, "billing issue", 5, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		fmt.Printf("  %.3f %s\n", r.Score, r.Content)
	}

	// Agent state
	_, err = client.StateSet(ctx, "bot-1", "mood", "happy")
	if err != nil {
		log.Fatal(err)
	}
	entry, err := client.StateGet(ctx, "bot-1", "mood")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("State: %+v\n", entry)

	// Schema
	sources, _ := client.Sources(ctx)
	agents, _ := client.Agents(ctx)
	fmt.Printf("Sources: %v, Agents: %v\n", sources, agents)
}
```

## Authentication

```go
client := ecphoria.NewClient("http://localhost:8432", &ecphoria.ClientOptions{
	APIKey: "your-api-key",
})
```

## API Reference

| Method | Description |
|--------|-------------|
| `Health(ctx)` | Check server health |
| `Query(ctx, sql)` | Execute SQL against episodic store |
| `Ingest(ctx, source, events)` | Ingest events into episodic memory |
| `Search(ctx, vector, k, filters)` | Semantic search by vector |
| `Find(ctx, text, k, filters)` | Semantic search by text (auto-embed) |
| `StateGet(ctx, agentID, key)` | Get agent state (nil if not found) |
| `StateSet(ctx, agentID, key, value)` | Set agent state |
| `StateDelete(ctx, agentID, key)` | Delete agent state |
| `Sources(ctx)` | List all event sources |
| `Agents(ctx)` | List all agent IDs |
| `Backup(ctx)` | Trigger backup |
| `EnforceRetention(ctx)` | Enforce retention policy |
| `ClusterStatus(ctx)` | Get Raft cluster status |

## License

Apache-2.0
