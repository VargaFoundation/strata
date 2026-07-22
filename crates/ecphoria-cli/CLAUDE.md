# ecphoria-cli

## Responsibility

CLI admin tool. Communicates with ecphoria-server over HTTP (REST API).
Binary name: `ecphoria`.

## Commands

```
ecphoria status                          # server health check
ecphoria doctor [--config ecphoria.toml]   # lint config for misconfigs (local, no server)
ecphoria query "SELECT ..."              # execute SQL
ecphoria ingest --source X --file Y      # bulk ingest
ecphoria export --entity ID              # GDPR data export (NDJSON)
ecphoria export --to obsidian --path DIR # export memories → Obsidian markdown vault (round-trip)
ecphoria import --from obsidian --path DIR [--watch]  # import vault; --watch = live human→agent sync
ecphoria search "text" -k 5              # semantic search
ecphoria shell                           # interactive SQL REPL
ecphoria schema                          # list sources / agent IDs

# Admin (need --token / ECPHORIA_TOKEN when the server has auth enabled)
ecphoria backup                          # server-side backup of all stores
ecphoria restore --path <dir>            # restore from a backup dir (DESTRUCTIVE)
ecphoria retention enforce|list|set --source X --days N
ecphoria audit [--since ISO] [--tenant T]
ecphoria tenant delete|export|import --tenant T [--file F]
ecphoria memory add "<fact>" [--subject S --user U --importance F]
ecphoria memory search "<query>" [--user U -k N]
ecphoria memory list|get <id>|history <id> [--user U]
ecphoria memory decay|consolidate|reembed
ecphoria graph centrality|communities [--as-of RFC3339]
ecphoria graph path --src A --dst B    # shortest path
ecphoria graph neighbors <entity> [--depth N --limit N]
ecphoria reindex                         # reindex unembedded events
ecphoria rebalance --tenant T --target-shard N
```

Global flags: `--url` (`ECPHORIA_URL`), `--token` (`ECPHORIA_TOKEN`, Bearer for admin routes).

## Internal Architecture

```
src/
  main.rs          Clap argument parsing, command dispatch
  client.rs        HTTP client (reqwest) to ecphoria-server REST API
  output.rs        Table/JSON output formatting
  commands/
    mod.rs         Command enum + dispatch
    status.rs      Health check
    query.rs       SQL execution
    ingest.rs      Bulk ingestion
    export.rs      GDPR export
    backup.rs      Backup trigger
    restore.rs     Restore trigger
```

## Testing

- `cargo test -p ecphoria-cli`
