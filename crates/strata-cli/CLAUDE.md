# strata-cli

## Responsibility

CLI admin tool. Communicates with strata-server over HTTP (REST API).
Binary name: `strata`.

## Commands

```
strata status                          # server health check
strata query "SELECT ..."              # execute SQL
strata ingest --source X --file Y      # bulk ingest
strata export --entity ID              # GDPR data export
strata search "text" -k 5              # semantic search
strata shell                           # interactive SQL REPL
strata schema                          # list sources / agent IDs

# Admin (need --token / STRATA_TOKEN when the server has auth enabled)
strata backup                          # server-side backup of all stores
strata restore --path <dir>            # restore from a backup dir (DESTRUCTIVE)
strata retention enforce|list|set --source X --days N
strata audit [--since ISO] [--tenant T]
strata tenant delete|export|import --tenant T [--file F]
strata memory decay|consolidate
strata reindex                         # reindex unembedded events
strata rebalance --tenant T --target-shard N
```

Global flags: `--url` (`STRATA_URL`), `--token` (`STRATA_TOKEN`, Bearer for admin routes).

## Internal Architecture

```
src/
  main.rs          Clap argument parsing, command dispatch
  client.rs        HTTP client (reqwest) to strata-server REST API
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

- `cargo test -p strata-cli`
