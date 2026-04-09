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
strata backup --target s3://...        # trigger backup
strata restore --from s3://...         # restore from backup
```

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
