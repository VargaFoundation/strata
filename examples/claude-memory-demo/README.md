# Demo: Claude remembers across sessions

A 30-second, API-key-free demonstration of Strata's cognition layer — the thing that makes it
*memory*, not just storage: dedup, **contradiction resolution**, scoped recall, and bi-temporal
history.

## Run it

1. Start Strata (any of):
   ```bash
   docker run -p 8432:8432 ghcr.io/vargafoundation/strata:latest
   # or
   cargo run --bin strata-server
   ```
2. Run the demo (needs `curl` and `jq`):
   ```bash
   ./demo.sh
   ```

You'll see a fact get **superseded** by a contradicting one (the old value is kept as history),
then recalled in a "new session", then its full temporal history.

## Putting Claude in the loop

This demo calls Strata's REST API directly. To have **Claude** drive it, define two Anthropic
tools (`remember`, `recall`) that POST to `/api/v1/memories` and `/api/v1/memories/search`, scoped
by `user_id`. See [`../../docs/connect-claude.md`](../../docs/connect-claude.md) and the runnable
[`../claude-agent/`](../claude-agent/) example.

## Why this matters

Memory **persists across sessions and process restarts** (file-backed DuckDB/SQLite/USearch), and
it's *self-hosted* — the intelligence runs on your infrastructure, not a vendor's cloud. That's
the gap Strata fills versus cloud-first Mem0 and Zep's paywalled graph.
