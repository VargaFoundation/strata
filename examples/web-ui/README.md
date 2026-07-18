# Ecphoria Explorer (web UI)

> **Looking for the admin console?** The server now ships a full **Admin UI** built in — just open
> **`http://localhost:8432/ui`** (or `/`). It covers overview/cluster status, memory search+add,
> agent runs with **HITL approvals**, SQL, sessions, triggers/tools, and admin ops (backup/restore/
> reindex/rebalance/retention/decay/consolidate/tenant export-import/audit). No file to serve — it's
> embedded in the binary and served publicly (it authenticates to the API with the key you enter).

This folder is the older **standalone** single-file explorer — a lighter, dependency-free playground
for SQL queries over episodic memory, hybrid memory search, agent **runs** + step **traces**, and a
one-click "run agent" form. Prefer `/ui` for operations; keep this for quick offline poking.

## Use

Just open `index.html` in a browser (or serve the folder), set the server URL (default
`http://localhost:8432`) and an optional API key, and click **Connect**.

```bash
# either open the file directly…
xdg-open examples/web-ui/index.html
# …or serve it
python3 -m http.server -d examples/web-ui 3000   # then visit http://localhost:3000
```

> For browser access, the Ecphoria server's CORS must allow your origin (it's permissive by default
> when auth is disabled; set `gateway.cors_origins` when auth is enabled).
