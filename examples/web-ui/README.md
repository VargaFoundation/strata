# Strata Explorer (web UI)

A single-file, dependency-free web UI for exploring a running Strata instance — SQL queries over
episodic memory, hybrid memory search, agent **runs** + their step **traces**, and a one-click
"run agent" form.

## Use

Just open `index.html` in a browser (or serve the folder), set the server URL (default
`http://localhost:8432`) and an optional API key, and click **Connect**.

```bash
# either open the file directly…
xdg-open examples/web-ui/index.html
# …or serve it
python3 -m http.server -d examples/web-ui 3000   # then visit http://localhost:3000
```

> For browser access, the Strata server's CORS must allow your origin (it's permissive by default
> when auth is disabled; set `gateway.cors_origins` when auth is enabled).
