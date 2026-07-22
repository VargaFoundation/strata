# Deployment Guide

## Deployment Modes

| Mode | Command | Best For |
|------|---------|----------|
| Docker | `docker run` | Development, single-node production |
| Docker Compose | `docker compose up` | Teams, development with full stack |
| Docker Compose Cluster | `docker compose -f deploy/docker-compose.cluster.yml up` | Local 3-node HA testing |
| Kubernetes | `helm install ecphoria deploy/helm/ecphoria/` | Production, high availability |
| Binary | `./ecphoria-server` | Embedded, custom deployments |

## Docker (Standalone)

```bash
docker run -d \
  --name ecphoria \
  -p 5432:5432 \
  -p 8432:8432 \
  -v ecphoria-data:/data \
  ghcr.io/vargafoundation/ecphoria:latest
```

The Docker image includes a built-in HEALTHCHECK on the `/health` endpoint.

### Volumes

| Path | Purpose |
|------|---------|
| `/data` | All persistent data: DuckDB episodic database, USearch vector index, SQLite state DB |

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 5432 | TCP | PostgreSQL wire protocol |
| 8432 | HTTP | REST API, MCP server, LLM proxy, Prometheus metrics, health checks |
| 9432 | HTTP/2 | gRPC |
| 9433 | HTTP | Raft inter-node RPC (cluster mode only) |

## Docker Compose (Full Stack)

```bash
git clone https://github.com/VargaFoundation/ecphoria.git
cd ecphoria
docker compose up -d
```

Services:
- **ecphoria** — Context lake server
- **minio** — S3-compatible object storage (ports 9000, 9001)
- **minio-init** — One-shot bucket creation
- **ollama** — Local embedding model server (port 11434)

First-time setup for embeddings:

```bash
docker exec ecphoria-ollama-1 ollama pull nomic-embed-text
```

## Docker Compose (3-Node Cluster)

Test a Raft cluster locally:

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d
```

This starts:
- **ecphoria-1** (node_id=1, leader candidate) on port 8432
- **ecphoria-2** (node_id=2) on port 8433
- **ecphoria-3** (node_id=3) on port 8434
- **ollama** — shared embedding server

Check cluster status:

```bash
curl http://localhost:8432/cluster/status
# {"node_id":1,"state":"Leader","current_leader":1,"current_term":1,...}
```

Writes to a follower node return a 307 redirect to the leader:

```bash
curl -X POST http://localhost:8433/api/v1/ingest -d '...'
# 307 {"error":"not_leader","leader_id":1,"message":"..."}
```

## Kubernetes (Helm)

### Quick Start

```bash
helm install ecphoria deploy/helm/ecphoria/ \
  --set replicaCount=3 \
  --set config.embedding.ollamaUrl=http://ollama:11434
```

### Architecture

The Helm chart deploys a StatefulSet with automatic cluster configuration:

```
StatefulSet (3 replicas)
  ecphoria-0 (node_id=1) ─┐
  ecphoria-1 (node_id=2) ─┤── Raft consensus via headless service DNS
  ecphoria-2 (node_id=3) ─┘

Service (ClusterIP)       → load-balanced client reads
Headless Service          → direct pod-to-pod Raft RPCs
PodDisruptionBudget       → min 2 pods during rolling updates
ServiceMonitor (optional) → Prometheus scraping
```

Each pod automatically:
1. Derives its `node_id` from the StatefulSet ordinal (ecphoria-0 → node_id=1)
2. Discovers peers via headless service DNS (`ecphoria-1.ecphoria-headless.namespace.svc.cluster.local:9433`)
3. Forms a Raft cluster and elects a leader

### Production Values

```yaml
# values-production.yaml
replicaCount: 3

image:
  repository: ghcr.io/vargafoundation/ecphoria
  tag: "0.1.0"

config:
  storage:
    engine: local
  memory:
    episodic:
      dbPath: /data/episodic.duckdb
    semantic:
      indexDir: /data/vectors
    state:
      dbPath: /data/state.db
  gateway:
    authEnabled: true
    apiKeys:
      - "your-secret-api-key"
  cluster:
    enabled: true
  embedding:
    provider: ollama
    model: nomic-embed-text
    ollamaUrl: "http://ollama.default.svc.cluster.local:11434"

persistence:
  enabled: true
  storageClass: gp3
  size: 50Gi

resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"

serviceMonitor:
  enabled: true
  interval: 15s

podDisruptionBudget:
  enabled: true
  minAvailable: 2
```

Install:

```bash
helm install ecphoria deploy/helm/ecphoria/ -f values-production.yaml
```

### Helm Values Reference

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `3` | Number of Ecphoria nodes |
| `image.repository` | `ghcr.io/vargafoundation/ecphoria` | Docker image |
| `image.tag` | `latest` | Image tag |
| `config.cluster.enabled` | `true` | Enable Raft consensus |
| `config.gateway.authEnabled` | `false` | Enable API key auth |
| `config.gateway.apiKeys` | `[]` | List of valid API keys |
| `config.gateway.mcpEnabled` | `true` | Enable the MCP server |
| `config.gateway.llmProxyEnabled` | `false` | Enable the `/v1/*` LLM proxy |
| `config.gateway.allowInsecure` | `false` | Opt out of the secure-by-default guard |
| `config.gateway.oidc.enabled` | `false` | Enable OIDC (RS256/JWKS) auth |
| `config.gateway.jwtSecretExistingSecret` | `""` | Name of a Secret (key `jwt-secret`) for JWT HS256 |
| `config.embedding.provider` | `ollama` | Embedding provider |
| `config.embedding.ollamaUrl` | `http://ollama:11434` | Ollama server URL |
| `persistence.enabled` | `true` | Enable persistent volumes |
| `persistence.size` | `10Gi` | Volume size per node |
| `persistence.storageClass` | `""` | Storage class (default: cluster default) |
| `service.type` | `ClusterIP` | Service type |
| `serviceMonitor.enabled` | `false` | Enable Prometheus ServiceMonitor |
| `podDisruptionBudget.enabled` | `true` | Enable PDB |
| `podDisruptionBudget.minAvailable` | `2` | Minimum available pods |

> The chart also exposes the full set of advanced `config.gateway.*` tuning keys — `cdcSinkUrl`,
> `publishEnabled`/`publishTenant`, `webhookSecrets`/`webhookRequireSignature`, `rateLimitPerKey`,
> `corsOrigins`, `requireTenant`, `auditDbPath`, `maxPgConnections`, `llmCacheSimilarity`,
> `toolGatewayAllowPrivateNetworks`, and the `oidc.*` block. Each is optional (empty/`0`/`false` emits
> no env var) and maps 1:1 to the [Gateway — security & advanced](#gateway--security--advanced) env
> vars below. See `deploy/helm/ecphoria/values.yaml` for the complete list.

## Configuration Reference

Ecphoria loads configuration from three sources (in order of precedence):

1. **Built-in defaults** — sensible defaults for local development
2. **`ecphoria.toml`** — file in the working directory
3. **Environment variables** — prefixed with `ECPHORIA_`, nested with `__`

### Server

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `gateway.listen` | `ECPHORIA_GATEWAY__LISTEN` | `0.0.0.0:8432` | HTTP listen address |
| `gateway.pg_listen` | `ECPHORIA_GATEWAY__PG_LISTEN` | `0.0.0.0:5432` | PG wire listen address |
| `gateway.grpc_listen` | `ECPHORIA_GATEWAY__GRPC_LISTEN` | `0.0.0.0:9432` | gRPC listen address |
| `gateway.mcp_enabled` | `ECPHORIA_GATEWAY__MCP_ENABLED` | `true` | Enable MCP server |
| `gateway.llm_proxy_enabled` | `ECPHORIA_GATEWAY__LLM_PROXY_ENABLED` | `false` | Enable LLM proxy |
| `gateway.auth_enabled` | `ECPHORIA_GATEWAY__AUTH_ENABLED` | `false` | Enable API key authentication |
| `gateway.max_pg_connections` | `ECPHORIA_GATEWAY__MAX_PG_CONNECTIONS` | `256` | Max concurrent PG wire connections |

### Gateway — security & advanced

All optional; empty / `0` / `false` means "off / use default". Comma-separated lists (`api_keys`,
`cors_origins`, `webhook_secrets`) accept either a TOML array or a single comma-joined string.

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `gateway.allow_insecure` | `ECPHORIA_GATEWAY__ALLOW_INSECURE` | `false` | Opt out of the secure-by-default guard (bind non-loopback without auth). Trusted networks only |
| `gateway.api_keys` | `ECPHORIA_GATEWAY__API_KEYS` | `[]` | Valid keys as `<secret>@<tenant>:<role>` or hashed `sha256:<hex>@<tenant>:<role>` |
| `gateway.jwt_secret` | `ECPHORIA_GATEWAY__JWT_SECRET` | | HS256 signing secret for JWT auth |
| `gateway.cors_origins` | `ECPHORIA_GATEWAY__CORS_ORIGINS` | `[]` | Allowed CORS origins (exact match) |
| `gateway.rate_limit_per_key` | `ECPHORIA_GATEWAY__RATE_LIMIT_PER_KEY` | `0` | Requests/sec per API key (0 = unlimited) |
| `gateway.audit_db_path` | `ECPHORIA_GATEWAY__AUDIT_DB_PATH` | | Durable audit-log SQLite path (e.g. `/data/audit.db`); empty = in-memory only |
| `gateway.require_tenant` | `ECPHORIA_GATEWAY__REQUIRE_TENANT` | `false` | Reject requests that resolve to no tenant |
| `gateway.webhook_secret` | `ECPHORIA_GATEWAY__WEBHOOK_SECRET` | | Single shared webhook HMAC secret (all sources) |
| `gateway.webhook_secrets` | `ECPHORIA_GATEWAY__WEBHOOK_SECRETS` | `[]` | Per-vendor secrets: `github=<hmac>`, `slack=<signing-secret>`, … |
| `gateway.webhook_require_signature` | `ECPHORIA_GATEWAY__WEBHOOK_REQUIRE_SIGNATURE` | `false` | Fail-closed: reject webhook sources with no configured secret |
| `gateway.llm_cache_similarity` | `ECPHORIA_GATEWAY__LLM_CACHE_SIMILARITY` | `false` | Semantic (vs exact-match) LLM-proxy response cache |
| `gateway.tool_gateway_allow_private_networks` | `ECPHORIA_GATEWAY__TOOL_GATEWAY_ALLOW_PRIVATE_NETWORKS` | `false` | Let the MCP tool-gateway reach RFC1918 addresses (disables the SSRF guard) |
| `gateway.cdc_sink_url` | `ECPHORIA_GATEWAY__CDC_SINK_URL` | | Outbound CDC: POST every memory change to this URL (leader-gated in cluster mode) |
| `gateway.publish_enabled` | `ECPHORIA_GATEWAY__PUBLISH_ENABLED` | `false` | Serve UNAUTH read-only `/public` + `/public/memories` |
| `gateway.publish_tenant` | `ECPHORIA_GATEWAY__PUBLISH_TENANT` | `default` | Tenant whose `metadata.published=true` memories are exposed at `/public` |
| `gateway.oidc.enabled` | `ECPHORIA_GATEWAY__OIDC__ENABLED` | `false` | Enable OIDC (RS256/JWKS) auth |
| `gateway.oidc.issuer_url` | `ECPHORIA_GATEWAY__OIDC__ISSUER_URL` | | OIDC issuer URL |
| `gateway.oidc.jwks_uri` | `ECPHORIA_GATEWAY__OIDC__JWKS_URI` | | JWKS URI (defaults to `<issuer>/.well-known/jwks.json` if empty) |
| `gateway.oidc.audience` | `ECPHORIA_GATEWAY__OIDC__AUDIENCE` | | Expected `aud` claim |
| `gateway.oidc.role_claim` | `ECPHORIA_GATEWAY__OIDC__ROLE_CLAIM` | `roles` | JWT claim carrying RBAC roles |
| `gateway.pg_tls.cert_path` / `key_path` | `ECPHORIA_GATEWAY__PG_TLS__CERT_PATH` / `..__KEY_PATH` | | PEM cert+key enabling TLS on the PG-wire listener (encrypts the password = API key in transit) |

### Storage

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `storage.data_dir` | `ECPHORIA_STORAGE__DATA_DIR` | `./data` | Local data directory |
| `storage.engine` | `ECPHORIA_STORAGE__ENGINE` | `local` | `local` or `s3` |
| `storage.s3.endpoint` | `ECPHORIA_STORAGE__S3__ENDPOINT` | | S3 endpoint URL |
| `storage.s3.bucket` | `ECPHORIA_STORAGE__S3__BUCKET` | `ecphoria` | S3 bucket name |
| `storage.s3.access_key` | `ECPHORIA_STORAGE__S3__ACCESS_KEY` | | S3 access key |
| `storage.s3.secret_key` | `ECPHORIA_STORAGE__S3__SECRET_KEY` | | S3 secret key |
| `storage.s3.region` | `ECPHORIA_STORAGE__S3__REGION` | `us-east-1` | S3 region |

### Memory

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `memory.episodic.db_path` | `ECPHORIA_MEMORY__EPISODIC__DB_PATH` | `./data/episodic.duckdb` | DuckDB path (`:memory:` or file path) |
| `memory.episodic.default_retention_days` | `ECPHORIA_MEMORY__EPISODIC__DEFAULT_RETENTION_DAYS` | `365` | Default event retention |
| `memory.semantic.index_dir` | `ECPHORIA_MEMORY__SEMANTIC__INDEX_DIR` | `./data/vectors` | Vector index directory |
| `memory.semantic.default_dimension` | `ECPHORIA_MEMORY__SEMANTIC__DEFAULT_DIMENSION` | `768` | Default vector dimensions |
| `memory.semantic.metric` | `ECPHORIA_MEMORY__SEMANTIC__METRIC` | `cosine` | Distance metric |
| `memory.state.db_path` | `ECPHORIA_MEMORY__STATE__DB_PATH` | `./data/state.db` | State DB file path |

### Memory — cognition (the bi-temporal `memories` layer)

The deterministic core (subject-based contradiction resolution, dedup, importance) is always on.
LLM fact extraction and graph auto-population are opt-in.

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `memory.cognition.db_path` | `ECPHORIA_MEMORY__COGNITION__DB_PATH` | `./data/memories.duckdb` | DuckDB path for the bi-temporal `memories` table |
| `memory.cognition.dedup_threshold` | `ECPHORIA_MEMORY__COGNITION__DEDUP_THRESHOLD` | `0.92` | Cosine similarity at/above which a new memory merges into an existing one |
| `memory.cognition.extraction` | `ECPHORIA_MEMORY__COGNITION__EXTRACTION` | `none` | `remember` fact extraction: `none` (store as-is) or `llm` |
| `memory.cognition.extraction_provider` | `ECPHORIA_MEMORY__COGNITION__EXTRACTION_PROVIDER` | `none` | LLM backend for extraction: `ollama` \| `openai` \| `none` |
| `memory.cognition.extraction_model` | `ECPHORIA_MEMORY__COGNITION__EXTRACTION_MODEL` | `llama3.2` | Model used for LLM extraction |
| `memory.cognition.default_importance` | `ECPHORIA_MEMORY__COGNITION__DEFAULT_IMPORTANCE` | `0.5` | Importance assigned to a new memory (0.0–1.0) |
| `memory.cognition.decay_half_life_days` | `ECPHORIA_MEMORY__COGNITION__DECAY_HALF_LIFE_DAYS` | `30` | Half-life (days) for time-decay of importance |
| `memory.cognition.forget_threshold` | `ECPHORIA_MEMORY__COGNITION__FORGET_THRESHOLD` | `0.05` | Memories whose decayed importance falls below this are forgotten |
| `memory.cognition.read_pool_size` | `ECPHORIA_MEMORY__COGNITION__READ_POOL_SIZE` | `4` | Read-connection count (query concurrency) |
| `memory.cognition.max_memories_per_scope` | `ECPHORIA_MEMORY__COGNITION__MAX_MEMORIES_PER_SCOPE` | `0` | Per-scope active-memory cap (0 = unlimited) |
| `memory.cognition.retrieval_scan_cap` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_SCAN_CAP` | `2000` | Candidate width scanned per query (BM25 + vector) |
| `memory.cognition.retrieval_pool` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_POOL` | `200` | Fused pool kept after RRF for blend + rerank |
| `memory.cognition.graph_expansion` | `ECPHORIA_MEMORY__COGNITION__GRAPH_EXPANSION` | `false` | Query-time knowledge-graph expansion in `memory_search` |
| `memory.cognition.auto_graph` | `ECPHORIA_MEMORY__COGNITION__AUTO_GRAPH` | `false` | Auto-populate graph edges from each added memory |
| `memory.cognition.retrieval_importance_weight` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_IMPORTANCE_WEIGHT` | `0.3` | Importance weight in the retrieval blend (0 = pure relevance) |
| `memory.cognition.retrieval_recency_weight` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_RECENCY_WEIGHT` | `0.2` | Recency weight (30-day half-life) in the retrieval blend |
| `memory.cognition.retrieval_vector_weight` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_VECTOR_WEIGHT` | `1.0` | Weighted-RRF weight of the vector arm |
| `memory.cognition.retrieval_lexical_weight` | `ECPHORIA_MEMORY__COGNITION__RETRIEVAL_LEXICAL_WEIGHT` | `1.0` | Weighted-RRF weight of the lexical (BM25) arm |
| `memory.cognition.contradiction_review` | `ECPHORIA_MEMORY__COGNITION__CONTRADICTION_REVIEW` | `false` | HITL: contradictions queue for review instead of auto-superseding |
| `memory.cognition.decay_interval_secs` | `ECPHORIA_MEMORY__COGNITION__DECAY_INTERVAL_SECS` | `0` | Leader forgets decayed memories on this interval (0 = off) |

### Embedding

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `embedding.provider` | `ECPHORIA_EMBEDDING__PROVIDER` | `none` | `ollama` \| `openai` \| `local` (in-process ONNX, build `--features embed-local`) \| `none`. The shipped `ecphoria.toml` sets `ollama`; the built-in default (no config file) is `none` |
| `embedding.model` | `ECPHORIA_EMBEDDING__MODEL` | `nomic-embed-text` | Model name |
| `embedding.dimension` | `ECPHORIA_EMBEDDING__DIMENSION` | `768` | Vector dimension |
| `embedding.batch_size` | `ECPHORIA_EMBEDDING__BATCH_SIZE` | `64` | Max texts per embedding API call |
| `embedding.ollama_url` | `ECPHORIA_EMBEDDING__OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |
| `embedding.openai_api_key` | `ECPHORIA_EMBEDDING__OPENAI_API_KEY` | | OpenAI API key |

### Query

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `query.max_rows` | `ECPHORIA_QUERY__MAX_ROWS` | `10000` | Max rows returned per query |
| `query.timeout_ms` | `ECPHORIA_QUERY__TIMEOUT_MS` | `30000` | Query timeout in milliseconds |

### Reranking (read-path, opt-in)

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `rerank.provider` | `ECPHORIA_RERANK__PROVIDER` | `none` | `none` (off) or `llm` (LLM relevance judge). A local cross-encoder is available via `--features rerank-local` |
| `rerank.backend` | `ECPHORIA_RERANK__BACKEND` | `ollama` | Completion backend for `provider = llm`: `ollama` or `openai` |
| `rerank.model` | `ECPHORIA_RERANK__MODEL` | `llama3.2` | Reranker model name |
| `rerank.candidates` | `ECPHORIA_RERANK__CANDIDATES` | `50` | Fused candidates to over-fetch and rerank before truncating to `k` |

### Runtime (agent-run ledger)

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `runtime.db_path` | `ECPHORIA_RUNTIME__DB_PATH` | `./data/runs.db` | SQLite path for the durable agent-run ledger |

### Backup

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `backup.auto_enabled` | `ECPHORIA_BACKUP__AUTO_ENABLED` | `false` | Background S3 backups in the tiering task |
| `backup.interval_hours` | `ECPHORIA_BACKUP__INTERVAL_HOURS` | `24` | Hours between automatic backups |
| `backup.s3_prefix` | `ECPHORIA_BACKUP__S3_PREFIX` | `backups/` | Key prefix for S3 backups |
| `backup.max_backups` | `ECPHORIA_BACKUP__MAX_BACKUPS` | `7` | Local backup dirs to retain under `<data_dir>/backups` (0 = keep all) |

### Cluster

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `cluster.enabled` | `ECPHORIA_CLUSTER__ENABLED` | `false` | Enable Raft cluster mode |
| `cluster.node_id` | `ECPHORIA_CLUSTER__NODE_ID` | `1` | This node's Raft ID |
| `cluster.listen` | `ECPHORIA_CLUSTER__LISTEN` | `0.0.0.0:9433` | Raft RPC listen address |
| `cluster.peers` | `ECPHORIA_CLUSTER__PEERS` | `[]` | Comma-separated peer addresses |

## Sharded operations (multi-Raft)

When `cluster.shards > 1`, each shard is an independent Raft group owning a disjoint slice of tenants
(consistent-hash routing). A few operator notes:

**Cluster-wide admin.** `POST /api/v1/admin/backup`, `/admin/reindex`, and `/admin/retention`
**scatter-gather** across all shards: one call runs the op on the receiving shard *and* fans out to
every peer shard, returning a per-shard breakdown:

```json
{
  "cluster": true,
  "partial": false,
  "shards": [
    { "shard": 0, "status": "ok", "result": { "deleted": 42 } },
    { "shard": 1, "status": "ok", "result": { "deleted": 17 } }
  ]
}
```

If any shard fails, the response is **HTTP 207 (Multi-Status)** with `"partial": true` and a
`"status": "error"` entry for that shard — never a silent `200`. Note that **backup remains N
per-shard artifacts** (each shard's data lives on its own pods); the response is the manifest of
where each landed. For scheduled jobs, run a CronJob per shard StatefulSet, or point each shard's
backup at its own S3 prefix.

**Protocol routing.** REST, MCP, and the LLM proxy are **transparently reverse-proxied** to a
tenant's owning shard, so the official SDKs (Go/Python/TS, all REST) and Claude/MCP clients need no
special handling. **gRPC and PostgreSQL-wire** are *reject-with-owner* instead: a raw client
connecting to the wrong shard for its tenant gets a clear error naming the owning shard's address and
must reconnect (never wrong data). For a transparent sharded PostgreSQL front, put a tenant-aware
pooler such as **pgcat** in front of the shard Services.

## Production Checklist

Security controls are summarized here; see [security.md](security.md) and
[threat-model.md](threat-model.md) for the full model.

**Security**
- [ ] **Auth**: `gateway.auth_enabled = true` with real credentials (or OIDC); `jwt_secret` ≥32 bytes.
      (Ecphoria refuses to start unauthenticated on a non-loopback bind unless `allow_insecure=true`.)
- [ ] **Hashed keys**: provide `api_keys` pre-hashed as `sha256:<hex>@tenant:role` (no plaintext at rest).
- [ ] **TLS**: front REST/gRPC with a TLS-terminating proxy/Ingress. **PG wire (:5432) has no TLS** —
      keep it on loopback / a private subnet (its password is the API key).
- [ ] **Cluster secret**: set `ECPHORIA_CLUSTER__SECRET` on every node (multi-node).
- [ ] **Webhooks**: per-source `webhook_secrets` + `webhook_require_signature = true` (fail-closed).
- [ ] **Tool gateway**: leave `tool_gateway_allow_private_networks = false` unless downstream MCP
      servers live on a trusted private network.
- [ ] **Network**: NetworkPolicy so `:5432`/`:9432`/`:9433` are not publicly reachable.

**Reliability & ops**
- [ ] **Persistence**: Set `memory.episodic.db_path` to a file path (not `:memory:`)
- [ ] **Storage**: Configure persistent volumes for `/data` (encrypted volumes for data-at-rest)
- [ ] **Embedding**: Set up Ollama or configure OpenAI API key
- [ ] **Monitoring**: Enable ServiceMonitor or scrape `/metrics` with Prometheus
- [ ] **Cluster**: Enable `cluster.enabled = true` with 3+ replicas for HA
- [ ] **PDB**: Ensure PodDisruptionBudget is enabled for rolling updates
- [ ] **Backups**: Schedule regular backups of `/data`; run a **restore drill** (manifest verification)
- [ ] **Retention**: Configure `memory.episodic.default_retention_days`
- [ ] **Resources**: Allocate sufficient memory for vector indices (~4 bytes × dimensions × vectors)
- [ ] **Logging**: Set `RUST_LOG=info,ecphoria=debug` for production logging

## Health Checks

```bash
# HTTP health check
curl http://localhost:8432/health
# {"status":"ok","version":"0.1.0"}

# Cluster status (when cluster mode enabled)
curl http://localhost:8432/cluster/status
# {"node_id":1,"state":"Leader","current_leader":1,"current_term":2,...}

# Prometheus metrics
curl http://localhost:8432/metrics
# ecphoria_episodic_events_ingested_total 1234
# ecphoria_episodic_query_duration_seconds_bucket{le="0.01"} 567
# ...
```

For Docker, the built-in HEALTHCHECK uses `curl http://localhost:8432/health`.
For Kubernetes, liveness and readiness probes are configured in the Helm chart.

### Distributed tracing (OTLP)

Metrics ship to Prometheus out of the box. For **traces**, build with the `otlp` feature and point
Ecphoria at an OTLP collector (Tempo, Jaeger, Grafana Agent, OpenTelemetry Collector). Spans export
over OTLP/HTTP in parallel with the Prometheus exporter.

```bash
cargo build --release -p ecphoria-server --features otlp
# Full traces endpoint (OTLP/HTTP defaults to :4318):
ECPHORIA_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces ecphoria-server
```

When the feature is absent or `ECPHORIA_OTLP_ENDPOINT` is unset, only the fmt logger + Prometheus run
(zero overhead). The service reports as `service.name=ecphoria-server`.
