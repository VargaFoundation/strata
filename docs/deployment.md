# Deployment Guide

## Deployment Modes

| Mode | Command | Best For |
|------|---------|----------|
| Docker | `docker run` | Development, single-node production |
| Docker Compose | `docker compose up` | Teams, development with full stack |
| Docker Compose Cluster | `docker compose -f deploy/docker-compose.cluster.yml up` | Local 3-node HA testing |
| Kubernetes | `helm install strata deploy/helm/strata/` | Production, high availability |
| Binary | `./strata-server` | Embedded, custom deployments |

## Docker (Standalone)

```bash
docker run -d \
  --name strata \
  -p 5432:5432 \
  -p 8432:8432 \
  -v strata-data:/data \
  ghcr.io/vargafoundation/strata:latest
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
git clone https://github.com/VargaFoundation/strata.git
cd strata
docker compose up -d
```

Services:
- **strata** — Context lake server
- **minio** — S3-compatible object storage (ports 9000, 9001)
- **minio-init** — One-shot bucket creation
- **ollama** — Local embedding model server (port 11434)

First-time setup for embeddings:

```bash
docker exec strata-ollama-1 ollama pull nomic-embed-text
```

## Docker Compose (3-Node Cluster)

Test a Raft cluster locally:

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d
```

This starts:
- **strata-1** (node_id=1, leader candidate) on port 8432
- **strata-2** (node_id=2) on port 8433
- **strata-3** (node_id=3) on port 8434
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
helm install strata deploy/helm/strata/ \
  --set replicaCount=3 \
  --set config.embedding.ollamaUrl=http://ollama:11434
```

### Architecture

The Helm chart deploys a StatefulSet with automatic cluster configuration:

```
StatefulSet (3 replicas)
  strata-0 (node_id=1) ─┐
  strata-1 (node_id=2) ─┤── Raft consensus via headless service DNS
  strata-2 (node_id=3) ─┘

Service (ClusterIP)       → load-balanced client reads
Headless Service          → direct pod-to-pod Raft RPCs
PodDisruptionBudget       → min 2 pods during rolling updates
ServiceMonitor (optional) → Prometheus scraping
```

Each pod automatically:
1. Derives its `node_id` from the StatefulSet ordinal (strata-0 → node_id=1)
2. Discovers peers via headless service DNS (`strata-1.strata-headless.namespace.svc.cluster.local:9433`)
3. Forms a Raft cluster and elects a leader

### Production Values

```yaml
# values-production.yaml
replicaCount: 3

image:
  repository: ghcr.io/vargafoundation/strata
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
helm install strata deploy/helm/strata/ -f values-production.yaml
```

### Helm Values Reference

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `3` | Number of Strata nodes |
| `image.repository` | `ghcr.io/vargafoundation/strata` | Docker image |
| `image.tag` | `latest` | Image tag |
| `config.cluster.enabled` | `true` | Enable Raft consensus |
| `config.gateway.authEnabled` | `false` | Enable API key auth |
| `config.gateway.apiKeys` | `[]` | List of valid API keys |
| `config.embedding.provider` | `ollama` | Embedding provider |
| `config.embedding.ollamaUrl` | `http://ollama:11434` | Ollama server URL |
| `persistence.enabled` | `true` | Enable persistent volumes |
| `persistence.size` | `10Gi` | Volume size per node |
| `persistence.storageClass` | `""` | Storage class (default: cluster default) |
| `service.type` | `ClusterIP` | Service type |
| `serviceMonitor.enabled` | `false` | Enable Prometheus ServiceMonitor |
| `podDisruptionBudget.enabled` | `true` | Enable PDB |
| `podDisruptionBudget.minAvailable` | `2` | Minimum available pods |

## Configuration Reference

Strata loads configuration from three sources (in order of precedence):

1. **Built-in defaults** — sensible defaults for local development
2. **`strata.toml`** — file in the working directory
3. **Environment variables** — prefixed with `STRATA_`, nested with `__`

### Server

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `gateway.listen` | `STRATA_GATEWAY__LISTEN` | `0.0.0.0:8432` | HTTP listen address |
| `gateway.pg_listen` | `STRATA_GATEWAY__PG_LISTEN` | `0.0.0.0:5432` | PG wire listen address |
| `gateway.grpc_listen` | `STRATA_GATEWAY__GRPC_LISTEN` | `0.0.0.0:9432` | gRPC listen address |
| `gateway.mcp_enabled` | `STRATA_GATEWAY__MCP_ENABLED` | `true` | Enable MCP server |
| `gateway.llm_proxy_enabled` | `STRATA_GATEWAY__LLM_PROXY_ENABLED` | `false` | Enable LLM proxy |
| `gateway.auth_enabled` | `STRATA_GATEWAY__AUTH_ENABLED` | `false` | Enable API key authentication |
| `gateway.max_pg_connections` | `STRATA_GATEWAY__MAX_PG_CONNECTIONS` | `256` | Max concurrent PG wire connections |

### Storage

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `storage.data_dir` | `STRATA_STORAGE__DATA_DIR` | `./data` | Local data directory |
| `storage.engine` | `STRATA_STORAGE__ENGINE` | `local` | `local` or `s3` |
| `storage.s3.endpoint` | `STRATA_STORAGE__S3__ENDPOINT` | | S3 endpoint URL |
| `storage.s3.bucket` | `STRATA_STORAGE__S3__BUCKET` | `strata` | S3 bucket name |
| `storage.s3.access_key` | `STRATA_STORAGE__S3__ACCESS_KEY` | | S3 access key |
| `storage.s3.secret_key` | `STRATA_STORAGE__S3__SECRET_KEY` | | S3 secret key |
| `storage.s3.region` | `STRATA_STORAGE__S3__REGION` | `us-east-1` | S3 region |

### Memory

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `memory.episodic.db_path` | `STRATA_MEMORY__EPISODIC__DB_PATH` | `:memory:` | DuckDB path (`:memory:` or file path) |
| `memory.episodic.default_retention_days` | `STRATA_MEMORY__EPISODIC__DEFAULT_RETENTION_DAYS` | `365` | Default event retention |
| `memory.semantic.index_dir` | `STRATA_MEMORY__SEMANTIC__INDEX_DIR` | `./data/vectors` | Vector index directory |
| `memory.semantic.default_dimension` | `STRATA_MEMORY__SEMANTIC__DEFAULT_DIMENSION` | `768` | Default vector dimensions |
| `memory.semantic.metric` | `STRATA_MEMORY__SEMANTIC__METRIC` | `cosine` | Distance metric |
| `memory.state.db_path` | `STRATA_MEMORY__STATE__DB_PATH` | `./data/state.db` | State DB file path |

### Embedding

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `embedding.provider` | `STRATA_EMBEDDING__PROVIDER` | `ollama` | `ollama` or `openai` |
| `embedding.model` | `STRATA_EMBEDDING__MODEL` | `nomic-embed-text` | Model name |
| `embedding.dimension` | `STRATA_EMBEDDING__DIMENSION` | `768` | Vector dimension |
| `embedding.batch_size` | `STRATA_EMBEDDING__BATCH_SIZE` | `64` | Max texts per embedding API call |
| `embedding.ollama_url` | `STRATA_EMBEDDING__OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |
| `embedding.openai_api_key` | `STRATA_EMBEDDING__OPENAI_API_KEY` | | OpenAI API key |

### Query

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `query.max_rows` | `STRATA_QUERY__MAX_ROWS` | `10000` | Max rows returned per query |
| `query.timeout_ms` | `STRATA_QUERY__TIMEOUT_MS` | `30000` | Query timeout in milliseconds |

### Cluster

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `cluster.enabled` | `STRATA_CLUSTER__ENABLED` | `false` | Enable Raft cluster mode |
| `cluster.node_id` | `STRATA_CLUSTER__NODE_ID` | `1` | This node's Raft ID |
| `cluster.listen` | `STRATA_CLUSTER__LISTEN` | `0.0.0.0:9433` | Raft RPC listen address |
| `cluster.peers` | `STRATA_CLUSTER__PEERS` | `[]` | Comma-separated peer addresses |

## Production Checklist

Security controls are summarized here; see [security.md](security.md) and
[threat-model.md](threat-model.md) for the full model.

**Security**
- [ ] **Auth**: `gateway.auth_enabled = true` with real credentials (or OIDC); `jwt_secret` ≥32 bytes.
      (Strata refuses to start unauthenticated on a non-loopback bind unless `allow_insecure=true`.)
- [ ] **Hashed keys**: provide `api_keys` pre-hashed as `sha256:<hex>@tenant:role` (no plaintext at rest).
- [ ] **TLS**: front REST/gRPC with a TLS-terminating proxy/Ingress. **PG wire (:5432) has no TLS** —
      keep it on loopback / a private subnet (its password is the API key).
- [ ] **Cluster secret**: set `STRATA_CLUSTER__SECRET` on every node (multi-node).
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
- [ ] **Logging**: Set `RUST_LOG=info,strata=debug` for production logging

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
# strata_episodic_events_ingested_total 1234
# strata_episodic_query_duration_seconds_bucket{le="0.01"} 567
# ...
```

For Docker, the built-in HEALTHCHECK uses `curl http://localhost:8432/health`.
For Kubernetes, liveness and readiness probes are configured in the Helm chart.

### Distributed tracing (OTLP)

Metrics ship to Prometheus out of the box. For **traces**, build with the `otlp` feature and point
Strata at an OTLP collector (Tempo, Jaeger, Grafana Agent, OpenTelemetry Collector). Spans export
over OTLP/HTTP in parallel with the Prometheus exporter.

```bash
cargo build --release -p strata-server --features otlp
# Full traces endpoint (OTLP/HTTP defaults to :4318):
STRATA_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces strata-server
```

When the feature is absent or `STRATA_OTLP_ENDPOINT` is unset, only the fmt logger + Prometheus run
(zero overhead). The service reports as `service.name=strata-server`.
