# Deployment Guide

## Deployment Modes

| Mode | Command | Best For |
|------|---------|----------|
| Docker | `docker run` | Development, single-node production |
| Docker Compose | `docker compose up` | Teams, development with full stack |
| Kubernetes | `helm install` | Production, high availability |
| Binary | `./strata-server` | Embedded, custom deployments |

## Docker (Standalone)

```bash
docker run -d \
  --name strata \
  -p 5432:5432 \
  -p 8432:8432 \
  -v strata-data:/data \
  -e STRATA_STORAGE__DATA_DIR=/data \
  ghcr.io/vargafoundation/strata:latest
```

### Volumes

| Path | Purpose |
|------|---------|
| `/data` | All persistent data: WAL, vectors, state DB |

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 5432 | TCP | PostgreSQL wire protocol |
| 8432 | HTTP | REST API, MCP server, LLM proxy, health checks |
| 9432 | HTTP/2 | gRPC (optional) |

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

## Kubernetes (Helm)

> Helm chart is planned for Phase 3 (M6-M9).

Example values for a 3-replica production deployment:

```yaml
# values-production.yaml
replicaCount: 3

resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"

storage:
  engine: s3
  s3:
    endpoint: "s3.amazonaws.com"
    bucket: "strata-prod"
    region: "eu-west-1"

embedding:
  provider: ollama
  model: nomic-embed-text

cluster:
  enabled: true

monitoring:
  prometheus: true
```

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
| `gateway.auth_enabled` | `STRATA_GATEWAY__AUTH_ENABLED` | `false` | Enable authentication |

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
| `memory.episodic.wal_dir` | `STRATA_MEMORY__EPISODIC__WAL_DIR` | `./data/wal` | WAL directory |
| `memory.episodic.default_retention_days` | `STRATA_MEMORY__EPISODIC__DEFAULT_RETENTION_DAYS` | `365` | Default event retention |
| `memory.semantic.index_dir` | `STRATA_MEMORY__SEMANTIC__INDEX_DIR` | `./data/vectors` | Vector index directory |
| `memory.semantic.default_dimension` | `STRATA_MEMORY__SEMANTIC__DEFAULT_DIMENSION` | `768` | Default vector dimensions |
| `memory.semantic.metric` | `STRATA_MEMORY__SEMANTIC__METRIC` | `cosine` | Distance metric: `cosine`, `l2`, `ip` |
| `memory.state.db_path` | `STRATA_MEMORY__STATE__DB_PATH` | `./data/state.db` | State DB file path |

### Embedding

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `embedding.provider` | `STRATA_EMBEDDING__PROVIDER` | `ollama` | `ollama` or `openai` |
| `embedding.model` | `STRATA_EMBEDDING__MODEL` | `nomic-embed-text` | Model name |
| `embedding.dimension` | `STRATA_EMBEDDING__DIMENSION` | `768` | Vector dimension |
| `embedding.batch_size` | `STRATA_EMBEDDING__BATCH_SIZE` | `64` | Embedding batch size |
| `embedding.ollama_url` | `STRATA_EMBEDDING__OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |
| `embedding.openai_api_key` | `STRATA_EMBEDDING__OPENAI_API_KEY` | | OpenAI API key |

### Query

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `query.max_rows` | `STRATA_QUERY__MAX_ROWS` | `10000` | Max rows per query |
| `query.timeout_ms` | `STRATA_QUERY__TIMEOUT_MS` | `30000` | Query timeout in ms |

### Cluster

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `cluster.enabled` | `STRATA_CLUSTER__ENABLED` | `false` | Enable cluster mode |
| `cluster.node_id` | `STRATA_CLUSTER__NODE_ID` | `1` | This node's ID |
| `cluster.listen` | `STRATA_CLUSTER__LISTEN` | `0.0.0.0:9433` | Cluster listen address |
| `cluster.peers` | `STRATA_CLUSTER__PEERS` | `[]` | Peer addresses |

## Production Checklist

- [ ] **Storage**: Configure S3 backend for durability
- [ ] **Embedding**: Set up Ollama or configure OpenAI API key
- [ ] **Auth**: Enable authentication (`gateway.auth_enabled = true`)
- [ ] **TLS**: Place Strata behind a TLS-terminating reverse proxy
- [ ] **Monitoring**: Expose `/metrics` to Prometheus
- [ ] **Backups**: Schedule regular backups via `strata backup`
- [ ] **Retention**: Configure `memory.episodic.default_retention_days`
- [ ] **Resources**: Allocate sufficient memory for vector indices
- [ ] **Logging**: Set `RUST_LOG=info,strata=debug` for production logging

## Health Checks

```bash
# HTTP health check
curl http://localhost:8432/health

# Response
{"status":"ok","version":"0.1.0"}
```

For Docker/Kubernetes, use the health endpoint for liveness and readiness probes.
