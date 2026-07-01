use serde::Deserialize;

/// Read a secret value, supporting the `_FILE` convention for Docker/K8s secrets.
///
/// If the env var `{name}_FILE` is set, reads the secret from that file path.
/// Otherwise falls back to reading the env var `{name}` directly.
/// Returns empty string if neither is set.
pub fn resolve_secret(name: &str) -> String {
    let file_key = format!("{name}_FILE");
    if let Ok(path) = std::env::var(&file_key) {
        match std::fs::read_to_string(path.trim()) {
            Ok(secret) => return secret.trim().to_string(),
            Err(e) => {
                tracing::warn!(%file_key, error = %e, "failed to read secret file");
            }
        }
    }
    std::env::var(name).unwrap_or_default()
}

/// Format helper: redact secret strings in Debug output.
fn redact(s: &str) -> &str {
    if s.is_empty() {
        ""
    } else {
        "***"
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct CoreConfig {
    pub storage: StorageConfig,
    pub memory: MemoryConfig,
    pub embedding: EmbeddingConfig,
    pub rerank: RerankConfig,
    pub runtime: RuntimeConfig,
    pub query: QueryConfig,
    pub backup: BackupConfig,
}

impl std::fmt::Debug for CoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreConfig")
            .field("storage", &self.storage)
            .field("memory", &self.memory)
            .field("embedding", &self.embedding)
            .field("rerank", &self.rerank)
            .field("runtime", &self.runtime)
            .field("query", &self.query)
            .field("backup", &self.backup)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub engine: String,
    pub s3: S3Config,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".into(),
            engine: "local".into(),
            s3: S3Config::default(),
        }
    }
}

#[derive(Clone, Deserialize, Default)]
#[serde(default)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &redact(&self.access_key))
            .field("secret_key", &redact(&self.secret_key))
            .field("region", &self.region)
            .finish()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub episodic: EpisodicConfig,
    pub semantic: SemanticConfig,
    pub state: StateConfig,
    pub cognition: CognitionConfig,
}

/// Configuration for the memory-cognition layer (dedup, contradiction resolution, importance).
///
/// The deterministic core (subject-based contradiction resolution, exact/semantic dedup,
/// importance) is always on. LLM-based fact extraction is opt-in via `extraction = "llm"`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CognitionConfig {
    /// DuckDB path for the bi-temporal `memories` table.
    pub db_path: String,
    /// Cosine similarity at/above which a new memory is merged into an existing one
    /// (semantic dedup / consolidation) instead of inserted.
    pub dedup_threshold: f32,
    /// Fact-extraction strategy on `remember`: `none` (store as-is) or `llm` (opt-in).
    pub extraction: String,
    /// Completion provider for LLM extraction when `extraction = "llm"`: `ollama`, `openai`, or `none`.
    pub extraction_provider: String,
    /// Model used for LLM extraction (reuses embedding `ollama_url` / `openai_api_key`).
    pub extraction_model: String,
    /// Default importance assigned to a new memory (0.0..=1.0).
    pub default_importance: f32,
    /// Half-life (days) for time-decay of importance during `enforce_decay`.
    pub decay_half_life_days: f32,
    /// Memories whose time-decayed importance falls below this are forgotten (expired).
    pub forget_threshold: f32,
    /// Number of read connections (query concurrency). Min 1.
    #[serde(default = "default_read_pool_size")]
    pub read_pool_size: usize,
    /// Max active memories retained per scope (count-based forgetting + per-tenant quota). When a
    /// scope exceeds this after an add, the lowest-importance memories are evicted. 0 = unlimited.
    #[serde(default)]
    pub max_memories_per_scope: usize,
    /// Enable query-time knowledge-graph expansion in `memory_search` (read-path only): also pull
    /// memories linked by a graph edge to an entity mentioned in the query, so multi-hop facts that
    /// lexical/vector retrieval miss can surface. Off by default.
    /// Retrieval candidate width (read-path): active memories scanned for BM25 AND vector neighbors
    /// fetched — symmetric so hybrid fusion isn't BM25-dominated. Also caps graph edges scanned.
    #[serde(default = "default_retrieval_scan_cap")]
    pub retrieval_scan_cap: usize,
    /// Fused candidate pool kept after RRF (read-path) for the importance blend + rerank + top-k.
    #[serde(default = "default_retrieval_pool")]
    pub retrieval_pool: usize,
    #[serde(default)]
    pub graph_expansion: bool,
    /// Auto-populate knowledge-graph edges from each added memory via deterministic triple
    /// extraction (entity→relation→entity). Replication-safe (pure extraction + uuidv5 edge ids
    /// derived from the memory id, applied identically on every node). Off by default.
    #[serde(default)]
    pub auto_graph: bool,
}

impl Default for CognitionConfig {
    fn default() -> Self {
        Self {
            db_path: "./data/memories.duckdb".into(),
            dedup_threshold: 0.92,
            extraction: "none".into(),
            extraction_provider: "none".into(),
            extraction_model: "llama3.2".into(),
            default_importance: 0.5,
            decay_half_life_days: 30.0,
            forget_threshold: 0.05,
            read_pool_size: default_read_pool_size(),
            max_memories_per_scope: 0,
            retrieval_scan_cap: default_retrieval_scan_cap(),
            retrieval_pool: default_retrieval_pool(),
            graph_expansion: false,
            auto_graph: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EpisodicConfig {
    pub db_path: String,
    pub wal_dir: String,
    pub default_retention_days: u32,
    /// Number of read connections (concurrency for queries). Min 1.
    #[serde(default = "default_read_pool_size")]
    pub read_pool_size: usize,
}

/// Default read-connection pool size for the DuckDB-backed stores.
fn default_read_pool_size() -> usize {
    8
}

/// Retrieval candidate width: how many active memories BM25 scans AND how many vector neighbors are
/// fetched (kept symmetric so hybrid fusion isn't dominated by one arm). Also the graph-edge cap.
fn default_retrieval_scan_cap() -> usize {
    2048
}

/// Fused candidates kept after RRF for the importance blend + rerank + top-k.
fn default_retrieval_pool() -> usize {
    50
}

impl Default for EpisodicConfig {
    fn default() -> Self {
        Self {
            db_path: "./data/episodic.duckdb".into(),
            wal_dir: "./data/wal".into(),
            default_retention_days: 365,
            read_pool_size: default_read_pool_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SemanticConfig {
    pub index_dir: String,
    pub default_dimension: usize,
    pub metric: String,
}

impl Default for SemanticConfig {
    fn default() -> Self {
        Self {
            index_dir: "./data/vectors".into(),
            default_dimension: 768,
            metric: "cosine".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StateConfig {
    pub db_path: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            db_path: "./data/state.db".into(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct EmbeddingConfig {
    pub provider: String,
    pub model: String,
    pub dimension: usize,
    pub batch_size: usize,
    pub ollama_url: String,
    pub openai_api_key: String,
    /// Anthropic API key — used by the Claude completion provider (extraction / rerank / eval).
    pub anthropic_api_key: String,
}

impl std::fmt::Debug for EmbeddingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingConfig")
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("dimension", &self.dimension)
            .field("batch_size", &self.batch_size)
            .field("ollama_url", &self.ollama_url)
            .field("openai_api_key", &redact(&self.openai_api_key))
            .field("anthropic_api_key", &redact(&self.anthropic_api_key))
            .finish()
    }
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: "none".into(),
            model: "nomic-embed-text".into(),
            dimension: 768,
            batch_size: 64,
            ollama_url: "http://localhost:11434".into(),
            openai_api_key: String::new(),
            anthropic_api_key: String::new(),
        }
    }
}

/// Configuration for optional second-stage reranking of `memory_search` results.
///
/// Reranking runs only on the read path (no Raft/determinism impact) and is off by default.
/// When `provider = "llm"` it reuses a chat-completion backend (`backend`) to score the top
/// `candidates` fused hits and reorder them; network failures degrade gracefully to the
/// unreranked order.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RerankConfig {
    /// Reranking strategy: `none` (off) or `llm` (LLM relevance judge).
    pub provider: String,
    /// Chat-completion backend for `provider = "llm"`: `ollama` or `openai`
    /// (reuses `embedding.ollama_url` / `embedding.openai_api_key`).
    pub backend: String,
    /// Model name used by the reranker.
    pub model: String,
    /// Number of fused candidates to over-fetch and rerank before truncating to `k`.
    pub candidates: usize,
}

impl Default for RerankConfig {
    fn default() -> Self {
        Self {
            provider: "none".into(),
            backend: "ollama".into(),
            model: "llama3.2".into(),
            candidates: 50,
        }
    }
}

/// Configuration for the agentic-platform runtime (the agent-run ledger).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// SQLite path for the durable agent-run ledger.
    pub db_path: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            db_path: "./data/runs.db".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub max_rows: usize,
    pub timeout_ms: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BackupConfig {
    /// Enable automatic S3 backups in the background tiering task.
    pub auto_enabled: bool,
    /// Interval between automatic backups (in hours).
    pub interval_hours: u32,
    /// S3 key prefix for backup objects (e.g. "backups/").
    pub s3_prefix: String,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            auto_enabled: false,
            interval_hours: 24,
            s3_prefix: "backups/".into(),
        }
    }
}

/// Tenant context for multi-tenancy row-level security.
///
/// When set, all engine operations are automatically scoped to the given tenant.
/// Extracted from JWT claims (`tenant_id` field) by the gateway auth middleware.
#[derive(Debug, Clone)]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    /// The default tenant for backwards compatibility.
    pub fn default_tenant() -> Self {
        Self {
            tenant_id: "default".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = CoreConfig::default();
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.storage.engine, "local");
        assert_eq!(config.embedding.dimension, 768);
        assert_eq!(config.embedding.provider, "none");
        assert_eq!(config.embedding.model, "nomic-embed-text");
        assert_eq!(config.embedding.batch_size, 64);
        assert_eq!(config.query.max_rows, 10_000);
        assert_eq!(config.query.timeout_ms, 30_000);
    }

    #[test]
    fn default_rerank_config() {
        let config = CoreConfig::default();
        assert_eq!(config.rerank.provider, "none");
        assert_eq!(config.rerank.backend, "ollama");
        assert_eq!(config.rerank.candidates, 50);
    }

    #[test]
    fn deserialize_rerank_from_toml() {
        let toml_str = r#"
            [rerank]
            provider = "llm"
            backend = "openai"
            model = "gpt-4o-mini"
            candidates = 30
        "#;
        let config: CoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.rerank.provider, "llm");
        assert_eq!(config.rerank.backend, "openai");
        assert_eq!(config.rerank.model, "gpt-4o-mini");
        assert_eq!(config.rerank.candidates, 30);
    }

    #[test]
    fn default_storage_config() {
        let config = StorageConfig::default();
        assert_eq!(config.data_dir, "./data");
        assert_eq!(config.engine, "local");
        assert!(config.s3.endpoint.is_empty());
        assert!(config.s3.bucket.is_empty());
    }

    #[test]
    fn default_memory_config() {
        let config = MemoryConfig::default();
        assert_eq!(config.episodic.wal_dir, "./data/wal");
        assert_eq!(config.episodic.default_retention_days, 365);
        assert_eq!(config.semantic.index_dir, "./data/vectors");
        assert_eq!(config.semantic.default_dimension, 768);
        assert_eq!(config.semantic.metric, "cosine");
        assert_eq!(config.state.db_path, "./data/state.db");
    }

    #[test]
    fn deserialize_from_toml() {
        let toml_str = r#"
            [storage]
            data_dir = "/custom/path"
            engine = "s3"

            [storage.s3]
            endpoint = "http://minio:9000"
            bucket = "test-bucket"

            [embedding]
            provider = "openai"
            model = "text-embedding-3-small"
            dimension = 1536

            [query]
            max_rows = 500
            timeout_ms = 5000
        "#;
        let config: CoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.data_dir, "/custom/path");
        assert_eq!(config.storage.engine, "s3");
        assert_eq!(config.storage.s3.endpoint, "http://minio:9000");
        assert_eq!(config.storage.s3.bucket, "test-bucket");
        assert_eq!(config.embedding.provider, "openai");
        assert_eq!(config.embedding.dimension, 1536);
        assert_eq!(config.query.max_rows, 500);
        assert_eq!(config.query.timeout_ms, 5000);
    }

    #[test]
    fn deserialize_partial_toml_uses_defaults() {
        let toml_str = r#"
            [storage]
            data_dir = "/my/data"
        "#;
        let config: CoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.data_dir, "/my/data");
        // Other fields should be defaults
        assert_eq!(config.storage.engine, "local");
        assert_eq!(config.embedding.provider, "none");
        assert_eq!(config.embedding.dimension, 768);
    }

    #[test]
    fn deserialize_empty_toml_uses_all_defaults() {
        let config: CoreConfig = toml::from_str("").unwrap();
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.embedding.provider, "none");
    }

    #[test]
    fn config_is_clone() {
        let config = CoreConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.storage.data_dir, config.storage.data_dir);
    }

    #[test]
    fn config_is_debug() {
        let config = CoreConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("CoreConfig"));
        assert!(debug.contains("./data"));
    }
}
