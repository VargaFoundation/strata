use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CoreConfig {
    pub storage: StorageConfig,
    pub memory: MemoryConfig,
    pub embedding: EmbeddingConfig,
    pub query: QueryConfig,
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

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub episodic: EpisodicConfig,
    pub semantic: SemanticConfig,
    pub state: StateConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EpisodicConfig {
    pub wal_dir: String,
    pub default_retention_days: u32,
}

impl Default for EpisodicConfig {
    fn default() -> Self {
        Self {
            wal_dir: "./data/wal".into(),
            default_retention_days: 365,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EmbeddingConfig {
    pub provider: String,
    pub model: String,
    pub dimension: usize,
    pub batch_size: usize,
    pub ollama_url: String,
    pub openai_api_key: String,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: "ollama".into(),
            model: "nomic-embed-text".into(),
            dimension: 768,
            batch_size: 64,
            ollama_url: "http://localhost:11434".into(),
            openai_api_key: String::new(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = CoreConfig::default();
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.storage.engine, "local");
        assert_eq!(config.embedding.dimension, 768);
        assert_eq!(config.embedding.provider, "ollama");
        assert_eq!(config.embedding.model, "nomic-embed-text");
        assert_eq!(config.embedding.batch_size, 64);
        assert_eq!(config.query.max_rows, 10_000);
        assert_eq!(config.query.timeout_ms, 30_000);
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
        assert_eq!(config.embedding.provider, "ollama");
        assert_eq!(config.embedding.dimension, 768);
    }

    #[test]
    fn deserialize_empty_toml_uses_all_defaults() {
        let config: CoreConfig = toml::from_str("").unwrap();
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.embedding.provider, "ollama");
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
