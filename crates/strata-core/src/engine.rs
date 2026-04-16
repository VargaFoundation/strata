use std::path::Path;
use std::sync::Arc;

use crate::config::CoreConfig;
use crate::embedding::ollama::OllamaProvider;
use crate::embedding::openai::OpenAiProvider;
use crate::embedding::EmbeddingProvider;
use crate::ingest::IngestPipeline;
use crate::memory::episodic::{EpisodicStore, Event};
use crate::memory::semantic::{SearchResult, SemanticEntry, SemanticStore};
use crate::memory::state::StateStore;
use crate::query::{QueryExecutor, QueryPlanner};
use crate::Result;

/// Top-level engine that owns all subsystems of the Strata context lake.
pub struct StrataEngine {
    config: CoreConfig,
    episodic: Arc<EpisodicStore>,
    semantic: Arc<SemanticStore>,
    state: Arc<StateStore>,
    ingest: IngestPipeline,
    /// Shared embedding provider for embed-and-search operations.
    embedding: Option<Arc<dyn EmbeddingProvider>>,
}

impl std::fmt::Debug for StrataEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrataEngine")
            .field("has_embedding", &self.embedding.is_some())
            .finish()
    }
}

impl StrataEngine {
    /// Create and initialize a new Strata engine.
    pub async fn new(config: CoreConfig) -> Result<Self> {
        // Initialize episodic store (file-backed or in-memory DuckDB)
        let episodic_path = Path::new(&config.memory.episodic.db_path);
        let episodic = Arc::new(EpisodicStore::open(episodic_path).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "falling back to in-memory episodic store");
            EpisodicStore::new()
        }));
        if config.memory.episodic.db_path != ":memory:" {
            tracing::info!(path = %config.memory.episodic.db_path, "episodic store: file-backed");
        }

        // Initialize semantic store
        let semantic = Arc::new(
            SemanticStore::with_dimension(config.embedding.dimension)
                .unwrap_or_else(|_| SemanticStore::new()),
        );

        // Initialize state store
        let state_path = Path::new(&config.memory.state.db_path);
        let state = Arc::new(StateStore::open(state_path).unwrap_or_else(|_| {
            tracing::warn!("falling back to in-memory state store");
            StateStore::new()
        }));

        // Initialize embedding provider from config
        let embedding: Option<Arc<dyn EmbeddingProvider>> = match config.embedding.provider.as_str()
        {
            "ollama" => {
                tracing::info!(
                    model = %config.embedding.model,
                    url = %config.embedding.ollama_url,
                    "embedding provider: ollama"
                );
                Some(Arc::new(OllamaProvider::new(
                    config.embedding.ollama_url.clone(),
                    config.embedding.model.clone(),
                    config.embedding.dimension,
                )))
            }
            "openai" if !config.embedding.openai_api_key.is_empty() => {
                tracing::info!(model = %config.embedding.model, "embedding provider: openai");
                Some(Arc::new(OpenAiProvider::new(
                    config.embedding.openai_api_key.clone(),
                    config.embedding.model.clone(),
                    config.embedding.dimension,
                )))
            }
            "none" | "" => {
                tracing::info!("embedding provider: none (semantic search disabled)");
                tracing::info!("  → to enable: set STRATA_EMBEDDING__PROVIDER=ollama or openai");
                None
            }
            other => {
                tracing::warn!(
                    provider = %other,
                    "unknown embedding provider, auto-embedding disabled"
                );
                tracing::info!("  → supported providers: ollama, openai, none");
                None
            }
        };

        // Initialize ingest pipeline (keep a reference to embedding for embed-and-search)
        let embedding_ref = embedding.clone();
        let ingest = match embedding {
            Some(emb) => IngestPipeline::with_embedding(
                episodic.clone(),
                semantic.clone(),
                emb,
                config.embedding.batch_size,
            ),
            None => IngestPipeline::new(episodic.clone()),
        };

        tracing::info!("Strata engine initialized");

        Ok(Self {
            config,
            episodic,
            embedding: embedding_ref,
            semantic,
            state,
            ingest,
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    // ── Episodic Memory ──────────────────────────────────────────────

    /// Ingest events via the pipeline.
    pub async fn ingest(&self, events: Vec<Event>) -> Result<u64> {
        self.ingest.ingest(events).await
    }

    /// Ingest events scoped to a specific tenant.
    ///
    /// Sets the tenant_id on all events before ingestion so that
    /// tenant-scoped queries only see their own data.
    pub async fn ingest_for_tenant(
        &self,
        mut events: Vec<Event>,
        tenant: &crate::config::TenantContext,
    ) -> Result<u64> {
        // Tag each event's payload with the tenant_id
        for event in &mut events {
            if let serde_json::Value::Object(ref mut map) = event.payload {
                map.insert(
                    "_tenant_id".to_string(),
                    serde_json::Value::String(tenant.tenant_id.clone()),
                );
            }
        }
        // After ingest, update the tenant_id column
        let count = self.ingest.ingest(events).await?;

        // Batch update tenant_id for events that don't have one
        let tenant_id = tenant.tenant_id.clone();
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let _ = db.execute(
                "UPDATE episodic SET tenant_id = ? WHERE tenant_id = 'default' OR tenant_id IS NULL",
                duckdb::params![tenant_id],
            );
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?;

        Ok(count)
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> Result<Vec<Event>> {
        self.episodic.query_by_source(source, limit).await
    }

    /// Execute SQL against the engine, intercepting strata_search() and strata_state()
    /// virtual functions via the query planner/executor pipeline.
    ///
    /// Pure SQL SELECT queries run on a blocking thread against DuckDB.
    /// strata_search('text', k) embeds the text and searches semantic memory.
    /// strata_state('agent_id', 'key') looks up a state key.
    /// Enforces the configured query timeout.
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let plan = QueryPlanner::plan(sql)?;
        let max_rows = self.config.query.max_rows;
        let timeout = std::time::Duration::from_millis(self.config.query.timeout_ms);

        let executor = QueryExecutor::new(
            self.episodic.clone(),
            self.semantic.clone(),
            self.state.clone(),
            self.embedding.clone(),
        );

        tokio::time::timeout(timeout, executor.execute(plan, max_rows))
            .await
            .map_err(|_| crate::Error::Query("query timed out".into()))?
    }

    /// Count total events.
    pub async fn event_count(&self) -> Result<u64> {
        self.episodic.count().await
    }

    // ── Semantic Memory ──────────────────────────────────────────────

    /// Upsert a semantic entry.
    pub async fn semantic_upsert(&self, entry: &SemanticEntry) -> Result<()> {
        self.semantic.upsert(entry).await
    }

    /// Search semantic memory by vector.
    pub async fn semantic_search(&self, vector: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.semantic.search(vector, k).await
    }

    /// Search semantic memory with metadata filters.
    ///
    /// Filters can match on source, event_type, or any metadata field.
    pub async fn semantic_search_filtered(
        &self,
        vector: &[f32],
        k: usize,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let source_owned = source.map(|s| s.to_string());
        let event_type_owned = event_type.map(|s| s.to_string());

        self.semantic
            .search_filtered(vector, k, move |entry| {
                if let Some(ref src) = source_owned {
                    if let Some(meta_src) = entry.metadata.get("source").and_then(|v| v.as_str()) {
                        if meta_src != src {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                if let Some(ref et) = event_type_owned {
                    if let Some(meta_et) = entry.metadata.get("event_type").and_then(|v| v.as_str())
                    {
                        if meta_et != et {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            })
            .await
    }

    /// Delete a semantic entry by UUID.
    pub async fn semantic_delete(&self, id: uuid::Uuid) -> Result<()> {
        self.semantic.delete(id).await
    }

    /// Number of entries in semantic memory.
    pub fn semantic_count(&self) -> usize {
        self.semantic.len()
    }

    // ── State Memory ─────────────────────────────────────────────────

    /// Get agent state.
    pub async fn state_get(
        &self,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<crate::memory::state::StateEntry>> {
        self.state.get(agent_id, key).await
    }

    /// Set agent state.
    pub async fn state_set(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<u64> {
        self.state.set(agent_id, key, value).await
    }

    /// Delete agent state.
    pub async fn state_delete(&self, agent_id: &str, key: &str) -> Result<()> {
        self.state.delete(agent_id, key).await
    }

    /// Subscribe to state change notifications (for WebSocket watchers).
    pub fn state_subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::memory::state::StateChange> {
        self.state.subscribe()
    }

    /// List state keys for an agent.
    pub async fn state_list_keys(&self, agent_id: &str) -> Result<Vec<String>> {
        self.state.list_keys(agent_id).await
    }

    // ── Sessions ─────────────────────────────────────────────────────

    /// Start a new conversation session.
    pub async fn session_start(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        self.episodic
            .start_session(session_id, agent_id, parent_session_id, metadata)
            .await
    }

    /// End a conversation session with an optional summary.
    pub async fn session_end(&self, session_id: &str, summary: Option<&str>) -> Result<()> {
        self.episodic.end_session(session_id, summary).await
    }

    /// Get details of a session.
    pub async fn session_get(&self, session_id: &str) -> Result<Option<serde_json::Value>> {
        self.episodic.get_session(session_id).await
    }

    /// List sessions for an agent.
    pub async fn session_list(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        self.episodic.list_sessions(agent_id, limit).await
    }

    /// Recall all events in a session.
    pub async fn session_recall(&self, session_id: &str) -> Result<Vec<serde_json::Value>> {
        self.episodic.recall_session(session_id).await
    }

    // ── Embed & Search ────────────────────────────────────────────────

    /// Embed a text string using the configured embedding provider.
    pub async fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
        let provider = self
            .embedding
            .as_ref()
            .ok_or_else(|| crate::Error::Embedding("no embedding provider configured".into()))?;
        let results = provider.embed(&[text.to_string()]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Embedding("embedding returned empty result".into()))
    }

    /// Embed text and search semantic memory in a single call.
    ///
    /// This is the primary DX-friendly search method: text in, results out.
    pub async fn embed_and_search(
        &self,
        text: &str,
        k: usize,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let vector = self.embed_text(text).await?;
        if source.is_some() || event_type.is_some() {
            self.semantic_search_filtered(&vector, k, source, event_type)
                .await
        } else {
            self.semantic_search(&vector, k).await
        }
    }

    // ── Schema Introspection ────────────────────────────────────────

    /// List all distinct event sources in the episodic store.
    pub async fn list_sources(&self) -> Result<Vec<String>> {
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT source FROM episodic ORDER BY source")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let sources: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(sources)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// List all distinct agent IDs in the state store.
    pub async fn list_agents(&self) -> Result<Vec<String>> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT agent_id FROM state ORDER BY agent_id")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let agents: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(agents)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    // ── Retention ─────────────────────────────────────────────────────

    /// Clean up expired state entries (TTL).
    ///
    /// Returns the number of entries deleted.
    pub async fn cleanup_expired_state(&self) -> Result<u64> {
        self.state.cleanup_expired().await
    }

    /// Delete events older than the configured retention period.
    ///
    /// Checks per-source retention policies first (stored in state),
    /// then falls back to the default retention period.
    /// Returns the number of events deleted.
    pub async fn enforce_retention(&self) -> Result<u64> {
        let default_days = self.config.memory.episodic.default_retention_days;

        // Load per-source retention policies from state store
        let policies = self.retention_policies().await?;

        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut total_deleted = 0u64;

            // Apply per-source policies first
            for (source, days) in &policies {
                if *days == 0 {
                    continue;
                }
                let cutoff = chrono::Utc::now() - chrono::Duration::days(*days as i64);
                let cutoff_str = cutoff.to_rfc3339();
                let deleted = db
                    .execute(
                        "DELETE FROM episodic WHERE source = ? AND ts < ?::TIMESTAMPTZ",
                        duckdb::params![source, cutoff_str],
                    )
                    .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                total_deleted += deleted as u64;
            }

            // Apply default retention to sources without a specific policy
            if default_days > 0 {
                let cutoff = chrono::Utc::now() - chrono::Duration::days(default_days as i64);
                let cutoff_str = cutoff.to_rfc3339();

                if policies.is_empty() {
                    // No per-source policies — apply globally
                    let deleted = db
                        .execute(
                            "DELETE FROM episodic WHERE ts < ?::TIMESTAMPTZ",
                            duckdb::params![cutoff_str],
                        )
                        .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                    total_deleted += deleted as u64;
                } else {
                    // Apply default only to sources without a specific policy
                    let policy_sources: Vec<&str> =
                        policies.iter().map(|(s, _)| s.as_str()).collect();
                    let placeholders: Vec<String> =
                        policy_sources.iter().map(|_| "?".to_string()).collect();
                    if !placeholders.is_empty() {
                        let sql = format!(
                            "DELETE FROM episodic WHERE ts < ?::TIMESTAMPTZ AND source NOT IN ({})",
                            placeholders.join(", ")
                        );
                        let mut stmt = db
                            .prepare(&sql)
                            .map_err(|e| crate::Error::Storage(format!("prepare: {e}")))?;

                        // Build params: cutoff + source names
                        let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
                        params.push(Box::new(cutoff_str));
                        for s in &policy_sources {
                            params.push(Box::new(s.to_string()));
                        }
                        let param_refs: Vec<&dyn duckdb::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let deleted = stmt
                            .execute(param_refs.as_slice())
                            .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                        total_deleted += deleted as u64;
                    }
                }
            }

            Ok(total_deleted)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// Get all per-source retention policies.
    pub async fn retention_policies(&self) -> Result<Vec<(String, u32)>> {
        match self.state.get("_system", "retention_policies").await? {
            Some(entry) => {
                let policies: Vec<(String, u32)> =
                    serde_json::from_value(entry.value).unwrap_or_default();
                Ok(policies)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Set a retention policy for a specific source.
    pub async fn set_retention_policy(&self, source: &str, retention_days: u32) -> Result<()> {
        let mut policies = self.retention_policies().await?;
        // Update or insert
        if let Some(existing) = policies.iter_mut().find(|(s, _)| s == source) {
            existing.1 = retention_days;
        } else {
            policies.push((source.to_string(), retention_days));
        }
        self.state
            .set(
                "_system",
                "retention_policies",
                serde_json::to_value(&policies).map_err(|e| crate::Error::State(e.to_string()))?,
            )
            .await?;
        Ok(())
    }

    /// Remove a retention policy for a source (falls back to default).
    pub async fn remove_retention_policy(&self, source: &str) -> Result<()> {
        let mut policies = self.retention_policies().await?;
        policies.retain(|(s, _)| s != source);
        self.state
            .set(
                "_system",
                "retention_policies",
                serde_json::to_value(&policies).map_err(|e| crate::Error::State(e.to_string()))?,
            )
            .await?;
        Ok(())
    }

    // ── Backup / Restore ─────────────────────────────────────────────

    /// Save all persistent stores to a directory for backup.
    ///
    /// Creates: episodic.duckdb (EXPORT), vectors/ (USearch index), state.db (SQLite copy).
    pub async fn backup(&self, dir: &std::path::Path) -> Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;

        // Backup episodic store via DuckDB EXPORT
        let export_dir = dir.join("episodic_export");
        let export_dir_str = export_dir.to_string_lossy().to_string();
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&export_dir)
                .map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;
            let db = episodic.write_conn();
            db.execute_batch(&format!("EXPORT DATABASE '{export_dir_str}'"))
                .map_err(|e| crate::Error::Storage(format!("duckdb export: {e}")))?;
            Ok::<(), crate::Error>(())
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        // Backup semantic index
        let vectors_dir = dir.join("vectors");
        self.semantic.save(&vectors_dir)?;

        tracing::info!(path = %dir.display(), "backup complete");
        Ok(())
    }

    /// Backup all stores to S3 using the configured StorageBackend.
    ///
    /// Creates a local backup first, then uploads each file to S3 under the
    /// configured prefix with a timestamp directory.
    pub async fn backup_to_s3(&self) -> Result<()> {
        use crate::storage::StorageBackend;

        let s3_config = &self.config.storage.s3;
        if s3_config.bucket.is_empty() {
            return Err(crate::Error::Config(
                "S3 bucket not configured for backup".into(),
            ));
        }

        let s3 = crate::storage::s3::S3Storage::from_config(s3_config).await?;

        // Create a temporary local backup
        let tmp = std::env::temp_dir().join(format!(
            "strata-backup-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
        ));
        self.backup(&tmp).await?;

        let prefix = &self.config.backup.s3_prefix;
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();

        // Walk the temp directory and upload each file
        let mut entries = Vec::new();
        Self::walk_dir(&tmp, &mut entries)?;

        for file_path in &entries {
            let relative = file_path
                .strip_prefix(&tmp)
                .map_err(|e| crate::Error::Storage(format!("path strip: {e}")))?;
            let s3_key = format!("{prefix}{timestamp}/{}", relative.to_string_lossy());
            let data = tokio::fs::read(file_path)
                .await
                .map_err(|e| crate::Error::Storage(format!("read backup file: {e}")))?;
            s3.put(&s3_key, bytes::Bytes::from(data)).await?;
        }

        // Clean up temp directory
        let _ = tokio::fs::remove_dir_all(&tmp).await;

        tracing::info!(
            prefix = %prefix,
            timestamp = %timestamp,
            files = entries.len(),
            "S3 backup complete"
        );
        Ok(())
    }

    /// Recursively walk a directory, collecting file paths.
    fn walk_dir(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }
        for entry in
            std::fs::read_dir(dir).map_err(|e| crate::Error::Storage(format!("readdir: {e}")))?
        {
            let entry = entry.map_err(|e| crate::Error::Storage(format!("entry: {e}")))?;
            let path = entry.path();
            if path.is_dir() {
                Self::walk_dir(&path, out)?;
            } else {
                out.push(path);
            }
        }
        Ok(())
    }

    // ── Store Accessors (for snapshot/restore) ────────────────────────

    /// Access the episodic store directly (for snapshot operations).
    pub fn episodic_store(&self) -> Arc<EpisodicStore> {
        self.episodic.clone()
    }

    /// Access the semantic store directly (for snapshot operations).
    pub fn semantic_store(&self) -> Arc<SemanticStore> {
        self.semantic.clone()
    }

    // ── Health Checks ────────────────────────────────────────────────

    /// Check if the DuckDB episodic store is accessible.
    pub async fn check_episodic(&self) -> bool {
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            db.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    /// Check if the SQLite state store is accessible.
    pub async fn check_state(&self) -> bool {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            db.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    /// Gracefully shut down the engine, persisting semantic index.
    pub async fn shutdown(self) -> Result<()> {
        // Save semantic index if index_dir is configured
        let index_dir = &self.config.memory.semantic.index_dir;
        if !index_dir.is_empty() && index_dir != ":memory:" {
            if let Err(e) = self.semantic.save(std::path::Path::new(index_dir)) {
                tracing::warn!(error = %e, "failed to save semantic index on shutdown");
            } else {
                tracing::info!(path = %index_dir, "semantic index saved");
            }
        }
        tracing::info!("Strata engine shutting down");
        Ok(())
    }
}

// Compile-time assertion: StrataEngine must be Send + Sync for Arc usage.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StrataEngine>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_lifecycle() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn engine_ingest_and_count() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        let events = vec![Event {
            id: uuid::Uuid::new_v4(),
            source: "test".into(),
            event_type: "click".into(),
            payload: serde_json::json!({"page": "/home"}),
            timestamp: chrono::Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        }];

        let count = engine.ingest(events).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(engine.event_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn engine_state_crud() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        let v = engine
            .state_set("bot", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        assert_eq!(v, 1);

        let entry = engine.state_get("bot", "mood").await.unwrap().unwrap();
        assert_eq!(entry.value, serde_json::json!("happy"));

        engine.state_delete("bot", "mood").await.unwrap();
        assert!(engine.state_get("bot", "mood").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn engine_query_sql() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
        let rows = engine
            .query_sql("SELECT 42::VARCHAR as answer")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["answer"], "42");
    }

    #[tokio::test]
    async fn engine_semantic_search() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        // Use distinct vectors so cosine similarity clearly differentiates them
        let mut rust_vec = vec![0.0f32; 768];
        rust_vec[0] = 1.0; // points strongly in dimension 0

        let mut python_vec = vec![0.0f32; 768];
        python_vec[1] = 1.0; // points strongly in dimension 1

        let entry1 = SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "Rust programming language".into(),
            embedding: rust_vec.clone(),
            metadata: serde_json::json!({}),
        };
        engine.semantic_upsert(&entry1).await.unwrap();

        let entry2 = SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "Python scripting".into(),
            embedding: python_vec,
            metadata: serde_json::json!({}),
        };
        engine.semantic_upsert(&entry2).await.unwrap();

        assert_eq!(engine.semantic_count(), 2);

        // Search for vector close to "Rust"
        let results = engine.semantic_search(&rust_vec, 1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].entry.content, "Rust programming language");
    }
}
