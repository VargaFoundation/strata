use std::path::Path;
use std::sync::Arc;

use crate::config::CoreConfig;
use crate::embedding::ollama::OllamaProvider;
use crate::embedding::openai::OpenAiProvider;
use crate::embedding::EmbeddingProvider;
use crate::ingest::IngestPipeline;
use crate::llm::CompletionProvider;
use crate::memory::cognition::{
    Memory, MemoryAdd, MemoryHit, MemoryInput, MemoryOutcome, MemoryRow, MemoryScope, MemoryState,
    MemoryStore,
};
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
    /// Bi-temporal store of distilled memories (cognition layer).
    memory_store: Arc<MemoryStore>,
    /// Vector index over memories only (kept separate from event embeddings).
    memory_index: Arc<SemanticStore>,
    /// Per-modality vector indexes (mixed-dimension multi-modal embeddings).
    modal: crate::memory::semantic::MultiModalStore,
    ingest: IngestPipeline,
    /// Shared embedding provider for embed-and-search operations.
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    /// Optional completion provider for opt-in LLM fact extraction (cognition layer).
    completion: Option<Arc<dyn CompletionProvider>>,
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
        let episodic = Arc::new(
            EpisodicStore::open(episodic_path, config.memory.episodic.read_pool_size)
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "falling back to in-memory episodic store");
                    EpisodicStore::new()
                }),
        );
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

        // Initialize memory-cognition store (bi-temporal facts) + its dedicated vector index
        let memory_path = Path::new(&config.memory.cognition.db_path);
        let memory_store = Arc::new(
            MemoryStore::open(memory_path, config.memory.cognition.read_pool_size).unwrap_or_else(
                |e| {
                    tracing::warn!(error = %e, "falling back to in-memory cognition store");
                    MemoryStore::new()
                },
            ),
        );
        let memory_index = Arc::new(
            SemanticStore::with_dimension(config.embedding.dimension)
                .unwrap_or_else(|_| SemanticStore::new()),
        );
        // Rebuild the in-memory vector index from persisted embeddings (no provider call).
        match memory_store.load_active_with_embeddings().await {
            Ok(rows) => {
                let n = rows.len();
                for (mem, emb) in rows {
                    let _ = memory_index.upsert(&mem.to_semantic_entry(emb)).await;
                }
                if n > 0 {
                    tracing::info!(memories = n, "rebuilt memory vector index from disk");
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to rebuild memory index"),
        }

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

        // Initialize optional completion provider for opt-in LLM fact extraction.
        let completion: Option<Arc<dyn CompletionProvider>> =
            match config.memory.cognition.extraction_provider.as_str() {
                "ollama" => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: ollama"
                    );
                    Some(Arc::new(crate::llm::ollama::OllamaCompletion::new(
                        config.embedding.ollama_url.clone(),
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                "openai" if !config.embedding.openai_api_key.is_empty() => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: openai"
                    );
                    Some(Arc::new(crate::llm::openai::OpenAiCompletion::new(
                        config.embedding.openai_api_key.clone(),
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                _ => None,
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
            memory_store,
            memory_index,
            modal: crate::memory::semantic::MultiModalStore::new(),
            ingest,
            completion,
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

    /// Number of events whose vectors are not yet in the semantic index (cross-store drift gauge).
    pub async fn unembedded_count(&self) -> Result<u64> {
        self.episodic.unembedded_count().await
    }

    /// Re-embed and index up to `limit` events that were left unembedded (e.g. the embedding
    /// provider was down at ingest). Returns the number newly indexed. Closes the cross-store gap.
    pub async fn reindex_unembedded(&self, limit: usize) -> Result<usize> {
        let events = self.episodic.unembedded_events(limit).await?;
        if events.is_empty() {
            return Ok(0);
        }
        Ok(self.ingest.embed_and_index(&events).await)
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
        // Tag each event's payload with the tenant so the episodic store sets the tenant_id
        // column per-row AT INSERT TIME (atomic, race-free, deterministic for Raft apply),
        // and so the embedding metadata carries the tenant. No post-insert UPDATE — that was
        // a cross-tenant leak under concurrency and a non-determinism hazard in cluster apply.
        for event in &mut events {
            if let serde_json::Value::Object(ref mut map) = event.payload {
                map.insert(
                    "_tenant_id".to_string(),
                    serde_json::Value::String(tenant.tenant_id.clone()),
                );
            }
        }
        self.ingest.ingest(events).await
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> Result<Vec<Event>> {
        let limit = limit.min(self.config.query.max_rows);
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

    /// Execute SQL scoped to a single tenant — every `episodic` reference is rewritten to a
    /// per-tenant filtered view, so the caller can only read its own rows (row-level isolation).
    pub async fn query_sql_for_tenant(
        &self,
        sql: &str,
        tenant: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let plan = QueryPlanner::plan(sql)?;
        let max_rows = self.config.query.max_rows;
        let timeout = std::time::Duration::from_millis(self.config.query.timeout_ms);

        let executor = QueryExecutor::new(
            self.episodic.clone(),
            self.semantic.clone(),
            self.state.clone(),
            self.embedding.clone(),
        )
        .with_tenant(tenant);

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

    /// Upsert a pre-computed embedding of any modality (text/image/audio/…). Strata becomes the
    /// multi-modal memory store; callers bring their own modality encoder (CLIP, etc.). The vector
    /// must match the index dimension — mixed-dimension modalities need separate indexes (future).
    pub async fn semantic_upsert_modal(
        &self,
        id: uuid::Uuid,
        modality: &str,
        content: impl Into<String>,
        embedding: Vec<f32>,
        mut metadata: serde_json::Value,
    ) -> Result<()> {
        if !metadata.is_object() {
            metadata = serde_json::json!({});
        }
        metadata["modality"] = serde_json::json!(modality);
        // Route to the per-modality index so mixed dimensions (e.g. 512-d CLIP, 768-d text) coexist.
        self.modal
            .upsert(
                modality,
                &SemanticEntry {
                    id,
                    content: content.into(),
                    embedding,
                    metadata,
                },
            )
            .await
    }

    /// Vector search restricted to one modality (or all matching-dimension modalities when None).
    pub async fn semantic_search_modal(
        &self,
        vector: &[f32],
        k: usize,
        modality: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        match modality {
            Some(m) => self.modal.search(m, vector, k).await,
            None => self.modal.search_all(vector, k).await,
        }
    }

    /// Modalities that currently have a vector index.
    pub fn modalities(&self) -> Vec<String> {
        self.modal.modalities()
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

    // Tenant-scoped state: the agent_id is namespaced by tenant so one tenant can never
    // read/write another tenant's agent state, even with a colliding agent_id.

    /// Tenant-scoped state get (the returned `agent_id` has the tenant prefix stripped).
    pub async fn state_get_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<crate::memory::state::StateEntry>> {
        let mut entry = self.state.get(&scoped_agent(tenant, agent_id), key).await?;
        if let Some(ref mut e) = entry {
            e.agent_id = agent_id.to_string();
        }
        Ok(entry)
    }

    /// Tenant-scoped state set.
    pub async fn state_set_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<u64> {
        self.state
            .set(&scoped_agent(tenant, agent_id), key, value)
            .await
    }

    /// Tenant-scoped state delete.
    pub async fn state_delete_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
    ) -> Result<()> {
        self.state
            .delete(&scoped_agent(tenant, agent_id), key)
            .await
    }

    /// Tenant-scoped state key listing.
    pub async fn state_list_keys_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
    ) -> Result<Vec<String>> {
        self.state.list_keys(&scoped_agent(tenant, agent_id)).await
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
        let limit = limit.min(self.config.query.max_rows);
        self.episodic.list_sessions(agent_id, limit).await
    }

    /// Recall all events in a session.
    pub async fn session_recall(&self, session_id: &str) -> Result<Vec<serde_json::Value>> {
        self.episodic.recall_session(session_id).await
    }

    /// Start a session scoped to a tenant.
    pub async fn session_start_for_tenant(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
        tenant: &str,
    ) -> Result<()> {
        self.episodic
            .start_session_for_tenant(session_id, agent_id, parent_session_id, metadata, tenant)
            .await
    }

    /// End a session scoped to a tenant (true iff a session was updated).
    pub async fn session_end_for_tenant(
        &self,
        session_id: &str,
        summary: Option<&str>,
        tenant: &str,
    ) -> Result<bool> {
        self.episodic
            .end_session_for_tenant(session_id, summary, tenant)
            .await
    }

    /// Recall a session's events scoped to a tenant.
    pub async fn session_recall_for_tenant(
        &self,
        session_id: &str,
        tenant: &str,
    ) -> Result<Vec<serde_json::Value>> {
        self.episodic
            .recall_session_for_tenant(session_id, tenant)
            .await
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

    /// Vector search over event embeddings, scoped to a tenant (row-level isolation).
    pub async fn semantic_search_for_tenant(
        &self,
        vector: &[f32],
        k: usize,
        tenant: &str,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let tenant = tenant.to_string();
        let source_owned = source.map(|s| s.to_string());
        let event_type_owned = event_type.map(|s| s.to_string());
        self.semantic
            .search_filtered(vector, k, move |entry| {
                let mtenant = entry
                    .metadata
                    .get("tenant_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                if mtenant != tenant {
                    return false;
                }
                if let Some(ref src) = source_owned {
                    if entry.metadata.get("source").and_then(|v| v.as_str()) != Some(src.as_str()) {
                        return false;
                    }
                }
                if let Some(ref et) = event_type_owned {
                    if entry.metadata.get("event_type").and_then(|v| v.as_str())
                        != Some(et.as_str())
                    {
                        return false;
                    }
                }
                true
            })
            .await
    }

    /// Embed text and vector-search event embeddings scoped to a tenant.
    pub async fn embed_and_search_for_tenant(
        &self,
        text: &str,
        k: usize,
        tenant: &str,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let vector = self.embed_text(text).await?;
        self.semantic_search_for_tenant(&vector, k, tenant, source, event_type)
            .await
    }

    // ── Memory Cognition ──────────────────────────────────────────────

    /// Add a memory through the deterministic cognition pipeline.
    ///
    /// Behaviour (deterministic core, no LLM required):
    /// - If `subject` is set and an active memory with the same `(scope, subject)` exists:
    ///   identical content → reinforce (`Confirmed`); different content → supersede the old
    ///   and insert the new (`Superseded`, bi-temporal — the old one keeps its history and
    ///   stays answerable via [`Self::memory_as_of`]).
    /// - Else, when an embedding provider is configured, a near-duplicate (cosine ≥
    ///   `dedup_threshold`) in the same scope is merged/updated (`Merged`).
    /// - Otherwise a fresh memory is inserted (`Inserted`).
    pub async fn memory_add(&self, input: MemoryInput) -> Result<MemoryAdd> {
        let (result, rows) = self.memory_plan(input).await?;
        let scope = result.memory.scope.clone();
        self.memory_apply_rows(rows).await?;
        // Count-based forgetting / per-tenant quota: evict the lowest-importance memories beyond cap.
        let cap = self.config.memory.cognition.max_memories_per_scope;
        if cap > 0 {
            if let Ok(evicted) = self.memory_store.enforce_scope_cap(&scope, cap).await {
                for id in evicted {
                    let _ = self.memory_index.delete(id).await;
                }
            }
        }
        Ok(result)
    }

    /// Compute the materialized change-set for adding a memory **without writing anything**.
    ///
    /// Runs the same deterministic cognition as [`Self::memory_add`] (subject contradiction →
    /// semantic merge → insert) but returns the resulting [`MemoryRow`]s instead of applying
    /// them. This lets the cluster leader run cognition once, propose the rows through Raft, and
    /// have every node apply an identical result via [`Self::memory_apply_rows`] — avoiding the
    /// failover-divergence of re-running non-deterministic logic (new uuids/timestamps) per node.
    pub async fn memory_plan(&self, mut input: MemoryInput) -> Result<(MemoryAdd, Vec<MemoryRow>)> {
        if input.scope.tenant_id.is_empty() {
            input.scope.tenant_id = "default".into();
        }
        let cog = &self.config.memory.cognition;
        let importance = input.importance.unwrap_or(cog.default_importance);
        // Embedding is best-effort: the deterministic paths work without it.
        let embedding = self.embed_text(&input.content).await.ok();

        // 1. Subject-based contradiction resolution (authoritative, no embedding required).
        if let Some(subject) = input.subject.clone() {
            let actives = self
                .memory_store
                .find_active_by_subject(&input.scope, &subject)
                .await?;
            if let Some(existing) = actives.first() {
                if existing.content.trim() == input.content.trim() {
                    // Confirmed: bump importance + version; preserve the existing embedding when
                    // we couldn't re-embed (else upsert_raw would NULL the stored vector).
                    let mut m = existing.clone();
                    m.importance = importance.max(existing.importance);
                    m.version += 1;
                    m.updated_at = chrono::Utc::now();
                    let emb = match embedding.clone() {
                        Some(e) => Some(e),
                        None => self.memory_store.get_embedding(existing.id).await?,
                    };
                    return Ok((
                        MemoryAdd {
                            memory: m.clone(),
                            outcome: MemoryOutcome::Confirmed,
                        },
                        vec![MemoryRow {
                            memory: m,
                            embedding: emb,
                        }],
                    ));
                }
                // Contradiction: supersede every active memory for this subject, insert new.
                let now = chrono::Utc::now();
                let mut rows: Vec<MemoryRow> = Vec::with_capacity(actives.len() + 1);
                for a in &actives {
                    let mut old = a.clone();
                    old.state = MemoryState::Superseded;
                    old.valid_to = Some(now);
                    old.updated_at = now;
                    rows.push(MemoryRow {
                        memory: old,
                        embedding: None,
                    });
                }
                let mut mem = Memory::new(input.scope.clone(), input.content.clone());
                mem.subject = Some(subject);
                mem.importance = importance;
                mem.supersedes = Some(actives[0].id);
                mem.valid_from = now;
                mem.source_event_ids = input.source_event_ids.clone();
                mem.metadata = input.metadata.clone();
                if let Some(t) = &input.mem_type {
                    mem.mem_type = t.clone();
                }
                rows.push(MemoryRow {
                    memory: mem.clone(),
                    embedding: embedding.clone(),
                });
                return Ok((
                    MemoryAdd {
                        memory: mem,
                        outcome: MemoryOutcome::Superseded,
                    },
                    rows,
                ));
            }
        } else if let Some(emb) = embedding.as_deref() {
            // 2. Semantic dedup/merge (subjectless facts, only when embeddings are available).
            let hits = self.memory_index_search(emb, &input.scope, 1).await?;
            if let Some(top) = hits.first() {
                if top.score >= cog.dedup_threshold {
                    let mut m = top.memory.clone();
                    m.content = input.content.clone();
                    m.importance = importance.max(top.memory.importance);
                    m.version += 1;
                    m.updated_at = chrono::Utc::now();
                    return Ok((
                        MemoryAdd {
                            memory: m.clone(),
                            outcome: MemoryOutcome::Merged,
                        },
                        vec![MemoryRow {
                            memory: m,
                            embedding: embedding.clone(),
                        }],
                    ));
                }
            }
        }

        // 3. Insert a fresh memory.
        let mut mem = Memory::new(input.scope.clone(), input.content.clone());
        mem.subject = input.subject.clone();
        mem.importance = importance;
        mem.source_event_ids = input.source_event_ids.clone();
        mem.metadata = input.metadata.clone();
        if let Some(t) = &input.mem_type {
            mem.mem_type = t.clone();
        }
        Ok((
            MemoryAdd {
                memory: mem.clone(),
                outcome: MemoryOutcome::Inserted,
            },
            vec![MemoryRow {
                memory: mem,
                embedding,
            }],
        ))
    }

    /// Apply a materialized memory change-set: persist each row and maintain the vector index
    /// (active rows are (re)indexed when they carry an embedding; superseded/expired rows are
    /// removed from the index). Deterministic — used by both [`Self::memory_add`] and Raft apply.
    pub async fn memory_apply_rows(&self, rows: Vec<MemoryRow>) -> Result<u64> {
        let n = rows.len() as u64;
        for row in &rows {
            self.memory_store
                .upsert_raw(&row.memory, row.embedding.as_deref())
                .await?;
            match row.memory.state {
                MemoryState::Active => {
                    if let Some(emb) = &row.embedding {
                        let _ = self
                            .memory_index
                            .upsert(&row.memory.to_semantic_entry(emb.clone()))
                            .await;
                    }
                }
                _ => {
                    let _ = self.memory_index.delete(row.memory.id).await;
                }
            }
        }
        Ok(n)
    }

    /// Scoped semantic search over the memory vector index.
    async fn memory_index_search(
        &self,
        vector: &[f32],
        scope: &MemoryScope,
        k: usize,
    ) -> Result<Vec<MemoryHit>> {
        let scope = scope.clone();
        let results = self
            .memory_index
            .search_filtered(vector, k, move |entry| {
                crate::memory::cognition::scope_matches_metadata(&scope, &entry.metadata)
            })
            .await?;
        let mut hits = Vec::with_capacity(results.len());
        for r in results {
            if let Some(mem) = self.memory_store.get(r.entry.id).await? {
                if mem.state == MemoryState::Active {
                    hits.push(MemoryHit {
                        memory: mem,
                        score: r.score,
                    });
                }
            }
        }
        Ok(hits)
    }

    /// Maximum active memories scanned for lexical (BM25) ranking per scope.
    const MEMORY_LEXICAL_SCAN_CAP: usize = 512;
    /// Reciprocal Rank Fusion constant (standard default).
    const MEMORY_RRF_K: f32 = 60.0;

    /// Hybrid search over a scope's memories: deterministic BM25 lexical ranking fused
    /// (via Reciprocal Rank Fusion) with vector search when an embedding provider is
    /// configured. Lexical ranking is always on — so quality beats pure-recency even with
    /// no provider, and no external/FTS dependency is required. Empty query → recency.
    pub async fn memory_search(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
    ) -> Result<Vec<MemoryHit>> {
        use crate::memory::cognition::{lexical_rank, rrf_fuse};

        if query.trim().is_empty() || k == 0 {
            let mems = self.memory_store.list_active(scope, k.max(1)).await?;
            return Ok(mems
                .into_iter()
                .map(|memory| MemoryHit { memory, score: 0.0 })
                .collect());
        }

        // Candidate universe for lexical ranking + id→memory map.
        let candidates = self
            .memory_store
            .list_active(scope, Self::MEMORY_LEXICAL_SCAN_CAP)
            .await?;
        let mut by_id: std::collections::HashMap<uuid::Uuid, Memory> =
            candidates.iter().cloned().map(|m| (m.id, m)).collect();

        // Lexical (BM25) ranking — always available.
        let lex_ids: Vec<uuid::Uuid> = lexical_rank(query, &candidates)
            .into_iter()
            .map(|(i, _)| candidates[i].id)
            .collect();

        // Vector ranking — best-effort, only when embeddings are configured.
        let mut vec_ids: Vec<uuid::Uuid> = Vec::new();
        if !self.memory_index.is_empty() {
            if let Ok(vector) = self.embed_text(query).await {
                if let Ok(hits) = self.memory_index_search(&vector, scope, k * 4).await {
                    for h in hits {
                        vec_ids.push(h.memory.id);
                        by_id.entry(h.memory.id).or_insert(h.memory);
                    }
                }
            }
        }

        let mut rankings: Vec<Vec<uuid::Uuid>> = Vec::new();
        if !vec_ids.is_empty() {
            rankings.push(vec_ids);
        }
        if !lex_ids.is_empty() {
            rankings.push(lex_ids);
        }

        // Nothing matched lexically or by vector → fall back to importance/recency.
        if rankings.is_empty() {
            let mems = self.memory_store.list_active(scope, k).await?;
            return Ok(mems
                .into_iter()
                .map(|memory| MemoryHit { memory, score: 0.0 })
                .collect());
        }

        // Over-fetch, then re-rank by relevance blended with importance + recency, so a recent or
        // important memory can outrank a marginally-more-relevant stale one.
        let fused = rrf_fuse(&rankings, Self::MEMORY_RRF_K, (k * 3).max(k));
        let now = chrono::Utc::now();
        let mut scored: Vec<MemoryHit> = Vec::with_capacity(fused.len());
        for (id, rrf) in fused {
            let memory = match by_id.remove(&id) {
                Some(m) => m,
                None => match self.memory_store.get(id).await {
                    Ok(Some(m)) => m,
                    _ => continue,
                },
            };
            // Recency in [0,1] with a 30-day half-life; importance in [0,1].
            let age_days = (now - memory.updated_at).num_seconds().max(0) as f32 / 86_400.0;
            let recency = 0.5_f32.powf(age_days / 30.0);
            let score = rrf * (1.0 + 0.3 * memory.importance + 0.2 * recency);
            scored.push(MemoryHit { memory, score });
        }
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        Ok(scored)
    }

    /// Get a memory by id.
    pub async fn memory_get(&self, id: uuid::Uuid) -> Result<Option<Memory>> {
        self.memory_store.get(id).await
    }

    /// List active memories in a scope (importance/recency order).
    pub async fn memory_all(&self, scope: &MemoryScope, limit: usize) -> Result<Vec<Memory>> {
        // Clamp to the configured cap so a caller can't request an unbounded result set (OOM).
        let limit = limit.min(self.config.query.max_rows);
        self.memory_store.list_active(scope, limit).await
    }

    /// GDPR erasure: delete ALL of a tenant's data across every store — episodic events + sessions,
    /// memories + their vectors, agent state, and event embeddings. Sequential best-effort (the
    /// stores are independent engines, so it isn't a single transaction); returns a per-store
    /// summary. Idempotent.
    pub async fn delete_tenant(&self, tenant: &str) -> Result<serde_json::Value> {
        let events = self.episodic.delete_by_tenant(tenant).await?;
        let mem_ids = self.memory_store.delete_by_tenant(tenant).await?;
        for id in &mem_ids {
            let _ = self.memory_index.delete(*id).await;
        }
        let state = self
            .state
            .delete_by_prefix(&format!("{tenant}{TENANT_AGENT_SEP}"))
            .await?;
        let vectors = self.semantic.delete_by_tenant(tenant).await?;
        Ok(serde_json::json!({
            "tenant": tenant,
            "events_deleted": events,
            "memories_deleted": mem_ids.len(),
            "state_deleted": state,
            "vectors_deleted": vectors,
        }))
    }

    /// Full temporal history for a `(scope, subject)` — every version, oldest first.
    pub async fn memory_history(&self, scope: &MemoryScope, subject: &str) -> Result<Vec<Memory>> {
        self.memory_store.history(scope, subject).await
    }

    /// The memory that was valid for a `(scope, subject)` at instant `at` (bi-temporal).
    pub async fn memory_as_of(
        &self,
        scope: &MemoryScope,
        subject: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Memory>> {
        self.memory_store.as_of(scope, subject, at).await
    }

    /// Delete a memory (and its vector).
    pub async fn memory_delete(&self, id: uuid::Uuid) -> Result<()> {
        let _ = self.memory_index.delete(id).await;
        self.memory_store.delete(id).await
    }

    /// Expire memories by id (bi-temporal soft-delete + drop their vectors). Deterministic — used by
    /// Raft apply to replicate consolidation's retirement of the folded originals.
    pub async fn memory_expire(&self, ids: &[uuid::Uuid]) -> Result<()> {
        for id in ids {
            let _ = self.memory_store.expire(*id).await;
            let _ = self.memory_index.delete(*id).await;
        }
        Ok(())
    }

    /// Get a memory by id, scoped to a tenant (None if owned by another tenant).
    pub async fn memory_get_scoped(&self, id: uuid::Uuid, tenant: &str) -> Result<Option<Memory>> {
        self.memory_store.get_scoped(id, tenant).await
    }

    /// Delete a memory by id, scoped to a tenant. Returns true iff a row was deleted.
    pub async fn memory_delete_scoped(&self, id: uuid::Uuid, tenant: &str) -> Result<bool> {
        let deleted = self.memory_store.delete_scoped(id, tenant).await?;
        if deleted {
            let _ = self.memory_index.delete(id).await;
        }
        Ok(deleted)
    }

    /// Total memory count (all states).
    pub async fn memory_count(&self) -> Result<u64> {
        self.memory_store.count().await
    }

    /// Export all active memories for a tenant (for moving a tenant between shards on a reshard).
    pub async fn export_tenant_memories(&self, tenant: &str) -> Result<Vec<Memory>> {
        self.memory_store
            .list_by_tenant(tenant, self.config.query.max_rows)
            .await
    }

    /// Import memories verbatim (ids/scope/timestamps preserved) — the receiving side of a tenant
    /// move. Vectors are rebuilt lazily (via reindex). Returns the count imported.
    pub async fn import_memories(&self, memories: &[Memory]) -> Result<usize> {
        for m in memories {
            self.memory_store.upsert_raw(m, None).await?;
        }
        Ok(memories.len())
    }

    /// Move a tenant's memories from this engine to `dest`, then remove them here (a rebalance move).
    /// Returns the number moved. Best-effort across two independent engines (not a single txn).
    pub async fn migrate_tenant_memories_to(
        &self,
        dest: &StrataEngine,
        tenant: &str,
    ) -> Result<usize> {
        let memories = self.export_tenant_memories(tenant).await?;
        let n = dest.import_memories(&memories).await?;
        // Remove ONLY the moved memories (+ their vectors and graph edges) from the source — NOT the
        // tenant's episodic events / state. A cascade `delete_tenant` here would lose events/state
        // that were never copied to the destination.
        let removed = self.memory_store.delete_by_tenant(tenant).await?;
        for id in removed {
            let _ = self.memory_index.delete(id).await;
        }
        Ok(n)
    }

    /// Forget low-value memories via time-decay of importance (configurable half-life /
    /// threshold). Forgotten memories are expired (kept for history) and dropped from the
    /// vector index. Returns the number forgotten.
    pub async fn memory_enforce_decay(&self) -> Result<u64> {
        let cog = &self.config.memory.cognition;
        let forgotten = self
            .memory_store
            .decay(cog.decay_half_life_days, cog.forget_threshold)
            .await?;
        for id in &forgotten {
            let _ = self.memory_index.delete(*id).await;
        }
        if !forgotten.is_empty() {
            tracing::info!(
                forgotten = forgotten.len(),
                "memory decay forgot low-value memories"
            );
        }
        Ok(forgotten.len() as u64)
    }

    /// System prompt for opt-in LLM fact extraction.
    const EXTRACT_SYSTEM: &'static str = "You extract atomic, durable facts from the user's text \
        for an agent memory store. Return ONLY a JSON array of objects with keys \"subject\" (a \
        short stable key like \"favorite_color\", or null) and \"content\" (the fact as a \
        standalone sentence). Extract only meaningful, lasting facts; ignore pleasantries. \
        Example: [{\"subject\":\"favorite_color\",\"content\":\"Their favorite color is blue\"}]";

    /// Remember raw text as one or more memories.
    ///
    /// When `cognition.extraction = "llm"` and a completion provider is configured, the text
    /// is distilled into atomic facts (each routed through [`Self::memory_add`], so dedup and
    /// contradiction resolution still apply). Otherwise the text is stored as a single memory
    /// (deterministic fallback — no LLM dependency).
    pub async fn memory_remember(
        &self,
        raw_text: &str,
        scope: &MemoryScope,
    ) -> Result<Vec<MemoryAdd>> {
        let facts = self.extract_facts(raw_text).await;
        let mut out = Vec::with_capacity(facts.len());
        for (subject, content) in facts {
            let mut input = MemoryInput::new(scope.clone(), content);
            input.subject = subject;
            out.push(self.memory_add(input).await?);
        }
        Ok(out)
    }

    /// Consolidate a scope's lowest-importance memories into one summary memory.
    ///
    /// When the scope has more than `keep` active memories, the lowest-importance tail is summarized
    /// (LLM if `extraction = "llm"` + provider, else a deterministic bullet list), inserted as a new
    /// memory citing its sources in metadata, and the originals are expired (bi-temporal — history is
    /// kept). Returns the consolidated memory, or `None` if there was nothing to fold.
    pub async fn memory_consolidate(
        &self,
        scope: &MemoryScope,
        keep: usize,
    ) -> Result<Option<MemoryAdd>> {
        let Some((input, expired)) = self.memory_consolidate_plan(scope, keep).await? else {
            return Ok(None);
        };
        let added = self.memory_add(input).await?;
        self.memory_expire(&expired).await?;
        Ok(Some(added))
    }

    /// Compute a consolidation **without writing**: the summary memory input + the ids of the
    /// originals to expire. Returns None if the scope is within `keep`. The gateway uses this in
    /// cluster mode to replicate consolidation through the Raft log (summary `MemoryUpsert` + the
    /// originals via `MemoryExpire`) instead of applying it only locally.
    pub async fn memory_consolidate_plan(
        &self,
        scope: &MemoryScope,
        keep: usize,
    ) -> Result<Option<(MemoryInput, Vec<uuid::Uuid>)>> {
        let actives = self
            .memory_store
            .list_active(scope, self.config.query.max_rows)
            .await?;
        if actives.len() <= keep {
            return Ok(None);
        }
        // list_active is ordered importance DESC, so the tail past `keep` is the lowest-importance set.
        let to_fold: Vec<Memory> = actives[keep..].to_vec();
        if to_fold.is_empty() {
            return Ok(None);
        }
        let summary = self.summarize_memories(&to_fold).await;
        let source_ids: Vec<String> = to_fold.iter().map(|m| m.id.to_string()).collect();
        let mut input = MemoryInput::new(scope.clone(), summary);
        input.importance = Some(0.6);
        input.metadata = serde_json::json!({
            "consolidated": true,
            "source_memory_ids": source_ids,
        });
        let expired: Vec<uuid::Uuid> = to_fold.iter().map(|m| m.id).collect();
        Ok(Some((input, expired)))
    }

    /// Summarize a set of memories (opt-in LLM, else a deterministic bullet list).
    async fn summarize_memories(&self, mems: &[Memory]) -> String {
        let joined = mems
            .iter()
            .map(|m| format!("- {}", m.content))
            .collect::<Vec<_>>()
            .join("\n");
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                if let Ok(text) = provider
                    .complete(
                        "Summarize the following memories into a concise paragraph capturing the \
                         durable facts. Output only the summary.",
                        &joined,
                    )
                    .await
                {
                    let t = text.trim();
                    if !t.is_empty() {
                        return t.to_string();
                    }
                }
            }
        }
        format!("Consolidated {} memories:\n{}", mems.len(), joined)
    }

    /// Add a graph edge (entity → relation → entity) for a tenant.
    pub async fn memory_link(
        &self,
        tenant: &str,
        src: &str,
        relation: &str,
        dst: &str,
        source: Option<uuid::Uuid>,
    ) -> Result<()> {
        let edge = crate::memory::cognition::Edge {
            id: uuid::Uuid::new_v4(),
            src: src.to_string(),
            relation: relation.to_string(),
            dst: dst.to_string(),
            weight: 1.0,
            source_memory_id: source,
        };
        self.memory_store.add_edge(tenant, &edge).await
    }

    /// Apply a fully-materialized graph edge (deterministic — used by Raft apply so every node
    /// inserts the identical row, edge id included).
    pub async fn graph_apply_edge(
        &self,
        tenant: Option<&str>,
        edge: &crate::memory::cognition::Edge,
    ) -> Result<()> {
        self.memory_store
            .add_edge(tenant.unwrap_or("default"), edge)
            .await
    }

    /// Edges incident to `entity` (its 1-hop neighborhood) for a tenant.
    pub async fn memory_neighbors(
        &self,
        tenant: &str,
        entity: &str,
        limit: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        self.memory_store
            .neighbors(tenant, entity, limit.min(self.config.query.max_rows))
            .await
    }

    /// Multi-hop neighborhood: BFS from `entity` out to `depth` hops, returning the reachable edges
    /// (deduplicated), capped at `max_edges`.
    pub async fn memory_subgraph(
        &self,
        tenant: &str,
        entity: &str,
        depth: usize,
        max_edges: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        let cap = max_edges.min(self.config.query.max_rows).max(1);
        let mut seen_entities = std::collections::HashSet::new();
        let mut seen_edges = std::collections::HashSet::new();
        let mut frontier = vec![entity.to_string()];
        seen_entities.insert(entity.to_string());
        let mut edges = Vec::new();
        for _ in 0..depth.max(1) {
            let mut next = Vec::new();
            for e in &frontier {
                for edge in self.memory_store.neighbors(tenant, e, cap).await? {
                    if seen_edges.insert(edge.id) {
                        for endpoint in [edge.src.clone(), edge.dst.clone()] {
                            if seen_entities.insert(endpoint.clone()) {
                                next.push(endpoint);
                            }
                        }
                        edges.push(edge);
                        if edges.len() >= cap {
                            return Ok(edges);
                        }
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok(edges)
    }

    /// Extract `(subject, relation, object)` triples from text and add them as graph edges.
    /// Returns the number of edges added.
    pub async fn memory_graph_from_text(
        &self,
        tenant: &str,
        text: &str,
        source: Option<uuid::Uuid>,
    ) -> Result<usize> {
        let triples = self.extract_triples_any(text).await;
        let n = triples.len();
        for (s, r, o) in triples {
            self.memory_link(tenant, &s, &r, &o, source).await?;
        }
        Ok(n)
    }

    /// Extract triples via LLM when `extraction = "llm"` + a provider is configured, else fall back
    /// to the deterministic verb-pattern extractor.
    async fn extract_triples_any(&self, text: &str) -> Vec<(String, String, String)> {
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                if let Ok(out) = provider
                    .complete(
                        "Extract factual relationships from the text as lines of \
                         `subject | relation | object`. Output only those lines, nothing else.",
                        text,
                    )
                    .await
                {
                    let parsed = parse_triple_lines(&out);
                    if !parsed.is_empty() {
                        return parsed;
                    }
                }
            }
        }
        crate::memory::cognition::extract_triples(text)
    }

    /// Extract facts from raw text (opt-in LLM, else single-memory fallback).
    async fn extract_facts(&self, raw_text: &str) -> Vec<(Option<String>, String)> {
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                match provider.complete(Self::EXTRACT_SYSTEM, raw_text).await {
                    Ok(text) => match parse_extracted_facts(&text) {
                        Some(facts) if !facts.is_empty() => return facts,
                        _ => tracing::warn!("LLM extraction returned no parseable facts"),
                    },
                    Err(e) => tracing::warn!(error = %e, "LLM extraction failed"),
                }
            }
        }
        // Deterministic fallback: store the text as a single memory.
        vec![(None, raw_text.to_string())]
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

    /// List event sources for a single tenant.
    pub async fn list_sources_for_tenant(&self, tenant: &str) -> Result<Vec<String>> {
        let episodic = self.episodic.clone();
        let tenant = tenant.to_string();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT source FROM episodic WHERE tenant_id = ? ORDER BY source")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let sources: Vec<String> = stmt
                .query_map(duckdb::params![tenant], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(sources)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// List agent IDs for a single tenant (the tenant prefix is stripped from the result).
    pub async fn list_agents_for_tenant(&self, tenant: &str) -> Result<Vec<String>> {
        let state = self.state.clone();
        let prefix = format!("{tenant}{TENANT_AGENT_SEP}");
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            let like = format!("{prefix}%");
            let mut stmt = db
                .prepare(
                    "SELECT DISTINCT agent_id FROM state WHERE agent_id LIKE ?1 ORDER BY agent_id",
                )
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let agents: Vec<String> = stmt
                .query_map(rusqlite::params![like], |row| row.get::<_, String>(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .map(|a| a.strip_prefix(&prefix).unwrap_or(&a).to_string())
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

        // Backup the memories store (DuckDB EXPORT)
        let mem_export = dir.join("memories_export");
        let memory_store = self.memory_store.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&mem_export)
                .map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;
            memory_store.export_to(&mem_export)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        // Backup the state store (SQLite VACUUM INTO)
        let state_path = dir.join("state.db");
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || state.backup_to(&state_path))
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        tracing::info!(path = %dir.display(), "backup complete");
        Ok(())
    }

    /// Restore all stores from a backup directory produced by [`Self::backup`].
    ///
    /// Used by the Raft snapshot-install path and for disaster recovery. Episodic and memories
    /// are restored atomically (stage-then-swap); the memory vector index is rebuilt afterward.
    pub async fn restore_from_backup(&self, dir: &std::path::Path) -> Result<()> {
        let ep_export = dir.join("episodic_export");
        if ep_export.exists() {
            let episodic = self.episodic.clone();
            let staging = dir.join("episodic_staging.duckdb");
            tokio::task::spawn_blocking(move || episodic.restore_from_export(&ep_export, &staging))
                .await
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let mem_export = dir.join("memories_export");
        if mem_export.exists() {
            let memory_store = self.memory_store.clone();
            let staging = dir.join("memories_staging.duckdb");
            tokio::task::spawn_blocking(move || {
                memory_store.restore_from_export(&mem_export, &staging)
            })
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let state_path = dir.join("state.db");
        if state_path.exists() {
            let state = self.state.clone();
            tokio::task::spawn_blocking(move || state.restore_from(&state_path))
                .await
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let vectors_dir = dir.join("vectors");
        if vectors_dir.exists() {
            self.semantic
                .load_from(&vectors_dir)
                .map_err(|e| crate::Error::Storage(format!("semantic restore: {e}")))?;
        }

        // Rebuild the memory vector index from the restored memories (no provider call).
        if let Ok(rows) = self.memory_store.load_active_with_embeddings().await {
            for (mem, emb) in rows {
                let _ = self.memory_index.upsert(&mem.to_semantic_entry(emb)).await;
            }
        }

        tracing::info!(path = %dir.display(), "restore complete");
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

/// Separator that namespaces an `agent_id` by tenant for state isolation. A control char
/// (unit separator) that is extremely unlikely to occur in a real agent id.
pub(crate) const TENANT_AGENT_SEP: char = '\u{1f}';

/// Namespace an agent id by tenant so agent state is isolated per tenant.
pub(crate) fn scoped_agent(tenant: &str, agent_id: &str) -> String {
    format!("{tenant}{TENANT_AGENT_SEP}{agent_id}")
}

/// Leniently parse an LLM extraction response into `(subject, content)` facts.
///
/// Tolerates surrounding prose / Markdown fences by extracting the outermost `[...]` array.
/// Parse LLM output of `subject | relation | object` lines into triples (relations normalized).
fn parse_triple_lines(text: &str) -> Vec<(String, String, String)> {
    text.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('|').map(|p| p.trim()).collect();
            if parts.len() == 3 && !parts[0].is_empty() && !parts[2].is_empty() {
                Some((
                    parts[0].to_string(),
                    parts[1].to_lowercase().replace(' ', "_"),
                    parts[2].to_string(),
                ))
            } else {
                None
            }
        })
        .collect()
}

fn parse_extracted_facts(text: &str) -> Option<Vec<(Option<String>, String)>> {
    let start = text.find('[')?;
    let end = text.rfind(']')?;
    if end <= start {
        return None;
    }
    let arr: Vec<serde_json::Value> = serde_json::from_str(&text[start..=end]).ok()?;
    let mut facts = Vec::new();
    for item in arr {
        let Some(content) = item
            .get("content")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let subject = item
            .get("subject")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        facts.push((subject, content));
    }
    Some(facts)
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
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn engine_ingest_and_count() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();

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
        let engine = StrataEngine::new(inmem_config()).await.unwrap();

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
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let rows = engine
            .query_sql("SELECT 42::VARCHAR as answer")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["answer"], "42");
    }

    #[tokio::test]
    async fn engine_semantic_search() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();

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

    /// Fully in-memory config so cognition tests don't touch `./data`.
    fn inmem_config() -> CoreConfig {
        let mut c = CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        c
    }

    #[tokio::test]
    async fn delete_tenant_erases_all_stores() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let ta = crate::config::TenantContext::new("tenant-a");

        // tenant-a data across stores.
        engine
            .ingest_for_tenant(vec![Event::new("s", "e", serde_json::json!({"x": 1}))], &ta)
            .await
            .unwrap();
        engine
            .memory_add(MemoryInput::new(
                MemoryScope::tenant("tenant-a"),
                "likes tea",
            ))
            .await
            .unwrap();
        engine
            .state_set_for_tenant("tenant-a", "bot", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        // tenant-b control data that must survive.
        engine
            .memory_add(MemoryInput::new(MemoryScope::tenant("tenant-b"), "b-fact"))
            .await
            .unwrap();

        let summary = engine.delete_tenant("tenant-a").await.unwrap();
        assert_eq!(summary["events_deleted"], 1);
        assert_eq!(summary["memories_deleted"], 1);
        assert_eq!(summary["state_deleted"], 1);

        // tenant-a is gone…
        let a_events = engine
            .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-a")
            .await
            .unwrap();
        assert_eq!(a_events[0]["c"], "0");
        assert_eq!(
            engine
                .memory_all(&MemoryScope::tenant("tenant-a"), 100)
                .await
                .unwrap()
                .len(),
            0
        );
        assert!(engine
            .state_get_for_tenant("tenant-a", "bot", "mood")
            .await
            .unwrap()
            .is_none());
        // …but tenant-b survives.
        assert_eq!(
            engine
                .memory_all(&MemoryScope::tenant("tenant-b"), 100)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn parse_triple_lines_parses_pipe_format() {
        let t = parse_triple_lines("Alice | likes | coffee\nBob | works at | Acme\ngarbage line");
        assert_eq!(t.len(), 2);
        assert_eq!(t[0], ("Alice".into(), "likes".into(), "coffee".into()));
        assert_eq!(t[1], ("Bob".into(), "works_at".into(), "Acme".into()));
    }

    #[tokio::test]
    async fn memory_subgraph_traverses_multiple_hops() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        // A → B → C → D chain.
        engine
            .memory_link("default", "A", "to", "B", None)
            .await
            .unwrap();
        engine
            .memory_link("default", "B", "to", "C", None)
            .await
            .unwrap();
        engine
            .memory_link("default", "C", "to", "D", None)
            .await
            .unwrap();
        // 1 hop from A reaches only the A→B edge.
        assert_eq!(
            engine
                .memory_subgraph("default", "A", 1, 100)
                .await
                .unwrap()
                .len(),
            1
        );
        // 2 hops reaches A→B and B→C.
        assert_eq!(
            engine
                .memory_subgraph("default", "A", 2, 100)
                .await
                .unwrap()
                .len(),
            2
        );
        // 3 hops reaches the whole chain.
        assert_eq!(
            engine
                .memory_subgraph("default", "A", 3, 100)
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn multimodal_indexes_support_mixed_dimensions() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        // Different modalities can have DIFFERENT vector dimensions (separate per-modality indexes).
        let text_vec = vec![0.1f32; 768]; // e.g. a text embedder
        let img_vec = vec![0.2f32; 512]; // e.g. CLIP image embeddings
        engine
            .semantic_upsert_modal(
                uuid::Uuid::new_v4(),
                "text",
                "a caption",
                text_vec.clone(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        engine
            .semantic_upsert_modal(
                uuid::Uuid::new_v4(),
                "image",
                "cat.png",
                img_vec.clone(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(
            engine.modalities().len(),
            2,
            "two independent modality indexes"
        );

        // Each modality searches with its own dimension.
        let img_hits = engine
            .semantic_search_modal(&img_vec, 5, Some("image"))
            .await
            .unwrap();
        assert_eq!(img_hits.len(), 1);
        assert_eq!(img_hits[0].entry.content, "cat.png");
        let text_hits = engine
            .semantic_search_modal(&text_vec, 5, Some("text"))
            .await
            .unwrap();
        assert_eq!(text_hits.len(), 1);
        assert_eq!(text_hits[0].entry.content, "a caption");

        // search_all with a 512-d vector only hits the matching-dimension (image) index.
        let all = engine
            .semantic_search_modal(&img_vec, 5, None)
            .await
            .unwrap();
        assert!(all.iter().all(|h| h.entry.content == "cat.png"));
    }

    #[tokio::test]
    async fn unembedded_events_are_visible_for_reindex() {
        // No embedding provider configured → events are ingested but left unembedded, and the gap
        // is now visible/recoverable instead of silently lost.
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        engine
            .ingest(vec![
                Event::new("s", "e", serde_json::json!({"x": 1})),
                Event::new("s", "e", serde_json::json!({"x": 2})),
                Event::new("s", "e", serde_json::json!({"x": 3})),
            ])
            .await
            .unwrap();
        assert_eq!(engine.unembedded_count().await.unwrap(), 3);
        // Reindex without a provider is a no-op (nothing to embed), leaving them recoverable.
        assert_eq!(engine.reindex_unembedded(100).await.unwrap(), 0);
        assert_eq!(engine.unembedded_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn migrate_tenant_memories_moves_between_engines() {
        // Simulates a rebalance move: tenant-a's memories move from shard A to shard B.
        let shard_a = StrataEngine::new(inmem_config()).await.unwrap();
        let shard_b = StrataEngine::new(inmem_config()).await.unwrap();
        for i in 0..3 {
            shard_a
                .memory_add(MemoryInput::new(
                    MemoryScope::tenant("tenant-a"),
                    format!("fact {i}"),
                ))
                .await
                .unwrap();
        }
        // A different tenant on B must be untouched.
        shard_b
            .memory_add(MemoryInput::new(MemoryScope::tenant("tenant-b"), "b-fact"))
            .await
            .unwrap();

        let moved = shard_a
            .migrate_tenant_memories_to(&shard_b, "tenant-a")
            .await
            .unwrap();
        assert_eq!(moved, 3);
        // tenant-a is gone from A, present on B; tenant-b on B survives.
        assert_eq!(
            shard_a
                .export_tenant_memories("tenant-a")
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            shard_b
                .export_tenant_memories("tenant-a")
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            shard_b
                .export_tenant_memories("tenant-b")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn migrate_tenant_memories_preserves_source_events() {
        // A rebalance memory-move must NOT cascade-delete the tenant's episodic events on the source.
        let a = StrataEngine::new(inmem_config()).await.unwrap();
        let b = StrataEngine::new(inmem_config()).await.unwrap();
        let ta = crate::config::TenantContext::new("t");
        a.ingest_for_tenant(vec![Event::new("s", "e", serde_json::json!({"x": 1}))], &ta)
            .await
            .unwrap();
        a.memory_add(MemoryInput::new(MemoryScope::tenant("t"), "fact"))
            .await
            .unwrap();

        a.migrate_tenant_memories_to(&b, "t").await.unwrap();

        // Memories moved off the source, onto the destination.
        assert_eq!(
            a.memory_all(&MemoryScope::tenant("t"), 10)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            b.memory_all(&MemoryScope::tenant("t"), 10)
                .await
                .unwrap()
                .len(),
            1
        );
        // Episodic events stay on the source (not cascade-deleted).
        let ev = a
            .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "t")
            .await
            .unwrap();
        assert_eq!(ev[0]["c"], "1");
    }

    #[tokio::test]
    async fn memory_consolidate_folds_lowest_importance() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");
        for i in 0..5 {
            engine
                .memory_add(MemoryInput::new(scope.clone(), format!("fact number {i}")))
                .await
                .unwrap();
        }
        // Keep the top 2; fold the other 3 into one summary memory.
        let consolidated = engine.memory_consolidate(&scope, 2).await.unwrap();
        assert!(consolidated.is_some());
        let mems = engine.memory_all(&scope, 100).await.unwrap();
        assert_eq!(
            mems.len(),
            3,
            "2 kept + 1 consolidated; the 3 originals are expired"
        );
        let summary = mems
            .iter()
            .find(|m| m.content.starts_with("Consolidated 3 memories"))
            .expect("a consolidated memory should exist");
        assert_eq!(summary.metadata["consolidated"], serde_json::json!(true));
        // Nothing to fold when within budget.
        assert!(engine
            .memory_consolidate(&scope, 10)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_type_roundtrips_and_defaults() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");
        // Default type is "semantic".
        let added = engine
            .memory_add(MemoryInput::new(scope.clone(), "a plain fact"))
            .await
            .unwrap();
        assert_eq!(added.memory.mem_type, "semantic");
        // Explicit "procedural" type round-trips through DuckDB.
        let mut input = MemoryInput::new(scope.clone(), "how to deploy: run make");
        input.mem_type = Some("procedural".into());
        engine.memory_add(input).await.unwrap();
        let mems = engine.memory_all(&scope, 100).await.unwrap();
        assert!(mems
            .iter()
            .any(|m| m.mem_type == "procedural" && m.content.contains("deploy")));
        assert!(mems.iter().any(|m| m.mem_type == "semantic"));
    }

    #[tokio::test]
    async fn memory_scope_cap_evicts_lowest() {
        let mut cfg = inmem_config();
        cfg.memory.cognition.max_memories_per_scope = 3;
        let engine = StrataEngine::new(cfg).await.unwrap();
        let scope = MemoryScope::user("alice");
        for i in 0..5 {
            engine
                .memory_add(MemoryInput::new(
                    scope.clone(),
                    format!("distinct fact {i}"),
                ))
                .await
                .unwrap();
        }
        let mems = engine.memory_all(&scope, 100).await.unwrap();
        assert_eq!(mems.len(), 3, "scope should be capped at 3 memories");
    }

    #[tokio::test]
    async fn memory_all_clamps_to_max_rows() {
        let mut cfg = inmem_config();
        cfg.query.max_rows = 2; // hard cap
        let engine = StrataEngine::new(cfg).await.unwrap();
        let scope = MemoryScope::user("alice");
        for i in 0..5 {
            engine
                .memory_add(MemoryInput::new(scope.clone(), format!("fact {i}")))
                .await
                .unwrap();
        }
        // A huge requested limit is clamped to max_rows.
        let mems = engine.memory_all(&scope, usize::MAX).await.unwrap();
        assert_eq!(mems.len(), 2, "memory_all must clamp to query.max_rows");
    }

    #[tokio::test]
    async fn memory_add_insert_and_get() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let added = engine
            .memory_add(MemoryInput::new(
                MemoryScope::user("alice"),
                "likes espresso",
            ))
            .await
            .unwrap();
        assert_eq!(added.outcome, MemoryOutcome::Inserted);
        let got = engine.memory_get(added.memory.id).await.unwrap().unwrap();
        assert_eq!(got.content, "likes espresso");
        assert_eq!(engine.memory_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn memory_subject_contradiction_supersedes_with_history() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");

        let first = engine
            .memory_add(
                MemoryInput::new(scope.clone(), "favorite color is blue")
                    .with_subject("favorite_color"),
            )
            .await
            .unwrap();
        assert_eq!(first.outcome, MemoryOutcome::Inserted);

        let second = engine
            .memory_add(
                MemoryInput::new(scope.clone(), "favorite color is green")
                    .with_subject("favorite_color"),
            )
            .await
            .unwrap();
        assert_eq!(second.outcome, MemoryOutcome::Superseded);
        assert_eq!(second.memory.supersedes, Some(first.memory.id));

        // Only the latest is active.
        let active = engine.memory_all(&scope, 10).await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].content, "favorite color is green");

        // History keeps both, oldest first.
        let hist = engine
            .memory_history(&scope, "favorite_color")
            .await
            .unwrap();
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].content, "favorite color is blue");

        // Bi-temporal: the superseded value is still answerable "as of" its validity window.
        let before = engine
            .memory_as_of(&scope, "favorite_color", first.memory.valid_from)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.content, "favorite color is blue");
    }

    #[tokio::test]
    async fn memory_identical_is_confirmed_not_duplicated() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("bob");
        engine
            .memory_add(MemoryInput::new(scope.clone(), "works at ACME").with_subject("employer"))
            .await
            .unwrap();
        let again = engine
            .memory_add(MemoryInput::new(scope.clone(), "works at ACME").with_subject("employer"))
            .await
            .unwrap();
        assert_eq!(again.outcome, MemoryOutcome::Confirmed);
        assert_eq!(engine.memory_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn memory_search_lexical_ranks_relevant_first() {
        // No embedding provider in the default config → pure deterministic BM25 ranking.
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");
        for content in [
            "alice loves hiking in the mountains",
            "alice works as a software engineer",
            "the weather is sunny today",
        ] {
            engine
                .memory_add(MemoryInput::new(scope.clone(), content))
                .await
                .unwrap();
        }

        let hits = engine
            .memory_search("software engineering job", &scope, 3)
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].memory.content, "alice works as a software engineer");
    }

    #[test]
    fn parse_extracted_facts_handles_fenced_json() {
        let text = "Sure!\n```json\n[{\"subject\":\"city\",\"content\":\"Lives in Paris\"},\
                    {\"content\":\"Likes jazz\"}]\n```";
        let facts = super::parse_extracted_facts(text).unwrap();
        assert_eq!(facts.len(), 2);
        assert_eq!(
            facts[0],
            (Some("city".to_string()), "Lives in Paris".to_string())
        );
        assert_eq!(facts[1], (None, "Likes jazz".to_string()));
    }

    #[tokio::test]
    async fn memory_remember_fallback_stores_raw_text() {
        // Default config has extraction = "none" → deterministic single-memory fallback.
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");
        let added = engine
            .memory_remember("alice prefers tea over coffee", &scope)
            .await
            .unwrap();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].memory.content, "alice prefers tea over coffee");
        assert_eq!(engine.memory_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn memory_enforce_decay_keeps_fresh_memories() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let scope = MemoryScope::user("alice");
        engine
            .memory_add(MemoryInput::new(scope.clone(), "fresh fact"))
            .await
            .unwrap();
        // Nothing is old enough to forget yet.
        assert_eq!(engine.memory_enforce_decay().await.unwrap(), 0);
        assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn semantic_search_for_tenant_isolates() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        let mut v = vec![0.0f32; 768];
        v[0] = 1.0; // both entries point the same way → both would match without scoping
        engine
            .semantic_upsert(&SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: "tenant A secret".into(),
                embedding: v.clone(),
                metadata: serde_json::json!({"tenant_id": "tenant-a"}),
            })
            .await
            .unwrap();
        engine
            .semantic_upsert(&SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: "tenant B secret".into(),
                embedding: v.clone(),
                metadata: serde_json::json!({"tenant_id": "tenant-b"}),
            })
            .await
            .unwrap();

        let hits = engine
            .semantic_search_for_tenant(&v, 5, "tenant-a", None, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.content, "tenant A secret");
    }

    #[tokio::test]
    async fn memory_scope_isolation() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        engine
            .memory_add(MemoryInput::new(MemoryScope::user("alice"), "secret A"))
            .await
            .unwrap();
        engine
            .memory_add(MemoryInput::new(MemoryScope::user("bob"), "secret B"))
            .await
            .unwrap();

        let alice = engine
            .memory_all(&MemoryScope::user("alice"), 10)
            .await
            .unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].content, "secret A");
    }

    #[tokio::test]
    async fn backup_and_restore_roundtrips_all_stores() {
        let dir = tempfile::tempdir().unwrap();
        let backup_dir = dir.path().join("backup");

        // Source: an episodic event, a memory, and agent state.
        let src = StrataEngine::new(inmem_config()).await.unwrap();
        src.ingest(vec![Event::new("src", "e", serde_json::json!({"x": 1}))])
            .await
            .unwrap();
        src.memory_add(MemoryInput::new(MemoryScope::user("alice"), "likes tea"))
            .await
            .unwrap();
        src.state_set("bot", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        src.backup(&backup_dir).await.unwrap();

        // Fresh engine → restore → all three stores are present.
        let dst = StrataEngine::new(inmem_config()).await.unwrap();
        assert_eq!(dst.event_count().await.unwrap(), 0);
        dst.restore_from_backup(&backup_dir).await.unwrap();

        assert_eq!(dst.event_count().await.unwrap(), 1);
        assert_eq!(dst.memory_count().await.unwrap(), 1);
        assert_eq!(
            dst.memory_all(&MemoryScope::user("alice"), 10)
                .await
                .unwrap()[0]
                .content,
            "likes tea"
        );
        assert_eq!(
            dst.state_get("bot", "mood").await.unwrap().unwrap().value,
            serde_json::json!("happy")
        );
    }

    #[tokio::test]
    async fn concurrent_tenant_ingest_does_not_cross_tag() {
        let engine = Arc::new(StrataEngine::new(inmem_config()).await.unwrap());
        let (e1, e2) = (engine.clone(), engine.clone());
        let h1 = tokio::spawn(async move {
            for i in 0..50 {
                e1.ingest_for_tenant(
                    vec![Event::new("a", "e", serde_json::json!({ "n": i }))],
                    &crate::config::TenantContext::new("tenant-a"),
                )
                .await
                .unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            for i in 0..50 {
                e2.ingest_for_tenant(
                    vec![Event::new("b", "e", serde_json::json!({ "n": i }))],
                    &crate::config::TenantContext::new("tenant-b"),
                )
                .await
                .unwrap();
            }
        });
        h1.await.unwrap();
        h2.await.unwrap();

        // Each tenant sees EXACTLY its own 50 events — no cross-tagging under concurrency.
        let a = engine
            .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-a")
            .await
            .unwrap();
        assert_eq!(a[0]["c"], "50");
        let b = engine
            .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-b")
            .await
            .unwrap();
        assert_eq!(b[0]["c"], "50");
    }

    #[tokio::test]
    async fn strata_state_sql_function_is_tenant_scoped() {
        let engine = StrataEngine::new(inmem_config()).await.unwrap();
        engine
            .state_set_for_tenant("tenant-a", "bot", "secret", serde_json::json!("a-value"))
            .await
            .unwrap();

        // tenant-b querying the same agent/key via strata_state() sees nothing.
        let rows = engine
            .query_sql_for_tenant("SELECT * FROM strata_state('bot', 'secret')", "tenant-b")
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "strata_state() leaked tenant-a state to tenant-b!"
        );

        // tenant-a sees its own.
        let rows = engine
            .query_sql_for_tenant("SELECT * FROM strata_state('bot', 'secret')", "tenant-a")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }
}
