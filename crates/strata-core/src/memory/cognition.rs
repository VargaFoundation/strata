//! Memory cognition store — first-class, bi-temporal *memories* (distinct from raw events).
//!
//! Where [`super::episodic`] is an append-only log of *what happened*, this store holds
//! distilled *memories*: atomic facts/statements about a subject, scoped to a
//! tenant/user/agent/session, with bi-temporal validity (`valid_from`/`valid_to`) so the
//! engine can answer **"what was true at time T"** and resolve contradictions over time
//! (supersede an old memory when a newer, conflicting one arrives) — deterministically,
//! without any LLM.
//!
//! Backed by DuckDB (same engine/pattern as [`super::episodic::EpisodicStore`]). The vector
//! index used for semantic dedup/search lives separately in [`super::semantic::SemanticStore`];
//! the embedding is also persisted here as a column so the index is rebuildable on restart
//! without re-calling the embedding provider.

use chrono::{DateTime, Utc};
use duckdb::Connection;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use super::semantic::SemanticEntry;

/// The scope a memory belongs to. Memories are matched (for dedup, contradiction and
/// search) on the **exact** scope tuple — a memory stored for user `alice` is never
/// confused with one for user `bob` or with an unscoped (`None`) memory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryScope {
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

impl MemoryScope {
    /// Scope for a tenant (default tenant is `"default"`).
    pub fn tenant(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            ..Default::default()
        }
    }

    /// Scope for a specific user within the default tenant.
    pub fn user(user_id: impl Into<String>) -> Self {
        Self {
            tenant_id: "default".into(),
            user_id: Some(user_id.into()),
            ..Default::default()
        }
    }

    /// Build the `WHERE` fragment + ordered params for an exact scope match.
    /// `None` fields match `IS NULL` (exact), `Some(v)` fields match `= v`.
    fn where_clause(&self) -> (String, Vec<String>) {
        let mut clauses = vec!["tenant_id = ?".to_string()];
        let mut params = vec![if self.tenant_id.is_empty() {
            "default".to_string()
        } else {
            self.tenant_id.clone()
        }];
        for (col, val) in [
            ("user_id", &self.user_id),
            ("agent_id", &self.agent_id),
            ("session_id", &self.session_id),
        ] {
            match val {
                Some(v) => {
                    clauses.push(format!("{col} = ?"));
                    params.push(v.clone());
                }
                None => clauses.push(format!("{col} IS NULL")),
            }
        }
        (clauses.join(" AND "), params)
    }
}

/// Lifecycle state of a memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryState {
    /// Currently believed true.
    Active,
    /// Replaced by a newer, conflicting memory (still kept for history / "as of T").
    Superseded,
    /// Aged out / forgotten.
    Expired,
}

impl MemoryState {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryState::Active => "active",
            MemoryState::Superseded => "superseded",
            MemoryState::Expired => "expired",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "superseded" => MemoryState::Superseded,
            "expired" => MemoryState::Expired,
            _ => MemoryState::Active,
        }
    }
}

/// A first-class memory: an atomic, scoped, bi-temporal fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    pub id: Uuid,
    #[serde(flatten)]
    pub scope: MemoryScope,
    /// Optional stable "key" the memory is about (e.g. `user.favorite_color`). When set,
    /// a newer memory with the same `(scope, subject)` but different content supersedes the
    /// old one — this is the deterministic contradiction-resolution signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub content: String,
    pub importance: f32,
    pub valid_from: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<DateTime<Utc>>,
    pub state: MemoryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_event_ids: Vec<Uuid>,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl Memory {
    /// Create a fresh, active memory valid from now.
    pub fn new(scope: MemoryScope, content: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            scope,
            subject: None,
            content: content.into(),
            importance: 0.5,
            valid_from: now,
            valid_to: None,
            state: MemoryState::Active,
            supersedes: None,
            source_event_ids: Vec::new(),
            version: 1,
            created_at: now,
            updated_at: now,
            metadata: serde_json::json!({}),
        }
    }

    /// Build the vector-index entry for this memory (id-stable so updates re-key cleanly).
    pub fn to_semantic_entry(&self, embedding: Vec<f32>) -> SemanticEntry {
        SemanticEntry {
            id: self.id,
            content: self.content.clone(),
            embedding,
            metadata: serde_json::json!({
                "kind": "memory",
                "tenant_id": self.scope.tenant_id,
                "user_id": self.scope.user_id,
                "agent_id": self.scope.agent_id,
                "session_id": self.scope.session_id,
                "subject": self.subject,
            }),
        }
    }
}

/// Input to add a memory through the cognition pipeline.
#[derive(Debug, Clone)]
pub struct MemoryInput {
    pub scope: MemoryScope,
    pub subject: Option<String>,
    pub content: String,
    pub importance: Option<f32>,
    pub source_event_ids: Vec<Uuid>,
    pub metadata: serde_json::Value,
}

impl MemoryInput {
    /// Minimal input: a scope and content.
    pub fn new(scope: MemoryScope, content: impl Into<String>) -> Self {
        Self {
            scope,
            subject: None,
            content: content.into(),
            importance: None,
            source_event_ids: Vec::new(),
            metadata: serde_json::json!({}),
        }
    }

    /// Set the stable subject key (enables deterministic contradiction resolution).
    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }
}

/// A memory search hit with similarity score (0.0 when ranked by recency fallback).
#[derive(Debug, Clone, Serialize)]
pub struct MemoryHit {
    pub memory: Memory,
    pub score: f32,
}

/// What the cognition pipeline did with an added memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOutcome {
    /// A brand-new memory was inserted.
    Inserted,
    /// An existing identical memory was reinforced (no new row).
    Confirmed,
    /// A near-duplicate was merged/updated (semantic dedup / consolidation).
    Merged,
    /// A contradicting memory was superseded and a new one inserted.
    Superseded,
}

/// Result of adding a memory through the cognition pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryAdd {
    pub memory: Memory,
    pub outcome: MemoryOutcome,
}

/// Tokenize text into lowercase alphanumeric terms (shared by lexical search).
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// BM25 lexical ranking of `candidates` against `query` — deterministic, no model, no
/// external dependency. Returns `(index_into_candidates, score)` sorted by score desc,
/// keeping only positive scores. This is the always-on keyword half of hybrid search.
pub fn lexical_rank(query: &str, candidates: &[Memory]) -> Vec<(usize, f32)> {
    use std::collections::{HashMap, HashSet};

    let q_terms: HashSet<String> = tokenize(query).into_iter().collect();
    if q_terms.is_empty() || candidates.is_empty() {
        return Vec::new();
    }

    let docs: Vec<Vec<String>> = candidates.iter().map(|m| tokenize(&m.content)).collect();
    let n = docs.len() as f32;
    let avgdl = (docs.iter().map(|d| d.len()).sum::<usize>() as f32 / n).max(1.0);

    // Document frequency per query term.
    let mut df: HashMap<&str, f32> = HashMap::new();
    for d in &docs {
        let unique: HashSet<&str> = d.iter().map(|s| s.as_str()).collect();
        for t in &q_terms {
            if unique.contains(t.as_str()) {
                *df.entry(t.as_str()).or_insert(0.0) += 1.0;
            }
        }
    }

    const K1: f32 = 1.2;
    const B: f32 = 0.75;
    let mut scored: Vec<(usize, f32)> = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        let dl = d.len() as f32;
        let mut score = 0.0;
        for t in &q_terms {
            let f = d.iter().filter(|w| w.as_str() == t.as_str()).count() as f32;
            if f == 0.0 {
                continue;
            }
            let n_q = *df.get(t.as_str()).unwrap_or(&0.0);
            let idf = ((n - n_q + 0.5) / (n_q + 0.5) + 1.0).ln();
            let denom = f + K1 * (1.0 - B + B * dl / avgdl);
            score += idf * (f * (K1 + 1.0)) / denom.max(1e-6);
        }
        if score > 0.0 {
            scored.push((i, score));
        }
    }
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored
}

/// Reciprocal Rank Fusion of several ranked id lists. Deterministic, weightless fusion of
/// the vector and lexical rankings into one ordering. Returns `(id, fused_score)` desc,
/// truncated to `top_k`.
pub fn rrf_fuse(rankings: &[Vec<Uuid>], k_rrf: f32, top_k: usize) -> Vec<(Uuid, f32)> {
    use std::collections::HashMap;
    let mut scores: HashMap<Uuid, f32> = HashMap::new();
    for ranking in rankings {
        for (rank, id) in ranking.iter().enumerate() {
            *scores.entry(*id).or_insert(0.0) += 1.0 / (k_rrf + (rank as f32 + 1.0));
        }
    }
    let mut fused: Vec<(Uuid, f32)> = scores.into_iter().collect();
    fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    fused.truncate(top_k);
    fused
}

/// Whether a stored vector entry's metadata matches the requested scope (exact match,
/// `None`/null fields included). Used to keep dedup/search scoped to the right owner.
pub fn scope_matches_metadata(scope: &MemoryScope, metadata: &serde_json::Value) -> bool {
    let want_tenant = if scope.tenant_id.is_empty() {
        "default"
    } else {
        scope.tenant_id.as_str()
    };
    let get = |k: &str| metadata.get(k).and_then(|v| v.as_str());
    if get("tenant_id").unwrap_or("default") != want_tenant {
        return false;
    }
    for (k, v) in [
        ("user_id", &scope.user_id),
        ("agent_id", &scope.agent_id),
        ("session_id", &scope.session_id),
    ] {
        match (v.as_deref(), get(k)) {
            (Some(want), Some(got)) if want == got => {}
            (None, None) => {}
            _ => return false,
        }
    }
    true
}

/// A fully-materialized memory row plus its embedding — the unit replicated through Raft so a
/// follower can persist the row AND (re)index its vector without re-running cognition. The leader
/// computes these via [`crate::StrataEngine::memory_plan`]; every node applies them identically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRow {
    pub memory: Memory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
}

/// Bi-temporal memory store backed by DuckDB.
///
/// Mirrors [`super::episodic::EpisodicStore`]: one write connection + a round-robin pool of
/// read connections (via `try_clone`).
pub struct MemoryStore {
    write_db: Arc<Mutex<Connection>>,
    read_pool: Vec<Mutex<Connection>>,
    read_next: AtomicUsize,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("read_pool_size", &self.read_pool.len())
            .finish()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    const DEFAULT_READ_POOL_SIZE: usize = 4;

    const SELECT_COLS: &'static str = "id, tenant_id, user_id, agent_id, session_id, subject, \
         content, importance, valid_from::VARCHAR, valid_to::VARCHAR, state, supersedes, \
         source_event_ids, version, created_at::VARCHAR, updated_at::VARCHAR, metadata::VARCHAR";

    /// Create an in-memory store (testing).
    pub fn new() -> Self {
        Self::open(Path::new(":memory:"), Self::DEFAULT_READ_POOL_SIZE)
            .expect("failed to create in-memory memory store")
    }

    /// Open or create a memory store at the given path.
    pub fn open(path: &Path, read_pool_size: usize) -> crate::Result<Self> {
        let read_pool_size = read_pool_size.max(1);
        let write_conn = if path.as_os_str() == ":memory:" {
            Connection::open_in_memory()
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::Error::Storage(format!("failed to create directory: {e}"))
                })?;
            }
            Connection::open(path)
        }
        .map_err(|e| crate::Error::Storage(format!("failed to open duckdb: {e}")))?;

        Self::init_schema(&write_conn)?;

        let mut read_pool = Vec::with_capacity(read_pool_size);
        for _ in 0..read_pool_size {
            let conn = write_conn
                .try_clone()
                .map_err(|e| crate::Error::Storage(format!("failed to clone read conn: {e}")))?;
            read_pool.push(Mutex::new(conn));
        }

        Ok(Self {
            write_db: Arc::new(Mutex::new(write_conn)),
            read_pool,
            read_next: AtomicUsize::new(0),
        })
    }

    fn init_schema(conn: &Connection) -> crate::Result<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS memories (
                id               VARCHAR PRIMARY KEY,
                tenant_id        VARCHAR NOT NULL DEFAULT 'default',
                user_id          VARCHAR,
                agent_id         VARCHAR,
                session_id       VARCHAR,
                subject          VARCHAR,
                content          VARCHAR NOT NULL,
                importance       DOUBLE NOT NULL DEFAULT 0.5,
                valid_from       TIMESTAMPTZ NOT NULL,
                valid_to         TIMESTAMPTZ,
                state            VARCHAR NOT NULL DEFAULT 'active',
                supersedes       VARCHAR,
                source_event_ids VARCHAR,
                version          BIGINT NOT NULL DEFAULT 1,
                created_at       TIMESTAMPTZ NOT NULL,
                updated_at       TIMESTAMPTZ NOT NULL,
                metadata         JSON DEFAULT '{}',
                embedding        JSON
            );",
        )
        .map_err(|e| crate::Error::Storage(format!("failed to create memories table: {e}")))?;

        let _ = conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories(tenant_id, user_id, agent_id);
             CREATE INDEX IF NOT EXISTS idx_memories_subject ON memories(subject);
             CREATE INDEX IF NOT EXISTS idx_memories_state ON memories(state);",
        );

        Ok(())
    }

    fn read_conn(&self) -> parking_lot::MutexGuard<'_, Connection> {
        let idx = self.read_next.fetch_add(1, Ordering::Relaxed) % self.read_pool.len();
        self.read_pool[idx].lock()
    }

    fn parse_memory(row: &duckdb::Row<'_>) -> duckdb::Result<Memory> {
        let id_str: String = row.get(0)?;
        let importance: f64 = row.get(7)?;
        let valid_from: String = row.get(8)?;
        let valid_to: Option<String> = row.get(9).ok().flatten();
        let state_str: String = row.get(10)?;
        let supersedes: Option<String> = row.get(11).ok().flatten();
        let source_ids: Option<String> = row.get(12).ok().flatten();
        let version: i64 = row.get(13)?;
        let created_at: String = row.get(14)?;
        let updated_at: String = row.get(15)?;
        let metadata_str: Option<String> = row.get(16).ok().flatten();

        let parse_ts = |s: &str| {
            // DuckDB renders TIMESTAMPTZ::VARCHAR as "YYYY-MM-DD HH:MM:SS.ffffff+00" (space
            // separator, short offset) — not RFC3339 — so try both forms before giving up.
            DateTime::parse_from_rfc3339(s)
                .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z"))
                .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z"))
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now())
        };

        Ok(Memory {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::nil()),
            scope: MemoryScope {
                tenant_id: row.get::<_, String>(1).unwrap_or_else(|_| "default".into()),
                user_id: row.get::<_, Option<String>>(2).ok().flatten(),
                agent_id: row.get::<_, Option<String>>(3).ok().flatten(),
                session_id: row.get::<_, Option<String>>(4).ok().flatten(),
            },
            subject: row.get::<_, Option<String>>(5).ok().flatten(),
            content: row.get(6)?,
            importance: importance as f32,
            valid_from: parse_ts(&valid_from),
            valid_to: valid_to.as_deref().map(parse_ts),
            state: MemoryState::from_str(&state_str),
            supersedes: supersedes.and_then(|s| Uuid::parse_str(&s).ok()),
            source_event_ids: source_ids
                .map(|s| {
                    s.split(',')
                        .filter_map(|t| Uuid::parse_str(t).ok())
                        .collect()
                })
                .unwrap_or_default(),
            version: version as u64,
            created_at: parse_ts(&created_at),
            updated_at: parse_ts(&updated_at),
            metadata: metadata_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::Value::Null),
        })
    }

    /// Run a scoped SELECT and parse results into memories.
    fn query_memories(&self, sql: &str, params: &[String]) -> crate::Result<Vec<Memory>> {
        let db = self.read_conn();
        let mut stmt = db
            .prepare(sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let boxed: Vec<Box<dyn duckdb::ToSql>> = params
            .iter()
            .map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql>)
            .collect();
        let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
        let rows = stmt
            .query_map(refs.as_slice(), Self::parse_memory)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Insert a memory, optionally persisting its embedding for index rebuild.
    pub async fn insert(&self, memory: &Memory, embedding: Option<&[f32]>) -> crate::Result<()> {
        let db = self.write_db.lock();
        let source_ids = if memory.source_event_ids.is_empty() {
            None
        } else {
            Some(
                memory
                    .source_event_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };
        let embedding_json = embedding.map(|e| serde_json::to_string(e).unwrap_or_default());
        let metadata_str = serde_json::to_string(&memory.metadata).unwrap_or_else(|_| "{}".into());

        db.execute(
            "INSERT OR IGNORE INTO memories
             (id, tenant_id, user_id, agent_id, session_id, subject, content, importance,
              valid_from, valid_to, state, supersedes, source_event_ids, version,
              created_at, updated_at, metadata, embedding)
             VALUES (?,?,?,?,?,?,?,?, ?::TIMESTAMPTZ,?::TIMESTAMPTZ,?,?,?,?, \
                     ?::TIMESTAMPTZ,?::TIMESTAMPTZ,?::JSON,?::JSON)",
            duckdb::params![
                memory.id.to_string(),
                memory.scope.tenant_id,
                memory.scope.user_id,
                memory.scope.agent_id,
                memory.scope.session_id,
                memory.subject,
                memory.content,
                memory.importance as f64,
                memory.valid_from.to_rfc3339(),
                memory.valid_to.map(|t| t.to_rfc3339()),
                memory.state.as_str(),
                memory.supersedes.map(|s| s.to_string()),
                source_ids,
                memory.version as i64,
                memory.created_at.to_rfc3339(),
                memory.updated_at.to_rfc3339(),
                metadata_str,
                embedding_json,
            ],
        )
        .map_err(|e| crate::Error::Ingest(format!("insert memory: {e}")))?;
        Ok(())
    }

    /// Insert or **replace** a fully-materialized memory row by id (deterministic).
    ///
    /// Used by Raft apply to replicate the result of cognition that the leader already computed
    /// (so followers don't re-run non-deterministic dedup/contradiction/LLM logic).
    pub async fn upsert_raw(
        &self,
        memory: &Memory,
        embedding: Option<&[f32]>,
    ) -> crate::Result<()> {
        let db = self.write_db.lock();
        let source_ids = if memory.source_event_ids.is_empty() {
            None
        } else {
            Some(
                memory
                    .source_event_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };
        let embedding_json = embedding.map(|e| serde_json::to_string(e).unwrap_or_default());
        let metadata_str = serde_json::to_string(&memory.metadata).unwrap_or_else(|_| "{}".into());
        db.execute(
            "INSERT OR REPLACE INTO memories
             (id, tenant_id, user_id, agent_id, session_id, subject, content, importance,
              valid_from, valid_to, state, supersedes, source_event_ids, version,
              created_at, updated_at, metadata, embedding)
             VALUES (?,?,?,?,?,?,?,?, ?::TIMESTAMPTZ,?::TIMESTAMPTZ,?,?,?,?, \
                     ?::TIMESTAMPTZ,?::TIMESTAMPTZ,?::JSON,?::JSON)",
            duckdb::params![
                memory.id.to_string(),
                memory.scope.tenant_id,
                memory.scope.user_id,
                memory.scope.agent_id,
                memory.scope.session_id,
                memory.subject,
                memory.content,
                memory.importance as f64,
                memory.valid_from.to_rfc3339(),
                memory.valid_to.map(|t| t.to_rfc3339()),
                memory.state.as_str(),
                memory.supersedes.map(|s| s.to_string()),
                source_ids,
                memory.version as i64,
                memory.created_at.to_rfc3339(),
                memory.updated_at.to_rfc3339(),
                metadata_str,
                embedding_json,
            ],
        )
        .map_err(|e| crate::Error::Ingest(format!("upsert memory: {e}")))?;
        Ok(())
    }

    /// Get a memory by id.
    pub async fn get(&self, id: Uuid) -> crate::Result<Option<Memory>> {
        let sql = format!("SELECT {} FROM memories WHERE id = ?", Self::SELECT_COLS);
        Ok(self
            .query_memories(&sql, &[id.to_string()])?
            .into_iter()
            .next())
    }

    /// Get a memory by id, scoped to a tenant (returns None if it belongs to another tenant).
    pub async fn get_scoped(&self, id: Uuid, tenant: &str) -> crate::Result<Option<Memory>> {
        let sql = format!(
            "SELECT {} FROM memories WHERE id = ? AND tenant_id = ?",
            Self::SELECT_COLS
        );
        Ok(self
            .query_memories(&sql, &[id.to_string(), tenant.to_string()])?
            .into_iter()
            .next())
    }

    /// Active memories for an exact `(scope, subject)` — used for contradiction detection.
    pub async fn find_active_by_subject(
        &self,
        scope: &MemoryScope,
        subject: &str,
    ) -> crate::Result<Vec<Memory>> {
        let (where_sql, mut params) = scope.where_clause();
        params.push(subject.to_string());
        let sql = format!(
            "SELECT {} FROM memories WHERE {} AND subject = ? AND state = 'active' \
             ORDER BY valid_from DESC",
            Self::SELECT_COLS,
            where_sql
        );
        self.query_memories(&sql, &params)
    }

    /// Active memories in a scope, most important / most recent first.
    pub async fn list_active(
        &self,
        scope: &MemoryScope,
        limit: usize,
    ) -> crate::Result<Vec<Memory>> {
        let (where_sql, params) = scope.where_clause();
        let sql = format!(
            "SELECT {} FROM memories WHERE {} AND state = 'active' \
             ORDER BY importance DESC, valid_from DESC LIMIT {}",
            Self::SELECT_COLS,
            where_sql,
            limit.clamp(1, 10_000)
        );
        self.query_memories(&sql, &params)
    }

    /// Full temporal history for a `(scope, subject)` — every version, oldest first.
    pub async fn history(&self, scope: &MemoryScope, subject: &str) -> crate::Result<Vec<Memory>> {
        let (where_sql, mut params) = scope.where_clause();
        params.push(subject.to_string());
        let sql = format!(
            "SELECT {} FROM memories WHERE {} AND subject = ? ORDER BY valid_from ASC",
            Self::SELECT_COLS,
            where_sql
        );
        self.query_memories(&sql, &params)
    }

    /// The memory that was valid for a `(scope, subject)` at instant `at` (bi-temporal).
    pub async fn as_of(
        &self,
        scope: &MemoryScope,
        subject: &str,
        at: DateTime<Utc>,
    ) -> crate::Result<Option<Memory>> {
        let (where_sql, mut params) = scope.where_clause();
        params.push(subject.to_string());
        params.push(at.to_rfc3339());
        params.push(at.to_rfc3339());
        let sql = format!(
            "SELECT {} FROM memories WHERE {} AND subject = ? \
             AND valid_from <= ?::TIMESTAMPTZ AND (valid_to IS NULL OR valid_to > ?::TIMESTAMPTZ) \
             ORDER BY valid_from DESC LIMIT 1",
            Self::SELECT_COLS,
            where_sql
        );
        Ok(self.query_memories(&sql, &params)?.into_iter().next())
    }

    /// Mark a memory superseded at `valid_to` (contradiction resolution).
    pub async fn supersede(&self, id: Uuid, valid_to: DateTime<Utc>) -> crate::Result<()> {
        let db = self.write_db.lock();
        db.execute(
            "UPDATE memories SET state = 'superseded', valid_to = ?::TIMESTAMPTZ, \
             updated_at = ?::TIMESTAMPTZ WHERE id = ?",
            duckdb::params![
                valid_to.to_rfc3339(),
                Utc::now().to_rfc3339(),
                id.to_string()
            ],
        )
        .map_err(|e| crate::Error::State(format!("supersede memory: {e}")))?;
        Ok(())
    }

    /// Merge new content into an existing memory (semantic-dedup consolidation):
    /// updates content/importance/embedding and bumps the version.
    pub async fn merge_into(
        &self,
        id: Uuid,
        content: &str,
        importance: f32,
        embedding: Option<&[f32]>,
    ) -> crate::Result<()> {
        let db = self.write_db.lock();
        let embedding_json = embedding.map(|e| serde_json::to_string(e).unwrap_or_default());
        db.execute(
            "UPDATE memories SET content = ?, importance = ?, version = version + 1, \
             updated_at = ?::TIMESTAMPTZ, embedding = ?::JSON WHERE id = ?",
            duckdb::params![
                content,
                importance as f64,
                Utc::now().to_rfc3339(),
                embedding_json,
                id.to_string()
            ],
        )
        .map_err(|e| crate::Error::State(format!("merge memory: {e}")))?;
        Ok(())
    }

    /// Reinforce an existing memory (duplicate confirmation): raise importance, bump version.
    pub async fn touch(&self, id: Uuid, importance: f32) -> crate::Result<()> {
        let db = self.write_db.lock();
        db.execute(
            "UPDATE memories SET importance = GREATEST(importance, ?), version = version + 1, \
             updated_at = ?::TIMESTAMPTZ WHERE id = ?",
            duckdb::params![importance as f64, Utc::now().to_rfc3339(), id.to_string()],
        )
        .map_err(|e| crate::Error::State(format!("touch memory: {e}")))?;
        Ok(())
    }

    /// Delete a memory by id.
    pub async fn delete(&self, id: Uuid) -> crate::Result<()> {
        let db = self.write_db.lock();
        db.execute(
            "DELETE FROM memories WHERE id = ?",
            duckdb::params![id.to_string()],
        )
        .map_err(|e| crate::Error::State(format!("delete memory: {e}")))?;
        Ok(())
    }

    /// Delete all memories for a tenant (GDPR erasure). Returns the deleted ids so the caller can
    /// purge their vectors from the index.
    pub async fn delete_by_tenant(&self, tenant: &str) -> crate::Result<Vec<Uuid>> {
        let db = self.write_db.lock();
        let ids: Vec<Uuid> = {
            let mut stmt = db
                .prepare("SELECT id FROM memories WHERE tenant_id = ?")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let rows = stmt
                .query_map([tenant], |r| r.get::<_, String>(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            rows.filter_map(|r| r.ok())
                .filter_map(|s| Uuid::parse_str(&s).ok())
                .collect()
        };
        db.execute(
            "DELETE FROM memories WHERE tenant_id = ?",
            duckdb::params![tenant],
        )
        .map_err(|e| crate::Error::State(format!("delete memories by tenant: {e}")))?;
        Ok(ids)
    }

    /// Delete a memory by id, scoped to a tenant. Returns true iff a row was deleted.
    pub async fn delete_scoped(&self, id: Uuid, tenant: &str) -> crate::Result<bool> {
        let db = self.write_db.lock();
        let n = db
            .execute(
                "DELETE FROM memories WHERE id = ? AND tenant_id = ?",
                duckdb::params![id.to_string(), tenant.to_string()],
            )
            .map_err(|e| crate::Error::State(format!("delete memory: {e}")))?;
        Ok(n > 0)
    }

    /// Total memory count (all states).
    pub async fn count(&self) -> crate::Result<u64> {
        let db = self.read_conn();
        let count: i64 = db
            .query_row("SELECT count(*) FROM memories", [], |row| row.get(0))
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(count as u64)
    }

    /// Forget (expire) active memories whose time-decayed importance has fallen below
    /// `forget_threshold`. Decay is `importance * 0.5^(age_days / half_life_days)`.
    /// Expired memories are retained (state='expired') for history/audit, not hard-deleted.
    /// Returns the ids that were forgotten (so the caller can drop their vectors).
    pub async fn decay(
        &self,
        half_life_days: f32,
        forget_threshold: f32,
    ) -> crate::Result<Vec<Uuid>> {
        let half_life = (half_life_days as f64).max(0.001);
        let threshold = forget_threshold as f64;
        let now = Utc::now().to_rfc3339();
        let db = self.write_db.lock();

        // Same predicate is reused for SELECT (to learn ids) and UPDATE (to expire) with the
        // same `now`, so the matched set is identical.
        let cond = "state = 'active' AND importance * power(0.5, \
             date_diff('day', valid_from, ?::TIMESTAMPTZ)::DOUBLE / ?) < ?";

        let ids: Vec<Uuid> = {
            let sql = format!("SELECT id FROM memories WHERE {cond}");
            let mut stmt = db
                .prepare(&sql)
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let rows = stmt
                .query_map(duckdb::params![now, half_life, threshold], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            rows.filter_map(|r| r.ok())
                .filter_map(|s| Uuid::parse_str(&s).ok())
                .collect()
        };

        if !ids.is_empty() {
            let sql = format!(
                "UPDATE memories SET state='expired', valid_to=?::TIMESTAMPTZ, \
                 updated_at=?::TIMESTAMPTZ WHERE {cond}"
            );
            db.execute(&sql, duckdb::params![now, now, now, half_life, threshold])
                .map_err(|e| crate::Error::State(format!("decay: {e}")))?;
        }
        Ok(ids)
    }

    /// Export the `memories` table to a DuckDB `EXPORT DATABASE` directory (backup/snapshot).
    pub fn export_to(&self, dir: &Path) -> crate::Result<()> {
        let db = self.write_db.lock();
        db.execute_batch(&format!("EXPORT DATABASE '{}'", dir.to_string_lossy()))
            .map_err(|e| crate::Error::Storage(format!("memory export: {e}")))?;
        Ok(())
    }

    /// Atomically restore the `memories` table from an `EXPORT DATABASE` directory (stage then
    /// swap inside a transaction, so a corrupt snapshot never destroys existing memories).
    pub fn restore_from_export(&self, export_dir: &Path, staging_path: &Path) -> crate::Result<()> {
        let export_str = export_dir.to_string_lossy();
        let staging_str = staging_path.to_string_lossy();
        {
            let staging = Connection::open(staging_path)
                .map_err(|e| crate::Error::Storage(format!("open staging db: {e}")))?;
            staging
                .execute_batch(&format!("IMPORT DATABASE '{export_str}'"))
                .map_err(|e| crate::Error::Storage(format!("import memories snapshot: {e}")))?;
        }
        let db = self.write_db.lock();
        let swap = format!(
            "ATTACH '{staging_str}' AS snap (READ_ONLY);
             BEGIN TRANSACTION;
             DELETE FROM memories;
             INSERT INTO memories SELECT * FROM snap.memories;
             COMMIT;"
        );
        if let Err(e) = db.execute_batch(&swap) {
            let _ = db.execute_batch("ROLLBACK");
            let _ = db.execute_batch("DETACH snap");
            return Err(crate::Error::Storage(format!("memory restore swap: {e}")));
        }
        let _ = db.execute_batch("DETACH snap");
        Ok(())
    }

    /// Load all active memories together with their persisted embeddings, so the in-memory
    /// vector index can be rebuilt on startup without re-calling the embedding provider.
    pub async fn load_active_with_embeddings(&self) -> crate::Result<Vec<(Memory, Vec<f32>)>> {
        let db = self.read_conn();
        let sql = format!(
            "SELECT {}, embedding::VARCHAR FROM memories \
             WHERE state = 'active' AND embedding IS NOT NULL",
            Self::SELECT_COLS
        );
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let memory = Self::parse_memory(row)?;
                let emb_str: String = row.get(17)?;
                let embedding: Vec<f32> = serde_json::from_str(&emb_str).unwrap_or_default();
                Ok((memory, embedding))
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(rows
            .filter_map(|r| r.ok())
            .filter(|(_, e)| !e.is_empty())
            .collect())
    }

    /// Load just the persisted embedding for a memory id (None if absent/null). Used to preserve
    /// an existing vector when re-materializing a memory that wasn't re-embedded.
    pub async fn get_embedding(&self, id: Uuid) -> crate::Result<Option<Vec<f32>>> {
        let db = self.read_conn();
        let mut stmt = db
            .prepare("SELECT embedding::VARCHAR FROM memories WHERE id = ?")
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        match stmt.query_row([id.to_string()], |r| r.get::<_, Option<String>>(0)) {
            Ok(Some(s)) => Ok(serde_json::from_str::<Vec<f32>>(&s)
                .ok()
                .filter(|e| !e.is_empty())),
            Ok(None) => Ok(None),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(crate::Error::Query(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> MemoryScope {
        MemoryScope::user("alice")
    }

    #[tokio::test]
    async fn insert_and_get() {
        let store = MemoryStore::new();
        let mem = Memory::new(scope(), "likes coffee");
        store.insert(&mem, None).await.unwrap();
        let got = store.get(mem.id).await.unwrap().unwrap();
        assert_eq!(got.content, "likes coffee");
        assert_eq!(got.state, MemoryState::Active);
        assert_eq!(got.scope.user_id.as_deref(), Some("alice"));
    }

    #[tokio::test]
    async fn scope_isolation_exact_match() {
        let store = MemoryStore::new();
        let mut a = Memory::new(MemoryScope::user("alice"), "fact A");
        a.subject = Some("color".into());
        let mut b = Memory::new(MemoryScope::user("bob"), "fact B");
        b.subject = Some("color".into());
        store.insert(&a, None).await.unwrap();
        store.insert(&b, None).await.unwrap();

        let alice = store
            .find_active_by_subject(&MemoryScope::user("alice"), "color")
            .await
            .unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].content, "fact A");
    }

    #[tokio::test]
    async fn supersede_sets_valid_to_and_history() {
        let store = MemoryStore::new();
        let s = scope();

        let mut old = Memory::new(s.clone(), "favorite color is blue");
        old.subject = Some("favorite_color".into());
        old.valid_from = Utc::now() - chrono::Duration::hours(2);
        store.insert(&old, None).await.unwrap();

        // Contradiction arrives: supersede old, insert new.
        let t1 = Utc::now() - chrono::Duration::hours(1);
        store.supersede(old.id, t1).await.unwrap();
        let mut new = Memory::new(s.clone(), "favorite color is green");
        new.subject = Some("favorite_color".into());
        new.supersedes = Some(old.id);
        new.valid_from = t1;
        store.insert(&new, None).await.unwrap();

        // Only the new one is active.
        let active = store
            .find_active_by_subject(&s, "favorite_color")
            .await
            .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].content, "favorite color is green");

        // History keeps both, oldest first.
        let hist = store.history(&s, "favorite_color").await.unwrap();
        assert_eq!(hist.len(), 2);
        assert_eq!(hist[0].content, "favorite color is blue");
        assert_eq!(hist[0].state, MemoryState::Superseded);
        assert_eq!(hist[1].supersedes, Some(old.id));
    }

    #[tokio::test]
    async fn as_of_returns_value_valid_at_time() {
        let store = MemoryStore::new();
        let s = scope();
        let t0 = Utc::now() - chrono::Duration::hours(3);
        let t1 = Utc::now() - chrono::Duration::hours(1);

        let mut old = Memory::new(s.clone(), "lives in Paris");
        old.subject = Some("city".into());
        old.valid_from = t0;
        store.insert(&old, None).await.unwrap();
        store.supersede(old.id, t1).await.unwrap();

        let mut new = Memory::new(s.clone(), "lives in Lyon");
        new.subject = Some("city".into());
        new.valid_from = t1;
        store.insert(&new, None).await.unwrap();

        // Two hours ago → Paris; now → Lyon.
        let at_past = store
            .as_of(&s, "city", Utc::now() - chrono::Duration::hours(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_past.content, "lives in Paris");

        let at_now = store.as_of(&s, "city", Utc::now()).await.unwrap().unwrap();
        assert_eq!(at_now.content, "lives in Lyon");
    }

    #[tokio::test]
    async fn decay_forgets_old_low_importance() {
        let store = MemoryStore::new();
        let s = scope();

        let mut old = Memory::new(s.clone(), "trivial old fact");
        old.importance = 0.1;
        old.valid_from = Utc::now() - chrono::Duration::days(365);
        store.insert(&old, None).await.unwrap();

        let mut fresh = Memory::new(s.clone(), "important recent fact");
        fresh.importance = 0.9;
        store.insert(&fresh, None).await.unwrap();

        let forgotten = store.decay(30.0, 0.05).await.unwrap();
        assert_eq!(forgotten, vec![old.id]);

        // Only the fresh, important memory stays active; the old one is retained as history.
        let active = store.list_active(&s, 10).await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, fresh.id);
        assert_eq!(
            store.get(old.id).await.unwrap().unwrap().state,
            MemoryState::Expired
        );
    }

    #[tokio::test]
    async fn embeddings_roundtrip_for_rebuild() {
        let store = MemoryStore::new();
        let mem = Memory::new(scope(), "vectorized fact");
        store.insert(&mem, Some(&[0.1, 0.2, 0.3])).await.unwrap();
        let loaded = store.load_active_with_embeddings().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn tokenize_splits_on_nonalnum() {
        assert_eq!(tokenize("Hello, World! 42"), vec!["hello", "world", "42"]);
    }

    #[test]
    fn lexical_rank_orders_by_overlap() {
        let s = scope();
        let cands = vec![
            Memory::new(s.clone(), "alice loves hiking in the mountains"),
            Memory::new(s.clone(), "alice works as a software engineer"),
            Memory::new(s.clone(), "the weather is sunny today"),
        ];
        let ranked = lexical_rank("software engineering job", &cands);
        assert!(!ranked.is_empty());
        // The "software engineer" doc (index 1) should rank first.
        assert_eq!(ranked[0].0, 1);
    }

    #[test]
    fn lexical_rank_empty_query_is_empty() {
        let cands = vec![Memory::new(scope(), "anything")];
        assert!(lexical_rank("   ", &cands).is_empty());
    }

    #[test]
    fn rrf_fuse_rewards_agreement() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        // `a` is top of both rankings → highest fused score.
        let fused = rrf_fuse(&[vec![a, b], vec![a, c]], 60.0, 3);
        assert_eq!(fused[0].0, a);
        assert_eq!(fused.len(), 3);
    }

    #[tokio::test]
    async fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memories.duckdb");
        let mem = Memory::new(scope(), "durable fact");
        {
            let store = MemoryStore::open(&path, 4).unwrap();
            store.insert(&mem, None).await.unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
        }
        {
            let store = MemoryStore::open(&path, 4).unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
            assert_eq!(
                store.get(mem.id).await.unwrap().unwrap().content,
                "durable fact"
            );
        }
    }
}
