//! Raft log storage and state machine — SQLite-backed persistent implementation.
//!
//! The Raft log, vote, and metadata are persisted in a SQLite database.
//! An in-memory BTreeMap cache provides fast reads. All writes go to both
//! SQLite and the cache atomically.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use ecphoria_core::EcphoriaEngine;
use openraft::storage::LogState;
use openraft::{
    AnyError, Entry, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use rusqlite::Connection;

use super::types::{AppRequest, AppResponse, NodeId, NodeInfo, TypeConfig};

/// Convert a storage-write failure (SQLite or serialization) into an openraft `StorageError`.
///
/// Returning this from a `RaftStorage` write method makes openraft treat the write as NOT durable
/// and shut the node down, rather than proceeding as if the vote/log/state were persisted. That is
/// exactly what prevents a forgotten vote (→ double-vote → split-brain) or a lost committed entry:
/// we must never report `Ok(())` for a write that did not reach stable storage.
fn sm_write_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageIOError::<NodeId>::write(AnyError::new(e)).into()
}

/// Shared state for the Raft store (cache + persistent SQLite).
#[derive(Debug)]
struct StoreInner {
    /// SQLite connection for persistence (None = in-memory only).
    db: Option<Connection>,
    /// In-memory cache of log entries.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Current vote.
    vote: Option<Vote<NodeId>>,
    /// Last purged log ID.
    last_purged: Option<LogId<NodeId>>,
    /// Last applied log ID.
    last_applied: Option<LogId<NodeId>>,
    /// Last applied membership.
    last_membership: StoredMembership<NodeId, NodeInfo>,
    /// Current snapshot.
    snapshot: Option<StoredSnapshot>,
    /// Committed log id.
    committed: Option<LogId<NodeId>>,
}

// The persistence helpers return openraft's `StorageError`, which is large by design (it carries
// subject/verb/source for a fatal data-crash report). Boxing it here would just force an unbox at
// the `RaftStorage` trait boundary, so allow the large-Err variant on these internal helpers.
#[allow(clippy::result_large_err)]
impl StoreInner {
    fn init_schema(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS raft_log (
                idx     INTEGER PRIMARY KEY,
                entry   BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raft_meta (
                key   TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );",
        )
        .expect("failed to create raft schema");
    }

    /// Persist a log entry to SQLite. Errors propagate so openraft never believes an entry is
    /// durable when the write (or its serialization) actually failed.
    fn persist_entry(
        &self,
        idx: u64,
        entry: &Entry<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(ref db) = self.db {
            let data = rmp_serde::to_vec(entry).map_err(|e| sm_write_err(&e))?;
            db.execute(
                "INSERT OR REPLACE INTO raft_log (idx, entry) VALUES (?1, ?2)",
                rusqlite::params![idx as i64, data],
            )
            .map_err(|e| sm_write_err(&e))?;
        }
        Ok(())
    }

    /// Delete log entries from SQLite (index >= from_idx).
    fn delete_entries_from(&self, from_idx: u64) -> Result<(), StorageError<NodeId>> {
        if let Some(ref db) = self.db {
            db.execute(
                "DELETE FROM raft_log WHERE idx >= ?1",
                rusqlite::params![from_idx as i64],
            )
            .map_err(|e| sm_write_err(&e))?;
        }
        Ok(())
    }

    fn delete_entries_upto(&self, upto_idx: u64) -> Result<(), StorageError<NodeId>> {
        if let Some(ref db) = self.db {
            db.execute(
                "DELETE FROM raft_log WHERE idx <= ?1",
                rusqlite::params![upto_idx as i64],
            )
            .map_err(|e| sm_write_err(&e))?;
        }
        Ok(())
    }

    /// Persist metadata (vote, committed, last_applied, …) to SQLite. Errors propagate.
    fn persist_meta(&self, key: &str, value: &[u8]) -> Result<(), StorageError<NodeId>> {
        if let Some(ref db) = self.db {
            db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, value],
            )
            .map_err(|e| sm_write_err(&e))?;
        }
        Ok(())
    }

    /// Load metadata from SQLite.
    fn load_meta(&self, key: &str) -> Option<Vec<u8>> {
        self.db.as_ref().and_then(|db| {
            db.query_row(
                "SELECT value FROM raft_meta WHERE key = ?1",
                rusqlite::params![key],
                |row| row.get(0),
            )
            .ok()
        })
    }

    /// Hydrate the in-memory cache from SQLite.
    fn hydrate(&mut self) {
        if let Some(ref db) = self.db {
            // Load log entries
            if let Ok(mut stmt) = db.prepare("SELECT idx, entry FROM raft_log ORDER BY idx") {
                if let Ok(rows) = stmt.query_map([], |row| {
                    let idx: i64 = row.get(0)?;
                    let data: Vec<u8> = row.get(1)?;
                    Ok((idx as u64, data))
                }) {
                    for row in rows.flatten() {
                        if let Ok(entry) = rmp_serde::from_slice::<Entry<TypeConfig>>(&row.1) {
                            self.log.insert(row.0, entry);
                        }
                    }
                }
            }

            // Load vote
            if let Some(data) = self.load_meta("vote") {
                self.vote = rmp_serde::from_slice(&data).ok();
            }

            // Load committed
            if let Some(data) = self.load_meta("committed") {
                self.committed = rmp_serde::from_slice(&data).ok();
            }

            // Load last_purged
            if let Some(data) = self.load_meta("last_purged") {
                self.last_purged = rmp_serde::from_slice(&data).ok();
            }

            // Load last_applied
            if let Some(data) = self.load_meta("last_applied") {
                self.last_applied = rmp_serde::from_slice(&data).ok();
            }

            // Load last_membership
            if let Some(data) = self.load_meta("last_membership") {
                if let Ok(m) = rmp_serde::from_slice(&data) {
                    self.last_membership = m;
                }
            }

            if !self.log.is_empty() {
                tracing::info!(
                    entries = self.log.len(),
                    "hydrated Raft log from persistent storage"
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, NodeInfo>,
    data: Vec<u8>,
}

/// Persistent Raft store backed by SQLite + in-memory cache.
///
/// Holds both the Raft log and state machine. The state machine applies
/// entries to a `EcphoriaEngine` reference.
#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<Mutex<StoreInner>>,
    engine: Option<Arc<EcphoriaEngine>>,
}

impl MemStore {
    /// Create a new in-memory store (no persistence).
    pub fn new(engine: Option<Arc<EcphoriaEngine>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                db: None,
                log: BTreeMap::new(),
                vote: None,
                last_purged: None,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot: None,
                committed: None,
            })),
            engine,
        }
    }

    /// Create a persistent store backed by a SQLite file.
    ///
    /// On startup, the log and metadata are hydrated from the database.
    pub fn open(path: &Path, engine: Option<Arc<EcphoriaEngine>>) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| crate::Error::Raft(format!("mkdir: {e}")))?;
        }

        let conn =
            Connection::open(path).map_err(|e| crate::Error::Raft(format!("open raft db: {e}")))?;

        // Durability: WAL + `synchronous=FULL`. FULL (not NORMAL) is required for a *consensus*
        // log — NORMAL does not fsync the WAL on commit, so a committed Raft entry or a granted
        // vote can roll back on an OS crash / power loss, violating Raft's durability guarantee
        // (lost committed write, or a forgotten vote → double-vote → split-brain). The extra fsync
        // per commit is the price of that guarantee.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|e| crate::Error::Raft(format!("set pragmas: {e}")))?;

        StoreInner::init_schema(&conn);

        let mut inner = StoreInner {
            db: Some(conn),
            log: BTreeMap::new(),
            vote: None,
            last_purged: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot: None,
            committed: None,
        };

        inner.hydrate();

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            engine,
        })
    }

    /// Apply an application request to the engine.
    /// Apply one committed request to the engine. Errors PROPAGATE (they are no longer swallowed):
    /// a failed apply must not be marked applied, or the entry would be silently dropped on this
    /// node and diverge from its peers. The caller turns an error into a `StorageError` so openraft
    /// halts the node; the entry is re-applied on restart (apply is idempotent for these variants).
    async fn apply_request(&self, req: &AppRequest) -> Result<AppResponse, ecphoria_core::Error> {
        let Some(engine) = &self.engine else {
            return Ok(AppResponse::Ok);
        };

        let resp = match req {
            // Events are already fully formed (ids + timestamps fixed by the leader), so apply
            // is deterministic across nodes.
            AppRequest::Ingest { events, tenant } => {
                // Episodic append ONLY — deterministic. Vectors are indexed locally + best-effort by
                // the background reindex loop, so apply makes no external, non-deterministic
                // embedding call (which would diverge the index across nodes and stall apply).
                let n = engine
                    .ingest_replicated(events.clone(), tenant.as_deref())
                    .await?;
                AppResponse::Ingested(n)
            }
            AppRequest::StateSet {
                agent_id,
                key,
                value,
                tenant,
            } => {
                let v = match tenant {
                    Some(t) => {
                        engine
                            .state_set_for_tenant(t, agent_id, key, value.clone())
                            .await?
                    }
                    None => engine.state_set(agent_id, key, value.clone()).await?,
                };
                AppResponse::StateVersion(v)
            }
            AppRequest::StateDelete {
                agent_id,
                key,
                tenant,
            } => {
                match tenant {
                    Some(t) => engine.state_delete_for_tenant(t, agent_id, key).await?,
                    None => engine.state_delete(agent_id, key).await?,
                };
                AppResponse::Deleted
            }
            AppRequest::SemanticUpsert {
                id,
                content,
                embedding,
                metadata,
            } => {
                let entry = ecphoria_core::memory::semantic::SemanticEntry {
                    id: *id,
                    content: content.clone(),
                    embedding: embedding.clone(),
                    metadata: metadata.clone(),
                };
                engine.semantic_upsert(&entry).await?;
                AppResponse::Ok
            }
            AppRequest::SemanticDelete { id } => {
                engine.semantic_delete(*id).await?;
                AppResponse::Ok
            }
            // Materialized memory rows (leader already ran cognition) → deterministic replay.
            AppRequest::MemoryUpsert { rows } => {
                let n = engine.memory_apply_rows(rows.clone()).await?;
                AppResponse::MemoryCount(n)
            }
            AppRequest::MemoryDelete { id } => {
                engine.memory_delete(*id).await?;
                AppResponse::MemoryCount(1)
            }
            AppRequest::GraphAddEdge { tenant, edge } => {
                engine.graph_apply_edge(tenant.as_deref(), edge).await?;
                AppResponse::Ok
            }
            AppRequest::GraphSupersede {
                tenant,
                src,
                relation,
                at,
                by,
            } => {
                engine
                    .graph_supersede_apply(tenant.as_deref(), src, relation, *at, *by)
                    .await?;
                AppResponse::Ok
            }
            AppRequest::MemoryExpire { ids } => {
                engine.memory_expire(ids).await?;
                AppResponse::MemoryCount(ids.len() as u64)
            }
            AppRequest::RunCreate { run } => {
                engine.run_apply_create(run).await?;
                AppResponse::Ok
            }
            AppRequest::RunUpdate {
                id,
                patch,
                updated_at,
            } => {
                engine.run_apply_update(*id, patch, *updated_at).await?;
                AppResponse::Ok
            }
        };
        Ok(resp)
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new(None)
    }
}

// ── RaftLogReader ──────────────────────────────────────────────────

impl RaftLogReader<TypeConfig> for MemStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let entries: Vec<_> = inner.log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

// ── RaftSnapshotBuilder ────────────────────────────────────────────

impl RaftSnapshotBuilder<TypeConfig> for MemStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, membership) = {
            let inner = self.inner.lock();
            (inner.last_applied, inner.last_membership.clone())
        };

        // Build a real snapshot from the engine state. If it FAILS, return an error rather than
        // emitting an empty blob with a real `last_log_id` — a follower installing that would
        // fast-forward `last_applied` over state it never received (silent divergence).
        let data = if let Some(engine) = &self.engine {
            crate::replication::snapshot::SnapshotManager::build(engine)
                .await
                .map_err(|e| StorageIOError::<NodeId>::write_snapshot(None, AnyError::new(&e)))?
        } else {
            Vec::new()
        };

        let snapshot_id = format!(
            "{}-{}",
            last_applied
                .map(|id| format!("{}-{}", id.leader_id, id.index))
                .unwrap_or_default(),
            uuid::Uuid::new_v4()
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // Store snapshot locally
        {
            let mut inner = self.inner.lock();
            inner.snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ── RaftStorage (v1 unified trait) ─────────────────────────────────

impl openraft::RaftStorage<TypeConfig> for MemStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Persist to stable storage FIRST; only then update the in-memory cache. If the durable
        // write fails we return the error (openraft shuts the node down) instead of pretending the
        // vote was granted — the guard against a forgotten vote → double-vote → split-brain.
        let data = rmp_serde::to_vec(vote).map_err(|e| sm_write_err(&e))?;
        let mut inner = self.inner.lock();
        inner.persist_meta("vote", &data)?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = rmp_serde::to_vec(&committed).map_err(|e| sm_write_err(&e))?;
        let mut inner = self.inner.lock();
        inner.persist_meta("committed", &data)?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let last_purged = inner.last_purged;
        let last = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = self.inner.lock();
        for entry in entries {
            let idx = entry.log_id.index;
            // Persist to disk before updating the in-memory cache.
            inner.persist_entry(idx, &entry)?;
            inner.log.insert(idx, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.delete_entries_from(log_id.index)?;
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let data = rmp_serde::to_vec(&Some(log_id)).map_err(|e| sm_write_err(&e))?;
        let mut inner = self.inner.lock();
        inner.persist_meta("last_purged", &data)?;
        inner.last_purged = Some(log_id);
        inner.delete_entries_upto(log_id.index)?;
        let to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeInfo>), StorageError<NodeId>>
    {
        let inner = self.inner.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<AppResponse>, StorageError<NodeId>> {
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            let log_id = entry.log_id;

            // Apply the entry's effect BEFORE persisting `last_applied`, so a crash between the two
            // re-applies the entry on restart (safe — apply is idempotent for the common variants)
            // rather than marking it applied while its data write was lost.
            // NOTE: full atomicity of the engine data write with `last_applied` (an idempotent
            // apply, or storing `last_applied` inside the engine transaction) is a separate
            // follow-up; this ordering only removes the "mark-then-lose" window.
            let resp = match entry.payload {
                openraft::EntryPayload::Blank => AppResponse::Ok,
                // A committed entry that fails to apply must NOT be marked applied (that would
                // silently drop it and diverge from peers). Surface a StorageError so openraft halts
                // this node; on restart the entry is re-applied (apply is idempotent for the
                // replicated variants).
                openraft::EntryPayload::Normal(ref req) => match self.apply_request(req).await {
                    Ok(resp) => resp,
                    Err(e) => return Err(StorageIOError::apply(log_id, AnyError::new(&e)).into()),
                },
                openraft::EntryPayload::Membership(ref membership) => {
                    let mut inner = self.inner.lock();
                    inner.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    let data =
                        rmp_serde::to_vec(&inner.last_membership).map_err(|e| sm_write_err(&e))?;
                    inner.persist_meta("last_membership", &data)?;
                    AppResponse::Ok
                }
            };

            {
                let data = rmp_serde::to_vec(&Some(log_id)).map_err(|e| sm_write_err(&e))?;
                let mut inner = self.inner.lock();
                inner.persist_meta("last_applied", &data)?;
                inner.last_applied = Some(log_id);
            }

            responses.push(resp);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeInfo>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();

        // A snapshot that claims a real `last_log_id` but carries no data is broken: installing it
        // would fast-forward `last_applied` over state we never received. Refuse it.
        if data.is_empty() && meta.last_log_id.is_some() {
            return Err(StorageIOError::<NodeId>::read_snapshot(
                None,
                AnyError::error("empty snapshot with a non-null last_log_id — refusing to install"),
            )
            .into());
        }

        // Restore engine state from the snapshot data. If the restore FAILS, do NOT advance
        // `last_applied` (that would silently diverge) — surface the error so openraft retries.
        if !data.is_empty() {
            if let Some(engine) = &self.engine {
                crate::replication::snapshot::SnapshotManager::restore(engine, &data)
                    .await
                    .map_err(|e| {
                        StorageIOError::<NodeId>::read_snapshot(None, AnyError::new(&e))
                    })?;
            }
        }

        let mut inner = self.inner.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        // Persist applied state.
        let d = rmp_serde::to_vec(&inner.last_applied).map_err(|e| sm_write_err(&e))?;
        inner.persist_meta("last_applied", &d)?;
        let d = rmp_serde::to_vec(&inner.last_membership).map_err(|e| sm_write_err(&e))?;
        inner.persist_meta("last_membership", &d)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stores() {
        let store = MemStore::new(None);
        assert!(store.inner.lock().log.is_empty());
    }

    async fn inmem_engine() -> Arc<EcphoriaEngine> {
        let mut c = ecphoria_core::CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        Arc::new(EcphoriaEngine::new(c).await.unwrap())
    }

    #[tokio::test]
    async fn apply_ingest_is_deterministic_across_nodes() {
        // The SAME committed log entry...
        let ev =
            ecphoria_core::memory::episodic::Event::new("src", "e", serde_json::json!({"x": 1}));
        let req = AppRequest::Ingest {
            events: vec![ev.clone()],
            tenant: None,
        };

        // ...applied on two independent nodes...
        let (n1, n2) = (inmem_engine().await, inmem_engine().await);
        MemStore::new(Some(n1.clone()))
            .apply_request(&req)
            .await
            .unwrap();
        MemStore::new(Some(n2.clone()))
            .apply_request(&req)
            .await
            .unwrap();

        // ...yields identical state (same event id) — the determinism property Raft requires.
        let r1 = n1.query_sql("SELECT id FROM episodic").await.unwrap();
        let r2 = n2.query_sql("SELECT id FROM episodic").await.unwrap();
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0]["id"], r2[0]["id"]);
        assert_eq!(r1[0]["id"].as_str().unwrap(), ev.id.to_string());
    }

    #[tokio::test]
    async fn apply_memory_upsert_replicates_to_engine() {
        let engine = inmem_engine().await;
        let store = MemStore::new(Some(engine.clone()));
        let mem = ecphoria_core::memory::cognition::Memory::new(
            ecphoria_core::memory::cognition::MemoryScope::user("alice"),
            "likes tea",
        );
        let resp = store
            .apply_request(&AppRequest::MemoryUpsert {
                rows: vec![ecphoria_core::memory::cognition::MemoryRow {
                    memory: mem,
                    embedding: None,
                }],
            })
            .await
            .unwrap();
        assert!(matches!(resp, AppResponse::MemoryCount(1)));
        assert_eq!(engine.memory_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn apply_memory_expire_replicates_to_engine() {
        let engine = inmem_engine().await;
        let store = MemStore::new(Some(engine.clone()));
        let scope = ecphoria_core::memory::cognition::MemoryScope::user("alice");
        let added = engine
            .memory_add(ecphoria_core::memory::cognition::MemoryInput::new(
                scope.clone(),
                "a fact",
            ))
            .await
            .unwrap();
        assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 1);
        let resp = store
            .apply_request(&AppRequest::MemoryExpire {
                ids: vec![added.memory.id],
            })
            .await
            .unwrap();
        assert!(matches!(resp, AppResponse::MemoryCount(1)));
        // The expired memory is no longer active.
        assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn apply_graph_edge_replicates_to_engine() {
        let engine = inmem_engine().await;
        let store = MemStore::new(Some(engine.clone()));
        let edge = ecphoria_core::memory::cognition::Edge {
            id: uuid::Uuid::new_v4(),
            src: "Alice".into(),
            relation: "likes".into(),
            dst: "coffee".into(),
            weight: 1.0,
            source_memory_id: None,
            ..Default::default()
        };
        let resp = store
            .apply_request(&AppRequest::GraphAddEdge {
                tenant: None,
                edge: edge.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, AppResponse::Ok));
        let n = engine
            .memory_neighbors("default", "Alice", 10)
            .await
            .unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(
            n[0].id, edge.id,
            "edge id replicated verbatim (deterministic)"
        );
    }

    #[tokio::test]
    async fn apply_graph_supersede_is_deterministic_across_engines() {
        // The same seed edge + the same GraphSupersede applied to two engines must yield byte-
        // identical state (no now()/uuid at apply time) — the replication-safety contract.
        let seed = ecphoria_core::memory::cognition::Edge {
            id: uuid::Uuid::new_v4(),
            src: "Alice".into(),
            relation: "lives_in".into(),
            dst: "Berlin".into(),
            weight: 1.0,
            valid_from: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
            ..Default::default()
        };
        let at = chrono::Utc::now();
        let by = uuid::Uuid::new_v4();

        let mut states = Vec::new();
        for _ in 0..2 {
            let engine = inmem_engine().await;
            let store = MemStore::new(Some(engine.clone()));
            store
                .apply_request(&AppRequest::GraphAddEdge {
                    tenant: None,
                    edge: seed.clone(),
                })
                .await
                .unwrap();
            let resp = store
                .apply_request(&AppRequest::GraphSupersede {
                    tenant: None,
                    src: "Alice".into(),
                    relation: "lives_in".into(),
                    at,
                    by: Some(by),
                })
                .await
                .unwrap();
            assert!(matches!(resp, AppResponse::Ok));
            states.push(
                engine
                    .memory_neighbors("default", "Alice", 10)
                    .await
                    .unwrap(),
            );
        }

        for edges in &states {
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].id, seed.id);
            assert_eq!(
                edges[0].state,
                ecphoria_core::memory::cognition::EdgeState::Superseded
            );
            assert_eq!(edges[0].invalidated_by, Some(by));
        }
        assert_eq!(
            states[0][0].valid_to, states[1][0].valid_to,
            "valid_to identical across replicas"
        );
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = MemStore::new(None);
        let vote = Vote::new(1, 1);

        openraft::RaftStorage::<TypeConfig>::save_vote(&mut store, &vote)
            .await
            .unwrap();

        let read = openraft::RaftStorage::<TypeConfig>::read_vote(&mut store)
            .await
            .unwrap();
        assert!(read.is_some());
    }

    #[tokio::test]
    async fn get_log_state_empty() {
        let mut store = MemStore::new(None);
        let state = openraft::RaftStorage::<TypeConfig>::get_log_state(&mut store)
            .await
            .unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn persistent_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("raft.db");

        // Write vote
        {
            let mut store = MemStore::open(&db_path, None).unwrap();
            let vote = Vote::new(2, 3);
            openraft::RaftStorage::<TypeConfig>::save_vote(&mut store, &vote)
                .await
                .unwrap();
        }

        // Reopen and verify
        {
            let mut store = MemStore::open(&db_path, None).unwrap();
            let vote = openraft::RaftStorage::<TypeConfig>::read_vote(&mut store)
                .await
                .unwrap();
            assert!(vote.is_some());
            let v = vote.unwrap();
            assert_eq!(v.leader_id().voted_for().unwrap(), 3);
        }
    }
}
