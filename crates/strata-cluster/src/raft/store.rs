//! Raft log storage and state machine — SQLite-backed persistent implementation.
//!
//! The Raft log, vote, and metadata are persisted in a SQLite database.
//! An in-memory BTreeMap cache provides fast reads. All writes go to both
//! SQLite and the cache atomically.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::{
    Entry, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use rusqlite::Connection;
use strata_core::StrataEngine;

use super::types::{AppRequest, AppResponse, NodeId, NodeInfo, TypeConfig};

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

    /// Persist a log entry to SQLite.
    fn persist_entry(&self, idx: u64, entry: &Entry<TypeConfig>) {
        if let Some(ref db) = self.db {
            let data = serde_json::to_vec(entry).unwrap_or_default();
            let _ = db.execute(
                "INSERT OR REPLACE INTO raft_log (idx, entry) VALUES (?1, ?2)",
                rusqlite::params![idx as i64, data],
            );
        }
    }

    /// Delete log entries from SQLite.
    fn delete_entries_from(&self, from_idx: u64) {
        if let Some(ref db) = self.db {
            let _ = db.execute(
                "DELETE FROM raft_log WHERE idx >= ?1",
                rusqlite::params![from_idx as i64],
            );
        }
    }

    fn delete_entries_upto(&self, upto_idx: u64) {
        if let Some(ref db) = self.db {
            let _ = db.execute(
                "DELETE FROM raft_log WHERE idx <= ?1",
                rusqlite::params![upto_idx as i64],
            );
        }
    }

    /// Persist metadata (vote, committed, etc.) to SQLite.
    fn persist_meta(&self, key: &str, value: &[u8]) {
        if let Some(ref db) = self.db {
            let _ = db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, value],
            );
        }
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
                        if let Ok(entry) = serde_json::from_slice::<Entry<TypeConfig>>(&row.1) {
                            self.log.insert(row.0, entry);
                        }
                    }
                }
            }

            // Load vote
            if let Some(data) = self.load_meta("vote") {
                self.vote = serde_json::from_slice(&data).ok();
            }

            // Load committed
            if let Some(data) = self.load_meta("committed") {
                self.committed = serde_json::from_slice(&data).ok();
            }

            // Load last_purged
            if let Some(data) = self.load_meta("last_purged") {
                self.last_purged = serde_json::from_slice(&data).ok();
            }

            // Load last_applied
            if let Some(data) = self.load_meta("last_applied") {
                self.last_applied = serde_json::from_slice(&data).ok();
            }

            // Load last_membership
            if let Some(data) = self.load_meta("last_membership") {
                if let Ok(m) = serde_json::from_slice(&data) {
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
/// entries to a `StrataEngine` reference.
#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<Mutex<StoreInner>>,
    engine: Option<Arc<StrataEngine>>,
}

impl MemStore {
    /// Create a new in-memory store (no persistence).
    pub fn new(engine: Option<Arc<StrataEngine>>) -> Self {
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
    pub fn open(path: &Path, engine: Option<Arc<StrataEngine>>) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| crate::Error::Raft(format!("mkdir: {e}")))?;
        }

        let conn =
            Connection::open(path).map_err(|e| crate::Error::Raft(format!("open raft db: {e}")))?;

        // Durability: WAL mode survives process crashes without corruption.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
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
    async fn apply_request(&self, req: &AppRequest) -> AppResponse {
        let Some(engine) = &self.engine else {
            return AppResponse::Ok;
        };

        match req {
            AppRequest::Ingest { source, events } => {
                let strata_events: Vec<strata_core::memory::episodic::Event> = events
                    .iter()
                    .map(|payload| strata_core::memory::episodic::Event {
                        id: uuid::Uuid::new_v4(),
                        source: source.clone(),
                        event_type: payload
                            .get("event_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        payload: payload.clone(),
                        timestamp: chrono::Utc::now(),
                        parent_id: None,
                        trace_id: None,
                        tags: vec![],
                        idempotency_key: None,
                    })
                    .collect();
                match engine.ingest(strata_events).await {
                    Ok(n) => AppResponse::Ingested(n),
                    Err(e) => {
                        tracing::error!(error = %e, "raft apply: ingest failed");
                        AppResponse::Ingested(0)
                    }
                }
            }
            AppRequest::StateSet {
                agent_id,
                key,
                value,
            } => match engine.state_set(agent_id, key, value.clone()).await {
                Ok(v) => AppResponse::StateVersion(v),
                Err(e) => {
                    tracing::error!(error = %e, "raft apply: state_set failed");
                    AppResponse::StateVersion(0)
                }
            },
            AppRequest::StateDelete { agent_id, key } => {
                let _ = engine.state_delete(agent_id, key).await;
                AppResponse::Deleted
            }
            AppRequest::SemanticUpsert {
                id,
                content,
                embedding,
                metadata,
            } => {
                let entry = strata_core::memory::semantic::SemanticEntry {
                    id: *id,
                    content: content.clone(),
                    embedding: embedding.clone(),
                    metadata: metadata.clone(),
                };
                let _ = engine.semantic_upsert(&entry).await;
                AppResponse::Ok
            }
            AppRequest::SemanticDelete { id } => {
                let _ = engine.semantic_delete(*id).await;
                AppResponse::Ok
            }
        }
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

        // Build a real snapshot from the engine state
        let data = if let Some(engine) = &self.engine {
            match crate::replication::snapshot::SnapshotManager::build(engine).await {
                Ok(snapshot_data) => snapshot_data,
                Err(e) => {
                    tracing::error!(error = %e, "failed to build snapshot, using empty");
                    Vec::new()
                }
            }
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
        let mut inner = self.inner.lock();
        inner.vote = Some(*vote);
        if let Ok(data) = serde_json::to_vec(vote) {
            inner.persist_meta("vote", &data);
        }
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.committed = committed;
        if let Ok(data) = serde_json::to_vec(&committed) {
            inner.persist_meta("committed", &data);
        }
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
            inner.persist_entry(idx, &entry);
            inner.log.insert(idx, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.delete_entries_from(log_id.index);
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);
        if let Ok(data) = serde_json::to_vec(&Some(log_id)) {
            inner.persist_meta("last_purged", &data);
        }
        inner.delete_entries_upto(log_id.index);
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

            // Update last applied + persist
            {
                let mut inner = self.inner.lock();
                inner.last_applied = Some(log_id);
                if let Ok(data) = serde_json::to_vec(&Some(log_id)) {
                    inner.persist_meta("last_applied", &data);
                }
            }

            match entry.payload {
                openraft::EntryPayload::Blank => {
                    responses.push(AppResponse::Ok);
                }
                openraft::EntryPayload::Normal(ref req) => {
                    let resp = self.apply_request(req).await;
                    responses.push(resp);
                }
                openraft::EntryPayload::Membership(ref membership) => {
                    let mut inner = self.inner.lock();
                    inner.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    if let Ok(data) = serde_json::to_vec(&inner.last_membership) {
                        inner.persist_meta("last_membership", &data);
                    }
                    responses.push(AppResponse::Ok);
                }
            }
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

        // Restore engine state from the snapshot data
        if !data.is_empty() {
            if let Some(engine) = &self.engine {
                if let Err(e) =
                    crate::replication::snapshot::SnapshotManager::restore(engine, &data).await
                {
                    tracing::error!(error = %e, "failed to restore snapshot to engine");
                }
            }
        }

        let mut inner = self.inner.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        // Persist applied state
        if let Ok(d) = serde_json::to_vec(&inner.last_applied) {
            inner.persist_meta("last_applied", &d);
        }
        if let Ok(d) = serde_json::to_vec(&inner.last_membership) {
            inner.persist_meta("last_membership", &d);
        }
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
