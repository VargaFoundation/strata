//! Episodic memory store — append-only event storage backed by DuckDB.

use chrono::{DateTime, Utc};
use duckdb::Connection;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// A single episodic event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    /// Optional parent event for causal chains.
    // NB: no `skip_serializing_if` — the Raft transport encodes structs positionally (MessagePack
    // compact), so omitting a None field shifts the array and misaligns the decoder. Keep constant
    // arity. See the round-trip regression test in strata-cluster raft::types.
    #[serde(default)]
    pub parent_id: Option<Uuid>,
    /// Optional trace ID for grouping related events across agents.
    #[serde(default)]
    pub trace_id: Option<String>,
    /// Tags for structured filtering.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional idempotency key for deduplication.
    /// If set, duplicate inserts with the same key are silently skipped.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

impl Event {
    /// Create a new event with only the required fields.
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            source: source.into(),
            event_type: event_type.into(),
            payload,
            timestamp: Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        }
    }
}

/// Append-only event store backed by DuckDB.
///
/// Uses separate write and read connections for concurrency:
/// - File-backed: 1 write connection + N read connections (round-robin pool)
/// - In-memory: single shared connection (DuckDB limitation)
pub struct EpisodicStore {
    /// Connection used for writes (INSERT, DDL). Also used for reads in in-memory mode.
    write_db: Arc<Mutex<Connection>>,
    /// Pool of read-only connections (file-backed mode only).
    read_pool: Vec<Mutex<Connection>>,
    /// Round-robin counter for read pool.
    read_next: AtomicUsize,
}

impl std::fmt::Debug for EpisodicStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpisodicStore")
            .field("read_pool_size", &self.read_pool.len())
            .finish()
    }
}

impl EpisodicStore {
    /// Create an in-memory episodic store.
    pub fn new() -> Self {
        Self::open(Path::new(":memory:"), Self::DEFAULT_READ_POOL_SIZE)
            .expect("failed to create in-memory episodic store")
    }

    /// Number of reader connections in the pool.
    const DEFAULT_READ_POOL_SIZE: usize = 4;

    /// Open or create an episodic store at the given path.
    ///
    /// Use `:memory:` for an in-memory store (testing) or a file path for persistence.
    /// For file-backed stores, a pool of read connections is created for concurrent queries.
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

        // Create read connection pool via try_clone (shares the same underlying database)
        let mut read_pool = Vec::with_capacity(read_pool_size);
        for _ in 0..read_pool_size {
            let conn = write_conn
                .try_clone()
                .map_err(|e| crate::Error::Storage(format!("failed to clone read conn: {e}")))?;
            read_pool.push(Mutex::new(conn));
        }

        tracing::info!(
            pool_size = read_pool.len(),
            "episodic read connection pool created"
        );

        Ok(Self {
            write_db: Arc::new(Mutex::new(write_conn)),
            read_pool,
            read_next: AtomicUsize::new(0),
        })
    }

    /// Acquire the write connection (for DDL, backup, retention operations).
    pub fn write_conn(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.write_db.lock()
    }

    /// Export all events for a tenant (for moving a tenant between shards). The returned `Event`s
    /// carry their original ids/timestamps; re-ingesting them on the destination preserves both.
    pub async fn events_by_tenant(&self, tenant: &str, limit: usize) -> crate::Result<Vec<Event>> {
        let db = self.read_conn();
        let sql = format!(
            "SELECT {} FROM episodic WHERE tenant_id = ? ORDER BY ts ASC LIMIT {}",
            Self::SELECT_COLS,
            limit.clamp(1, 10_000_000)
        );
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map(duckdb::params![tenant], Self::parse_event)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Delete all events and sessions for a tenant (GDPR erasure). Returns events deleted.
    pub async fn delete_by_tenant(&self, tenant: &str) -> crate::Result<u64> {
        let db = self.write_db.lock();
        let n = db
            .execute(
                "DELETE FROM episodic WHERE tenant_id = ?",
                duckdb::params![tenant],
            )
            .map_err(|e| crate::Error::Query(format!("delete episodic by tenant: {e}")))?;
        let _ = db.execute(
            "DELETE FROM sessions WHERE tenant_id = ?",
            duckdb::params![tenant],
        );
        Ok(n as u64)
    }

    /// Acquire a connection for reading from the round-robin pool.
    fn read_conn(&self) -> parking_lot::MutexGuard<'_, Connection> {
        let idx = self.read_next.fetch_add(1, Ordering::Relaxed) % self.read_pool.len();
        self.read_pool[idx].lock()
    }

    fn init_schema(conn: &Connection) -> crate::Result<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS episodic (
                id              VARCHAR PRIMARY KEY,
                source          VARCHAR NOT NULL,
                event_type      VARCHAR NOT NULL,
                payload         JSON NOT NULL,
                ts              TIMESTAMPTZ NOT NULL,
                parent_id       VARCHAR,
                trace_id        VARCHAR,
                tags            VARCHAR,
                idempotency_key VARCHAR UNIQUE
            );",
        )
        .map_err(|e| crate::Error::Storage(format!("failed to create table: {e}")))?;

        // Migration: add new columns for existing DBs
        let _ = conn.execute_batch(
            "ALTER TABLE episodic ADD COLUMN IF NOT EXISTS parent_id VARCHAR;
             ALTER TABLE episodic ADD COLUMN IF NOT EXISTS trace_id VARCHAR;
             ALTER TABLE episodic ADD COLUMN IF NOT EXISTS tags VARCHAR;
             ALTER TABLE episodic ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR UNIQUE;",
        );

        // Indexes for frequent query patterns
        let _ = conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_episodic_source ON episodic(source);
             CREATE INDEX IF NOT EXISTS idx_episodic_ts ON episodic(ts);
             CREATE INDEX IF NOT EXISTS idx_episodic_trace ON episodic(trace_id);",
        );

        // Multi-tenancy: tenant_id column for row-level security
        let _ = conn.execute_batch(
            "ALTER TABLE episodic ADD COLUMN IF NOT EXISTS tenant_id VARCHAR DEFAULT 'default';
             CREATE INDEX IF NOT EXISTS idx_episodic_tenant ON episodic(tenant_id);",
        );

        // Cross-store atomicity marker: true once the event's vector is in the semantic index.
        // Events left false (provider down at ingest) are recoverable via reindex.
        let _ = conn.execute_batch(
            "ALTER TABLE episodic ADD COLUMN IF NOT EXISTS embedded BOOLEAN DEFAULT false;
             CREATE INDEX IF NOT EXISTS idx_episodic_embedded ON episodic(embedded);",
        );

        // Sessions table for conversation threading
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
                session_id       VARCHAR PRIMARY KEY,
                parent_session_id VARCHAR,
                agent_id         VARCHAR NOT NULL,
                started_at       TIMESTAMPTZ NOT NULL,
                ended_at         TIMESTAMPTZ,
                summary          VARCHAR,
                metadata         JSON DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_agent ON sessions(agent_id);
            ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant_id VARCHAR DEFAULT 'default';
            ALTER TABLE episodic ADD COLUMN IF NOT EXISTS session_id VARCHAR;
            CREATE INDEX IF NOT EXISTS idx_episodic_session ON episodic(session_id);",
        )
        .map_err(|e| crate::Error::Storage(format!("failed to create sessions table: {e}")))?;

        Ok(())
    }

    /// Append events to the episodic store.
    pub async fn append(&self, events: &[Event]) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        // Fast path: DuckDB is columnar, so row-wise INSERT is slow. When no event needs idempotency
        // dedup (which requires `INSERT OR IGNORE` on the UNIQUE key — unsupported by the Appender),
        // bulk-load via the Appender (~order of magnitude faster).
        if events.iter().all(|e| e.idempotency_key.is_none()) {
            return self.append_fast(events);
        }

        let start = std::time::Instant::now();
        let db = self.write_db.lock();

        db.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| crate::Error::Ingest(format!("begin transaction: {e}")))?;

        let result = (|| {
            // Use INSERT OR IGNORE so that duplicate idempotency_keys are silently skipped.
            // tenant_id is set per-row here (from the payload's `_tenant_id`, injected by
            // ingest_for_tenant) so tenant tagging is atomic and race-free — never via a
            // post-insert UPDATE that could mis-tag concurrent batches.
            let mut stmt = db
                .prepare(
                    "INSERT OR IGNORE INTO episodic (id, source, event_type, payload, ts, parent_id, trace_id, tags, idempotency_key, tenant_id, session_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .map_err(|e| crate::Error::Ingest(format!("prepare error: {e}")))?;

            let mut inserted = 0u64;
            for event in events {
                let payload_str = serde_json::to_string(&event.payload)
                    .map_err(|e| crate::Error::Ingest(e.to_string()))?;
                let ts_str = event.timestamp.to_rfc3339();
                let parent_str = event.parent_id.map(|id| id.to_string());
                let tags_str = if event.tags.is_empty() {
                    None
                } else {
                    Some(event.tags.join(","))
                };
                let tenant = event
                    .payload
                    .get("_tenant_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                // session_id links steps to a run/session (e.g. an agent run); read from the
                // `_session_id` payload tag, mirroring `_tenant_id`.
                let session = event.payload.get("_session_id").and_then(|v| v.as_str());
                let rows = stmt
                    .execute(duckdb::params![
                        event.id.to_string(),
                        event.source,
                        event.event_type,
                        payload_str,
                        ts_str,
                        parent_str,
                        event.trace_id,
                        tags_str,
                        event.idempotency_key,
                        tenant,
                        session,
                    ])
                    .map_err(|e| crate::Error::Ingest(format!("insert error: {e}")))?;
                inserted += rows as u64;
            }

            Ok(inserted)
        })();

        match &result {
            Ok(_) => {
                db.execute_batch("COMMIT")
                    .map_err(|e| crate::Error::Ingest(format!("commit: {e}")))?;
            }
            Err(_) => {
                let _ = db.execute_batch("ROLLBACK");
            }
        }

        // Record metrics
        metrics::histogram!("strata_episodic_append_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        if let Ok(count) = &result {
            metrics::counter!("strata_episodic_events_ingested_total").increment(*count);
        }

        result
    }

    /// Bulk-append via the DuckDB Appender. Caller guarantees no event carries an `idempotency_key`
    /// (the Appender has no `OR IGNORE`, and the column is `UNIQUE`). The `ts` TIMESTAMPTZ is written
    /// as a typed `Value::Timestamp` (the Appender does not coerce strings); other columns are
    /// strings/Options; `embedded`/`idempotency_key` use their column defaults (NULL/false).
    fn append_fast(&self, events: &[Event]) -> crate::Result<u64> {
        let start = std::time::Instant::now();
        let db = self.write_db.lock();
        {
            let mut appender = db
                .appender_with_columns(
                    "episodic",
                    &[
                        "id",
                        "source",
                        "event_type",
                        "payload",
                        "ts",
                        "parent_id",
                        "trace_id",
                        "tags",
                        "tenant_id",
                        "session_id",
                    ],
                )
                .map_err(|e| crate::Error::Ingest(format!("appender: {e}")))?;
            for ev in events {
                let payload_str = serde_json::to_string(&ev.payload)
                    .map_err(|e| crate::Error::Ingest(e.to_string()))?;
                let tags = if ev.tags.is_empty() {
                    None
                } else {
                    Some(ev.tags.join(","))
                };
                let tenant = ev
                    .payload
                    .get("_tenant_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default")
                    .to_string();
                let session = ev
                    .payload
                    .get("_session_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                appender
                    .append_row(duckdb::params![
                        ev.id.to_string(),
                        ev.source,
                        ev.event_type,
                        payload_str,
                        duckdb::types::Value::Timestamp(
                            duckdb::types::TimeUnit::Microsecond,
                            ev.timestamp.timestamp_micros()
                        ),
                        ev.parent_id.map(|p| p.to_string()),
                        ev.trace_id.clone(),
                        tags,
                        tenant,
                        session,
                    ])
                    .map_err(|e| crate::Error::Ingest(format!("append_row: {e}")))?;
            }
            appender
                .flush()
                .map_err(|e| crate::Error::Ingest(format!("appender flush: {e}")))?;
        }
        let count = events.len() as u64;
        metrics::histogram!("strata_episodic_append_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("strata_episodic_events_ingested_total").increment(count);
        Ok(count)
    }

    /// Parse an Event from a DuckDB row.
    /// Expected columns: id(0), source(1), event_type(2), payload::VARCHAR(3), ts::VARCHAR(4),
    ///                    parent_id(5), trace_id(6), tags(7), idempotency_key(8)
    /// Parse a DuckDB `TIMESTAMPTZ::VARCHAR` rendering, which is NOT RFC3339 — it uses a space
    /// separator and a short `+00` offset (e.g. `2024-03-15 10:30:00.123456+00`), and the Appender
    /// path can render with no offset at all. Try the variants, assuming UTC when none is present.
    fn parse_ts(s: &str) -> DateTime<Utc> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return dt.with_timezone(&Utc);
        }
        if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return dt.with_timezone(&Utc);
        }
        if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
            return dt.with_timezone(&Utc);
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            return ndt.and_utc();
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return ndt.and_utc();
        }
        Utc::now()
    }

    fn parse_event(row: &duckdb::Row<'_>) -> duckdb::Result<Event> {
        let id_str: String = row.get(0)?;
        let payload_str: String = row.get(3)?;
        let ts_str: String = row.get(4)?;
        let parent_str: Option<String> = row.get(5).ok();
        let trace_id: Option<String> = row.get(6).ok();
        let tags_str: Option<String> = row.get(7).ok();
        let idempotency_key: Option<String> = row.get(8).ok();

        Ok(Event {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::nil()),
            source: row.get(1)?,
            event_type: row.get(2)?,
            payload: serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null),
            timestamp: Self::parse_ts(&ts_str),
            parent_id: parent_str.and_then(|s| Uuid::parse_str(&s).ok()),
            trace_id,
            tags: tags_str
                .map(|s| s.split(',').map(|t| t.to_string()).collect())
                .unwrap_or_default(),
            idempotency_key,
        })
    }

    const SELECT_COLS: &'static str =
        "id, source, event_type, payload::VARCHAR, ts::VARCHAR, parent_id, trace_id, tags, idempotency_key";

    /// Query events within a time range.
    pub async fn query_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> crate::Result<Vec<Event>> {
        let start_str = start.to_rfc3339();
        let end_str = end.to_rfc3339();

        let db = self.read_conn();
        let sql = format!(
            "SELECT {} FROM episodic WHERE ts >= ?::TIMESTAMPTZ AND ts <= ?::TIMESTAMPTZ ORDER BY ts ASC",
            Self::SELECT_COLS
        );
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let rows = stmt
            .query_map(duckdb::params![start_str, end_str], Self::parse_event)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> crate::Result<Vec<Event>> {
        let db = self.read_conn();
        let sql = format!(
            "SELECT {} FROM episodic WHERE source = ? ORDER BY ts DESC LIMIT ?",
            Self::SELECT_COLS
        );
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let rows = stmt
            .query_map(duckdb::params![source, limit as i64], Self::parse_event)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Events whose vectors are not yet in the semantic index (`embedded = false`), oldest first.
    pub async fn unembedded_events(&self, limit: usize) -> crate::Result<Vec<Event>> {
        let db = self.read_conn();
        let sql = format!(
            "SELECT {} FROM episodic WHERE embedded IS NOT TRUE ORDER BY ts ASC LIMIT ?",
            Self::SELECT_COLS
        );
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], Self::parse_event)
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Count events not yet embedded (cross-store drift gauge).
    pub async fn unembedded_count(&self) -> crate::Result<u64> {
        let db = self.read_conn();
        let n: i64 = db
            .query_row(
                "SELECT count(*) FROM episodic WHERE embedded IS NOT TRUE",
                [],
                |r| r.get(0),
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(n as u64)
    }

    /// Mark events as embedded (their vectors are now in the semantic index).
    pub async fn mark_embedded(&self, ids: &[Uuid]) -> crate::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("UPDATE episodic SET embedded = true WHERE id IN ({placeholders})");
        let params: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
        let boxed: Vec<Box<dyn duckdb::ToSql>> = params
            .iter()
            .map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql>)
            .collect();
        let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
        let db = self.write_db.lock();
        db.execute(&sql, refs.as_slice())
            .map_err(|e| crate::Error::State(format!("mark embedded: {e}")))?;
        Ok(())
    }

    /// Validate that a SQL string contains only SELECT statements **and** does not call any DuckDB
    /// filesystem/network function.
    ///
    /// A bare `SELECT` is not enough of a sandbox on its own: DuckDB exposes table- and scalar
    /// functions such as `read_text`, `read_csv`, `read_blob`, `read_parquet`, `read_json` and
    /// `glob` that are legal inside a `SELECT` but read the **server's** filesystem (e.g.
    /// `SELECT * FROM read_text('/etc/passwd')`, `SELECT * FROM glob('/proc/self/environ')`) — and,
    /// if the `httpfs` extension is loaded, make outbound network requests. Because DuckDB
    /// `enable_external_access` is a *database-global* setting (the read pool is `try_clone`d from
    /// the same instance the backup path `EXPORT DATABASE`s through), we can't disable it at the
    /// connection level without breaking backup/restore — so we reject these functions in the
    /// untrusted-SQL validator instead. Fails **closed**.
    fn validate_read_only(sql: &str) -> crate::Result<()> {
        use sqlparser::ast::{visit_expressions, visit_relations, Expr};
        use sqlparser::dialect::DuckDbDialect;
        use sqlparser::parser::Parser;
        use std::ops::ControlFlow;

        let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
            .map_err(|e| crate::Error::Query(format!("SQL parse error: {e}")))?;

        if statements.is_empty() {
            return Err(crate::Error::Query("empty SQL statement".into()));
        }

        for stmt in &statements {
            match stmt {
                sqlparser::ast::Statement::Query(_) => {}
                other => {
                    return Err(crate::Error::Query(format!(
                        "only SELECT queries are allowed, got: {}",
                        other
                    )));
                }
            }
        }

        let forbidden = crate::Error::Query(
            "SQL function is not permitted (filesystem/network access is disabled)".into(),
        );
        for stmt in &statements {
            // Table-function position: `FROM read_text('/etc/passwd')`, `FROM glob('/etc/*')`.
            let hit = visit_relations(stmt, |name| match name.0.last() {
                Some(ident) if Self::is_forbidden_sql_function(&ident.value) => {
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            });
            if hit.is_break() {
                return Err(forbidden);
            }
            // Scalar-function position: `SELECT read_text('/etc/passwd')`.
            let hit = visit_expressions(stmt, |expr| {
                if let Expr::Function(f) = expr {
                    if f.name
                        .0
                        .last()
                        .map(|i| Self::is_forbidden_sql_function(&i.value))
                        .unwrap_or(false)
                    {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            });
            if hit.is_break() {
                return Err(forbidden);
            }
        }

        Ok(())
    }

    /// Whether `name` is a DuckDB function that reads the filesystem or network. These are the
    /// clearly function-shaped names (all contain an underscore or are the well-known `glob`), so
    /// the risk of colliding with a legitimate table/CTE identifier is negligible.
    fn is_forbidden_sql_function(name: &str) -> bool {
        const FORBIDDEN: &[&str] = &[
            "read_text",
            "read_blob",
            "read_csv",
            "read_csv_auto",
            "read_json",
            "read_json_auto",
            "read_json_objects",
            "read_ndjson",
            "read_ndjson_auto",
            "read_ndjson_objects",
            "read_parquet",
            "read_xlsx",
            "parquet_scan",
            "parquet_metadata",
            "parquet_schema",
            "parquet_file_metadata",
            "parquet_kv_metadata",
            "csv_sniff",
            "sniff_csv",
            "glob",
        ];
        let lower = name.to_ascii_lowercase();
        FORBIDDEN.contains(&lower.as_str())
    }

    /// Execute a read-only SQL query and return results as JSON rows.
    ///
    /// Only SELECT queries are permitted. DDL/DML (DROP, INSERT, etc.) is rejected.
    /// Results are capped at `max_rows` to prevent unbounded memory allocation.
    pub fn query_sql(&self, sql: &str) -> crate::Result<Vec<serde_json::Value>> {
        self.query_sql_limited(sql, 10_000)
    }

    /// Execute a read-only SQL query with an explicit row limit.
    pub fn query_sql_limited(
        &self,
        sql: &str,
        max_rows: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let start = std::time::Instant::now();
        Self::validate_read_only(sql)?;

        let db = self.read_conn();
        let mut stmt = db
            .prepare(sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let mut rows_iter = stmt
            .query([])
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        // Get column names after execution
        let column_count = rows_iter.as_ref().unwrap().column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| {
                rows_iter
                    .as_ref()
                    .unwrap()
                    .column_name(i)
                    .map_or("?".to_string(), |v| v.to_string())
            })
            .collect();

        let mut results = Vec::with_capacity(max_rows.min(1024));
        while let Some(row) = rows_iter
            .next()
            .map_err(|e| crate::Error::Query(e.to_string()))?
        {
            if results.len() >= max_rows {
                break;
            }
            let mut obj = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let val: String = row.get::<_, String>(i).unwrap_or_default();
                obj.insert(name.clone(), serde_json::Value::String(val));
            }
            results.push(serde_json::Value::Object(obj));
        }

        metrics::histogram!("strata_episodic_query_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("strata_episodic_queries_total").increment(1);

        Ok(results)
    }

    /// Execute a read-only SQL query scoped to a single tenant.
    ///
    /// Rewrites every tenant-owned table reference (`episodic`, `sessions`) — via the SQL AST,
    /// never string literals — to a per-tenant filtered view, so a tenant can only ever read its
    /// own rows. Direct references to the internal `*__t_*` views are rejected. Fails **closed**:
    /// if the SQL cannot be parsed/scoped, it is rejected rather than run unscoped.
    pub fn query_sql_for_tenant(
        &self,
        sql: &str,
        tenant: &str,
        max_rows: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let ep_view = Self::tenant_view_name(tenant);
        let sess_view = Self::tenant_session_view_name(tenant);
        // Ensure the per-tenant filtered views exist (idempotent; catalog is shared with readers).
        // BOTH `episodic` AND `sessions` are tenant-owned, so both get a filtered view — the scoping
        // is an allowlist of tenant tables, not just `episodic` (a bare `SELECT * FROM sessions`
        // used to leak every tenant's sessions).
        {
            let db = self.write_db.lock();
            let escaped = tenant.replace('\'', "''");
            db.execute_batch(&format!(
                "CREATE OR REPLACE VIEW {ep_view} AS SELECT * FROM episodic WHERE tenant_id = '{escaped}';
                 CREATE OR REPLACE VIEW {sess_view} AS SELECT * FROM sessions WHERE tenant_id = '{escaped}'"
            ))
            .map_err(|e| crate::Error::Query(format!("create tenant view: {e}")))?;
        }
        let rewritten = Self::scope_sql_to_view(sql, tenant)?;
        self.query_sql_limited(&rewritten, max_rows)
    }

    /// Deterministic, collision-resistant, SQL-safe per-tenant view name for `table`
    /// (`<table>__t_<sanitized-tenant>_<fnv1a-hash>`).
    fn tenant_scoped_view_name(table: &str, tenant: &str) -> String {
        let mut sani: String = tenant
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect();
        sani.truncate(40);
        // FNV-1a so distinct tenants never share a view even if they sanitize alike.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in tenant.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        format!("{table}__t_{sani}_{h:016x}")
    }

    /// Per-tenant filtered view name for the `episodic` table.
    fn tenant_view_name(tenant: &str) -> String {
        Self::tenant_scoped_view_name("episodic", tenant)
    }

    /// Per-tenant filtered view name for the `sessions` table.
    fn tenant_session_view_name(tenant: &str) -> String {
        Self::tenant_scoped_view_name("sessions", tenant)
    }

    /// Rewrite tenant-owned relation references (`episodic`, `sessions`) to the caller's per-tenant
    /// views via the SQL AST (not string matching), and reject any direct reference to an internal
    /// `*__t_*` view (which would let a tenant read another tenant's rows). Fails closed.
    fn scope_sql_to_view(sql: &str, tenant: &str) -> crate::Result<String> {
        use sqlparser::ast::{Ident, ObjectName};
        use sqlparser::dialect::DuckDbDialect;
        use sqlparser::parser::Parser;
        use std::ops::ControlFlow;

        let ep_view = Self::tenant_view_name(tenant);
        let sess_view = Self::tenant_session_view_name(tenant);

        let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)
            .map_err(|e| crate::Error::Query(format!("SQL parse error (tenant scope): {e}")))?;
        if statements.is_empty() {
            return Err(crate::Error::Query("empty SQL statement".into()));
        }
        let mut forbidden = false;
        for stmt in statements.iter_mut() {
            let flow = sqlparser::ast::visit_relations_mut(stmt, |name: &mut ObjectName| {
                let last = name
                    .0
                    .last()
                    .map(|i| i.value.to_ascii_lowercase())
                    .unwrap_or_default();
                if last == "episodic" {
                    *name = ObjectName(vec![Ident::new(ep_view.as_str())]);
                } else if last == "sessions" {
                    *name = ObjectName(vec![Ident::new(sess_view.as_str())]);
                } else if last.starts_with("episodic__t_") || last.starts_with("sessions__t_") {
                    // A tenant must never address an internal per-tenant view by name.
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            });
            if flow.is_break() {
                forbidden = true;
                break;
            }
        }
        if forbidden {
            return Err(crate::Error::Query(
                "reference to an internal per-tenant view is not permitted".into(),
            ));
        }
        Ok(statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; "))
    }

    /// Return the total number of stored events.
    pub async fn count(&self) -> crate::Result<u64> {
        let db = self.read_conn();
        let count: i64 = db
            .query_row("SELECT count(*) FROM episodic", [], |row| row.get(0))
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(count as u64)
    }

    /// Atomically restore the episodic table from a DuckDB `EXPORT DATABASE` directory.
    ///
    /// Imports into a throwaway **staging** database first, then swaps into the live table
    /// inside a single transaction. A corrupt/missing snapshot therefore fails *before* the
    /// live data is touched — unlike a bare `DELETE` + `IMPORT`, which loses data if the
    /// import fails. Used by the Raft snapshot-install path.
    pub fn restore_from_export(&self, export_dir: &Path, staging_path: &Path) -> crate::Result<()> {
        let export_str = export_dir.to_string_lossy();
        let staging_str = staging_path.to_string_lossy();

        // 1. Import the snapshot into a fresh staging DB, off to the side. If the snapshot
        //    is missing/corrupt this fails here and the live table is never modified.
        {
            let staging = Connection::open(staging_path)
                .map_err(|e| crate::Error::Storage(format!("open staging db: {e}")))?;
            staging
                .execute_batch(&format!("IMPORT DATABASE '{export_str}'"))
                .map_err(|e| crate::Error::Storage(format!("import snapshot: {e}")))?;
        }

        // 2. Swap into the live table transactionally; roll back on any failure.
        let db = self.write_db.lock();
        let swap = format!(
            "ATTACH '{staging_str}' AS snap (READ_ONLY);
             BEGIN TRANSACTION;
             DELETE FROM episodic;
             INSERT INTO episodic SELECT * FROM snap.episodic;
             COMMIT;"
        );
        if let Err(e) = db.execute_batch(&swap) {
            let _ = db.execute_batch("ROLLBACK");
            let _ = db.execute_batch("DETACH snap");
            return Err(crate::Error::Storage(format!("restore swap failed: {e}")));
        }
        let _ = db.execute_batch("DETACH snap");
        Ok(())
    }

    // ── Session Management ──────────────────────────────────────────

    /// Start a new conversation session.
    pub async fn start_session(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> crate::Result<()> {
        let db = self.write_db.lock();
        let meta_str = serde_json::to_string(&metadata.unwrap_or(serde_json::json!({})))
            .unwrap_or_else(|_| "{}".to_string());
        db.execute(
            "INSERT INTO sessions (session_id, parent_session_id, agent_id, started_at, metadata)
             VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?::JSON)",
            duckdb::params![session_id, parent_session_id, agent_id, meta_str],
        )
        .map_err(|e| crate::Error::Ingest(format!("start session: {e}")))?;
        Ok(())
    }

    /// End a session, optionally setting a summary.
    pub async fn end_session(&self, session_id: &str, summary: Option<&str>) -> crate::Result<()> {
        let db = self.write_db.lock();
        db.execute(
            "UPDATE sessions SET ended_at = CURRENT_TIMESTAMP, summary = ?
             WHERE session_id = ?",
            duckdb::params![summary, session_id],
        )
        .map_err(|e| crate::Error::Ingest(format!("end session: {e}")))?;
        Ok(())
    }

    /// Get session details.
    pub async fn get_session(&self, session_id: &str) -> crate::Result<Option<serde_json::Value>> {
        let db = self.read_conn();
        let result = db
            .query_row(
                "SELECT session_id, parent_session_id, agent_id,
                        started_at::VARCHAR, ended_at::VARCHAR, summary, metadata::VARCHAR
                 FROM sessions WHERE session_id = ?",
                duckdb::params![session_id],
                |row| {
                    let meta_str: String = row.get(6)?;
                    Ok(serde_json::json!({
                        "session_id": row.get::<_, String>(0)?,
                        "parent_session_id": row.get::<_, Option<String>>(1)?,
                        "agent_id": row.get::<_, String>(2)?,
                        "started_at": row.get::<_, String>(3)?,
                        "ended_at": row.get::<_, Option<String>>(4)?,
                        "summary": row.get::<_, Option<String>>(5)?,
                        "metadata": serde_json::from_str::<serde_json::Value>(&meta_str).unwrap_or_default(),
                    }))
                },
            )
            .ok();
        Ok(result)
    }

    /// List sessions for an agent.
    pub async fn list_sessions(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let db = self.read_conn();
        let mut stmt = db
            .prepare(
                "SELECT session_id, started_at::VARCHAR, ended_at::VARCHAR, summary
                 FROM sessions WHERE agent_id = ?
                 ORDER BY started_at DESC LIMIT ?",
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map(duckdb::params![agent_id, limit as i64], |row| {
                Ok(serde_json::json!({
                    "session_id": row.get::<_, String>(0)?,
                    "started_at": row.get::<_, String>(1)?,
                    "ended_at": row.get::<_, Option<String>>(2)?,
                    "summary": row.get::<_, Option<String>>(3)?,
                }))
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Recall all events in a session.
    pub async fn recall_session(&self, session_id: &str) -> crate::Result<Vec<serde_json::Value>> {
        let db = self.read_conn();
        let mut stmt = db
            .prepare(
                "SELECT id, source, event_type, payload::VARCHAR, ts::VARCHAR
                 FROM episodic WHERE session_id = ?
                 ORDER BY ts ASC",
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map(duckdb::params![session_id], |row| {
                let payload_str: String = row.get(3)?;
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "source": row.get::<_, String>(1)?,
                    "event_type": row.get::<_, String>(2)?,
                    "payload": serde_json::from_str::<serde_json::Value>(&payload_str).unwrap_or_default(),
                    "ts": row.get::<_, String>(4)?,
                }))
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Start a session tagged with a tenant (for isolation).
    pub async fn start_session_for_tenant(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
        tenant: &str,
    ) -> crate::Result<()> {
        let db = self.write_db.lock();
        let meta_str = serde_json::to_string(&metadata.unwrap_or(serde_json::json!({})))
            .unwrap_or_else(|_| "{}".to_string());
        db.execute(
            "INSERT INTO sessions (session_id, parent_session_id, agent_id, started_at, metadata, tenant_id)
             VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?::JSON, ?)",
            duckdb::params![session_id, parent_session_id, agent_id, meta_str, tenant],
        )
        .map_err(|e| crate::Error::Ingest(format!("start session: {e}")))?;
        Ok(())
    }

    /// End a session scoped to a tenant. Returns true iff a row was updated.
    pub async fn end_session_for_tenant(
        &self,
        session_id: &str,
        summary: Option<&str>,
        tenant: &str,
    ) -> crate::Result<bool> {
        let db = self.write_db.lock();
        let n = db
            .execute(
                "UPDATE sessions SET ended_at = CURRENT_TIMESTAMP, summary = ?
                 WHERE session_id = ? AND tenant_id = ?",
                duckdb::params![summary, session_id, tenant],
            )
            .map_err(|e| crate::Error::Ingest(format!("end session: {e}")))?;
        Ok(n > 0)
    }

    /// Recall a session's events, scoped to a tenant.
    pub async fn recall_session_for_tenant(
        &self,
        session_id: &str,
        tenant: &str,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let db = self.read_conn();
        let mut stmt = db
            .prepare(
                "SELECT id, source, event_type, payload::VARCHAR, ts::VARCHAR
                 FROM episodic WHERE session_id = ? AND tenant_id = ?
                 ORDER BY ts ASC",
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        let rows = stmt
            .query_map(duckdb::params![session_id, tenant], |row| {
                let payload_str: String = row.get(3)?;
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "source": row.get::<_, String>(1)?,
                    "event_type": row.get::<_, String>(2)?,
                    "payload": serde_json::from_str::<serde_json::Value>(&payload_str).unwrap_or_default(),
                    "ts": row.get::<_, String>(4)?,
                }))
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }
}

impl Default for EpisodicStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(source: &str, event_type: &str) -> Event {
        Event {
            id: Uuid::new_v4(),
            source: source.into(),
            event_type: event_type.into(),
            payload: serde_json::json!({"key": "value"}),
            timestamp: Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn new_store_has_zero_count() {
        let store = EpisodicStore::new();
        assert_eq!(store.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn append_and_count() {
        let store = EpisodicStore::new();
        let events = vec![make_event("app", "click"), make_event("app", "view")];
        let count = store.append(&events).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn append_empty_batch() {
        let store = EpisodicStore::new();
        let count = store.append(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn fast_path_preserves_timestamp_and_payload() {
        // No idempotency key → the Appender fast path. Verify the typed TIMESTAMPTZ + JSON payload
        // round-trip exactly (the riskiest part of the Appender rewrite).
        let store = EpisodicStore::new();
        let ts = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00.123456Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut ev = Event::new("src", "etype", serde_json::json!({"k": "v", "n": 42}));
        ev.timestamp = ts;
        assert!(ev.idempotency_key.is_none());
        store.append(std::slice::from_ref(&ev)).await.unwrap();

        let got = store
            .query_time_range(
                ts - chrono::Duration::seconds(1),
                ts + chrono::Duration::seconds(1),
            )
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, ev.id);
        assert_eq!(got[0].payload, ev.payload);
        assert_eq!(got[0].timestamp.timestamp_micros(), ts.timestamp_micros());
    }

    #[tokio::test]
    async fn query_by_source() {
        let store = EpisodicStore::new();
        store
            .append(&[
                make_event("app-a", "click"),
                make_event("app-b", "view"),
                make_event("app-a", "submit"),
            ])
            .await
            .unwrap();

        let events = store.query_by_source("app-a", 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.source == "app-a"));
    }

    #[tokio::test]
    async fn query_time_range() {
        let store = EpisodicStore::new();
        let past = Utc::now() - chrono::Duration::hours(1);
        let future = Utc::now() + chrono::Duration::hours(1);

        store.append(&[make_event("app", "event")]).await.unwrap();

        let events = store.query_time_range(past, future).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn query_sql_count() {
        let store = EpisodicStore::new();
        store.append(&[make_event("src", "type")]).await.unwrap();

        let rows = store
            .query_sql("SELECT count(*)::VARCHAR as cnt FROM episodic")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["cnt"], "1");
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = make_event("my-app", "order.placed");
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, event.id);
        assert_eq!(deserialized.source, "my-app");
    }

    #[tokio::test]
    async fn large_batch_append() {
        let store = EpisodicStore::new();
        let events: Vec<Event> = (0..500)
            .map(|i| make_event("bench", &format!("event.{i}")))
            .collect();
        let count = store.append(&events).await.unwrap();
        assert_eq!(count, 500);
        assert_eq!(store.count().await.unwrap(), 500);
    }

    #[test]
    fn query_sql_rejects_drop_table() {
        let store = EpisodicStore::new();
        let result = store.query_sql("DROP TABLE episodic");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("only SELECT"));
    }

    #[test]
    fn query_sql_rejects_insert() {
        let store = EpisodicStore::new();
        let result = store.query_sql("INSERT INTO episodic VALUES ('a','b','c','d','e')");
        assert!(result.is_err());
    }

    #[test]
    fn query_sql_allows_select() {
        let store = EpisodicStore::new();
        let result = store.query_sql("SELECT 1::VARCHAR as v");
        assert!(result.is_ok());
    }

    #[test]
    fn query_sql_rejects_filesystem_functions() {
        let store = EpisodicStore::new();
        // These are legal SELECTs but read the server filesystem — must be rejected (fail-closed),
        // in both table-function and scalar-function position, and through subqueries/CTEs.
        for sql in [
            "SELECT * FROM read_text('/etc/passwd')",
            "SELECT content FROM read_csv('/etc/passwd')",
            "SELECT * FROM glob('/proc/self/environ')",
            "SELECT * FROM read_parquet('/data/other-tenant.parquet')",
            "SELECT read_text('/etc/passwd')",
            "WITH x AS (SELECT * FROM read_blob('/etc/shadow')) SELECT * FROM x",
        ] {
            let result = store.query_sql(sql);
            assert!(result.is_err(), "must reject: {sql}");
            assert!(
                result.unwrap_err().to_string().contains("not permitted"),
                "wrong error for: {sql}"
            );
        }
        // A string literal that merely spells a function name is fine (not a call).
        assert!(store.query_sql("SELECT 'read_text' AS label").is_ok());
    }

    #[tokio::test]
    async fn tenant_scoping_covers_sessions_and_blocks_internal_views() {
        let store = EpisodicStore::new();
        store
            .start_session_for_tenant("sess-a", "agent", None, None, "tenant-a")
            .await
            .unwrap();
        store
            .start_session_for_tenant("sess-b", "agent", None, None, "tenant-b")
            .await
            .unwrap();

        // `sessions` is now tenant-scoped (previously `SELECT * FROM sessions` leaked all tenants').
        let a = store
            .query_sql_for_tenant("SELECT session_id FROM sessions", "tenant-a", 100)
            .unwrap();
        assert_eq!(a.len(), 1, "tenant A must see only its own session");
        assert_eq!(a[0]["session_id"], "sess-a");

        // Addressing another tenant's internal view by name is rejected (fail-closed).
        let escape = store.query_sql_for_tenant(
            "SELECT * FROM episodic__t_tenant_b_0000000000000000",
            "tenant-a",
            100,
        );
        assert!(
            escape.is_err(),
            "direct reference to an internal per-tenant view must be rejected"
        );
    }

    #[tokio::test]
    async fn idempotency_key_dedup() {
        let store = EpisodicStore::new();
        let mut e1 = make_event("app", "click");
        e1.idempotency_key = Some("dedup-key-1".into());
        let mut e2 = make_event("app", "click");
        e2.idempotency_key = Some("dedup-key-1".into()); // same key

        store.append(&[e1]).await.unwrap();
        let inserted = store.append(&[e2]).await.unwrap();
        assert_eq!(inserted, 0); // duplicate skipped
        assert_eq!(store.count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn idempotency_key_different_keys() {
        let store = EpisodicStore::new();
        let mut e1 = make_event("app", "click");
        e1.idempotency_key = Some("key-a".into());
        let mut e2 = make_event("app", "click");
        e2.idempotency_key = Some("key-b".into()); // different key

        store.append(&[e1]).await.unwrap();
        let inserted = store.append(&[e2]).await.unwrap();
        assert_eq!(inserted, 1);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn idempotency_key_none_allows_duplicates() {
        let store = EpisodicStore::new();
        let e1 = make_event("app", "click"); // idempotency_key = None
        let e2 = make_event("app", "click"); // idempotency_key = None

        store.append(&[e1, e2]).await.unwrap();
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn restore_from_export_replaces_data() {
        let dir = tempfile::tempdir().unwrap();
        let export_dir = dir.path().join("export");

        // Build a source store with two events and EXPORT it.
        {
            let src = EpisodicStore::open(&dir.path().join("src.duckdb"), 4).unwrap();
            src.append(&[make_event("snap", "e1"), make_event("snap", "e2")])
                .await
                .unwrap();
            let db = src.write_conn();
            db.execute_batch(&format!(
                "EXPORT DATABASE '{}'",
                export_dir.to_string_lossy()
            ))
            .unwrap();
        }

        // Target has unrelated data; restore replaces it with the snapshot's.
        let tgt = EpisodicStore::new();
        tgt.append(&[make_event("old", "x")]).await.unwrap();
        assert_eq!(tgt.count().await.unwrap(), 1);

        tgt.restore_from_export(&export_dir, &dir.path().join("staging.duckdb"))
            .unwrap();
        assert_eq!(tgt.count().await.unwrap(), 2);
        assert_eq!(tgt.query_by_source("snap", 10).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn restore_from_bad_export_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let tgt = EpisodicStore::new();
        tgt.append(&[make_event("keep", "x")]).await.unwrap();

        // A missing/corrupt snapshot must fail without destroying live data.
        let res =
            tgt.restore_from_export(&dir.path().join("nope"), &dir.path().join("staging.duckdb"));
        assert!(res.is_err());
        assert_eq!(tgt.count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn query_sql_for_tenant_isolates_rows() {
        let store = EpisodicStore::new();
        store
            .append(&[make_event("appA", "e"), make_event("appB", "e")])
            .await
            .unwrap();
        {
            let db = store.write_conn();
            db.execute(
                "UPDATE episodic SET tenant_id = 'tenant-a' WHERE source = 'appA'",
                [],
            )
            .unwrap();
            db.execute(
                "UPDATE episodic SET tenant_id = 'tenant-b' WHERE source = 'appB'",
                [],
            )
            .unwrap();
        }

        // tenant-a sees ONLY its row, even with `SELECT *`.
        let rows = store
            .query_sql_for_tenant("SELECT source FROM episodic", "tenant-a", 100)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["source"], "appA");

        // A string literal 'episodic' must NOT be rewritten (AST-only, not text matching).
        let lit = store
            .query_sql_for_tenant("SELECT 'episodic'::VARCHAR AS lit", "tenant-a", 100)
            .unwrap();
        assert_eq!(lit[0]["lit"], "episodic");

        // Injection in the tenant id is neutralized (escaped → matches nothing, no error).
        let inj = store
            .query_sql_for_tenant("SELECT source FROM episodic", "x' OR '1'='1", 100)
            .unwrap();
        assert_eq!(inj.len(), 0);
    }

    #[tokio::test]
    async fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("episodic.duckdb");

        // Write data
        {
            let store = EpisodicStore::open(&db_path, 4).unwrap();
            store.append(&[make_event("app", "click")]).await.unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
        }

        // Reopen and verify data survived
        {
            let store = EpisodicStore::open(&db_path, 4).unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
            let events = store.query_by_source("app", 10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "click");
        }
    }
}
