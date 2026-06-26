//! Audit logging — records every authenticated request.
//!
//! Stores entries in a dedicated DuckDB `audit_log` table (in-memory by default).
//! Queryable via `GET /api/v1/admin/audit?since=2026-01-01`.

use std::sync::Arc;
use std::time::Duration;

use duckdb::{params, Connection};
use parking_lot::Mutex;

/// A single audit log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub identity: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub duration_ms: f64,
}

/// Audit log backed by an in-memory DuckDB table.
#[derive(Clone)]
pub struct AuditLog {
    conn: Arc<Mutex<Connection>>,
}

const AUDIT_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS audit_log (
    timestamp  TIMESTAMPTZ NOT NULL DEFAULT now(),
    identity   VARCHAR NOT NULL,
    method     VARCHAR NOT NULL,
    path       VARCHAR NOT NULL,
    status     INTEGER NOT NULL,
    duration_ms DOUBLE NOT NULL
);";

impl AuditLog {
    /// Create a new in-memory audit log (lost on restart — use [`Self::open`] for durability).
    pub fn new() -> Result<Self, crate::Error> {
        let conn = Connection::open_in_memory()
            .map_err(|e| crate::Error::Auth(format!("audit log init: {e}")))?;
        conn.execute_batch(AUDIT_SCHEMA)
            .map_err(|e| crate::Error::Auth(format!("audit table creation: {e}")))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open a **durable** file-backed audit log (survives restart — required for compliance).
    /// `:memory:` or empty falls back to in-memory.
    pub fn open(path: &std::path::Path) -> Result<Self, crate::Error> {
        if path.as_os_str().is_empty() || path.as_os_str() == ":memory:" {
            return Self::new();
        }
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)
            .map_err(|e| crate::Error::Auth(format!("audit log open: {e}")))?;
        conn.execute_batch(AUDIT_SCHEMA)
            .map_err(|e| crate::Error::Auth(format!("audit table creation: {e}")))?;
        tracing::info!(path = %path.display(), "audit log: durable (file-backed)");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Record an audit entry.
    pub fn record(
        &self,
        identity: &str,
        method: &str,
        path: &str,
        status: u16,
        duration: Duration,
    ) {
        let conn = self.conn.lock();
        let duration_ms = duration.as_secs_f64() * 1000.0;
        if let Err(e) = conn.execute(
            "INSERT INTO audit_log (identity, method, path, status, duration_ms)
             VALUES (?, ?, ?, ?, ?)",
            params![identity, method, path, status as i32, duration_ms],
        ) {
            tracing::warn!(error = %e, "failed to write audit log entry");
        }
    }

    /// Query audit entries since a given ISO-8601 date.
    ///
    /// Returns at most 1000 entries, ordered newest first.
    pub fn query_since(&self, since: &str) -> Result<Vec<AuditEntry>, crate::Error> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT timestamp::VARCHAR, identity, method, path, status, duration_ms
                 FROM audit_log
                 WHERE timestamp >= ?::TIMESTAMPTZ
                 ORDER BY timestamp DESC
                 LIMIT 1000",
            )
            .map_err(|e| crate::Error::Auth(format!("audit query prepare: {e}")))?;

        let rows = stmt
            .query_map(params![since], |row| {
                Ok(AuditEntry {
                    timestamp: row.get(0)?,
                    identity: row.get(1)?,
                    method: row.get(2)?,
                    path: row.get(3)?,
                    status: row.get::<_, i32>(4)? as u16,
                    duration_ms: row.get(5)?,
                })
            })
            .map_err(|e| crate::Error::Auth(format!("audit query: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            match row {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(error = %e, "skipping malformed audit row");
                }
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_log_roundtrip() {
        let log = AuditLog::new().unwrap();
        log.record(
            "user-1",
            "GET",
            "/api/v1/query",
            200,
            Duration::from_millis(42),
        );
        log.record(
            "agent-2",
            "POST",
            "/api/v1/ingest",
            201,
            Duration::from_millis(100),
        );

        let entries = log.query_since("2000-01-01").unwrap();
        assert_eq!(entries.len(), 2);
        // Newest first
        assert_eq!(entries[0].identity, "agent-2");
        assert_eq!(entries[0].method, "POST");
        assert_eq!(entries[0].status, 201);
        assert_eq!(entries[1].identity, "user-1");
        assert_eq!(entries[1].method, "GET");
        assert_eq!(entries[1].status, 200);
    }

    #[test]
    fn audit_log_since_filter() {
        let log = AuditLog::new().unwrap();
        log.record(
            "user-1",
            "GET",
            "/api/v1/query",
            200,
            Duration::from_millis(10),
        );

        // Query with future date — should return nothing
        let entries = log.query_since("2099-01-01").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn audit_log_empty() {
        let log = AuditLog::new().unwrap();
        let entries = log.query_since("2000-01-01").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn audit_log_persists_to_disk() {
        let path =
            std::env::temp_dir().join(format!("strata-audit-test-{}.duckdb", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let log = AuditLog::open(&path).unwrap();
            log.record("u", "GET", "/x", 200, Duration::from_millis(1));
        }
        // Reopen — the entry survived the restart.
        let log = AuditLog::open(&path).unwrap();
        assert_eq!(log.query_since("2000-01-01").unwrap().len(), 1);
        let _ = std::fs::remove_file(&path);
    }
}
