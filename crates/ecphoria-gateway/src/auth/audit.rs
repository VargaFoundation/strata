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
    /// Tenant the caller was scoped to (None for unscoped API keys / disabled auth).
    pub tenant: Option<String>,
    /// Client IP from `X-Forwarded-For`/`X-Real-IP`, when present behind a proxy.
    pub ip: Option<String>,
}

/// Audit log backed by an in-memory DuckDB table.
#[derive(Clone)]
pub struct AuditLog {
    conn: Arc<Mutex<Connection>>,
}

// The two ADD COLUMN statements migrate pre-existing (durable) audit tables that predate the
// tenant/ip columns; they are no-ops on a freshly created table.
const AUDIT_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS audit_log (
    timestamp  TIMESTAMPTZ NOT NULL DEFAULT now(),
    identity   VARCHAR NOT NULL,
    method     VARCHAR NOT NULL,
    path       VARCHAR NOT NULL,
    status     INTEGER NOT NULL,
    duration_ms DOUBLE NOT NULL,
    tenant     VARCHAR,
    ip         VARCHAR
);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant VARCHAR;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS ip VARCHAR;";

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
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        identity: &str,
        method: &str,
        path: &str,
        status: u16,
        duration: Duration,
        tenant: Option<&str>,
        ip: Option<&str>,
    ) {
        let conn = self.conn.lock();
        let duration_ms = duration.as_secs_f64() * 1000.0;
        if let Err(e) = conn.execute(
            "INSERT INTO audit_log (identity, method, path, status, duration_ms, tenant, ip)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                identity,
                method,
                path,
                status as i32,
                duration_ms,
                tenant,
                ip
            ],
        ) {
            tracing::warn!(error = %e, "failed to write audit log entry");
        }
    }

    /// Query audit entries since a given ISO-8601 date, optionally filtered to one `tenant`.
    ///
    /// Returns at most 1000 entries, ordered newest first.
    pub fn query_since(
        &self,
        since: &str,
        tenant: Option<&str>,
    ) -> Result<Vec<AuditEntry>, crate::Error> {
        let conn = self.conn.lock();
        let base =
            "SELECT timestamp::VARCHAR, identity, method, path, status, duration_ms, tenant, ip
                    FROM audit_log WHERE timestamp >= ?::TIMESTAMPTZ";
        let order = " ORDER BY timestamp DESC LIMIT 1000";

        let mut entries = Vec::new();
        let collect = |rows: duckdb::MappedRows<'_, _>, out: &mut Vec<AuditEntry>| {
            for row in rows {
                match row {
                    Ok(entry) => out.push(entry),
                    Err(e) => tracing::warn!(error = %e, "skipping malformed audit row"),
                }
            }
        };

        if let Some(t) = tenant {
            let sql = format!("{base} AND tenant = ?{order}");
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| crate::Error::Auth(format!("audit query prepare: {e}")))?;
            let rows = stmt
                .query_map(params![since, t], row_to_entry)
                .map_err(|e| crate::Error::Auth(format!("audit query: {e}")))?;
            collect(rows, &mut entries);
        } else {
            let sql = format!("{base}{order}");
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| crate::Error::Auth(format!("audit query prepare: {e}")))?;
            let rows = stmt
                .query_map(params![since], row_to_entry)
                .map_err(|e| crate::Error::Auth(format!("audit query: {e}")))?;
            collect(rows, &mut entries);
        }
        Ok(entries)
    }
}

fn row_to_entry(row: &duckdb::Row<'_>) -> duckdb::Result<AuditEntry> {
    Ok(AuditEntry {
        timestamp: row.get(0)?,
        identity: row.get(1)?,
        method: row.get(2)?,
        path: row.get(3)?,
        status: row.get::<_, i32>(4)? as u16,
        duration_ms: row.get(5)?,
        tenant: row.get(6)?,
        ip: row.get(7)?,
    })
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
            Some("acme"),
            Some("10.0.0.1"),
        );
        log.record(
            "agent-2",
            "POST",
            "/api/v1/ingest",
            201,
            Duration::from_millis(100),
            None,
            None,
        );

        let entries = log.query_since("2000-01-01", None).unwrap();
        assert_eq!(entries.len(), 2);
        // Tenant + IP round-trip; the tenant filter narrows results.
        assert_eq!(entries[1].tenant.as_deref(), Some("acme"));
        assert_eq!(entries[1].ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(
            log.query_since("2000-01-01", Some("acme")).unwrap().len(),
            1
        );
        assert!(log
            .query_since("2000-01-01", Some("other"))
            .unwrap()
            .is_empty());
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
            None,
            None,
        );

        // Query with future date — should return nothing
        let entries = log.query_since("2099-01-01", None).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn audit_log_empty() {
        let log = AuditLog::new().unwrap();
        let entries = log.query_since("2000-01-01", None).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn audit_log_persists_to_disk() {
        let path =
            std::env::temp_dir().join(format!("ecphoria-audit-test-{}.duckdb", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let log = AuditLog::open(&path).unwrap();
            log.record("u", "GET", "/x", 200, Duration::from_millis(1), None, None);
        }
        // Reopen — the entry survived the restart.
        let log = AuditLog::open(&path).unwrap();
        assert_eq!(log.query_since("2000-01-01", None).unwrap().len(), 1);
        let _ = std::fs::remove_file(&path);
    }
}
