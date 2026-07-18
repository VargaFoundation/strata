//! Tiny versioned schema-migration runner for the DuckDB-backed stores.
//!
//! Replaces ad-hoc `ALTER TABLE ... IF NOT EXISTS` sprawl with an ordered, version-tracked list so
//! schema changes (and future *data* migrations) are applied exactly once and auditable. A
//! `{table}` row records each applied version; re-running is a no-op.

use duckdb::Connection;

/// One ordered schema change. `sql` may contain multiple statements (run as a batch).
pub struct Migration {
    pub version: u32,
    pub sql: &'static str,
}

/// Apply pending migrations (those with `version` greater than the recorded max) in ascending
/// order, recording each in `table`. Idempotent. `migrations` MUST be sorted ascending by version.
/// Returns the resulting schema version.
pub fn run_migrations(
    conn: &Connection,
    table: &str,
    migrations: &[Migration],
) -> crate::Result<u32> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {table} \
         (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now());"
    ))
    .map_err(|e| crate::Error::Storage(format!("create migrations table {table}: {e}")))?;

    let current: u32 = conn
        .query_row(
            &format!("SELECT COALESCE(MAX(version), 0) FROM {table}"),
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|v| v as u32)
        .unwrap_or(0);

    let mut applied = current;
    for m in migrations.iter().filter(|m| m.version > current) {
        conn.execute_batch(m.sql)
            .map_err(|e| crate::Error::Storage(format!("migration v{}: {e}", m.version)))?;
        conn.execute(
            &format!("INSERT INTO {table} (version) VALUES (?)"),
            duckdb::params![m.version as i64],
        )
        .map_err(|e| crate::Error::Storage(format!("record migration v{}: {e}", m.version)))?;
        applied = m.version;
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_pending_once_and_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        let migs = [
            Migration {
                version: 1,
                sql: "CREATE TABLE t (id INTEGER);",
            },
            Migration {
                version: 2,
                sql: "ALTER TABLE t ADD COLUMN name VARCHAR;",
            },
        ];
        assert_eq!(run_migrations(&conn, "_mig", &migs).unwrap(), 2);
        // Re-running applies nothing new (and does not error on the already-created table/column).
        assert_eq!(run_migrations(&conn, "_mig", &migs).unwrap(), 2);
        // Exactly two versions recorded.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _mig", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        // The column from v2 exists.
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'x')", [])
            .unwrap();
    }

    #[test]
    fn resumes_from_partial_version() {
        let conn = Connection::open_in_memory().unwrap();
        let v1 = [Migration {
            version: 1,
            sql: "CREATE TABLE t (id INTEGER);",
        }];
        assert_eq!(run_migrations(&conn, "_mig", &v1).unwrap(), 1);
        // A later run with an additional migration applies only the new one.
        let v12 = [
            Migration {
                version: 1,
                sql: "CREATE TABLE t (id INTEGER);", // would fail if re-run — proves it's skipped
            },
            Migration {
                version: 2,
                sql: "ALTER TABLE t ADD COLUMN name VARCHAR;",
            },
        ];
        assert_eq!(run_migrations(&conn, "_mig", &v12).unwrap(), 2);
    }
}
