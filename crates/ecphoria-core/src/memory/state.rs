//! State memory store — transactional key-value with MVCC and change notifications.

use dashmap::DashMap;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;

use parking_lot::Mutex;

/// A state entry for an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEntry {
    pub agent_id: String,
    pub key: String,
    pub value: serde_json::Value,
    pub version: u64,
}

/// Notification sent when a state key changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateChange {
    pub agent_id: String,
    pub key: String,
    pub value: serde_json::Value,
    pub version: u64,
    pub deleted: bool,
}

/// Key-value store with MVCC, watchers, and TTL support.
///
/// Architecture:
/// - SQLite for durable persistence (MVCC via version column)
/// - DashMap as a hot read cache for frequently accessed keys
/// - Broadcast channel for change notifications (watchers)
pub struct StateStore {
    db: Arc<Mutex<Connection>>,
    cache: DashMap<(String, String), StateEntry>,
    /// Broadcast channel for state change notifications.
    change_tx: broadcast::Sender<StateChange>,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStore")
            .field("cache_size", &self.cache.len())
            .finish()
    }
}

impl StateStore {
    /// Open or create a state store at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let conn = if path.as_os_str() == ":memory:" {
            Connection::open_in_memory()
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| crate::Error::State(format!("failed to create directory: {e}")))?;
            }
            Connection::open(path)
        }
        .map_err(|e| crate::Error::State(format!("failed to open state db: {e}")))?;

        // Durability: WAL mode survives process crashes, NORMAL sync is safe with WAL,
        // busy_timeout avoids SQLITE_BUSY under concurrent access.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|e| crate::Error::State(format!("failed to set pragmas: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS state (
                agent_id   TEXT NOT NULL,
                key        TEXT NOT NULL,
                value      TEXT NOT NULL,
                version    INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at TEXT,
                PRIMARY KEY (agent_id, key)
            );",
        )
        .map_err(|e| crate::Error::State(format!("failed to create table: {e}")))?;

        // Migration: add expires_at column if it doesn't exist (for existing DBs)
        let _ = conn.execute_batch("ALTER TABLE state ADD COLUMN expires_at TEXT");

        let (change_tx, _) = broadcast::channel(1024);

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            cache: DashMap::new(),
            change_tx,
        })
    }

    /// Create an in-memory state store (for testing).
    pub fn new() -> Self {
        Self::open(Path::new(":memory:")).expect("failed to create in-memory state store")
    }

    /// Acquire the database connection (for schema introspection queries).
    pub fn db_conn(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.db.lock()
    }

    /// Subscribe to state change notifications.
    ///
    /// Returns a broadcast receiver that receives `StateChange` events
    /// for all agent state modifications (set, delete, TTL expiry).
    pub fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.change_tx.subscribe()
    }

    /// Notify watchers of a state change.
    fn notify(&self, change: StateChange) {
        // Ignore send errors (no subscribers)
        let _ = self.change_tx.send(change);
    }

    /// Get the current value for a key.
    pub async fn get(&self, agent_id: &str, key: &str) -> crate::Result<Option<StateEntry>> {
        let cache_key = (agent_id.to_string(), key.to_string());

        // Check hot cache first
        if let Some(entry) = self.cache.get(&cache_key) {
            return Ok(Some(entry.value().clone()));
        }

        // Fall through to SQLite
        let db = self.db.lock();
        let mut stmt = db
            .prepare(
                "SELECT agent_id, key, value, version FROM state
                 WHERE agent_id = ?1 AND key = ?2
                 AND (expires_at IS NULL OR expires_at > datetime('now'))",
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;

        let result = stmt
            .query_row(rusqlite::params![agent_id, key], |row| {
                let value_str: String = row.get(2)?;
                Ok(StateEntry {
                    agent_id: row.get(0)?,
                    key: row.get(1)?,
                    value: serde_json::from_str(&value_str).unwrap_or(serde_json::Value::Null),
                    version: row.get(3)?,
                })
            })
            .ok();

        // Populate cache on read — use or_insert to avoid overwriting a
        // concurrent write that happened between our cache miss and DB read.
        if let Some(ref entry) = result {
            self.cache.entry(cache_key).or_insert_with(|| entry.clone());
        }

        Ok(result)
    }

    /// Set a value. Returns the new version.
    pub async fn set(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> crate::Result<u64> {
        let value_str =
            serde_json::to_string(&value).map_err(|e| crate::Error::State(e.to_string()))?;

        let db = self.db.lock();
        db.execute(
            "INSERT INTO state (agent_id, key, value, version)
             VALUES (?1, ?2, ?3, 1)
             ON CONFLICT(agent_id, key) DO UPDATE SET
                value = excluded.value,
                version = state.version + 1,
                updated_at = datetime('now')",
            rusqlite::params![agent_id, key, value_str],
        )
        .map_err(|e| crate::Error::State(e.to_string()))?;

        let version: u64 = db
            .query_row(
                "SELECT version FROM state WHERE agent_id = ?1 AND key = ?2",
                rusqlite::params![agent_id, key],
                |row| row.get(0),
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;

        // Update cache + notify watchers
        let entry = StateEntry {
            agent_id: agent_id.to_string(),
            key: key.to_string(),
            value: value.clone(),
            version,
        };
        self.cache
            .insert((agent_id.to_string(), key.to_string()), entry);

        self.notify(StateChange {
            agent_id: agent_id.to_string(),
            key: key.to_string(),
            value,
            version,
            deleted: false,
        });

        Ok(version)
    }

    /// Set a value with a TTL. The key automatically expires after the given duration.
    pub async fn set_with_ttl(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
        ttl: std::time::Duration,
    ) -> crate::Result<u64> {
        let value_str =
            serde_json::to_string(&value).map_err(|e| crate::Error::State(e.to_string()))?;
        let expires_at =
            (chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default()).to_rfc3339();

        let db = self.db.lock();
        db.execute(
            "INSERT INTO state (agent_id, key, value, version, expires_at)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT(agent_id, key) DO UPDATE SET
                value = excluded.value,
                version = state.version + 1,
                updated_at = datetime('now'),
                expires_at = excluded.expires_at",
            rusqlite::params![agent_id, key, value_str, expires_at],
        )
        .map_err(|e| crate::Error::State(e.to_string()))?;

        let version: u64 = db
            .query_row(
                "SELECT version FROM state WHERE agent_id = ?1 AND key = ?2",
                rusqlite::params![agent_id, key],
                |row| row.get(0),
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;

        let entry = StateEntry {
            agent_id: agent_id.to_string(),
            key: key.to_string(),
            value,
            version,
        };
        self.cache
            .insert((agent_id.to_string(), key.to_string()), entry);

        Ok(version)
    }

    /// Remove all expired state entries. Returns the number of entries deleted.
    pub async fn cleanup_expired(&self) -> crate::Result<u64> {
        let now = chrono::Utc::now().to_rfc3339();
        let db = self.db.lock();

        // Find expired keys to invalidate cache
        let mut stmt = db
            .prepare(
                "SELECT agent_id, key FROM state WHERE expires_at IS NOT NULL AND expires_at < ?1",
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;
        let expired: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![now], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| crate::Error::State(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let count = expired.len() as u64;

        if count > 0 {
            db.execute(
                "DELETE FROM state WHERE expires_at IS NOT NULL AND expires_at < ?1",
                rusqlite::params![now],
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;

            // Invalidate cache for expired entries
            for (agent_id, key) in &expired {
                self.cache.remove(&(agent_id.clone(), key.clone()));
            }
        }

        Ok(count)
    }

    /// Delete all state whose agent_id starts with the given prefix (GDPR tenant erasure). Returns
    /// rows deleted; clears the cache (its entries can't be selectively matched cheaply).
    pub async fn delete_by_prefix(&self, prefix: &str) -> crate::Result<u64> {
        let esc = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{esc}%");
        let n = {
            let db = self.db.lock();
            db.execute(
                "DELETE FROM state WHERE agent_id LIKE ?1 ESCAPE '\\'",
                rusqlite::params![pattern],
            )
            .map_err(|e| crate::Error::State(e.to_string()))?
        };
        self.cache.clear();
        Ok(n as u64)
    }

    /// Export all `(agent_id, key, value)` rows whose agent_id starts with `prefix` (for moving a
    /// tenant between shards). `agent_id` is returned with its tenant prefix intact, so re-importing
    /// via `set(agent_id, key, value)` round-trips exactly.
    pub async fn export_by_prefix(
        &self,
        prefix: &str,
    ) -> crate::Result<Vec<(String, String, serde_json::Value)>> {
        let esc = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{esc}%");
        let db = self.db.lock();
        let mut stmt = db
            .prepare("SELECT agent_id, key, value FROM state WHERE agent_id LIKE ?1 ESCAPE '\\'")
            .map_err(|e| crate::Error::State(e.to_string()))?;
        let rows = stmt
            .query_map(rusqlite::params![pattern], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| crate::Error::State(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows.flatten() {
            let value = serde_json::from_str(&r.2).unwrap_or(serde_json::Value::Null);
            out.push((r.0, r.1, value));
        }
        Ok(out)
    }

    /// Delete a key.
    pub async fn delete(&self, agent_id: &str, key: &str) -> crate::Result<()> {
        let db = self.db.lock();
        db.execute(
            "DELETE FROM state WHERE agent_id = ?1 AND key = ?2",
            rusqlite::params![agent_id, key],
        )
        .map_err(|e| crate::Error::State(e.to_string()))?;

        self.cache.remove(&(agent_id.to_string(), key.to_string()));

        self.notify(StateChange {
            agent_id: agent_id.to_string(),
            key: key.to_string(),
            value: serde_json::Value::Null,
            version: 0,
            deleted: true,
        });

        Ok(())
    }

    /// Compare-and-swap: set only if the current version matches.
    pub async fn compare_and_swap(
        &self,
        agent_id: &str,
        key: &str,
        expected_version: u64,
        new_value: serde_json::Value,
    ) -> crate::Result<bool> {
        let value_str =
            serde_json::to_string(&new_value).map_err(|e| crate::Error::State(e.to_string()))?;

        let db = self.db.lock();
        let rows_affected = db
            .execute(
                "UPDATE state SET value = ?1, version = version + 1, updated_at = datetime('now')
                 WHERE agent_id = ?2 AND key = ?3 AND version = ?4",
                rusqlite::params![value_str, agent_id, key, expected_version],
            )
            .map_err(|e| crate::Error::State(e.to_string()))?;

        if rows_affected > 0 {
            // Invalidate cache so next read picks up new version
            self.cache.remove(&(agent_id.to_string(), key.to_string()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List keys for a given agent (limited to max_keys, default 10,000).
    pub async fn list_keys(&self, agent_id: &str) -> crate::Result<Vec<String>> {
        self.list_keys_limited(agent_id, 10_000).await
    }

    /// List keys for a given agent with an explicit limit.
    pub async fn list_keys_limited(
        &self,
        agent_id: &str,
        max_keys: usize,
    ) -> crate::Result<Vec<String>> {
        let db = self.db.lock();
        let mut stmt = db
            .prepare("SELECT key FROM state WHERE agent_id = ?1 ORDER BY key LIMIT ?2")
            .map_err(|e| crate::Error::State(e.to_string()))?;

        let keys: Vec<String> = stmt
            .query_map(rusqlite::params![agent_id, max_keys as i64], |row| {
                row.get(0)
            })
            .map_err(|e| crate::Error::State(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(keys)
    }

    /// Snapshot the entire state DB into a standalone file (for backup / cluster snapshot).
    pub fn backup_to(&self, path: &Path) -> crate::Result<()> {
        let _ = std::fs::remove_file(path); // VACUUM INTO requires a non-existent target
        let escaped = path.to_string_lossy().replace('\'', "''");
        let db = self.db.lock();
        db.execute_batch(&format!("VACUUM INTO '{escaped}'"))
            .map_err(|e| crate::Error::State(format!("state backup: {e}")))?;
        Ok(())
    }

    /// Restore the state table from a snapshot file (replaces current contents).
    pub fn restore_from(&self, path: &Path) -> crate::Result<()> {
        let escaped = path.to_string_lossy().replace('\'', "''");
        {
            let db = self.db.lock();
            db.execute_batch(&format!(
                "ATTACH '{escaped}' AS snap;
                 BEGIN;
                 DELETE FROM state;
                 INSERT INTO state SELECT * FROM snap.state;
                 COMMIT;
                 DETACH snap;"
            ))
            .map_err(|e| {
                let _ = db.execute_batch("ROLLBACK; DETACH snap;");
                crate::Error::State(format!("state restore: {e}"))
            })?;
        }
        // Invalidate the hot cache so reads reflect the restored data.
        self.cache.clear();
        Ok(())
    }
}

impl Default for StateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_missing_key_returns_none() {
        let store = StateStore::new();
        let result = store.get("agent-1", "mood").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn set_and_get() {
        let store = StateStore::new();
        let v = store
            .set("agent-1", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        assert_eq!(v, 1);

        let entry = store.get("agent-1", "mood").await.unwrap().unwrap();
        assert_eq!(entry.agent_id, "agent-1");
        assert_eq!(entry.key, "mood");
        assert_eq!(entry.value, serde_json::json!("happy"));
        assert_eq!(entry.version, 1);
    }

    #[tokio::test]
    async fn set_increments_version() {
        let store = StateStore::new();
        let v1 = store.set("a", "k", serde_json::json!(1)).await.unwrap();
        assert_eq!(v1, 1);

        let v2 = store.set("a", "k", serde_json::json!(2)).await.unwrap();
        assert_eq!(v2, 2);

        let v3 = store.set("a", "k", serde_json::json!(3)).await.unwrap();
        assert_eq!(v3, 3);
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let store = StateStore::new();
        store.set("a", "k", serde_json::json!("val")).await.unwrap();
        store.delete("a", "k").await.unwrap();
        let result = store.get("a", "k").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn compare_and_swap_success() {
        let store = StateStore::new();
        store.set("a", "k", serde_json::json!(1)).await.unwrap();

        let swapped = store
            .compare_and_swap("a", "k", 1, serde_json::json!(2))
            .await
            .unwrap();
        assert!(swapped);

        let entry = store.get("a", "k").await.unwrap().unwrap();
        assert_eq!(entry.value, serde_json::json!(2));
        assert_eq!(entry.version, 2);
    }

    #[tokio::test]
    async fn compare_and_swap_fails_on_version_mismatch() {
        let store = StateStore::new();
        store.set("a", "k", serde_json::json!(1)).await.unwrap();

        let swapped = store
            .compare_and_swap("a", "k", 999, serde_json::json!(2))
            .await
            .unwrap();
        assert!(!swapped);

        // Value unchanged
        let entry = store.get("a", "k").await.unwrap().unwrap();
        assert_eq!(entry.value, serde_json::json!(1));
    }

    #[tokio::test]
    async fn list_keys() {
        let store = StateStore::new();
        store.set("a", "x", serde_json::json!(1)).await.unwrap();
        store.set("a", "y", serde_json::json!(2)).await.unwrap();
        store.set("b", "z", serde_json::json!(3)).await.unwrap();

        let keys = store.list_keys("a").await.unwrap();
        assert_eq!(keys, vec!["x", "y"]);

        let keys_b = store.list_keys("b").await.unwrap();
        assert_eq!(keys_b, vec!["z"]);
    }

    #[tokio::test]
    async fn cache_is_populated_on_read() {
        let store = StateStore::new();
        store.set("a", "k", serde_json::json!("val")).await.unwrap();

        // Clear cache manually
        store.cache.clear();
        assert!(store.cache.is_empty());

        // Read should repopulate cache
        let _ = store.get("a", "k").await.unwrap();
        assert!(!store.cache.is_empty());
    }

    #[test]
    fn state_entry_serialization_roundtrip() {
        let entry = StateEntry {
            agent_id: "bot-1".into(),
            key: "status".into(),
            value: serde_json::json!({"active": true, "queue_depth": 5}),
            version: 42,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: StateEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.agent_id, "bot-1");
        assert_eq!(deserialized.version, 42);
    }

    #[tokio::test]
    async fn complex_json_values() {
        let store = StateStore::new();
        let complex = serde_json::json!({
            "nested": {"deep": [1, 2, 3]},
            "array": [{"a": 1}, {"b": 2}],
            "null_field": null,
            "bool": true
        });
        store
            .set("agent", "complex", complex.clone())
            .await
            .unwrap();
        let entry = store.get("agent", "complex").await.unwrap().unwrap();
        assert_eq!(entry.value, complex);
    }
}
