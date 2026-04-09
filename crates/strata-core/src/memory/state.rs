//! State memory store — transactional key-value with MVCC.

use serde::{Deserialize, Serialize};

/// A state entry for an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEntry {
    pub agent_id: String,
    pub key: String,
    pub value: serde_json::Value,
    pub version: u64,
}

/// Key-value store with MVCC, watchers, and TTL support.
#[derive(Debug)]
pub struct StateStore {
    // TODO: rusqlite connection, DashMap hot cache
}

impl StateStore {
    pub fn new() -> Self {
        Self {}
    }

    /// Get the current value for a key.
    pub async fn get(&self, _agent_id: &str, _key: &str) -> crate::Result<Option<StateEntry>> {
        Ok(None)
    }

    /// Set a value. Returns the new version.
    pub async fn set(
        &self,
        _agent_id: &str,
        _key: &str,
        _value: serde_json::Value,
    ) -> crate::Result<u64> {
        Ok(1)
    }

    /// Delete a key.
    pub async fn delete(&self, _agent_id: &str, _key: &str) -> crate::Result<()> {
        Ok(())
    }

    /// Compare-and-swap: set only if the current version matches.
    pub async fn compare_and_swap(
        &self,
        _agent_id: &str,
        _key: &str,
        _expected_version: u64,
        _new_value: serde_json::Value,
    ) -> crate::Result<bool> {
        Ok(false)
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
    async fn get_with_empty_agent_id() {
        let store = StateStore::new();
        let result = store.get("", "key").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn set_returns_version() {
        let store = StateStore::new();
        let version = store
            .set("agent-1", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        assert!(version > 0);
    }

    #[tokio::test]
    async fn delete_succeeds() {
        let store = StateStore::new();
        store.delete("agent-1", "mood").await.unwrap();
    }

    #[tokio::test]
    async fn compare_and_swap_returns_bool() {
        let store = StateStore::new();
        let swapped = store
            .compare_and_swap("agent-1", "counter", 0, serde_json::json!(1))
            .await
            .unwrap();
        // Stub returns false
        assert!(!swapped);
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
        assert_eq!(deserialized.key, "status");
        assert_eq!(deserialized.version, 42);
        assert_eq!(deserialized.value["active"], true);
    }

    #[test]
    fn default_trait() {
        let store = StateStore::default();
        let _ = store;
    }
}
