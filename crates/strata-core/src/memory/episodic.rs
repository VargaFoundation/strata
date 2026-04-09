//! Episodic memory store — WAL-based append-only event storage.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single episodic event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

/// Append-only event store backed by a write-ahead log.
#[derive(Debug)]
pub struct EpisodicStore {
    // TODO: WAL file handle, DuckDB connection
}

impl EpisodicStore {
    pub fn new() -> Self {
        Self {}
    }

    /// Append events to the episodic store.
    pub async fn append(&self, _events: &[Event]) -> crate::Result<()> {
        // TODO: write to WAL, index in DuckDB
        Ok(())
    }

    /// Query events within a time range.
    pub async fn query_time_range(
        &self,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> crate::Result<Vec<Event>> {
        // TODO: query DuckDB
        Ok(vec![])
    }

    /// Return the total number of stored events.
    pub async fn count(&self) -> crate::Result<u64> {
        Ok(0)
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
        }
    }

    #[tokio::test]
    async fn new_store_has_zero_count() {
        let store = EpisodicStore::new();
        assert_eq!(store.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn append_empty_batch_succeeds() {
        let store = EpisodicStore::new();
        store.append(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn append_single_event_succeeds() {
        let store = EpisodicStore::new();
        let event = make_event("test-app", "user.signup");
        store.append(&[event]).await.unwrap();
    }

    #[tokio::test]
    async fn append_multiple_events_succeeds() {
        let store = EpisodicStore::new();
        let events: Vec<Event> = (0..10)
            .map(|i| make_event("test-app", &format!("event.{i}")))
            .collect();
        store.append(&events).await.unwrap();
    }

    #[tokio::test]
    async fn query_time_range_returns_empty() {
        let store = EpisodicStore::new();
        let start = Utc::now() - chrono::Duration::hours(1);
        let end = Utc::now();
        let results = store.query_time_range(start, end).await.unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = make_event("my-app", "order.placed");
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, event.id);
        assert_eq!(deserialized.source, "my-app");
        assert_eq!(deserialized.event_type, "order.placed");
    }

    #[test]
    fn event_clone() {
        let event = make_event("src", "type");
        let cloned = event.clone();
        assert_eq!(cloned.id, event.id);
        assert_eq!(cloned.source, event.source);
    }

    #[test]
    fn default_trait() {
        let store = EpisodicStore::default();
        // Should not panic
        let _ = store;
    }
}
