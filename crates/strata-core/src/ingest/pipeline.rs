//! Ingestion pipeline — receives events, stores them, triggers embedding.

use crate::memory::episodic::Event;

/// Pipeline that processes incoming events into the memory stores.
pub struct IngestPipeline {
    // TODO: references to EpisodicStore, EmbeddingProvider
}

impl IngestPipeline {
    pub fn new() -> Self {
        Self {}
    }

    /// Ingest a batch of events.
    pub async fn ingest(&self, _events: Vec<Event>) -> crate::Result<u64> {
        // TODO: validate, store in episodic, auto-embed, update semantic
        Ok(0)
    }
}

impl Default for IngestPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event(source: &str) -> Event {
        Event {
            id: Uuid::new_v4(),
            source: source.into(),
            event_type: "test.event".into(),
            payload: serde_json::json!({"data": 1}),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn ingest_empty_batch() {
        let pipeline = IngestPipeline::new();
        let count = pipeline.ingest(vec![]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn ingest_single_event() {
        let pipeline = IngestPipeline::new();
        let count = pipeline.ingest(vec![make_event("app")]).await.unwrap();
        assert_eq!(count, 0); // Stub returns 0
    }

    #[tokio::test]
    async fn ingest_multiple_events() {
        let pipeline = IngestPipeline::new();
        let events: Vec<Event> = (0..100).map(|_| make_event("load-test")).collect();
        let count = pipeline.ingest(events).await.unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn default_trait() {
        let pipeline = IngestPipeline::default();
        let _ = pipeline;
    }
}
