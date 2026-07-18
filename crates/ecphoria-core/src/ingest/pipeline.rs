//! Ingestion pipeline — receives events, stores them, optionally embeds.

use std::sync::Arc;

use crate::config::EmbeddingConfig;
use crate::embedding::EmbeddingProvider;
use crate::memory::episodic::{EpisodicStore, Event};
use crate::memory::semantic::{SemanticEntry, SemanticStore};

/// Pipeline that processes incoming events into the memory stores.
pub struct IngestPipeline {
    episodic: Arc<EpisodicStore>,
    semantic: Option<Arc<SemanticStore>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    batch_size: usize,
}

impl std::fmt::Debug for IngestPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestPipeline")
            .field("has_semantic", &self.semantic.is_some())
            .field("has_embedding", &self.embedding.is_some())
            .finish()
    }
}

impl IngestPipeline {
    /// Create a pipeline with only episodic storage.
    pub fn new(episodic: Arc<EpisodicStore>) -> Self {
        Self {
            episodic,
            semantic: None,
            embedding: None,
            batch_size: EmbeddingConfig::default().batch_size,
        }
    }

    /// Create a pipeline with auto-embedding support.
    pub fn with_embedding(
        episodic: Arc<EpisodicStore>,
        semantic: Arc<SemanticStore>,
        embedding: Arc<dyn EmbeddingProvider>,
        batch_size: usize,
    ) -> Self {
        Self {
            episodic,
            semantic: Some(semantic),
            embedding: Some(embedding),
            batch_size: if batch_size == 0 { 64 } else { batch_size },
        }
    }

    /// Max characters for embedding text (roughly ~512 tokens).
    const MAX_EMBED_CHARS: usize = 2048;

    /// Extract semantic content from an event for embedding.
    ///
    /// Strategy: extract human-readable values from the payload JSON,
    /// excluding high-cardinality IDs and numeric-only fields.
    /// Metadata (source, event_type) is NOT included in the embedding text —
    /// it's stored separately for pre-filter search.
    fn extract_embedding_text(event: &Event) -> String {
        let mut parts = Vec::new();

        // Include event_type as a semantic signal (it's descriptive)
        parts.push(event.event_type.replace('.', " "));

        // Extract string values from payload (skip IDs, numbers-only)
        if let Some(obj) = event.payload.as_object() {
            for (key, value) in obj {
                // Skip internal fields (e.g. `_tenant_id`) so they don't pollute embeddings.
                if key.starts_with('_') {
                    continue;
                }
                match value {
                    serde_json::Value::String(s) => {
                        // Skip likely IDs (UUID-like, short hex, pure numbers)
                        if s.len() > 2 && !s.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
                            parts.push(format!("{key}: {s}"));
                        }
                    }
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        // Recursively extract from nested structures (truncated)
                        let nested = serde_json::to_string(value).unwrap_or_default();
                        if nested.len() < 500 {
                            parts.push(nested);
                        }
                    }
                    _ => {}
                }
            }
        } else {
            // Non-object payload — use as-is
            parts.push(serde_json::to_string(&event.payload).unwrap_or_default());
        }

        // Include tags as semantic content
        if !event.tags.is_empty() {
            parts.push(event.tags.join(" "));
        }

        let text = parts.join(". ");
        // Truncate to max embedding length
        if text.len() > Self::MAX_EMBED_CHARS {
            text[..Self::MAX_EMBED_CHARS].to_string()
        } else {
            text
        }
    }

    /// Ingest a batch of events.
    ///
    /// 1. Append all events to episodic store
    /// 2. If embedding provider is configured, embed event payloads and upsert to semantic store
    pub async fn ingest(&self, events: Vec<Event>) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        // Step 1: Store in episodic memory (committed).
        let count = self.episodic.append(&events).await?;
        tracing::debug!(count, "ingested events into episodic store");

        // Step 2: Best-effort embed + index. Events that fail (or have no provider) stay marked
        // `embedded = false` in episodic and are recoverable via `embed_and_index` (reindex).
        self.embed_and_index(&events).await;

        Ok(count)
    }

    /// Append events to the episodic store WITHOUT embedding — the deterministic half of ingest.
    /// Used on the Raft apply path: apply must be a pure function of the request, so it must not make
    /// a (non-deterministic, external, un-timed) embedding call that would diverge the vector index
    /// across nodes and stall the apply loop on a hung provider. Events land `embedded = false` and
    /// their vectors are (re)built locally + best-effort by the background reindex loop.
    pub async fn ingest_episodic_only(&self, events: Vec<Event>) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }
        self.episodic.append(&events).await
    }

    /// Embed events, upsert their vectors, and mark the **successful** ones `embedded = true` in
    /// episodic. No-op (returns 0) when no embedding provider is configured. This is the shared path
    /// for both initial ingest and reindex/repair of previously-unembedded events — so a transient
    /// embedding outage no longer silently loses vectors.
    pub async fn embed_and_index(&self, events: &[Event]) -> usize {
        let (Some(semantic), Some(embedding)) = (&self.semantic, &self.embedding) else {
            return 0;
        };
        let texts: Vec<String> = events.iter().map(Self::extract_embedding_text).collect();
        let paired: Vec<(&Event, &String)> = events.iter().zip(texts.iter()).collect();

        let mut succeeded: Vec<uuid::Uuid> = Vec::new();
        for chunk in paired.chunks(self.batch_size) {
            let chunk_texts: Vec<String> = chunk.iter().map(|(_, t)| (*t).clone()).collect();
            match embedding.embed_documents(&chunk_texts).await {
                Ok(embeddings) => {
                    for ((event, text), emb) in chunk.iter().zip(embeddings) {
                        let entry = SemanticEntry {
                            id: event.id,
                            content: (*text).clone(),
                            embedding: emb,
                            metadata: serde_json::json!({
                                "source": event.source,
                                "event_type": event.event_type,
                                "timestamp": event.timestamp.to_rfc3339(),
                                "trace_id": event.trace_id,
                                "tags": event.tags,
                                "tenant_id": event
                                    .payload
                                    .get("_tenant_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("default"),
                            }),
                        };
                        match semantic.upsert(&entry).await {
                            Ok(()) => succeeded.push(event.id),
                            Err(e) => tracing::warn!(error = %e, "failed to upsert semantic entry"),
                        }
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    batch_size = chunk.len(),
                    "auto-embedding batch failed; events left unembedded (recoverable via reindex)"
                ),
            }
        }

        // Mark only the events whose vectors actually landed (closes the cross-store gap visibly).
        if !succeeded.is_empty() {
            if let Err(e) = self.episodic.mark_embedded(&succeeded).await {
                tracing::warn!(error = %e, "failed to mark events embedded");
            }
        }
        succeeded.len()
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
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn ingest_empty_batch() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store);
        let count = pipeline.ingest(vec![]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn ingest_events_persisted() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store.clone());

        let count = pipeline
            .ingest(vec![make_event("app"), make_event("app")])
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn ingest_episodic_only_appends_without_embedding() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store.clone());
        let n = pipeline
            .ingest_episodic_only(vec![make_event("a"), make_event("b")])
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn ingest_multiple_batches() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store.clone());

        pipeline.ingest(vec![make_event("a")]).await.unwrap();
        pipeline.ingest(vec![make_event("b")]).await.unwrap();
        assert_eq!(store.count().await.unwrap(), 2);
    }
}
