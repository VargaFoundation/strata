//! Raft type definitions for openraft integration.

use std::io::Cursor;

use ecphoria_core::memory::episodic::Event;
use serde::{Deserialize, Serialize};

/// Node identifier in the Raft cluster.
pub type NodeId = u64;

/// Application-level request data sent through Raft consensus (MessagePack on the wire).
///
/// IMPORTANT — apply MUST be deterministic: applying the same committed entry on every node
/// must produce identical state. So requests carry **fully materialized** values (ids,
/// timestamps, and any non-deterministic results computed once on the leader at propose time),
/// never "commands" that would re-run non-deterministic logic (uuid/now/LLM) at apply time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppRequest {
    /// Ingest fully-formed events (ids + timestamps assigned on the leader), so every node
    /// applies an identical, deterministic result. `tenant` scopes the ingest (None = default).
    Ingest {
        events: Vec<Event>,
        #[serde(default)]
        tenant: Option<String>,
    },
    /// Set agent state (tenant-scoped; version is computed deterministically from prior state).
    StateSet {
        agent_id: String,
        key: String,
        value: serde_json::Value,
        #[serde(default)]
        tenant: Option<String>,
    },
    /// Delete agent state (tenant-scoped).
    StateDelete {
        agent_id: String,
        key: String,
        #[serde(default)]
        tenant: Option<String>,
    },
    /// Upsert a semantic entry (pre-embedded).
    SemanticUpsert {
        id: uuid::Uuid,
        content: String,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    },
    /// Delete a semantic entry.
    SemanticDelete { id: uuid::Uuid },
    /// Replace materialized memory rows (the leader runs the non-deterministic cognition —
    /// dedup / contradiction / LLM extraction — and proposes the resulting rows + embeddings so
    /// every node applies an identical result). Supersession is captured as upserts of the
    /// affected rows.
    MemoryUpsert {
        rows: Vec<ecphoria_core::memory::cognition::MemoryRow>,
    },
    /// Delete a memory by id (deterministic).
    MemoryDelete { id: uuid::Uuid },
    /// Add a graph edge (the leader generates the edge id so every node applies an identical row).
    GraphAddEdge {
        #[serde(default)]
        tenant: Option<String>,
        edge: ecphoria_core::memory::cognition::Edge,
    },
    /// Close all active edges matching `(tenant, src, relation)` as of `at`, marking them
    /// superseded (the leader supplies `at`/`by` so every node applies the identical close — used
    /// for functional-relation supersession, proposed just before the replacing `GraphAddEdge`).
    GraphSupersede {
        #[serde(default)]
        tenant: Option<String>,
        src: String,
        relation: String,
        at: chrono::DateTime<chrono::Utc>,
        #[serde(default)]
        by: Option<uuid::Uuid>,
    },
    /// Expire memories by id (bi-temporal soft-delete). Used to replicate consolidation: the leader
    /// proposes a `MemoryUpsert` of the summary plus this to retire the folded originals.
    MemoryExpire { ids: Vec<uuid::Uuid> },
    /// Create an agent run (the leader materializes id + timestamps; every node persists the
    /// identical row — the agentic-platform run ledger, replicated for HA).
    RunCreate { run: ecphoria_core::runtime::Run },
    /// Patch an agent run with a leader-supplied `updated_at` (deterministic apply).
    RunUpdate {
        id: uuid::Uuid,
        patch: ecphoria_core::runtime::RunPatch,
        updated_at: chrono::DateTime<chrono::Utc>,
    },
}

/// Application-level response from applying a Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppResponse {
    /// Number of events ingested.
    Ingested(u64),
    /// New version of state entry.
    StateVersion(u64),
    /// State deleted.
    Deleted,
    /// Semantic entry upserted/deleted.
    Ok,
    /// Number of memories affected by a memory operation.
    MemoryCount(u64),
}

/// Cluster node info for openraft membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// HTTP address for Raft RPC (e.g., "http://10.0.0.1:9433").
    pub addr: String,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

// NodeInfo automatically implements openraft::Node via blanket impl
// (requires: Sized + Send + Sync + Eq + Debug + Clone + Default + Serialize + Deserialize)

// Use the openraft macro to declare the type configuration.
openraft::declare_raft_types!(
    /// Ecphoria's Raft type configuration.
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeInfo,
        Entry = openraft::Entry<Self>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<Self>,
);

/// Snapshot data for Raft state transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_request_roundtrip() {
        let req = AppRequest::Ingest {
            events: vec![Event::new(
                "test",
                "click",
                serde_json::json!({"key": "val"}),
            )],
            tenant: None,
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: AppRequest = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            AppRequest::Ingest { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].source, "test");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn memory_request_roundtrip() {
        let mem = ecphoria_core::memory::cognition::Memory::new(
            ecphoria_core::memory::cognition::MemoryScope::user("alice"),
            "likes tea",
        );
        let req = AppRequest::MemoryUpsert {
            rows: vec![ecphoria_core::memory::cognition::MemoryRow {
                memory: mem,
                embedding: Some(vec![0.1, 0.2]),
            }],
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: AppRequest = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            AppRequest::MemoryUpsert { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].memory.content, "likes tea");
                assert_eq!(rows[0].embedding.as_deref(), Some(&[0.1, 0.2][..]));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn run_request_roundtrip_with_none_fields() {
        // Regression: Run/RunPatch must round-trip through MessagePack (positional encoding) even
        // with None optionals. `skip_serializing_if` would omit fields and misalign the positional
        // decoder over the real gRPC transport ("invalid type: string \"pending\", expected a 16
        // byte array") — only caught by serialization, not by in-process multi-node tests.
        use ecphoria_core::runtime::{Run, RunPatch, RunStatus};
        let now = chrono::Utc::now();
        let run = Run {
            id: uuid::Uuid::new_v4(),
            tenant_id: "default".into(),
            agent_id: None,
            parent_run_id: None,
            status: RunStatus::Pending,
            input: serde_json::json!({}),
            result: serde_json::Value::Null,
            error: None,
            cursor: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
            started_at: None,
            ended_at: None,
        };
        let bytes = rmp_serde::to_vec(&AppRequest::RunCreate { run }).unwrap();
        match rmp_serde::from_slice::<AppRequest>(&bytes).unwrap() {
            AppRequest::RunCreate { run } => {
                assert_eq!(run.status, RunStatus::Pending);
                assert!(run.agent_id.is_none());
                assert!(run.parent_run_id.is_none());
            }
            _ => panic!("wrong variant"),
        }

        let req = AppRequest::RunUpdate {
            id: uuid::Uuid::new_v4(),
            patch: RunPatch {
                status: Some(RunStatus::Succeeded),
                ..Default::default()
            },
            updated_at: now,
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        assert!(matches!(
            rmp_serde::from_slice::<AppRequest>(&bytes).unwrap(),
            AppRequest::RunUpdate { .. }
        ));
    }

    #[test]
    fn ingest_step_event_with_trace_roundtrips() {
        // Agent driver step events have parent_id=None but trace_id=Some(run_id) — the exact
        // positional-misalignment case (a string trace_id read where a 16-byte parent_id Uuid is
        // expected). Must round-trip through the transport's MessagePack codec.
        let mut ev = Event::new("agent", "llm_answer", serde_json::json!({ "answer": "hi" }));
        ev.trace_id = Some("run-123".to_string());
        let req = AppRequest::Ingest {
            events: vec![ev],
            tenant: None,
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        match rmp_serde::from_slice::<AppRequest>(&bytes).unwrap() {
            AppRequest::Ingest { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].trace_id.as_deref(), Some("run-123"));
                assert!(events[0].parent_id.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn app_response_roundtrip() {
        let resp = AppResponse::Ingested(42);
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: AppResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            AppResponse::Ingested(n) => assert_eq!(n, 42),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn node_info_display() {
        let node = NodeInfo {
            addr: "http://10.0.0.1:9433".into(),
        };
        assert_eq!(format!("{node}"), "http://10.0.0.1:9433");
    }

    /// Justifies the gRPC/MessagePack migration: the binary wire encoding is materially smaller
    /// than JSON on embedding-heavy AppendEntries payloads (the dominant replication traffic).
    #[test]
    fn messagepack_is_smaller_than_json_for_embeddings() {
        use ecphoria_core::memory::cognition::{Memory, MemoryRow, MemoryScope};
        // 100 memory rows, each with a 768-dim embedding (≈ a full AppendEntries batch).
        let rows: Vec<MemoryRow> = (0..100)
            .map(|i| MemoryRow {
                memory: Memory::new(MemoryScope::user("u"), format!("fact number {i}")),
                embedding: Some(vec![0.123_456_f32; 768]),
            })
            .collect();
        let req = AppRequest::MemoryUpsert { rows };

        let json = serde_json::to_vec(&req).unwrap();
        let mp = rmp_serde::to_vec(&req).unwrap();
        println!(
            "embedding-heavy MemoryUpsert: JSON={} B, MessagePack={} B ({:.2}x smaller)",
            json.len(),
            mp.len(),
            json.len() as f64 / mp.len() as f64
        );
        // Conservatively require MessagePack < 80% of JSON (observed ~1.7x smaller).
        assert!(
            (mp.len() as f64) < (json.len() as f64) * 0.8,
            "MessagePack ({}) not materially smaller than JSON ({})",
            mp.len(),
            json.len()
        );
    }
}
