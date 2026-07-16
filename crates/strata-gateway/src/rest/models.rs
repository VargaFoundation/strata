//! Request and response DTOs for the REST API.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub subsystems: SubsystemHealth,
}

#[derive(Debug, Serialize)]
pub struct SubsystemHealth {
    pub episodic: SubsystemStatus,
    pub state: SubsystemStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raft: Option<SubsystemStatus>,
}

#[derive(Debug, Serialize)]
pub struct SubsystemStatus {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub source: String,
    pub events: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub ingested: u64,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub query: String,
    /// Optional pre-computed vector for direct similarity search.
    pub vector: Option<Vec<f32>>,
    #[serde(default = "default_k")]
    pub k: usize,
    /// Optional metadata filters for search refinement.
    #[serde(default)]
    pub filters: Option<SearchFilters>,
    /// Minimum similarity score threshold (0.0–1.0). Results below this are excluded.
    #[serde(default)]
    pub min_score: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub struct SearchFilters {
    pub source: Option<String>,
    pub event_type: Option<String>,
}

fn default_k() -> usize {
    5
}

/// Embed text and search in one call.
#[derive(Debug, Deserialize)]
pub struct EmbedAndSearchRequest {
    /// Natural language query text (will be embedded automatically).
    pub text: String,
    #[serde(default = "default_k")]
    pub k: usize,
    /// Optional metadata filters.
    #[serde(default)]
    pub filters: Option<SearchFilters>,
    /// Minimum similarity score threshold (0.0–1.0). Results below this are excluded.
    #[serde(default)]
    pub min_score: Option<f32>,
}

// ── Memory cognition DTOs ───────────────────────────────────────────

/// Add a memory through the cognition pipeline.
#[derive(Debug, Deserialize)]
pub struct MemoryAddRequest {
    /// The atomic fact/statement to remember.
    pub content: String,
    /// Optional stable key the memory is about (enables contradiction resolution).
    #[serde(default)]
    pub subject: Option<String>,
    /// Optional importance override (0.0–1.0).
    #[serde(default)]
    pub importance: Option<f32>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// Memory type: "semantic" (default), "episodic", or "procedural".
    #[serde(default)]
    pub mem_type: Option<String>,
}

/// Consolidate a scope's lowest-importance memories into one summary.
#[derive(Debug, Deserialize)]
pub struct MemoryConsolidateRequest {
    /// Keep this many highest-importance memories; fold the rest. Defaults to 20.
    #[serde(default)]
    pub keep: Option<usize>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Consolidate semantically-similar memories in a scope into abstractions.
#[derive(Debug, Deserialize)]
pub struct MemoryConsolidateSimilarRequest {
    /// Cosine similarity at/above which memories cluster. Defaults to 0.92.
    #[serde(default)]
    pub threshold: Option<f32>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Move a tenant's full data from the current shard to `target_shard`.
#[derive(Debug, Deserialize)]
pub struct RebalanceRequest {
    pub tenant: String,
    pub target_shard: usize,
}

/// Upsert a pre-computed multi-modal embedding (caller brings its own modality encoder).
#[derive(Debug, Deserialize)]
pub struct SemanticUpsertRequest {
    #[serde(default)]
    pub id: Option<String>,
    pub modality: String,
    pub content: String,
    pub embedding: Vec<f32>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// Vector search optionally restricted to one modality.
#[derive(Debug, Deserialize)]
pub struct ModalSearchRequest {
    pub vector: Vec<f32>,
    #[serde(default)]
    pub k: Option<usize>,
    #[serde(default)]
    pub modality: Option<String>,
}

/// Add a graph edge (entity → relation → entity).
#[derive(Debug, Deserialize)]
pub struct MemoryLinkRequest {
    pub src: String,
    pub relation: String,
    pub dst: String,
    /// For a **functional** relation: close any active edge with the same `(src, relation)` before
    /// adding this one (bi-temporal supersession), so the graph reflects only the latest value.
    #[serde(default)]
    pub supersede: bool,
}

/// Feedback on a retrieved memory (feedback loop): `helpful` | `wrong` | `obsolete`.
#[derive(Debug, Deserialize)]
pub struct MemoryFeedbackRequest {
    pub verdict: String,
}

/// Scope query for the contradiction review queue.
#[derive(Debug, Deserialize)]
pub struct ContradictionsQuery {
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Resolve a contradiction: keep `keep_id`, supersede the other active memories for `subject`.
#[derive(Debug, Deserialize)]
pub struct ResolveContradictionRequest {
    pub subject: String,
    pub keep_id: uuid::Uuid,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Create an agent/workflow run.
#[derive(Debug, Deserialize)]
pub struct CreateRunRequest {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub parent_run_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub input: serde_json::Value,
}

/// Register a downstream MCP tool server.
#[derive(Debug, Deserialize)]
pub struct RegisterToolServer {
    pub name: String,
    pub url: String,
}

/// Invoke a tool on a registered downstream MCP server.
#[derive(Debug, Deserialize)]
pub struct CallToolRequest {
    pub tool: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

/// Run an agent end-to-end (durable LLM↔tool loop).
#[derive(Debug, Deserialize)]
pub struct RunAgentRequest {
    pub agent_id: String,
    pub question: String,
    #[serde(default)]
    pub max_turns: Option<usize>,
}

/// Restore all stores from a backup directory (destructive; admin-only).
#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    /// Server-local path of a backup directory produced by `POST /admin/backup`.
    pub path: String,
}

/// Register an event trigger.
#[derive(Debug, Deserialize)]
pub struct RegisterTriggerRequest {
    pub name: String,
    #[serde(default = "wildcard")]
    pub source: String,
    #[serde(default = "wildcard")]
    pub event_type: String,
    pub agent_id: String,
}

fn wildcard() -> String {
    "*".into()
}

/// Request human approval for a run (HITL).
#[derive(Debug, Deserialize)]
pub struct RequestApprovalRequest {
    #[serde(default)]
    pub prompt: String,
}

/// Approve or reject a run awaiting approval (HITL).
#[derive(Debug, Deserialize)]
pub struct ApproveRequest {
    #[serde(default)]
    pub approve: bool,
}

/// List runs, optionally filtered by status.
#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Query a memory entity's neighborhood (1 hop by default, `depth` for multi-hop traversal).
#[derive(Debug, Deserialize)]
pub struct MemoryGraphQuery {
    pub entity: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
}

/// Search memories within a scope.
#[derive(Debug, Deserialize)]
pub struct MemorySearchRequest {
    pub query: String,
    #[serde(default = "default_k")]
    pub k: usize,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    /// Also include memories shared with this user via a grant (cross-scope read). Default false.
    #[serde(default)]
    pub shared: bool,
}

/// Grant a user read access to another user's memories (within the token's tenant).
#[derive(Debug, Deserialize)]
pub struct MemoryGrantRequest {
    /// The user who will be able to read.
    pub grantee_user_id: String,
    /// The user whose memories become readable.
    pub grantor_user_id: String,
}

/// Query params for listing a user's grants.
#[derive(Debug, Deserialize)]
pub struct GrantListParams {
    pub grantee: String,
}

/// Query parameters for listing memories in a scope (GET /api/v1/memories).
#[derive(Debug, Deserialize)]
pub struct MemoryListParams {
    #[serde(default = "default_memory_limit")]
    pub limit: usize,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
}

fn default_memory_limit() -> usize {
    50
}

/// Query parameters for the audit log endpoint.
#[derive(Debug, Deserialize)]
pub struct AuditQueryParams {
    /// ISO-8601 date/datetime to filter from (e.g. "2026-01-01").
    #[serde(default = "default_audit_since")]
    pub since: String,
    /// Optional tenant filter — return only entries for this tenant.
    #[serde(default)]
    pub tenant: Option<String>,
}

fn default_audit_since() -> String {
    // Default: last 24 hours
    (chrono::Utc::now() - chrono::Duration::hours(24))
        .format("%Y-%m-%d")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_serializes() {
        let resp = HealthResponse {
            status: "ok".into(),
            version: "0.1.0".into(),
            subsystems: SubsystemHealth {
                episodic: SubsystemStatus {
                    status: "ok".into(),
                },
                state: SubsystemStatus {
                    status: "ok".into(),
                },
                raft: None,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["version"], "0.1.0");
        assert_eq!(json["subsystems"]["episodic"]["status"], "ok");
        assert_eq!(json["subsystems"]["state"]["status"], "ok");
        assert!(json["subsystems"]["raft"].is_null());
    }

    #[test]
    fn health_response_with_raft() {
        let resp = HealthResponse {
            status: "degraded".into(),
            version: "0.1.0".into(),
            subsystems: SubsystemHealth {
                episodic: SubsystemStatus {
                    status: "ok".into(),
                },
                state: SubsystemStatus {
                    status: "down".into(),
                },
                raft: Some(SubsystemStatus {
                    status: "ok".into(),
                }),
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "degraded");
        assert_eq!(json["subsystems"]["state"]["status"], "down");
        assert_eq!(json["subsystems"]["raft"]["status"], "ok");
    }

    #[test]
    fn ingest_response_serializes() {
        let resp = IngestResponse { ingested: 42 };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["ingested"], 42);
    }

    #[test]
    fn query_request_deserializes() {
        let json = serde_json::json!({"sql": "SELECT 1"});
        let req: QueryRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.sql, "SELECT 1");
    }

    #[test]
    fn ingest_request_deserializes() {
        let json = serde_json::json!({
            "source": "my-app",
            "events": [{"type": "click"}, {"type": "view"}]
        });
        let req: IngestRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.source, "my-app");
        assert_eq!(req.events.len(), 2);
    }

    #[test]
    fn search_request_with_default_k() {
        let json = serde_json::json!({"query": "test query"});
        let req: SearchRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.query, "test query");
        assert_eq!(req.k, 5); // default
    }

    #[test]
    fn search_request_with_custom_k() {
        let json = serde_json::json!({"query": "test", "k": 10});
        let req: SearchRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.k, 10);
    }
}
