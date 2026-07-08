//! Agent-run ledger — the durable record of agent/workflow executions (agentic-platform substrate).
//!
//! A *run* is one execution of an agent or workflow: status + cursor + input/result, with a
//! `parent_run_id` for sub-agent trees. Its *steps* (LLM calls, tool calls, …) are episodic events
//! tagged with `session_id = run_id`, so the full trace is `engine.session_recall(run_id)` — no
//! separate step storage. SQLite-backed (mirrors [`crate::memory::state::StateStore`]); writes carry
//! leader-materialized timestamps so the ledger is deterministic to replicate through Raft.

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lifecycle status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    #[default]
    Pending,
    Running,
    /// Paused awaiting a human-in-the-loop approval.
    WaitingApproval,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Pending => "pending",
            RunStatus::Running => "running",
            RunStatus::WaitingApproval => "waiting_approval",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "running" => RunStatus::Running,
            "waiting_approval" => RunStatus::WaitingApproval,
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => RunStatus::Pending,
        }
    }

    /// A terminal run no longer makes progress (a dispatcher can stop driving it).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

fn default_tenant() -> String {
    "default".into()
}

/// A durable agent/workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    #[serde(default = "default_tenant")]
    pub tenant_id: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
    pub status: RunStatus,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default)]
    pub error: Option<String>,
    /// Opaque driver position (e.g. the next workflow node) — reconstructable run state.
    #[serde(default)]
    pub cursor: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
}

/// A partial update to a run — only the `Some` fields change. Carries materialized values (the
/// `updated_at` is supplied separately by the writer) so it is deterministic to replicate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunPatch {
    #[serde(default)]
    pub status: Option<RunStatus>,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
}

/// A node in a workflow DAG: a sub-agent invocation gated on `deps` (other node ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub id: String,
    pub agent_id: String,
    pub question: String,
    #[serde(default)]
    pub deps: Vec<String>,
}

const COLS: &str = "id, tenant_id, agent_id, parent_run_id, status, input, result, error, cursor, \
                    created_at, updated_at, started_at, ended_at";

fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
}

fn row_to_run(r: &rusqlite::Row) -> rusqlite::Result<Run> {
    Ok(Run {
        id: Uuid::parse_str(&r.get::<_, String>(0)?).unwrap_or_else(|_| Uuid::nil()),
        tenant_id: r.get(1)?,
        agent_id: r.get(2)?,
        parent_run_id: r
            .get::<_, Option<String>>(3)?
            .and_then(|s| Uuid::parse_str(&s).ok()),
        status: RunStatus::from_str(&r.get::<_, String>(4)?),
        input: parse_json(&r.get::<_, String>(5)?),
        result: parse_json(&r.get::<_, String>(6)?),
        error: r.get(7)?,
        cursor: parse_json(&r.get::<_, String>(8)?),
        created_at: parse_ts(&r.get::<_, String>(9)?),
        updated_at: parse_ts(&r.get::<_, String>(10)?),
        started_at: r.get::<_, Option<String>>(11)?.as_deref().map(parse_ts),
        ended_at: r.get::<_, Option<String>>(12)?.as_deref().map(parse_ts),
    })
}

/// SQLite-backed durable store of [`Run`]s.
pub struct RunStore {
    db: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for RunStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunStore").finish()
    }
}

impl RunStore {
    pub fn open(path: &Path) -> crate::Result<Self> {
        let conn = if path.as_os_str() == ":memory:" {
            Connection::open_in_memory()
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| crate::Error::State(format!("runs mkdir: {e}")))?;
            }
            Connection::open(path)
        }
        .map_err(|e| crate::Error::State(format!("open runs db: {e}")))?;

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|e| crate::Error::State(format!("runs pragmas: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS runs (
                id            TEXT PRIMARY KEY,
                tenant_id     TEXT NOT NULL DEFAULT 'default',
                agent_id      TEXT,
                parent_run_id TEXT,
                status        TEXT NOT NULL DEFAULT 'pending',
                input         TEXT NOT NULL DEFAULT '{}',
                result        TEXT NOT NULL DEFAULT 'null',
                error         TEXT,
                cursor        TEXT NOT NULL DEFAULT 'null',
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                started_at    TEXT,
                ended_at      TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runs_tenant ON runs(tenant_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(tenant_id, status);",
        )
        .map_err(|e| crate::Error::State(format!("create runs table: {e}")))?;

        // Driver-lease columns (concurrency control). Best-effort ALTER for pre-existing DBs — each
        // runs independently so a partially-migrated DB still gets both. These columns are LOCAL to
        // the node (never replicated through Raft), unlike status/cursor.
        let _ = conn.execute("ALTER TABLE runs ADD COLUMN lease_owner TEXT", []);
        let _ = conn.execute("ALTER TABLE runs ADD COLUMN lease_expires_at TEXT", []);

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
        })
    }

    /// Atomically claim (or renew) the driver lease on a run: succeeds if the run is unleased, its
    /// lease has expired, or `owner` already holds it. Returns true iff `owner` now holds it — a
    /// concurrent driver (duplicate resume, dispatcher + manual) gets false and must not drive.
    pub async fn try_claim_lease(
        &self,
        id: Uuid,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> crate::Result<bool> {
        let db = self.db.lock();
        // RFC3339 UTC timestamps compare lexicographically, so `lease_expires_at <= now` is a valid
        // expiry test.
        let n = db
            .execute(
                "UPDATE runs SET lease_owner = ?1, lease_expires_at = ?2 \
                 WHERE id = ?3 AND (lease_owner IS NULL OR lease_owner = ?1 \
                                    OR lease_expires_at IS NULL OR lease_expires_at <= ?4)",
                rusqlite::params![
                    owner,
                    expires_at.to_rfc3339(),
                    id.to_string(),
                    now.to_rfc3339(),
                ],
            )
            .map_err(|e| crate::Error::State(format!("claim run lease: {e}")))?;
        Ok(n == 1)
    }

    /// Release the driver lease `owner` holds on a run (no-op if it holds a different owner's lease).
    pub async fn release_lease(&self, id: Uuid, owner: &str) -> crate::Result<()> {
        let db = self.db.lock();
        db.execute(
            "UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE id = ?1 AND lease_owner = ?2",
            rusqlite::params![id.to_string(), owner],
        )
        .map_err(|e| crate::Error::State(format!("release run lease: {e}")))?;
        Ok(())
    }

    pub fn new() -> Self {
        Self::open(Path::new(":memory:")).expect("in-memory run store")
    }

    /// Insert a run. Idempotent (re-applying the same id is a no-op) so Raft replay is safe.
    pub async fn create(&self, run: &Run) -> crate::Result<()> {
        let db = self.db.lock();
        db.execute(
            "INSERT INTO runs (id, tenant_id, agent_id, parent_run_id, status, input, result, \
             error, cursor, created_at, updated_at, started_at, ended_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) ON CONFLICT(id) DO NOTHING",
            rusqlite::params![
                run.id.to_string(),
                run.tenant_id,
                run.agent_id,
                run.parent_run_id.map(|i| i.to_string()),
                run.status.as_str(),
                run.input.to_string(),
                run.result.to_string(),
                run.error,
                run.cursor.to_string(),
                run.created_at.to_rfc3339(),
                run.updated_at.to_rfc3339(),
                run.started_at.map(|t| t.to_rfc3339()),
                run.ended_at.map(|t| t.to_rfc3339()),
            ],
        )
        .map_err(|e| crate::Error::State(format!("create run: {e}")))?;
        Ok(())
    }

    /// Apply a patch (only present fields change) with a writer-supplied `updated_at`. Returns
    /// whether a row was updated. `COALESCE(?, col)` keeps the existing value for absent fields, so
    /// this is a deterministic function of `(patch, updated_at)`.
    pub async fn update(
        &self,
        id: Uuid,
        patch: &RunPatch,
        updated_at: DateTime<Utc>,
    ) -> crate::Result<bool> {
        let db = self.db.lock();
        let n = db
            .execute(
                "UPDATE runs SET \
                 status = COALESCE(?1, status), \
                 result = COALESCE(?2, result), \
                 error = COALESCE(?3, error), \
                 cursor = COALESCE(?4, cursor), \
                 started_at = COALESCE(?5, started_at), \
                 ended_at = COALESCE(?6, ended_at), \
                 updated_at = ?7 \
                 WHERE id = ?8",
                rusqlite::params![
                    patch.status.map(|s| s.as_str()),
                    patch.result.as_ref().map(|v| v.to_string()),
                    patch.error,
                    patch.cursor.as_ref().map(|v| v.to_string()),
                    patch.started_at.map(|t| t.to_rfc3339()),
                    patch.ended_at.map(|t| t.to_rfc3339()),
                    updated_at.to_rfc3339(),
                    id.to_string(),
                ],
            )
            .map_err(|e| crate::Error::State(format!("update run: {e}")))?;
        Ok(n > 0)
    }

    pub async fn get(&self, id: Uuid) -> crate::Result<Option<Run>> {
        let db = self.db.lock();
        db.query_row(
            &format!("SELECT {COLS} FROM runs WHERE id = ?1"),
            rusqlite::params![id.to_string()],
            row_to_run,
        )
        .optional()
        .map_err(|e| crate::Error::State(format!("get run: {e}")))
    }

    /// List runs for a tenant (newest first), optionally filtered by status.
    pub async fn list(
        &self,
        tenant: &str,
        status: Option<RunStatus>,
        limit: usize,
    ) -> crate::Result<Vec<Run>> {
        let db = self.db.lock();
        let (sql, params): (String, Vec<Box<dyn rusqlite::ToSql>>) = match status {
            Some(s) => (
                format!(
                    "SELECT {COLS} FROM runs WHERE tenant_id = ?1 AND status = ?2 \
                     ORDER BY created_at DESC LIMIT ?3"
                ),
                vec![
                    Box::new(tenant.to_string()),
                    Box::new(s.as_str().to_string()),
                    Box::new(limit as i64),
                ],
            ),
            None => (
                format!(
                    "SELECT {COLS} FROM runs WHERE tenant_id = ?1 ORDER BY created_at DESC LIMIT ?2"
                ),
                vec![Box::new(tenant.to_string()), Box::new(limit as i64)],
            ),
        };
        let mut stmt = db
            .prepare(&sql)
            .map_err(|e| crate::Error::State(format!("list runs: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), row_to_run)
            .map_err(|e| crate::Error::State(format!("list runs: {e}")))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Non-terminal runs a dispatcher can resume: `pending`/`running` and not touched since
    /// `stale_before` (so a run actively driven on the current leader — recent `updated_at` — is
    /// skipped). Oldest-stale first. RFC3339 timestamps compare lexicographically (all UTC).
    pub async fn list_resumable(
        &self,
        stale_before: DateTime<Utc>,
        limit: usize,
    ) -> crate::Result<Vec<Run>> {
        let db = self.db.lock();
        let mut stmt = db
            .prepare(&format!(
                "SELECT {COLS} FROM runs WHERE status IN ('pending','running') \
                 AND updated_at < ?1 ORDER BY updated_at ASC LIMIT ?2"
            ))
            .map_err(|e| crate::Error::State(format!("list resumable: {e}")))?;
        let rows = stmt
            .query_map(
                rusqlite::params![stale_before.to_rfc3339(), limit as i64],
                row_to_run,
            )
            .map_err(|e| crate::Error::State(format!("list resumable: {e}")))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}

impl Default for RunStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_run(id: Uuid) -> Run {
        let now = Utc::now();
        Run {
            id,
            tenant_id: "default".into(),
            agent_id: None,
            parent_run_id: None,
            status: RunStatus::Running,
            input: serde_json::Value::Null,
            result: serde_json::Value::Null,
            error: None,
            cursor: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
            started_at: None,
            ended_at: None,
        }
    }

    #[tokio::test]
    async fn lease_is_exclusive_until_expiry_or_release() {
        let store = RunStore::new();
        let id = Uuid::new_v4();
        store.create(&make_run(id)).await.unwrap();
        let now = Utc::now();
        let far = now + chrono::Duration::seconds(300);

        // A claims the lease; B cannot while it is valid; A can renew its own.
        assert!(store.try_claim_lease(id, "A", now, far).await.unwrap());
        assert!(!store.try_claim_lease(id, "B", now, far).await.unwrap());
        assert!(store.try_claim_lease(id, "A", now, far).await.unwrap());

        // Once A's lease has expired, B can claim; A can no longer renew.
        let later = far + chrono::Duration::seconds(1);
        let b_exp = later + chrono::Duration::seconds(300);
        assert!(store.try_claim_lease(id, "B", later, b_exp).await.unwrap());
        assert!(!store.try_claim_lease(id, "A", later, b_exp).await.unwrap());

        // B releases → A can claim again.
        store.release_lease(id, "B").await.unwrap();
        assert!(store.try_claim_lease(id, "A", later, b_exp).await.unwrap());
    }
}
