//! Agentic-platform runtime substrate.
//!
//! Currently the durable **agent-run ledger** ([`store::RunStore`]): runs carry status + cursor +
//! input/result, their steps are episodic events (`session_id = run_id`). The orchestration driver
//! (agent loop, scheduler, tool gateway) builds on this.

pub mod replicate;
pub mod store;
pub mod tools;

pub use replicate::RunReplicator;
pub use store::{Run, RunPatch, RunStatus, RunStore, WorkflowNode};
pub use tools::ToolExecutor;
