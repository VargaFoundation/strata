//! Agentic-platform runtime substrate.
//!
//! Currently the durable **agent-run ledger** ([`store::RunStore`]): runs carry status + cursor +
//! input/result, their steps are episodic events (`session_id = run_id`). The orchestration driver
//! (agent loop, scheduler, tool gateway) builds on this.

pub mod store;

pub use store::{Run, RunPatch, RunStatus, RunStore};
