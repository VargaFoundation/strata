//! Raft type definitions for openraft.

use serde::{Deserialize, Serialize};

/// Node identifier in the Raft cluster.
pub type NodeId = u64;

/// A log entry in the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub data: Vec<u8>,
}

/// Snapshot data for Raft state transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub data: Vec<u8>,
}
