//! Snapshot creation and transfer for Raft state machine.

/// Handles snapshot creation and transfer to peers.
pub struct SnapshotManager {
    // TODO: snapshot directory, transfer state
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}
