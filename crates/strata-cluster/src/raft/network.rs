//! Raft network implementation — HTTP-based node-to-node communication.

/// HTTP-based Raft network transport.
pub struct RaftNetwork {
    // TODO: reqwest client, peer addresses
}

impl RaftNetwork {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RaftNetwork {
    fn default() -> Self {
        Self::new()
    }
}
