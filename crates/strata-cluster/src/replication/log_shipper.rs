//! WAL segment shipping to follower nodes.

/// Ships WAL segments from leader to followers.
pub struct LogShipper {
    // TODO: peer connections, shipping state
}

impl LogShipper {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for LogShipper {
    fn default() -> Self {
        Self::new()
    }
}
