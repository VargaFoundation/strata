//! Cluster coordinator — leader election awareness and request routing.

use super::config::ClusterConfig;

/// Coordinates cluster membership and routes requests to the leader.
pub struct ClusterCoordinator {
    _config: ClusterConfig,
}

impl ClusterCoordinator {
    pub fn new(config: ClusterConfig) -> Self {
        Self { _config: config }
    }

    /// Whether this node is the current Raft leader.
    pub fn is_leader(&self) -> bool {
        // Single-node mode: always leader
        true
    }

    /// Get the current leader's node ID.
    pub fn leader_id(&self) -> Option<u64> {
        Some(self._config.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_is_leader() {
        let coord = ClusterCoordinator::new(ClusterConfig::default());
        assert!(coord.is_leader());
    }
}
