//! Raft log storage and state machine implementations.

/// In-memory Raft log store (will be replaced with persistent storage).
pub struct RaftLogStore {
    // TODO: persistent log storage
}

impl RaftLogStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RaftLogStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Raft state machine — applies committed log entries to the engine.
pub struct RaftStateMachine {
    // TODO: reference to StrataEngine
}

impl RaftStateMachine {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RaftStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stores() {
        let _log = RaftLogStore::new();
        let _sm = RaftStateMachine::new();
    }
}
