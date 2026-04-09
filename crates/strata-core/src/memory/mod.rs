pub mod episodic;
pub mod semantic;
pub mod state;

/// Coordinates access across all three memory subsystems.
#[derive(Debug)]
pub struct MemoryManager {
    _episodic: episodic::EpisodicStore,
    _semantic: semantic::SemanticStore,
    _state: state::StateStore,
}

impl MemoryManager {
    pub fn new() -> Self {
        Self {
            _episodic: episodic::EpisodicStore::new(),
            _semantic: semantic::SemanticStore::new(),
            _state: state::StateStore::new(),
        }
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::new()
    }
}
