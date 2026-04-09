use crate::config::CoreConfig;
use crate::materialized::MaterializedViewManager;
use crate::memory::MemoryManager;
use crate::Result;

/// Top-level engine that owns all subsystems of the Strata context lake.
#[derive(Debug)]
pub struct StrataEngine {
    _config: CoreConfig,
    _memory: MemoryManager,
    _materialized: MaterializedViewManager,
}

impl StrataEngine {
    /// Create and initialize a new Strata engine.
    pub async fn new(config: CoreConfig) -> Result<Self> {
        // Initialize subsystems (stubs for now)
        let _memory = MemoryManager::new();
        let _materialized = MaterializedViewManager::new();

        tracing::info!("Strata engine initialized");

        Ok(Self {
            _config: config,
            _memory,
            _materialized,
        })
    }

    /// Gracefully shut down the engine.
    pub async fn shutdown(self) -> Result<()> {
        tracing::info!("Strata engine shutting down");
        Ok(())
    }
}

// Compile-time assertion: StrataEngine must be Send + Sync for Arc usage.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StrataEngine>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_lifecycle() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
        engine.shutdown().await.unwrap();
    }
}
