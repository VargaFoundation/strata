//! PostgreSQL wire protocol handler — implements pgwire query handlers.

/// Handles PostgreSQL simple and extended query protocol messages.
pub struct PgWireHandler {
    // TODO: reference to StrataEngine, query executor
}

impl PgWireHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for PgWireHandler {
    fn default() -> Self {
        Self::new()
    }
}
