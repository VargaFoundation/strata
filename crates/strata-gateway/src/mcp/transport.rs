//! MCP Streamable HTTP transport — SSE streaming via axum.

/// MCP transport server using Server-Sent Events.
pub struct McpTransport {
    // TODO: SSE channel, session management
}

impl McpTransport {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for McpTransport {
    fn default() -> Self {
        Self::new()
    }
}
