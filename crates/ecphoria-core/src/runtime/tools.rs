//! Tool-execution abstraction for the agent loop.
//!
//! The agent driver (in `ecphoria-core`) must be able to invoke **external** tools (e.g. downstream
//! MCP servers) without `ecphoria-core` depending on the protocol layer. So core defines this trait
//! and the gateway's MCP tool-gateway implements it; the gateway injects an implementation into the
//! engine via [`crate::EcphoriaEngine::set_tool_executor`].

use async_trait::async_trait;

/// Executes a named tool on a named (downstream) server, returning the tool's JSON result.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> crate::Result<serde_json::Value>;
}
