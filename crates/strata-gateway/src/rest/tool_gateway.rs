//! MCP tool-gateway — a governed registry of **downstream** MCP servers that Strata can call.
//!
//! Today Strata's MCP server *exposes* its own data tools; this is the other half: registering
//! external MCP servers and invoking their tools (`tools/call`) on behalf of an agent run. It is
//! the "tool catalog / tool firewall" platform primitive — calls flow through the gateway, so the
//! existing auth/RBAC/rate-limit/audit layers govern who can register and invoke tools.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// A registered downstream MCP server (Streamable-HTTP transport).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolServer {
    pub name: String,
    pub url: String,
}

/// In-memory registry of downstream MCP servers + an outbound MCP client.
pub struct ToolGateway {
    servers: RwLock<HashMap<String, String>>,
    client: reqwest::Client,
}

impl Default for ToolGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolGateway {
    pub fn new() -> Self {
        Self {
            servers: RwLock::new(HashMap::new()),
            client: reqwest::Client::new(),
        }
    }

    /// Register (or replace) a downstream MCP server by name.
    pub fn register(&self, name: impl Into<String>, url: impl Into<String>) {
        self.servers
            .write()
            .unwrap()
            .insert(name.into(), url.into());
    }

    /// List the registered servers.
    pub fn list(&self) -> Vec<ToolServer> {
        self.servers
            .read()
            .unwrap()
            .iter()
            .map(|(name, url)| ToolServer {
                name: name.clone(),
                url: url.clone(),
            })
            .collect()
    }

    fn url_of(&self, server: &str) -> Option<String> {
        self.servers.read().unwrap().get(server).cloned()
    }

    /// Invoke `tool` on a registered downstream MCP `server` via JSON-RPC `tools/call`, returning
    /// the tool's `result` (or an error string). The outbound side effect happens here (the leader),
    /// so an agent driver journals the materialized result.
    pub async fn call(
        &self,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let url = self
            .url_of(server)
            .ok_or_else(|| format!("unknown tool server: {server}"))?;
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        });
        let resp = self
            .client
            .post(format!("{}/mcp", url.trim_end_matches('/')))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("downstream MCP request failed: {e}"))?;
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("downstream MCP response parse failed: {e}"))?;
        if let Some(err) = json.get("error") {
            return Err(format!("downstream MCP error: {err}"));
        }
        Ok(json
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_list() {
        let gw = ToolGateway::new();
        gw.register("github", "http://localhost:9001");
        gw.register("search", "http://localhost:9002");
        let mut names: Vec<String> = gw.list().into_iter().map(|s| s.name).collect();
        names.sort();
        assert_eq!(names, vec!["github", "search"]);
        assert_eq!(
            gw.url_of("github").as_deref(),
            Some("http://localhost:9001")
        );
    }

    #[tokio::test]
    async fn call_unknown_server_errors() {
        let gw = ToolGateway::new();
        assert!(gw.call("nope", "do", serde_json::json!({})).await.is_err());
    }
}
