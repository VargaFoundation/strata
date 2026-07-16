//! MCP tool-gateway — a governed registry of **downstream** MCP servers that Strata can call.
//!
//! Today Strata's MCP server *exposes* its own data tools; this is the other half: registering
//! external MCP servers and invoking their tools (`tools/call`) on behalf of an agent run. It is
//! the "tool catalog / tool firewall" platform primitive — calls flow through the gateway, so the
//! existing auth/RBAC/rate-limit/audit layers govern who can register and invoke tools.

use std::collections::HashMap;
use std::net::IpAddr;
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
    /// When false (default), outbound tool calls to loopback/private/CGNAT/ULA addresses are
    /// rejected — an SSRF guard so a writer can't turn the gateway into a proxy into the cluster's
    /// internal network. Link-local (incl. the 169.254.169.254 cloud-metadata endpoint),
    /// unspecified and multicast targets are ALWAYS rejected regardless of this flag.
    allow_private_networks: bool,
}

impl Default for ToolGateway {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ToolGateway {
    pub fn new(allow_private_networks: bool) -> Self {
        Self {
            servers: RwLock::new(HashMap::new()),
            // Do not follow redirects: a 3xx to an internal address would bypass the pre-call
            // SSRF check performed against the registered URL.
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            allow_private_networks,
        }
    }

    /// Register (or replace) a downstream MCP server by name.
    ///
    /// Validates the URL shape here (scheme must be http/https, host required) so a misconfigured
    /// server fails fast and visibly. The network-level SSRF check (DNS resolution + address-range
    /// filtering) runs at call time in [`Self::call`] — that is the security-critical boundary and
    /// re-checking there also defends against DNS rebinding.
    pub fn register(&self, name: impl Into<String>, url: impl Into<String>) -> Result<(), String> {
        let url = url.into();
        validate_url_shape(&url)?;
        self.servers.write().unwrap().insert(name.into(), url);
        Ok(())
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
        // SSRF guard: resolve the host and reject blocked address ranges BEFORE issuing the
        // request (re-checked here, not just at register, to defend against DNS rebinding).
        validate_outbound_url(&url, self.allow_private_networks).await?;
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

/// Validate the *shape* of a downstream URL (no network I/O): scheme must be http/https and a
/// host must be present. Cheap, synchronous — used at register time for fast, visible failure.
fn validate_url_shape(url: &str) -> Result<(), String> {
    let parsed =
        reqwest::Url::parse(url).map_err(|e| format!("invalid tool server URL '{url}': {e}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "tool server URL scheme must be http(s), got '{other}'"
            ))
        }
    }
    if parsed.host().is_none() {
        return Err(format!("tool server URL '{url}' has no host"));
    }
    Ok(())
}

/// Is `ip` in a range we must never let the gateway reach?
///
/// `allow_private` relaxes the loopback/private/CGNAT/ULA rules (for local dev or trusted internal
/// deployments). Link-local (which includes the 169.254.169.254 cloud-metadata endpoint),
/// unspecified and multicast are *always* blocked — they are never a legitimate MCP target and are
/// the classic SSRF payloads.
fn is_blocked_ip(ip: IpAddr, allow_private: bool) -> bool {
    // Normalize IPv4-mapped IPv6 (::ffff:127.0.0.1) back to IPv4 so the v4 rules apply.
    let ip = match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    };
    match ip {
        IpAddr::V4(v4) => {
            // Always blocked.
            if v4.is_link_local() || v4.is_unspecified() || v4.is_multicast() || v4.is_broadcast() {
                return true;
            }
            // Carrier-grade NAT (100.64.0.0/10) — internal infra, never a public MCP host.
            let cgnat = v4.octets()[0] == 100 && (64..=127).contains(&v4.octets()[1]);
            if allow_private {
                return false;
            }
            v4.is_loopback() || v4.is_private() || cgnat
        }
        IpAddr::V6(v6) => {
            if v6.is_unspecified() || v6.is_multicast() {
                return true;
            }
            // Link-local fe80::/10 — always blocked.
            if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            if allow_private {
                return false;
            }
            // Unique-local fc00::/7.
            let ula = (v6.segments()[0] & 0xfe00) == 0xfc00;
            v6.is_loopback() || ula
        }
    }
}

/// Full outbound SSRF check: parse the URL, resolve every address the host maps to, and reject the
/// call if any resolves into a blocked range. Resolving *all* addresses (not just the first) closes
/// the gap where a name returns both a public and an internal A record.
async fn validate_outbound_url(url: &str, allow_private: bool) -> Result<(), String> {
    validate_url_shape(url)?;
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid URL: {e}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL has no host".to_string())?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "URL has no port".to_string())?;

    // If the host is an IP literal, check it directly; otherwise resolve it.
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_blocked_ip(ip, allow_private) {
            return Err(format!("blocked tool server address (SSRF guard): {ip}"));
        }
        return Ok(());
    }

    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("cannot resolve tool server host '{host}': {e}"))?;
    let mut any = false;
    for addr in addrs {
        any = true;
        if is_blocked_ip(addr.ip(), allow_private) {
            return Err(format!(
                "blocked tool server address (SSRF guard): {host} → {}",
                addr.ip()
            ));
        }
    }
    if !any {
        return Err(format!(
            "tool server host '{host}' resolved to no addresses"
        ));
    }
    Ok(())
}

/// Bridge so the agent loop (in `strata-core`) can invoke downstream MCP tools through the gateway.
#[async_trait::async_trait]
impl strata_core::runtime::ToolExecutor for ToolGateway {
    async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> strata_core::Result<serde_json::Value> {
        self.call(server, tool, arguments)
            .await
            .map_err(strata_core::Error::Ingest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_list() {
        let gw = ToolGateway::new(false);
        gw.register("github", "http://localhost:9001").unwrap();
        gw.register("search", "http://localhost:9002").unwrap();
        let mut names: Vec<String> = gw.list().into_iter().map(|s| s.name).collect();
        names.sort();
        assert_eq!(names, vec!["github", "search"]);
        assert_eq!(
            gw.url_of("github").as_deref(),
            Some("http://localhost:9001")
        );
    }

    #[test]
    fn register_rejects_bad_scheme() {
        let gw = ToolGateway::new(false);
        assert!(gw.register("evil", "file:///etc/passwd").is_err());
        assert!(gw.register("evil2", "ftp://example.com").is_err());
        assert!(gw.register("evil3", "not a url").is_err());
        assert!(gw.list().is_empty());
    }

    #[tokio::test]
    async fn call_unknown_server_errors() {
        let gw = ToolGateway::new(false);
        assert!(gw.call("nope", "do", serde_json::json!({})).await.is_err());
    }

    #[tokio::test]
    async fn call_blocks_cloud_metadata_and_loopback() {
        // The crown-jewel SSRF target and localhost must be refused without a network call.
        let gw = ToolGateway::new(false);
        gw.register("metadata", "http://169.254.169.254").unwrap();
        gw.register("local", "http://127.0.0.1:9001").unwrap();
        let e = gw
            .call("metadata", "x", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            e.contains("SSRF"),
            "metadata call should be SSRF-blocked: {e}"
        );
        let e = gw
            .call("local", "x", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            e.contains("SSRF"),
            "loopback call should be SSRF-blocked: {e}"
        );
    }

    #[tokio::test]
    async fn allow_private_still_blocks_metadata() {
        // Even in permissive mode, link-local (metadata) is never reachable; loopback is.
        let gw = ToolGateway::new(true);
        gw.register("metadata", "http://169.254.169.254").unwrap();
        let e = gw
            .call("metadata", "x", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            e.contains("SSRF"),
            "metadata must be blocked even when private is allowed: {e}"
        );
    }

    #[test]
    fn blocked_ip_ranges() {
        use std::net::Ipv4Addr;
        // Always blocked.
        assert!(is_blocked_ip(
            Ipv4Addr::new(169, 254, 169, 254).into(),
            true
        ));
        assert!(is_blocked_ip(Ipv4Addr::new(0, 0, 0, 0).into(), true));
        // Blocked only in strict mode.
        assert!(is_blocked_ip(Ipv4Addr::new(127, 0, 0, 1).into(), false));
        assert!(is_blocked_ip(Ipv4Addr::new(10, 0, 0, 5).into(), false));
        assert!(is_blocked_ip(Ipv4Addr::new(192, 168, 1, 1).into(), false));
        assert!(is_blocked_ip(Ipv4Addr::new(100, 100, 0, 1).into(), false)); // CGNAT
        assert!(!is_blocked_ip(Ipv4Addr::new(127, 0, 0, 1).into(), true));
        // Public is always fine.
        assert!(!is_blocked_ip(
            Ipv4Addr::new(93, 184, 216, 34).into(),
            false
        ));
        // IPv4-mapped IPv6 loopback normalizes and is caught.
        assert!(is_blocked_ip("::ffff:127.0.0.1".parse().unwrap(), false));
    }
}
