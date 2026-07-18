//! HTTP client wrapper for communicating with ecphoria-server.

use reqwest::{Client, RequestBuilder};

/// HTTP client for the Ecphoria REST API. Attaches a `Bearer` token (from `ECPHORIA_TOKEN`) when set —
/// required for admin routes on an auth-enabled server.
pub struct EcphoriaClient {
    http: Client,
    base_url: String,
    token: Option<String>,
}

impl EcphoriaClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            token: std::env::var("ECPHORIA_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }

    fn auth(&self, rb: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    /// GET `<path>` → JSON.
    pub async fn get_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        Ok(self.auth(self.http.get(url)).send().await?.json().await?)
    }

    /// POST `<path>` with a JSON body → JSON.
    pub async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        Ok(self
            .auth(self.http.post(url).json(&body))
            .send()
            .await?
            .json()
            .await?)
    }

    /// PUT `<path>` with a JSON body → JSON.
    pub async fn put_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        Ok(self
            .auth(self.http.put(url).json(&body))
            .send()
            .await?
            .json()
            .await?)
    }

    /// DELETE `<path>` → JSON.
    pub async fn delete_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        Ok(self
            .auth(self.http.delete(url))
            .send()
            .await?
            .json()
            .await?)
    }

    // ── Convenience wrappers (token-aware via the helpers above) ──────────────

    pub async fn health(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/health").await
    }

    pub async fn query(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        self.post_json("/api/v1/query", serde_json::json!({ "sql": sql }))
            .await
    }

    pub async fn ingest(&self, source: &str, file: &str) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/api/v1/ingest",
            serde_json::json!({ "source": source, "file": file }),
        )
        .await
    }

    pub async fn schema_sources(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/api/v1/schema/sources").await
    }

    pub async fn schema_agents(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/api/v1/schema/agents").await
    }

    pub async fn search(&self, text: &str, k: usize) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/api/v1/embed-and-search",
            serde_json::json!({ "text": text, "k": k }),
        )
        .await
    }

    /// Return the base URL (for testing/display).
    #[cfg(test)]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_trims_trailing_slash() {
        let client = EcphoriaClient::new("http://localhost:8432/");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_preserves_url_without_slash() {
        let client = EcphoriaClient::new("http://localhost:8432");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_trims_multiple_trailing_slashes() {
        // trim_end_matches removes all trailing matches
        let client = EcphoriaClient::new("http://localhost:8432///");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_with_custom_port() {
        let client = EcphoriaClient::new("http://10.0.0.1:9999");
        assert_eq!(client.base_url(), "http://10.0.0.1:9999");
    }
}
