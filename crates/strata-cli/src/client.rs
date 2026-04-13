//! HTTP client wrapper for communicating with strata-server.

use reqwest::Client;

/// HTTP client for the Strata REST API.
pub struct StrataClient {
    http: Client,
    base_url: String,
}

impl StrataClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// GET /health
    pub async fn health(&self) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/health", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// POST /api/v1/query
    pub async fn query(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/api/v1/query", self.base_url))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// POST /api/v1/ingest
    pub async fn ingest(&self, source: &str, file: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/api/v1/ingest", self.base_url))
            .json(&serde_json::json!({ "source": source, "file": file }))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// GET /api/v1/schema/sources
    pub async fn schema_sources(&self) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/api/v1/schema/sources", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// GET /api/v1/schema/agents
    pub async fn schema_agents(&self) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/api/v1/schema/agents", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// POST /api/v1/embed-and-search
    pub async fn search(&self, text: &str, k: usize) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/api/v1/embed-and-search", self.base_url))
            .json(&serde_json::json!({ "text": text, "k": k }))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
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
        let client = StrataClient::new("http://localhost:8432/");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_preserves_url_without_slash() {
        let client = StrataClient::new("http://localhost:8432");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_trims_multiple_trailing_slashes() {
        // trim_end_matches removes all trailing matches
        let client = StrataClient::new("http://localhost:8432///");
        assert_eq!(client.base_url(), "http://localhost:8432");
    }

    #[test]
    fn client_with_custom_port() {
        let client = StrataClient::new("http://10.0.0.1:9999");
        assert_eq!(client.base_url(), "http://10.0.0.1:9999");
    }
}
