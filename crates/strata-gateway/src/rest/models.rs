//! Request and response DTOs for the REST API.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub source: String,
    pub events: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub ingested: u64,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default = "default_k")]
    pub k: usize,
}

fn default_k() -> usize {
    5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_serializes() {
        let resp = HealthResponse {
            status: "ok".into(),
            version: "0.1.0".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["version"], "0.1.0");
    }

    #[test]
    fn ingest_response_serializes() {
        let resp = IngestResponse { ingested: 42 };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["ingested"], 42);
    }

    #[test]
    fn query_request_deserializes() {
        let json = serde_json::json!({"sql": "SELECT 1"});
        let req: QueryRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.sql, "SELECT 1");
    }

    #[test]
    fn ingest_request_deserializes() {
        let json = serde_json::json!({
            "source": "my-app",
            "events": [{"type": "click"}, {"type": "view"}]
        });
        let req: IngestRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.source, "my-app");
        assert_eq!(req.events.len(), 2);
    }

    #[test]
    fn search_request_with_default_k() {
        let json = serde_json::json!({"query": "test query"});
        let req: SearchRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.query, "test query");
        assert_eq!(req.k, 5); // default
    }

    #[test]
    fn search_request_with_custom_k() {
        let json = serde_json::json!({"query": "test", "k": 10});
        let req: SearchRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.k, 10);
    }
}
