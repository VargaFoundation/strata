//! gRPC service implementation backed by StrataEngine.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use strata_core::StrataEngine;

use crate::grpc::proto;
use crate::grpc::proto::strata_server::Strata;

/// gRPC service implementation.
pub struct StrataGrpcService {
    engine: Arc<StrataEngine>,
}

impl StrataGrpcService {
    pub fn new(engine: Arc<StrataEngine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl Strata for StrataGrpcService {
    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        let req = request.into_inner();
        match self.engine.query_sql(&req.sql).await {
            Ok(rows) => {
                let count = rows.len() as i64;
                let rows_json: Vec<String> = rows
                    .iter()
                    .map(|r| serde_json::to_string(r).unwrap_or_default())
                    .collect();
                Ok(Response::new(proto::QueryResponse { rows_json, count }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn ingest(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestResponse>, Status> {
        let req = request.into_inner();
        let events: Vec<strata_core::memory::episodic::Event> = req
            .events_json
            .iter()
            .filter_map(|json_str| {
                let payload: serde_json::Value = serde_json::from_str(json_str).ok()?;
                Some(strata_core::memory::episodic::Event {
                    id: uuid::Uuid::new_v4(),
                    source: req.source.clone(),
                    event_type: payload
                        .get("event_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    payload,
                    timestamp: chrono::Utc::now(),
                    parent_id: None,
                    trace_id: None,
                    tags: vec![],
                    idempotency_key: None,
                })
            })
            .collect();

        match self.engine.ingest(events).await {
            Ok(count) => Ok(Response::new(proto::IngestResponse { ingested: count })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn search(
        &self,
        request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        let req = request.into_inner();

        if req.vector.is_empty() {
            return Ok(Response::new(proto::SearchResponse { results: vec![] }));
        }

        let k = if req.k == 0 { 5 } else { req.k as usize };

        match self.engine.semantic_search(&req.vector, k).await {
            Ok(results) => {
                let proto_results: Vec<proto::SearchResult> = results
                    .iter()
                    .map(|r| proto::SearchResult {
                        id: r.entry.id.to_string(),
                        content: r.entry.content.clone(),
                        score: r.score,
                        metadata_json: serde_json::to_string(&r.entry.metadata).unwrap_or_default(),
                    })
                    .collect();
                Ok(Response::new(proto::SearchResponse {
                    results: proto_results,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_state(
        &self,
        request: Request<proto::GetStateRequest>,
    ) -> Result<Response<proto::GetStateResponse>, Status> {
        let req = request.into_inner();
        match self.engine.state_get(&req.agent_id, &req.key).await {
            Ok(Some(entry)) => Ok(Response::new(proto::GetStateResponse {
                agent_id: entry.agent_id,
                key: entry.key,
                value_json: serde_json::to_string(&entry.value).unwrap_or_default(),
                version: entry.version,
                found: true,
            })),
            Ok(None) => Ok(Response::new(proto::GetStateResponse {
                agent_id: req.agent_id,
                key: req.key,
                value_json: String::new(),
                version: 0,
                found: false,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn set_state(
        &self,
        request: Request<proto::SetStateRequest>,
    ) -> Result<Response<proto::SetStateResponse>, Status> {
        let req = request.into_inner();
        let value: serde_json::Value = serde_json::from_str(&req.value_json)
            .unwrap_or(serde_json::Value::String(req.value_json));

        match self.engine.state_set(&req.agent_id, &req.key, value).await {
            Ok(version) => Ok(Response::new(proto::SetStateResponse { version })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        Ok(Response::new(proto::HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }))
    }
}

/// Handle returned by `start_grpc` to control graceful shutdown.
pub struct GrpcHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl GrpcHandle {
    /// Signal the gRPC server to begin graceful shutdown.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Start the gRPC server on the given address.
///
/// Returns a handle that can be used to trigger graceful shutdown.
pub async fn start_grpc(
    addr: &str,
    engine: Arc<StrataEngine>,
) -> Result<GrpcHandle, Box<dyn std::error::Error>> {
    let parsed_addr = addr
        .parse()
        .map_err(|e| format!("invalid gRPC address: {e}"))?;

    let service = StrataGrpcService::new(engine);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tracing::info!(%addr, "gRPC server listening");

    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(
                proto::strata_server::StrataServer::new(service)
                    .max_decoding_message_size(16 * 1024 * 1024),
            )
            .serve_with_shutdown(parsed_addr, async {
                let _ = shutdown_rx.await;
                tracing::info!("gRPC server draining");
            })
            .await
        {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    Ok(GrpcHandle { shutdown_tx })
}
