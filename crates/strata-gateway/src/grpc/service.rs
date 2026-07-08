//! gRPC service implementation backed by StrataEngine.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use strata_core::StrataEngine;

use crate::grpc::proto;
use crate::grpc::proto::strata_server::Strata;

/// gRPC service implementation.
pub struct StrataGrpcService {
    engine: Arc<StrataEngine>,
    /// When set, RPCs require a valid `authorization: Bearer <jwt>` and are tenant-scoped.
    auth: Option<crate::auth::middleware::AuthState>,
    /// Sharded mode (shards > 1): reject requests for tenants this shard doesn't own, pointing the
    /// caller at the owning shard — gRPC has no reverse-proxy, so this prevents serving wrong data.
    shard: Option<crate::cluster::shard_route::ShardRoutingState>,
}

impl StrataGrpcService {
    pub fn new(
        engine: Arc<StrataEngine>,
        auth: Option<crate::auth::middleware::AuthState>,
        shard: Option<crate::cluster::shard_route::ShardRoutingState>,
    ) -> Self {
        Self {
            engine,
            auth,
            shard,
        }
    }

    /// In sharded mode, verify this shard owns `tenant`; otherwise reject with the owning shard's
    /// address (the gRPC analogue of the HTTP reverse-proxy — clients reconnect to that shard).
    #[allow(clippy::result_large_err)] // Status is large but pervasive across this gRPC service.
    fn check_shard(&self, tenant: &Option<String>) -> Result<(), Status> {
        use crate::cluster::shard_route::{route_decision, ShardTarget};
        let (Some(shard), Some(t)) = (&self.shard, tenant.as_deref()) else {
            return Ok(());
        };
        match route_decision(t, &shard.router, shard.my_shard, &shard.base_urls) {
            ShardTarget::Local => Ok(()),
            ShardTarget::Forward(url) => Err(Status::failed_precondition(format!(
                "tenant '{t}' is owned by another shard — connect to its gRPC endpoint (HTTP base {url})"
            ))),
            ShardTarget::Unroutable => Err(Status::failed_precondition(format!(
                "tenant '{t}' is owned by another shard with no configured address"
            ))),
        }
    }

    /// Resolve the caller's tenant for a **read** RPC. See [`Self::resolve`].
    async fn tenant_from<T>(&self, req: &Request<T>) -> Result<Option<String>, Status> {
        self.resolve(req, false).await
    }

    /// Resolve the caller's tenant for a **mutating** RPC, additionally enforcing the RBAC role
    /// (a Reader token is rejected on writes — the gRPC analogue of the REST middleware's method
    /// check, which gRPC previously skipped).
    async fn tenant_from_write<T>(&self, req: &Request<T>) -> Result<Option<String>, Status> {
        self.resolve(req, true).await
    }

    /// Resolve the caller's tenant from `authorization: Bearer <jwt>`; when `write`, require a role
    /// permitted to write; enforce shard ownership. Auth disabled → `None` (no scoping, dev mode);
    /// a missing/invalid token is rejected.
    async fn resolve<T>(&self, req: &Request<T>, write: bool) -> Result<Option<String>, Status> {
        let Some(state) = &self.auth else {
            return Ok(None);
        };
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            .map(|s| s.to_string());
        let ctx = match token {
            Some(t) => state
                .authenticate(&t)
                .await
                .ok_or_else(|| Status::unauthenticated("invalid token"))?,
            None => return Err(Status::unauthenticated("missing bearer token")),
        };
        if write && !ctx.role.allows_method(&axum::http::Method::POST) {
            return Err(Status::permission_denied(format!(
                "role {:?} is not permitted to write",
                ctx.role
            )));
        }
        self.check_shard(&ctx.tenant_id)?;
        Ok(ctx.tenant_id)
    }
}

#[tonic::async_trait]
impl Strata for StrataGrpcService {
    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();
        let result = match tenant {
            Some(t) => self.engine.query_sql_for_tenant(&req.sql, &t).await,
            None => self.engine.query_sql(&req.sql).await,
        };
        match result {
            Ok(rows) => {
                let count = rows.len() as i64;
                let rows = rows
                    .into_iter()
                    .map(super::convert::json_to_struct)
                    .collect();
                Ok(Response::new(proto::QueryResponse { rows, count }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn ingest(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestResponse>, Status> {
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let events: Vec<strata_core::memory::episodic::Event> = req
            .events
            .into_iter()
            .map(|s| {
                let payload = super::convert::struct_to_json(s);
                strata_core::memory::episodic::Event {
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
                }
            })
            .collect();

        let result = match tenant {
            Some(t) => {
                self.engine
                    .ingest_for_tenant(events, &strata_core::config::TenantContext::new(t))
                    .await
            }
            None => self.engine.ingest(events).await,
        };
        match result {
            Ok(count) => Ok(Response::new(proto::IngestResponse { ingested: count })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn search(
        &self,
        request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();

        if req.vector.is_empty() {
            return Ok(Response::new(proto::SearchResponse { results: vec![] }));
        }

        let k = if req.k == 0 { 5 } else { req.k as usize };

        let search = match tenant {
            Some(t) => {
                self.engine
                    .semantic_search_for_tenant(&req.vector, k, &t, None, None)
                    .await
            }
            None => self.engine.semantic_search(&req.vector, k).await,
        };
        match search {
            Ok(results) => {
                let proto_results: Vec<proto::SearchResult> = results
                    .iter()
                    .map(|r| proto::SearchResult {
                        id: r.entry.id.to_string(),
                        content: r.entry.content.clone(),
                        score: r.score,
                        metadata: Some(super::convert::json_to_struct(r.entry.metadata.clone())),
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
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();
        let got = match tenant {
            Some(t) => {
                self.engine
                    .state_get_for_tenant(&t, &req.agent_id, &req.key)
                    .await
            }
            None => self.engine.state_get(&req.agent_id, &req.key).await,
        };
        match got {
            Ok(Some(entry)) => Ok(Response::new(proto::GetStateResponse {
                agent_id: entry.agent_id,
                key: entry.key,
                value: Some(super::convert::json_to_pvalue(entry.value)),
                version: entry.version,
                found: true,
            })),
            Ok(None) => Ok(Response::new(proto::GetStateResponse {
                agent_id: req.agent_id,
                key: req.key,
                value: None,
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
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let value = req
            .value
            .map(super::convert::pvalue_to_json)
            .unwrap_or(serde_json::Value::Null);

        let set = match tenant {
            Some(t) => {
                self.engine
                    .state_set_for_tenant(&t, &req.agent_id, &req.key, value)
                    .await
            }
            None => self.engine.state_set(&req.agent_id, &req.key, value).await,
        };
        match set {
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

    // ── Memory cognition layer (tenant-scoped) ──

    async fn add_memory(
        &self,
        request: Request<proto::AddMemoryRequest>,
    ) -> Result<Response<prost_types::Struct>, Status> {
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let input = strata_core::memory::cognition::MemoryInput {
            scope: scope_from(req.scope, &tenant),
            subject: req.subject,
            content: req.content,
            importance: req.importance,
            source_event_ids: vec![],
            metadata: serde_json::json!({}),
            mem_type: None,
        };
        let added = self
            .engine
            .memory_add(input)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let v = serde_json::to_value(added).unwrap_or_default();
        Ok(Response::new(super::convert::json_to_struct(v)))
    }

    async fn search_memory(
        &self,
        request: Request<proto::SearchMemoryRequest>,
    ) -> Result<Response<proto::MemoryHits>, Status> {
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();
        let scope = scope_from(req.scope, &tenant);
        let k = if req.k == 0 { 5 } else { req.k as usize };
        let hits = self
            .engine
            .memory_search(&req.query, &scope, k)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let hits = hits
            .into_iter()
            .map(|h| super::convert::json_to_struct(serde_json::to_value(h).unwrap_or_default()))
            .collect();
        Ok(Response::new(proto::MemoryHits { hits }))
    }

    async fn get_memories(
        &self,
        request: Request<proto::GetMemoriesRequest>,
    ) -> Result<Response<proto::MemoryList>, Status> {
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();
        let scope = scope_from(req.scope, &tenant);
        let limit = if req.limit == 0 {
            50
        } else {
            req.limit as usize
        };
        let mems = self
            .engine
            .memory_all(&scope, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let memories = mems
            .into_iter()
            .map(|m| super::convert::json_to_struct(serde_json::to_value(m).unwrap_or_default()))
            .collect();
        Ok(Response::new(proto::MemoryList { memories }))
    }

    async fn delete_memory(
        &self,
        request: Request<proto::DeleteMemoryRequest>,
    ) -> Result<Response<proto::DeleteMemoryResponse>, Status> {
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let id = uuid::Uuid::parse_str(&req.id)
            .map_err(|_| Status::invalid_argument("invalid memory id"))?;
        let deleted = match tenant {
            Some(t) => self
                .engine
                .memory_delete_scoped(id, &t)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            None => {
                self.engine
                    .memory_delete(id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                true
            }
        };
        Ok(Response::new(proto::DeleteMemoryResponse { deleted }))
    }

    // ── Sessions (tenant-scoped) ──

    async fn start_session(
        &self,
        request: Request<proto::StartSessionRequest>,
    ) -> Result<Response<proto::SessionResponse>, Status> {
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let parent = req.parent_session_id.as_deref();
        let res = match &tenant {
            Some(t) => {
                self.engine
                    .session_start_for_tenant(&req.session_id, &req.agent_id, parent, None, t)
                    .await
            }
            None => {
                self.engine
                    .session_start(&req.session_id, &req.agent_id, parent, None)
                    .await
            }
        };
        res.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::SessionResponse {
            session_id: req.session_id,
            status: "started".into(),
        }))
    }

    async fn end_session(
        &self,
        request: Request<proto::EndSessionRequest>,
    ) -> Result<Response<proto::SessionResponse>, Status> {
        let tenant = self.tenant_from_write(&request).await?;
        let req = request.into_inner();
        let summary = req.summary.as_deref();
        match &tenant {
            Some(t) => self
                .engine
                .session_end_for_tenant(&req.session_id, summary, t)
                .await
                .map(|_| ()),
            None => self.engine.session_end(&req.session_id, summary).await,
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::SessionResponse {
            session_id: req.session_id,
            status: "ended".into(),
        }))
    }

    async fn recall_session(
        &self,
        request: Request<proto::RecallSessionRequest>,
    ) -> Result<Response<proto::RecallSessionResponse>, Status> {
        let tenant = self.tenant_from(&request).await?;
        let req = request.into_inner();
        let events = match &tenant {
            Some(t) => {
                self.engine
                    .session_recall_for_tenant(&req.session_id, t)
                    .await
            }
            None => self.engine.session_recall(&req.session_id).await,
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        let events = events
            .into_iter()
            .map(super::convert::json_to_struct)
            .collect();
        Ok(Response::new(proto::RecallSessionResponse { events }))
    }
}

/// Build a cognition scope from a proto scope + the caller's authenticated tenant (which always
/// wins — a client cannot claim another tenant). Empty user/agent/session fields become None.
fn scope_from(
    s: Option<proto::MemoryScope>,
    tenant: &Option<String>,
) -> strata_core::memory::cognition::MemoryScope {
    let s = s.unwrap_or_default();
    let n = |x: String| if x.is_empty() { None } else { Some(x) };
    strata_core::memory::cognition::MemoryScope {
        tenant_id: tenant.clone().unwrap_or_else(|| "default".into()),
        user_id: n(s.user_id),
        agent_id: n(s.agent_id),
        session_id: n(s.session_id),
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
    auth: Option<crate::auth::middleware::AuthState>,
    shard: Option<crate::cluster::shard_route::ShardRoutingState>,
) -> Result<GrpcHandle, Box<dyn std::error::Error>> {
    let parsed_addr = addr
        .parse()
        .map_err(|e| format!("invalid gRPC address: {e}"))?;

    let service = StrataGrpcService::new(engine, auth, shard);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::middleware::AuthState;

    const SECRET: &str = "test-secret-key-256-bits-long!!!";

    fn jwt_role(tenant: &str, role: &str) -> String {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let claims = serde_json::json!({"sub":"u","role":role,"exp":exp,"tenant_id":tenant});
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    async fn svc() -> StrataGrpcService {
        let mut config = strata_core::CoreConfig::default();
        config.memory.episodic.db_path = ":memory:".into();
        config.memory.state.db_path = ":memory:".into();
        config.memory.cognition.db_path = ":memory:".into();
        let engine = Arc::new(StrataEngine::new(config).await.unwrap());
        let auth = AuthState::new(vec![], Some(SECRET.into()), 0);
        StrataGrpcService::new(engine, Some(auth), None)
    }

    fn authed<T>(msg: T, tenant: &str) -> Request<T> {
        authed_role(msg, tenant, "writer")
    }

    fn authed_role<T>(msg: T, tenant: &str, role: &str) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", jwt_role(tenant, role))
                .parse()
                .unwrap(),
        );
        req
    }

    #[tokio::test]
    async fn grpc_reader_role_cannot_write() {
        let service = svc().await;
        // A Reader token → ingest (write) is rejected with PermissionDenied (RBAC now enforced on gRPC).
        let err = service
            .ingest(authed_role(
                proto::IngestRequest {
                    source: "s".into(),
                    events: vec![],
                },
                "t",
                "reader",
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        // A Writer token → the same write succeeds.
        assert!(service
            .ingest(authed(
                proto::IngestRequest {
                    source: "s".into(),
                    events: vec![],
                },
                "t",
            ))
            .await
            .is_ok());
    }

    async fn svc_sharded(my_shard: usize) -> StrataGrpcService {
        let mut config = strata_core::CoreConfig::default();
        config.memory.episodic.db_path = ":memory:".into();
        config.memory.state.db_path = ":memory:".into();
        config.memory.cognition.db_path = ":memory:".into();
        let engine = Arc::new(StrataEngine::new(config).await.unwrap());
        let auth = AuthState::new(vec![], Some(SECRET.into()), 0);
        let shard = crate::cluster::shard_route::ShardRoutingState {
            router: std::sync::Arc::new(strata_cluster::ShardRouter::new(2, 128)),
            my_shard,
            base_urls: std::sync::Arc::new(vec!["http://s0".into(), "http://s1".into()]),
            http: reqwest::Client::new(),
            forward_secret: None,
        };
        StrataGrpcService::new(engine, Some(auth), Some(shard))
    }

    fn tenant_on_shard(shard: usize) -> String {
        let router = strata_cluster::ShardRouter::new(2, 128);
        (0..)
            .map(|i| format!("t{i}"))
            .find(|t| router.shard_for(t) == shard)
            .unwrap()
    }

    #[tokio::test]
    async fn grpc_rejects_tenant_owned_by_another_shard() {
        let service = svc_sharded(0).await;
        // A tenant owned by shard 1 → rejected on shard 0 (no wrong-shard data served).
        let foreign = tenant_on_shard(1);
        let err = service
            .query(authed(
                proto::QueryRequest {
                    sql: "SELECT 1".into(),
                },
                &foreign,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        // A tenant owned by shard 0 → served locally.
        let mine = tenant_on_shard(0);
        assert!(service
            .query(authed(
                proto::QueryRequest {
                    sql: "SELECT 1".into(),
                },
                &mine,
            ))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn grpc_requires_auth_when_configured() {
        let service = svc().await;
        // No bearer token → unauthenticated.
        let resp = service
            .query(Request::new(proto::QueryRequest {
                sql: "SELECT 1".into(),
            }))
            .await;
        assert_eq!(resp.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn grpc_ingest_query_works_and_isolates_tenants() {
        let service = svc().await;

        // tenant-a ingests an event (proves gRPC ingest works).
        let ing = service
            .ingest(authed(
                proto::IngestRequest {
                    source: "sa".into(),
                    events: vec![crate::grpc::convert::json_to_struct(
                        serde_json::json!({"event_type": "e"}),
                    )],
                },
                "tenant-a",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ing.ingested, 1);

        // tenant-a query sees it.
        let qa = service
            .query(authed(
                proto::QueryRequest {
                    sql: "SELECT count(*)::VARCHAR AS c FROM episodic".into(),
                },
                "tenant-a",
            ))
            .await
            .unwrap()
            .into_inner();
        let row_a = crate::grpc::convert::struct_to_json(qa.rows[0].clone());
        assert_eq!(row_a["c"], "1", "tenant-a should see 1 row");

        // tenant-b sees nothing (isolation).
        let qb = service
            .query(authed(
                proto::QueryRequest {
                    sql: "SELECT count(*)::VARCHAR AS c FROM episodic".into(),
                },
                "tenant-b",
            ))
            .await
            .unwrap()
            .into_inner();
        let row_b = crate::grpc::convert::struct_to_json(qb.rows[0].clone());
        assert_eq!(row_b["c"], "0", "tenant-b leaked data!");
    }

    #[tokio::test]
    async fn grpc_state_isolates_tenants() {
        let service = svc().await;
        service
            .set_state(authed(
                proto::SetStateRequest {
                    agent_id: "bot".into(),
                    key: "mood".into(),
                    value: Some(crate::grpc::convert::json_to_pvalue(serde_json::json!(
                        "happy"
                    ))),
                },
                "tenant-a",
            ))
            .await
            .unwrap();

        // tenant-b reads the same agent/key → not found.
        let gb = service
            .get_state(authed(
                proto::GetStateRequest {
                    agent_id: "bot".into(),
                    key: "mood".into(),
                },
                "tenant-b",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!gb.found, "tenant-b read tenant-a state!");

        // tenant-a reads → found, agent_id un-prefixed.
        let ga = service
            .get_state(authed(
                proto::GetStateRequest {
                    agent_id: "bot".into(),
                    key: "mood".into(),
                },
                "tenant-a",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(ga.found);
        assert_eq!(ga.agent_id, "bot");
        // The typed value round-trips back to "happy".
        let v = crate::grpc::convert::pvalue_to_json(ga.value.unwrap());
        assert_eq!(v, serde_json::json!("happy"));
    }

    #[tokio::test]
    async fn grpc_memory_and_sessions_work_and_isolate_tenants() {
        let service = svc().await;
        let scope = || {
            Some(proto::MemoryScope {
                user_id: "alice".into(),
                ..Default::default()
            })
        };

        // tenant-a adds a memory via gRPC.
        service
            .add_memory(authed(
                proto::AddMemoryRequest {
                    content: "likes tea".into(),
                    scope: scope(),
                    subject: None,
                    importance: None,
                },
                "tenant-a",
            ))
            .await
            .unwrap();

        // tenant-a search finds it.
        let hits_a = service
            .search_memory(authed(
                proto::SearchMemoryRequest {
                    query: "tea".into(),
                    scope: scope(),
                    k: 5,
                },
                "tenant-a",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hits_a.hits.len(), 1);

        // tenant-b sees nothing (isolation).
        let hits_b = service
            .search_memory(authed(
                proto::SearchMemoryRequest {
                    query: "tea".into(),
                    scope: scope(),
                    k: 5,
                },
                "tenant-b",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hits_b.hits.len(), 0, "tenant-b leaked memory!");

        // Session lifecycle over gRPC.
        service
            .start_session(authed(
                proto::StartSessionRequest {
                    session_id: "s1".into(),
                    agent_id: "bot".into(),
                    parent_session_id: None,
                },
                "tenant-a",
            ))
            .await
            .unwrap();
        let recalled = service
            .recall_session(authed(
                proto::RecallSessionRequest {
                    session_id: "s1".into(),
                },
                "tenant-a",
            ))
            .await
            .unwrap()
            .into_inner();
        // A fresh session has no events yet — the call succeeds.
        assert_eq!(recalled.events.len(), 0);
    }
}
