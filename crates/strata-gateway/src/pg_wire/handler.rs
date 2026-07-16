//! PostgreSQL wire protocol handler — routes SQL to the Strata engine.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{
    ClientInfo, NoopErrorHandler, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use strata_core::StrataEngine;
use tokio_rustls::TlsAcceptor;

use crate::auth::middleware::AuthState;
use crate::cluster::shard_route::{route_decision, ShardRoutingState, ShardTarget};

/// TLS material for the PG wire listener (PEM cert chain + private key on disk).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PgTlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

/// Build a `tokio_rustls` acceptor from PEM cert + key files. Uses the ring crypto provider
/// explicitly (rather than the ambient default) so the build is unambiguous regardless of which
/// other rustls providers are linked in the binary.
fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<Arc<TlsAcceptor>, Box<dyn std::error::Error>> {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::ServerConfig;

    let cert_bytes =
        std::fs::read(cert_path).map_err(|e| format!("PG TLS: reading cert '{cert_path}': {e}"))?;
    let key_bytes =
        std::fs::read(key_path).map_err(|e| format!("PG TLS: reading key '{key_path}': {e}"))?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_bytes[..])
        .collect::<Result<_, _>>()
        .map_err(|e| format!("PG TLS: parsing cert PEM: {e}"))?;
    if certs.is_empty() {
        return Err(format!("PG TLS: no certificates found in '{cert_path}'").into());
    }
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_bytes[..])
        .map_err(|e| format!("PG TLS: parsing key PEM: {e}"))?
        .ok_or_else(|| format!("PG TLS: no private key found in '{key_path}'"))?;

    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("PG TLS: protocol setup: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("PG TLS: invalid cert/key pair: {e}"))?;

    Ok(Arc::new(TlsAcceptor::from(Arc::new(config))))
}

/// Startup handler with optional password-as-token authentication.
///
/// When auth is enabled the PG **password** is treated as an API key / JWT: it is validated against
/// [`AuthState`] and the resolved tenant is stored in the connection metadata (`tenant_id`), which
/// the query handler then scopes every query to. Auth disabled → the handshake completes with no
/// password step (dev mode). Connect e.g. `psql "host=… user=strata password=<API_KEY>"`.
pub struct StrataStartupHandler {
    auth: Option<AuthState>,
    params: DefaultServerParameterProvider,
}

#[async_trait]
impl StartupHandler for StrataStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                save_startup_parameters_to_metadata(client, startup);
                if self.auth.is_some() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    finish_authentication(client, &self.params).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                if let Some(auth) = &self.auth {
                    match auth.authenticate(&pwd.password).await {
                        Some(ctx) => {
                            if let Some(t) = ctx.tenant_id {
                                client.metadata_mut().insert("tenant_id".to_string(), t);
                            }
                            finish_authentication(client, &self.params).await?;
                        }
                        None => {
                            let error = ErrorResponse::from(ErrorInfo::new(
                                "FATAL".to_owned(),
                                "28P01".to_owned(),
                                "authentication failed (password must be a valid API key / token)"
                                    .to_owned(),
                            ));
                            client
                                .feed(PgWireBackendMessage::ErrorResponse(error))
                                .await?;
                            client.close().await?;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// PG wire handler backed by the Strata engine.
pub struct PgWireHandler {
    engine: Arc<StrataEngine>,
    query_parser: Arc<NoopQueryParser>,
    /// Sharded mode (shards > 1): reject tenants this shard doesn't own.
    shard: Option<ShardRoutingState>,
}

impl PgWireHandler {
    pub fn new(engine: Arc<StrataEngine>, shard: Option<ShardRoutingState>) -> Self {
        Self {
            engine,
            query_parser: Arc::new(NoopQueryParser::new()),
            shard,
        }
    }

    /// The connection's tenant (set at auth), and in sharded mode reject it if this shard doesn't
    /// own it (the PG analogue of the HTTP reverse-proxy — clients reconnect to the owning shard).
    fn resolve_tenant<C: ClientInfo>(&self, client: &C) -> PgWireResult<Option<String>> {
        let tenant = client.metadata().get("tenant_id").cloned();
        if let (Some(shard), Some(t)) = (&self.shard, &tenant) {
            match route_decision(t, &shard.router, shard.my_shard, &shard.base_urls) {
                ShardTarget::Local => {}
                ShardTarget::Forward(url) => {
                    return Err(pg_error(format!(
                        "tenant '{t}' is owned by another shard — connect to its PG endpoint (HTTP base {url})"
                    )))
                }
                ShardTarget::Unroutable => {
                    return Err(pg_error(format!(
                        "tenant '{t}' is owned by another shard with no configured address"
                    )))
                }
            }
        }
        Ok(tenant)
    }
}

fn pg_error(msg: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42000".to_owned(),
        msg,
    )))
}

#[async_trait]
impl SimpleQueryHandler for PgWireHandler {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Tenant-scope (and shard-check) via the authenticated connection, then route to DuckDB.
        let tenant = self.resolve_tenant(client)?;
        let result = match &tenant {
            Some(t) => self.engine.query_sql_for_tenant(query, t).await,
            None => self.engine.query_sql(query).await,
        };
        match result {
            Ok(rows) => {
                if rows.is_empty() {
                    // Could be a DDL/DML statement
                    return Ok(vec![Response::Execution(Tag::new("OK"))]);
                }

                // Build field info from first row's keys
                let field_names: Vec<String> = if let Some(first) = rows.first() {
                    first
                        .as_object()
                        .map(|obj| obj.keys().cloned().collect())
                        .unwrap_or_default()
                } else {
                    vec![]
                };

                // Infer types from first row's values
                let field_types: Vec<Type> = if let Some(first) = rows.first() {
                    field_names
                        .iter()
                        .map(|name| {
                            first
                                .as_object()
                                .and_then(|obj| obj.get(name))
                                .map(infer_pg_type)
                                .unwrap_or(Type::VARCHAR)
                        })
                        .collect()
                } else {
                    field_names.iter().map(|_| Type::VARCHAR).collect()
                };

                let fields: Vec<FieldInfo> = field_names
                    .iter()
                    .zip(field_types.iter())
                    .enumerate()
                    .map(|(i, (name, pg_type))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            pg_type.clone(),
                            pgwire::api::portal::Format::UnifiedText.format_for(i),
                        )
                    })
                    .collect();

                let schema = Arc::new(fields);

                // Encode rows
                let mut data_rows = Vec::new();
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    if let Some(obj) = row.as_object() {
                        for name in &field_names {
                            let val = obj.get(name).map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            });
                            encoder
                                .encode_field(&val)
                                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                        }
                    }
                    data_rows.push(encoder.finish());
                }

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(data_rows),
                ))])
            }
            Err(e) => {
                // Try as a non-SELECT statement
                Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42000".to_owned(),
                        e.to_string(),
                    ),
                )))
            }
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgWireHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        let tenant = self.resolve_tenant(client)?;
        let result = match &tenant {
            Some(t) => self.engine.query_sql_for_tenant(query, t).await,
            None => self.engine.query_sql(query).await,
        };
        match result {
            Ok(rows) if rows.is_empty() => Ok(Response::Execution(Tag::new("OK"))),
            Ok(rows) => {
                let field_names: Vec<String> = rows
                    .first()
                    .and_then(|r| r.as_object())
                    .map(|obj| obj.keys().cloned().collect())
                    .unwrap_or_default();

                let field_types: Vec<Type> = if let Some(first) = rows.first() {
                    field_names
                        .iter()
                        .map(|name| {
                            first
                                .as_object()
                                .and_then(|obj| obj.get(name))
                                .map(infer_pg_type)
                                .unwrap_or(Type::VARCHAR)
                        })
                        .collect()
                } else {
                    field_names.iter().map(|_| Type::VARCHAR).collect()
                };

                let fields: Vec<FieldInfo> = field_names
                    .iter()
                    .zip(field_types.iter())
                    .enumerate()
                    .map(|(i, (name, pg_type))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            pg_type.clone(),
                            portal.result_column_format.format_for(i),
                        )
                    })
                    .collect();
                let schema = Arc::new(fields);

                let mut data_rows = Vec::new();
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    if let Some(obj) = row.as_object() {
                        for name in &field_names {
                            let val = obj.get(name).map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            });
                            encoder
                                .encode_field(&val)
                                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                        }
                    }
                    data_rows.push(encoder.finish());
                }

                Ok(Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(data_rows),
                )))
            }
            Err(e) => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    e.to_string(),
                ),
            ))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribePortalResponse::new(vec![]))
    }
}

/// Factory that creates PG wire server handlers.
pub struct PgWireFactory {
    handler: Arc<PgWireHandler>,
    auth: Option<AuthState>,
}

impl PgWireFactory {
    pub fn new(
        engine: Arc<StrataEngine>,
        auth: Option<AuthState>,
        shard: Option<ShardRoutingState>,
    ) -> Self {
        Self {
            handler: Arc::new(PgWireHandler::new(engine, shard)),
            auth,
        }
    }
}

impl PgWireServerHandlers for PgWireFactory {
    type StartupHandler = StrataStartupHandler;
    type SimpleQueryHandler = PgWireHandler;
    type ExtendedQueryHandler = PgWireHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(StrataStartupHandler {
            auth: self.auth.clone(),
            params: DefaultServerParameterProvider::default(),
        })
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// Handle returned by `start_pg_wire` to control graceful shutdown.
pub struct PgWireHandle {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl PgWireHandle {
    /// Signal the PG wire server to stop accepting connections,
    /// then wait up to `drain_timeout` for in-flight connections to finish.
    pub async fn shutdown(self, drain_timeout: std::time::Duration) {
        let _ = self.shutdown_tx.send(true);
        // Give in-flight connections time to complete
        tokio::time::sleep(drain_timeout).await;
    }
}

/// Start the PG wire server on the given address.
///
/// `max_connections` limits concurrent PG wire connections via a semaphore.
/// Returns a handle that can be used to trigger graceful shutdown.
pub async fn start_pg_wire(
    addr: &str,
    engine: Arc<StrataEngine>,
    max_connections: usize,
    auth: Option<AuthState>,
    shard: Option<ShardRoutingState>,
    tls: Option<PgTlsConfig>,
) -> Result<PgWireHandle, Box<dyn std::error::Error>> {
    let factory = Arc::new(PgWireFactory::new(engine, auth, shard));
    // Optional TLS: when configured, the listener negotiates `SSLRequest` and upgrades the socket.
    // The PG password (= API key/JWT) then never crosses the wire in cleartext.
    let tls_acceptor = match tls {
        Some(cfg) => Some(build_tls_acceptor(&cfg.cert_path, &cfg.key_path)?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    tracing::info!(
        addr,
        max_connections,
        tls = tls_acceptor.is_some(),
        "PG wire server listening"
    );

    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active_conns = active_connections.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, peer_addr)) => {
                            let permit = match semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    tracing::warn!(
                                        %peer_addr,
                                        "PG wire connection rejected: max connections reached"
                                    );
                                    drop(socket);
                                    continue;
                                }
                            };
                            let factory_ref = factory.clone();
                            let conns = active_conns.clone();
                            let acceptor = tls_acceptor.clone();
                            conns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tokio::spawn(async move {
                                let _ = pgwire::tokio::process_socket(socket, acceptor, factory_ref).await;
                                conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                drop(permit);
                            });
                        }
                        Err(e) => {
                            tracing::error!("PG wire accept error: {e}");
                            break;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        let remaining = active_connections.load(std::sync::atomic::Ordering::Relaxed);
                        tracing::info!(
                            remaining_connections = remaining,
                            "PG wire server draining — no longer accepting connections"
                        );
                        if remaining > 0 {
                            tracing::warn!(
                                dropped = remaining,
                                "PG wire server shutting down with active connections"
                            );
                        }
                        break;
                    }
                }
            }
        }
    });

    Ok(PgWireHandle { shutdown_tx })
}

/// Infer a PostgreSQL type from a JSON value.
fn infer_pg_type(value: &serde_json::Value) -> Type {
    match value {
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                Type::INT8
            } else {
                Type::FLOAT8
            }
        }
        serde_json::Value::Bool(_) => Type::BOOL,
        serde_json::Value::Null => Type::VARCHAR,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Type::JSON,
        serde_json::Value::String(s) => {
            // Try to detect timestamps
            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                Type::TIMESTAMPTZ
            } else {
                Type::VARCHAR
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::{CoreConfig, StrataEngine};

    const SECRET: &str = "test-secret-key-256-bits-long!!!";

    fn jwt(tenant: &str) -> String {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let claims = serde_json::json!({"sub":"u","role":"writer","exp":exp,"tenant_id":tenant});
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    async fn engine() -> Arc<StrataEngine> {
        let mut c = CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        Arc::new(StrataEngine::new(c).await.unwrap())
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    // End-to-end: a real PG client authenticates with a JWT as its password and its queries are
    // tenant-scoped; a bad password is rejected.
    #[tokio::test]
    async fn pg_wire_authenticates_with_token_password() {
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let auth = AuthState::new(vec![], Some(SECRET.into()), 0);
        let _handle = start_pg_wire(&addr, engine().await, 16, Some(auth), None, None)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Valid token as password → connects and queries.
        let ok = format!(
            "host=127.0.0.1 port={port} user=strata password={} dbname=strata",
            jwt("tenant-1")
        );
        let (client, conn) = tokio_postgres::connect(&ok, tokio_postgres::NoTls)
            .await
            .expect("auth with valid token should succeed");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let rows = client
            .simple_query("SELECT 1 AS n")
            .await
            .expect("query should run");
        assert!(!rows.is_empty());

        // Garbage password → authentication fails.
        let bad =
            format!("host=127.0.0.1 port={port} user=strata password=not-a-token dbname=strata");
        assert!(
            tokio_postgres::connect(&bad, tokio_postgres::NoTls)
                .await
                .is_err(),
            "a non-token password must be rejected"
        );
    }

    // Self-signed test cert/key (RSA-2048, CN=strata-test) — TEST ONLY, never a real credential.
    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIDDTCCAfWgAwIBAgIUcFB9FNK2t1ruf6ayc4LApbSRvm8wDQYJKoZIhvcNAQEL\n\
BQAwFjEUMBIGA1UEAwwLc3RyYXRhLXRlc3QwHhcNMjYwNzE1MjMxNjQwWhcNMzYw\n\
NzEyMjMxNjQwWjAWMRQwEgYDVQQDDAtzdHJhdGEtdGVzdDCCASIwDQYJKoZIhvcN\n\
AQEBBQADggEPADCCAQoCggEBAPSA8Zho82YAEPRxvouYPO87Nf0sjLGU9vLP1xRK\n\
9LAPt9YwXH5k+xUuctHFpRoU0ps//6J+9TEBxcBUDGzQ3s5tlx/5EPuc98OWe+5T\n\
bg+O/qTYUqZAez8QSqWyTrlNUBateHbZRWIHU5ufydnecr3YZW9Nvv2E/iPBYJNc\n\
dbBFjpN7Z0F7vqPZc+kf/z5npZxcnC87NdZ6j2tCvRc5JmMUxmk4sRL4FX4iJPjg\n\
uKkyoj0JKthOi2L2QXvZnM+U0DFvZR29Bonb3n7IqVMgV/eblY1DV4ApV1JGeLWA\n\
fsW5kBOZosdMuNYZ94sglbI8BscdVN4fdvN5JHma7zLxct0CAwEAAaNTMFEwHQYD\n\
VR0OBBYEFEViAXHIUIxfAm0TuX812hT+rWbaMB8GA1UdIwQYMBaAFEViAXHIUIxf\n\
Am0TuX812hT+rWbaMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB\n\
ACV4dE+dbHPMwlku8XwBxLMW5ziL4Xqm/NriL67sVQVVg05Ep7raYFJzW71tWfcs\n\
MeIf4nVJ0EDPfBmRsfOy0l5hT5BVHOZJ3YJJYA0LoDjNGLAQTTIhyEvPg/2HdqPS\n\
GpxchBFssgCmH8fKFit+Qlnyu5Q5iNXJXW0s9oJrlK94kuvnLV+epxUfnY16GE7g\n\
JGZgKKY15Cqry7shzgV09xcaaBMX5NzEhlrTZ0n5kYX9CXr0mo67YhJxyo4x8x8I\n\
S7Bk4/tq5uZqcIjaC9SduyzvF7ISm7O1lxQvfp9pjo5Aqjj8q+kIzeRylZsFIid2\n\
UB2smy9Sl4xqIYNiJEsjATM=\n\
-----END CERTIFICATE-----\n";

    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQD0gPGYaPNmABD0\n\
cb6LmDzvOzX9LIyxlPbyz9cUSvSwD7fWMFx+ZPsVLnLRxaUaFNKbP/+ifvUxAcXA\n\
VAxs0N7ObZcf+RD7nPfDlnvuU24Pjv6k2FKmQHs/EEqlsk65TVAWrXh22UViB1Ob\n\
n8nZ3nK92GVvTb79hP4jwWCTXHWwRY6Te2dBe76j2XPpH/8+Z6WcXJwvOzXWeo9r\n\
Qr0XOSZjFMZpOLES+BV+IiT44LipMqI9CSrYToti9kF72ZzPlNAxb2UdvQaJ295+\n\
yKlTIFf3m5WNQ1eAKVdSRni1gH7FuZATmaLHTLjWGfeLIJWyPAbHHVTeH3bzeSR5\n\
mu8y8XLdAgMBAAECggEAAsRZd5XAeL1eyRVnHaH5wTn/UL/UpnHUIEf/3B1DtaEH\n\
6JGgNQH5jBzRdH7zG7TJSV5+YHK6svTygvcF3k64JsfmDO4++0n5zSnXzzOnLDU8\n\
ZoCC4ZobNZ8pPm93z/BdtqlR6AO/cu44S6uRl43v6HwZccVZzaQCqERDo9yeVwH8\n\
2G00WjV/vGYqXbbAHchYYpJtuQInfVguUyFAhi8FXFChjFwsnApOV0aNDWdyZvQH\n\
i3Of8YwcVvS2wTUp7aBQIoacinr8xelHas4S3WB9UdBdpCScjM4gs+DD0fms1/S0\n\
EllWNEkSvd8Xq88Bs0fCNE/DutkQIiMCffGQs9jNNQKBgQD6vvrkWOhqCl2TvSwS\n\
zRso3IZZAtVRcLH2mTHTy07e7CF6bBqeya41SbJxgfWtIpPijmEKSUZbaY9f+36v\n\
IAIdXm/H9qstNiSSoOfBlmOP3PNpB5dxrwyIEgnUviT1bx4ya4SrDaVWOxH8YHzt\n\
oBLvlTB7kpO9xRUQQHZj98bC5wKBgQD5oHq4RFObvUzmFMlL3sm5wfjoW7qERMLX\n\
4Q3smQt1JBzhGshddX/SFRA0zZz9HuuqHaCgjo2TlwKm2Y1Gsl29dwqFa1iFXvk5\n\
qKsO0N1ygzyN6gc+RCo1/n5TobMoIM1KbJJrBuA2rFjLG3KycSdQAjAG/bloOk6x\n\
D5Z04g3nmwKBgCkw4mpMqLFyzniMpQbZptKJl5BbxMtCJhoKhIL0bRp10/IWfDEF\n\
lJawap326XLtsTmQhiR4cRRnPORZnjAKpA5LCzXgMbKVqGBmCmxk1io1886XLqvA\n\
Q+C+hdrq+YtQG7fQrdSjwzttLME24I7wsuukqHhEVfzguVsYG9rEQ2SVAoGBAKz6\n\
vc+O2XkkdnNBmDQRECy+86LgXaFmnLZH6AQ6Eax8994tVwcccxS7L93HVbA5iwj5\n\
OuPHpOfPTzEbtEB3PWobYZkOx+qz43RHIzJDHhFKS93zfE1zouSDlDqT5Lg78sZN\n\
8jBkNV7tkyI7xQFOU/WnbmyJyb8mGH2t1Y7tTsFdAoGBAN8oDMDxA2fyTk91S4s2\n\
xoFV5V2qU1kQvCDV0TUor8aZQmji1AvHwAII1Ltirsf5+Z6rr5jfx3hMKxDO1sP5\n\
8+syB37Nszoc1Vbj1p/nbB4gXquT7KbFxrZ7m1sPnCNPb6mg2gX8ZXK02KB13ot5\n\
jEeVrC/BdHMwDCBig3WODjgE\n\
-----END PRIVATE KEY-----\n";

    #[test]
    fn tls_acceptor_builds_from_pem_and_rejects_bad_input() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, TEST_CERT_PEM).unwrap();
        std::fs::write(&key, TEST_KEY_PEM).unwrap();

        // A valid cert/key pair builds an acceptor.
        assert!(build_tls_acceptor(cert.to_str().unwrap(), key.to_str().unwrap()).is_ok());

        // A missing cert file errors (not panics).
        assert!(build_tls_acceptor("/no/such/cert.pem", key.to_str().unwrap()).is_err());

        // A file with no certificate in it errors.
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a pem").unwrap();
        assert!(build_tls_acceptor(empty.to_str().unwrap(), key.to_str().unwrap()).is_err());
    }
}
