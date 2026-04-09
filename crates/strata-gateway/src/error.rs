use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("pg wire error: {0}")]
    PgWire(String),

    #[error("rest error: {0}")]
    Rest(String),

    #[error("grpc error: {0}")]
    Grpc(String),

    #[error("mcp error: {0}")]
    Mcp(String),

    #[error("llm proxy error: {0}")]
    LlmProxy(String),

    #[error("auth error: {0}")]
    Auth(String),

    #[error("bind error: {0}")]
    Bind(String),

    #[error(transparent)]
    Core(#[from] strata_core::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
