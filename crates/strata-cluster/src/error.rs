use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("raft error: {0}")]
    Raft(String),

    #[error("replication error: {0}")]
    Replication(String),

    #[error("coordination error: {0}")]
    Coordination(String),

    #[error("node not leader, leader is: {0:?}")]
    NotLeader(Option<u64>),

    #[error(transparent)]
    Core(#[from] strata_core::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
