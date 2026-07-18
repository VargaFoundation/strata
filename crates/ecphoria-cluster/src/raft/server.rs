//! Raft gRPC server — receives inter-node RPCs from peers and forwards them to the local
//! `openraft::Raft` instance. The mirror of [`super::network::GrpcRaftNetwork`].

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::pb::raft_service_server::{RaftService, RaftServiceServer};
use super::pb::RaftBytes;
use crate::coordinator::EcphoriaRaft;

/// Max gRPC message size (encode + decode) — matches the client; large for `InstallSnapshot`.
const MAX_MSG_SIZE: usize = 512 * 1024 * 1024;

/// Constant-time byte comparison (avoids timing leaks; length difference is not secret here).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// gRPC service exposing this node's Raft instance to peers.
pub struct RaftGrpcServer {
    raft: Arc<EcphoriaRaft>,
    secret: Option<String>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<EcphoriaRaft>, secret: Option<String>) -> Self {
        Self { raft, secret }
    }

    /// Reject RPCs that don't present the configured cluster Bearer token. This prevents an
    /// unauthorized node from injecting AppendEntries/Vote and corrupting the cluster.
    #[allow(clippy::result_large_err)] // tonic::Status is the idiomatic error here
    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        if let Some(expected) = &self.secret {
            let got = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let token = got.strip_prefix("Bearer ").unwrap_or(got);
            if !ct_eq(token.as_bytes(), expected.as_bytes()) {
                return Err(Status::unauthenticated("invalid cluster credential"));
            }
        }
        Ok(())
    }

    /// Wrap into a tonic service with raised message-size limits (large snapshots).
    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
            .max_decoding_message_size(MAX_MSG_SIZE)
            .max_encoding_message_size(MAX_MSG_SIZE)
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn append_entries(&self, req: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        self.check_auth(&req)?;
        let rpc = rmp_serde::from_slice(&req.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = rmp_serde::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { data }))
    }

    async fn vote(&self, req: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        self.check_auth(&req)?;
        let rpc = rmp_serde::from_slice(&req.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = rmp_serde::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { data }))
    }

    async fn install_snapshot(
        &self,
        req: Request<RaftBytes>,
    ) -> Result<Response<RaftBytes>, Status> {
        self.check_auth(&req)?;
        let rpc = rmp_serde::from_slice(&req.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = rmp_serde::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { data }))
    }
}

#[cfg(test)]
mod tests {
    use super::ct_eq;

    #[test]
    fn constant_time_eq_matches_and_rejects() {
        assert!(ct_eq(b"secret-token", b"secret-token"));
        assert!(!ct_eq(b"secret-token", b"secret-toketX"));
        assert!(!ct_eq(b"secret-token", b"short"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }
}
