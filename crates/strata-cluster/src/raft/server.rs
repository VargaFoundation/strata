//! Raft gRPC server — receives inter-node RPCs from peers and forwards them to the local
//! `openraft::Raft` instance. The mirror of [`super::network::GrpcRaftNetwork`].

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::pb::raft_service_server::{RaftService, RaftServiceServer};
use super::pb::RaftBytes;
use crate::coordinator::StrataRaft;

/// Max gRPC message size (encode + decode) — matches the client; large for `InstallSnapshot`.
const MAX_MSG_SIZE: usize = 512 * 1024 * 1024;

/// gRPC service exposing this node's Raft instance to peers.
pub struct RaftGrpcServer {
    raft: Arc<StrataRaft>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<StrataRaft>) -> Self {
        Self { raft }
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
