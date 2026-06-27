//! Inter-node Raft transport — gRPC (tonic, HTTP/2).
//!
//! Each Raft RPC (AppendEntries, Vote, InstallSnapshot) is MessagePack-serialized and carried in
//! an opaque `RaftBytes` protobuf message to the target node's gRPC `RaftService`. Binary encoding
//! (vs the old JSON-over-HTTP/1.1) is ~2-3x smaller on embedding-heavy AppendEntries and avoids
//! float↔string formatting; HTTP/2 multiplexes all RPCs to a peer over one lazily-managed channel.

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use tonic::transport::{Channel, Endpoint};

use super::pb::raft_service_client::RaftServiceClient;
use super::pb::RaftBytes;
use super::types::{NodeId, NodeInfo, TypeConfig};

/// Max gRPC message size (encode + decode). `InstallSnapshot` ships the full state snapshot, so be
/// generous — this also removes the old 16 MB HTTP body cap that would have broken large snapshots.
const MAX_MSG_SIZE: usize = 512 * 1024 * 1024;

fn unreachable_io(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotConnected, msg)
}

/// Build a request carrying the optional cluster auth token (Bearer) in gRPC metadata.
fn auth_request(data: Vec<u8>, secret: &Option<String>) -> tonic::Request<RaftBytes> {
    let mut req = tonic::Request::new(RaftBytes { data });
    if let Some(s) = secret {
        if let Ok(value) = format!("Bearer {s}").parse() {
            req.metadata_mut().insert("authorization", value);
        }
    }
    req
}

/// A gRPC connection to a single peer. The tonic `Channel` connects lazily and reconnects
/// automatically, multiplexing all RPCs to that peer over one HTTP/2 connection.
pub struct GrpcRaftNetwork {
    addr: String,
    channel: Option<Channel>,
    secret: Option<String>,
}

impl GrpcRaftNetwork {
    fn client(&self) -> Result<RaftServiceClient<Channel>, std::io::Error> {
        let channel = self
            .channel
            .clone()
            .ok_or_else(|| unreachable_io(format!("invalid raft endpoint: {}", self.addr)))?;
        Ok(RaftServiceClient::new(channel)
            .max_decoding_message_size(MAX_MSG_SIZE)
            .max_encoding_message_size(MAX_MSG_SIZE))
    }
}

impl RaftNetwork<TypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        let data =
            rmp_serde::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let reply = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .append_entries(auth_request(data, &self.secret))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        rmp_serde::from_slice(&reply.into_inner().data)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let data =
            rmp_serde::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let reply = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .install_snapshot(auth_request(data, &self.secret))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        rmp_serde::from_slice(&reply.into_inner().data)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _o: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        let data =
            rmp_serde::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let reply = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .vote(auth_request(data, &self.secret))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        rmp_serde::from_slice(&reply.into_inner().data)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

/// Factory creating a lazily-connected gRPC `Channel` per peer (HTTP/2, auto-reconnect). Carries
/// the optional cluster auth token attached to every outbound RPC.
pub struct GrpcRaftNetworkFactory {
    pub secret: Option<String>,
}

impl RaftNetworkFactory<TypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, _target: NodeId, node: &NodeInfo) -> GrpcRaftNetwork {
        // `connect_lazy` never fails here; the first RPC establishes (and retries) the connection.
        let channel = Endpoint::from_shared(node.addr.clone())
            .ok()
            .map(|e| e.connect_lazy());
        GrpcRaftNetwork {
            addr: node.addr.clone(),
            channel,
            secret: self.secret.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn factory_builds_lazy_channel() {
        let mut factory = GrpcRaftNetworkFactory { secret: None };
        let net = factory
            .new_client(
                2,
                &NodeInfo {
                    addr: "http://10.0.0.1:9433".into(),
                },
            )
            .await;
        assert!(
            net.channel.is_some(),
            "valid endpoint should build a channel"
        );
    }

    #[tokio::test]
    async fn factory_tolerates_bad_endpoint() {
        let mut factory = GrpcRaftNetworkFactory { secret: None };
        let net = factory
            .new_client(
                2,
                &NodeInfo {
                    addr: "not a uri".into(),
                },
            )
            .await;
        // A bad address yields no channel; RPCs will surface a retryable Unreachable.
        assert!(net.channel.is_none());
    }
}
