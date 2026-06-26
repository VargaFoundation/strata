pub mod network;
pub mod server;
pub mod store;
pub mod types;

/// Generated gRPC types for the inter-node Raft transport (from `proto/raft.proto`).
pub mod pb {
    tonic::include_proto!("strata.raft");
}
