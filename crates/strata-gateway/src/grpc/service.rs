//! gRPC service implementations.

/// Strata gRPC service (stub — proto definitions pending).
pub struct StrataGrpcService {
    // TODO: implement tonic service traits once proto is defined
}

impl StrataGrpcService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StrataGrpcService {
    fn default() -> Self {
        Self::new()
    }
}
