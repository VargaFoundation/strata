//! Configuration loading — layers defaults, strata.toml, and environment variables.

use serde::Deserialize;
use strata_cluster::ClusterConfig;
use strata_core::CoreConfig;
use strata_gateway::server::GatewayConfig;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub core: CoreConfig,
    pub gateway: GatewayConfig,
    pub cluster: ClusterConfig,
}

/// Load configuration from strata.toml + STRATA_ env vars.
pub fn load() -> anyhow::Result<ServerConfig> {
    let config = config::Config::builder()
        .add_source(config::File::with_name("strata").required(false))
        .add_source(config::Environment::with_prefix("STRATA").separator("__"))
        .build()?;

    let server_config: ServerConfig = config.try_deserialize().unwrap_or_default();
    Ok(server_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_defaults() {
        let config = load().unwrap();
        assert_eq!(config.gateway.listen, "0.0.0.0:8432");
    }
}
