//! Configuration loading — layers defaults, strata.toml, and environment variables.

use serde::Deserialize;
use strata_cluster::ClusterConfig;
use strata_core::CoreConfig;
use strata_gateway::server::GatewayConfig;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    // Flattened so core sub-configs are addressable at the top level as documented, e.g.
    // `STRATA_STORAGE__DATA_DIR` / `STRATA_EMBEDDING__PROVIDER` (not `STRATA_CORE__STORAGE__…`).
    #[serde(flatten)]
    pub core: CoreConfig,
    pub gateway: GatewayConfig,
    pub cluster: ClusterConfig,
}

/// Load configuration from strata.toml + STRATA_ env vars.
pub fn load() -> anyhow::Result<ServerConfig> {
    let config = config::Config::builder()
        .add_source(config::File::with_name("strata").required(false))
        // `prefix_separator("_")` so the documented single-underscore form works
        // (`STRATA_GATEWAY__LISTEN`), while `separator("__")` splits nested keys. Without the
        // explicit prefix separator, the `config` crate reuses `__` after the prefix too and
        // silently ignores every `STRATA_*` var — booting nodes on default ports.
        .add_source(
            config::Environment::with_prefix("STRATA")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    // Propagate deserialization errors instead of silently falling back to ALL defaults — a single
    // bad env var (e.g. a malformed STRATA_CLUSTER__PEERS) must fail loudly, not boot a misconfigured
    // node on default ports.
    let server_config: ServerConfig = config.try_deserialize().map_err(|e| {
        anyhow::anyhow!("invalid configuration (check STRATA_* env vars / strata.toml): {e}")
    })?;
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
