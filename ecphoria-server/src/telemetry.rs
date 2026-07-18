//! Tracing/telemetry setup: always a fmt subscriber; optionally an OTLP trace exporter.
//!
//! With the `otlp` feature built in AND `ECPHORIA_OTLP_ENDPOINT` set, spans are exported to an OTLP
//! collector (Tempo/Grafana/Jaeger) over HTTP/protobuf, in parallel with the Prometheus metrics
//! exporter. Without the feature (default) or the env var, only the fmt subscriber runs — zero cost.
//!
//! The endpoint is the full traces URL, e.g. `http://localhost:4318/v1/traces`.

use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Kept alive for the process lifetime; `shutdown` flushes any buffered spans on exit.
#[derive(Default)]
pub struct TelemetryGuard {
    #[cfg(feature = "otlp")]
    provider: Option<opentelemetry_sdk::trace::TracerProvider>,
}

impl TelemetryGuard {
    /// Flush and stop the OTLP exporter (no-op without the feature / when disabled).
    pub fn shutdown(self) {
        #[cfg(feature = "otlp")]
        if let Some(provider) = self.provider {
            // Best-effort final flush so spans from just before shutdown aren't lost.
            let _ = provider.shutdown();
        }
    }
}

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,ecphoria=debug".parse().unwrap())
}

/// Initialize the global tracing subscriber. Returns a guard to shut down on exit.
pub fn init() -> TelemetryGuard {
    #[cfg(feature = "otlp")]
    {
        if let Ok(endpoint) = std::env::var("ECPHORIA_OTLP_ENDPOINT") {
            if !endpoint.is_empty() {
                match build_otlp_provider(&endpoint) {
                    Ok(provider) => {
                        use opentelemetry::trace::TracerProvider as _;
                        let tracer = provider.tracer("ecphoria");
                        tracing_subscriber::registry()
                            .with(env_filter())
                            .with(tracing_subscriber::fmt::layer())
                            .with(tracing_opentelemetry::layer().with_tracer(tracer))
                            .init();
                        tracing::info!(%endpoint, "OTLP trace export enabled");
                        return TelemetryGuard {
                            provider: Some(provider),
                        };
                    }
                    Err(e) => {
                        // Fall through to fmt-only rather than failing startup over telemetry.
                        eprintln!("OTLP exporter init failed ({e}); continuing with logs only");
                    }
                }
            }
        }
    }

    tracing_subscriber::registry()
        .with(env_filter())
        .with(tracing_subscriber::fmt::layer())
        .init();
    TelemetryGuard::default()
}

#[cfg(feature = "otlp")]
fn build_otlp_provider(endpoint: &str) -> anyhow::Result<opentelemetry_sdk::trace::TracerProvider> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{trace::TracerProvider, Resource};

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;
    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            env!("CARGO_PKG_NAME"),
        )]))
        .build();
    Ok(provider)
}

#[cfg(all(test, feature = "otlp"))]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// End-to-end: the OTLP HTTP exporter POSTs a recorded span to a mock collector at
    /// `/v1/traces`. Proves the exporter is wired correctly (endpoint, protocol, flush) without a
    /// real Tempo/Jaeger — the honest validation that OTLP export actually leaves the process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_exporter_posts_spans_to_collector() {
        use axum::{routing::post, Router};
        use opentelemetry::trace::{Tracer, TracerProvider as _};

        let hits = Arc::new(Mutex::new(0usize));
        let h = hits.clone();
        let app = Router::new().route(
            "/v1/traces",
            post(move || {
                let h = h.clone();
                async move {
                    *h.lock().unwrap() += 1;
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let provider = build_otlp_provider(&format!("http://{addr}/v1/traces")).unwrap();
        let tracer = provider.tracer("test");
        tracer.in_span("unit-span", |_| {});
        // Flush the batch processor, then give the async export a moment to land.
        for res in provider.force_flush() {
            res.expect("span export should succeed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = provider.shutdown();

        assert!(
            *hits.lock().unwrap() >= 1,
            "mock OTLP collector received no trace POST"
        );
    }
}
