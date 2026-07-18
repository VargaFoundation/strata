//! Data tiering — background task for retention enforcement and TTL cleanup.
//!
//! Runs periodically to:
//! - Delete episodic events older than `default_retention_days`
//! - Clean up expired state entries (TTL)
//! - (Future) Move cold data to S3

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

/// Background data lifecycle manager.
///
/// Periodically enforces retention policies, cleans up expired state,
/// and runs automatic S3 backups when configured.
pub struct TieringManager {
    interval: Duration,
    shutdown_rx: watch::Receiver<bool>,
    /// Tracks when the last S3 backup was run.
    last_backup: Option<std::time::Instant>,
}

/// Handle to stop the tiering background task.
pub struct TieringHandle {
    shutdown_tx: watch::Sender<bool>,
}

impl TieringHandle {
    /// Signal the background task to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl TieringManager {
    /// Create a new tiering manager with the given interval.
    pub fn new(interval_secs: u64) -> (Self, TieringHandle) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mgr = Self {
            interval: Duration::from_secs(if interval_secs == 0 {
                3600
            } else {
                interval_secs
            }),
            shutdown_rx,
            last_backup: None,
        };
        let handle = TieringHandle { shutdown_tx };
        (mgr, handle)
    }

    /// Run the background tiering loop.
    ///
    /// This should be spawned as a tokio task. It runs until shutdown is signaled.
    pub async fn run(mut self, engine: Arc<crate::EcphoriaEngine>) {
        tracing::info!(
            interval_secs = self.interval.as_secs(),
            "tiering manager started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {
                    self.run_pass(&engine).await;
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("tiering manager shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Run a single maintenance pass.
    async fn run_pass(&mut self, engine: &crate::EcphoriaEngine) {
        // 1. Enforce episodic retention
        match engine.enforce_retention().await {
            Ok(deleted) if deleted > 0 => {
                tracing::info!(deleted, "retention: deleted old episodic events");
                metrics::counter!("ecphoria_retention_events_deleted_total").increment(deleted);
            }
            Err(e) => tracing::warn!(error = %e, "retention pass failed"),
            _ => {}
        }

        // 2. Clean up expired state entries (TTL)
        match engine.cleanup_expired_state().await {
            Ok(deleted) if deleted > 0 => {
                tracing::info!(deleted, "retention: cleaned up expired state entries");
                metrics::counter!("ecphoria_state_expired_total").increment(deleted);
            }
            Err(e) => tracing::warn!(error = %e, "state TTL cleanup failed"),
            _ => {}
        }

        // 3. Automatic S3 backup (if configured)
        let backup_cfg = &engine.config().backup;
        if backup_cfg.auto_enabled {
            let backup_interval = Duration::from_secs(backup_cfg.interval_hours as u64 * 3600);
            let should_backup = self
                .last_backup
                .is_none_or(|last| last.elapsed() >= backup_interval);

            if should_backup {
                tracing::info!("starting automatic S3 backup");
                match engine.backup_to_s3().await {
                    Ok(()) => {
                        self.last_backup = Some(std::time::Instant::now());
                        metrics::counter!("ecphoria_backup_s3_total").increment(1);
                        tracing::info!("automatic S3 backup completed");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "automatic S3 backup failed");
                        metrics::counter!("ecphoria_backup_s3_errors_total").increment(1);
                    }
                }
            }
        }

        tracing::debug!("tiering pass complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_tiering_manager() {
        let (mgr, handle) = TieringManager::new(60);
        assert_eq!(mgr.interval.as_secs(), 60);
        handle.shutdown();
    }

    #[test]
    fn default_interval() {
        let (mgr, _handle) = TieringManager::new(0);
        assert_eq!(mgr.interval.as_secs(), 3600);
    }

    #[tokio::test]
    async fn shutdown_stops_loop() {
        let (mgr, handle) = TieringManager::new(1);
        let engine = Arc::new(
            crate::EcphoriaEngine::new(crate::CoreConfig::default())
                .await
                .unwrap(),
        );

        let task = tokio::spawn(mgr.run(engine));

        // Give it a moment then shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown();

        // Should complete quickly
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("tiering task should stop on shutdown")
            .expect("task should not panic");
    }
}
