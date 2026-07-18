//! Snapshot creation and transfer for Raft state machine.
//!
//! A snapshot captures the full state of all three memory stores
//! (episodic, semantic, state) so that new nodes can catch up
//! without replaying the entire Raft log.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ecphoria_core::EcphoriaEngine;

/// Handles snapshot creation and restoration for the Raft state machine.
///
/// Provides static methods to build and restore snapshots containing
/// all three memory stores (episodic, semantic, state).
pub struct SnapshotManager;

impl SnapshotManager {
    /// Build a snapshot of the current engine state.
    ///
    /// Creates a temporary directory, exports all stores into it,
    /// then serializes the directory contents into a byte vector.
    pub async fn build(engine: &Arc<EcphoriaEngine>) -> crate::Result<Vec<u8>> {
        let snap_dir = std::env::temp_dir().join(format!(
            "ecphoria-snapshot-{}",
            uuid::Uuid::new_v4().as_simple()
        ));

        let start = std::time::Instant::now();

        // Use the engine's existing backup method to export all stores
        engine
            .backup(&snap_dir)
            .await
            .map_err(|e| crate::Error::Replication(format!("snapshot backup failed: {e}")))?;

        // Serialize the directory contents into a single byte vector
        let data = Self::pack_directory(&snap_dir)?;

        // Clean up temp directory
        let _ = std::fs::remove_dir_all(&snap_dir);

        let duration = start.elapsed();
        metrics::histogram!("ecphoria_raft_snapshot_build_duration_seconds")
            .record(duration.as_secs_f64());

        tracing::info!(
            size_bytes = data.len(),
            duration_ms = duration.as_millis(),
            "Raft snapshot built"
        );

        Ok(data)
    }

    /// Restore engine state from a snapshot byte vector.
    ///
    /// Unpacks the snapshot data into a temporary directory, then
    /// restores each store from the exported files.
    pub async fn restore(engine: &Arc<EcphoriaEngine>, data: &[u8]) -> crate::Result<()> {
        if data.is_empty() {
            tracing::warn!("empty snapshot data, skipping restore");
            return Ok(());
        }

        let snap_dir = std::env::temp_dir().join(format!(
            "ecphoria-restore-{}",
            uuid::Uuid::new_v4().as_simple()
        ));

        let start = std::time::Instant::now();

        // Unpack the byte vector back into a directory structure
        Self::unpack_directory(data, &snap_dir)?;

        // Restore ALL stores (episodic + memories + state + semantic vectors) via the engine's
        // atomic restore path, so a catching-up node receives the full state — not just events.
        // Episodic/memories use stage-then-swap, so a corrupt snapshot never destroys live data.
        engine
            .restore_from_backup(&snap_dir)
            .await
            .map_err(|e| crate::Error::Replication(format!("restore: {e}")))?;

        // Clean up temp directory
        let _ = std::fs::remove_dir_all(&snap_dir);

        let duration = start.elapsed();
        metrics::histogram!("ecphoria_raft_snapshot_install_duration_seconds")
            .record(duration.as_secs_f64());

        tracing::info!(duration_ms = duration.as_millis(), "Raft snapshot restored");

        Ok(())
    }

    /// Pack a directory tree into a simple binary format.
    ///
    /// Format: sequence of (path_len: u32, path: bytes, data_len: u64, data: bytes)
    fn pack_directory(dir: &Path) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut files = Vec::new();
        Self::walk_dir(dir, &mut files)?;

        for file_path in &files {
            let relative = file_path
                .strip_prefix(dir)
                .map_err(|e| crate::Error::Replication(format!("path strip: {e}")))?;
            let rel_str = relative.to_string_lossy();
            let rel_bytes = rel_str.as_bytes();
            let data = std::fs::read(file_path)
                .map_err(|e| crate::Error::Replication(format!("read file: {e}")))?;

            // Write path length (u32) + path bytes
            buf.extend_from_slice(&(rel_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(rel_bytes);
            // Write data length (u64) + data bytes
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
            buf.extend_from_slice(&data);
        }

        Ok(buf)
    }

    /// Unpack a binary snapshot back into a directory tree.
    fn unpack_directory(data: &[u8], dir: &Path) -> crate::Result<()> {
        std::fs::create_dir_all(dir)
            .map_err(|e| crate::Error::Replication(format!("mkdir: {e}")))?;

        let mut cursor = 0;
        while cursor < data.len() {
            // Read path length
            if cursor + 4 > data.len() {
                break;
            }
            let path_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            // Read path
            if cursor + path_len > data.len() {
                break;
            }
            let path_str = std::str::from_utf8(&data[cursor..cursor + path_len])
                .map_err(|e| crate::Error::Replication(format!("invalid path: {e}")))?;
            cursor += path_len;

            // Read data length
            if cursor + 8 > data.len() {
                break;
            }
            let data_len =
                u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap()) as usize;
            cursor += 8;

            // Read data
            if cursor + data_len > data.len() {
                break;
            }
            let file_data = &data[cursor..cursor + data_len];
            cursor += data_len;

            // Write file
            let file_path = dir.join(path_str);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| crate::Error::Replication(format!("mkdir: {e}")))?;
            }
            std::fs::write(&file_path, file_data)
                .map_err(|e| crate::Error::Replication(format!("write file: {e}")))?;
        }

        Ok(())
    }

    /// Recursively walk a directory, collecting file paths.
    fn walk_dir(dir: &Path, out: &mut Vec<PathBuf>) -> crate::Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)
            .map_err(|e| crate::Error::Replication(format!("readdir: {e}")))?
        {
            let entry = entry.map_err(|e| crate::Error::Replication(format!("entry: {e}")))?;
            let path = entry.path();
            if path.is_dir() {
                Self::walk_dir(&path, out)?;
            } else {
                out.push(path);
            }
        }
        Ok(())
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();

        // Create test files
        std::fs::create_dir_all(src.path().join("subdir")).unwrap();
        std::fs::write(src.path().join("file1.txt"), b"hello").unwrap();
        std::fs::write(src.path().join("subdir/file2.txt"), b"world").unwrap();

        let packed = SnapshotManager::pack_directory(src.path()).unwrap();
        assert!(!packed.is_empty());

        SnapshotManager::unpack_directory(&packed, dst.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(dst.path().join("file1.txt")).unwrap(),
            "hello"
        );
        assert_eq!(
            std::fs::read_to_string(dst.path().join("subdir/file2.txt")).unwrap(),
            "world"
        );
    }

    #[test]
    fn unpack_empty_data() {
        let dir = tempfile::tempdir().unwrap();
        SnapshotManager::unpack_directory(&[], dir.path()).unwrap();
    }
}
