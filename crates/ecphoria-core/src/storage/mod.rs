pub mod local;
pub mod s3;
pub mod tiering;

use bytes::Bytes;

/// Abstraction over storage backends (local disk, S3/MinIO).
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store data at the given key.
    async fn put(&self, key: &str, data: Bytes) -> crate::Result<()>;

    /// Retrieve data by key. Returns None if not found.
    async fn get(&self, key: &str) -> crate::Result<Option<Bytes>>;

    /// Delete data by key.
    async fn delete(&self, key: &str) -> crate::Result<()>;

    /// List keys matching a prefix.
    async fn list(&self, prefix: &str) -> crate::Result<Vec<String>>;
}
