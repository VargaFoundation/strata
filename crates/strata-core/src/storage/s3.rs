//! S3/MinIO storage backend.

use bytes::Bytes;

/// S3-compatible object storage.
pub struct S3Storage {
    // TODO: aws_sdk_s3::Client
}

impl S3Storage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for S3Storage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl super::StorageBackend for S3Storage {
    async fn put(&self, _key: &str, _data: Bytes) -> crate::Result<()> {
        Ok(())
    }

    async fn get(&self, _key: &str) -> crate::Result<Option<Bytes>> {
        Ok(None)
    }

    async fn delete(&self, _key: &str) -> crate::Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> crate::Result<Vec<String>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageBackend;

    #[test]
    fn create_s3_storage() {
        let storage = S3Storage::new();
        let _ = storage;
    }

    #[test]
    fn default_trait() {
        let storage = S3Storage::default();
        let _ = storage;
    }

    #[tokio::test]
    async fn put_succeeds() {
        let storage = S3Storage::new();
        storage.put("key", Bytes::from("data")).await.unwrap();
    }

    #[tokio::test]
    async fn get_returns_none() {
        let storage = S3Storage::new();
        let result = storage.get("key").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn delete_succeeds() {
        let storage = S3Storage::new();
        storage.delete("key").await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_empty() {
        let storage = S3Storage::new();
        let keys = storage.list("prefix/").await.unwrap();
        assert!(keys.is_empty());
    }
}
