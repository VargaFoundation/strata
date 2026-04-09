//! Local filesystem storage backend.

use bytes::Bytes;
use std::path::PathBuf;

/// Local filesystem storage.
pub struct LocalStorage {
    _base_dir: PathBuf,
}

impl LocalStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            _base_dir: base_dir,
        }
    }
}

#[async_trait::async_trait]
impl super::StorageBackend for LocalStorage {
    async fn put(&self, _key: &str, _data: Bytes) -> crate::Result<()> {
        // TODO: write to data_dir/key
        Ok(())
    }

    async fn get(&self, _key: &str) -> crate::Result<Option<Bytes>> {
        // TODO: read from data_dir/key
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
    fn create_local_storage() {
        let storage = LocalStorage::new(PathBuf::from("/tmp/strata-test"));
        let _ = storage;
    }

    #[tokio::test]
    async fn put_succeeds() {
        let storage = LocalStorage::new(PathBuf::from("/tmp/strata-test-put"));
        storage.put("test-key", Bytes::from("hello")).await.unwrap();
    }

    #[tokio::test]
    async fn get_missing_key_returns_none() {
        let storage = LocalStorage::new(PathBuf::from("/tmp/strata-test-get"));
        let result = storage.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn delete_succeeds() {
        let storage = LocalStorage::new(PathBuf::from("/tmp/strata-test-del"));
        storage.delete("any-key").await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_empty() {
        let storage = LocalStorage::new(PathBuf::from("/tmp/strata-test-list"));
        let keys = storage.list("prefix/").await.unwrap();
        assert!(keys.is_empty());
    }
}
