//! Local filesystem storage backend.

use bytes::Bytes;
use std::path::PathBuf;

/// Local filesystem storage.
pub struct LocalStorage {
    base_dir: PathBuf,
}

impl LocalStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    fn key_path(&self, key: &str) -> PathBuf {
        // Sanitize key: replace path separators to prevent directory traversal
        let safe_key = key.replace(['/', '\\'], "_");
        self.base_dir.join(safe_key)
    }
}

#[async_trait::async_trait]
impl super::StorageBackend for LocalStorage {
    async fn put(&self, key: &str, data: Bytes) -> crate::Result<()> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| crate::Error::Storage(format!("mkdir failed: {e}")))?;
        }
        tokio::fs::write(&path, &data)
            .await
            .map_err(|e| crate::Error::Storage(format!("write failed: {e}")))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> crate::Result<Option<Bytes>> {
        let path = self.key_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::Error::Storage(format!("read failed: {e}"))),
        }
    }

    async fn delete(&self, key: &str) -> crate::Result<()> {
        let path = self.key_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::Error::Storage(format!("delete failed: {e}"))),
        }
    }

    async fn list(&self, prefix: &str) -> crate::Result<Vec<String>> {
        let mut entries = Vec::new();
        let safe_prefix = prefix.replace(['/', '\\'], "_");

        let mut dir = match tokio::fs::read_dir(&self.base_dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(crate::Error::Storage(format!("readdir failed: {e}"))),
        };

        while let Ok(Some(entry)) = dir.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&safe_prefix) {
                    entries.push(name.to_string());
                }
            }
        }

        entries.sort();
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageBackend;
    use tempfile::TempDir;

    fn tmp_storage() -> (TempDir, LocalStorage) {
        let dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(dir.path().to_path_buf());
        (dir, storage)
    }

    #[tokio::test]
    async fn put_and_get() {
        let (_dir, storage) = tmp_storage();
        storage
            .put("test-key", Bytes::from("hello world"))
            .await
            .unwrap();
        let data = storage.get("test-key").await.unwrap().unwrap();
        assert_eq!(data.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn get_missing_key_returns_none() {
        let (_dir, storage) = tmp_storage();
        let result = storage.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn put_overwrite() {
        let (_dir, storage) = tmp_storage();
        storage.put("key", Bytes::from("v1")).await.unwrap();
        storage.put("key", Bytes::from("v2")).await.unwrap();
        let data = storage.get("key").await.unwrap().unwrap();
        assert_eq!(data.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn delete_existing() {
        let (_dir, storage) = tmp_storage();
        storage.put("key", Bytes::from("data")).await.unwrap();
        storage.delete("key").await.unwrap();
        assert!(storage.get("key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let (_dir, storage) = tmp_storage();
        storage.delete("nope").await.unwrap();
    }

    #[tokio::test]
    async fn list_with_prefix() {
        let (_dir, storage) = tmp_storage();
        storage.put("data-1", Bytes::from("a")).await.unwrap();
        storage.put("data-2", Bytes::from("b")).await.unwrap();
        storage.put("other", Bytes::from("c")).await.unwrap();

        let keys = storage.list("data-").await.unwrap();
        assert_eq!(keys, vec!["data-1", "data-2"]);
    }

    #[tokio::test]
    async fn list_empty_dir() {
        let (_dir, storage) = tmp_storage();
        let keys = storage.list("").await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn binary_data() {
        let (_dir, storage) = tmp_storage();
        let data = Bytes::from(vec![0u8, 1, 2, 255, 254, 253]);
        storage.put("binary", data.clone()).await.unwrap();
        let retrieved = storage.get("binary").await.unwrap().unwrap();
        assert_eq!(retrieved, data);
    }
}
