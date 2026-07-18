//! S3/MinIO storage backend.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::config::S3Config;

/// S3-compatible object storage.
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    /// Create a new S3 storage backend from configuration.
    pub async fn from_config(config: &S3Config) -> crate::Result<Self> {
        let mut s3_config = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region.clone()))
            .force_path_style(true);

        if !config.endpoint.is_empty() {
            s3_config = s3_config.endpoint_url(&config.endpoint);
        }

        if !config.access_key.is_empty() {
            s3_config = s3_config.credentials_provider(aws_sdk_s3::config::Credentials::new(
                &config.access_key,
                &config.secret_key,
                None,
                None,
                "ecphoria",
            ));
        }

        let client = Client::from_conf(s3_config.build());

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
        })
    }

    /// Create with default/empty config (for testing — won't connect).
    pub fn new() -> Self {
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(handle) = rt {
            // We're in an async context — but can't await here.
            // Return a placeholder that will fail on use.
            let config = S3Config::default();
            let s3_config = S3ConfigBuilder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .build();
            let client = Client::from_conf(s3_config);
            let _ = handle;
            Self {
                client,
                bucket: config.bucket,
            }
        } else {
            let s3_config = S3ConfigBuilder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .build();
            let client = Client::from_conf(s3_config);
            Self {
                client,
                bucket: "ecphoria".into(),
            }
        }
    }
}

impl Default for S3Storage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl super::StorageBackend for S3Storage {
    async fn put(&self, key: &str, data: Bytes) -> crate::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| crate::Error::Storage(format!("S3 put failed: {e}")))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> crate::Result<Option<Bytes>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let data = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| crate::Error::Storage(format!("S3 read body failed: {e}")))?;
                Ok(Some(data.into_bytes()))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(crate::Error::Storage(format!(
                        "S3 get failed: {service_err}"
                    )))
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> crate::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| crate::Error::Storage(format!("S3 delete failed: {e}")))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> crate::Result<Vec<String>> {
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|e| crate::Error::Storage(format!("S3 list failed: {e}")))?;

        let keys: Vec<String> = output
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| k.to_string()))
            .collect();

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_s3_storage() {
        let _storage = S3Storage::new();
    }

    #[test]
    fn default_trait() {
        let _storage = S3Storage::default();
    }

    // Note: actual S3 operations require a running MinIO instance.
    // These are tested via docker-compose integration tests.
}
