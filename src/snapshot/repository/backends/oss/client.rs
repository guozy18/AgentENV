use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use object_store_operator::{
    build_object_store_operator, run_with_refresh, AddressingStyle, CachedCredentialSource,
    CredentialSource, ObjectStoreOperatorConfig, ObjectStoreOperatorError, OperatorWithCredential,
};
use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::info;
use url::Url;

use crate::observability::prometheus::MetricGuard;

/// Multipart part size for streaming file uploads. S3/OSS caps a multipart
/// upload at 10,000 parts, so this bounds the largest uploadable object
/// (~625 GiB at 64 MiB). Must be passed explicitly to opendal via
/// `writer_with().chunk()`: without it opendal falls back to the service's
/// minimum multipart part size (5 MiB), capping uploads at ~50 GiB.
const CHUNK_SIZE: usize = 64 * 1024 * 1024;
/// Number of multipart parts uploaded concurrently per file. A single
/// sequential stream tops out at roughly 100 MB/s to the OSS internal
/// endpoint; concurrent parts multiply effective throughput.
const UPLOAD_CONCURRENCY: usize = 8;
const OSS_OPERATION_DURATION: &str = "agentenv_snapshot_oss_operation_duration_seconds";

/// Snapshot artifacts uploaded to OSS. Used as the `artifact` label on upload
/// metrics and in upload completion logs so memory layers can be told apart
/// from rootfs/attached-drive layers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OssUploadArtifact {
    RootfsLayer,
    AttachedDriveLayer,
    MemoryLayer,
    VmState,
    FirecrackerManifest,
    CatalogRecord,
    Alias,
}

impl OssUploadArtifact {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RootfsLayer => "rootfs_layer",
            Self::AttachedDriveLayer => "attached_drive_layer",
            Self::MemoryLayer => "memory_layer",
            Self::VmState => "vm_state",
            Self::FirecrackerManifest => "manifest",
            Self::CatalogRecord => "record",
            Self::Alias => "alias",
        }
    }
}

/// Thin wrapper around the OSS client used by the repository and resolver.
#[derive(Clone, Debug)]
pub(crate) struct OssClient {
    operator_config: ObjectStoreOperatorConfig,
    prefix: String,
    credentials: Arc<CachedCredentialSource>,
    cached_operator: Arc<RwLock<Option<OperatorWithCredential>>>,
}

impl OssClient {
    pub(crate) fn new(
        bucket: String,
        endpoint: String,
        region: String,
        prefix: String,
        credential_source: CredentialSource,
        addressing_override: Option<AddressingStyle>,
    ) -> Result<Self> {
        // Detection also validates the endpoint URL, so it always runs; an
        // explicit config override then wins over the detected style.
        let detected_style = detect_addressing_style(&endpoint, &bucket)?;
        let addressing_style = addressing_override.unwrap_or(detected_style);
        if is_aliyun_endpoint(&endpoint)? {
            return Err(anyhow::anyhow!(
                "snapshot OSS backend requires a CAS-capable S3-compatible endpoint; Alibaba native OSS endpoints are unsupported"
            ));
        }
        Ok(Self {
            operator_config: ObjectStoreOperatorConfig {
                addressing_style,
                bucket,
                endpoint,
                region,
                timeout: None,
                max_retries: None,
            },
            prefix,
            credentials: Arc::new(CachedCredentialSource::new(credential_source)),
            cached_operator: Arc::new(RwLock::new(None)),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    pub(crate) fn managed_layers_repo_blob_url(&self) -> String {
        // overlaybd expects an S3-compatible repo blob URL, so the scheme
        // remains `s3://` rather than a provider-specific scheme.
        if self.prefix.is_empty() {
            format!("s3://{}/managed-layers", self.operator_config.bucket)
        } else {
            format!(
                "s3://{}/{}/managed-layers",
                self.operator_config.bucket, self.prefix
            )
        }
    }

    /// Read a small object entirely into memory.
    pub(crate) async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "get_bytes");
        let result = self
            .run_with_key(key, |operator, key| async move {
                operator.read(&key).await.map(|buffer| buffer.to_bytes())
            })
            .await
            .with_context(|| format!("oss get '{key}'"));
        metric.finish(&result);
        result
    }

    /// Download an object directly to a local file (atomic: temp + rename).
    pub(crate) async fn get_to_file(&self, key: &str, dest: &Path) -> Result<u64> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "get_to_file");
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create cache dir '{}'", parent.display()))?;
        }

        let dest = dest.to_path_buf();
        let oss_key = self.full_key(key);
        let result = self
            .run_with_operator(|operator| {
                let dest = dest.clone();
                let oss_key = oss_key.clone();
                async move { download_object_to_file(&operator, &oss_key, &dest).await }
            })
            .await
            .with_context(|| format!("oss download '{key}'"));
        metric.finish(&result);
        result
    }

    /// Check whether an object exists.
    pub(crate) async fn exists(&self, key: &str) -> Result<bool> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "exists");
        let result = self
            .run_with_key(
                key,
                |operator, key| async move { operator.exists(&key).await },
            )
            .await
            .with_context(|| format!("oss exists '{key}'"));
        metric.finish(&result);
        result
    }

    /// List all files recursively under a prefix.
    pub(crate) async fn list_keys_recursive(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = self
            .run_with_key(prefix, |operator, prefix| async move {
                let entries = operator.list_with(&prefix).recursive(true).await?;

                Ok(entries
                    .into_iter()
                    .filter(|entry| !entry.metadata().mode().is_dir())
                    .map(|entry| entry.path().to_string())
                    .collect())
            })
            .await
            .with_context(|| format!("oss list '{prefix}'"))?;

        if self.prefix.is_empty() {
            return Ok(keys);
        }
        let strip = format!("{}/", self.prefix);
        Ok(keys
            .into_iter()
            .map(|p| p.strip_prefix(&strip).unwrap_or(&p).to_string())
            .collect())
    }

    /// Write small data (catalog JSON, alias JSON, etc.).
    pub(crate) async fn put_bytes(
        &self,
        key: &str,
        data: impl Into<Bytes>,
        artifact: OssUploadArtifact,
    ) -> Result<()> {
        let data = data.into();
        let size = data.len() as u64;
        let oss_key = self.full_key(key);
        let mut metric =
            MetricGuard::operation_artifact(OSS_OPERATION_DURATION, "put_bytes", artifact.as_str());
        let result = self
            .run_with_operator(|operator| {
                let data = data.clone();
                let oss_key = oss_key.clone();
                async move { write_bytes_to_operator(&operator, &oss_key, data).await }
            })
            .await
            .with_context(|| format!("oss put '{key}'"));
        metric.finish(&result);
        if result.is_ok() {
            metrics::counter!(
                "agentenv_snapshot_oss_upload_bytes_total",
                "operation" => "put_bytes",
                "artifact" => artifact.as_str(),
            )
            .increment(size);
        }
        result?;
        Ok(())
    }

    /// Return the current ETag for a small catalog object.
    ///
    /// A missing object is represented as `Ok(None)`.  Backends that cannot
    /// return an ETag are rejected because callers cannot safely use a
    /// read/modify/write operation without one.
    pub(crate) async fn stat_etag(&self, key: &str) -> Result<Option<String>> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "stat_etag");
        let result = match self
            .run_with_key(key, |operator, key| async move {
                let metadata = operator.stat(&key).await?;
                etag_from_metadata(metadata, &key)
            })
            .await
        {
            Ok(etag) => Ok(Some(etag)),
            Err(error) if Self::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
        .with_context(|| format!("oss stat etag '{key}'"));
        metric.finish(&result);
        result
    }

    /// Create a small object only when the target key does not exist.
    ///
    /// This is a single PUT and returns the server-provided ETag.  Condition
    /// failures are deliberately returned to the caller; the conditional
    /// operation uses an operator with retries disabled so a stale claim is
    /// not transparently retried (and does not multiply OSS QPS).
    pub(crate) async fn put_bytes_if_not_exists(
        &self,
        key: &str,
        data: impl Into<Bytes>,
        artifact: OssUploadArtifact,
    ) -> Result<String> {
        self.put_bytes_conditionally(key, data.into(), artifact, ConditionalWrite::IfNotExists)
            .await
    }

    /// Replace a small object only when its current ETag matches `expected`.
    ///
    /// `expected` must be the exact ETag returned by `stat_etag`, including
    /// surrounding quotes when the backend includes them.
    pub(crate) async fn put_bytes_if_match(
        &self,
        key: &str,
        data: impl Into<Bytes>,
        expected: &str,
        artifact: OssUploadArtifact,
    ) -> Result<String> {
        let expected = expected.trim();
        if expected.is_empty() {
            return Err(anyhow::anyhow!(
                "conditional OSS write requires a non-empty ETag"
            ));
        }
        self.put_bytes_conditionally(
            key,
            data.into(),
            artifact,
            ConditionalWrite::IfMatch(expected.to_string()),
        )
        .await
    }

    /// Upload a local file to OSS.
    pub(crate) async fn put_file(
        &self,
        key: &str,
        path: &Path,
        artifact: OssUploadArtifact,
    ) -> Result<()> {
        let oss_key = self.full_key(key);
        let path = path.to_path_buf();
        let mut metric =
            MetricGuard::operation_artifact(OSS_OPERATION_DURATION, "put_file", artifact.as_str());
        let start = Instant::now();
        let result: Result<u64> = async {
            let size = tokio::fs::metadata(&path)
                .await
                .with_context(|| format!("stat oss upload source file '{}'", path.display()))?
                .len();
            self.run_with_operator(|operator| {
                let oss_key = oss_key.clone();
                let path = path.clone();
                async move { upload_file_to_operator(&operator, &oss_key, &path).await }
            })
            .await
            .with_context(|| format!("oss put file '{key}'"))?;
            Ok(size)
        }
        .await;
        metric.finish(&result);
        match result {
            Ok(size) => {
                metrics::counter!(
                    "agentenv_snapshot_oss_upload_bytes_total",
                    "operation" => "put_file",
                    "artifact" => artifact.as_str(),
                )
                .increment(size);
                info!(
                    key = %oss_key,
                    artifact = artifact.as_str(),
                    size_bytes = size,
                    elapsed_ms = start.elapsed().as_millis(),
                    "oss file uploaded"
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Delete a single object. Idempotent – missing objects are not errors.
    pub(crate) async fn delete(&self, key: &str) -> Result<()> {
        self.run_with_key(key, |operator, key| async move {
            match operator.delete(&key).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == OpenDalErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        .with_context(|| format!("oss delete '{key}'"))
    }

    /// Delete all objects under a prefix.
    pub(crate) async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        // `list_keys_recursive()` returns repository-relative keys with the
        // configured backend prefix stripped, while `delete()` expects that
        // same repository-relative form and re-applies the backend prefix.
        let keys = self.list_keys_recursive(prefix).await?;
        stream::iter(keys.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(16, |key| async move { self.delete(&key).await })
            .await
    }

    pub(crate) fn is_not_found_error(error: &anyhow::Error) -> bool {
        Self::contains_opendal_kind(error, OpenDalErrorKind::NotFound)
    }

    /// Return whether an OSS operation failed its server-side condition.
    ///
    /// S3-compatible services commonly report both HTTP 409 and HTTP 412 as
    /// OpenDAL `ConditionNotMatch`; callers should map this to a conflict and
    /// must not retry it with a freshly-read ETag.
    pub(crate) fn is_condition_not_match_error(error: &anyhow::Error) -> bool {
        Self::contains_opendal_kind(error, OpenDalErrorKind::ConditionNotMatch)
    }

    fn contains_opendal_kind(error: &anyhow::Error, kind: OpenDalErrorKind) -> bool {
        error.chain().any(|cause| {
            if let Some(opendal_error) = cause.downcast_ref::<OpenDalError>() {
                return opendal_error.kind() == kind;
            }
            if let Some(ObjectStoreOperatorError::OpenDal(opendal_error)) =
                cause.downcast_ref::<ObjectStoreOperatorError>()
            {
                return opendal_error.kind() == kind;
            }
            false
        })
    }

    async fn put_bytes_conditionally(
        &self,
        key: &str,
        data: Bytes,
        artifact: OssUploadArtifact,
        condition: ConditionalWrite,
    ) -> Result<String> {
        let size = data.len() as u64;
        let oss_key = self.full_key(key);
        let operation = condition.operation_name();
        let mut metric =
            MetricGuard::operation_artifact(OSS_OPERATION_DURATION, operation, artifact.as_str());
        let condition_for_run = condition.clone();
        let result = self
            .run_with_conditional_operator(move |operator| {
                let data = data.clone();
                let oss_key = oss_key.clone();
                let condition = condition_for_run.clone();
                async move {
                    ensure_conditional_write_capability(&operator, &condition)?;
                    let metadata = match condition {
                        ConditionalWrite::IfNotExists => {
                            operator
                                .write_with(&oss_key, data)
                                .if_not_exists(true)
                                .await?
                        }
                        ConditionalWrite::IfMatch(expected) => {
                            operator
                                .write_with(&oss_key, data)
                                .if_match(&expected)
                                .await?
                        }
                    };
                    etag_from_metadata(metadata, &oss_key)
                }
            })
            .await
            .with_context(|| format!("oss {operation} '{key}'"));
        metric.finish(&result);
        if result.is_ok() {
            metrics::counter!(
                "agentenv_snapshot_oss_upload_bytes_total",
                "operation" => operation,
                "artifact" => artifact.as_str(),
            )
            .increment(size);
        }
        result
    }

    async fn run_with_key<T, F, Fut>(&self, key: &str, operation: F) -> Result<T>
    where
        F: Fn(Operator, String) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        let key = self.full_key(key);
        self.run_with_operator(|operator| {
            let key = key.clone();
            operation(operator, key)
        })
        .await
    }

    async fn run_with_operator<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(Operator) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        // Centralizes one-shot operator construction plus credential-refresh
        // retry semantics so individual OSS operations don't each have to
        // reason about cached credentials and operator replacement.
        let current = self.ensure_fresh_operator().await?;
        let (value, refreshed) = run_with_refresh(
            &current,
            Some(self.credentials.as_ref()),
            &self.operator_config,
            operation,
        )
        .await
        .map_err(anyhow::Error::from)?;
        if let Some(refreshed) = refreshed {
            *self.cached_operator.write().await = Some(refreshed);
        }
        Ok(value)
    }

    /// Run a conditional metadata mutation without OpenDAL's general retry
    /// layer.  S3 maps HTTP 409 to a temporary error, which would otherwise
    /// cause RetryLayer to issue the same stale CAS request multiple times.
    /// Credential refresh remains enabled by `run_with_refresh`.
    async fn run_with_conditional_operator<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(Operator) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        let credential = self.credentials.current().await?.ok_or_else(|| {
            anyhow::anyhow!("snapshot OSS client requires non-anonymous credentials")
        })?;
        let mut config = self.operator_config.clone();
        config.max_retries = Some(0);
        let operator = build_object_store_operator(&config, Some(&credential))?;
        let current = OperatorWithCredential::new(operator, Some(credential));
        let (value, _refreshed) = run_with_refresh(
            &current,
            Some(self.credentials.as_ref()),
            &config,
            operation,
        )
        .await
        .map_err(anyhow::Error::from)?;
        Ok(value)
    }

    async fn ensure_fresh_operator(&self) -> Result<OperatorWithCredential> {
        let credential = self.credentials.current().await?.ok_or_else(|| {
            anyhow::anyhow!("snapshot OSS client requires non-anonymous credentials")
        })?;

        {
            let cached = self.cached_operator.read().await;
            if let Some(state) = cached.as_ref() {
                if state.credential() == Some(&credential) {
                    return Ok(state.clone());
                }
            }
        }

        let entry = OperatorWithCredential::new(
            build_object_store_operator(&self.operator_config, Some(&credential))?,
            Some(credential),
        );
        *self.cached_operator.write().await = Some(entry.clone());
        Ok(entry)
    }
}

fn detect_addressing_style(endpoint: &str, bucket: &str) -> Result<AddressingStyle> {
    let url = Url::parse(endpoint).context("parse snapshot OSS endpoint for addressing style")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("snapshot OSS endpoint host is missing"))?;
    let bucket_host = format!("{bucket}.");
    let is_bucket_virtual_host = host.starts_with(&bucket_host);

    if is_bucket_virtual_host {
        return Ok(AddressingStyle::Virtual);
    }
    Ok(AddressingStyle::Path)
}

fn is_aliyun_endpoint(endpoint: &str) -> Result<bool> {
    let url = Url::parse(endpoint).context("parse snapshot OSS endpoint")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("snapshot OSS endpoint host is missing"))?
        .to_ascii_lowercase();
    Ok(host.ends_with(".aliyuncs.com") || host.ends_with(".aliyun-inc.com"))
}

#[derive(Clone, Debug)]
enum ConditionalWrite {
    IfNotExists,
    IfMatch(String),
}

impl ConditionalWrite {
    fn operation_name(&self) -> &'static str {
        match self {
            Self::IfNotExists => "put_bytes_if_not_exists",
            Self::IfMatch(_) => "put_bytes_if_match",
        }
    }
}

fn ensure_conditional_write_capability(
    operator: &Operator,
    condition: &ConditionalWrite,
) -> opendal::Result<()> {
    let capability = operator.info().full_capability();
    let (supported, operation) = match condition {
        ConditionalWrite::IfNotExists => (capability.write_with_if_not_exists, "if_not_exists"),
        ConditionalWrite::IfMatch(_) => (capability.write_with_if_match, "if_match"),
    };
    if supported {
        return Ok(());
    }
    Err(OpenDalError::new(
        OpenDalErrorKind::Unsupported,
        format!("object-store backend does not support conditional write '{operation}'"),
    ))
}

fn etag_from_metadata(metadata: opendal::Metadata, key: &str) -> opendal::Result<String> {
    metadata
        .etag()
        .filter(|etag| !etag.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            OpenDalError::new(
                OpenDalErrorKind::Unexpected,
                "conditional write/stat response did not include an ETag",
            )
            .with_context("path", key)
        })
}

async fn download_object_to_file(
    operator: &Operator,
    key: &str,
    dest: &Path,
) -> opendal::Result<u64> {
    // Keep the tempfile handle alive until the final rename so any early
    // return still benefits from `NamedTempFile`'s automatic cleanup.
    let tmp = tempfile::NamedTempFile::new_in(dest.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|err| io_error_to_opendal(err, "create temporary download file"))?;
    let tmp_path = tmp.path().to_path_buf();
    let std_file = tmp
        .reopen()
        .map_err(|err| io_error_to_opendal(err, "reopen temporary download file"))?;
    let mut file = tokio::fs::File::from_std(std_file);
    let mut size = 0_u64;
    let mut stream = operator.reader(key).await?.into_stream(..).await?;
    while let Some(buffer) = stream.try_next().await? {
        for chunk in buffer {
            size += chunk.len() as u64;
            file.write_all(chunk.as_ref())
                .await
                .map_err(|err| io_error_to_opendal(err, "write downloaded object chunk"))?;
        }
    }

    file.flush()
        .await
        .map_err(|err| io_error_to_opendal(err, "flush downloaded object file"))?;
    file.sync_all()
        .await
        .map_err(|err| io_error_to_opendal(err, "sync downloaded object file"))?;
    drop(file);
    tokio::fs::rename(&tmp_path, dest)
        .await
        .map_err(|err| io_error_to_opendal(err, "rename downloaded object into place"))?;

    Ok(size)
}

async fn write_bytes_to_operator(
    operator: &Operator,
    key: &str,
    data: Bytes,
) -> opendal::Result<()> {
    operator.write(key, data).await.map(|_| ())
}

async fn upload_file_to_operator(
    operator: &Operator,
    key: &str,
    path: &Path,
) -> opendal::Result<()> {
    let mut writer = operator
        .writer_with(key)
        .chunk(CHUNK_SIZE)
        .concurrent(UPLOAD_CONCURRENCY)
        .await?;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|err| io_error_to_opendal(err, "open upload source file"))?;
    let mut buf = vec![0_u8; CHUNK_SIZE];
    loop {
        let read = file
            .read(&mut buf)
            .await
            .map_err(|err| io_error_to_opendal(err, "read upload source file"))?;
        if read == 0 {
            break;
        }
        writer.write(buf[..read].to_vec()).await?;
    }
    writer.close().await?;
    Ok(())
}

fn io_error_to_opendal(error: std::io::Error, message: &'static str) -> OpenDalError {
    OpenDalError::new(OpenDalErrorKind::Unexpected, message).set_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_override_takes_precedence_over_detection() {
        let detected = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("build client with detected style");
        assert_eq!(
            detected.operator_config.addressing_style,
            AddressingStyle::Path
        );

        let overridden = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .expect("build client with override");
        assert_eq!(
            overridden.operator_config.addressing_style,
            AddressingStyle::Virtual
        );
    }

    #[test]
    fn explicit_override_still_validates_endpoint() {
        OssClient::new(
            "snapshots".to_string(),
            "not a valid endpoint".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .expect_err("malformed endpoint must fail even with an explicit override");
    }

    #[test]
    fn native_aliyun_endpoints_are_rejected_before_runtime_use() {
        for endpoint in [
            "https://oss-cn-hangzhou.aliyuncs.com",
            "https://OSS-CN-HANGZHOU.ALIYUNCS.COM",
            "https://oss-cn-hangzhou.aliyun-inc.com",
        ] {
            let error = OssClient::new(
                "snapshots".to_string(),
                endpoint.to_string(),
                "cn-hangzhou".to_string(),
                String::new(),
                CredentialSource::Anonymous,
                None,
            )
            .expect_err("native Alibaba OSS endpoints must fail at construction");
            assert!(error
                .to_string()
                .contains("CAS-capable S3-compatible endpoint"));
        }
    }

    #[test]
    fn generic_s3_endpoint_is_accepted() {
        OssClient::new(
            "snapshots".to_string(),
            "http://minio.local:9000".to_string(),
            "us-east-1".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("generic S3-compatible endpoint should be accepted");
    }

    #[test]
    fn etag_extraction_preserves_wire_representation() {
        let metadata =
            opendal::Metadata::new(opendal::EntryMode::FILE).with_etag("\"abc123\"".to_string());
        assert_eq!(
            etag_from_metadata(metadata, "catalog/record.json").unwrap(),
            "\"abc123\""
        );

        let missing = opendal::Metadata::new(opendal::EntryMode::FILE);
        let error = etag_from_metadata(missing, "catalog/record.json").unwrap_err();
        assert_eq!(error.kind(), OpenDalErrorKind::Unexpected);
    }

    #[test]
    fn capability_gate_fails_closed_for_backends_without_conditional_writes() {
        let operator = Operator::new(opendal::services::Memory::default())
            .expect("build memory operator")
            .finish();

        let if_not_exists =
            ensure_conditional_write_capability(&operator, &ConditionalWrite::IfNotExists)
                .expect_err("memory backend must not claim conditional writes");
        assert_eq!(if_not_exists.kind(), OpenDalErrorKind::Unsupported);

        let if_match = ensure_conditional_write_capability(
            &operator,
            &ConditionalWrite::IfMatch(String::new()),
        )
        .expect_err("memory backend must not claim ETag CAS");
        assert_eq!(if_match.kind(), OpenDalErrorKind::Unsupported);
    }

    #[test]
    fn condition_failures_are_classified_without_retry() {
        let error = anyhow::Error::new(OpenDalError::new(
            OpenDalErrorKind::ConditionNotMatch,
            "stale etag",
        ));
        assert!(OssClient::is_condition_not_match_error(&error));
        assert!(!OssClient::is_not_found_error(&error));
    }
}
