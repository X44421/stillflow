use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::path::{Component, Path as FilePath, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore, PutPayload};
use stillflow_core::{
    ConnectorError, ConnectorResult, ErrorCategory, RequestContext, SourceConnection,
};
use tokio::time::Instant;

use crate::config::{ObjectStoreConfig, ProviderConfig, MAX_KEY_BYTES, MAX_UPLOAD_CHUNKS};
use crate::credentials::ObjectStoreCredentialResolver;

const MULTIPART_CHUNK_BYTES: usize = 5 * 1024 * 1024;

/// Bounded byte stream used by object reads and uploads.
pub type ObjectByteStream = Pin<Box<dyn Stream<Item = ConnectorResult<Bytes>> + Send + 'static>>;

/// Provider-neutral object metadata safe to keep inside the server boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    e_tag: Option<String>,
    version: Option<String>,
}

impl ObjectInfo {
    pub fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Unified server-side byte access for local and S3-compatible storage.
#[async_trait]
pub trait ObjectStorageAccess: Send + Sync {
    async fn list(
        &self,
        prefix: &str,
        context: &RequestContext,
    ) -> ConnectorResult<Vec<ObjectInfo>>;

    async fn head(&self, key: &str, context: &RequestContext) -> ConnectorResult<ObjectInfo>;

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
        context: &RequestContext,
    ) -> ConnectorResult<Bytes>;

    async fn stream(
        &self,
        key: &str,
        context: &RequestContext,
    ) -> ConnectorResult<ObjectByteStream>;

    async fn upload(
        &self,
        key: &str,
        body: ObjectByteStream,
        context: &RequestContext,
    ) -> ConnectorResult<ObjectInfo>;
}

#[derive(Clone)]
pub(crate) struct StoreAccess {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    local_root: Option<PathBuf>,
    container: String,
    config: ObjectStoreConfig,
}

impl std::fmt::Debug for StoreAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreAccess")
            .field("provider", &self.container)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl StoreAccess {
    pub(crate) async fn open(
        connection: &SourceConnection,
        resolver: &dyn ObjectStoreCredentialResolver,
        context: &RequestContext,
    ) -> ConnectorResult<Self> {
        context.ensure_active()?;
        let config = ObjectStoreConfig::parse(connection)?;
        let prefix = config
            .prefix
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| parse_relative(value, true))
            .transpose()?;
        let (store, local_root, container): (Arc<dyn ObjectStore>, _, _) = match &config.provider {
            ProviderConfig::Local { root } => {
                validate_absolute_root(root)?;
                let store = LocalFileSystem::new_with_prefix(root)
                    .map_err(|error| map_store_error(error, "initialize local object storage"))?;
                (Arc::new(store), Some(root.clone()), "local".to_owned())
            }
            ProviderConfig::S3 {
                bucket,
                region,
                endpoint,
                path_style,
                anonymous,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_virtual_hosted_style_request(!path_style)
                    .with_allow_http(*allow_http);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if *anonymous {
                    builder = builder.with_skip_signature(true);
                } else {
                    let material = run_control(
                        context,
                        config.request_timeout,
                        resolver.resolve_s3(connection.credential_ref()),
                    )
                    .await?;
                    let (access_key, secret_key, token) = material.take_parts();
                    builder = builder
                        .with_access_key_id(access_key)
                        .with_secret_access_key(secret_key);
                    if let Some(token) = token {
                        builder = builder.with_token(token);
                    }
                }
                let store = builder.build().map_err(|error| {
                    map_store_error(error, "initialize S3-compatible object storage")
                })?;
                (Arc::new(store), None, bucket.clone())
            }
        };
        Ok(Self {
            store,
            prefix,
            local_root,
            container,
            config,
        })
    }

    pub(crate) fn container(&self) -> &str {
        &self.container
    }

    pub(crate) fn identity_scope(&self) -> String {
        format!(
            "{}:{}",
            self.container,
            self.prefix.as_ref().map_or("", |path| path.as_ref())
        )
    }

    pub(crate) const fn max_object_bytes(&self) -> u64 {
        self.config.max_object_bytes
    }

    pub(crate) fn max_preview_source_bytes(&self) -> usize {
        self.config.max_preview_source_bytes
    }

    pub(crate) const fn request_timeout(&self) -> std::time::Duration {
        self.config.request_timeout
    }

    pub(crate) async fn probe(&self, context: &RequestContext) -> ConnectorResult<()> {
        context.ensure_active()?;
        let mut stream = self.store.list(self.prefix.as_ref());
        let next = run_control(context, self.config.request_timeout, async {
            stream
                .next()
                .await
                .transpose()
                .map_err(|error| map_store_error(error, "list object storage"))
        })
        .await?;
        if let Some(meta) = next {
            self.validate_internal_local_path(&meta.location, false)?;
        }
        Ok(())
    }

    pub(crate) async fn get_range_versioned(
        &self,
        key: &str,
        range: Range<u64>,
        expected: &ObjectInfo,
        context: &RequestContext,
    ) -> ConnectorResult<Bytes> {
        context.ensure_active()?;
        validate_range(&range, expected.size, self.config.max_preview_source_bytes)?;
        let location = self.location(key)?;
        self.validate_internal_local_path(&location, false)?;
        let options = GetOptions {
            if_match: expected.e_tag.clone(),
            version: expected.version.clone(),
            range: Some(GetRange::Bounded(range.clone())),
            ..Default::default()
        };
        let result = run_control(context, self.config.request_timeout, async {
            self.store
                .get_opts(&location, options)
                .await
                .map_err(|error| map_store_error(error, "read object range"))
        })
        .await?;
        ensure_same_object(expected, &result.meta)?;
        let bytes = run_control(context, self.config.request_timeout, async {
            result
                .bytes()
                .await
                .map_err(|error| map_store_error(error, "read object range body"))
        })
        .await?;
        let expected_length = usize::try_from(range.end - range.start).map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "object range length exceeds the platform range",
            )
        })?;
        if bytes.len() != expected_length {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "object range returned an unexpected byte count",
            ));
        }
        context.ensure_active()?;
        Ok(bytes)
    }

    fn location(&self, key: &str) -> ConnectorResult<Path> {
        let relative = parse_relative(key, false)?;
        let combined = match &self.prefix {
            Some(prefix) => Path::parse(format!("{prefix}/{relative}")),
            None => Ok(relative),
        }
        .map_err(|_| ConnectorError::invalid_configuration("object key is invalid"))?;
        Ok(combined)
    }

    fn list_location(&self, child_prefix: &str) -> ConnectorResult<Option<Path>> {
        if child_prefix.is_empty() {
            return Ok(self.prefix.clone());
        }
        self.location(child_prefix).map(Some)
    }

    fn relative_key(&self, location: &Path) -> ConnectorResult<String> {
        let raw = location.as_ref();
        let relative = match &self.prefix {
            None => raw,
            Some(prefix) if raw == prefix.as_ref() => "",
            Some(prefix) => raw
                .strip_prefix(prefix.as_ref())
                .and_then(|value| value.strip_prefix('/'))
                .ok_or_else(|| {
                    source_error(
                        ErrorCategory::InvalidData,
                        false,
                        "object listing escaped the configured prefix",
                    )
                })?,
        };
        Ok(relative.to_owned())
    }

    fn info_from_meta(
        &self,
        meta: ObjectMeta,
        enforce_byte_limit: bool,
    ) -> ConnectorResult<ObjectInfo> {
        if enforce_byte_limit && meta.size > self.config.max_object_bytes {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "object exceeds the configured byte limit",
            ));
        }
        Ok(ObjectInfo {
            key: self.relative_key(&meta.location)?,
            size: meta.size,
            last_modified: meta.last_modified,
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    fn validate_internal_local_path(
        &self,
        location: &Path,
        allow_missing_leaf: bool,
    ) -> ConnectorResult<()> {
        let Some(root) = &self.local_root else {
            return Ok(());
        };
        validate_local_components(root, location.as_ref(), allow_missing_leaf)
    }

    async fn abort_upload(&self, upload: &mut Box<dyn object_store::MultipartUpload>) {
        let _ = tokio::time::timeout(self.config.request_timeout, upload.abort()).await;
    }
}

#[async_trait]
impl ObjectStorageAccess for StoreAccess {
    async fn list(
        &self,
        prefix: &str,
        context: &RequestContext,
    ) -> ConnectorResult<Vec<ObjectInfo>> {
        context.ensure_active()?;
        let location = self.list_location(prefix)?;
        if let Some(location) = &location {
            self.validate_internal_local_path(location, true)?;
        }
        let mut stream = self.store.list(location.as_ref());
        let mut objects = Vec::new();
        loop {
            let next = run_control(context, self.config.request_timeout, async {
                stream
                    .next()
                    .await
                    .transpose()
                    .map_err(|error| map_store_error(error, "list object storage"))
            })
            .await?;
            let Some(meta) = next else {
                break;
            };
            self.validate_internal_local_path(&meta.location, false)?;
            if objects.len() >= self.config.max_discovered_assets {
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "object listing exceeds the configured asset limit",
                ));
            }
            objects.push(self.info_from_meta(meta, false)?);
        }
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        if objects
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.key == right.key))
        {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "object listing contains duplicate keys",
            ));
        }
        context.ensure_active()?;
        Ok(objects)
    }

    async fn head(&self, key: &str, context: &RequestContext) -> ConnectorResult<ObjectInfo> {
        context.ensure_active()?;
        let location = self.location(key)?;
        self.validate_internal_local_path(&location, false)?;
        let meta = run_control(context, self.config.request_timeout, async {
            self.store
                .head(&location)
                .await
                .map_err(|error| map_store_error(error, "inspect object metadata"))
        })
        .await?;
        self.info_from_meta(meta, true)
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
        context: &RequestContext,
    ) -> ConnectorResult<Bytes> {
        let expected = self.head(key, context).await?;
        self.get_range_versioned(key, range, &expected, context)
            .await
    }

    async fn stream(
        &self,
        key: &str,
        context: &RequestContext,
    ) -> ConnectorResult<ObjectByteStream> {
        let expected = self.head(key, context).await?;
        let location = self.location(key)?;
        self.validate_internal_local_path(&location, false)?;
        let options = GetOptions {
            if_match: expected.e_tag.clone(),
            version: expected.version.clone(),
            ..Default::default()
        };
        let result = run_control(context, self.config.request_timeout, async {
            self.store
                .get_opts(&location, options)
                .await
                .map_err(|error| map_store_error(error, "stream object"))
        })
        .await?;
        ensure_same_object(&expected, &result.meta)?;
        let state = StreamState {
            inner: result.into_stream(),
            context: context.clone(),
            timeout: self.config.request_timeout,
            expected_size: expected.size,
            bytes_seen: 0,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            let context = state.context.clone();
            let next = run_control(&context, state.timeout, async {
                state
                    .inner
                    .next()
                    .await
                    .transpose()
                    .map_err(|error| map_store_error(error, "stream object body"))
            })
            .await?;
            match next {
                Some(bytes) => {
                    state.bytes_seen = state
                        .bytes_seen
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| {
                            source_error(
                                ErrorCategory::InvalidData,
                                false,
                                "object stream byte count overflow",
                            )
                        })?;
                    if state.bytes_seen > state.expected_size {
                        return Err(source_error(
                            ErrorCategory::InvalidData,
                            false,
                            "object stream exceeded its declared size",
                        ));
                    }
                    Ok(Some((bytes, state)))
                }
                None if state.bytes_seen != state.expected_size => Err(source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "object stream ended before its declared size",
                )),
                None => Ok(None),
            }
        });
        Ok(Box::pin(stream))
    }

    async fn upload(
        &self,
        key: &str,
        mut body: ObjectByteStream,
        context: &RequestContext,
    ) -> ConnectorResult<ObjectInfo> {
        context.ensure_active()?;
        let location = self.location(key)?;
        self.validate_internal_local_path(&location, true)?;
        let mut upload = run_control(context, self.config.request_timeout, async {
            self.store
                .put_multipart(&location)
                .await
                .map_err(|error| map_store_error(error, "start object upload"))
        })
        .await?;
        let mut buffered = BytesMut::with_capacity(MULTIPART_CHUNK_BYTES);
        let mut bytes_seen = 0_u64;
        let mut chunks_seen = 0_usize;
        loop {
            let next = run_control(context, self.config.request_timeout, async {
                body.next().await.transpose()
            })
            .await;
            let next = match next {
                Ok(next) => next,
                Err(error) => {
                    self.abort_upload(&mut upload).await;
                    return Err(error);
                }
            };
            let Some(bytes) = next else {
                break;
            };
            chunks_seen = chunks_seen.checked_add(1).ok_or_else(|| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "object upload chunk count overflow",
                )
            })?;
            bytes_seen = bytes_seen.checked_add(bytes.len() as u64).ok_or_else(|| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "object upload byte count overflow",
                )
            })?;
            if chunks_seen > MAX_UPLOAD_CHUNKS || bytes_seen > self.config.max_object_bytes {
                self.abort_upload(&mut upload).await;
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "object upload exceeds the configured resource limit",
                ));
            }
            buffered.extend_from_slice(&bytes);
            while buffered.len() >= MULTIPART_CHUNK_BYTES {
                let part = buffered.split_to(MULTIPART_CHUNK_BYTES).freeze();
                let future = upload.put_part(PutPayload::from(part));
                if let Err(error) = run_control(context, self.config.request_timeout, async {
                    future
                        .await
                        .map_err(|error| map_store_error(error, "upload object part"))
                })
                .await
                {
                    self.abort_upload(&mut upload).await;
                    return Err(error);
                }
            }
        }
        if bytes_seen == 0 {
            self.abort_upload(&mut upload).await;
            run_control(context, self.config.request_timeout, async {
                self.store
                    .put(&location, PutPayload::from(Bytes::new()))
                    .await
                    .map_err(|error| map_store_error(error, "upload empty object"))
            })
            .await?;
        } else {
            if !buffered.is_empty() {
                let future = upload.put_part(PutPayload::from(buffered.freeze()));
                if let Err(error) = run_control(context, self.config.request_timeout, async {
                    future
                        .await
                        .map_err(|error| map_store_error(error, "upload final object part"))
                })
                .await
                {
                    self.abort_upload(&mut upload).await;
                    return Err(error);
                }
            }
            if let Err(error) = run_control(context, self.config.request_timeout, async {
                upload
                    .complete()
                    .await
                    .map_err(|error| map_store_error(error, "complete object upload"))
            })
            .await
            {
                self.abort_upload(&mut upload).await;
                return Err(error);
            }
        }
        let info = self.head(key, context).await?;
        if info.size != bytes_seen {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "uploaded object size does not match its input",
            ));
        }
        Ok(info)
    }
}

struct StreamState {
    inner: futures::stream::BoxStream<'static, object_store::Result<Bytes>>,
    context: RequestContext,
    timeout: std::time::Duration,
    expected_size: u64,
    bytes_seen: u64,
}

fn parse_relative(value: &str, prefix: bool) -> ConnectorResult<Path> {
    let value = if prefix {
        value.trim_end_matches('/')
    } else {
        value
    };
    if value.is_empty()
        || value.len() > MAX_KEY_BYTES
        || value.starts_with('/')
        || (!prefix && value.ends_with('/'))
        || value.contains('\\')
        || value
            .chars()
            .any(|character| character.is_control() || matches!(character, '?' | '#'))
        || contains_encoded_traversal(value)
        || value
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(ConnectorError::invalid_configuration(if prefix {
            "object storage prefix is invalid"
        } else {
            "object key is invalid"
        }));
    }
    Path::parse(value).map_err(|_| {
        ConnectorError::invalid_configuration(if prefix {
            "object storage prefix is invalid"
        } else {
            "object key is invalid"
        })
    })
}

fn contains_encoded_traversal(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("%2e") || lower.contains("%2f") || lower.contains("%5c")
}

fn validate_range(range: &Range<u64>, size: u64, maximum: usize) -> ConnectorResult<()> {
    let length = range
        .end
        .checked_sub(range.start)
        .ok_or_else(|| ConnectorError::invalid_configuration("object byte range is invalid"))?;
    if length == 0 || range.end > size || length > u64::try_from(maximum).unwrap_or(u64::MAX) {
        return Err(ConnectorError::invalid_configuration(
            "object byte range is outside the supported bounds",
        ));
    }
    Ok(())
}

fn ensure_same_object(expected: &ObjectInfo, actual: &ObjectMeta) -> ConnectorResult<()> {
    let same = if let Some(version) = &expected.version {
        actual.version.as_ref() == Some(version)
    } else if let Some(e_tag) = &expected.e_tag {
        actual.e_tag.as_ref() == Some(e_tag)
    } else {
        actual.size == expected.size && actual.last_modified == expected.last_modified
    };
    if !same {
        return Err(source_error(
            ErrorCategory::InvalidData,
            false,
            "object changed while it was being read",
        ));
    }
    Ok(())
}

fn validate_absolute_root(root: &FilePath) -> ConnectorResult<()> {
    if !root.is_absolute() {
        return Err(ConnectorError::invalid_configuration(
            "local object storage root must be absolute",
        ));
    }
    let mut current = PathBuf::new();
    for component in root.components() {
        current.push(component.as_os_str());
        if !matches!(component, Component::Normal(_)) {
            continue;
        }
        let metadata = std::fs::symlink_metadata(&current).map_err(|_| {
            ConnectorError::invalid_configuration("local object storage root is unavailable")
        })?;
        if metadata.file_type().is_symlink() {
            return Err(source_error(
                ErrorCategory::Authorization,
                false,
                "local object storage root must not traverse a link",
            ));
        }
    }
    if !root.is_dir() {
        return Err(ConnectorError::invalid_configuration(
            "local object storage root must be a directory",
        ));
    }
    Ok(())
}

fn validate_local_components(
    root: &FilePath,
    relative: &str,
    allow_missing_leaf: bool,
) -> ConnectorResult<()> {
    let mut current = root.to_path_buf();
    let components = relative.split('/').collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(source_error(
                    ErrorCategory::Authorization,
                    false,
                    "local object path must not traverse a link",
                ));
            }
            Ok(_) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && allow_missing_leaf
                    && index + 1 == components.len() => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing_leaf => {
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(source_error(
                    ErrorCategory::NotFound,
                    false,
                    "object was not found",
                ));
            }
            Err(_) => {
                return Err(source_error(
                    ErrorCategory::Authorization,
                    false,
                    "local object path is not accessible",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) async fn run_control<T, F>(
    context: &RequestContext,
    timeout: std::time::Duration,
    future: F,
) -> ConnectorResult<T>
where
    F: Future<Output = ConnectorResult<T>>,
{
    context.ensure_active()?;
    let configured = Instant::now() + timeout;
    let deadline = context
        .deadline()
        .map_or(configured, |deadline| deadline.min(configured));
    tokio::select! {
        biased;
        _ = context.cancellation().cancelled() => Err(ConnectorError::cancelled()),
        _ = tokio::time::sleep_until(deadline) => Err(ConnectorError::timeout("object storage request timed out")),
        result = future => result,
    }
}

fn map_store_error(error: object_store::Error, operation: &'static str) -> ConnectorError {
    let (category, retryable, detail) = match error {
        object_store::Error::NotFound { .. } => (ErrorCategory::NotFound, false, "not_found"),
        object_store::Error::PermissionDenied { .. } => {
            (ErrorCategory::Authorization, false, "permission_denied")
        }
        object_store::Error::Unauthenticated { .. } => {
            (ErrorCategory::Authentication, false, "unauthenticated")
        }
        object_store::Error::InvalidPath { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => (
            ErrorCategory::InvalidConfiguration,
            false,
            "invalid_configuration",
        ),
        object_store::Error::AlreadyExists { .. }
        | object_store::Error::Precondition { .. }
        | object_store::Error::NotModified { .. } => {
            (ErrorCategory::InvalidData, false, "precondition")
        }
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented => (
            ErrorCategory::UnsupportedCapability,
            false,
            "unsupported_operation",
        ),
        object_store::Error::Generic { .. } => {
            (ErrorCategory::TransientSource, true, "provider_failure")
        }
        object_store::Error::JoinError { .. } => (
            ErrorCategory::TransientSource,
            true,
            "provider_task_failure",
        ),
        _ => (ErrorCategory::TransientSource, true, "provider_failure"),
    };
    ConnectorError::with_category(
        category,
        retryable,
        format!("object storage could not {operation}"),
        vec![detail.to_owned()],
        BTreeMap::new(),
    )
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use stillflow_core::{ConnectorKind, CredentialRef};
    use tempfile::tempdir;

    use super::*;

    fn local_connection(root: &FilePath) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::ObjectStore,
            "local objects",
            serde_json::json!({
                "provider": "local",
                "root": root,
                "maxPreviewSourceBytes": 1024
            }),
            CredentialRef::new("cred://tests/local").expect("credential ref"),
        )
        .expect("connection")
    }

    #[tokio::test]
    async fn local_access_lists_ranges_streams_and_uploads() {
        let directory = tempdir().expect("tempdir");
        std::fs::write(directory.path().join("first.csv"), b"id,name\n1,Ada\n").expect("fixture");
        let context = RequestContext::new();
        let access = StoreAccess::open(
            &local_connection(directory.path()),
            &crate::credentials::RejectingCredentialResolver,
            &context,
        )
        .await
        .expect("access");

        let listed = access.list("", &context).await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "first.csv");
        assert_eq!(
            access
                .get_range("first.csv", 0..2, &context)
                .await
                .expect("range"),
            Bytes::from_static(b"id")
        );
        let streamed = access
            .stream("first.csv", &context)
            .await
            .expect("stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("stream body")
            .concat();
        assert_eq!(streamed, b"id,name\n1,Ada\n");

        let body: ObjectByteStream = Box::pin(futures::stream::iter([
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"objects")),
        ]));
        let uploaded = access
            .upload("nested/output.txt", body, &context)
            .await
            .expect("upload");
        assert_eq!(uploaded.size, 13);
        assert_eq!(
            std::fs::read(directory.path().join("nested/output.txt")).expect("uploaded file"),
            b"hello objects"
        );
    }

    #[tokio::test]
    async fn rejects_traversal_and_honours_cancellation() {
        let directory = tempdir().expect("tempdir");
        let context = RequestContext::new();
        let access = StoreAccess::open(
            &local_connection(directory.path()),
            &crate::credentials::RejectingCredentialResolver,
            &context,
        )
        .await
        .expect("access");
        for key in ["../secret", "safe/%2e%2e/secret", "/absolute", "a\\b"] {
            let error = access.head(key, &context).await.expect_err("invalid key");
            assert_eq!(error.category(), ErrorCategory::InvalidConfiguration);
        }

        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let cancelled = RequestContext::with_cancellation(token);
        assert_eq!(
            access
                .list("", &cancelled)
                .await
                .expect_err("cancelled")
                .category(),
            ErrorCategory::Cancelled
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_access_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("tempdir");
        let outside = tempdir().expect("outside");
        std::fs::write(outside.path().join("secret.csv"), b"secret").expect("secret");
        symlink(outside.path(), directory.path().join("escape")).expect("symlink");
        let context = RequestContext::new();
        let access = StoreAccess::open(
            &local_connection(directory.path()),
            &crate::credentials::RejectingCredentialResolver,
            &context,
        )
        .await
        .expect("access");
        let error = access
            .head("escape/secret.csv", &context)
            .await
            .expect_err("escape rejected");
        assert_eq!(error.category(), ErrorCategory::Authorization);
    }
}
