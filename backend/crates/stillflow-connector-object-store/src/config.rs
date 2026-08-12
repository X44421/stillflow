use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use stillflow_core::{ConnectorError, ConnectorKind, ConnectorResult, SourceConnection};

pub(crate) const DEFAULT_MAX_DISCOVERED_ASSETS: usize = 10_000;
pub(crate) const MAX_DISCOVERED_ASSETS: usize = 100_000;
pub(crate) const MAX_OBJECT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
pub(crate) const DEFAULT_PREVIEW_SOURCE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_PREVIEW_SOURCE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 30_000;
pub(crate) const MAX_TIMEOUT_MS: u64 = 300_000;
pub(crate) const MAX_KEY_BYTES: usize = 1_024;
pub(crate) const MAX_UPLOAD_CHUNKS: usize = 1_000_000;

#[derive(Debug, Clone)]
pub(crate) struct ObjectStoreConfig {
    pub(crate) provider: ProviderConfig,
    pub(crate) prefix: Option<String>,
    pub(crate) max_discovered_assets: usize,
    pub(crate) max_object_bytes: u64,
    pub(crate) max_preview_source_bytes: usize,
    pub(crate) request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(crate) enum ProviderConfig {
    Local {
        root: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        path_style: bool,
        anonymous: bool,
        allow_http: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "provider",
    rename_all = "lowercase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum RawConfig {
    Local {
        root: String,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default = "default_discovered_assets")]
        max_discovered_assets: usize,
        #[serde(default = "default_max_object_bytes")]
        max_object_bytes: u64,
        #[serde(default = "default_preview_source_bytes")]
        max_preview_source_bytes: usize,
        #[serde(default = "default_timeout_ms")]
        request_timeout_ms: u64,
    },
    S3 {
        bucket: String,
        #[serde(default = "default_region")]
        region: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default = "default_true")]
        path_style: bool,
        #[serde(default)]
        anonymous: bool,
        #[serde(default)]
        allow_http: bool,
        #[serde(default = "default_discovered_assets")]
        max_discovered_assets: usize,
        #[serde(default = "default_max_object_bytes")]
        max_object_bytes: u64,
        #[serde(default = "default_preview_source_bytes")]
        max_preview_source_bytes: usize,
        #[serde(default = "default_timeout_ms")]
        request_timeout_ms: u64,
    },
}

impl ObjectStoreConfig {
    pub(crate) fn parse(connection: &SourceConnection) -> ConnectorResult<Self> {
        if connection.kind() != ConnectorKind::ObjectStore {
            return Err(ConnectorError::invalid_configuration(
                "object storage connector requires an objectStore connection",
            ));
        }
        let raw: RawConfig = serde_json::from_value(connection.config().clone()).map_err(|_| {
            ConnectorError::invalid_configuration("invalid object storage configuration")
        })?;
        let (provider, prefix, assets, object_bytes, preview_bytes, timeout_ms) = match raw {
            RawConfig::Local {
                root,
                prefix,
                max_discovered_assets,
                max_object_bytes,
                max_preview_source_bytes,
                request_timeout_ms,
            } => {
                let root = PathBuf::from(root);
                if !root.is_absolute() {
                    return Err(ConnectorError::invalid_configuration(
                        "local object storage root must be absolute",
                    ));
                }
                (
                    ProviderConfig::Local { root },
                    prefix,
                    max_discovered_assets,
                    max_object_bytes,
                    max_preview_source_bytes,
                    request_timeout_ms,
                )
            }
            RawConfig::S3 {
                bucket,
                region,
                endpoint,
                prefix,
                path_style,
                anonymous,
                allow_http,
                max_discovered_assets,
                max_object_bytes,
                max_preview_source_bytes,
                request_timeout_ms,
            } => {
                validate_bucket(&bucket)?;
                validate_region(&region)?;
                validate_endpoint(endpoint.as_deref(), allow_http)?;
                (
                    ProviderConfig::S3 {
                        bucket,
                        region,
                        endpoint,
                        path_style,
                        anonymous,
                        allow_http,
                    },
                    prefix,
                    max_discovered_assets,
                    max_object_bytes,
                    max_preview_source_bytes,
                    request_timeout_ms,
                )
            }
        };
        if !(1..=MAX_DISCOVERED_ASSETS).contains(&assets) {
            return Err(ConnectorError::invalid_configuration(
                "maxDiscoveredAssets is outside the supported range",
            ));
        }
        if !(1..=MAX_OBJECT_BYTES).contains(&object_bytes) {
            return Err(ConnectorError::invalid_configuration(
                "maxObjectBytes is outside the supported range",
            ));
        }
        if !(1..=MAX_PREVIEW_SOURCE_BYTES).contains(&preview_bytes) {
            return Err(ConnectorError::invalid_configuration(
                "maxPreviewSourceBytes is outside the supported range",
            ));
        }
        if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(ConnectorError::invalid_configuration(
                "requestTimeoutMs is outside the supported range",
            ));
        }
        if prefix
            .as_ref()
            .is_some_and(|value| value.len() > MAX_KEY_BYTES)
        {
            return Err(ConnectorError::invalid_configuration(
                "object storage prefix exceeds the supported length",
            ));
        }
        Ok(Self {
            provider,
            prefix,
            max_discovered_assets: assets,
            max_object_bytes: object_bytes,
            max_preview_source_bytes: preview_bytes,
            request_timeout: Duration::from_millis(timeout_ms),
        })
    }
}

fn validate_bucket(bucket: &str) -> ConnectorResult<()> {
    if !(3..=63).contains(&bucket.len())
        || !bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        || !bucket
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(ConnectorError::invalid_configuration(
            "S3 bucket name is invalid",
        ));
    }
    Ok(())
}

fn validate_region(region: &str) -> ConnectorResult<()> {
    if region.is_empty()
        || region.len() > 128
        || !region
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ConnectorError::invalid_configuration(
            "S3 region is invalid",
        ));
    }
    Ok(())
}

fn validate_endpoint(endpoint: Option<&str>, allow_http: bool) -> ConnectorResult<()> {
    let Some(endpoint) = endpoint else {
        if allow_http {
            return Err(ConnectorError::invalid_configuration(
                "allowHttp requires an explicit development endpoint",
            ));
        }
        return Ok(());
    };
    if endpoint.len() > 2_048
        || endpoint.bytes().any(|byte| byte.is_ascii_whitespace())
        || endpoint
            .chars()
            .any(|character| matches!(character, '?' | '#' | '@'))
        || endpoint.ends_with('/')
    {
        return Err(ConnectorError::invalid_configuration(
            "S3 endpoint is invalid",
        ));
    }
    if let Some(authority) = endpoint.strip_prefix("https://") {
        if authority.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "S3 endpoint is invalid",
            ));
        }
        return Ok(());
    }
    let Some(authority) = endpoint.strip_prefix("http://") else {
        return Err(ConnectorError::invalid_configuration(
            "S3 endpoint must use HTTPS",
        ));
    };
    let authority = authority
        .split_once('/')
        .map_or(authority, |(authority, _)| authority);
    let host = if let Some(ipv6) = authority.strip_prefix('[') {
        ipv6.split_once(']').map_or(ipv6, |(host, _)| host)
    } else {
        authority
            .split_once(':')
            .map_or(authority, |(host, _)| host)
    };
    if !allow_http || !is_development_host(host) {
        return Err(ConnectorError::invalid_configuration(
            "plain HTTP is restricted to explicit local development endpoints",
        ));
    }
    Ok(())
}

fn is_development_host(host: &str) -> bool {
    if matches!(host, "localhost" | "::1")
        || host.starts_with("127.")
        || host.starts_with("10.")
        || host.starts_with("192.168.")
    {
        return true;
    }
    let mut parts = host.split('.');
    matches!(
        (
            parts.next(),
            parts.next().and_then(|part| part.parse::<u8>().ok())
        ),
        (Some("172"), Some(16..=31))
    )
}

const fn default_discovered_assets() -> usize {
    DEFAULT_MAX_DISCOVERED_ASSETS
}

const fn default_max_object_bytes() -> u64 {
    MAX_OBJECT_BYTES
}

const fn default_preview_source_bytes() -> usize {
    DEFAULT_PREVIEW_SOURCE_BYTES
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use stillflow_core::{CredentialRef, SourceConnection};

    use super::*;

    fn connection(config: serde_json::Value) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::ObjectStore,
            "objects",
            config,
            CredentialRef::new("cred://tests/objects").expect("credential reference"),
        )
        .expect("connection")
    }

    #[test]
    fn parses_strict_local_and_s3_configuration() {
        let local = ObjectStoreConfig::parse(&connection(serde_json::json!({
            "provider": "local",
            "root": std::env::temp_dir()
        })))
        .expect("local config");
        assert_eq!(local.max_discovered_assets, DEFAULT_MAX_DISCOVERED_ASSETS);

        let s3 = ObjectStoreConfig::parse(&connection(serde_json::json!({
            "provider": "s3",
            "bucket": "stillflow-tests",
            "endpoint": "http://127.0.0.1:9000",
            "allowHttp": true
        })))
        .expect("S3 config");
        assert!(matches!(s3.provider, ProviderConfig::S3 { .. }));

        for invalid in [
            serde_json::json!({"provider":"local","root":"relative"}),
            serde_json::json!({"provider":"s3","bucket":"BAD_BUCKET"}),
            serde_json::json!({"provider":"s3","bucket":"valid-bucket","endpoint":"http://example.com","allowHttp":true}),
            serde_json::json!({"provider":"local","root":std::env::temp_dir(),"maxObjectBytes":0}),
            serde_json::json!({"provider":"local","root":std::env::temp_dir(),"unknown":true}),
        ] {
            ObjectStoreConfig::parse(&connection(invalid)).expect_err("invalid config");
        }
    }
}
