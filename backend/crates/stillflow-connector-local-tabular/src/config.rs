use std::path::PathBuf;

use serde::Deserialize;
use stillflow_core::{ConnectorError, ConnectorKind, ConnectorResult, SourceConnection};

pub(crate) const DEFAULT_MAX_DISCOVERY_DEPTH: usize = 16;
pub(crate) const MAX_DISCOVERY_DEPTH: usize = 64;
pub(crate) const DEFAULT_MAX_DISCOVERED_ASSETS: usize = 10_000;
pub(crate) const MAX_DISCOVERED_ASSETS: usize = 100_000;
pub(crate) const DEFAULT_INFERENCE_ROWS: usize = 10_000;
pub(crate) const MAX_INFERENCE_ROWS: usize = 100_000;
pub(crate) const DEFAULT_INFERENCE_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_INFERENCE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct LocalTabularConfig {
    pub(crate) allowed_roots: Vec<PathBuf>,
    pub(crate) max_discovery_depth: usize,
    pub(crate) max_discovered_assets: usize,
    pub(crate) inference_rows: usize,
    pub(crate) inference_bytes: usize,
    pub(crate) csv_delimiter: u8,
    pub(crate) csv_quote: u8,
    pub(crate) csv_has_header: bool,
    pub(crate) tsv_has_header: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawConfig {
    allowed_roots: Vec<String>,
    #[serde(default = "default_discovery_depth")]
    max_discovery_depth: usize,
    #[serde(default = "default_discovered_assets")]
    max_discovered_assets: usize,
    #[serde(default)]
    schema_inference: RawInference,
    #[serde(default)]
    csv: RawCsv,
    #[serde(default)]
    tsv: RawTsv,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawInference {
    #[serde(default = "default_inference_rows")]
    max_rows: usize,
    #[serde(default = "default_inference_bytes")]
    max_bytes: usize,
}

impl Default for RawInference {
    fn default() -> Self {
        Self {
            max_rows: default_inference_rows(),
            max_bytes: default_inference_bytes(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawCsv {
    #[serde(default = "default_csv_delimiter")]
    delimiter: String,
    #[serde(default = "default_csv_quote")]
    quote: String,
    #[serde(default = "default_true")]
    has_header: bool,
}

impl Default for RawCsv {
    fn default() -> Self {
        Self {
            delimiter: default_csv_delimiter(),
            quote: default_csv_quote(),
            has_header: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawTsv {
    #[serde(default = "default_true")]
    has_header: bool,
}

impl Default for RawTsv {
    fn default() -> Self {
        Self { has_header: true }
    }
}

impl LocalTabularConfig {
    pub(crate) fn parse(connection: &SourceConnection) -> ConnectorResult<Self> {
        if connection.kind() != ConnectorKind::LocalFile {
            return Err(ConnectorError::invalid_configuration(
                "local tabular connector requires a localFile connection",
            ));
        }
        let raw: RawConfig = serde_json::from_value(connection.config().clone()).map_err(|_| {
            ConnectorError::invalid_configuration("invalid local tabular configuration")
        })?;
        if raw.allowed_roots.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "allowedRoots must contain at least one directory",
            ));
        }
        if raw.max_discovery_depth > MAX_DISCOVERY_DEPTH {
            return Err(ConnectorError::invalid_configuration(
                "maxDiscoveryDepth exceeds the supported maximum",
            ));
        }
        if !(1..=MAX_DISCOVERED_ASSETS).contains(&raw.max_discovered_assets) {
            return Err(ConnectorError::invalid_configuration(
                "maxDiscoveredAssets is outside the supported range",
            ));
        }
        if !(1..=MAX_INFERENCE_ROWS).contains(&raw.schema_inference.max_rows) {
            return Err(ConnectorError::invalid_configuration(
                "schemaInference.maxRows is outside the supported range",
            ));
        }
        if !(1..=MAX_INFERENCE_BYTES).contains(&raw.schema_inference.max_bytes) {
            return Err(ConnectorError::invalid_configuration(
                "schemaInference.maxBytes is outside the supported range",
            ));
        }

        let delimiter = one_ascii_byte(&raw.csv.delimiter, "csv.delimiter")?;
        let quote = one_ascii_byte(&raw.csv.quote, "csv.quote")?;
        if delimiter == quote || matches!(delimiter, b'\n' | b'\r' | 0) {
            return Err(ConnectorError::invalid_configuration(
                "csv.delimiter conflicts with quote, newline, or NUL",
            ));
        }
        if matches!(quote, b'\n' | b'\r' | 0) {
            return Err(ConnectorError::invalid_configuration(
                "csv.quote must not be newline or NUL",
            ));
        }

        Ok(Self {
            allowed_roots: raw.allowed_roots.into_iter().map(PathBuf::from).collect(),
            max_discovery_depth: raw.max_discovery_depth,
            max_discovered_assets: raw.max_discovered_assets,
            inference_rows: raw.schema_inference.max_rows,
            inference_bytes: raw.schema_inference.max_bytes,
            csv_delimiter: delimiter,
            csv_quote: quote,
            csv_has_header: raw.csv.has_header,
            tsv_has_header: raw.tsv.has_header,
        })
    }
}

fn one_ascii_byte(value: &str, field: &'static str) -> ConnectorResult<u8> {
    let bytes = value.as_bytes();
    let [byte] = bytes else {
        return Err(ConnectorError::invalid_configuration(format!(
            "{field} must be exactly one ASCII byte"
        )));
    };
    if !byte.is_ascii() {
        return Err(ConnectorError::invalid_configuration(format!(
            "{field} must be exactly one ASCII byte"
        )));
    }
    Ok(*byte)
}

const fn default_discovery_depth() -> usize {
    DEFAULT_MAX_DISCOVERY_DEPTH
}

const fn default_discovered_assets() -> usize {
    DEFAULT_MAX_DISCOVERED_ASSETS
}

const fn default_inference_rows() -> usize {
    DEFAULT_INFERENCE_ROWS
}

const fn default_inference_bytes() -> usize {
    DEFAULT_INFERENCE_BYTES
}

fn default_csv_delimiter() -> String {
    ",".to_owned()
}

fn default_csv_quote() -> String {
    "\"".to_owned()
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use stillflow_core::CredentialRef;

    use super::*;

    fn connection(config: serde_json::Value) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::LocalFile,
            "local",
            config,
            CredentialRef::new("cred://local/files").expect("credential reference"),
        )
        .expect("connection")
    }

    #[test]
    fn applies_defaults_and_rejects_unknown_or_invalid_values() {
        let parsed = LocalTabularConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["/data"]
        })))
        .expect("default config");
        assert_eq!(parsed.max_discovery_depth, DEFAULT_MAX_DISCOVERY_DEPTH);
        assert_eq!(parsed.csv_delimiter, b',');

        for invalid in [
            serde_json::json!({"allowedRoots": []}),
            serde_json::json!({"allowedRoots": ["/data"], "unknown": true}),
            serde_json::json!({"allowedRoots": ["/data"], "maxDiscoveryDepth": 65}),
            serde_json::json!({"allowedRoots": ["/data"], "maxDiscoveredAssets": 0}),
            serde_json::json!({"allowedRoots": ["/data"], "schemaInference": {"maxRows": 0}}),
            serde_json::json!({"allowedRoots": ["/data"], "schemaInference": {"maxBytes": 0}}),
            serde_json::json!({"allowedRoots": ["/data"], "csv": {"delimiter": "::"}}),
            serde_json::json!({"allowedRoots": ["/data"], "csv": {"delimiter": "\""}}),
        ] {
            LocalTabularConfig::parse(&connection(invalid)).expect_err("invalid config");
        }

        assert!(SourceConnection::try_new(
            ConnectorKind::LocalFile,
            "secret-bearing",
            serde_json::json!({"allowedRoots": ["/data"], "apiToken": "embedded"}),
            CredentialRef::new("cred://local/files").expect("credential reference"),
        )
        .is_err());
    }
}
