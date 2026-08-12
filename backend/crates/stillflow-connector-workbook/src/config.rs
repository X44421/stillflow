use std::path::PathBuf;

use serde::Deserialize;
use stillflow_core::{ConnectorError, ConnectorKind, ConnectorResult, SourceConnection};

pub(crate) const DEFAULT_MAX_DISCOVERY_DEPTH: usize = 16;
pub(crate) const MAX_DISCOVERY_DEPTH: usize = 64;
pub(crate) const DEFAULT_MAX_DISCOVERED_ASSETS: usize = 10_000;
pub(crate) const MAX_DISCOVERED_ASSETS: usize = 100_000;
pub(crate) const DEFAULT_MAX_WORKBOOK_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_WORKBOOK_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_ARCHIVE_ENTRIES: usize = 4_096;
pub(crate) const MAX_ARCHIVE_ENTRIES: usize = 16_384;
pub(crate) const DEFAULT_MAX_EXPANDED_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAX_EXPANDED_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_SHEET_CELLS: u64 = 2_000_000;
pub(crate) const MAX_SHEET_CELLS: u64 = 4_000_000;
pub(crate) const DEFAULT_MAX_REGION_CANDIDATES: usize = 128;
pub(crate) const MAX_REGION_CANDIDATES: usize = 1_024;
pub(crate) const DEFAULT_ANALYSIS_ROWS: usize = 10_000;
pub(crate) const MAX_ANALYSIS_ROWS: usize = 100_000;
pub(crate) const DEFAULT_ANALYSIS_COLUMNS: usize = 256;
pub(crate) const MAX_ANALYSIS_COLUMNS: usize = 4_096;

#[derive(Debug, Clone)]
pub(crate) struct WorkbookConfig {
    pub(crate) allowed_roots: Vec<PathBuf>,
    pub(crate) max_discovery_depth: usize,
    pub(crate) max_discovered_assets: usize,
    pub(crate) max_workbook_bytes: u64,
    pub(crate) max_archive_entries: usize,
    pub(crate) max_expanded_archive_bytes: u64,
    pub(crate) max_sheet_cells: u64,
    pub(crate) max_region_candidates: usize,
    pub(crate) analysis_rows: usize,
    pub(crate) analysis_columns: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawConfig {
    allowed_roots: Vec<String>,
    #[serde(default = "default_discovery_depth")]
    max_discovery_depth: usize,
    #[serde(default = "default_discovered_assets")]
    max_discovered_assets: usize,
    #[serde(default = "default_workbook_bytes")]
    max_workbook_bytes: u64,
    #[serde(default = "default_archive_entries")]
    max_archive_entries: usize,
    #[serde(default = "default_expanded_archive_bytes")]
    max_expanded_archive_bytes: u64,
    #[serde(default = "default_sheet_cells")]
    max_sheet_cells: u64,
    #[serde(default = "default_region_candidates")]
    max_region_candidates: usize,
    #[serde(default = "default_analysis_rows")]
    analysis_rows: usize,
    #[serde(default = "default_analysis_columns")]
    analysis_columns: usize,
}

impl WorkbookConfig {
    pub(crate) fn parse(connection: &SourceConnection) -> ConnectorResult<Self> {
        if connection.kind() != ConnectorKind::ExcelWorkbook {
            return Err(ConnectorError::invalid_configuration(
                "workbook connector requires an excelWorkbook connection",
            ));
        }
        let raw: RawConfig = serde_json::from_value(connection.config().clone())
            .map_err(|_| ConnectorError::invalid_configuration("invalid workbook configuration"))?;
        if raw.allowed_roots.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "allowedRoots must contain at least one directory",
            ));
        }
        bounded_usize(
            raw.max_discovery_depth,
            1,
            MAX_DISCOVERY_DEPTH,
            "maxDiscoveryDepth",
        )?;
        bounded_usize(
            raw.max_discovered_assets,
            1,
            MAX_DISCOVERED_ASSETS,
            "maxDiscoveredAssets",
        )?;
        bounded_u64(
            raw.max_workbook_bytes,
            1,
            MAX_WORKBOOK_BYTES,
            "maxWorkbookBytes",
        )?;
        bounded_usize(
            raw.max_archive_entries,
            1,
            MAX_ARCHIVE_ENTRIES,
            "maxArchiveEntries",
        )?;
        bounded_u64(
            raw.max_expanded_archive_bytes,
            1,
            MAX_EXPANDED_ARCHIVE_BYTES,
            "maxExpandedArchiveBytes",
        )?;
        bounded_u64(
            raw.max_sheet_cells,
            1,
            MAX_SHEET_CELLS,
            "maxSheetCells",
        )?;
        bounded_usize(
            raw.max_region_candidates,
            1,
            MAX_REGION_CANDIDATES,
            "maxRegionCandidates",
        )?;
        bounded_usize(raw.analysis_rows, 1, MAX_ANALYSIS_ROWS, "analysisRows")?;
        bounded_usize(
            raw.analysis_columns,
            1,
            MAX_ANALYSIS_COLUMNS,
            "analysisColumns",
        )?;
        let allowed_roots = raw
            .allowed_roots
            .into_iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        if allowed_roots.iter().any(|root| !root.is_absolute()) {
            return Err(ConnectorError::invalid_configuration(
                "allowedRoots entries must be absolute paths",
            ));
        }
        Ok(Self {
            allowed_roots,
            max_discovery_depth: raw.max_discovery_depth,
            max_discovered_assets: raw.max_discovered_assets,
            max_workbook_bytes: raw.max_workbook_bytes,
            max_archive_entries: raw.max_archive_entries,
            max_expanded_archive_bytes: raw.max_expanded_archive_bytes,
            max_sheet_cells: raw.max_sheet_cells,
            max_region_candidates: raw.max_region_candidates,
            analysis_rows: raw.analysis_rows,
            analysis_columns: raw.analysis_columns,
        })
    }
}

fn bounded_usize(
    value: usize,
    minimum: usize,
    maximum: usize,
    field: &'static str,
) -> ConnectorResult<()> {
    if !(minimum..=maximum).contains(&value) {
        return Err(ConnectorError::invalid_configuration(format!(
            "{field} is outside the supported range"
        )));
    }
    Ok(())
}

fn bounded_u64(
    value: u64,
    minimum: u64,
    maximum: u64,
    field: &'static str,
) -> ConnectorResult<()> {
    if !(minimum..=maximum).contains(&value) {
        return Err(ConnectorError::invalid_configuration(format!(
            "{field} is outside the supported range"
        )));
    }
    Ok(())
}

const fn default_discovery_depth() -> usize {
    DEFAULT_MAX_DISCOVERY_DEPTH
}
const fn default_discovered_assets() -> usize {
    DEFAULT_MAX_DISCOVERED_ASSETS
}
const fn default_workbook_bytes() -> u64 {
    DEFAULT_MAX_WORKBOOK_BYTES
}
const fn default_archive_entries() -> usize {
    DEFAULT_MAX_ARCHIVE_ENTRIES
}
const fn default_expanded_archive_bytes() -> u64 {
    DEFAULT_MAX_EXPANDED_ARCHIVE_BYTES
}
const fn default_sheet_cells() -> u64 {
    DEFAULT_MAX_SHEET_CELLS
}
const fn default_region_candidates() -> usize {
    DEFAULT_MAX_REGION_CANDIDATES
}
const fn default_analysis_rows() -> usize {
    DEFAULT_ANALYSIS_ROWS
}
const fn default_analysis_columns() -> usize {
    DEFAULT_ANALYSIS_COLUMNS
}

#[cfg(test)]
mod tests {
    use stillflow_core::CredentialRef;

    use super::*;

    fn connection(config: serde_json::Value) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::ExcelWorkbook,
            "workbooks",
            config,
            CredentialRef::new("cred://local/workbooks").expect("credential reference"),
        )
        .expect("connection")
    }

    #[test]
    fn applies_defaults_and_rejects_unknown_or_excessive_values() {
        let parsed = WorkbookConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["/tmp"]
        })))
        .expect("default config");
        assert_eq!(parsed.max_workbook_bytes, DEFAULT_MAX_WORKBOOK_BYTES);
        assert_eq!(parsed.max_sheet_cells, DEFAULT_MAX_SHEET_CELLS);

        assert!(WorkbookConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["/tmp"],
            "maxSheetCells": MAX_SHEET_CELLS + 1
        })))
        .is_err());
        assert!(WorkbookConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["/tmp"],
            "unknown": true
        })))
        .is_err());
        assert!(WorkbookConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["relative"],
        })))
        .is_err());
        assert!(WorkbookConfig::parse(&connection(serde_json::json!({
            "allowedRoots": ["/tmp"],
            "maxDiscoveryDepth": 0
        })))
        .is_err());
    }

    #[test]
    fn source_connection_rejects_embedded_secret_fields() {
        let error = SourceConnection::try_new(
            ConnectorKind::ExcelWorkbook,
            "workbooks",
            serde_json::json!({
                "allowedRoots": ["/tmp"],
                "password": "not-allowed"
            }),
            CredentialRef::new("cred://local/workbooks").expect("credential reference"),
        )
        .expect_err("raw secrets must not enter connection configuration");
        assert_eq!(error.category(), stillflow_core::ErrorCategory::InvalidConfiguration);
    }
}
