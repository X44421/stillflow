use chrono::Utc;
use stillflow_core::{
    AssetKind, AssetLocator, ConnectorError, ConnectorResult, RequestContext, SourceAsset,
};
use uuid::Uuid;

use crate::config::WorkbookConfig;
use crate::path::{root_label, RootSet};
use crate::preflight::preflight;
use crate::workbook::WorkbookReader;

const SHEET_NAMESPACE: Uuid = Uuid::from_u128(0xb35c1f91_2cb4_5d46_88ae_84417bf25983);

pub(crate) fn discover_sheets(
    roots: &RootSet,
    connection_id: Uuid,
    parent: Option<&str>,
    config: &WorkbookConfig,
    context: &RequestContext,
) -> ConnectorResult<Vec<SourceAsset>> {
    let files = roots.discover_files(
        parent,
        context,
        config.max_discovery_depth,
        config.max_discovered_assets,
    )?;
    let discovered_at = Utc::now();
    let mut assets = Vec::new();
    for file in files {
        context.ensure_active()?;
        let opened = roots.open_discovered(&file)?;
        let package = preflight(&opened.file, opened.format, config, context)?;
        let reader = WorkbookReader::open(opened.file, opened.format, package)?;
        for sheet in reader.sheets() {
            if assets.len() >= config.max_discovered_assets {
                return Err(ConnectorError::with_category(
                    stillflow_core::ErrorCategory::InvalidData,
                    false,
                    "workbook discovery exceeded maxDiscoveredAssets",
                    Vec::new(),
                    std::collections::BTreeMap::new(),
                ));
            }
            let identity = format!(
                "{}\0{}\0{}\0{}\0{}",
                connection_id, file.root_identity, file.relative, sheet.ordinal, sheet.name
            );
            assets.push(SourceAsset {
                id: Uuid::new_v5(&SHEET_NAMESPACE, identity.as_bytes()),
                connection_id,
                kind: AssetKind::Sheet,
                name: format!("{} — {}", file.name, sheet.name),
                locator: AssetLocator {
                    path: file.relative.clone(),
                    container: Some(root_label(file.root_index)),
                    schema: None,
                    sheet: Some(sheet.name),
                    workbook_region: None,
                },
                discovered_at,
            });
        }
    }
    Ok(assets)
}
