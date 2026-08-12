use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::WorkbookRegionSelection;

/// Kind of discoverable source asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AssetKind {
    File,
    Sheet,
    Table,
    View,
    Document,
}

/// Connector-specific locator metadata for a discovered asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetLocator {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sheet: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workbook_region: Option<WorkbookRegionSelection>,
}

/// A file, sheet, table, view or document discovered through a connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceAsset {
    pub id: Uuid,
    pub connection_id: Uuid,
    pub kind: AssetKind,
    pub name: String,
    pub locator: AssetLocator,
    pub discovered_at: DateTime<Utc>,
}

impl SourceAsset {
    pub fn new(
        connection_id: Uuid,
        kind: AssetKind,
        name: impl Into<String>,
        locator: AssetLocator,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            connection_id,
            kind,
            name: name.into(),
            locator,
            discovered_at: Utc::now(),
        }
    }
}
