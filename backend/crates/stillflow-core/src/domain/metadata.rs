use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::LogicalSchema;

/// Non-fatal inspection finding surfaced during schema or format analysis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionFinding {
    pub code: String,
    pub message: String,
    pub severity: FindingSeverity,
}

/// Severity of an inspection finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FindingSeverity {
    Info,
    Warning,
    Error,
}

/// Schema, size, timestamps, format and inspection findings for an asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetMetadata {
    pub schema: LogicalSchema,
    pub format: String,
    pub size_bytes: Option<u64>,
    pub row_count: Option<u64>,
    pub modified_at: Option<DateTime<Utc>>,
    pub findings: Vec<InspectionFinding>,
}

impl AssetMetadata {
    pub fn new(schema: LogicalSchema, format: impl Into<String>) -> Self {
        Self {
            schema,
            format: format.into(),
            size_bytes: None,
            row_count: None,
            modified_at: None,
            findings: Vec::new(),
        }
    }
}
