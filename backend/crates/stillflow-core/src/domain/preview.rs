use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::request::RequestContext;
use crate::BatchEnvelope;
use crate::ColumnId;
use crate::ConnectorError;
use crate::ConnectorResult;
use crate::ErrorCategory;
use crate::LogicalSchema;
use crate::LogicalSchemaFingerprint;
use crate::SourceAsset;
use crate::SourceFilter;

/// Strategy used when sampling rows for preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum SamplingStrategy {
    #[default]
    Head,
    Reservoir,
    Random,
}

/// Bounded preview request with projection, filter and resource limits.
#[derive(Debug, Clone)]
pub struct PreviewRequest {
    pub context: RequestContext,
    pub asset: SourceAsset,
    /// Optional caller-authorized schema used instead of source inference.
    pub schema_override: Option<LogicalSchema>,
    pub projection: Option<Vec<ColumnId>>,
    pub filter: Option<SourceFilter>,
    pub row_limit: usize,
    pub byte_limit: usize,
    pub sampling: SamplingStrategy,
}

impl PreviewRequest {
    pub const DEFAULT_ROW_LIMIT: usize = 1_000;
    pub const MAX_ROW_LIMIT: usize = 10_000;
    pub const MAX_BYTE_LIMIT: usize = 50 * 1024 * 1024;

    pub fn new(asset: SourceAsset, row_limit: usize, byte_limit: usize) -> Self {
        Self {
            context: RequestContext::default(),
            asset,
            schema_override: None,
            projection: None,
            filter: None,
            row_limit,
            byte_limit,
            sampling: SamplingStrategy::default(),
        }
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.context.ensure_active()?;
        if let Some(selection) = &self.asset.locator.workbook_region {
            selection.validate()?;
        }
        if let Some(schema) = &self.schema_override {
            schema.validate().map_err(|error| {
                ConnectorError::invalid_configuration(format!(
                    "invalid preview schema override: {error}"
                ))
            })?;
        }
        if self.row_limit == 0 {
            return Err(ConnectorError::invalid_configuration(
                "preview row_limit must be greater than zero",
            ));
        }
        if self.row_limit > Self::MAX_ROW_LIMIT {
            return Err(ConnectorError::invalid_configuration(format!(
                "preview row_limit exceeds maximum of {}",
                Self::MAX_ROW_LIMIT
            )));
        }
        if self.byte_limit == 0 {
            return Err(ConnectorError::invalid_configuration(
                "preview byte_limit must be greater than zero",
            ));
        }
        if self.byte_limit > Self::MAX_BYTE_LIMIT {
            return Err(ConnectorError::invalid_configuration(format!(
                "preview byte_limit exceeds maximum of {}",
                Self::MAX_BYTE_LIMIT
            )));
        }
        if let Some(projection) = &self.projection {
            if projection.is_empty() {
                return Err(ConnectorError::invalid_configuration(
                    "preview projection must not be empty when provided",
                ));
            }
            if projection.iter().copied().collect::<BTreeSet<_>>().len() != projection.len() {
                return Err(ConnectorError::invalid_configuration(
                    "preview projection must not contain duplicate column ids",
                ));
            }
        }
        if let Some(filter) = &self.filter {
            filter.expression.validate_shape().map_err(|error| {
                ConnectorError::invalid_configuration(format!("invalid preview filter: {error}"))
            })?;
        }
        Ok(())
    }
}

/// Bounded preview payload returned by connectors.
#[derive(Debug, Clone)]
pub struct PreviewData {
    pub schema: LogicalSchema,
    pub batches: Vec<BatchEnvelope>,
    pub rows_returned: usize,
    pub rows_truncated: bool,
    pub bytes_returned: usize,
    pub bytes_truncated: bool,
    pub warnings: Vec<String>,
}

impl PreviewData {
    pub fn empty(schema: LogicalSchema) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            rows_returned: 0,
            rows_truncated: false,
            bytes_returned: 0,
            bytes_truncated: false,
            warnings: Vec::new(),
        }
    }

    /// Validates aggregate preview bounds and every enclosed batch envelope.
    pub fn validate(
        &self,
        expected_source_asset_id: Uuid,
        row_limit: usize,
        byte_limit: usize,
    ) -> ConnectorResult<()> {
        self.schema.validate().map_err(|error| {
            preview_error(
                ErrorCategory::InvalidData,
                format!("invalid preview logical schema: {error}"),
            )
        })?;
        let expected_fingerprint = LogicalSchemaFingerprint::try_from_schema(&self.schema)
            .map_err(ConnectorError::from)?;
        let mut expected_sequence = Some(0_u64);
        let mut rows = 0_usize;
        let mut bytes = 0_usize;

        for envelope in &self.batches {
            if envelope.source_asset_id() != expected_source_asset_id {
                return Err(preview_error(
                    ErrorCategory::InvalidData,
                    format!(
                        "preview batch lineage {} does not match expected source asset {}",
                        envelope.source_asset_id(),
                        expected_source_asset_id
                    ),
                ));
            }
            let Some(sequence) = expected_sequence else {
                return Err(preview_error(
                    ErrorCategory::InvalidData,
                    "preview batch sequence exceeded the version 1 range",
                ));
            };
            if envelope.sequence() != sequence {
                return Err(preview_error(
                    ErrorCategory::InvalidData,
                    format!(
                        "preview batch sequence {} does not match expected sequence {sequence}",
                        envelope.sequence()
                    ),
                ));
            }
            if envelope.schema_fingerprint() != expected_fingerprint
                || envelope.schema() != &self.schema
            {
                return Err(preview_error(
                    ErrorCategory::SchemaDrift,
                    format!("preview logical schema changed at batch sequence {sequence}"),
                ));
            }

            rows = rows.checked_add(envelope.row_count()).ok_or_else(|| {
                preview_error(ErrorCategory::InvalidData, "preview row count overflow")
            })?;
            bytes = bytes.checked_add(envelope.byte_count()).ok_or_else(|| {
                preview_error(ErrorCategory::InvalidData, "preview byte count overflow")
            })?;
            expected_sequence = sequence.checked_add(1);
        }

        if rows != self.rows_returned {
            return Err(preview_error(
                ErrorCategory::InvalidData,
                format!(
                    "preview row count {} does not match payload row count {rows}",
                    self.rows_returned
                ),
            ));
        }
        if bytes != self.bytes_returned {
            return Err(preview_error(
                ErrorCategory::InvalidData,
                format!(
                    "preview byte count {} does not match payload byte count {bytes}",
                    self.bytes_returned
                ),
            ));
        }
        if rows > row_limit {
            return Err(preview_error(
                ErrorCategory::InvalidData,
                format!("preview returned {rows} rows; request limit is {row_limit}"),
            ));
        }
        if bytes > byte_limit {
            return Err(preview_error(
                ErrorCategory::InvalidData,
                format!("preview returned {bytes} bytes; request limit is {byte_limit}"),
            ));
        }
        Ok(())
    }
}

fn preview_error(category: ErrorCategory, message: impl Into<String>) -> ConnectorError {
    ConnectorError::with_category(
        category,
        false,
        message,
        Vec::new(),
        std::collections::BTreeMap::new(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};

    use super::*;
    use crate::logical_schema_to_arrow;
    use crate::AssetKind;
    use crate::AssetLocator;
    use crate::LogicalField;
    use crate::LogicalType;

    fn sample_asset() -> SourceAsset {
        SourceAsset::new(
            uuid::Uuid::new_v4(),
            AssetKind::File,
            "orders.csv",
            AssetLocator {
                path: "/orders.csv".to_owned(),
                container: None,
                schema: None,
                sheet: None,
                workbook_region: None,
            },
        )
    }

    #[test]
    fn rejects_zero_row_limit() {
        let request = PreviewRequest::new(sample_asset(), 0, 1024);
        request.validate().expect_err("zero row limit");
    }

    #[test]
    fn rejects_excessive_row_limit() {
        let request = PreviewRequest::new(sample_asset(), PreviewRequest::MAX_ROW_LIMIT + 1, 1024);
        request.validate().expect_err("excessive row limit");
    }

    #[test]
    fn rejects_duplicate_projection_ids() {
        let column = ColumnId::from_uuid(uuid::Uuid::from_u128(1));
        let mut request = PreviewRequest::new(sample_asset(), 10, 1024);
        request.projection = Some(vec![column, column]);
        request.validate().expect_err("duplicate projection");
    }

    #[test]
    fn rejects_invalid_schema_override() {
        let mut schema = LogicalSchema::empty();
        schema.version += 1;
        let mut request = PreviewRequest::new(sample_asset(), 10, 1024);
        request.schema_override = Some(schema);
        request.validate().expect_err("invalid schema override");
    }

    fn preview_fixture(source_asset_id: Uuid, sequence: u64) -> PreviewData {
        let schema = LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(1)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field")])
        .expect("schema");
        let shared = Arc::new(schema.clone());
        let arrow = logical_schema_to_arrow(&schema).expect("Arrow schema");
        let batch = RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))])
            .expect("record batch");
        let envelope =
            BatchEnvelope::try_new(shared, source_asset_id, sequence, batch).expect("envelope");
        PreviewData {
            schema,
            rows_returned: envelope.row_count(),
            bytes_returned: envelope.byte_count(),
            batches: vec![envelope],
            rows_truncated: false,
            bytes_truncated: false,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn validates_preview_envelopes_and_exact_counts() {
        let source = Uuid::from_u128(7);
        let preview = preview_fixture(source, 0);
        preview
            .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
            .expect("valid preview");
    }

    #[test]
    fn rejects_preview_sequence_lineage_schema_counts_and_bounds() {
        let source = Uuid::from_u128(7);

        let wrong_sequence = preview_fixture(source, 1);
        assert!(wrong_sequence
            .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
            .is_err());

        let wrong_lineage = preview_fixture(Uuid::from_u128(8), 0);
        assert!(wrong_lineage
            .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
            .is_err());

        let mut wrong_schema = preview_fixture(source, 0);
        wrong_schema.schema = LogicalSchema::empty();
        assert_eq!(
            wrong_schema
                .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
                .expect_err("schema drift")
                .category(),
            ErrorCategory::SchemaDrift
        );

        let mut wrong_rows = preview_fixture(source, 0);
        wrong_rows.rows_returned += 1;
        assert!(wrong_rows
            .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
            .is_err());

        let mut wrong_bytes = preview_fixture(source, 0);
        wrong_bytes.bytes_returned += 1;
        assert!(wrong_bytes
            .validate(source, 10, PreviewRequest::MAX_BYTE_LIMIT)
            .is_err());

        let too_many_rows = preview_fixture(source, 0);
        assert!(too_many_rows
            .validate(source, 1, PreviewRequest::MAX_BYTE_LIMIT)
            .is_err());

        let too_many_bytes = preview_fixture(source, 0);
        assert!(too_many_bytes.validate(source, 10, 0).is_err());
    }
}
