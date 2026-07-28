use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use serde::{Deserialize, Serialize};

use crate::domain::SourceFilter;
use crate::request::RequestContext;
use crate::ConnectorError;
use crate::ConnectorResult;
use crate::SourceAsset;

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
    pub projection: Option<Vec<String>>,
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
            projection: None,
            filter: None,
            row_limit,
            byte_limit,
            sampling: SamplingStrategy::default(),
        }
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.context.ensure_active()?;
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
        }
        if let Some(filter) = &self.filter {
            if filter.expression.trim().is_empty() {
                return Err(ConnectorError::invalid_configuration(
                    "preview filter expression must not be empty when provided",
                ));
            }
        }
        Ok(())
    }
}

/// Bounded preview payload returned by connectors.
#[derive(Debug, Clone)]
pub struct PreviewData {
    pub schema: Arc<Schema>,
    pub batches: Vec<RecordBatch>,
    pub rows_returned: usize,
    pub rows_truncated: bool,
    pub bytes_returned: usize,
    pub bytes_truncated: bool,
    pub warnings: Vec<String>,
}

impl PreviewData {
    pub fn empty(schema: Arc<Schema>) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssetKind;
    use crate::AssetLocator;

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
}
