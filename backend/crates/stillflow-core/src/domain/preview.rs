use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use serde::{Deserialize, Serialize};

use crate::domain::SourceFilter;
use crate::request::RequestContext;
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
