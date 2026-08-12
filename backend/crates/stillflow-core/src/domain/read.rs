use std::collections::BTreeSet;

use crate::domain::Checkpoint;
use crate::request::RequestContext;
use crate::ColumnId;
use crate::ConnectorError;
use crate::ConnectorResult;
use crate::LogicalSchema;
use crate::SourceAsset;
use crate::SourceFilter;

/// Streaming read request with projection, predicate and resume state.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub context: RequestContext,
    pub asset: SourceAsset,
    /// Optional caller-authorized schema used instead of source inference.
    pub schema_override: Option<LogicalSchema>,
    pub projection: Option<Vec<ColumnId>>,
    pub filter: Option<SourceFilter>,
    pub checkpoint: Option<Checkpoint>,
    pub batch_size: usize,
}

impl ReadRequest {
    pub const MIN_BATCH_SIZE: usize = 1;
    pub const MAX_BATCH_SIZE: usize = 65_536;

    pub fn new(asset: SourceAsset, batch_size: usize) -> Self {
        Self {
            context: RequestContext::default(),
            asset,
            schema_override: None,
            projection: None,
            filter: None,
            checkpoint: None,
            batch_size,
        }
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.context.ensure_active()?;
        if let Some(schema) = &self.schema_override {
            schema.validate().map_err(|error| {
                ConnectorError::invalid_configuration(format!(
                    "invalid read schema override: {error}"
                ))
            })?;
        }
        if self.batch_size < Self::MIN_BATCH_SIZE || self.batch_size > Self::MAX_BATCH_SIZE {
            return Err(ConnectorError::invalid_configuration(format!(
                "batch_size must be between {} and {}",
                Self::MIN_BATCH_SIZE,
                Self::MAX_BATCH_SIZE
            )));
        }
        if let Some(projection) = &self.projection {
            if projection.is_empty() {
                return Err(ConnectorError::invalid_configuration(
                    "read projection must not be empty when provided",
                ));
            }
            if projection.iter().copied().collect::<BTreeSet<_>>().len() != projection.len() {
                return Err(ConnectorError::invalid_configuration(
                    "read projection must not contain duplicate column ids",
                ));
            }
        }
        if let Some(filter) = &self.filter {
            filter.expression.validate_shape().map_err(|error| {
                ConnectorError::invalid_configuration(format!("invalid read filter: {error}"))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssetKind;
    use crate::AssetLocator;

    #[test]
    fn rejects_invalid_batch_size() {
        let asset = SourceAsset::new(
            uuid::Uuid::new_v4(),
            AssetKind::File,
            "orders.csv",
            AssetLocator {
                path: "/orders.csv".to_owned(),
                container: None,
                schema: None,
                sheet: None,
            },
        );
        let request = ReadRequest::new(asset, 0);
        request.validate().expect_err("invalid batch size");
    }

    #[test]
    fn rejects_duplicate_projection_ids() {
        let asset = SourceAsset::new(
            uuid::Uuid::new_v4(),
            AssetKind::File,
            "orders.csv",
            AssetLocator {
                path: "/orders.csv".to_owned(),
                container: None,
                schema: None,
                sheet: None,
            },
        );
        let column = ColumnId::from_uuid(uuid::Uuid::from_u128(1));
        let mut request = ReadRequest::new(asset, 1024);
        request.projection = Some(vec![column, column]);
        request.validate().expect_err("duplicate projection");
    }

    #[test]
    fn rejects_invalid_schema_override() {
        let asset = SourceAsset::new(
            uuid::Uuid::new_v4(),
            AssetKind::File,
            "orders.csv",
            AssetLocator {
                path: "/orders.csv".to_owned(),
                container: None,
                schema: None,
                sheet: None,
            },
        );
        let mut schema = LogicalSchema::empty();
        schema.version += 1;
        let mut request = ReadRequest::new(asset, 1024);
        request.schema_override = Some(schema);
        request.validate().expect_err("invalid schema override");
    }
}
