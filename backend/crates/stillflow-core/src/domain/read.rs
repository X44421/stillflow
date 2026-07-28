use crate::domain::{Checkpoint, SourceFilter};
use crate::request::RequestContext;
use crate::ConnectorError;
use crate::ConnectorResult;
use crate::SourceAsset;

/// Streaming read request with projection, predicate and resume state.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub context: RequestContext,
    pub asset: SourceAsset,
    pub projection: Option<Vec<String>>,
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
            projection: None,
            filter: None,
            checkpoint: None,
            batch_size,
        }
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.context.ensure_active()?;
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
        }
        if let Some(filter) = &self.filter {
            if filter.expression.trim().is_empty() {
                return Err(ConnectorError::invalid_configuration(
                    "read filter expression must not be empty when provided",
                ));
            }
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
}
