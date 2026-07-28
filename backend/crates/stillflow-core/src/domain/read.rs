use crate::domain::{Checkpoint, SourceFilter};
use crate::request::RequestContext;
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
}
