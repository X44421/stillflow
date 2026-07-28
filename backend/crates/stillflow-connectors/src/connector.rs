use std::sync::Arc;

use async_trait::async_trait;

use stillflow_core::{
    attach_request_context, BatchStream, Checkpoint, ConnectionStatus, ConnectorKind,
    ConnectorResult, DiscoverRequest, PreviewData, PreviewRequest, ReadRequest, SourceAsset,
};

use crate::capabilities::ConnectorCapabilities;

/// Object-safe connector implementation handle.
pub type SourceConnectorRef = Arc<dyn SourceConnector>;

/// Arrow-based connector contract for discovery, inspection, preview and reads.
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Stable connector kind used by the registry.
    fn kind(&self) -> ConnectorKind;

    /// Declared connector capabilities.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Verifies that the configured source is reachable.
    async fn test_connection(&self) -> ConnectorResult<ConnectionStatus>;

    /// Discovers assets available through the configured source.
    async fn discover(&self, request: DiscoverRequest) -> ConnectorResult<Vec<SourceAsset>>;

    /// Returns schema, format and inspection findings for one asset.
    async fn inspect(&self, asset: &SourceAsset) -> ConnectorResult<stillflow_core::AssetMetadata>;

    /// Returns a bounded Arrow preview for one asset.
    async fn preview(&self, request: PreviewRequest) -> ConnectorResult<PreviewData>;

    /// Opens a bounded Arrow batch stream for one asset.
    async fn read_batches(&self, request: ReadRequest) -> ConnectorResult<BatchStream>;

    /// Returns the latest checkpoint for incremental reads, if any.
    async fn checkpoint(&self, asset: &SourceAsset) -> ConnectorResult<Option<Checkpoint>>;
}

/// Attaches request cancellation and deadlines to a connector batch stream.
#[allow(dead_code)]
pub fn wrap_batch_stream(stream: BatchStream, request: &ReadRequest) -> BatchStream {
    attach_request_context(stream, request.context.clone())
}
