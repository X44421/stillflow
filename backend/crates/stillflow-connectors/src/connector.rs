use std::sync::Arc;

use async_trait::async_trait;

use stillflow_core::{
    AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus, ConnectorKind, ConnectorResult,
    DiscoverRequest, InspectRequest, PreviewData, PreviewRequest, ReadRequest, SourceConnection,
    TestConnectionRequest,
};

use crate::capabilities::ConnectorCapabilities;
use crate::raw_batch_stream::RawBatchStream;

/// Object-safe connector implementation handle.
pub type SourceConnectorRef = Arc<dyn SourceConnector>;

/// Arrow-based connector contract for discovery, inspection, preview and reads.
///
/// Implementations return [`RawBatchStream`] from [`Self::read_batches`]. Request
/// context wrapping is enforced by [`crate::ConnectorRegistry::read_batches`].
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Stable connector kind used by the registry.
    fn kind(&self) -> ConnectorKind;

    /// Declared connector capabilities.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Verifies that the configured source is reachable.
    async fn test_connection(
        &self,
        connection: &SourceConnection,
        request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus>;

    /// Discovers assets available through the configured source.
    async fn discover(
        &self,
        connection: &SourceConnection,
        request: DiscoverRequest,
    ) -> ConnectorResult<Vec<stillflow_core::SourceAsset>>;

    /// Returns schema, format and inspection findings for one asset.
    async fn inspect(
        &self,
        connection: &SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<AssetMetadata>;

    /// Returns a bounded Arrow preview for one asset.
    async fn preview(
        &self,
        connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData>;

    /// Opens a bounded Arrow batch stream for one asset without request wrapping.
    async fn read_batches(
        &self,
        connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<RawBatchStream>;

    /// Returns the latest checkpoint for incremental reads, if any.
    async fn checkpoint(
        &self,
        connection: &SourceConnection,
        request: CheckpointRequest,
    ) -> ConnectorResult<Option<Checkpoint>>;
}
