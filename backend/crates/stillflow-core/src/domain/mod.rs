//! Core ingestion domain types.

mod asset;
mod checkpoint;
mod connection;
mod dataset;
mod metadata;
mod preview;
mod read;
mod session;
mod snapshot;

pub use asset::{AssetKind, AssetLocator, SourceAsset};
pub use checkpoint::Checkpoint;
pub use connection::{ConnectionStatus, CredentialRef, SourceConnection};
pub use dataset::Dataset;
pub use metadata::{AssetMetadata, FindingSeverity, InspectionFinding};
pub use preview::{PreviewData, PreviewRequest, SamplingStrategy};
pub use read::ReadRequest;
pub use session::Session;
pub use snapshot::{DatasetSnapshot, SnapshotError, SnapshotStats, DATASET_SNAPSHOT_VERSION};

/// Request parameters for asset discovery.
#[derive(Debug, Clone)]
pub struct DiscoverRequest {
    /// Shared deadline and cancellation controls.
    pub context: crate::RequestContext,
    /// Optional parent path or container to scope discovery.
    pub parent_path: Option<String>,
}

impl DiscoverRequest {
    pub fn validate(&self) -> crate::ConnectorResult<()> {
        self.context.ensure_active()
    }
}

/// Request parameters for connection testing.
#[derive(Debug, Clone)]
pub struct TestConnectionRequest {
    pub context: crate::RequestContext,
}

impl TestConnectionRequest {
    pub fn validate(&self) -> crate::ConnectorResult<()> {
        self.context.ensure_active()
    }
}

/// Request parameters for asset inspection.
#[derive(Debug, Clone)]
pub struct InspectRequest {
    pub context: crate::RequestContext,
    pub asset: SourceAsset,
}

impl InspectRequest {
    pub fn validate(&self) -> crate::ConnectorResult<()> {
        self.context.ensure_active()
    }
}

/// Request parameters for checkpoint reads.
#[derive(Debug, Clone)]
pub struct CheckpointRequest {
    pub context: crate::RequestContext,
    pub asset: SourceAsset,
}

impl CheckpointRequest {
    pub fn validate(&self) -> crate::ConnectorResult<()> {
        self.context.ensure_active()
    }
}
