//! Core ingestion domain types.

use serde::{Deserialize, Serialize};

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
pub use metadata::{AssetMetadata, InspectionFinding};
pub use preview::{PreviewData, PreviewRequest, SamplingStrategy};
pub use read::ReadRequest;
pub use session::Session;
pub use snapshot::DatasetSnapshot;

/// Request parameters for asset discovery.
#[derive(Debug, Clone)]
pub struct DiscoverRequest {
    /// Shared deadline and cancellation controls.
    pub context: crate::RequestContext,
    /// Optional parent path or container to scope discovery.
    pub parent_path: Option<String>,
}

/// A portable filter expression carried on preview and read requests.
///
/// Connectors that do not support predicate pushdown must return
/// [`crate::ConnectorError`] with category [`crate::ErrorCategory::UnsupportedCapability`]
/// when a non-empty filter is supplied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFilter {
    /// SQL-like expression text; interpretation is connector-specific.
    pub expression: String,
}
