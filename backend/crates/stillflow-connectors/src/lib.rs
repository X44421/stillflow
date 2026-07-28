//! Connector contracts and registry boundary for Stillflow data sources.

mod capabilities;
mod connector;
mod registry;

pub use capabilities::{Capability, ConnectorCapabilities};
pub use connector::{SourceConnector, SourceConnectorRef};
pub use registry::ConnectorRegistry;

pub use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, BatchItem, BatchStream, Checkpoint, ConnectionStatus,
    ConnectorError, ConnectorKind, ConnectorResult, DatasetSnapshot, DiscoverRequest, PreviewData,
    PreviewRequest, ReadRequest, RequestContext, SourceAsset,
};
