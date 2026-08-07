//! Connector contracts and registry boundary for Stillflow data sources.

mod capabilities;
mod connector;
mod raw_batch_stream;
mod registry;

pub use capabilities::{Capability, ConnectorCapabilities};
pub use connector::{SourceConnector, SourceConnectorRef};
pub use raw_batch_stream::RawBatchStream;
pub use registry::ConnectorRegistry;

pub use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, BatchEnvelope, BatchItem, BatchStream, Checkpoint,
    CheckpointRequest, ConnectionStatus, ConnectorError, ConnectorKind, ConnectorResult,
    DatasetSnapshot, DiscoverRequest, InspectRequest, PreviewData, PreviewRequest, ReadRequest,
    RequestContext, SourceAsset, SourceConnection, TestConnectionRequest,
};
