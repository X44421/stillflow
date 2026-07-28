//! Domain model and shared data contracts for the Stillflow ingestion backend.
//!
//! `stillflow-core` owns the source-of-truth types that every other crate builds
//! on: sessions, objects, datasets, snapshots, schema descriptors and typed
//! errors. It depends on no other workspace crate, and Apache Arrow is the
//! interchange protocol at its boundary.

pub mod domain;
pub mod error;
pub mod events;
pub mod request;
pub mod stream;

#[cfg(test)]
mod serde_tests;

pub use domain::{
    AssetKind, AssetLocator, AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus,
    CredentialRef, Dataset, DatasetSnapshot, DiscoverRequest, FilterDialect, InspectRequest,
    InspectionFinding, PreviewData, PreviewRequest, ReadRequest, SamplingStrategy,
    SchemaFieldSnapshot, Session, SourceAsset, SourceConnection, SourceFilter,
    TestConnectionRequest,
};
pub use error::{
    ensure_no_secret_fields, ensure_safe_event_metadata, ConnectorError, ConnectorResult,
    ErrorCategory, SanitizedErrorSummary,
};
pub use events::{ConnectorKind, IngestionEvent, ObjectKind, RelationshipKind};
pub use request::RequestContext;
pub use stream::{attach_request_context, BatchItem, BatchStream};
