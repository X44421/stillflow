//! Domain model and shared data contracts for the Stillflow ingestion backend.
//!
//! `stillflow-core` owns the source-of-truth types that every other crate builds
//! on: sessions, objects, datasets, snapshots, schema descriptors and typed
//! errors. It depends on no other workspace crate, and Apache Arrow is the
//! interchange protocol at its boundary.

pub mod batch;
pub mod domain;
pub mod error;
pub mod events;
pub mod expression;
pub mod logical;
pub mod request;
pub mod stream;

#[cfg(test)]
mod serde_tests;

pub use batch::{
    logical_schema_from_arrow, logical_schema_to_arrow, BatchEnvelope, BatchError,
    LogicalSchemaFingerprint, BATCH_ENVELOPE_VERSION, LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM,
    MAX_BATCH_BYTES, MAX_BATCH_ROWS,
};
pub use domain::{
    AssetKind, AssetLocator, AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus,
    CredentialRef, Dataset, DatasetSnapshot, DiscoverRequest, InspectRequest, InspectionFinding,
    PreviewData, PreviewRequest, ReadRequest, SamplingStrategy, Session, SnapshotError,
    SnapshotStats, SourceAsset, SourceConnection, TestConnectionRequest, DATASET_SNAPSHOT_VERSION,
};
pub use error::{
    ensure_no_secret_fields, ensure_safe_event_metadata, ConnectorError, ConnectorResult,
    ErrorCategory, SanitizedErrorSummary,
};
pub use events::{ConnectorKind, IngestionEvent, ObjectKind, RelationshipKind};
pub use expression::{BinaryOperator, Expr, FiniteF64, ScalarValue, SourceFilter, UnaryOperator};
pub use logical::{
    ColumnId, LogicalError, LogicalField, LogicalSchema, LogicalType, TimeUnit,
    LOGICAL_SCHEMA_VERSION,
};
pub use request::RequestContext;
pub use stream::{attach_request_context, BatchItem, BatchStream};
