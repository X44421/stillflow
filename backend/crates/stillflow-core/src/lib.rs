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
    logical_schema_from_arrow, logical_schema_to_arrow, BatchEnvelope, BatchEnvelopeFactory,
    BatchError, LogicalSchemaFingerprint, BATCH_ENVELOPE_VERSION,
    LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM, MAX_BATCH_BYTES, MAX_BATCH_ROWS,
};
pub use domain::{
    digest_hex, ArtifactKind, ArtifactProvenance, ArtifactProvenanceDraft, ArtifactProvenanceInput,
    ArtifactSummary, AssetKind, AssetLocator, AssetMetadata, CandidateConfidence, CellCoordinate,
    CellRange, Checkpoint, CheckpointRequest, ConnectionStatus, CredentialRef, Dataset,
    DatasetSnapshot, DiscoverRequest, FindingSeverity, InputRef, InspectRequest, InspectionFinding,
    LogicalInputRef, PreviewData, PreviewRequest, ReadRequest, RuleRef, SamplingStrategy, Session,
    SnapshotError, SnapshotStats, SourceAsset, SourceConnection, SourceRowRef,
    TestConnectionRequest, WorkbookHeaderCandidate, WorkbookHeaderSelection, WorkbookInspection,
    WorkbookRegionCandidate, WorkbookRegionSelection, WorkbookSheetVisibility,
    DATASET_SNAPSHOT_VERSION,
};
pub use error::{
    ensure_no_secret_fields, ensure_safe_event_metadata, ConnectorError, ConnectorResult,
    ErrorCategory, SanitizedErrorSummary,
};
pub use events::{ConnectorKind, IngestionEvent, ObjectKind, RelationshipKind};
pub use expression::{BinaryOperator, Expr, FiniteF64, ScalarValue, SourceFilter, UnaryOperator};
pub use logical::{
    ColumnId, LogicalError, LogicalField, LogicalSchema, LogicalType, TimeUnit,
    LOGICAL_SCHEMA_VERSION, MAX_SCHEMA_FIELDS, MAX_SCHEMA_NESTING_DEPTH, MAX_SCHEMA_TEXT_BYTES,
};
pub use request::RequestContext;
pub use stream::{attach_request_context, BatchItem, BatchStream};
