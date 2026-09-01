//! Domain model and shared data contracts for the Stillflow ingestion backend.
//!
//! `stillflow-core` owns the source-of-truth types that every other crate builds
//! on: sessions, objects, datasets, snapshots, schema descriptors and typed
//! errors. It depends on no other workspace crate, and Apache Arrow is the
//! interchange protocol at its boundary.

pub mod batch;
pub mod control_plane;
pub mod domain;
pub mod error;
pub mod events;
pub mod export;
pub mod expression;
pub mod logical;
pub mod request;
pub mod stream;
pub mod verification;

#[cfg(test)]
mod serde_tests;

pub use batch::{
    logical_schema_from_arrow, logical_schema_to_arrow, BatchEnvelope, BatchEnvelopeFactory,
    BatchError, LogicalSchemaFingerprint, BATCH_ENVELOPE_VERSION,
    LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM, MAX_BATCH_BYTES, MAX_BATCH_ROWS,
};
pub use control_plane::{
    asset_input, snapshot_input, ArtifactRefState, ControlPlaneEventType, ControlPlaneInput,
    DatasetState, EventStreamKind, JobState, PlanState, PlanVersionState, RunState, SessionState,
    SourceAssetState, SourceConnectionState, WorkspaceState, MAX_EVENT_PAGE_SIZE,
    MAX_EVENT_PAYLOAD_BYTES, MAX_QUEUED_JOBS_PER_WORKSPACE,
};
pub use domain::{
    AssetKind, AssetLocator, AssetMetadata, CandidateConfidence, CellCoordinate, CellRange,
    Checkpoint, CheckpointRequest, ConnectionStatus, CredentialRef, Dataset, DatasetSnapshot,
    DiscoverRequest, FindingSeverity, InspectRequest, InspectionFinding, PreviewData,
    PreviewRequest, ReadRequest, SamplingStrategy, Session, SnapshotError, SnapshotStats,
    SourceAsset, SourceConnection, TestConnectionRequest, WorkbookHeaderCandidate,
    WorkbookHeaderSelection, WorkbookInspection, WorkbookRegionCandidate, WorkbookRegionSelection,
    WorkbookSheetVisibility, DATASET_SNAPSHOT_VERSION,
};
pub use error::{
    ensure_no_secret_fields, ensure_safe_event_metadata, ConnectorError, ConnectorResult,
    ErrorCategory, SanitizedErrorSummary,
};
pub use events::{ConnectorKind, IngestionEvent, ObjectKind, RelationshipKind};
pub use export::{
    validate_export_component, ExportDestination, ExportError, ExportFormat, ExportInputIdentity,
    ExportPolicy, ExportResult, ExportResultFile, ExportShape, EXPORT_DEFAULT_DEADLINE_SECONDS,
    EXPORT_ENCODER_VERSION, EXPORT_FORMAT_CONTRACT_VERSION, EXPORT_JSONL_FLOAT_ENCODER,
    EXPORT_MANIFEST_VERSION, EXPORT_TEXT_FLOAT_ENCODER, MAX_ACTIVE_EXPORT_PUBLISHERS,
    MAX_EXPORT_OUTPUT_BYTES, MAX_EXPORT_PARTITIONS, MAX_EXPORT_PATH_DEPTH, MAX_EXPORT_ROWS,
    MAX_EXPORT_SINGLE_FILE_BYTES, MAX_EXPORT_TEMP_BYTES,
};
pub use expression::{BinaryOperator, Expr, FiniteF64, ScalarValue, SourceFilter, UnaryOperator};
pub use logical::{
    ColumnId, LogicalError, LogicalField, LogicalSchema, LogicalType, TimeUnit,
    LOGICAL_SCHEMA_VERSION, MAX_SCHEMA_FIELDS, MAX_SCHEMA_NESTING_DEPTH, MAX_SCHEMA_TEXT_BYTES,
};
pub use request::RequestContext;
pub use stream::{attach_request_context, BatchItem, BatchStream};
pub use verification::{
    ArtifactKind, ArtifactProvenance, ArtifactProvenanceDraft, ArtifactProvenanceInput,
    ArtifactSummary, ContentDigest, InputRef, LogicalInputRef, RuleRef, SourceRowRef,
    DEDUP_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
    DEDUP_RULE_SUMMARY_DUPLICATE_COUNT_COLUMN_ID, DEDUP_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
    DEDUP_RULE_SUMMARY_INPUT_ID_COLUMN_ID, DEDUP_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
    DEDUP_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
    DEDUP_RULE_SUMMARY_KEY_COLUMN_COUNT_COLUMN_ID, DEDUP_RULE_SUMMARY_NODE_ID_COLUMN_ID,
    DEDUP_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID, DEDUP_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
    DEDUP_RULE_SUMMARY_UNIQUE_COUNT_COLUMN_ID, DUPLICATE_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
    DUPLICATE_FINDING_ENCODED_KEY_BYTE_COUNT_COLUMN_ID,
    DUPLICATE_FINDING_FIRST_SOURCE_ROW_ORDINAL_COLUMN_ID, DUPLICATE_FINDING_INPUT_ID_COLUMN_ID,
    DUPLICATE_FINDING_INPUT_KIND_COLUMN_ID, DUPLICATE_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
    DUPLICATE_FINDING_KEY_COLUMN_COUNT_COLUMN_ID, DUPLICATE_FINDING_NODE_ID_COLUMN_ID,
    DUPLICATE_FINDING_PLAN_FINGERPRINT_COLUMN_ID, DUPLICATE_FINDING_RULE_ORDINAL_COLUMN_ID,
    DUPLICATE_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID, REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID,
    REJECTED_INPUT_ID_COLUMN_ID, REJECTED_INPUT_KIND_COLUMN_ID,
    REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID, REJECTED_KIND_COLUMN_ID, REJECTED_NODE_ID_COLUMN_ID,
    REJECTED_PLAN_FINGERPRINT_COLUMN_ID, REJECTED_RULE_ORDINAL_COLUMN_ID,
    REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID, VALIDATION_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
    VALIDATION_FINDING_INPUT_ID_COLUMN_ID, VALIDATION_FINDING_INPUT_KIND_COLUMN_ID,
    VALIDATION_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID, VALIDATION_FINDING_NODE_ID_COLUMN_ID,
    VALIDATION_FINDING_PLAN_FINGERPRINT_COLUMN_ID, VALIDATION_FINDING_PREDICATE_OUTCOME_COLUMN_ID,
    VALIDATION_FINDING_RULE_ORDINAL_COLUMN_ID, VALIDATION_FINDING_SEVERITY_COLUMN_ID,
    VALIDATION_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_ERROR_COUNT_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_FAIL_COUNT_COLUMN_ID, VALIDATION_RULE_SUMMARY_FALSE_COUNT_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_INPUT_ID_COLUMN_ID, VALIDATION_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_MESSAGE_COLUMN_ID, VALIDATION_RULE_SUMMARY_NODE_ID_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_NULL_COUNT_COLUMN_ID, VALIDATION_RULE_SUMMARY_PASS_COUNT_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
    VALIDATION_RULE_SUMMARY_WARNING_COUNT_COLUMN_ID, VERIFICATION_CONTRACT_VERSION,
};
