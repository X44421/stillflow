//! Versioned SQLite metadata and immutable Parquet snapshot storage.
//!
//! The crate owns local persistence adapters only. Stable logical identities and
//! schemas remain in `stillflow-core`.

pub mod artifact;
pub mod bundle;
pub mod control_plane;
pub mod dedup;
mod digest;
mod error;
pub mod export;
mod manifest;
mod store;

pub use artifact::{
    dedup_rule_summary_section_schema, duplicate_finding_section_schema,
    rejected_rows_control_fields, rejected_rows_section_schema, validation_finding_section_schema,
    validation_rule_summary_section_schema, ArtifactManifest, ArtifactPartition, ArtifactSection,
    ArtifactSectionId, ArtifactSectionStats, MAX_BUNDLE_REPORT_BYTES, MAX_BUNDLE_REPORT_PARTITIONS,
    MAX_BUNDLE_REPORT_ROWS, MAX_REPORT_BYTES, MAX_REPORT_PARTITIONS, MAX_REPORT_ROWS,
    REPORT_PACK_BYTES, REPORT_PACK_ROWS,
};
pub use bundle::{
    AcceptedSnapshotArtifact, DeduplicationReportArtifact, RejectedRowsArtifact,
    ValidationReportArtifact, VerificationBundle, VerificationBundleDraft,
    VerificationBundleMembership, VerificationBundleWriter,
};
pub use control_plane::{
    ArtifactCursor, ArtifactPage, ArtifactRefDraft, ArtifactRefRecord, ControlPlaneStore,
    DatasetRecord, EventCursor, EventDraft, EventPage, EventRecord, ExternalRefKind, FailureInfo,
    JobCursor, JobPage, JobRecord, JobSubmission, PlanRecord, PlanVersionDraft, PlanVersionRecord,
    RunCursor, RunPage, RunRecord, SessionRecord, SourceAssetRecord, SourceConnectionRecord,
    SubmitOutcome, WorkspaceRecord,
};
pub use dedup::{
    DedupIndex, DedupInsert, MAX_DEDUP_INDEX_CACHE_KIB, MAX_DEDUP_INDEX_DISK_BYTES,
    MAX_DEDUP_INDEX_PAGES, MAX_DEDUP_KEY_BYTES,
};
pub use digest::{ContentDigest, DIGEST_BUFFER_BYTES};
pub use error::{IntegrityFailure, StorageError};
pub use export::{
    compute_export_set_digest, ExportManifest, ExportManifestFile, ExportPlan, ExportProvenance,
    ExportWriter, StagedExportFile,
};
pub use manifest::{
    GarbageCollectionReport, RecoveryReport, SnapshotDraft, SnapshotManifest, SnapshotPartition,
    StorageLimits, MAX_ACTIVE_PUBLISHERS, MAX_ACTIVE_READERS, MAX_INPUT_ENVELOPES,
    MAX_MAINTENANCE_CANDIDATES, MAX_SNAPSHOT_PARTITIONS, MAX_SNAPSHOT_ROWS,
    MAX_SNAPSHOT_STORED_BYTES, SQLITE_BUSY_TIMEOUT_MILLIS, STORAGE_SCHEMA_VERSION,
};
pub use store::{SnapshotBatchReader, SnapshotStore, SnapshotWriter};

pub(crate) use digest::digest_file;
pub(crate) use manifest::build_snapshot;
#[cfg(test)]
pub(crate) use store::acquire_maintenance;
pub(crate) use store::{
    abort_bundle_publication, acquire_activity, create_exact_directory, ensure_managed_directory,
    ensure_private_directory, format_timestamp, insert_visible_snapshot, integrity_error,
    load_manifest_inner, open_connection, parse_timestamp, partitions_root, read_partition,
    staging_root, sync_directory, write_envelope_parquet, ActivityGuard, ActivityKind, StoreInner,
};
