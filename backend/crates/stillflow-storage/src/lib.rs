//! Versioned SQLite metadata and immutable Parquet snapshot storage.
//!
//! The crate owns local persistence adapters only. Stable logical identities and
//! schemas remain in `stillflow-core`.

pub mod artifact;
pub mod audit;
pub mod automation;
pub mod backup;
pub mod bundle;
pub mod control_plane;
pub mod dedup;
mod digest;
mod error;
pub mod export;
pub mod identity;
mod manifest;
pub mod profile_history;
pub mod retention;
mod store;

pub use artifact::{
    dedup_rule_summary_section_schema, duplicate_finding_section_schema,
    rejected_rows_control_fields, rejected_rows_section_schema, validation_finding_section_schema,
    validation_rule_summary_section_schema, ArtifactManifest, ArtifactPartition, ArtifactSection,
    ArtifactSectionId, ArtifactSectionStats, MAX_BUNDLE_REPORT_BYTES, MAX_BUNDLE_REPORT_PARTITIONS,
    MAX_BUNDLE_REPORT_ROWS, MAX_REPORT_BYTES, MAX_REPORT_PARTITIONS, MAX_REPORT_ROWS,
    REPORT_PACK_BYTES, REPORT_PACK_ROWS,
};
pub use audit::{
    AuditActor, AuditActorKind, AuditCursor, AuditEventDraft, AuditEventRecord, AuditLineageEdge,
    AuditObjectRef, AuditPage, AuditQuery, AuditRetentionState, AuditStore, AUDIT_VERSION,
    MAX_AUDIT_LINEAGE_EDGES, MAX_AUDIT_PAGE_SIZE, MAX_AUDIT_TEXT_BYTES,
};
pub use automation::{
    AutomationScheduleDraft, AutomationScheduleRecord, AutomationScheduleState, AutomationTrigger,
    AutomationTriggerLease, DEFAULT_AUTOMATION_CLAIM_LEASE_SECONDS,
    MAX_AUTOMATION_SUBMISSION_ATTEMPTS, MAX_AUTOMATION_TEMPLATE_BYTES,
};
pub use backup::{BackupFile, BackupManifest, BACKUP_FORMAT_VERSION, BACKUP_MANIFEST_FILE};
pub use bundle::{
    AcceptedSnapshotArtifact, DeduplicationReportArtifact, RejectedRowsArtifact,
    ValidationReportArtifact, VerificationBundle, VerificationBundleDraft,
    VerificationBundleMembership, VerificationBundleWriter,
};
pub use control_plane::{
    ArtifactBodyRecord, ArtifactCursor, ArtifactOutputRef, ArtifactPage, ArtifactRefDraft,
    ArtifactRefRecord, ControlPlaneStore, DatasetRecord, EventCursor, EventDraft, EventPage,
    EventRecord, ExternalRefKind, FailureInfo, JobCursor, JobPage, JobRecord, JobRecoveryDraft,
    JobRecoveryResult, JobSubmission, PlanRecord, PlanVersionDraft, PlanVersionRecord,
    ReconciliationCandidate, RunCursor, RunPage, RunRecord, SessionRecord, SnapshotOutputRef,
    SourceAssetRecord, SourceConnectionRecord, SubmitOutcome, TerminalOutputRef, WorkspaceRecord,
};
pub use dedup::{
    DedupIndex, DedupInsert, MAX_DEDUP_INDEX_CACHE_KIB, MAX_DEDUP_INDEX_DISK_BYTES,
    MAX_DEDUP_INDEX_PAGES, MAX_DEDUP_KEY_BYTES,
};
pub use digest::{ContentDigest, DIGEST_BUFFER_BYTES};
pub use error::{IntegrityFailure, StorageError};
pub use export::{
    compute_export_set_digest, ExportFileChunk, ExportManifest, ExportManifestFile, ExportPlan,
    ExportProvenance, ExportWriter, StagedExportFile,
};
pub use identity::{
    CredentialOwner, CredentialProvider, CredentialProviderError, CredentialProviderRegistry,
    CredentialRefDraft, CredentialRefRecord, CredentialState, EnvironmentCredentialProvider,
    ExternalCredentialBackend, ExternalCredentialProvider, IdentityState, IdentityStore,
    KeychainBackend, MemberRecord, OsKeychainProvider, PrincipalKind, RoleRecord, SecretMaterial,
    ServiceAccountRecord,
};
pub use manifest::{
    GarbageCollectionReport, RecoveryReport, SnapshotDraft, SnapshotManifest, SnapshotPartition,
    StorageLimits, MAX_ACTIVE_PUBLISHERS, MAX_ACTIVE_READERS, MAX_INPUT_ENVELOPES,
    MAX_MAINTENANCE_CANDIDATES, MAX_SNAPSHOT_PARTITIONS, MAX_SNAPSHOT_ROWS,
    MAX_SNAPSHOT_STORED_BYTES, SQLITE_BUSY_TIMEOUT_MILLIS, STORAGE_SCHEMA_VERSION,
};
pub use profile_history::{
    DriftComparisonRecord, DriftReportCursor, DriftReportDraft, DriftReportPage,
    ProfileHistoryCursor, ProfileHistoryDraft, ProfileHistoryEntry, ProfileHistoryPage,
    ProfileHistoryState,
};
pub use retention::{RetentionCandidate, RetentionObjectKind, RetentionPolicy, RetentionReport};
pub use store::{SnapshotBatchReader, SnapshotStore, SnapshotWriter};

pub(crate) use bundle::verification_bundle_version_digest_inner;
pub(crate) use control_plane::{
    append_event_tx, compact_json, map_constraint, validate_artifact_body, validate_safe_json,
};
pub(crate) use digest::digest_file;
pub(crate) use manifest::build_snapshot;
pub(crate) use store::acquire_maintenance;
pub(crate) use store::{
    abort_bundle_publication, acquire_activity, create_exact_directory, ensure_managed_directory,
    ensure_private_directory, format_timestamp, insert_visible_snapshot, integrity_error,
    load_manifest_inner, open_connection, parse_timestamp, partitions_root, read_partition,
    snapshot_version_digest_inner, staging_root, sync_directory, write_envelope_parquet,
    ActivityGuard, ActivityKind, StoreInner,
};
