//! Atomic `VerificationBundle` publication, loading, and bounded section
//! reading (contract sections 8, 10.1–10.5).
//!
//! The bundle is the only visibility boundary: readers either load the
//! complete bundle by `bundle_id`, by `run_id`, or by accepted snapshot id,
//! or see none of it. Publication follows the journal-before-staging facts
//! recorded in `docs/issues/storage-publication-recovery-inventory.md`: the
//! bundle publication journal commits before staging exists, final artifact
//! files precede SQLite visibility, and visibility plus journal deletion
//! share one SQLite transaction.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use rusqlite::{params, TransactionBehavior};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use stillflow_core::{
    logical_schema_to_arrow, ArtifactKind, ArtifactProvenance, ArtifactProvenanceDraft,
    ArtifactSummary, BatchEnvelope, LogicalSchema, LogicalSchemaFingerprint, SnapshotStats,
    MAX_BATCH_ROWS, VERIFICATION_CONTRACT_VERSION,
};

use crate::artifact;

use crate::artifact::{
    accepted_partition_canonical_digest, accepted_snapshot_manifest_digest, canonical_batch_bytes,
    compute_artifact_provenance_digest, compute_bundle_provenance_digest, compute_partition_digest,
    compute_section_digest, AcceptedCanonicalPartition, ArtifactManifest, ArtifactPartition,
    ArtifactSection, ArtifactSectionId, ArtifactSectionStats, MAX_BUNDLE_REPORT_BYTES,
    MAX_BUNDLE_REPORT_PARTITIONS, MAX_BUNDLE_REPORT_ROWS, MAX_REPORT_BYTES, MAX_REPORT_PARTITIONS,
    MAX_REPORT_ROWS,
};
use crate::dedup::{self, DedupIndex};
use crate::{
    abort_bundle_publication, acquire_activity, build_snapshot, create_exact_directory,
    format_timestamp, integrity_error, load_manifest_inner, open_connection, staging_root,
    sync_directory, write_envelope_parquet, ActivityGuard, ActivityKind, SnapshotDraft,
    SnapshotManifest, SnapshotPartition, SnapshotStore, StorageError, StoreInner,
    MAX_INPUT_ENVELOPES,
};

use rusqlite::OptionalExtension;

const MEMBERSHIP_DOCUMENT_VERSION: u16 = 1;
const PROVENANCE_DOCUMENT_VERSION: u16 = 1;

/// Versioned compact-JSON persistence document (contract section 8.1).
#[derive(Serialize, Deserialize)]
struct MembershipDocument {
    version: u16,
    #[serde(flatten)]
    membership: VerificationBundleMembership,
}

/// Versioned compact-JSON persistence document (contract section 8.1).
#[derive(Serialize, Deserialize)]
struct ProvenanceDocument {
    version: u16,
    #[serde(flatten)]
    provenance: ArtifactProvenance,
}

/// Exact artifact identities committed atomically with every manifest
/// (contract section 8.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationBundleMembership {
    bundle_id: Uuid,
    run_id: Uuid,
    bundle_artifact_id: Uuid,
    accepted_snapshot_id: Uuid,
    validation_report_artifact_id: Uuid,
    rejected_rows_artifact_id: Option<Uuid>,
    deduplication_report_artifact_id: Uuid,
}

impl VerificationBundleMembership {
    pub const fn bundle_id(&self) -> Uuid {
        self.bundle_id
    }

    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub const fn bundle_artifact_id(&self) -> Uuid {
        self.bundle_artifact_id
    }

    pub const fn accepted_snapshot_id(&self) -> Uuid {
        self.accepted_snapshot_id
    }

    pub const fn validation_report_artifact_id(&self) -> Uuid {
        self.validation_report_artifact_id
    }

    /// `None` exactly when the rejected artifact is absent (zero terminal
    /// rejections; contract 10.2).
    pub const fn rejected_rows_artifact_id(&self) -> Option<Uuid> {
        self.rejected_rows_artifact_id
    }

    pub const fn deduplication_report_artifact_id(&self) -> Uuid {
        self.deduplication_report_artifact_id
    }
}

macro_rules! artifact_wrapper {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            manifest: ArtifactManifest,
            provenance: ArtifactProvenance,
        }

        impl $name {
            pub const fn manifest(&self) -> &ArtifactManifest {
                &self.manifest
            }

            pub const fn provenance(&self) -> &ArtifactProvenance {
                &self.provenance
            }
        }
    };
}

artifact_wrapper!(ValidationReportArtifact);
artifact_wrapper!(RejectedRowsArtifact);
artifact_wrapper!(DeduplicationReportArtifact);

/// The always-present accepted snapshot child with its committed provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedSnapshotArtifact {
    manifest: SnapshotManifest,
    provenance: ArtifactProvenance,
}

impl AcceptedSnapshotArtifact {
    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub const fn provenance(&self) -> &ArtifactProvenance {
        &self.provenance
    }
}

/// One atomically published verification bundle (contract section 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationBundle {
    membership: VerificationBundleMembership,
    provenance: ArtifactProvenance,
    accepted: AcceptedSnapshotArtifact,
    validation_report: ValidationReportArtifact,
    rejected_rows: Option<RejectedRowsArtifact>,
    deduplication_report: DeduplicationReportArtifact,
}

impl VerificationBundle {
    pub const fn membership(&self) -> &VerificationBundleMembership {
        &self.membership
    }

    /// Bundle-level provenance record; distinct from every child artifact.
    pub const fn provenance(&self) -> &ArtifactProvenance {
        &self.provenance
    }

    pub const fn accepted(&self) -> &AcceptedSnapshotArtifact {
        &self.accepted
    }

    pub const fn validation_report(&self) -> &ValidationReportArtifact {
        &self.validation_report
    }

    pub const fn rejected_rows(&self) -> Option<&RejectedRowsArtifact> {
        self.rejected_rows.as_ref()
    }

    pub const fn deduplication_report(&self) -> &DeduplicationReportArtifact {
        &self.deduplication_report
    }
}

/// Deterministic test hook at the Prepared-window boundary (journal
/// committed, staging not yet created). Test-only; always `None` in
/// production builds.
#[cfg(test)]
#[allow(clippy::type_complexity)]
pub(crate) static PREPARED_WINDOW_HOOK: std::sync::Mutex<
    Option<std::sync::Arc<dyn Fn(&StoreInner, Uuid) + Send + Sync>>,
> = std::sync::Mutex::new(None);

/// Engine-assembled draft handed to `begin_verification_bundle` (contract
/// sections 7.3, 10.5, and 11). Validated before any storage I/O.
#[derive(Debug, Clone)]
pub struct VerificationBundleDraft {
    provenance: ArtifactProvenanceDraft,
    accepted: SnapshotDraft,
    validation_report_artifact_id: Uuid,
    rejected_rows_artifact_id: Option<Uuid>,
    deduplication_report_artifact_id: Uuid,
    /// Frozen logical Scan-output schema the rejected artifact binds to
    /// (issue #176, D2). `None` keeps the deprecated accepted-derived
    /// fallback for callers that have not migrated yet.
    rejected_source_schema: Option<LogicalSchema>,
}

impl VerificationBundleDraft {
    /// Validates every frozen identity rule; storage I/O stays impossible
    /// until `begin_verification_bundle` accepts the draft.
    pub fn try_new(
        provenance: ArtifactProvenanceDraft,
        accepted: SnapshotDraft,
        validation_report_artifact_id: Uuid,
        rejected_rows_artifact_id: Option<Uuid>,
        deduplication_report_artifact_id: Uuid,
    ) -> Result<Self, StorageError> {
        let input = &provenance.input;
        if provenance.verification_contract_version != VERIFICATION_CONTRACT_VERSION {
            return Err(StorageError::InvalidDraft(
                "unsupported verification contract version",
            ));
        }
        if input.artifact_kind != ArtifactKind::VerificationBundle {
            return Err(StorageError::InvalidDraft(
                "bundle provenance must use the verification-bundle artifact kind",
            ));
        }
        if provenance.engine_build.trim().is_empty() {
            return Err(StorageError::InvalidDraft(
                "engine build identity must not be empty",
            ));
        }
        let bundle_artifact_id = input.artifact_id;
        let mut identities = [
            Some(input.run_id),
            Some(input.bundle_id),
            Some(bundle_artifact_id),
            Some(accepted.id()),
            Some(validation_report_artifact_id),
            rejected_rows_artifact_id,
            Some(deduplication_report_artifact_id),
        ];
        identities.sort();
        for window in identities.windows(2) {
            if window[0] == window[1] {
                return Err(StorageError::InvalidDraft(
                    "verification identities must be pairwise distinct",
                ));
            }
        }
        if [
            input.run_id,
            input.bundle_id,
            bundle_artifact_id,
            accepted.id(),
            validation_report_artifact_id,
            deduplication_report_artifact_id,
        ]
        .iter()
        .any(|id| id.is_nil())
            || rejected_rows_artifact_id.is_some_and(|id| id.is_nil())
        {
            return Err(StorageError::InvalidDraft(
                "verification identities must not be nil",
            ));
        }
        if input.lineage.iter().any(Uuid::is_nil) {
            return Err(StorageError::InvalidDraft(
                "lineage identities must not be nil",
            ));
        }
        if !(input.created_at <= input.started_at && input.started_at <= input.committed_at) {
            return Err(StorageError::InvalidTimestampOrder(
                "provenance created, started, and committed timestamps",
            ));
        }

        Ok(Self {
            provenance,
            accepted,
            validation_report_artifact_id,
            rejected_rows_artifact_id,
            deduplication_report_artifact_id,
            rejected_source_schema: None,
        })
    }

    /// Binds the rejected artifact to the frozen logical Scan-output schema
    /// (contract §8.6/8.7, V21). Required whenever the accepted (materialized)
    /// schema can diverge from the scan output — e.g. after Drop, Rename,
    /// Cast, or Derive rules — because the rejected payload rows are original
    /// Scan-output rows. Without this binding the section schema falls back
    /// to the accepted-derived derivation, which is deprecated for divergent
    /// schemas (issue #176, D2; the E4-S2 engine wires this in REBIND-R2).
    pub fn with_rejected_source_schema(mut self, schema: LogicalSchema) -> Self {
        self.rejected_source_schema = Some(schema);
        self
    }

    pub const fn provenance(&self) -> &ArtifactProvenanceDraft {
        &self.provenance
    }

    pub const fn accepted(&self) -> &SnapshotDraft {
        &self.accepted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BundleState {
    Staged,
    Installing,
    Committed,
    Failed,
}

struct SectionStaging {
    section_id: ArtifactSectionId,
    artifact_id: Uuid,
    kind: ArtifactKind,
    schema: Arc<LogicalSchema>,
    fingerprint: LogicalSchemaFingerprint,
    next_sequence: u64,
    envelope_count: u32,
    partitions: Vec<ArtifactPartition>,
    finding_count: u64,
    warning_count: u64,
    error_count: u64,
    duplicate_count: u64,
}

/// Publisher-side writer for one bundle staging context. Dropping an
/// uncommitted writer aborts the whole bundle (contract section 10.3).
pub struct VerificationBundleWriter {
    inner: Arc<StoreInner>,
    _activity: Option<ActivityGuard>,
    draft: VerificationBundleDraft,
    started_at: DateTime<Utc>,
    staging_dir: PathBuf,
    state: BundleState,
    accepted_next_sequence: u64,
    accepted_envelope_count: u32,
    accepted_partitions: Vec<SnapshotPartition>,
    /// Logical per-partition digest inputs (contract 8.1.1): canonical Arrow
    /// IPC bytes and canonical byte counts, kept strictly separate from the
    /// physical `accepted_partitions` Parquet file facts.
    accepted_canonical_partitions: Vec<AcceptedCanonicalPartition>,
    accepted_row_count: u64,
    accepted_stored_byte_count: u64,
    sections: Vec<SectionStaging>,
    installed_dirs: Vec<PathBuf>,
}

impl fmt::Debug for VerificationBundleWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerificationBundleWriter")
            .field("bundle_id", &self.draft.provenance.input.bundle_id)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

fn section_plan(draft: &VerificationBundleDraft) -> Result<Vec<SectionStaging>, StorageError> {
    let fingerprint_of = |schema: &LogicalSchema| {
        LogicalSchemaFingerprint::try_from_schema(schema)
            .map_err(|_| StorageError::InvalidManifest("report schema fingerprint failed"))
    };
    let mut sections = vec![
        SectionStaging {
            section_id: ArtifactSectionId::ValidationRuleSummary,
            artifact_id: draft.validation_report_artifact_id,
            kind: ArtifactKind::ValidationReport,
            fingerprint: fingerprint_of(&artifact::validation_rule_summary_section_schema())?,
            schema: Arc::new(artifact::validation_rule_summary_section_schema()),
            next_sequence: 0,
            envelope_count: 0,
            partitions: Vec::new(),
            finding_count: 0,
            warning_count: 0,
            error_count: 0,
            duplicate_count: 0,
        },
        SectionStaging {
            section_id: ArtifactSectionId::ValidationFinding,
            artifact_id: draft.validation_report_artifact_id,
            kind: ArtifactKind::ValidationReport,
            fingerprint: fingerprint_of(&artifact::validation_finding_section_schema())?,
            schema: Arc::new(artifact::validation_finding_section_schema()),
            next_sequence: 0,
            envelope_count: 0,
            partitions: Vec::new(),
            finding_count: 0,
            warning_count: 0,
            error_count: 0,
            duplicate_count: 0,
        },
    ];
    if let Some(rejected_id) = draft.rejected_rows_artifact_id {
        // Issue #176 (D2): the rejected artifact binds to the frozen logical
        // Scan-output schema when the publisher supplies it; the accepted-
        // derived fallback is deprecated and only matches callers whose
        // materialized schema still equals the scan output.
        let rejected_base = match draft.rejected_source_schema.as_ref() {
            Some(schema) => schema,
            None => draft.accepted.schema(),
        };
        let schema = artifact::rejected_rows_section_schema(rejected_base)?;
        sections.push(SectionStaging {
            section_id: ArtifactSectionId::RejectedRows,
            artifact_id: rejected_id,
            kind: ArtifactKind::RejectedRows,
            fingerprint: fingerprint_of(&schema)?,
            schema: Arc::new(schema),
            next_sequence: 0,
            envelope_count: 0,
            partitions: Vec::new(),
            finding_count: 0,
            warning_count: 0,
            error_count: 0,
            duplicate_count: 0,
        });
    }
    sections.push(SectionStaging {
        section_id: ArtifactSectionId::DedupRuleSummary,
        artifact_id: draft.deduplication_report_artifact_id,
        kind: ArtifactKind::DeduplicationReport,
        fingerprint: fingerprint_of(&artifact::dedup_rule_summary_section_schema())?,
        schema: Arc::new(artifact::dedup_rule_summary_section_schema()),
        next_sequence: 0,
        envelope_count: 0,
        partitions: Vec::new(),
        finding_count: 0,
        warning_count: 0,
        error_count: 0,
        duplicate_count: 0,
    });
    sections.push(SectionStaging {
        section_id: ArtifactSectionId::DuplicateFinding,
        artifact_id: draft.deduplication_report_artifact_id,
        kind: ArtifactKind::DeduplicationReport,
        fingerprint: fingerprint_of(&artifact::duplicate_finding_section_schema())?,
        schema: Arc::new(artifact::duplicate_finding_section_schema()),
        next_sequence: 0,
        envelope_count: 0,
        partitions: Vec::new(),
        finding_count: 0,
        warning_count: 0,
        error_count: 0,
        duplicate_count: 0,
    });
    Ok(sections)
}

impl SnapshotStore {
    /// Begins one bundle staging context under exactly one publisher permit
    /// (contract section 10.1 step 4). The publication journal row commits
    /// before the staging directory exists, freezing the `Prepared` window.
    pub fn begin_verification_bundle(
        &self,
        draft: VerificationBundleDraft,
        started_at: DateTime<Utc>,
    ) -> Result<VerificationBundleWriter, StorageError> {
        if draft.provenance.input.created_at > started_at {
            return Err(StorageError::InvalidTimestampOrder(
                "provenance creation and publication start",
            ));
        }
        let activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let result = self.begin_verification_bundle_inner(&draft, started_at);
        match result {
            Ok(mut writer) => {
                writer._activity = Some(activity);
                Ok(writer)
            }
            Err(error) => {
                drop(activity);
                Err(error)
            }
        }
    }

    fn begin_verification_bundle_inner(
        &self,
        draft: &VerificationBundleDraft,
        started_at: DateTime<Utc>,
    ) -> Result<VerificationBundleWriter, StorageError> {
        let inner = &self.inner;
        let bundle_id = draft.provenance.input.bundle_id;
        let mut connection = open_connection(inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin bundle journal transaction"))?;
        // Symmetric identity reservation (contract 10.5): every identity that
        // maps to a `partitions/<id>` directory — the accepted snapshot id and
        // each artifact id — must be free across BOTH families. It may not be
        // held by an ordinary snapshot (committed or pending), by any pending
        // bundle journal row, or by any committed bundle, regardless of which
        // column carries it. A `None` rejected id binds SQL NULL, which never
        // matches.
        let conflict: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM snapshots
                                 WHERE id IN (?1, ?2, ?3, ?4, ?5, ?6))
                   OR EXISTS(SELECT 1 FROM publications
                              WHERE snapshot_id IN (?1, ?2, ?3, ?4, ?5, ?6))
                   OR EXISTS(SELECT 1 FROM bundle_publications WHERE
                              bundle_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR accepted_snapshot_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR bundle_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR validation_report_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR rejected_rows_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR deduplication_report_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6))
                   OR EXISTS(SELECT 1 FROM verification_bundles WHERE
                              bundle_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR accepted_snapshot_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR bundle_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR validation_report_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR rejected_rows_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6)
                           OR deduplication_report_artifact_id IN (?1, ?2, ?3, ?4, ?5, ?6))",
                params![
                    bundle_id.to_string(),
                    draft.accepted.id().to_string(),
                    draft.provenance.input.artifact_id.to_string(),
                    draft.validation_report_artifact_id.to_string(),
                    draft.rejected_rows_artifact_id.map(|id| id.to_string()),
                    draft.deduplication_report_artifact_id.to_string(),
                ],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("check bundle identity conflicts"))?;
        if conflict {
            return Err(StorageError::AlreadyExists(bundle_id));
        }
        transaction
            .execute(
                "INSERT INTO bundle_publications(
                     bundle_id, run_id, accepted_snapshot_id, bundle_artifact_id,
                     validation_report_artifact_id, rejected_rows_artifact_id,
                     deduplication_report_artifact_id, started_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    bundle_id.to_string(),
                    draft.provenance.input.run_id.to_string(),
                    draft.accepted.id().to_string(),
                    draft.provenance.input.artifact_id.to_string(),
                    draft.validation_report_artifact_id.to_string(),
                    draft.rejected_rows_artifact_id.map(|id| id.to_string()),
                    draft.deduplication_report_artifact_id.to_string(),
                    format_timestamp(&started_at),
                ],
            )
            .map_err(|_| StorageError::database("insert bundle publication journal"))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit bundle publication journal"))?;
        drop(connection);

        // Deterministic test hook at the exact Prepared-window boundary: the
        // publication journal is committed and the staging directory does not
        // exist yet. The order-sensitivity test asserts both facts here, so a
        // regression that creates staging before the journal commit fails the
        // invariant.
        #[cfg(test)]
        {
            let hook = PREPARED_WINDOW_HOOK
                .lock()
                .expect("prepared window hook mutex")
                .as_ref()
                .cloned();
            if let Some(hook) = hook {
                hook(inner, bundle_id);
            }
        }

        let staging_dir = staging_root(inner).join(bundle_id.to_string());
        if let Err(error) = create_exact_directory(&staging_dir, "create bundle staging directory")
        {
            abort_bundle_publication(inner, bundle_id);
            return Err(error);
        }

        Ok(VerificationBundleWriter {
            inner: Arc::clone(inner),
            _activity: None,
            draft: draft.clone(),
            started_at,
            staging_dir,
            state: BundleState::Staged,
            accepted_next_sequence: 0,
            accepted_envelope_count: 0,
            accepted_partitions: Vec::new(),
            accepted_canonical_partitions: Vec::new(),
            accepted_row_count: 0,
            accepted_stored_byte_count: 0,
            sections: section_plan(draft)?,
            installed_dirs: Vec::new(),
        })
    }

    /// Opens one run's exclusive temporary dedup index (contract 9.1).
    ///
    /// The whole open critical section runs under a reader-class activity
    /// guard, which the maintenance gate excludes: recovery can therefore
    /// never scan, classify, or unlink a `.lock` between its creation and its
    /// flock (E4-S1-R1 blocker D). After the index returns, the OS flock plus
    /// the in-open lock-identity revalidation carry ownership; no guard is
    /// held for the index lifetime.
    pub fn open_dedup_index(
        &self,
        run_id: Uuid,
        bundle_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<DedupIndex, StorageError> {
        // Reader class (not publisher) so a bundle flow already holding its
        // publisher permit can still open its index; readers equally exclude
        // maintenance/recovery.
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        dedup::open_dedup_index(&self.inner, run_id, bundle_id, started_at)
    }

    /// Loads one committed bundle by its atomic visibility identity.
    pub fn load_verification_bundle(
        &self,
        bundle_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        load_bundle_inner(&self.inner, bundle_id)
    }

    /// Loads the unique committed bundle containing this accepted snapshot.
    pub fn load_verification_bundle_by_snapshot(
        &self,
        snapshot_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let bundle_id = lookup_bundle_id(
            &self.inner,
            "accepted_snapshot_id",
            &snapshot_id.to_string(),
        )?
        .ok_or(StorageError::NotFound(snapshot_id))?;
        load_bundle_inner(&self.inner, bundle_id)
    }

    /// Loads the unique committed bundle for this run; cancellation, failure,
    /// or unknown runs yield `NotFound` (contract 8.1).
    pub fn load_verification_bundle_by_run_id(
        &self,
        run_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let bundle_id = lookup_bundle_id(&self.inner, "run_id", &run_id.to_string())?
            .ok_or(StorageError::NotFound(run_id))?;
        load_bundle_inner(&self.inner, bundle_id)
    }

    /// Opens one artifact section through bundle membership; no reader can
    /// bypass the bundle transaction (contract 8.1).
    pub fn open_artifact_section(
        &self,
        bundle_id: Uuid,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
    ) -> Result<ArtifactBatchReader, StorageError> {
        let activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let membership = load_membership_inner(&self.inner, bundle_id)?
            .ok_or(StorageError::NotFound(bundle_id))?;
        if artifact_id == membership.bundle_artifact_id {
            return Err(StorageError::InvalidManifest(
                "the bundle provenance record owns no artifact sections",
            ));
        }
        if artifact_id != membership.validation_report_artifact_id
            && artifact_id != membership.deduplication_report_artifact_id
            && Some(artifact_id) != membership.rejected_rows_artifact_id
        {
            return Err(StorageError::NotFound(artifact_id));
        }
        let (manifest, _) = load_artifact_row(&self.inner, bundle_id, artifact_id)?;
        let section = manifest
            .section(section_id)
            .cloned()
            .ok_or(StorageError::NotFound(artifact_id))?;
        let accepted = load_manifest_inner(&self.inner, membership.accepted_snapshot_id)?;
        let source_asset_id = accepted.snapshot().source_asset_id();
        Ok(ArtifactBatchReader {
            inner: Arc::clone(&self.inner),
            _activity: activity,
            artifact_id,
            section,
            source_asset_id,
            next_partition: 0,
        })
    }
}

fn lookup_bundle_id(
    inner: &StoreInner,
    column: &str,
    value: &str,
) -> Result<Option<Uuid>, StorageError> {
    // Column names come only from the two call sites above, never from input.
    let statement = match column {
        "run_id" => "SELECT bundle_id FROM verification_bundles WHERE run_id = ?1",
        _ => "SELECT bundle_id FROM verification_bundles WHERE accepted_snapshot_id = ?1",
    };
    let connection = open_connection(inner)?;
    let raw: Option<String> = connection
        .query_row(statement, params![value], |row| row.get(0))
        .optional()
        .map_err(|_| StorageError::database("look up bundle identity"))?;
    raw.map(|text| {
        Uuid::parse_str(&text).map_err(|_| StorageError::InvalidManifest("bundle identity"))
    })
    .transpose()
}

fn load_membership_inner(
    inner: &StoreInner,
    bundle_id: Uuid,
) -> Result<Option<VerificationBundleMembership>, StorageError> {
    let connection = open_connection(inner)?;
    let membership_json: Option<String> = connection
        .query_row(
            "SELECT membership_json FROM verification_bundles WHERE bundle_id = ?1",
            params![bundle_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("load bundle membership"))?;
    let Some(membership_json) = membership_json else {
        return Ok(None);
    };
    let document: MembershipDocument = serde_json::from_str(&membership_json)
        .map_err(|_| StorageError::Serialization("decode bundle membership"))?;
    if document.version != MEMBERSHIP_DOCUMENT_VERSION {
        return Err(StorageError::UnsupportedStorageVersion(i64::from(
            document.version,
        )));
    }
    if document.membership.bundle_id != bundle_id {
        return Err(StorageError::InvalidManifest(
            "bundle membership identity mismatch",
        ));
    }
    Ok(Some(document.membership))
}

fn decode_kind(text: &str) -> Result<ArtifactKind, StorageError> {
    match text {
        "verification-bundle" => Ok(ArtifactKind::VerificationBundle),
        "accepted-snapshot" => Ok(ArtifactKind::AcceptedSnapshot),
        "validation-report" => Ok(ArtifactKind::ValidationReport),
        "rejected-rows" => Ok(ArtifactKind::RejectedRows),
        "deduplication-report" => Ok(ArtifactKind::DeduplicationReport),
        _ => Err(StorageError::InvalidManifest("artifact kind is invalid")),
    }
}

fn load_artifact_row(
    inner: &StoreInner,
    bundle_id: Uuid,
    artifact_id: Uuid,
) -> Result<(ArtifactManifest, ArtifactProvenance), StorageError> {
    let connection = open_connection(inner)?;
    let row: Option<(String, String, String)> = connection
        .query_row(
            "SELECT kind, manifest_json, provenance_json FROM artifact_manifests
             WHERE artifact_id = ?1 AND bundle_id = ?2",
            params![artifact_id.to_string(), bundle_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("load artifact manifest"))?;
    let Some((kind_text, manifest_json, provenance_json)) = row else {
        return Err(StorageError::NotFound(artifact_id));
    };
    let expected_kind = decode_kind(&kind_text)?;

    let manifest: ArtifactManifest = serde_json::from_str(&manifest_json)
        .map_err(|_| StorageError::Serialization("decode artifact manifest"))?;
    if manifest.artifact_id() != artifact_id {
        return Err(StorageError::InvalidManifest(
            "artifact manifest identity mismatch",
        ));
    }
    if manifest.kind() != expected_kind {
        return Err(StorageError::InvalidManifest(
            "artifact kind does not match its row",
        ));
    }
    // Reload-fidelity gate: recompute every section and manifest digest from
    // the persisted structures and require exact equality (contract 8.1).
    let verified = ArtifactManifest::try_new(
        manifest.artifact_id(),
        manifest.kind(),
        manifest.sections().to_vec(),
    )?;
    if verified != manifest {
        return Err(StorageError::InvalidManifest(
            "artifact manifest digest mismatch",
        ));
    }

    let document: ProvenanceDocument = serde_json::from_str(&provenance_json)
        .map_err(|_| StorageError::Serialization("decode artifact provenance"))?;
    if document.version != PROVENANCE_DOCUMENT_VERSION {
        return Err(StorageError::UnsupportedStorageVersion(i64::from(
            document.version,
        )));
    }
    let provenance = document.provenance;
    if provenance.draft.input.artifact_id != artifact_id {
        return Err(StorageError::InvalidManifest(
            "artifact provenance identity mismatch",
        ));
    }
    let expected_content = compute_artifact_provenance_digest(
        provenance.draft.input.run_id,
        provenance.draft.input.bundle_id,
        artifact_id,
        provenance.draft.input.artifact_kind,
        &provenance.draft.canonical_plan_digest,
        &provenance.draft.input.input.version_digest,
        manifest.sections(),
        &manifest.manifest_digest(),
    )?;
    if expected_content != provenance.content_digest {
        return Err(StorageError::InvalidManifest(
            "artifact provenance content digest mismatch",
        ));
    }
    Ok((manifest, provenance))
}

fn child_draft(
    bundle_draft: &ArtifactProvenanceDraft,
    artifact_id: Uuid,
    kind: ArtifactKind,
) -> ArtifactProvenanceDraft {
    let mut draft = bundle_draft.clone();
    draft.input.artifact_id = artifact_id;
    draft.input.artifact_kind = kind;
    draft
}

fn accepted_provenance_from(
    bundle_draft: &ArtifactProvenanceDraft,
    manifest: &SnapshotManifest,
    content_digest: [u8; 32],
) -> ArtifactProvenance {
    let stats = manifest.snapshot().stats();
    ArtifactProvenance {
        draft: child_draft(
            bundle_draft,
            manifest.snapshot().id(),
            ArtifactKind::AcceptedSnapshot,
        ),
        summary: ArtifactSummary {
            row_count: stats.row_count(),
            stored_byte_count: stats.stored_byte_count(),
            partition_count: stats.partition_count(),
            finding_count: 0,
            warning_count: 0,
            error_count: 0,
            duplicate_count: 0,
        },
        content_digest,
    }
}

fn load_bundle_inner(
    inner: &StoreInner,
    bundle_id: Uuid,
) -> Result<VerificationBundle, StorageError> {
    let membership =
        load_membership_inner(inner, bundle_id)?.ok_or(StorageError::NotFound(bundle_id))?;

    let connection = open_connection(inner)?;
    let provenance_json: String = connection
        .query_row(
            "SELECT provenance_json FROM verification_bundles WHERE bundle_id = ?1",
            params![bundle_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("load bundle provenance"))?;
    drop(connection);
    let document: ProvenanceDocument = serde_json::from_str(&provenance_json)
        .map_err(|_| StorageError::Serialization("decode bundle provenance"))?;
    if document.version != PROVENANCE_DOCUMENT_VERSION {
        return Err(StorageError::UnsupportedStorageVersion(i64::from(
            document.version,
        )));
    }
    let bundle_draft = document.provenance.draft.clone();
    let bundle_provenance = document.provenance;
    if bundle_provenance.draft.input.artifact_id != membership.bundle_artifact_id
        || bundle_provenance.draft.input.artifact_kind != ArtifactKind::VerificationBundle
    {
        return Err(StorageError::InvalidManifest(
            "bundle provenance identity mismatch",
        ));
    }

    let accepted_manifest = load_manifest_inner(inner, membership.accepted_snapshot_id)?;
    // Recompute the logical accepted digest from the installed partitions:
    // each Parquet file is decoded (physical E3 integrity is verified inside
    // `read_partition`), re-canonicalized, and hashed with the frozen
    // formula. Bundles committed by older interim builds carry physical-file
    // digests and fail closed here with a typed mismatch.
    let mut accepted_canonical_partitions: Vec<AcceptedCanonicalPartition> =
        Vec::with_capacity(accepted_manifest.partitions().len());
    for partition in accepted_manifest.partitions() {
        let envelope = crate::read_partition(inner, accepted_manifest.snapshot(), partition)?;
        let canonical = canonical_batch_bytes(envelope.payload())?;
        let stored_byte_count = u64::try_from(canonical.len())
            .map_err(|_| StorageError::ArithmeticOverflow("canonical byte count"))?;
        let row_count = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("envelope row count"))?;
        let digest = accepted_partition_canonical_digest(
            membership.accepted_snapshot_id,
            partition.sequence(),
            row_count,
            stored_byte_count,
            std::slice::from_ref(&canonical),
        );
        accepted_canonical_partitions.push(AcceptedCanonicalPartition {
            sequence: partition.sequence(),
            row_count,
            stored_byte_count,
            digest,
        });
    }
    let accepted_digest = accepted_snapshot_manifest_digest(
        accepted_manifest.snapshot(),
        &accepted_canonical_partitions,
    )?;

    let (validation_manifest, validation_provenance) =
        load_artifact_row(inner, bundle_id, membership.validation_report_artifact_id)?;
    let (dedup_manifest, dedup_provenance) = load_artifact_row(
        inner,
        bundle_id,
        membership.deduplication_report_artifact_id,
    )?;
    let rejected = match membership.rejected_rows_artifact_id {
        None => None,
        Some(rejected_id) => {
            let (manifest, provenance) = load_artifact_row(inner, bundle_id, rejected_id)?;
            Some((manifest, provenance))
        }
    };

    let mut children: Vec<(Uuid, [u8; 32], [u8; 32])> = Vec::new();
    children.push((
        membership.accepted_snapshot_id,
        accepted_digest,
        accepted_digest,
    ));
    children.push((
        membership.validation_report_artifact_id,
        *validation_manifest.manifest_digest().as_bytes(),
        validation_provenance.content_digest,
    ));
    if let Some((rejected_manifest, rejected_provenance)) = &rejected {
        children.push((
            rejected_manifest.artifact_id(),
            *rejected_manifest.manifest_digest().as_bytes(),
            rejected_provenance.content_digest,
        ));
    }
    children.push((
        membership.deduplication_report_artifact_id,
        *dedup_manifest.manifest_digest().as_bytes(),
        dedup_provenance.content_digest,
    ));

    let expected_bundle_digest = compute_bundle_provenance_digest(
        membership.run_id,
        membership.bundle_id,
        membership.bundle_artifact_id,
        membership.accepted_snapshot_id,
        membership.validation_report_artifact_id,
        membership.rejected_rows_artifact_id,
        membership.deduplication_report_artifact_id,
        &children,
    );
    if expected_bundle_digest != bundle_provenance.content_digest {
        return Err(StorageError::InvalidManifest(
            "bundle provenance content digest mismatch",
        ));
    }

    Ok(VerificationBundle {
        provenance: bundle_provenance,
        accepted: AcceptedSnapshotArtifact {
            provenance: accepted_provenance_from(
                &bundle_draft,
                &accepted_manifest,
                accepted_digest,
            ),
            manifest: accepted_manifest,
        },
        validation_report: ValidationReportArtifact {
            manifest: validation_manifest,
            provenance: validation_provenance,
        },
        rejected_rows: rejected.map(|(manifest, provenance)| RejectedRowsArtifact {
            manifest,
            provenance,
        }),
        deduplication_report: DeduplicationReportArtifact {
            manifest: dedup_manifest,
            provenance: dedup_provenance,
        },
        membership,
    })
}

impl VerificationBundleWriter {
    fn ensure_writable(&self) -> Result<(), StorageError> {
        if self.state != BundleState::Staged {
            return Err(StorageError::InvalidDraft(
                "bundle writer is no longer accepting appends",
            ));
        }
        Ok(())
    }

    /// Appends one accepted logical Scan output envelope; each non-empty
    /// envelope becomes one immutable Parquet partition with unchanged E2
    /// snapshot semantics and limits.
    pub fn append_accepted(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let limits = self.inner.limits;
        let envelope_count = self
            .accepted_envelope_count
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("accepted envelope count"))?;
        if envelope_count > limits.max_input_envelopes() {
            return Err(StorageError::EnvelopeLimitExceeded {
                actual: envelope_count,
                maximum: limits.max_input_envelopes(),
            });
        }
        if envelope.sequence() != self.accepted_next_sequence {
            return Err(StorageError::Sequence {
                expected: self.accepted_next_sequence,
                actual: envelope.sequence(),
            });
        }
        if envelope.source_asset_id() != self.draft.accepted.source_asset_id() {
            return Err(StorageError::LineageMismatch {
                sequence: envelope.sequence(),
            });
        }
        if std::env::var_os("STILLFLOW_LAYOUT_DEBUG").is_some() {
            eprintln!(
                "[drift-debug] envelope fp={:?} schema={:?} | draft fp={:?} schema={:?}",
                envelope.schema_fingerprint(),
                envelope.schema(),
                self.draft.accepted.schema_fingerprint(),
                self.draft.accepted.schema()
            );
        }
        if envelope.schema_fingerprint() != self.draft.accepted.schema_fingerprint()
            || envelope.schema() != self.draft.accepted.schema()
        {
            return Err(StorageError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }
        self.accepted_next_sequence += 1;
        self.accepted_envelope_count = envelope_count;
        if envelope.row_count() == 0 {
            return Ok(());
        }

        let sequence = u32::try_from(self.accepted_partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition sequence"))?;
        let partition_count = sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("partition count"))?;
        if partition_count > limits.max_partitions() {
            return Err(StorageError::PartitionLimitExceeded {
                actual: partition_count,
                maximum: limits.max_partitions(),
            });
        }
        let envelope_rows = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("envelope row count"))?;
        let row_count = self
            .accepted_row_count
            .checked_add(envelope_rows)
            .ok_or(StorageError::ArithmeticOverflow("snapshot row count"))?;
        if row_count > limits.max_rows() {
            return Err(StorageError::RowLimitExceeded {
                actual: row_count,
                maximum: limits.max_rows(),
            });
        }

        // Logical canonical facts are derived BEFORE any filesystem write so a
        // canonicalization failure leaves the writer untouched (contract
        // 8.1.1: accepted partition digests cover canonical Arrow IPC bytes,
        // never the Parquet encoding).
        let canonical = canonical_batch_bytes(envelope.payload())?;
        let canonical_stored = u64::try_from(canonical.len())
            .map_err(|_| StorageError::ArithmeticOverflow("canonical byte count"))?;
        let canonical_digest = accepted_partition_canonical_digest(
            self.draft.accepted.id(),
            sequence,
            envelope_rows,
            canonical_stored,
            std::slice::from_ref(&canonical),
        );
        drop(canonical);

        let path = self
            .staging_dir
            .join(format!("accepted-{sequence:010}.parquet"));
        let (rows, file_bytes, file_digest) = write_envelope_parquet(&path, envelope)?;
        let stored_byte_count = self
            .accepted_stored_byte_count
            .checked_add(file_bytes)
            .ok_or(StorageError::ArithmeticOverflow(
                "snapshot stored byte count",
            ))?;
        if stored_byte_count > limits.max_stored_bytes() {
            let _ = fs::remove_file(&path);
            return Err(StorageError::StoredByteLimitExceeded {
                actual: stored_byte_count,
                maximum: limits.max_stored_bytes(),
            });
        }
        self.accepted_partitions.push(SnapshotPartition::try_new(
            sequence,
            rows,
            file_bytes,
            file_digest,
        )?);
        self.accepted_canonical_partitions
            .push(AcceptedCanonicalPartition {
                sequence,
                row_count: envelope_rows,
                stored_byte_count: canonical_stored,
                digest: canonical_digest,
            });
        self.accepted_row_count = row_count;
        self.accepted_stored_byte_count = stored_byte_count;
        Ok(())
    }

    /// Appends one report or rejected-row pack envelope. Each non-empty
    /// envelope becomes one artifact partition whose stored byte count is the
    /// canonical logical payload byte count (contract 8.1.1).
    pub fn append_validation_rule_summary(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(ArtifactSectionId::ValidationRuleSummary, envelope)
    }

    pub fn append_validation_findings(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(ArtifactSectionId::ValidationFinding, envelope)
    }

    pub fn append_rejected_rows(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        self.append_section(ArtifactSectionId::RejectedRows, envelope)
    }

    pub fn append_dedup_rule_summary(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(ArtifactSectionId::DedupRuleSummary, envelope)
    }

    pub fn append_duplicate_findings(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(ArtifactSectionId::DuplicateFinding, envelope)
    }

    fn append_section(
        &mut self,
        section_id: ArtifactSectionId,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let Some(section_index) = self
            .sections
            .iter()
            .position(|section| section.section_id == section_id)
        else {
            // Only the rejected section can be absent from the plan, and it is
            // absent exactly when the run declared `None` (contract 10.5).
            return Err(StorageError::InvalidDraft(
                "rejected rows artifact is not authorized for this bundle",
            ));
        };
        {
            let section = &self.sections[section_index];
            if envelope.sequence() != section.next_sequence {
                return Err(StorageError::Sequence {
                    expected: section.next_sequence,
                    actual: envelope.sequence(),
                });
            }
            if envelope.source_asset_id() != self.draft.accepted.source_asset_id() {
                return Err(StorageError::LineageMismatch {
                    sequence: envelope.sequence(),
                });
            }
            if envelope.schema_fingerprint() != section.fingerprint
                || envelope.schema() != section.schema.as_ref()
            {
                return Err(StorageError::SchemaDrift {
                    sequence: envelope.sequence(),
                });
            }
        }
        {
            let section = &mut self.sections[section_index];
            let envelope_count = section
                .envelope_count
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("section envelope count"))?;
            if envelope_count > MAX_INPUT_ENVELOPES {
                return Err(StorageError::EnvelopeLimitExceeded {
                    actual: envelope_count,
                    maximum: MAX_INPUT_ENVELOPES,
                });
            }
            section.envelope_count = envelope_count;
            section.next_sequence += 1;
        }
        if envelope.row_count() == 0 {
            return Ok(());
        }

        // Validate finding payloads before any filesystem write, but apply
        // the tallies only AFTER the ceiling check so a refused pack leaves
        // the summary counts untouched (E4-S1-R1 evidence gap: no tally
        // pollution on a failed append).
        let severity_tallies = if section_id == ArtifactSectionId::ValidationFinding {
            Some(count_severities(envelope)?)
        } else {
            None
        };

        let canonical = canonical_batch_bytes(envelope.payload())?;
        let envelope_rows = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("envelope row count"))?;
        let stored = u64::try_from(canonical.len())
            .map_err(|_| StorageError::ArithmeticOverflow("canonical byte count"))?;
        self.ensure_section_limits(section_index, envelope_rows, stored)?;

        if let Some((warnings, errors)) = severity_tallies {
            let section = &mut self.sections[section_index];
            section.warning_count = section
                .warning_count
                .checked_add(warnings)
                .ok_or(StorageError::ArithmeticOverflow("warning count"))?;
            section.error_count = section
                .error_count
                .checked_add(errors)
                .ok_or(StorageError::ArithmeticOverflow("error count"))?;
        }

        let section = &mut self.sections[section_index];
        let sequence = u32::try_from(section.partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition sequence"))?;
        let digest = compute_partition_digest(
            section.artifact_id,
            section.section_id,
            sequence,
            envelope_rows,
            stored,
            std::slice::from_ref(&canonical),
        );
        let path = self.staging_dir.join(format!(
            "s{:02x}-{sequence:010}.parquet",
            section.section_id.tag()
        ));
        write_envelope_parquet(&path, envelope)?;
        section.partitions.push(ArtifactPartition::try_new(
            sequence,
            envelope_rows,
            stored,
            digest,
        )?);
        match section_id {
            ArtifactSectionId::ValidationFinding | ArtifactSectionId::DuplicateFinding => {
                section.finding_count = section
                    .finding_count
                    .checked_add(envelope_rows)
                    .ok_or(StorageError::ArithmeticOverflow("finding count"))?;
            }
            _ => {}
        }
        if section_id == ArtifactSectionId::DuplicateFinding {
            section.duplicate_count = section
                .duplicate_count
                .checked_add(envelope_rows)
                .ok_or(StorageError::ArithmeticOverflow("duplicate count"))?;
        }
        Ok(())
    }

    /// Applies the per-artifact ceilings after aggregating all of the
    /// artifact's sections and again across the two always-present reports
    /// (bundle-wide ceiling), before any visible write (contract 8.1.1).
    fn ensure_section_limits(
        &self,
        section_index: usize,
        extra_rows: u64,
        extra_bytes: u64,
    ) -> Result<(), StorageError> {
        const REPORT_IDS: [ArtifactSectionId; 2] = [
            ArtifactSectionId::ValidationRuleSummary,
            ArtifactSectionId::ValidationFinding,
        ];
        const DEDUP_IDS: [ArtifactSectionId; 2] = [
            ArtifactSectionId::DedupRuleSummary,
            ArtifactSectionId::DuplicateFinding,
        ];

        // Aggregate one artifact group, adding the prospective partition to
        // the group that owns the candidate section.
        let aggregate = |ids: &[ArtifactSectionId; 2]| -> (u64, u64, u32) {
            let mut rows = 0_u64;
            let mut bytes = 0_u64;
            let mut partitions = 0_u32;
            for section in &self.sections {
                if !ids.contains(&section.section_id) {
                    continue;
                }
                for partition in &section.partitions {
                    rows += partition.row_count();
                    bytes += partition.stored_byte_count();
                }
                partitions += u32::try_from(section.partitions.len()).unwrap_or(u32::MAX);
                if self.sections[section_index].section_id == section.section_id {
                    rows += extra_rows;
                    bytes += extra_bytes;
                    partitions += 1;
                }
            }
            (rows, bytes, partitions)
        };

        let candidate_kind = self.sections[section_index].kind;
        match candidate_kind {
            ArtifactKind::RejectedRows => {
                let limits = self.inner.limits;
                let rows = self.sections[section_index]
                    .partitions
                    .iter()
                    .map(ArtifactPartition::row_count)
                    .sum::<u64>()
                    .checked_add(extra_rows)
                    .ok_or(StorageError::ArithmeticOverflow("rejected row count"))?;
                let bytes = self.sections[section_index]
                    .partitions
                    .iter()
                    .map(ArtifactPartition::stored_byte_count)
                    .sum::<u64>()
                    .checked_add(extra_bytes)
                    .ok_or(StorageError::ArithmeticOverflow("rejected byte count"))?;
                let partitions = u32::try_from(self.sections[section_index].partitions.len() + 1)
                    .map_err(|_| {
                    StorageError::ArithmeticOverflow("rejected partition count")
                })?;
                if rows > limits.max_rows() {
                    return Err(StorageError::RowLimitExceeded {
                        actual: rows,
                        maximum: limits.max_rows(),
                    });
                }
                if bytes > limits.max_stored_bytes() {
                    return Err(StorageError::StoredByteLimitExceeded {
                        actual: bytes,
                        maximum: limits.max_stored_bytes(),
                    });
                }
                if partitions > limits.max_partitions() {
                    return Err(StorageError::PartitionLimitExceeded {
                        actual: partitions,
                        maximum: limits.max_partitions(),
                    });
                }
            }
            ArtifactKind::ValidationReport | ArtifactKind::DeduplicationReport => {
                let own_ids = if candidate_kind == ArtifactKind::ValidationReport {
                    &REPORT_IDS
                } else {
                    &DEDUP_IDS
                };
                let (rows, bytes, partitions) = aggregate(own_ids);
                if rows > MAX_REPORT_ROWS {
                    return Err(StorageError::ArtifactRowLimitExceeded {
                        actual: rows,
                        maximum: MAX_REPORT_ROWS,
                    });
                }
                if bytes > MAX_REPORT_BYTES {
                    return Err(StorageError::ArtifactByteLimitExceeded {
                        actual: bytes,
                        maximum: MAX_REPORT_BYTES,
                    });
                }
                if partitions > MAX_REPORT_PARTITIONS {
                    return Err(StorageError::ArtifactPartitionLimitExceeded {
                        actual: partitions,
                        maximum: MAX_REPORT_PARTITIONS,
                    });
                }

                // Bundle-wide ceiling across both always-present reports.
                let (validation_rows, validation_bytes, validation_partitions) =
                    aggregate(&REPORT_IDS);
                let (dedup_rows, dedup_bytes, dedup_partitions) = aggregate(&DEDUP_IDS);
                let bundle_rows = checked_sum(validation_rows, dedup_rows, "bundle report rows")?;
                let bundle_bytes =
                    checked_sum(validation_bytes, dedup_bytes, "bundle report bytes")?;
                let bundle_partitions = validation_partitions.saturating_add(dedup_partitions);
                if bundle_rows > MAX_BUNDLE_REPORT_ROWS {
                    return Err(StorageError::ArtifactRowLimitExceeded {
                        actual: bundle_rows,
                        maximum: MAX_BUNDLE_REPORT_ROWS,
                    });
                }
                if bundle_bytes > MAX_BUNDLE_REPORT_BYTES {
                    return Err(StorageError::ArtifactByteLimitExceeded {
                        actual: bundle_bytes,
                        maximum: MAX_BUNDLE_REPORT_BYTES,
                    });
                }
                if bundle_partitions > MAX_BUNDLE_REPORT_PARTITIONS {
                    return Err(StorageError::ArtifactPartitionLimitExceeded {
                        actual: bundle_partitions,
                        maximum: MAX_BUNDLE_REPORT_PARTITIONS,
                    });
                }
            }
            ArtifactKind::VerificationBundle | ArtifactKind::AcceptedSnapshot => {
                return Err(StorageError::InvalidManifest(
                    "sections cannot belong to bundle-level artifacts",
                ));
            }
        }
        Ok(())
    }

    /// Makes every present artifact visible in one SQLite transaction
    /// (contract 10.1 step 8). The commit is the only visibility point.
    pub fn commit(
        mut self,
        committed_at: DateTime<Utc>,
    ) -> Result<VerificationBundle, StorageError> {
        let result = self.commit_inner(committed_at);
        if result.is_ok() {
            self.state = BundleState::Committed;
            let _ = fs::remove_dir_all(&self.staging_dir);
        } else {
            self.state = BundleState::Failed;
            self.abort();
        }
        result
    }

    fn abort(&mut self) {
        for directory in &self.installed_dirs {
            let _ = fs::remove_dir_all(directory);
        }
        self.installed_dirs.clear();
        let _ = fs::remove_dir_all(&self.staging_dir);
        abort_bundle_publication(&self.inner, self.draft.provenance.input.bundle_id);
    }

    fn commit_inner(
        &mut self,
        committed_at: DateTime<Utc>,
    ) -> Result<VerificationBundle, StorageError> {
        if self.state != BundleState::Staged {
            return Err(StorageError::InvalidDraft(
                "bundle writer cannot commit in its current state",
            ));
        }
        if committed_at < self.started_at {
            return Err(StorageError::InvalidTimestampOrder(
                "publication start and commit",
            ));
        }

        // ---- Assemble the accepted snapshot ----
        let partition_count = u32::try_from(self.accepted_partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition count"))?;
        let stats = SnapshotStats::try_new(
            self.accepted_row_count,
            self.accepted_stored_byte_count,
            partition_count,
        )?;
        let snapshot = build_snapshot(&self.draft.accepted, stats)?;
        let accepted_manifest =
            SnapshotManifest::try_new(snapshot, self.accepted_partitions.clone())?;
        // Logical digest: canonical Arrow IPC facts recorded on append, never
        // Parquet file hashes (contract 8.1.1).
        let accepted_digest = accepted_snapshot_manifest_digest(
            accepted_manifest.snapshot(),
            &self.accepted_canonical_partitions,
        )?;

        // ---- Assemble report and rejected manifests ----
        let mut validation_sections: Vec<ArtifactSection> = Vec::new();
        let mut dedup_sections: Vec<ArtifactSection> = Vec::new();
        let mut rejected_section: Option<ArtifactSection> = None;
        for section in &self.sections {
            let section_stats = ArtifactSectionStats::try_from_partitions(&section.partitions)?;
            let section_digest = compute_section_digest(
                section.artifact_id,
                section.section_id,
                &section.schema,
                section.fingerprint.as_bytes(),
                &section_stats,
                &section.partitions,
            )?;
            let built = ArtifactSection {
                section_id: section.section_id,
                schema: (*section.schema).clone(),
                schema_fingerprint: section.fingerprint,
                stats: section_stats,
                partitions: section.partitions.clone(),
                section_digest,
            };
            match section.kind {
                ArtifactKind::ValidationReport => validation_sections.push(built),
                ArtifactKind::DeduplicationReport => dedup_sections.push(built),
                ArtifactKind::RejectedRows => {
                    // Zero terminal rejections publish no rejected artifact at
                    // all (contract 10.2); an authorized-but-unused id stays
                    // unused without error.
                    if !built.partitions.is_empty() {
                        rejected_section = Some(built);
                    }
                }
                ArtifactKind::VerificationBundle | ArtifactKind::AcceptedSnapshot => {
                    return Err(StorageError::InvalidManifest(
                        "sections cannot belong to bundle-level artifacts",
                    ));
                }
            }
        }

        let input = &self.draft.provenance.input;
        let validation_manifest = ArtifactManifest::try_new(
            self.draft.validation_report_artifact_id,
            ArtifactKind::ValidationReport,
            validation_sections,
        )?;
        let dedup_manifest = ArtifactManifest::try_new(
            self.draft.deduplication_report_artifact_id,
            ArtifactKind::DeduplicationReport,
            dedup_sections,
        )?;
        let rejected_manifest = match (rejected_section, self.draft.rejected_rows_artifact_id) {
            (Some(section), Some(rejected_id)) => Some(ArtifactManifest::try_new(
                rejected_id,
                ArtifactKind::RejectedRows,
                vec![section],
            )?),
            _ => None,
        };

        // ---- Provenance: summaries then content digests ----
        // Structural totals come from the manifest; finding-class tallies are
        // accumulated on append and overlaid here so committed provenance
        // carries real counts instead of zeros.
        let validation_summary = overlay_tallies(
            summarize(&validation_manifest),
            &self.sections,
            self.draft.validation_report_artifact_id,
            ArtifactSectionId::ValidationFinding,
        );
        let dedup_summary = overlay_tallies(
            summarize(&dedup_manifest),
            &self.sections,
            self.draft.deduplication_report_artifact_id,
            ArtifactSectionId::DuplicateFinding,
        );
        let rejected_summary = rejected_manifest
            .as_ref()
            .map(summarize)
            .unwrap_or_default();

        let validation_provenance = artifact_provenance(
            &self.draft.provenance,
            self.draft.validation_report_artifact_id,
            ArtifactKind::ValidationReport,
            validation_summary,
            &validation_manifest,
        )?;
        let dedup_provenance = artifact_provenance(
            &self.draft.provenance,
            self.draft.deduplication_report_artifact_id,
            ArtifactKind::DeduplicationReport,
            dedup_summary,
            &dedup_manifest,
        )?;
        let rejected_provenance = rejected_manifest
            .as_ref()
            .map(|manifest| {
                artifact_provenance(
                    &self.draft.provenance,
                    manifest.artifact_id(),
                    ArtifactKind::RejectedRows,
                    rejected_summary,
                    manifest,
                )
            })
            .transpose()?;

        let accepted_provenance =
            accepted_provenance_from(&self.draft.provenance, &accepted_manifest, accepted_digest);

        let mut bundle_summary = sum_summaries(&[
            accepted_provenance.summary,
            validation_provenance.summary,
            dedup_provenance.summary,
        ])?;
        if let Some(provenance) = &rejected_provenance {
            bundle_summary = sum_summaries(&[bundle_summary, provenance.summary])?;
        }

        let mut children: Vec<(Uuid, [u8; 32], [u8; 32])> = Vec::new();
        children.push((self.draft.accepted.id(), accepted_digest, accepted_digest));
        children.push((
            validation_manifest.artifact_id(),
            *validation_manifest.manifest_digest().as_bytes(),
            validation_provenance.content_digest,
        ));
        if let Some(manifest) = &rejected_manifest {
            let provenance = rejected_provenance
                .as_ref()
                .ok_or(StorageError::InvalidManifest(
                    "rejected provenance is missing",
                ))?;
            children.push((
                manifest.artifact_id(),
                *manifest.manifest_digest().as_bytes(),
                provenance.content_digest,
            ));
        }
        children.push((
            dedup_manifest.artifact_id(),
            *dedup_manifest.manifest_digest().as_bytes(),
            dedup_provenance.content_digest,
        ));
        let bundle_content_digest = compute_bundle_provenance_digest(
            input.run_id,
            input.bundle_id,
            input.artifact_id,
            self.draft.accepted.id(),
            self.draft.validation_report_artifact_id,
            rejected_manifest
                .as_ref()
                .map(ArtifactManifest::artifact_id),
            self.draft.deduplication_report_artifact_id,
            &children,
        );
        let bundle_provenance = ArtifactProvenance {
            draft: self.draft.provenance.clone(),
            summary: bundle_summary,
            content_digest: bundle_content_digest,
        };

        let membership = VerificationBundleMembership {
            bundle_id: input.bundle_id,
            run_id: input.run_id,
            bundle_artifact_id: input.artifact_id,
            accepted_snapshot_id: self.draft.accepted.id(),
            validation_report_artifact_id: self.draft.validation_report_artifact_id,
            rejected_rows_artifact_id: rejected_manifest
                .as_ref()
                .map(ArtifactManifest::artifact_id),
            deduplication_report_artifact_id: self.draft.deduplication_report_artifact_id,
        };

        // ---- Installing: final directories precede SQLite visibility ----
        self.state = BundleState::Installing;
        let mut final_dirs: Vec<Uuid> = vec![
            self.draft.accepted.id(),
            validation_manifest.artifact_id(),
            dedup_manifest.artifact_id(),
        ];
        if let Some(manifest) = &rejected_manifest {
            final_dirs.push(manifest.artifact_id());
        }
        for id in final_dirs {
            let directory = crate::partitions_root(&self.inner).join(id.to_string());
            create_exact_directory(&directory, "create final artifact directory")?;
            self.installed_dirs.push(directory);
        }
        self.install_files(
            &accepted_manifest,
            &validation_manifest,
            &dedup_manifest,
            &rejected_manifest,
        )?;

        // ---- Committing: one SQLite transaction is the visibility point ----
        let membership_json = serde_json::to_string(&MembershipDocument {
            version: MEMBERSHIP_DOCUMENT_VERSION,
            membership: membership.clone(),
        })
        .map_err(|_| StorageError::Serialization("encode bundle membership"))?;
        let bundle_provenance_json = serde_json::to_string(&ProvenanceDocument {
            version: PROVENANCE_DOCUMENT_VERSION,
            provenance: bundle_provenance.clone(),
        })
        .map_err(|_| StorageError::Serialization("encode bundle provenance"))?;
        let mut artifact_rows: Vec<(Uuid, ArtifactKind, String, String)> = Vec::new();
        for (manifest, provenance) in [
            (&validation_manifest, &validation_provenance),
            (&dedup_manifest, &dedup_provenance),
        ]
        .into_iter()
        .chain(rejected_manifest.iter().zip(rejected_provenance.iter()))
        {
            let manifest_json = serde_json::to_string(manifest)
                .map_err(|_| StorageError::Serialization("encode artifact manifest"))?;
            let provenance_json = serde_json::to_string(&ProvenanceDocument {
                version: PROVENANCE_DOCUMENT_VERSION,
                provenance: provenance.clone(),
            })
            .map_err(|_| StorageError::Serialization("encode artifact provenance"))?;
            artifact_rows.push((
                manifest.artifact_id(),
                manifest.kind(),
                manifest_json,
                provenance_json,
            ));
        }

        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin bundle commit transaction"))?;
        crate::insert_visible_snapshot(&transaction, &accepted_manifest, false)?;
        transaction
            .execute(
                "INSERT INTO verification_bundles(
                     bundle_id, version, run_id, bundle_artifact_id,
                     accepted_snapshot_id, validation_report_artifact_id,
                     rejected_rows_artifact_id, deduplication_report_artifact_id,
                     membership_json, provenance_json, committed_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    membership.bundle_id.to_string(),
                    i64::from(MEMBERSHIP_DOCUMENT_VERSION),
                    membership.run_id.to_string(),
                    membership.bundle_artifact_id.to_string(),
                    membership.accepted_snapshot_id.to_string(),
                    membership.validation_report_artifact_id.to_string(),
                    membership
                        .rejected_rows_artifact_id
                        .map(|id| id.to_string()),
                    membership.deduplication_report_artifact_id.to_string(),
                    membership_json,
                    bundle_provenance_json,
                    format_timestamp(&committed_at),
                ],
            )
            .map_err(|_| StorageError::database("insert bundle membership"))?;
        for (artifact_id, kind, manifest_json, provenance_json) in &artifact_rows {
            transaction
                .execute(
                    "INSERT INTO artifact_manifests(
                         artifact_id, bundle_id, kind, manifest_json, provenance_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        artifact_id.to_string(),
                        membership.bundle_id.to_string(),
                        match kind {
                            ArtifactKind::VerificationBundle => "verification-bundle",
                            ArtifactKind::AcceptedSnapshot => "accepted-snapshot",
                            ArtifactKind::ValidationReport => "validation-report",
                            ArtifactKind::RejectedRows => "rejected-rows",
                            ArtifactKind::DeduplicationReport => "deduplication-report",
                        },
                        manifest_json,
                        provenance_json,
                    ],
                )
                .map_err(|_| StorageError::database("insert artifact manifest"))?;
        }
        let deleted = transaction
            .execute(
                "DELETE FROM bundle_publications WHERE bundle_id = ?1",
                params![membership.bundle_id.to_string()],
            )
            .map_err(|_| StorageError::database("complete bundle publication journal"))?;
        if deleted != 1 {
            return Err(StorageError::InvalidManifest(
                "bundle publication journal completion count is invalid",
            ));
        }
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit visible verification bundle"))?;

        Ok(VerificationBundle {
            membership,
            provenance: bundle_provenance,
            accepted: AcceptedSnapshotArtifact {
                manifest: accepted_manifest,
                provenance: accepted_provenance,
            },
            validation_report: ValidationReportArtifact {
                manifest: validation_manifest,
                provenance: validation_provenance,
            },
            rejected_rows: rejected_manifest.zip(rejected_provenance).map(
                |(manifest, provenance)| RejectedRowsArtifact {
                    manifest,
                    provenance,
                },
            ),
            deduplication_report: DeduplicationReportArtifact {
                manifest: dedup_manifest,
                provenance: dedup_provenance,
            },
        })
    }

    fn install_files(
        &self,
        accepted: &SnapshotManifest,
        validation: &ArtifactManifest,
        dedup: &ArtifactManifest,
        rejected: &Option<ArtifactManifest>,
    ) -> Result<(), StorageError> {
        let accepted_dir =
            crate::partitions_root(&self.inner).join(accepted.snapshot().id().to_string());
        for partition in accepted.partitions() {
            let staged = self
                .staging_dir
                .join(format!("accepted-{:010}.parquet", partition.sequence()));
            let final_path = accepted_dir.join(format!(
                "{:010}-{}.parquet",
                partition.sequence(),
                partition.digest()
            ));
            fs::rename(staged, final_path)
                .map_err(|error| StorageError::io("install accepted partition", &error))?;
        }
        for manifest in [validation, dedup] {
            install_manifest_partitions(
                &self.staging_dir,
                &crate::partitions_root(&self.inner),
                manifest,
            )?;
        }
        if let Some(manifest) = rejected {
            install_manifest_partitions(
                &self.staging_dir,
                &crate::partitions_root(&self.inner),
                manifest,
            )?;
        }
        sync_directory(&crate::partitions_root(&self.inner))
    }
}

fn install_manifest_partitions(
    staging_dir: &Path,
    partitions_root: &Path,
    manifest: &ArtifactManifest,
) -> Result<(), StorageError> {
    let artifact_dir = partitions_root.join(manifest.artifact_id().to_string());
    for section in manifest.sections() {
        for partition in section.partitions() {
            let staged = staging_dir.join(format!(
                "s{:02x}-{:010}.parquet",
                section.section_id().tag(),
                partition.sequence()
            ));
            let final_path = artifact_dir.join(format!(
                "{:010}-{}.parquet",
                partition.sequence(),
                partition.digest()
            ));
            fs::rename(staged, final_path)
                .map_err(|error| StorageError::io("install artifact partition", &error))?;
        }
    }
    Ok(())
}

fn count_severities(envelope: &BatchEnvelope) -> Result<(u64, u64), StorageError> {
    let payload = envelope.payload();
    let (index, _) =
        payload
            .schema()
            .column_with_name("severity")
            .ok_or(StorageError::InvalidManifest(
                "validation finding batch lacks the severity column",
            ))?;
    let values = payload
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(StorageError::InvalidManifest(
            "severity column must be UTF-8",
        ))?;
    let mut warnings = 0_u64;
    let mut errors = 0_u64;
    for value in values.iter().flatten() {
        match value {
            "warning" => {
                warnings = warnings
                    .checked_add(1)
                    .ok_or(StorageError::ArithmeticOverflow("warning count"))?
            }
            "error" => {
                errors = errors
                    .checked_add(1)
                    .ok_or(StorageError::ArithmeticOverflow("error count"))?
            }
            _ => {
                return Err(StorageError::InvalidManifest(
                    "validation finding severity must be warning or error",
                ));
            }
        }
    }
    Ok((warnings, errors))
}

fn artifact_provenance(
    bundle_draft: &ArtifactProvenanceDraft,
    artifact_id: Uuid,
    kind: ArtifactKind,
    summary: ArtifactSummary,
    manifest: &ArtifactManifest,
) -> Result<ArtifactProvenance, StorageError> {
    let draft = child_draft(bundle_draft, artifact_id, kind);
    let content_digest = compute_artifact_provenance_digest(
        draft.input.run_id,
        draft.input.bundle_id,
        artifact_id,
        kind,
        &draft.canonical_plan_digest,
        &draft.input.input.version_digest,
        manifest.sections(),
        &manifest.manifest_digest(),
    )?;
    Ok(ArtifactProvenance {
        draft,
        summary,
        content_digest,
    })
}

fn checked_sum(left: u64, right: u64, label: &'static str) -> Result<u64, StorageError> {
    left.checked_add(right)
        .ok_or(StorageError::ArithmeticOverflow(label))
}

/// Overlays the append-time finding tallies of one staged section onto a
/// manifest-derived summary (contract 8.1.1 summary semantics).
fn overlay_tallies(
    mut summary: ArtifactSummary,
    sections: &[SectionStaging],
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
) -> ArtifactSummary {
    if let Some(section) = sections
        .iter()
        .find(|section| section.artifact_id == artifact_id && section.section_id == section_id)
    {
        summary.finding_count = summary.finding_count.saturating_add(section.finding_count);
        summary.warning_count = summary.warning_count.saturating_add(section.warning_count);
        summary.error_count = summary.error_count.saturating_add(section.error_count);
        summary.duplicate_count = summary
            .duplicate_count
            .saturating_add(section.duplicate_count);
    }
    summary
}

fn summarize(manifest: &ArtifactManifest) -> ArtifactSummary {
    let mut summary = ArtifactSummary::default();
    for section in manifest.sections() {
        summary.row_count += section.stats().row_count();
        summary.stored_byte_count += section.stats().stored_byte_count();
        summary.partition_count += section.stats().partition_count();
    }
    summary
}

fn sum_summaries(parts: &[ArtifactSummary]) -> Result<ArtifactSummary, StorageError> {
    let mut total = ArtifactSummary::default();
    for part in parts {
        total.row_count = total
            .row_count
            .checked_add(part.row_count)
            .ok_or(StorageError::ArithmeticOverflow("summary row count"))?;
        total.stored_byte_count = total
            .stored_byte_count
            .checked_add(part.stored_byte_count)
            .ok_or(StorageError::ArithmeticOverflow("summary byte count"))?;
        total.partition_count = total.partition_count.saturating_add(part.partition_count);
        total.finding_count = total
            .finding_count
            .checked_add(part.finding_count)
            .ok_or(StorageError::ArithmeticOverflow("summary finding count"))?;
        total.warning_count = total
            .warning_count
            .checked_add(part.warning_count)
            .ok_or(StorageError::ArithmeticOverflow("summary warning count"))?;
        total.error_count = total
            .error_count
            .checked_add(part.error_count)
            .ok_or(StorageError::ArithmeticOverflow("summary error count"))?;
        total.duplicate_count = total
            .duplicate_count
            .checked_add(part.duplicate_count)
            .ok_or(StorageError::ArithmeticOverflow("summary duplicate count"))?;
    }
    Ok(total)
}

impl Drop for VerificationBundleWriter {
    /// Dropping an uncommitted writer aborts the whole bundle staging
    /// context (contract 10.3).
    fn drop(&mut self) {
        if self.state == BundleState::Committed {
            return;
        }
        self.abort();
    }
}

/// Bounded iterator over one artifact section's partitions. It can never
/// bypass bundle membership because it is constructed only through
/// `SnapshotStore::open_artifact_section` (contract 8.1).
pub struct ArtifactBatchReader {
    inner: Arc<StoreInner>,
    _activity: ActivityGuard,
    artifact_id: Uuid,
    section: ArtifactSection,
    source_asset_id: Uuid,
    next_partition: usize,
}

impl ArtifactBatchReader {
    pub fn section(&self) -> &ArtifactSection {
        &self.section
    }
}

impl Iterator for ArtifactBatchReader {
    type Item = Result<BatchEnvelope, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.section.partitions().get(self.next_partition)?.clone();
        self.next_partition += 1;
        Some(read_artifact_partition(
            &self.inner,
            self.artifact_id,
            &self.section,
            &partition,
            self.source_asset_id,
        ))
    }
}

fn read_artifact_partition(
    inner: &StoreInner,
    artifact_id: Uuid,
    section: &ArtifactSection,
    partition: &ArtifactPartition,
    source_asset_id: Uuid,
) -> Result<BatchEnvelope, StorageError> {
    let directory = crate::partitions_root(inner).join(artifact_id.to_string());
    let directory_metadata = fs::symlink_metadata(&directory)
        .map_err(|error| StorageError::io("inspect artifact partition directory", &error))?;
    if directory_metadata.file_type().is_symlink() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::Symlink,
        ));
    }
    if !directory_metadata.is_dir() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::NotRegularFile,
        ));
    }
    let path = directory.join(format!(
        "{:010}-{}.parquet",
        partition.sequence(),
        partition.digest()
    ));
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| StorageError::io("inspect artifact partition file", &error))?;
    if metadata.file_type().is_symlink() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::Symlink,
        ));
    }
    if !metadata.is_file() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::NotRegularFile,
        ));
    }

    let file = std::fs::File::open(&path)
        .map_err(|error| StorageError::io("open artifact partition file", &error))?;
    let canonical_schema = logical_schema_to_arrow(section.schema()).map_err(|_| {
        integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::SchemaMismatch,
        )
    })?;
    let options = ArrowReaderOptions::new().with_schema(Arc::clone(&canonical_schema));
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).map_err(|_| {
            integrity_error(
                artifact_id,
                partition.sequence(),
                crate::IntegrityFailure::InvalidParquet,
            )
        })?;
    let mut reader = builder
        .with_batch_size(MAX_BATCH_ROWS)
        .build()
        .map_err(|_| {
            integrity_error(
                artifact_id,
                partition.sequence(),
                crate::IntegrityFailure::InvalidParquet,
            )
        })?;
    let batch = reader
        .next()
        .ok_or_else(|| {
            integrity_error(
                artifact_id,
                partition.sequence(),
                crate::IntegrityFailure::UnexpectedBatchCount,
            )
        })?
        .map_err(|_| {
            integrity_error(
                artifact_id,
                partition.sequence(),
                crate::IntegrityFailure::InvalidParquet,
            )
        })?;
    let rows = u64::try_from(batch.num_rows())
        .map_err(|_| StorageError::ArithmeticOverflow("decoded partition row count"))?;
    if rows != partition.row_count() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::RowCountMismatch,
        ));
    }
    if reader.next().is_some() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::UnexpectedBatchCount,
        ));
    }

    // Canonical identity gate: recompute the frozen partition preimage from
    // the decoded payload and require exact equality (contract 8.1.1).
    let canonical = canonical_batch_bytes(&batch)?;
    let recomputed = compute_partition_digest(
        artifact_id,
        section.section_id(),
        partition.sequence(),
        rows,
        u64::try_from(canonical.len()).unwrap_or(u64::MAX),
        std::slice::from_ref(&canonical),
    );
    if recomputed != partition.digest() {
        return Err(integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::DigestMismatch,
        ));
    }
    let batch = RecordBatch::try_new(canonical_schema, batch.columns().to_vec()).map_err(|_| {
        integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::SchemaMismatch,
        )
    })?;
    BatchEnvelope::try_new(
        Arc::new(section.schema().clone()),
        source_asset_id,
        u64::from(partition.sequence()),
        batch,
    )
    .map_err(|_| {
        integrity_error(
            artifact_id,
            partition.sequence(),
            crate::IntegrityFailure::SchemaMismatch,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray, UInt32Array, UInt64Array};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::{StorageLimits, MAX_MAINTENANCE_CANDIDATES};
    use stillflow_core::ArtifactProvenanceInput;

    const RUN: u128 = 0xE400;
    const BUNDLE: u128 = 0xE401;
    const BUNDLE_ARTIFACT: u128 = 0xE402;
    const ACCEPTED: u128 = 0xE403;
    const VALIDATION: u128 = 0xE404;
    const REJECTED: u128 = 0xE405;
    const DEDUP: u128 = 0xE406;
    const SOURCE_ASSET: u128 = 0xE407;

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("valid timestamp")
    }

    const CREATED: i64 = 1_700_000_000;
    const STARTED: i64 = 1_700_000_100;
    const COMMITTED: i64 = 1_700_000_200;

    fn source_schema() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![stillflow_core::LogicalField::new(
                stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x11)),
                "value",
                stillflow_core::LogicalType::Int64,
                false,
            )
            .expect("field")])
            .expect("schema"),
        )
    }

    fn open_store(temp: &TempDir) -> SnapshotStore {
        SnapshotStore::open(temp.path(), StorageLimits::default()).expect("store")
    }

    fn provenance_input() -> ArtifactProvenanceInput {
        ArtifactProvenanceInput {
            run_id: Uuid::from_u128(RUN),
            bundle_id: Uuid::from_u128(BUNDLE),
            artifact_id: Uuid::from_u128(BUNDLE_ARTIFACT),
            artifact_kind: ArtifactKind::VerificationBundle,
            session_id: Uuid::from_u128(0xE408),
            input: stillflow_core::LogicalInputRef {
                input: stillflow_core::InputRef::Asset {
                    asset_id: Uuid::from_u128(SOURCE_ASSET),
                },
                version_digest: [0x5A; 32],
            },
            lineage: std::collections::BTreeSet::from([Uuid::from_u128(0xE409)]),
            created_at: at(CREATED),
            started_at: at(STARTED),
            committed_at: at(COMMITTED),
        }
    }

    fn bundle_draft(rejected_authorized: bool) -> VerificationBundleDraft {
        draft_with_ids(
            RUN,
            BUNDLE,
            BUNDLE_ARTIFACT,
            ACCEPTED,
            VALIDATION,
            rejected_authorized.then_some(REJECTED),
            DEDUP,
        )
    }

    /// Builds a fully explicit draft so reservation tests can compose
    /// arbitrary (possibly conflicting) identity sets.
    fn draft_with_ids(
        run: u128,
        bundle: u128,
        bundle_artifact: u128,
        accepted: u128,
        validation: u128,
        rejected: Option<u128>,
        dedup: u128,
    ) -> VerificationBundleDraft {
        draft_with_accepted_schema(
            source_schema().as_ref().clone(),
            run,
            bundle,
            bundle_artifact,
            accepted,
            validation,
            rejected,
            dedup,
        )
    }

    /// Builds a fully explicit draft whose accepted (materialized) schema may
    /// diverge from the logical Scan output (issue #176, D2 coverage).
    #[allow(clippy::too_many_arguments)]
    fn draft_with_accepted_schema(
        accepted_schema: LogicalSchema,
        run: u128,
        bundle: u128,
        bundle_artifact: u128,
        accepted: u128,
        validation: u128,
        rejected: Option<u128>,
        dedup: u128,
    ) -> VerificationBundleDraft {
        let provenance = ArtifactProvenanceDraft {
            input: ArtifactProvenanceInput {
                run_id: Uuid::from_u128(run),
                bundle_id: Uuid::from_u128(bundle),
                artifact_id: Uuid::from_u128(bundle_artifact),
                artifact_kind: ArtifactKind::VerificationBundle,
                session_id: Uuid::from_u128(0xE408),
                input: stillflow_core::LogicalInputRef {
                    input: stillflow_core::InputRef::Asset {
                        asset_id: Uuid::from_u128(SOURCE_ASSET),
                    },
                    version_digest: [0x5A; 32],
                },
                lineage: std::collections::BTreeSet::from([Uuid::from_u128(0xE409)]),
                created_at: at(CREATED),
                started_at: at(STARTED),
                committed_at: at(COMMITTED),
            },
            plan_fingerprint: [0x21; 32],
            canonical_plan_digest: [0x22; 32],
            engine_contract_version: 1,
            engine_build: "test-engine-build".to_owned(),
            verification_contract_version: VERIFICATION_CONTRACT_VERSION,
        };
        let accepted_draft = SnapshotDraft::try_new(
            Uuid::from_u128(accepted),
            Uuid::from_u128(0xE410),
            Uuid::from_u128(0xE411),
            Uuid::from_u128(SOURCE_ASSET),
            accepted_schema,
            std::collections::BTreeSet::from([Uuid::from_u128(0xE412)]),
            Some(95),
            at(CREATED),
        )
        .expect("accepted draft");
        VerificationBundleDraft::try_new(
            provenance,
            accepted_draft,
            Uuid::from_u128(validation),
            rejected.map(Uuid::from_u128),
            Uuid::from_u128(dedup),
        )
        .expect("bundle draft")
    }

    fn fill_batch(
        schema: &LogicalSchema,
        rows: usize,
        overrides: BTreeMap<&'static str, ArrayRef>,
    ) -> RecordBatch {
        let arrow = logical_schema_to_arrow(schema).expect("arrow schema");
        let mut columns: Vec<ArrayRef> = Vec::new();
        for field in &schema.fields {
            if let Some(value) = overrides.get(field.name.as_str()) {
                columns.push(Arc::clone(value));
                continue;
            }
            let column: ArrayRef = match &field.data_type {
                stillflow_core::LogicalType::Utf8 => {
                    let values: Vec<String> = (0..rows)
                        .map(|row| format!("{}-{row}", field.name))
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                stillflow_core::LogicalType::UInt64 => Arc::new(UInt64Array::from(
                    (0..rows).map(|row| row as u64).collect::<Vec<_>>(),
                )),
                stillflow_core::LogicalType::UInt32 => Arc::new(UInt32Array::from(
                    (0..rows).map(|row| row as u32).collect::<Vec<_>>(),
                )),
                stillflow_core::LogicalType::Int64 => Arc::new(Int64Array::from(
                    (0..rows).map(|row| row as i64).collect::<Vec<_>>(),
                )),
                other => panic!("fixture type unsupported: {other:?}"),
            };
            columns.push(column);
        }
        RecordBatch::try_new(arrow, columns).expect("batch")
    }

    fn section_envelope(
        schema: Arc<LogicalSchema>,
        sequence: u64,
        rows: usize,
        overrides: BTreeMap<&'static str, ArrayRef>,
    ) -> BatchEnvelope {
        let batch = fill_batch(&schema, rows, overrides);
        BatchEnvelope::try_new(schema, Uuid::from_u128(SOURCE_ASSET), sequence, batch)
            .expect("envelope")
    }

    fn accepted_envelope(sequence: u64, values: Vec<i64>) -> BatchEnvelope {
        let schema = source_schema();
        let arrow = logical_schema_to_arrow(&schema).expect("arrow schema");
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(values))]).expect("batch");
        BatchEnvelope::try_new(schema, Uuid::from_u128(SOURCE_ASSET), sequence, batch)
            .expect("envelope")
    }

    fn begin(store: &SnapshotStore, rejected: bool) -> VerificationBundleWriter {
        store
            .begin_verification_bundle(bundle_draft(rejected), at(STARTED))
            .expect("begin")
    }

    fn publish_standard_bundle(store: &SnapshotStore, rejected: bool) -> VerificationBundle {
        publish_standard_bundle_with(store, rejected, BTreeMap::new())
    }

    fn publish_standard_bundle_with(
        store: &SnapshotStore,
        rejected: bool,
        rejected_overrides: BTreeMap<&'static str, ArrayRef>,
    ) -> VerificationBundle {
        let mut writer = begin(store, rejected);
        writer
            .append_accepted(&accepted_envelope(0, vec![1, 2, 3]))
            .expect("accepted append");
        let summary_schema = Arc::new(artifact::validation_rule_summary_section_schema());
        writer
            .append_validation_rule_summary(&section_envelope(
                summary_schema,
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("summary append");
        let finding_schema = Arc::new(artifact::validation_finding_section_schema());
        let mut severity = BTreeMap::new();
        severity.insert(
            "severity",
            Arc::new(StringArray::from(vec!["warning", "error"])) as ArrayRef,
        );
        writer
            .append_validation_findings(&section_envelope(finding_schema, 0, 2, severity))
            .expect("findings append");
        if rejected {
            writer
                .append_rejected_rows(&section_envelope(
                    Arc::new(
                        artifact::rejected_rows_section_schema(&source_schema()).expect("rejected"),
                    ),
                    0,
                    1,
                    rejected_overrides,
                ))
                .expect("rejected append");
        }
        let dedup_summary_schema = Arc::new(artifact::dedup_rule_summary_section_schema());
        writer
            .append_dedup_rule_summary(&section_envelope(
                dedup_summary_schema,
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("dedup summary append");
        let duplicate_schema = Arc::new(artifact::duplicate_finding_section_schema());
        writer
            .append_duplicate_findings(&section_envelope(duplicate_schema, 0, 2, BTreeMap::new()))
            .expect("duplicate append");
        writer.commit(at(COMMITTED)).expect("commit")
    }

    #[test]
    fn bundle_round_trips_through_all_three_lookups_and_reopen() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let bundle = publish_standard_bundle(&store, true);

        assert_eq!(bundle.membership().bundle_id(), Uuid::from_u128(BUNDLE));
        assert_eq!(
            bundle.membership().rejected_rows_artifact_id(),
            Some(Uuid::from_u128(REJECTED))
        );
        let by_run = store
            .load_verification_bundle_by_run_id(Uuid::from_u128(RUN))
            .expect("by run");
        let by_snapshot = store
            .load_verification_bundle_by_snapshot(Uuid::from_u128(ACCEPTED))
            .expect("by snapshot");
        assert_eq!(by_run, bundle);
        assert_eq!(by_snapshot, bundle);

        // Reload fidelity across a full close/reopen of the store root.
        drop(store);
        let reopened = open_store(&temp);
        let reloaded = reopened
            .load_verification_bundle(Uuid::from_u128(BUNDLE))
            .expect("reloaded");
        assert_eq!(reloaded, bundle);
        // Accepted snapshot stays visible through the existing snapshot API.
        reopened
            .load_manifest(Uuid::from_u128(ACCEPTED))
            .expect("accepted manifest");
    }

    #[test]
    fn zero_rejection_publishes_no_rejected_artifact() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let bundle = publish_standard_bundle(&store, false);
        assert_eq!(bundle.membership().rejected_rows_artifact_id(), None);
        assert!(bundle.rejected_rows().is_none());
        // The authorized-but-unused id is never visible as a snapshot.
        assert!(store.load_manifest(Uuid::from_u128(REJECTED)).is_err());
        // Both report artifacts and the accepted snapshot are present.
        store
            .open_artifact_section(
                Uuid::from_u128(BUNDLE),
                Uuid::from_u128(VALIDATION),
                ArtifactSectionId::ValidationFinding,
            )
            .expect("validation section");
        store
            .open_artifact_section(
                Uuid::from_u128(BUNDLE),
                Uuid::from_u128(DEDUP),
                ArtifactSectionId::DuplicateFinding,
            )
            .expect("dedup section");
    }

    #[test]
    fn rejected_section_reader_roundtrips_and_detects_tampering() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let _bundle = publish_standard_bundle(&store, true);
        let mut reader = store
            .open_artifact_section(
                Uuid::from_u128(BUNDLE),
                Uuid::from_u128(REJECTED),
                ArtifactSectionId::RejectedRows,
            )
            .expect("open section");
        let envelope = reader.next().expect("partition").expect("envelope");
        assert_eq!(envelope.sequence(), 0);
        assert_eq!(envelope.row_count(), 1);
        assert!(reader.next().is_none(), "bounded by the manifest");

        // Tampering: replace the partition with a different, still-valid
        // parquet payload of the same shape; the canonical digest must catch
        // the substitution.
        let artifact_dir =
            crate::partitions_root(&store.inner).join(Uuid::from_u128(REJECTED).to_string());
        let mut paths = std::fs::read_dir(&artifact_dir)
            .expect("artifact dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        assert_eq!(paths.len(), 1);
        let path = paths.remove(0).path();
        let rejected_schema = Arc::new(
            artifact::rejected_rows_section_schema(&source_schema()).expect("rejected schema"),
        );
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "value",
            Arc::new(Int64Array::from(vec![999_i64])) as ArrayRef,
        );
        let substitute = section_envelope(rejected_schema, 0, 1, overrides);
        std::fs::remove_file(&path).expect("remove original partition");
        write_envelope_parquet(&path, &substitute).expect("write substitute partition");
        let mut reader = store
            .open_artifact_section(
                Uuid::from_u128(BUNDLE),
                Uuid::from_u128(REJECTED),
                ArtifactSectionId::RejectedRows,
            )
            .expect("reopen section");
        let outcome = reader.next().expect("partition result");
        assert!(
            matches!(
                outcome,
                Err(StorageError::Integrity {
                    kind: crate::IntegrityFailure::DigestMismatch,
                    ..
                })
            ),
            "tampering must surface as DigestMismatch, got {outcome:?}"
        );
    }

    #[test]
    fn uncommitted_writer_is_invisible_and_drop_aborts_everything() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        {
            let mut writer = begin(&store, true);
            writer
                .append_accepted(&accepted_envelope(0, vec![1]))
                .expect("append");
            assert!(
                store
                    .load_verification_bundle(Uuid::from_u128(BUNDLE))
                    .is_err(),
                "an uncommitted bundle must be invisible"
            );
        } // Drop aborts the staging context.
        assert!(store
            .load_verification_bundle(Uuid::from_u128(BUNDLE))
            .is_err());
        assert!(!staging_root(&store.inner)
            .join(Uuid::from_u128(BUNDLE).to_string())
            .exists());
        let connection = open_connection(&store.inner).expect("connection");
        let journal: i64 = connection
            .query_row("SELECT COUNT(*) FROM bundle_publications", [], |row| {
                row.get(0)
            })
            .expect("journal count");
        assert_eq!(journal, 0, "drop removes the publication journal row");
    }

    #[test]
    fn staged_crash_window_is_recovered() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, true);
        writer
            .append_accepted(&accepted_envelope(0, vec![7, 8]))
            .expect("append");
        // Simulate a process crash after staging: the publisher permit dies
        // with the process while staged files and the journal row remain.
        drop(writer._activity.take());
        std::mem::forget(writer);
        assert!(staging_root(&store.inner)
            .join(Uuid::from_u128(BUNDLE).to_string())
            .exists());
        let report = store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("recover");
        assert!(report.recovered() >= 1);
        assert!(!staging_root(&store.inner)
            .join(Uuid::from_u128(BUNDLE).to_string())
            .exists());
        assert!(
            store
                .load_verification_bundle(Uuid::from_u128(BUNDLE))
                .is_err(),
            "no partial bundle is ever visible"
        );
        let connection = open_connection(&store.inner).expect("connection");
        let journal: i64 = connection
            .query_row("SELECT COUNT(*) FROM bundle_publications", [], |row| {
                row.get(0)
            })
            .expect("journal count");
        assert_eq!(journal, 0);
    }

    #[test]
    fn prepared_journal_window_is_recovered() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);

        // Block staging creation so begin aborts right after the Prepared
        // window; the pre-created regular file makes create_exact_directory
        // fail deterministically.
        let blocking_file = staging_root(&store.inner).join(Uuid::from_u128(BUNDLE).to_string());
        std::fs::write(&blocking_file, b"not a directory").expect("block staging");

        // Order-sensitivity: at the moment staging creation is about to run,
        // the journal row must already be committed and no staging directory
        // may exist. A regression that creates staging before committing the
        // journal row fails these assertions inside production code.
        let observed = Arc::new(std::sync::Mutex::new(None::<(bool, bool)>));
        {
            let observed = Arc::clone(&observed);
            *PREPARED_WINDOW_HOOK.lock().expect("hook") =
                Some(Arc::new(move |inner: &StoreInner, bundle_id: Uuid| {
                    let connection = open_connection(inner).expect("connection");
                    let journal: i64 = connection
                        .query_row(
                            "SELECT COUNT(*) FROM bundle_publications WHERE bundle_id = ?1",
                            params![bundle_id.to_string()],
                            |row| row.get(0),
                        )
                        .expect("journal count");
                    let staging_exists_as_dir =
                        staging_root(inner).join(bundle_id.to_string()).is_dir();
                    *observed.lock().expect("observed") =
                        Some((journal == 1, !staging_exists_as_dir));
                }));
        }

        assert!(
            store
                .begin_verification_bundle(bundle_draft(true), at(STARTED))
                .is_err(),
            "blocked staging must abort the publication"
        );
        *PREPARED_WINDOW_HOOK.lock().expect("hook") = None;

        let observed = observed.lock().expect("observed").take();
        assert_eq!(
            observed,
            Some((true, true)),
            "journal-before-staging ordering must hold at the window boundary"
        );

        // The aborted attempt leaves no journal row behind.
        let connection = open_connection(&store.inner).expect("connection");
        let journal: i64 = connection
            .query_row("SELECT COUNT(*) FROM bundle_publications", [], |row| {
                row.get(0)
            })
            .expect("journal count");
        assert_eq!(journal, 0, "Prepared recovery aborts the publication");
    }

    #[test]
    fn installing_window_is_recovered_with_installed_directories() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, true);
        writer
            .append_accepted(&accepted_envelope(0, vec![5]))
            .expect("append");
        // Simulate the Installing state: final artifact directories exist but
        // SQLite visibility has not begun.
        for id in [ACCEPTED, VALIDATION, REJECTED, DEDUP] {
            create_exact_directory(
                &crate::partitions_root(&store.inner).join(Uuid::from_u128(id).to_string()),
                "create simulated installed directory",
            )
            .expect("installed dir");
        }
        drop(writer._activity.take());
        std::mem::forget(writer);
        store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("recover");
        for id in [ACCEPTED, VALIDATION, REJECTED, DEDUP] {
            assert!(
                !crate::partitions_root(&store.inner)
                    .join(Uuid::from_u128(id).to_string())
                    .exists(),
                "installed artifact directories must be removed on recovery"
            );
        }
        assert!(store
            .load_verification_bundle(Uuid::from_u128(BUNDLE))
            .is_err());
    }

    #[test]
    fn committing_failure_rolls_back_every_manifest_row_and_directory() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, true);
        writer
            .append_accepted(&accepted_envelope(0, vec![1, 2]))
            .expect("append");
        // Sabotage the transaction: another visible bundle already owns this
        // run id, so the membership insert violates its UNIQUE constraint.
        {
            let connection = open_connection(&store.inner).expect("connection");
            connection
                .execute(
                    "INSERT INTO verification_bundles(
                         bundle_id, version, run_id, bundle_artifact_id,
                         accepted_snapshot_id, validation_report_artifact_id,
                         rejected_rows_artifact_id, deduplication_report_artifact_id,
                         membership_json, provenance_json, committed_at_utc
                     ) VALUES (?1, 1, ?2, ?3, ?4, ?5, NULL, ?6, '{}', '{}', ?7)",
                    rusqlite::params![
                        Uuid::from_u128(0xE4F0).to_string(),
                        Uuid::from_u128(RUN).to_string(),
                        Uuid::from_u128(0xE4F1).to_string(),
                        Uuid::from_u128(0xE4F2).to_string(),
                        Uuid::from_u128(0xE4F3).to_string(),
                        Uuid::from_u128(0xE4F4).to_string(),
                        format_timestamp(&at(COMMITTED)),
                    ],
                )
                .expect("sabotage row");
        }
        assert!(writer.commit(at(COMMITTED)).is_err());
        // Rollback removed every installed directory and the staging context.
        for id in [ACCEPTED, VALIDATION, REJECTED, DEDUP] {
            assert!(
                !crate::partitions_root(&store.inner)
                    .join(Uuid::from_u128(id).to_string())
                    .exists(),
                "failed commit must remove installed directories"
            );
        }
        // Contract 10.3 loader assertions, using lookups that cannot hit the
        // sabotage row (bundle 0xE4F0): the attempted bundle is invisible and
        // the accepted snapshot never became visible.
        assert!(store
            .load_verification_bundle(Uuid::from_u128(BUNDLE))
            .is_err());
        assert!(
            store.load_manifest(Uuid::from_u128(ACCEPTED)).is_err(),
            "the accepted snapshot row must be rolled back"
        );
        let manifests: i64 = {
            let connection = open_connection(&store.inner).expect("connection");
            connection
                .query_row(
                    "SELECT COUNT(*) FROM artifact_manifests WHERE bundle_id = ?1",
                    params![Uuid::from_u128(BUNDLE).to_string()],
                    |row| row.get(0),
                )
                .expect("manifest count")
        };
        assert_eq!(manifests, 0, "no artifact manifest rows may survive");
    }

    #[test]
    fn section_limits_fail_before_any_visible_write() {
        // Direct unit coverage of the aggregated ceilings: the writer is not
        // required, only the limit arithmetic over staged partitions.
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, false);
        // Push a staged partition whose aggregate exceeds the report ceiling.
        let section = writer
            .sections
            .iter_mut()
            .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
            .expect("section");
        section.partitions.push(
            ArtifactPartition::try_new(
                0,
                MAX_REPORT_ROWS,
                1,
                crate::ContentDigest::from_bytes([0xAA; 32]),
            )
            .expect("staged partition"),
        );
        let index = writer
            .sections
            .iter()
            .position(|section| section.section_id == ArtifactSectionId::ValidationFinding)
            .expect("index");
        assert!(matches!(
            writer.ensure_section_limits(index, 1, 1),
            Err(StorageError::ArtifactRowLimitExceeded { .. })
        ));

        // Byte branch: one staged partition at the exact byte ceiling plus a
        // single extra byte must fail on bytes, not rows.
        {
            let section = writer
                .sections
                .iter_mut()
                .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
                .expect("section");
            section.partitions.clear();
            section.partitions.push(
                ArtifactPartition::try_new(
                    0,
                    1,
                    MAX_REPORT_BYTES,
                    crate::ContentDigest::from_bytes([0xAB; 32]),
                )
                .expect("staged partition"),
            );
        }
        let index = writer
            .sections
            .iter()
            .position(|section| section.section_id == ArtifactSectionId::ValidationFinding)
            .expect("index");
        assert!(matches!(
            writer.ensure_section_limits(index, 0, 1),
            Err(StorageError::ArtifactByteLimitExceeded { .. })
        ));

        // Partition branch: filling every report partition slot fails the
        // candidate partition even with zero rows and bytes.
        {
            let section = writer
                .sections
                .iter_mut()
                .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
                .expect("section");
            section.partitions.clear();
            for sequence in 0..MAX_REPORT_PARTITIONS {
                section.partitions.push(
                    ArtifactPartition::try_new(
                        sequence,
                        1,
                        1,
                        crate::ContentDigest::from_bytes([0xAC; 32]),
                    )
                    .expect("staged partition"),
                );
            }
        }
        assert!(matches!(
            writer.ensure_section_limits(index, 0, 0),
            Err(StorageError::ArtifactPartitionLimitExceeded { .. })
        ));

        // Bundle-wide ceiling ordering, three isomorphic shapes. Every
        // bundle ceiling is exactly twice its per-report ceiling, so staging
        // both reports at ceiling − 10 and driving the candidate +21 puts
        // the owning report exactly 11 over its own ceiling while the bundle
        // sum lands exactly one over the bundle ceiling: both gates armed at
        // once, and the public arithmetic must surface the per-report error
        // first. Each proof pins the exact `actual` and `maximum`.
        const CANDIDATE_EXTRA: u64 = 21;
        const OVER_CEILING_ROWS: u64 = MAX_REPORT_ROWS + 11;
        const OVER_CEILING_BYTES: u64 = MAX_REPORT_BYTES + 11;

        // Row shape.
        {
            let near_max = MAX_REPORT_ROWS - 10;
            for section in &mut writer.sections {
                section.partitions.clear();
            }
            for (section_id, digest_byte) in [
                (ArtifactSectionId::ValidationFinding, 0xB1_u8),
                (ArtifactSectionId::DedupRuleSummary, 0xB2),
            ] {
                let section = writer
                    .sections
                    .iter_mut()
                    .find(|section| section.section_id == section_id)
                    .expect("report section");
                section.partitions.push(
                    ArtifactPartition::try_new(
                        0,
                        near_max,
                        1,
                        crate::ContentDigest::from_bytes([digest_byte; 32]),
                    )
                    .expect("staged partition"),
                );
            }
        }
        match writer.ensure_section_limits(index, CANDIDATE_EXTRA, 0) {
            Err(StorageError::ArtifactRowLimitExceeded { actual, maximum }) => {
                assert_eq!(actual, OVER_CEILING_ROWS);
                assert_eq!(maximum, MAX_REPORT_ROWS);
            }
            other => panic!("per-report row limit must fire first, got {other:?}"),
        }

        // Byte shape: identical staging at the byte ceiling − 10, candidate
        // +21 bytes. The per-report byte gate must beat a bundle byte sum of
        // exactly MAX_BUNDLE_REPORT_BYTES + 1.
        {
            let near_max = MAX_REPORT_BYTES - 10;
            for section in &mut writer.sections {
                section.partitions.clear();
            }
            for (section_id, digest_byte) in [
                (ArtifactSectionId::ValidationFinding, 0xB3_u8),
                (ArtifactSectionId::DedupRuleSummary, 0xB4),
            ] {
                let section = writer
                    .sections
                    .iter_mut()
                    .find(|section| section.section_id == section_id)
                    .expect("report section");
                section.partitions.push(
                    ArtifactPartition::try_new(
                        0,
                        1,
                        near_max,
                        crate::ContentDigest::from_bytes([digest_byte; 32]),
                    )
                    .expect("staged partition"),
                );
            }
        }
        match writer.ensure_section_limits(index, 0, CANDIDATE_EXTRA) {
            Err(StorageError::ArtifactByteLimitExceeded { actual, maximum }) => {
                assert_eq!(actual, OVER_CEILING_BYTES);
                assert_eq!(maximum, MAX_REPORT_BYTES);
            }
            other => panic!("per-report byte limit must fire first, got {other:?}"),
        }

        // Partition shape: both reports filled to MAX_REPORT_PARTITIONS
        // legal partitions each (sequences strictly increasing from zero,
        // 1 row / 1 byte apiece so no row or byte limit can pre-fire). The
        // prospective candidate implicitly adds one partition to the
        // validation report: own report = ceiling + 1, bundle sum =
        // MAX_BUNDLE_REPORT_PARTITIONS + 1; the per-report gate must win.
        {
            for section in &mut writer.sections {
                section.partitions.clear();
            }
            for (section_id, digest_byte) in [
                (ArtifactSectionId::ValidationFinding, 0xB5_u8),
                (ArtifactSectionId::DedupRuleSummary, 0xB6),
            ] {
                let section = writer
                    .sections
                    .iter_mut()
                    .find(|section| section.section_id == section_id)
                    .expect("report section");
                for sequence in 0..MAX_REPORT_PARTITIONS {
                    section.partitions.push(
                        ArtifactPartition::try_new(
                            sequence,
                            1,
                            1,
                            crate::ContentDigest::from_bytes([digest_byte; 32]),
                        )
                        .expect("staged partition"),
                    );
                }
            }
        }
        match writer.ensure_section_limits(index, 0, 0) {
            Err(StorageError::ArtifactPartitionLimitExceeded { actual, maximum }) => {
                assert_eq!(actual, MAX_REPORT_PARTITIONS + 1);
                assert_eq!(maximum, MAX_REPORT_PARTITIONS);
            }
            other => panic!("per-report partition limit must fire first, got {other:?}"),
        }
    }

    #[test]
    fn severity_counts_flow_into_committed_provenance() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let bundle = publish_standard_bundle(&store, false);
        let provenance = bundle.validation_report().provenance();
        assert_eq!(provenance.summary.finding_count, 2);
        assert_eq!(provenance.summary.warning_count, 1);
        assert_eq!(provenance.summary.error_count, 1);
        let dedup_provenance = bundle.deduplication_report().provenance();
        assert_eq!(dedup_provenance.summary.duplicate_count, 2);
        assert_eq!(dedup_provenance.summary.finding_count, 2);
        // No committed digest is a stub or zero value.
        assert_ne!(provenance.content_digest, [0_u8; 32]);
        assert_ne!(bundle.provenance().content_digest, [0_u8; 32]);
        assert_ne!(bundle.accepted().provenance().content_digest, [0_u8; 32]);
        // Bundle summary aggregates every present artifact.
        assert_eq!(bundle.provenance().summary.finding_count, 4);
    }

    #[test]
    fn invalid_finding_severity_is_refused_before_any_write() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, false);
        let finding_schema = Arc::new(artifact::validation_finding_section_schema());
        let mut severity = BTreeMap::new();
        severity.insert(
            "severity",
            Arc::new(StringArray::from(vec!["fatal"])) as ArrayRef,
        );
        assert!(writer
            .append_validation_findings(&section_envelope(finding_schema, 0, 1, severity))
            .is_err());
        // Nothing was staged for the finding section.
        let section = writer
            .sections
            .iter()
            .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
            .expect("section");
        assert!(section.partitions.is_empty());
    }

    #[test]
    fn concurrent_reader_never_sees_a_partial_bundle() {
        let temp = TempDir::new().expect("temp");
        let shared = std::sync::Arc::new(open_store(&temp));
        let reader = std::sync::Arc::clone(&shared);
        let handle = std::thread::spawn(move || {
            // Poll while the writer publishes; every observation before the
            // commit must be NotFound, never a partial bundle.
            for _ in 0..200 {
                match reader.load_verification_bundle(Uuid::from_u128(BUNDLE)) {
                    Ok(bundle) => {
                        // Whatever becomes visible must be complete.
                        assert_eq!(bundle.membership().run_id(), Uuid::from_u128(RUN));
                        assert!(bundle.validation_report().manifest().sections().len() == 2);
                        return;
                    }
                    Err(StorageError::NotFound(_)) => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("unexpected error: {error:?}"),
                }
            }
            panic!("bundle never became visible");
        });
        let _ = publish_standard_bundle(&shared, false);
        handle.join().expect("reader thread");
    }

    #[test]
    fn draft_validation_rejects_bad_identities_and_timestamps() {
        // Duplicate artifact ids.
        let mut draft_input = provenance_input();
        draft_input.artifact_id = Uuid::from_u128(VALIDATION);
        let provenance = ArtifactProvenanceDraft {
            input: draft_input,
            plan_fingerprint: [0; 32],
            canonical_plan_digest: [0; 32],
            engine_contract_version: 1,
            engine_build: "build".to_owned(),
            verification_contract_version: VERIFICATION_CONTRACT_VERSION,
        };
        let accepted = SnapshotDraft::try_new(
            Uuid::from_u128(ACCEPTED),
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(SOURCE_ASSET),
            source_schema().as_ref().clone(),
            std::collections::BTreeSet::new(),
            None,
            at(CREATED),
        )
        .expect("draft");
        assert!(VerificationBundleDraft::try_new(
            provenance,
            accepted,
            Uuid::from_u128(VALIDATION),
            None,
            Uuid::from_u128(DEDUP),
        )
        .is_err());
    }

    #[test]
    fn provenance_timestamp_ordering_is_enforced() {
        let mut input = provenance_input();
        input.created_at = at(COMMITTED + 10);
        let provenance = ArtifactProvenanceDraft {
            input,
            plan_fingerprint: [0; 32],
            canonical_plan_digest: [0; 32],
            engine_contract_version: 1,
            engine_build: "build".to_owned(),
            verification_contract_version: VERIFICATION_CONTRACT_VERSION,
        };
        let accepted = SnapshotDraft::try_new(
            Uuid::from_u128(ACCEPTED),
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(SOURCE_ASSET),
            source_schema().as_ref().clone(),
            std::collections::BTreeSet::new(),
            None,
            at(CREATED),
        )
        .expect("draft");
        assert!(matches!(
            VerificationBundleDraft::try_new(
                provenance,
                accepted,
                Uuid::from_u128(VALIDATION),
                None,
                Uuid::from_u128(DEDUP),
            ),
            Err(StorageError::InvalidTimestampOrder(_))
        ));
    }

    #[test]
    fn rejected_append_without_authorization_is_refused() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, false);
        let rejected_schema = Arc::new(
            artifact::rejected_rows_section_schema(&source_schema()).expect("rejected schema"),
        );
        assert!(matches!(
            writer.append_rejected_rows(&section_envelope(rejected_schema, 0, 1, BTreeMap::new())),
            Err(StorageError::InvalidDraft(_))
        ));
    }

    // ---- Symmetric identity reservation (E4-S1-R1 blocker C) ----

    fn ordinary_snapshot_draft(id: u128) -> SnapshotDraft {
        SnapshotDraft::try_new(
            Uuid::from_u128(id),
            Uuid::from_u128(0xE410),
            Uuid::from_u128(0xE411),
            Uuid::from_u128(SOURCE_ASSET),
            source_schema().as_ref().clone(),
            std::collections::BTreeSet::from([Uuid::from_u128(0xE412)]),
            Some(95),
            at(CREATED),
        )
        .expect("snapshot draft")
    }

    /// Any pending bundle reserves every one of its directory-mapped ids
    /// against both publication families: a second bundle reusing the
    /// accepted id, and an ordinary snapshot reusing a child artifact id,
    /// are both refused before any staging exists.
    #[test]
    fn begin_rejects_ids_reserved_by_pending_bundle_across_families() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let _pending = begin(&store, true);

        let conflicting_accepted = draft_with_ids(
            RUN + 0x100,
            BUNDLE + 0x100,
            BUNDLE_ARTIFACT + 0x100,
            ACCEPTED,
            VALIDATION + 0x100,
            Some(REJECTED + 0x100),
            DEDUP + 0x100,
        );
        assert!(matches!(
            store.begin_verification_bundle(conflicting_accepted, at(STARTED)),
            Err(StorageError::AlreadyExists(_))
        ));

        let child_id_conflict =
            store.begin_snapshot(ordinary_snapshot_draft(VALIDATION), at(STARTED));
        assert!(matches!(
            child_id_conflict,
            Err(StorageError::AlreadyExists(_))
        ));
    }

    /// Prevention half: while a bundle claim over S is pending, an ordinary
    /// publication of S is refused outright.
    #[test]
    fn begin_snapshot_reusing_pending_bundle_accepted_id_is_refused() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let _pending = begin(&store, true);
        assert!(matches!(
            store.begin_snapshot(ordinary_snapshot_draft(ACCEPTED), at(STARTED)),
            Err(StorageError::AlreadyExists(_))
        ));
    }

    /// The mandated recovery-safety regression, expressed against the only
    /// reachable historical state (the fix makes the live double-claim
    /// impossible at begin time): a committed ordinary snapshot whose id also
    /// appears on a stale foreign bundle journal row survives recovery
    /// untouched, files included. Deterministic: no sleeps, no races.
    #[test]
    fn recovery_never_deletes_ordinary_snapshot_committed_after_stale_claim() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);

        let mut snapshot_writer = store
            .begin_snapshot(ordinary_snapshot_draft(ACCEPTED), at(STARTED))
            .expect("ordinary begin");
        snapshot_writer
            .append(&accepted_envelope(0, vec![9, 9, 9]))
            .expect("append");
        snapshot_writer.commit().expect("ordinary commit");

        // Historical residue: a since-crashed bundle claim over the same id,
        // stamped older than the recovery cutoff.
        let connection = open_connection(&store.inner).expect("connection");
        connection
            .execute(
                "INSERT INTO bundle_publications(
                     bundle_id, run_id, accepted_snapshot_id, bundle_artifact_id,
                     validation_report_artifact_id, rejected_rows_artifact_id,
                     deduplication_report_artifact_id, started_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)",
                params![
                    Uuid::from_u128(0xE4F0).to_string(),
                    Uuid::from_u128(0xE4F1).to_string(),
                    Uuid::from_u128(ACCEPTED).to_string(),
                    Uuid::from_u128(0xE4F2).to_string(),
                    Uuid::from_u128(0xE4F3).to_string(),
                    Uuid::from_u128(0xE4F4).to_string(),
                    format_timestamp(&at(STARTED)),
                ],
            )
            .expect("inject stale foreign claim");

        let report = store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("recover");
        assert!(report.recovered() >= 1, "stale claim must be reclaimed");

        store
            .load_manifest(Uuid::from_u128(ACCEPTED))
            .expect("committed manifest survives recovery");
        store
            .verify_snapshot(Uuid::from_u128(ACCEPTED))
            .expect("partition files survive recovery");

        let second = store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("second recover");
        assert_eq!(second.examined(), 0, "recovery must be idempotent");
        assert_eq!(second.recovered(), 0, "recovery must be idempotent");
    }

    /// Recovery must also respect ids owned by a committed bundle when a
    /// foreign stale journal row (hand-injected to represent any historical
    /// state) references them.
    #[test]
    fn recovery_never_deletes_ids_owned_by_committed_bundle_from_stale_row() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        publish_standard_bundle(&store, false);

        let connection = open_connection(&store.inner).expect("connection");
        connection
            .execute(
                "INSERT INTO bundle_publications(
                     bundle_id, run_id, accepted_snapshot_id, bundle_artifact_id,
                     validation_report_artifact_id, rejected_rows_artifact_id,
                     deduplication_report_artifact_id, started_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)",
                params![
                    Uuid::from_u128(0xE4F0).to_string(),
                    Uuid::from_u128(0xE4F1).to_string(),
                    Uuid::from_u128(ACCEPTED).to_string(),
                    Uuid::from_u128(BUNDLE_ARTIFACT).to_string(),
                    Uuid::from_u128(0xE4F2).to_string(),
                    Uuid::from_u128(0xE4F3).to_string(),
                    format_timestamp(&at(STARTED)),
                ],
            )
            .expect("inject stale foreign claim");

        store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("recover");

        store
            .load_manifest(Uuid::from_u128(ACCEPTED))
            .expect("accepted partition directory survives");
        store
            .load_verification_bundle(Uuid::from_u128(BUNDLE))
            .expect("committed bundle survives with all artifacts");

        let second = store
            .recover(at(COMMITTED), Duration::ZERO, MAX_MAINTENANCE_CANDIDATES)
            .expect("second recover");
        assert_eq!(second.examined(), 0);
        assert_eq!(second.recovered(), 0);
    }

    // ---- E4-S1-R1 evidence gaps ----

    fn open_store_with_limits(temp: &TempDir, max_partitions: u32) -> SnapshotStore {
        let limits =
            StorageLimits::try_new(64, max_partitions, 1_000_000_000, 1_000_000_000_000, 8, 8)
                .expect("configured limits");
        SnapshotStore::open(temp.path(), limits).expect("store")
    }

    fn publish_with_severities(store: &SnapshotStore, severities: &[&str]) -> VerificationBundle {
        let mut writer = begin(store, false);
        writer
            .append_accepted(&accepted_envelope(0, vec![1]))
            .expect("accepted append");
        writer
            .append_validation_rule_summary(&section_envelope(
                Arc::new(artifact::validation_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("summary append");
        let finding_schema = Arc::new(artifact::validation_finding_section_schema());
        let mut severity = BTreeMap::new();
        severity.insert(
            "severity",
            Arc::new(StringArray::from(severities.to_vec())) as ArrayRef,
        );
        writer
            .append_validation_findings(&section_envelope(
                finding_schema,
                0,
                severities.len(),
                severity,
            ))
            .expect("findings append");
        writer
            .append_dedup_rule_summary(&section_envelope(
                Arc::new(artifact::dedup_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("dedup summary append");
        writer
            .append_duplicate_findings(&section_envelope(
                Arc::new(artifact::duplicate_finding_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("duplicate append");
        writer.commit(at(COMMITTED)).expect("commit")
    }

    /// V02 branch: `Some(rejected_rows_artifact_id)` with zero terminal
    /// rejections publishes successfully, keeps the id unused, and never
    /// materializes a rejected artifact.
    #[test]
    fn some_authorized_rejected_id_with_zero_rejections_publishes_cleanly() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, true); // Some(REJECTED) authorized
        writer
            .append_accepted(&accepted_envelope(0, vec![1, 2]))
            .expect("accepted");
        writer
            .append_validation_rule_summary(&section_envelope(
                Arc::new(artifact::validation_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("summary");
        writer
            .append_dedup_rule_summary(&section_envelope(
                Arc::new(artifact::dedup_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("dedup summary");
        let bundle = writer.commit(at(COMMITTED)).expect("commit");
        assert_eq!(bundle.membership().rejected_rows_artifact_id(), None);
        assert!(bundle.rejected_rows().is_none());
        assert!(store.load_manifest(Uuid::from_u128(REJECTED)).is_err());
    }

    /// Append-level byte ceiling: with the staged aggregate driven to just
    /// under the frozen per-report byte ceiling, a real (tiny) append crosses
    /// it and is refused before any write — and leaves the severity tallies
    /// untouched. The next valid pack commits with correct counts.
    #[test]
    fn byte_limit_append_refuses_and_keeps_tallies_clean() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let mut writer = begin(&store, true);
        let finding_schema = Arc::new(artifact::validation_finding_section_schema());

        // Stage the report aggregate to MAX_REPORT_BYTES - 1024 so any real
        // envelope tips it over the frozen ceiling.
        {
            let section = writer
                .sections
                .iter_mut()
                .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
                .expect("section");
            section.partitions.push(
                ArtifactPartition::try_new(
                    0,
                    1,
                    MAX_REPORT_BYTES - 1024,
                    crate::ContentDigest::from_bytes([0xAD; 32]),
                )
                .expect("staged partition"),
            );
        }

        let mut pack = BTreeMap::new();
        pack.insert(
            "severity",
            Arc::new(StringArray::from(vec!["warning", "error"])) as ArrayRef,
        );
        assert!(matches!(
            writer.append_validation_findings(&section_envelope(
                finding_schema.clone(),
                0,
                2,
                pack
            )),
            Err(StorageError::ArtifactByteLimitExceeded { .. })
        ));

        // The refused pack must not have touched the tallies.
        {
            let section = writer
                .sections
                .iter()
                .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
                .expect("section");
            assert_eq!(section.warning_count, 0);
            assert_eq!(section.error_count, 0);
        }

        // Remove the synthetic pressure; the same pack now appends cleanly.
        {
            let section = writer
                .sections
                .iter_mut()
                .find(|section| section.section_id == ArtifactSectionId::ValidationFinding)
                .expect("section");
            section.partitions.clear();
        }
        // The refused pack burned its sequence slot (monotonic envelope
        // contract); the retry continues from the next sequence.
        let mut retry = BTreeMap::new();
        retry.insert(
            "severity",
            Arc::new(StringArray::from(vec!["warning", "error"])) as ArrayRef,
        );
        writer
            .append_validation_findings(&section_envelope(finding_schema, 1, 2, retry))
            .expect("valid pack after refusal");

        let bundle = writer.commit(at(COMMITTED)).expect("commit");
        let provenance = bundle.validation_report().provenance();
        assert_eq!(provenance.summary.finding_count, 2);
        assert_eq!(provenance.summary.warning_count, 1);
        assert_eq!(provenance.summary.error_count, 1);
    }

    /// Append-level partition ceiling through configured storage limits on
    /// the rejected-rows path (snapshot limits family).
    #[test]
    fn configured_partition_limit_fails_at_append_time() {
        let temp = TempDir::new().expect("temp");
        let store = open_store_with_limits(&temp, 2);
        let mut writer = begin(&store, true);
        let rejected_schema =
            Arc::new(artifact::rejected_rows_section_schema(&source_schema()).expect("rejected"));
        for sequence in 0..2_u64 {
            writer
                .append_rejected_rows(&section_envelope(
                    rejected_schema.clone(),
                    sequence,
                    1,
                    BTreeMap::new(),
                ))
                .expect("first two packs fit");
        }
        assert!(matches!(
            writer.append_rejected_rows(&section_envelope(rejected_schema, 2, 1, BTreeMap::new())),
            Err(StorageError::PartitionLimitExceeded {
                actual: 3,
                maximum: 2
            })
        ));
    }

    /// A single severity flip changes the committed validation-report digest
    /// and the committed bundle digest.
    #[test]
    fn severity_flip_changes_committed_digests() {
        let temp_a = TempDir::new().expect("temp a");
        let store_a = open_store(&temp_a);
        let bundle_a = publish_with_severities(&store_a, &["warning", "error"]);
        let temp_b = TempDir::new().expect("temp b");
        let store_b = open_store(&temp_b);
        let bundle_b = publish_with_severities(&store_b, &["error", "error"]);

        assert_ne!(
            bundle_a.validation_report().provenance().content_digest,
            bundle_b.validation_report().provenance().content_digest
        );
        assert_ne!(
            bundle_a.provenance().content_digest,
            bundle_b.provenance().content_digest
        );
        assert_eq!(
            bundle_a
                .validation_report()
                .provenance()
                .summary
                .warning_count,
            1
        );
        assert_eq!(
            bundle_b
                .validation_report()
                .provenance()
                .summary
                .warning_count,
            0
        );
    }

    /// A truncated installed Parquet partition fails closed with the typed
    /// integrity error during bundle load.
    #[test]
    fn truncated_partition_fails_closed_with_integrity() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        publish_standard_bundle(&store, true);

        let directory =
            crate::partitions_root(&store.inner).join(Uuid::from_u128(ACCEPTED).to_string());
        let file = std::fs::read_dir(&directory)
            .expect("partition dir")
            .next()
            .expect("partition file")
            .expect("entry")
            .path();
        let bytes = std::fs::read(&file).expect("read partition");

        // Truncation is caught by the physical length gate first.
        std::fs::write(&file, &bytes[..bytes.len() / 3]).expect("truncate");
        assert!(matches!(
            store.load_verification_bundle(Uuid::from_u128(BUNDLE)),
            Err(StorageError::Integrity {
                kind: crate::IntegrityFailure::LengthMismatch,
                ..
            })
        ));

        // Same-length tampering is caught by the physical digest gate.
        let mut flipped = bytes.clone();
        let middle = flipped.len() / 2;
        flipped[middle] ^= 0xFF;
        std::fs::write(&file, &flipped).expect("rewrite partition");
        assert!(matches!(
            store.load_verification_bundle(Uuid::from_u128(BUNDLE)),
            Err(StorageError::Integrity {
                kind: crate::IntegrityFailure::DigestMismatch,
                ..
            })
        ));
    }

    /// Corrupt persisted membership JSON fails closed with the typed
    /// serialization error instead of panicking or returning partial data.
    #[test]
    fn corrupt_membership_json_fails_closed_with_serialization() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        publish_standard_bundle(&store, true);

        let connection = open_connection(&store.inner).expect("connection");
        connection
            .execute(
                "UPDATE verification_bundles SET membership_json = '{broken' WHERE bundle_id = ?1",
                params![Uuid::from_u128(BUNDLE).to_string()],
            )
            .expect("corrupt membership");

        assert!(matches!(
            store.load_verification_bundle(Uuid::from_u128(BUNDLE)),
            Err(StorageError::Serialization(_))
        ));
    }

    /// Issue #176 (D1): a rejected payload containing a null used to fail the
    /// section-reader integrity gate because the canonical bytes depended on
    /// the physical validity-buffer layout. The digest is now computed over a
    /// layout-normalized batch on both the publication and the reader side.
    #[test]
    fn rejected_rows_with_nulls_survive_the_section_integrity_gate() {
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        // The scan field is nullable: null fidelity is exactly what D1 must
        // preserve (non-nullable columns cannot carry nulls at all).
        let nullable_source = Arc::new(
            stillflow_core::LogicalSchema::new(vec![stillflow_core::LogicalField::new(
                stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x11)),
                "value".to_owned(),
                stillflow_core::LogicalType::Int64,
                true,
            )
            .expect("field")])
            .expect("schema"),
        );
        let draft = draft_with_accepted_schema(
            nullable_source.as_ref().clone(),
            RUN,
            BUNDLE,
            BUNDLE_ARTIFACT,
            ACCEPTED,
            VALIDATION,
            Some(REJECTED),
            DEDUP,
        );
        let mut writer = store
            .begin_verification_bundle(draft, at(STARTED))
            .expect("begin");
        let accepted_arrow = stillflow_core::logical_schema_to_arrow(&nullable_source)
            .expect("accepted arrow schema");
        let accepted_batch =
            RecordBatch::try_new(accepted_arrow, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .expect("accepted batch");
        writer
            .append_accepted(
                &BatchEnvelope::try_new(
                    Arc::new(nullable_source.as_ref().clone()),
                    Uuid::from_u128(SOURCE_ASSET),
                    0,
                    accepted_batch,
                )
                .expect("accepted envelope"),
            )
            .expect("accepted append");
        writer
            .append_validation_rule_summary(&section_envelope(
                Arc::new(artifact::validation_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("summary append");
        let mut finding_overrides: BTreeMap<&'static str, ArrayRef> = BTreeMap::new();
        finding_overrides.insert(
            "severity",
            Arc::new(StringArray::from(vec!["warning"])) as ArrayRef,
        );
        writer
            .append_validation_findings(&section_envelope(
                Arc::new(artifact::validation_finding_section_schema()),
                0,
                1,
                finding_overrides,
            ))
            .expect("findings append");
        let mut overrides: BTreeMap<&'static str, ArrayRef> = BTreeMap::new();
        overrides.insert("value", Arc::new(Int64Array::from(vec![None])) as ArrayRef);
        let bundle_rejected_schema = Arc::new(
            artifact::rejected_rows_section_schema(&nullable_source).expect("rejected schema"),
        );
        writer
            .append_rejected_rows(&section_envelope(bundle_rejected_schema, 0, 1, overrides))
            .expect("rejected append");
        writer
            .append_dedup_rule_summary(&section_envelope(
                Arc::new(artifact::dedup_rule_summary_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("dedup summary append");
        writer
            .append_duplicate_findings(&section_envelope(
                Arc::new(artifact::duplicate_finding_section_schema()),
                0,
                1,
                BTreeMap::new(),
            ))
            .expect("duplicate append");
        writer.commit(at(COMMITTED)).expect("commit");
        let mut reader = store
            .open_artifact_section(
                Uuid::from_u128(BUNDLE),
                Uuid::from_u128(REJECTED),
                artifact::ArtifactSectionId::RejectedRows,
            )
            .expect("open rejected section");
        for item in reader.by_ref() {
            item.expect("rejected envelope must survive integrity");
        }
        let loaded = store
            .load_verification_bundle_by_run_id(Uuid::from_u128(RUN))
            .expect("load bundle");
        let rejected = loaded.rejected_rows().expect("rejected artifact");
        assert_eq!(rejected.manifest().artifact_id(), Uuid::from_u128(REJECTED));
    }

    /// Issue #176 (D2): the rejected artifact binds to the frozen logical
    /// Scan-output schema — terminal rejections must publish and read back
    /// after Drop, Rename, or Cast rules changed the materialized shape.
    #[test]
    fn rejected_section_binds_the_scan_output_schema_across_evolution() {
        let scan = Arc::new(
            stillflow_core::LogicalSchema::new(vec![
                stillflow_core::LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
                    "value".to_owned(),
                    stillflow_core::LogicalType::Int64,
                    false,
                )
                .expect("field"),
                stillflow_core::LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x22)),
                    "tag".to_owned(),
                    stillflow_core::LogicalType::Utf8,
                    true,
                )
                .expect("field"),
            ])
            .expect("scan schema"),
        );
        let variants: Vec<(&'static str, LogicalSchema)> = vec![
            (
                "drop",
                stillflow_core::LogicalSchema::new(vec![stillflow_core::LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
                    "value".to_owned(),
                    stillflow_core::LogicalType::Int64,
                    false,
                )
                .expect("field")])
                .expect("accepted schema"),
            ),
            (
                "rename",
                stillflow_core::LogicalSchema::new(vec![
                    stillflow_core::LogicalField::new(
                        stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
                        "value".to_owned(),
                        stillflow_core::LogicalType::Int64,
                        false,
                    )
                    .expect("field"),
                    stillflow_core::LogicalField::new(
                        stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x22)),
                        "renamed_tag".to_owned(),
                        stillflow_core::LogicalType::Utf8,
                        true,
                    )
                    .expect("field"),
                ])
                .expect("accepted schema"),
            ),
            (
                "cast",
                stillflow_core::LogicalSchema::new(vec![
                    stillflow_core::LogicalField::new(
                        stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
                        "value".to_owned(),
                        stillflow_core::LogicalType::Int32,
                        false,
                    )
                    .expect("field"),
                    stillflow_core::LogicalField::new(
                        stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x22)),
                        "tag".to_owned(),
                        stillflow_core::LogicalType::Utf8,
                        true,
                    )
                    .expect("field"),
                ])
                .expect("accepted schema"),
            ),
        ];
        for (label, accepted_schema) in variants {
            let temp = TempDir::new().expect("temp");
            let store = open_store(&temp);
            let draft = draft_with_accepted_schema(
                accepted_schema.clone(),
                RUN,
                BUNDLE,
                BUNDLE_ARTIFACT,
                ACCEPTED,
                VALIDATION,
                Some(REJECTED),
                DEDUP,
            )
            .with_rejected_source_schema(scan.as_ref().clone());
            let mut writer = store
                .begin_verification_bundle(draft, at(STARTED))
                .expect("begin");
            let accepted_arrow = stillflow_core::logical_schema_to_arrow(&accepted_schema)
                .expect("accepted arrow schema");
            let accepted_columns: Vec<ArrayRef> = accepted_schema
                .fields
                .iter()
                .map(|field| match field.data_type {
                    stillflow_core::LogicalType::Int64 => {
                        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef
                    }
                    stillflow_core::LogicalType::Int32 => {
                        Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as ArrayRef
                    }
                    _ => Arc::new(StringArray::from(vec!["a".to_owned(), "b".to_owned()]))
                        as ArrayRef,
                })
                .collect();
            let accepted_batch =
                RecordBatch::try_new(accepted_arrow, accepted_columns).expect("accepted batch");
            writer
                .append_accepted(
                    &BatchEnvelope::try_new(
                        Arc::new(accepted_schema.clone()),
                        Uuid::from_u128(SOURCE_ASSET),
                        0,
                        accepted_batch,
                    )
                    .expect("accepted envelope"),
                )
                .expect("accepted append");
            writer
                .append_validation_rule_summary(&section_envelope(
                    Arc::new(artifact::validation_rule_summary_section_schema()),
                    0,
                    1,
                    BTreeMap::new(),
                ))
                .expect("summary append");
            let mut finding_overrides: BTreeMap<&'static str, ArrayRef> = BTreeMap::new();
            finding_overrides.insert(
                "severity",
                Arc::new(StringArray::from(vec!["warning"])) as ArrayRef,
            );
            writer
                .append_validation_findings(&section_envelope(
                    Arc::new(artifact::validation_finding_section_schema()),
                    0,
                    1,
                    finding_overrides,
                ))
                .expect("findings append");
            let rejected_schema =
                Arc::new(artifact::rejected_rows_section_schema(&scan).expect("rejected schema"));
            writer
                .append_rejected_rows(&section_envelope(rejected_schema, 0, 1, BTreeMap::new()))
                .expect("rejected append");
            writer
                .append_dedup_rule_summary(&section_envelope(
                    Arc::new(artifact::dedup_rule_summary_section_schema()),
                    0,
                    1,
                    BTreeMap::new(),
                ))
                .expect("dedup summary append");
            writer
                .append_duplicate_findings(&section_envelope(
                    Arc::new(artifact::duplicate_finding_section_schema()),
                    0,
                    1,
                    BTreeMap::new(),
                ))
                .expect("duplicate append");
            let bundle = writer.commit(at(COMMITTED)).expect("commit");
            let rejected = bundle.rejected_rows().expect("rejected artifact");
            let section = rejected
                .manifest()
                .sections()
                .iter()
                .find(|section| section.section_id() == artifact::ArtifactSectionId::RejectedRows)
                .expect("rejected section");
            let expected = artifact::rejected_rows_section_schema(&scan).expect("expected schema");
            for (field, expected_field) in section.schema().fields.iter().zip(&expected.fields) {
                assert_eq!(
                    field.id, expected_field.id,
                    "{label}: ColumnId must stay bound to the scan output"
                );
                assert_eq!(field.name, expected_field.name, "{label}");
                assert_eq!(field.data_type, expected_field.data_type, "{label}");
            }
            let mut reader = store
                .open_artifact_section(
                    Uuid::from_u128(BUNDLE),
                    Uuid::from_u128(REJECTED),
                    artifact::ArtifactSectionId::RejectedRows,
                )
                .expect("open rejected section");
            for item in reader.by_ref() {
                item.expect("{label}: rejected envelope survives integrity");
            }
        }

        // Negative control: without the explicit binding, a divergent
        // accepted schema drifts the rejected section away from the
        // scan-output rows and publication must be refused.
        let scan = Arc::new(
            stillflow_core::LogicalSchema::new(vec![
                stillflow_core::LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
                    "value".to_owned(),
                    stillflow_core::LogicalType::Int64,
                    false,
                )
                .expect("field"),
                stillflow_core::LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x22)),
                    "tag".to_owned(),
                    stillflow_core::LogicalType::Utf8,
                    true,
                )
                .expect("field"),
            ])
            .expect("scan schema"),
        );
        let dropped = stillflow_core::LogicalSchema::new(vec![stillflow_core::LogicalField::new(
            stillflow_core::ColumnId::from_uuid(Uuid::from_u128(0x21)),
            "value".to_owned(),
            stillflow_core::LogicalType::Int64,
            false,
        )
        .expect("field")])
        .expect("accepted schema");
        let temp = TempDir::new().expect("temp");
        let store = open_store(&temp);
        let draft = draft_with_accepted_schema(
            dropped.clone(),
            RUN,
            BUNDLE,
            BUNDLE_ARTIFACT,
            ACCEPTED,
            VALIDATION,
            Some(REJECTED),
            DEDUP,
        );
        let mut writer = store
            .begin_verification_bundle(draft, at(STARTED))
            .expect("begin");
        let dropped_arrow =
            stillflow_core::logical_schema_to_arrow(&dropped).expect("accepted arrow schema");
        let dropped_batch =
            RecordBatch::try_new(dropped_arrow, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .expect("accepted batch");
        writer
            .append_accepted(
                &BatchEnvelope::try_new(
                    Arc::new(dropped.clone()),
                    Uuid::from_u128(SOURCE_ASSET),
                    0,
                    dropped_batch,
                )
                .expect("accepted envelope"),
            )
            .expect("accepted append");
        let rejected_schema =
            Arc::new(artifact::rejected_rows_section_schema(&scan).expect("rejected schema"));
        let error = writer
            .append_rejected_rows(&section_envelope(rejected_schema, 0, 1, BTreeMap::new()))
            .expect_err("unbound divergent publication must be refused");
        assert!(matches!(error, StorageError::SchemaDrift { .. }));
    }
}
