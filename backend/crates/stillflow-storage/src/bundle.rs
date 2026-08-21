//! Experimental VerificationBundle publication.
//!
//! Probe for Issue #54 sections 8 and 10. Digests hash durable Parquet files
//! rather than the unapproved canonical envelope preimage. Do not merge.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use stillflow_core::{
    ArtifactKind, ArtifactProvenance, ArtifactProvenanceDraft, ArtifactSummary, BatchEnvelope,
    LogicalSchema, LogicalSchemaFingerprint, SnapshotStats,
};
use uuid::Uuid;

use crate::digest::{digest_bytes, digest_file, ContentDigest};
use crate::manifest::{build_snapshot, SnapshotDraft, SnapshotManifest, SnapshotPartition};
use crate::store::{
    abort_publication, acquire_activity, create_exact_directory, create_final_snapshot_directory,
    format_timestamp, insert_publication, insert_snapshot_rows, load_manifest_inner,
    open_connection, parse_uuid, partitions_root, remove_uuid_directory, staging_root,
    write_partition, ActivityGuard, ActivityKind, SnapshotStore, StoreInner, SymlinkPolicy,
};
use crate::StorageError;

pub const ARTIFACT_MANIFEST_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArtifactSectionId {
    ValidationRuleSummary = 1,
    ValidationFinding = 2,
    RejectedRows = 3,
    DedupRuleSummary = 4,
    DuplicateFinding = 5,
}

impl ArtifactSectionId {
    pub const fn tag(self) -> u8 {
        self as u8
    }

    fn from_tag(tag: i64) -> Result<Self, StorageError> {
        match tag {
            1 => Ok(Self::ValidationRuleSummary),
            2 => Ok(Self::ValidationFinding),
            3 => Ok(Self::RejectedRows),
            4 => Ok(Self::DedupRuleSummary),
            5 => Ok(Self::DuplicateFinding),
            _ => Err(StorageError::InvalidManifest("unknown artifact section")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactSectionStats {
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub partition_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPartition {
    pub sequence: u32,
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactSection {
    pub section_id: ArtifactSectionId,
    pub schema: LogicalSchema,
    pub schema_fingerprint: LogicalSchemaFingerprint,
    pub stats: ArtifactSectionStats,
    pub partitions: Vec<ArtifactPartition>,
    pub section_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub version: u16,
    pub artifact_id: Uuid,
    pub kind: ArtifactKind,
    pub sections: Vec<ArtifactSection>,
    pub manifest_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationBundleMembership {
    pub bundle_id: Uuid,
    pub run_id: Uuid,
    pub bundle_artifact_id: Uuid,
    pub accepted_snapshot_id: Uuid,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedSnapshotArtifact {
    pub manifest: SnapshotManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationReportArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedRowsArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeduplicationReportArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationBundle {
    pub membership: VerificationBundleMembership,
    pub provenance: ArtifactProvenance,
    pub accepted: AcceptedSnapshotArtifact,
    pub validation_report: ValidationReportArtifact,
    pub rejected_rows: Option<RejectedRowsArtifact>,
    pub deduplication_report: DeduplicationReportArtifact,
}

pub struct VerificationBundleDraft {
    pub provenance: ArtifactProvenanceDraft,
    pub accepted: SnapshotDraft,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
}

struct SectionSink {
    section_id: ArtifactSectionId,
    schema: LogicalSchema,
    fingerprint: LogicalSchemaFingerprint,
    staging_dir: std::path::PathBuf,
    partitions: Vec<SnapshotPartition>,
    next_sequence: u64,
    envelope_count: u32,
    row_count: u64,
    stored_byte_count: u64,
}

pub struct VerificationBundleWriter {
    inner: Arc<StoreInner>,
    _activity: Option<ActivityGuard>,
    draft: VerificationBundleDraft,
    accepted_staging: std::path::PathBuf,
    accepted_partitions: Vec<SnapshotPartition>,
    accepted_next_sequence: u64,
    accepted_envelope_count: u32,
    accepted_row_count: u64,
    accepted_stored_bytes: u64,
    sections: BTreeMap<(Uuid, ArtifactSectionId), SectionSink>,
    installed: Vec<Uuid>,
    committed: bool,
    failed: bool,
}

impl SnapshotStore {
    pub fn begin_verification_bundle(
        &self,
        draft: VerificationBundleDraft,
        started_at: DateTime<Utc>,
    ) -> Result<VerificationBundleWriter, StorageError> {
        validate_draft(&draft, started_at)?;
        let activity = acquire_activity(self.inner(), ActivityKind::Publisher)?;
        insert_publication(self.inner(), draft.accepted.id(), &started_at)?;

        let bundle_staging =
            staging_root(self.inner()).join(draft.provenance.input.bundle_id.to_string());
        if let Err(error) =
            create_exact_directory(&bundle_staging, "create bundle staging directory")
        {
            abort_publication(self.inner(), draft.accepted.id());
            return Err(error);
        }
        let accepted_staging = bundle_staging.join(draft.accepted.id().to_string());
        if let Err(error) = create_exact_directory(&accepted_staging, "create accepted staging") {
            let _ = remove_uuid_directory(
                &staging_root(self.inner()),
                draft.provenance.input.bundle_id,
                SymlinkPolicy::Ignore,
                "abort bundle staging",
            );
            abort_publication(self.inner(), draft.accepted.id());
            return Err(error);
        }

        Ok(VerificationBundleWriter {
            inner: Arc::clone(self.inner()),
            _activity: Some(activity),
            draft,
            accepted_staging,
            accepted_partitions: Vec::new(),
            accepted_next_sequence: 0,
            accepted_envelope_count: 0,
            accepted_row_count: 0,
            accepted_stored_bytes: 0,
            sections: BTreeMap::new(),
            installed: Vec::new(),
            committed: false,
            failed: false,
        })
    }

    pub fn load_verification_bundle(
        &self,
        bundle_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(self.inner(), ActivityKind::Reader)?;
        load_bundle(self.inner(), "bundle_id", bundle_id)
    }

    pub fn load_verification_bundle_by_snapshot(
        &self,
        snapshot_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(self.inner(), ActivityKind::Reader)?;
        load_bundle(self.inner(), "accepted_snapshot_id", snapshot_id)
    }

    pub fn load_verification_bundle_by_run_id(
        &self,
        run_id: Uuid,
    ) -> Result<VerificationBundle, StorageError> {
        let _activity = acquire_activity(self.inner(), ActivityKind::Reader)?;
        load_bundle(self.inner(), "run_id", run_id)
    }

    pub fn open_artifact_section(
        &self,
        bundle_id: Uuid,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
    ) -> Result<ArtifactBatchReader, StorageError> {
        let activity = acquire_activity(self.inner(), ActivityKind::Reader)?;
        let bundle = load_bundle(self.inner(), "bundle_id", bundle_id)?;
        let manifest = artifact_manifest_in_bundle(&bundle, artifact_id)?;
        let section = manifest
            .sections
            .iter()
            .find(|section| section.section_id == section_id)
            .ok_or(StorageError::NotFound(artifact_id))?
            .clone();
        Ok(ArtifactBatchReader {
            inner: Arc::clone(self.inner()),
            _activity: activity,
            artifact_id,
            section,
            next_partition: 0,
        })
    }

    pub fn open_dedup_index(
        &self,
        run_id: Uuid,
        bundle_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<crate::dedup::DedupIndex, StorageError> {
        crate::dedup::DedupIndex::open(self.inner(), run_id, bundle_id, started_at)
    }
}

pub struct ArtifactBatchReader {
    inner: Arc<StoreInner>,
    _activity: ActivityGuard,
    artifact_id: Uuid,
    section: ArtifactSection,
    next_partition: usize,
}

impl Iterator for ArtifactBatchReader {
    type Item = Result<BatchEnvelope, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.section.partitions.get(self.next_partition)?.clone();
        self.next_partition += 1;
        Some(read_artifact_partition(
            &self.inner,
            self.artifact_id,
            &self.section,
            &partition,
        ))
    }
}

impl VerificationBundleWriter {
    pub fn append_accepted(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        self.append_to_accepted(envelope)
    }

    pub fn append_validation_rule_summary(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(
            self.draft.validation_report_artifact_id,
            ArtifactSectionId::ValidationRuleSummary,
            envelope,
        )
    }

    pub fn append_validation_findings(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(
            self.draft.validation_report_artifact_id,
            ArtifactSectionId::ValidationFinding,
            envelope,
        )
    }

    pub fn append_rejected_rows(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        let artifact_id =
            self.draft
                .rejected_rows_artifact_id
                .ok_or(StorageError::InvalidDraft(
                    "rejected rows artifact id is missing",
                ))?;
        self.append_section(artifact_id, ArtifactSectionId::RejectedRows, envelope)
    }

    pub fn append_dedup_rule_summary(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(
            self.draft.deduplication_report_artifact_id,
            ArtifactSectionId::DedupRuleSummary,
            envelope,
        )
    }

    pub fn append_duplicate_findings(
        &mut self,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        self.append_section(
            self.draft.deduplication_report_artifact_id,
            ArtifactSectionId::DuplicateFinding,
            envelope,
        )
    }

    pub fn bind_section_schema(
        &mut self,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
        schema: LogicalSchema,
    ) -> Result<(), StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "bundle writer is already in a failed state",
            ));
        }
        let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
            .map_err(|_| StorageError::InvalidDraft("artifact schema fingerprint failed"))?;
        let staging_dir = section_staging_dir(
            &self.inner,
            self.draft.provenance.input.bundle_id,
            artifact_id,
            section_id,
        );
        if let Entry::Vacant(entry) = self.sections.entry((artifact_id, section_id)) {
            create_section_dirs(&staging_dir)?;
            entry.insert(SectionSink {
                section_id,
                schema,
                fingerprint,
                staging_dir,
                partitions: Vec::new(),
                next_sequence: 0,
                envelope_count: 0,
                row_count: 0,
                stored_byte_count: 0,
            });
        }
        Ok(())
    }

    pub fn commit(
        mut self,
        committed_at: DateTime<Utc>,
    ) -> Result<VerificationBundle, StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "bundle writer cannot commit after a failed append",
            ));
        }
        if committed_at < self.draft.provenance.input.started_at {
            return Err(StorageError::InvalidTimestampOrder(
                "bundle commit and publication start",
            ));
        }

        let accepted_stats = SnapshotStats::try_new(
            self.accepted_row_count,
            self.accepted_stored_bytes,
            u32::try_from(self.accepted_partitions.len())
                .map_err(|_| StorageError::ArithmeticOverflow("accepted partition count"))?,
        )?;
        let accepted_snapshot = build_snapshot(&self.draft.accepted, accepted_stats)?;
        let accepted_manifest =
            SnapshotManifest::try_new(accepted_snapshot, self.accepted_partitions.clone())?;

        create_final_snapshot_directory(&self.inner, self.draft.accepted.id())?;
        self.installed.push(self.draft.accepted.id());
        install_named_partitions(
            &self.inner,
            self.draft.accepted.id(),
            &self.accepted_staging,
            &self.accepted_partitions,
        )?;

        let validation = self.finish_artifact(
            self.draft.validation_report_artifact_id,
            ArtifactKind::ValidationReport,
            &[
                ArtifactSectionId::ValidationRuleSummary,
                ArtifactSectionId::ValidationFinding,
            ],
        )?;
        let rejected = match self.draft.rejected_rows_artifact_id {
            Some(artifact_id)
                if self
                    .sections
                    .get(&(artifact_id, ArtifactSectionId::RejectedRows))
                    .is_some_and(|sink| sink.row_count > 0) =>
            {
                Some(self.finish_artifact(
                    artifact_id,
                    ArtifactKind::RejectedRows,
                    &[ArtifactSectionId::RejectedRows],
                )?)
            }
            _ => None,
        };
        let dedup = self.finish_artifact(
            self.draft.deduplication_report_artifact_id,
            ArtifactKind::DeduplicationReport,
            &[
                ArtifactSectionId::DedupRuleSummary,
                ArtifactSectionId::DuplicateFinding,
            ],
        )?;

        let mut provenance = self.draft.provenance.clone();
        provenance.input.committed_at = committed_at;
        let bundle = commit_bundle_rows(
            &self.inner,
            &provenance,
            &accepted_manifest,
            &validation,
            rejected.as_ref(),
            &dedup,
        )?;
        self.committed = true;
        let _ = remove_uuid_directory(
            &staging_root(&self.inner),
            provenance.input.bundle_id,
            SymlinkPolicy::Ignore,
            "remove committed bundle staging",
        );
        Ok(bundle)
    }

    fn append_to_accepted(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "bundle writer is already in a failed state",
            ));
        }
        let result = self.append_to_accepted_inner(envelope);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_to_accepted_inner(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        let envelope_count = self
            .accepted_envelope_count
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("input envelope count"))?;
        if envelope_count > self.inner.limits.max_input_envelopes() {
            return Err(StorageError::EnvelopeLimitExceeded {
                actual: envelope_count,
                maximum: self.inner.limits.max_input_envelopes(),
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
        if envelope.schema() != self.draft.accepted.schema()
            || envelope.schema_fingerprint() != self.draft.accepted.schema_fingerprint()
        {
            return Err(StorageError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }
        self.accepted_next_sequence = self
            .accepted_next_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("input sequence"))?;
        self.accepted_envelope_count = envelope_count;
        if envelope.row_count() == 0 {
            return Ok(());
        }
        let partition_sequence = u32::try_from(self.accepted_partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition sequence"))?;
        let partition_count = partition_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("partition count"))?;
        if partition_count > self.inner.limits.max_partitions() {
            return Err(StorageError::PartitionLimitExceeded {
                actual: partition_count,
                maximum: self.inner.limits.max_partitions(),
            });
        }
        let envelope_rows = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("envelope row count"))?;
        let row_count = self
            .accepted_row_count
            .checked_add(envelope_rows)
            .ok_or(StorageError::ArithmeticOverflow("snapshot row count"))?;
        if row_count > self.inner.limits.max_rows() {
            return Err(StorageError::RowLimitExceeded {
                actual: row_count,
                maximum: self.inner.limits.max_rows(),
            });
        }
        let partition = write_partition(&self.accepted_staging, partition_sequence, envelope)?;
        let stored = self
            .accepted_stored_bytes
            .checked_add(partition.stored_byte_count())
            .ok_or(StorageError::ArithmeticOverflow(
                "snapshot stored byte count",
            ))?;
        if stored > self.inner.limits.max_stored_bytes() {
            return Err(StorageError::StoredByteLimitExceeded {
                actual: stored,
                maximum: self.inner.limits.max_stored_bytes(),
            });
        }
        self.accepted_partitions.push(partition);
        self.accepted_row_count = row_count;
        self.accepted_stored_bytes = stored;
        Ok(())
    }

    fn append_section(
        &mut self,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "bundle writer is already in a failed state",
            ));
        }
        let result = self.append_section_inner(artifact_id, section_id, envelope);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_section_inner(
        &mut self,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
        envelope: &BatchEnvelope,
    ) -> Result<(), StorageError> {
        if !self.sections.contains_key(&(artifact_id, section_id)) {
            self.bind_section_schema(artifact_id, section_id, envelope.schema().clone())?;
        }
        let limits = self.inner.limits;
        let sink = self
            .sections
            .get_mut(&(artifact_id, section_id))
            .ok_or(StorageError::InvalidDraft("artifact section is missing"))?;
        if envelope.schema() != &sink.schema || envelope.schema_fingerprint() != sink.fingerprint {
            return Err(StorageError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }
        if envelope.sequence() != sink.next_sequence {
            return Err(StorageError::Sequence {
                expected: sink.next_sequence,
                actual: envelope.sequence(),
            });
        }
        let envelope_count = sink
            .envelope_count
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("artifact envelope count"))?;
        if envelope_count > limits.max_input_envelopes() {
            return Err(StorageError::EnvelopeLimitExceeded {
                actual: envelope_count,
                maximum: limits.max_input_envelopes(),
            });
        }
        sink.next_sequence = sink
            .next_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("artifact sequence"))?;
        sink.envelope_count = envelope_count;
        if envelope.row_count() == 0 {
            return Ok(());
        }
        let partition_sequence = u32::try_from(sink.partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("artifact partition sequence"))?;
        let partition_count = partition_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("artifact partition count"))?;
        if partition_count > limits.max_partitions() {
            return Err(StorageError::PartitionLimitExceeded {
                actual: partition_count,
                maximum: limits.max_partitions(),
            });
        }
        let envelope_rows = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("artifact envelope row count"))?;
        let row_count = sink
            .row_count
            .checked_add(envelope_rows)
            .ok_or(StorageError::ArithmeticOverflow("artifact row count"))?;
        if row_count > limits.max_rows() {
            return Err(StorageError::RowLimitExceeded {
                actual: row_count,
                maximum: limits.max_rows(),
            });
        }
        let partition = write_partition(&sink.staging_dir, partition_sequence, envelope)?;
        let stored = sink
            .stored_byte_count
            .checked_add(partition.stored_byte_count())
            .ok_or(StorageError::ArithmeticOverflow("artifact stored bytes"))?;
        if stored > limits.max_stored_bytes() {
            return Err(StorageError::StoredByteLimitExceeded {
                actual: stored,
                maximum: limits.max_stored_bytes(),
            });
        }
        sink.partitions.push(partition);
        sink.row_count = row_count;
        sink.stored_byte_count = stored;
        Ok(())
    }

    fn finish_artifact(
        &mut self,
        artifact_id: Uuid,
        kind: ArtifactKind,
        section_ids: &[ArtifactSectionId],
    ) -> Result<FinishedArtifact, StorageError> {
        let mut sections = Vec::new();
        for section_id in section_ids {
            let sink = self.sections.remove(&(artifact_id, *section_id));
            let section = match sink {
                Some(sink) => freeze_section(sink)?,
                None => empty_section(*section_id)?,
            };
            if !self.installed.contains(&artifact_id) {
                create_final_snapshot_directory(&self.inner, artifact_id)?;
                self.installed.push(artifact_id);
            }
            let staging = section_staging_dir(
                &self.inner,
                self.draft.provenance.input.bundle_id,
                artifact_id,
                *section_id,
            );
            let partitions: Vec<SnapshotPartition> = section
                .partitions
                .iter()
                .map(|partition| {
                    SnapshotPartition::try_new(
                        partition.sequence,
                        partition.row_count,
                        partition.stored_byte_count,
                        partition.digest,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            install_section_partitions(
                &self.inner,
                artifact_id,
                *section_id,
                &staging,
                &partitions,
            )?;
            sections.push(section);
        }
        let summary = summary_from_sections(kind, &sections);
        let manifest_digest = digest_manifest(artifact_id, kind, &sections);
        Ok(FinishedArtifact {
            manifest: ArtifactManifest {
                version: ARTIFACT_MANIFEST_VERSION,
                artifact_id,
                kind,
                sections,
                manifest_digest,
            },
            summary,
        })
    }
}

impl Drop for VerificationBundleWriter {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let bundle_id = self.draft.provenance.input.bundle_id;
        let _ = remove_uuid_directory(
            &staging_root(&self.inner),
            bundle_id,
            SymlinkPolicy::Ignore,
            "abort bundle staging directory",
        );
        for artifact_id in &self.installed {
            let _ = remove_uuid_directory(
                &partitions_root(&self.inner),
                *artifact_id,
                SymlinkPolicy::Ignore,
                "abort installed bundle artifact",
            );
        }
        abort_publication(&self.inner, self.draft.accepted.id());
    }
}

struct FinishedArtifact {
    manifest: ArtifactManifest,
    summary: ArtifactSummary,
}

fn validate_draft(
    draft: &VerificationBundleDraft,
    started_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let input = &draft.provenance.input;
    if input.run_id.is_nil()
        || input.bundle_id.is_nil()
        || input.artifact_id.is_nil()
        || input.session_id.is_nil()
        || draft.validation_report_artifact_id.is_nil()
        || draft.deduplication_report_artifact_id.is_nil()
        || draft.accepted.id().is_nil()
    {
        return Err(StorageError::InvalidDraft(
            "verification identities must not be nil",
        ));
    }
    if draft
        .rejected_rows_artifact_id
        .is_some_and(|id| id.is_nil())
    {
        return Err(StorageError::InvalidDraft(
            "rejected rows artifact id must not be nil",
        ));
    }
    if input.created_at > input.started_at || input.started_at > started_at {
        return Err(StorageError::InvalidTimestampOrder(
            "verification provenance timestamps",
        ));
    }
    Ok(())
}

fn section_staging_dir(
    inner: &StoreInner,
    bundle_id: Uuid,
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
) -> std::path::PathBuf {
    staging_root(inner)
        .join(bundle_id.to_string())
        .join(artifact_id.to_string())
        .join(format!("{:02}", section_id.tag()))
}

fn create_section_dirs(path: &std::path::Path) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| StorageError::io("create artifact staging parent", &error))?;
    }
    create_exact_directory(path, "create artifact section staging")
}

fn freeze_section(sink: SectionSink) -> Result<ArtifactSection, StorageError> {
    let partition_count = u32::try_from(sink.partitions.len())
        .map_err(|_| StorageError::ArithmeticOverflow("section partition count"))?;
    let partitions = sink
        .partitions
        .iter()
        .map(|partition| ArtifactPartition {
            sequence: partition.sequence(),
            row_count: partition.row_count(),
            stored_byte_count: partition.stored_byte_count(),
            digest: partition.digest(),
        })
        .collect::<Vec<_>>();
    let section_digest = digest_section(sink.section_id, &partitions);
    Ok(ArtifactSection {
        section_id: sink.section_id,
        schema: sink.schema,
        schema_fingerprint: sink.fingerprint,
        stats: ArtifactSectionStats {
            row_count: sink.row_count,
            stored_byte_count: sink.stored_byte_count,
            partition_count,
        },
        partitions,
        section_digest,
    })
}

fn empty_section(section_id: ArtifactSectionId) -> Result<ArtifactSection, StorageError> {
    let schema = LogicalSchema::new(Vec::new())
        .map_err(|_| StorageError::InvalidDraft("empty artifact schema is invalid"))?;
    let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
        .map_err(|_| StorageError::InvalidDraft("empty artifact fingerprint failed"))?;
    Ok(ArtifactSection {
        section_id,
        schema,
        schema_fingerprint: fingerprint,
        stats: ArtifactSectionStats {
            row_count: 0,
            stored_byte_count: 0,
            partition_count: 0,
        },
        partitions: Vec::new(),
        section_digest: digest_section(section_id, &[]),
    })
}

fn summary_from_sections(kind: ArtifactKind, sections: &[ArtifactSection]) -> ArtifactSummary {
    let mut summary = ArtifactSummary::default();
    for section in sections {
        summary.row_count = summary.row_count.saturating_add(section.stats.row_count);
        summary.stored_byte_count = summary
            .stored_byte_count
            .saturating_add(section.stats.stored_byte_count);
        summary.partition_count = summary
            .partition_count
            .saturating_add(section.stats.partition_count);
        match (kind, section.section_id) {
            (ArtifactKind::ValidationReport, ArtifactSectionId::ValidationFinding) => {
                summary.finding_count = section.stats.row_count;
            }
            (ArtifactKind::DeduplicationReport, ArtifactSectionId::DuplicateFinding) => {
                summary.finding_count = section.stats.row_count;
                summary.duplicate_count = section.stats.row_count;
            }
            _ => {}
        }
    }
    summary
}

fn digest_section(
    section_id: ArtifactSectionId,
    partitions: &[ArtifactPartition],
) -> ContentDigest {
    let mut bytes = Vec::from(b"stillflow.e4.section.v1\0".as_slice());
    bytes.push(section_id.tag());
    bytes.extend_from_slice(&u32::try_from(partitions.len()).unwrap_or(0).to_le_bytes());
    for partition in partitions {
        bytes.extend_from_slice(&partition.sequence.to_le_bytes());
        bytes.extend_from_slice(partition.digest.as_bytes());
    }
    digest_bytes(&bytes)
}

fn digest_manifest(
    artifact_id: Uuid,
    kind: ArtifactKind,
    sections: &[ArtifactSection],
) -> ContentDigest {
    let mut bytes = Vec::from(b"stillflow.e4.manifest.v1\0".as_slice());
    bytes.extend_from_slice(artifact_id.as_bytes());
    bytes.push(kind.tag());
    bytes.extend_from_slice(&u32::try_from(sections.len()).unwrap_or(0).to_le_bytes());
    for section in sections {
        bytes.push(section.section_id.tag());
        bytes.extend_from_slice(section.section_digest.as_bytes());
    }
    digest_bytes(&bytes)
}

fn digest_provenance(
    draft: &ArtifactProvenanceDraft,
    summary: &ArtifactSummary,
    manifest_digest: ContentDigest,
) -> [u8; 32] {
    let mut bytes = Vec::from(b"stillflow.e4.artifact-provenance.v1\0".as_slice());
    bytes.extend_from_slice(draft.input.run_id.as_bytes());
    bytes.extend_from_slice(draft.input.bundle_id.as_bytes());
    bytes.extend_from_slice(draft.input.artifact_id.as_bytes());
    bytes.push(draft.input.artifact_kind.tag());
    bytes.extend_from_slice(&draft.canonical_plan_digest);
    bytes.extend_from_slice(&summary.row_count.to_le_bytes());
    bytes.extend_from_slice(manifest_digest.as_bytes());
    *digest_bytes(&bytes).as_bytes()
}

fn install_named_partitions(
    inner: &StoreInner,
    artifact_id: Uuid,
    staging_dir: &std::path::Path,
    partitions: &[SnapshotPartition],
) -> Result<(), StorageError> {
    let final_dir = crate::store::final_snapshot_dir(inner, artifact_id);
    for partition in partitions {
        let staged = staging_dir.join(format!("{:010}.parquet", partition.sequence()));
        let final_path = crate::store::final_partition_path(inner, artifact_id, partition);
        fs::rename(staged, final_path)
            .map_err(|error| StorageError::io("install bundle partition", &error))?;
    }
    crate::store::sync_directory(&final_dir)?;
    crate::store::sync_directory(&crate::store::partitions_root(inner))
}

fn install_section_partitions(
    inner: &StoreInner,
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
    staging_dir: &std::path::Path,
    partitions: &[SnapshotPartition],
) -> Result<(), StorageError> {
    let final_dir = crate::store::final_snapshot_dir(inner, artifact_id)
        .join(format!("{:02}", section_id.tag()));
    if !partitions.is_empty() {
        fs::create_dir_all(&final_dir)
            .map_err(|error| StorageError::io("create artifact section directory", &error))?;
    }
    for partition in partitions {
        let staged = staging_dir.join(format!("{:010}.parquet", partition.sequence()));
        let final_path = final_dir.join(format!("{:010}.parquet", partition.sequence()));
        fs::rename(staged, final_path)
            .map_err(|error| StorageError::io("install artifact partition", &error))?;
    }
    Ok(())
}

fn commit_bundle_rows(
    inner: &StoreInner,
    provenance: &ArtifactProvenanceDraft,
    accepted: &SnapshotManifest,
    validation: &FinishedArtifact,
    rejected: Option<&FinishedArtifact>,
    dedup: &FinishedArtifact,
) -> Result<VerificationBundle, StorageError> {
    let accepted_summary = ArtifactSummary {
        row_count: accepted.snapshot().stats().row_count(),
        stored_byte_count: accepted.snapshot().stats().stored_byte_count(),
        partition_count: accepted.snapshot().stats().partition_count(),
        finding_count: 0,
        warning_count: 0,
        error_count: 0,
        duplicate_count: 0,
    };
    let accepted_digest =
        digest_bytes(format!("stillflow.e4.accepted.v1\0{}", accepted.snapshot().id()).as_bytes());
    let accepted_provenance = ArtifactProvenance {
        draft: with_kind(
            provenance,
            ArtifactKind::AcceptedSnapshot,
            accepted.snapshot().id(),
        ),
        summary: accepted_summary,
        content_digest: *accepted_digest.as_bytes(),
    };
    let validation_provenance = ArtifactProvenance {
        draft: with_kind(
            provenance,
            ArtifactKind::ValidationReport,
            validation.manifest.artifact_id,
        ),
        summary: validation.summary,
        content_digest: digest_provenance(
            &with_kind(
                provenance,
                ArtifactKind::ValidationReport,
                validation.manifest.artifact_id,
            ),
            &validation.summary,
            validation.manifest.manifest_digest,
        ),
    };
    let rejected_artifact = rejected.map(|finished| {
        let draft = with_kind(
            provenance,
            ArtifactKind::RejectedRows,
            finished.manifest.artifact_id,
        );
        RejectedRowsArtifact {
            manifest: finished.manifest.clone(),
            provenance: ArtifactProvenance {
                content_digest: digest_provenance(
                    &draft,
                    &finished.summary,
                    finished.manifest.manifest_digest,
                ),
                summary: finished.summary,
                draft,
            },
        }
    });
    let dedup_draft = with_kind(
        provenance,
        ArtifactKind::DeduplicationReport,
        dedup.manifest.artifact_id,
    );
    let dedup_artifact = DeduplicationReportArtifact {
        manifest: dedup.manifest.clone(),
        provenance: ArtifactProvenance {
            content_digest: digest_provenance(
                &dedup_draft,
                &dedup.summary,
                dedup.manifest.manifest_digest,
            ),
            summary: dedup.summary,
            draft: dedup_draft,
        },
    };

    let membership = VerificationBundleMembership {
        bundle_id: provenance.input.bundle_id,
        run_id: provenance.input.run_id,
        bundle_artifact_id: provenance.input.artifact_id,
        accepted_snapshot_id: accepted.snapshot().id(),
        validation_report_artifact_id: validation.manifest.artifact_id,
        rejected_rows_artifact_id: rejected.map(|item| item.manifest.artifact_id),
        deduplication_report_artifact_id: dedup.manifest.artifact_id,
    };
    let mut bundle_summary = accepted_summary
        .saturating_add(validation.summary)
        .saturating_add(dedup.summary);
    if let Some(rejected) = rejected {
        bundle_summary = bundle_summary.saturating_add(rejected.summary);
    }
    let bundle_digest = digest_bytes(
        [
            b"stillflow.e4.bundle-provenance.v1\0".as_slice(),
            provenance.input.run_id.as_bytes(),
            provenance.input.bundle_id.as_bytes(),
            provenance.input.artifact_id.as_bytes(),
            accepted.snapshot().id().as_bytes(),
        ]
        .concat()
        .as_slice(),
    );
    let bundle_provenance = ArtifactProvenance {
        draft: provenance.clone(),
        summary: bundle_summary,
        content_digest: *bundle_digest.as_bytes(),
    };

    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin verification bundle transaction"))?;
    insert_snapshot_rows(&transaction, accepted)?;
    transaction
        .execute(
            "INSERT INTO verification_bundles(
                 bundle_id, run_id, bundle_artifact_id, accepted_snapshot_id,
                 validation_report_artifact_id, rejected_rows_artifact_id,
                 deduplication_report_artifact_id, provenance_json,
                 created_at_utc, started_at_utc, committed_at_utc
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                membership.bundle_id.to_string(),
                membership.run_id.to_string(),
                membership.bundle_artifact_id.to_string(),
                membership.accepted_snapshot_id.to_string(),
                membership.validation_report_artifact_id.to_string(),
                membership
                    .rejected_rows_artifact_id
                    .map(|id| id.to_string()),
                membership.deduplication_report_artifact_id.to_string(),
                serde_json::to_string(&bundle_provenance)
                    .map_err(|_| StorageError::Serialization("encode bundle provenance"))?,
                format_timestamp(&provenance.input.created_at),
                format_timestamp(&provenance.input.started_at),
                format_timestamp(&provenance.input.committed_at),
            ],
        )
        .map_err(|_| StorageError::database("insert verification bundle"))?;
    insert_artifact_rows(
        &transaction,
        membership.bundle_id,
        &validation.manifest,
        &validation_provenance,
    )?;
    if let Some(rejected) = &rejected_artifact {
        insert_artifact_rows(
            &transaction,
            membership.bundle_id,
            &rejected.manifest,
            &rejected.provenance,
        )?;
    }
    insert_artifact_rows(
        &transaction,
        membership.bundle_id,
        &dedup_artifact.manifest,
        &dedup_artifact.provenance,
    )?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit verification bundle"))?;

    Ok(VerificationBundle {
        membership,
        provenance: bundle_provenance,
        accepted: AcceptedSnapshotArtifact {
            manifest: accepted.clone(),
            provenance: accepted_provenance,
        },
        validation_report: ValidationReportArtifact {
            manifest: validation.manifest.clone(),
            provenance: validation_provenance,
        },
        rejected_rows: rejected_artifact,
        deduplication_report: dedup_artifact,
    })
}

fn with_kind(
    provenance: &ArtifactProvenanceDraft,
    kind: ArtifactKind,
    artifact_id: Uuid,
) -> ArtifactProvenanceDraft {
    let mut draft = provenance.clone();
    draft.input.artifact_kind = kind;
    draft.input.artifact_id = artifact_id;
    draft
}

fn insert_artifact_rows(
    transaction: &rusqlite::Transaction<'_>,
    bundle_id: Uuid,
    manifest: &ArtifactManifest,
    provenance: &ArtifactProvenance,
) -> Result<(), StorageError> {
    transaction
        .execute(
            "INSERT INTO artifacts(
                 artifact_id, bundle_id, kind, version, manifest_digest, provenance_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                manifest.artifact_id.to_string(),
                bundle_id.to_string(),
                i64::from(manifest.kind.tag()),
                i64::from(manifest.version),
                manifest.manifest_digest.to_string(),
                serde_json::to_string(provenance)
                    .map_err(|_| StorageError::Serialization("encode artifact provenance"))?,
            ],
        )
        .map_err(|_| StorageError::database("insert artifact manifest"))?;
    for section in &manifest.sections {
        let schema_json = serde_json::to_string(&section.schema)
            .map_err(|_| StorageError::Serialization("encode artifact schema"))?;
        transaction
            .execute(
                "INSERT INTO artifact_sections(
                     artifact_id, section_id, schema_json, schema_fingerprint,
                     row_count, stored_byte_count, partition_count, section_digest
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    manifest.artifact_id.to_string(),
                    i64::from(section.section_id.tag()),
                    schema_json,
                    section.schema_fingerprint.to_string(),
                    crate::store::checked_i64(section.stats.row_count, "section row count")?,
                    crate::store::checked_i64(
                        section.stats.stored_byte_count,
                        "section stored bytes"
                    )?,
                    i64::from(section.stats.partition_count),
                    section.section_digest.to_string(),
                ],
            )
            .map_err(|_| StorageError::database("insert artifact section"))?;
        for partition in &section.partitions {
            transaction
                .execute(
                    "INSERT INTO artifact_partitions(
                         artifact_id, section_id, sequence, row_count, stored_byte_count, sha256
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        manifest.artifact_id.to_string(),
                        i64::from(section.section_id.tag()),
                        i64::from(partition.sequence),
                        crate::store::checked_i64(partition.row_count, "artifact partition rows")?,
                        crate::store::checked_i64(
                            partition.stored_byte_count,
                            "artifact partition bytes"
                        )?,
                        partition.digest.to_string(),
                    ],
                )
                .map_err(|_| StorageError::database("insert artifact partition"))?;
        }
    }
    Ok(())
}

fn load_bundle(
    inner: &StoreInner,
    column: &'static str,
    id: Uuid,
) -> Result<VerificationBundle, StorageError> {
    type BundleRow = (
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
    );
    let sql = format!(
        "SELECT bundle_id, run_id, bundle_artifact_id, accepted_snapshot_id,
                validation_report_artifact_id, rejected_rows_artifact_id,
                deduplication_report_artifact_id, provenance_json
         FROM verification_bundles WHERE {column} = ?1"
    );
    let connection = open_connection(inner)?;
    let row: Option<BundleRow> = connection
        .query_row(&sql, params![id.to_string()], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })
        .optional()
        .map_err(|_| StorageError::database("load verification bundle"))?;
    let Some(row) = row else {
        return Err(StorageError::NotFound(id));
    };
    let membership = VerificationBundleMembership {
        bundle_id: parse_uuid(&row.0, "bundle id")?,
        run_id: parse_uuid(&row.1, "run id")?,
        bundle_artifact_id: parse_uuid(&row.2, "bundle artifact id")?,
        accepted_snapshot_id: parse_uuid(&row.3, "accepted snapshot id")?,
        validation_report_artifact_id: parse_uuid(&row.4, "validation artifact id")?,
        rejected_rows_artifact_id: row
            .5
            .as_deref()
            .map(|value| parse_uuid(value, "rejected artifact id"))
            .transpose()?,
        deduplication_report_artifact_id: parse_uuid(&row.6, "dedup artifact id")?,
    };
    let provenance: ArtifactProvenance = serde_json::from_str(&row.7)
        .map_err(|_| StorageError::Serialization("decode bundle provenance"))?;
    let accepted_manifest = load_manifest_inner(inner, membership.accepted_snapshot_id)?;
    let validation = load_artifact(
        inner,
        membership.bundle_id,
        membership.validation_report_artifact_id,
    )?;
    let rejected_rows = membership
        .rejected_rows_artifact_id
        .map(|artifact_id| load_artifact(inner, membership.bundle_id, artifact_id))
        .transpose()?
        .map(|loaded| RejectedRowsArtifact {
            manifest: loaded.0,
            provenance: loaded.1,
        });
    let dedup = load_artifact(
        inner,
        membership.bundle_id,
        membership.deduplication_report_artifact_id,
    )?;
    let accepted_digest = digest_bytes(
        format!(
            "stillflow.e4.accepted.v1\0{}",
            accepted_manifest.snapshot().id()
        )
        .as_bytes(),
    );
    Ok(VerificationBundle {
        membership: membership.clone(),
        provenance,
        accepted: AcceptedSnapshotArtifact {
            manifest: accepted_manifest.clone(),
            provenance: ArtifactProvenance {
                draft: with_kind(
                    &accepted_manifest_provenance_stub(&accepted_manifest, &membership),
                    ArtifactKind::AcceptedSnapshot,
                    membership.accepted_snapshot_id,
                ),
                summary: ArtifactSummary {
                    row_count: accepted_manifest.snapshot().stats().row_count(),
                    stored_byte_count: accepted_manifest.snapshot().stats().stored_byte_count(),
                    partition_count: accepted_manifest.snapshot().stats().partition_count(),
                    ..ArtifactSummary::default()
                },
                content_digest: *accepted_digest.as_bytes(),
            },
        },
        validation_report: ValidationReportArtifact {
            manifest: validation.0,
            provenance: validation.1,
        },
        rejected_rows,
        deduplication_report: DeduplicationReportArtifact {
            manifest: dedup.0,
            provenance: dedup.1,
        },
    })
}

fn accepted_manifest_provenance_stub(
    accepted: &SnapshotManifest,
    membership: &VerificationBundleMembership,
) -> ArtifactProvenanceDraft {
    ArtifactProvenanceDraft {
        input: stillflow_core::ArtifactProvenanceInput {
            run_id: membership.run_id,
            bundle_id: membership.bundle_id,
            artifact_id: membership.accepted_snapshot_id,
            artifact_kind: ArtifactKind::AcceptedSnapshot,
            session_id: accepted.snapshot().session_id(),
            input: stillflow_core::LogicalInputRef {
                input: stillflow_core::InputRef::Asset {
                    asset_id: accepted.snapshot().source_asset_id(),
                },
                version_digest: [0; 32],
            },
            lineage: accepted.snapshot().lineage().clone(),
            created_at: *accepted.snapshot().created_at(),
            started_at: *accepted.snapshot().created_at(),
            committed_at: *accepted.snapshot().created_at(),
        },
        plan_fingerprint: [0; 32],
        canonical_plan_digest: [0; 32],
        engine_contract_version: 1,
        engine_build: "experimental".to_owned(),
        verification_contract_version: 1,
    }
}

fn load_artifact(
    inner: &StoreInner,
    bundle_id: Uuid,
    artifact_id: Uuid,
) -> Result<(ArtifactManifest, ArtifactProvenance), StorageError> {
    let connection = open_connection(inner)?;
    let raw: Option<(i64, i64, String, String)> = connection
        .query_row(
            "SELECT kind, version, manifest_digest, provenance_json
             FROM artifacts WHERE artifact_id = ?1 AND bundle_id = ?2",
            params![artifact_id.to_string(), bundle_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("load artifact"))?;
    let Some((kind, version, digest, provenance_json)) = raw else {
        return Err(StorageError::NotFound(artifact_id));
    };
    let kind = match u8::try_from(kind) {
        Ok(0x03) => ArtifactKind::ValidationReport,
        Ok(0x04) => ArtifactKind::RejectedRows,
        Ok(0x05) => ArtifactKind::DeduplicationReport,
        _ => return Err(StorageError::InvalidManifest("unknown artifact kind")),
    };
    let provenance: ArtifactProvenance = serde_json::from_str(&provenance_json)
        .map_err(|_| StorageError::Serialization("decode artifact provenance"))?;
    let mut sections = Vec::new();
    let mut stmt = connection
        .prepare(
            "SELECT section_id, schema_json, schema_fingerprint, row_count,
                    stored_byte_count, partition_count, section_digest
             FROM artifact_sections WHERE artifact_id = ?1 ORDER BY section_id",
        )
        .map_err(|_| StorageError::database("prepare artifact sections"))?;
    let rows = stmt
        .query_map(params![artifact_id.to_string()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
            ))
        })
        .map_err(|_| StorageError::database("query artifact sections"))?;
    for row in rows {
        let (section_tag, schema_json, fingerprint_hex, row_count, stored, partition_count, digest) =
            row.map_err(|_| StorageError::database("read artifact section"))?;
        let section_id = ArtifactSectionId::from_tag(section_tag)?;
        let schema: LogicalSchema = serde_json::from_str(&schema_json)
            .map_err(|_| StorageError::Serialization("decode artifact schema"))?;
        let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
            .map_err(|_| StorageError::InvalidManifest("artifact schema fingerprint"))?;
        if fingerprint.to_string() != fingerprint_hex {
            return Err(StorageError::InvalidManifest(
                "artifact schema fingerprint mismatch",
            ));
        }
        let partitions = load_artifact_partitions(&connection, artifact_id, section_id)?;
        sections.push(ArtifactSection {
            section_id,
            schema,
            schema_fingerprint: fingerprint,
            stats: ArtifactSectionStats {
                row_count: u64::try_from(row_count)
                    .map_err(|_| StorageError::InvalidManifest("section row count"))?,
                stored_byte_count: u64::try_from(stored)
                    .map_err(|_| StorageError::InvalidManifest("section stored bytes"))?,
                partition_count: u32::try_from(partition_count)
                    .map_err(|_| StorageError::InvalidManifest("section partition count"))?,
            },
            partitions,
            section_digest: ContentDigest::try_from_hex(&digest)?,
        });
    }
    Ok((
        ArtifactManifest {
            version: u16::try_from(version)
                .map_err(|_| StorageError::InvalidManifest("artifact version"))?,
            artifact_id,
            kind,
            sections,
            manifest_digest: ContentDigest::try_from_hex(&digest)?,
        },
        provenance,
    ))
}

fn load_artifact_partitions(
    connection: &rusqlite::Connection,
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
) -> Result<Vec<ArtifactPartition>, StorageError> {
    let mut stmt = connection
        .prepare(
            "SELECT sequence, row_count, stored_byte_count, sha256
             FROM artifact_partitions
             WHERE artifact_id = ?1 AND section_id = ?2
             ORDER BY sequence",
        )
        .map_err(|_| StorageError::database("prepare artifact partitions"))?;
    let rows = stmt
        .query_map(
            params![artifact_id.to_string(), i64::from(section_id.tag())],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .map_err(|_| StorageError::database("query artifact partitions"))?;
    let mut partitions = Vec::new();
    for row in rows {
        let (sequence, row_count, stored, digest) =
            row.map_err(|_| StorageError::database("read artifact partition"))?;
        partitions.push(ArtifactPartition {
            sequence: u32::try_from(sequence)
                .map_err(|_| StorageError::InvalidManifest("artifact partition sequence"))?,
            row_count: u64::try_from(row_count)
                .map_err(|_| StorageError::InvalidManifest("artifact partition rows"))?,
            stored_byte_count: u64::try_from(stored)
                .map_err(|_| StorageError::InvalidManifest("artifact partition bytes"))?,
            digest: ContentDigest::try_from_hex(&digest)?,
        });
    }
    Ok(partitions)
}

fn artifact_manifest_in_bundle(
    bundle: &VerificationBundle,
    artifact_id: Uuid,
) -> Result<&ArtifactManifest, StorageError> {
    if bundle.validation_report.manifest.artifact_id == artifact_id {
        return Ok(&bundle.validation_report.manifest);
    }
    if bundle.deduplication_report.manifest.artifact_id == artifact_id {
        return Ok(&bundle.deduplication_report.manifest);
    }
    if let Some(rejected) = &bundle.rejected_rows {
        if rejected.manifest.artifact_id == artifact_id {
            return Ok(&rejected.manifest);
        }
    }
    Err(StorageError::NotFound(artifact_id))
}

fn read_artifact_partition(
    inner: &StoreInner,
    artifact_id: Uuid,
    section: &ArtifactSection,
    partition: &ArtifactPartition,
) -> Result<BatchEnvelope, StorageError> {
    use std::fs::File;
    use std::io::{Seek, SeekFrom};
    use std::sync::Arc as StdArc;

    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};

    use stillflow_core::{logical_schema_to_arrow, BatchEnvelopeFactory, MAX_BATCH_ROWS};

    let path = crate::store::final_snapshot_dir(inner, artifact_id)
        .join(format!("{:02}", section.section_id.tag()))
        .join(format!("{:010}.parquet", partition.sequence));
    let mut file =
        File::open(&path).map_err(|error| StorageError::io("open artifact partition", &error))?;
    let digest = digest_file(&mut file)?;
    if digest != partition.digest {
        return Err(StorageError::Integrity {
            snapshot_id: artifact_id,
            sequence: partition.sequence,
            kind: crate::IntegrityFailure::DigestMismatch,
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| StorageError::io("rewind artifact partition", &error))?;
    let canonical_schema = logical_schema_to_arrow(&section.schema)
        .map_err(|_| StorageError::Batch("artifact schema"))?;
    let options = ArrowReaderOptions::new().with_schema(StdArc::clone(&canonical_schema));
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .map_err(|_| StorageError::parquet("open artifact parquet"))?;
    let mut reader = builder
        .with_batch_size(MAX_BATCH_ROWS)
        .build()
        .map_err(|_| StorageError::parquet("build artifact reader"))?;
    let batch = reader
        .next()
        .ok_or(StorageError::parquet("missing artifact batch"))?
        .map_err(|_| StorageError::parquet("read artifact batch"))?;
    let factory = BatchEnvelopeFactory::try_new(Arc::new(section.schema.clone()), artifact_id)
        .map_err(|_| StorageError::Batch("artifact envelope factory"))?;
    factory
        .try_build(u64::from(partition.sequence), batch)
        .map_err(|_| StorageError::Batch("artifact envelope"))
}
