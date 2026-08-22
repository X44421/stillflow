//! E4 artifact manifests, sections, partitions, and frozen digest preimages.
//!
//! Implements the storage-owned half of contract section 8 (`ArtifactManifest`,
//! `ArtifactSection`, `ArtifactSectionStats`, `ArtifactPartition`) and the
//! byte-exact digest formulas of section 8.1.1. Digest preimages follow one
//! documented convention derived from that section: multi-byte integers are
//! little-endian, UUIDs are their raw 16 `Uuid::as_bytes()` bytes, enum tags
//! are fixed `u8` values, domain prefixes are ASCII bytes followed by `0x00`,
//! and every embedded byte string (schema descriptors, fingerprints, digests,
//! canonical batch bytes) is emitted as a `u32` little-endian length followed
//! by the exact bytes.

use std::fmt;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use stillflow_core::{
    ArtifactKind, ColumnId, DatasetSnapshot, LogicalField, LogicalSchema, LogicalSchemaFingerprint,
    LogicalType, REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID, REJECTED_INPUT_ID_COLUMN_ID,
    REJECTED_INPUT_KIND_COLUMN_ID, REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID,
    REJECTED_KIND_COLUMN_ID, REJECTED_NODE_ID_COLUMN_ID, REJECTED_PLAN_FINGERPRINT_COLUMN_ID,
    REJECTED_RULE_ORDINAL_COLUMN_ID, REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID,
};

use crate::{ContentDigest, StorageError, MAX_SNAPSHOT_PARTITIONS};

/// Frozen report pack row limit per partition-sized envelope (contract 11).
pub const REPORT_PACK_ROWS: usize = 1_024;
/// Frozen report pack byte limit (contract 11).
pub const REPORT_PACK_BYTES: usize = 2 * 1024 * 1024;
/// Per-report-artifact partition ceiling applied after section aggregation
/// (contract 8.1.1 and 14).
pub const MAX_REPORT_PARTITIONS: u32 = MAX_SNAPSHOT_PARTITIONS;
/// Per-report-artifact row ceiling: `MAX_REPORT_PARTITIONS * REPORT_PACK_ROWS`
/// (contract 8.1.1 and 14).
pub const MAX_REPORT_ROWS: u64 = MAX_REPORT_PARTITIONS as u64 * REPORT_PACK_ROWS as u64;
/// Per-report-artifact stored-byte ceiling
/// (`MAX_REPORT_PARTITIONS * REPORT_PACK_BYTES`, contract 14).
pub const MAX_REPORT_BYTES: u64 = MAX_REPORT_PARTITIONS as u64 * REPORT_PACK_BYTES as u64;
/// Bundle-wide ceiling across both always-present report artifacts
/// (contract 8.1.1).
pub const MAX_BUNDLE_REPORT_PARTITIONS: u32 = 2 * MAX_REPORT_PARTITIONS;
/// Bundle-wide report row ceiling (contract 8.1.1).
pub const MAX_BUNDLE_REPORT_ROWS: u64 = 2 * MAX_REPORT_ROWS;
/// Bundle-wide report byte ceiling (contract 8.1.1).
pub const MAX_BUNDLE_REPORT_BYTES: u64 = 2 * MAX_REPORT_BYTES;

pub(crate) const PARTITION_DOMAIN: &str = "stillflow.e4.partition.v1";
pub(crate) const SECTION_DOMAIN: &str = "stillflow.e4.section.v1";
pub(crate) const MANIFEST_DOMAIN: &str = "stillflow.e4.manifest.v1";
pub(crate) const ARTIFACT_PROVENANCE_DOMAIN: &str = "stillflow.e4.artifact-provenance.v1";
pub(crate) const BUNDLE_PROVENANCE_DOMAIN: &str = "stillflow.e4.bundle-provenance.v1";
pub(crate) const ACCEPTED_SNAPSHOT_DOMAIN: &str = "stillflow.e4.accepted-snapshot.v1";

/// Version of the storage-owned artifact manifest structure.
pub const ARTIFACT_MANIFEST_VERSION: u16 = 1;

/// One report/rejected section of an [`ArtifactManifest`] (contract 8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArtifactSectionId {
    ValidationRuleSummary,
    ValidationFinding,
    RejectedRows,
    DedupRuleSummary,
    DuplicateFinding,
}

impl ArtifactSectionId {
    /// Fixed digest-input tag from contract section 8.1.1.
    pub const fn tag(self) -> u8 {
        match self {
            Self::ValidationRuleSummary => 0x01,
            Self::ValidationFinding => 0x02,
            Self::RejectedRows => 0x03,
            Self::DedupRuleSummary => 0x04,
            Self::DuplicateFinding => 0x05,
        }
    }

    pub fn try_from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::ValidationRuleSummary),
            0x02 => Some(Self::ValidationFinding),
            0x03 => Some(Self::RejectedRows),
            0x04 => Some(Self::DedupRuleSummary),
            0x05 => Some(Self::DuplicateFinding),
            _ => None,
        }
    }
}

impl fmt::Display for ArtifactSectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ValidationRuleSummary => "validation-rule-summary",
            Self::ValidationFinding => "validation-finding",
            Self::RejectedRows => "rejected-rows",
            Self::DedupRuleSummary => "dedup-rule-summary",
            Self::DuplicateFinding => "duplicate-finding",
        };
        formatter.write_str(name)
    }
}

impl Serialize for ArtifactSectionId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ArtifactSectionId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "validation-rule-summary" => Ok(Self::ValidationRuleSummary),
            "validation-finding" => Ok(Self::ValidationFinding),
            "rejected-rows" => Ok(Self::RejectedRows),
            "dedup-rule-summary" => Ok(Self::DedupRuleSummary),
            "duplicate-finding" => Ok(Self::DuplicateFinding),
            other => Err(DeError::custom(format!(
                "unknown artifact section id {other}"
            ))),
        }
    }
}

/// One immutable physical partition of an artifact section (contract 8.1).
///
/// `stored_byte_count` is the canonical logical payload byte count of the
/// partition's batches, not a filesystem allocation or Parquet footer size
/// (contract 8.1.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPartition {
    pub(crate) sequence: u32,
    pub(crate) row_count: u64,
    pub(crate) stored_byte_count: u64,
    pub(crate) digest: ContentDigest,
}

impl ArtifactPartition {
    pub(crate) fn try_new(
        sequence: u32,
        row_count: u64,
        stored_byte_count: u64,
        digest: ContentDigest,
    ) -> Result<Self, StorageError> {
        if row_count == 0 || stored_byte_count == 0 {
            return Err(StorageError::InvalidManifest(
                "artifact partitions must be non-empty",
            ));
        }
        Ok(Self {
            sequence,
            row_count,
            stored_byte_count,
            digest,
        })
    }

    pub const fn sequence(&self) -> u32 {
        self.sequence
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn stored_byte_count(&self) -> u64 {
        self.stored_byte_count
    }

    pub const fn digest(&self) -> ContentDigest {
        self.digest
    }
}

/// Aggregated statistics over one artifact section (contract 8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSectionStats {
    pub(crate) row_count: u64,
    pub(crate) stored_byte_count: u64,
    pub(crate) partition_count: u32,
}

impl ArtifactSectionStats {
    pub(crate) fn try_from_partitions(
        partitions: &[ArtifactPartition],
    ) -> Result<Self, StorageError> {
        let mut row_count = 0_u64;
        let mut stored_byte_count = 0_u64;
        for partition in partitions {
            row_count = row_count
                .checked_add(partition.row_count)
                .ok_or(StorageError::ArithmeticOverflow("section row count"))?;
            stored_byte_count = stored_byte_count
                .checked_add(partition.stored_byte_count)
                .ok_or(StorageError::ArithmeticOverflow(
                    "section stored byte count",
                ))?;
        }
        let partition_count = u32::try_from(partitions.len())
            .map_err(|_| StorageError::ArithmeticOverflow("section partition count"))?;
        Ok(Self {
            row_count,
            stored_byte_count,
            partition_count,
        })
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn stored_byte_count(&self) -> u64 {
        self.stored_byte_count
    }

    pub const fn partition_count(&self) -> u32 {
        self.partition_count
    }
}

/// One section of an [`ArtifactManifest`] (contract 8.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSection {
    pub(crate) section_id: ArtifactSectionId,
    pub(crate) schema: LogicalSchema,
    pub(crate) schema_fingerprint: LogicalSchemaFingerprint,
    pub(crate) stats: ArtifactSectionStats,
    pub(crate) partitions: Vec<ArtifactPartition>,
    pub(crate) section_digest: ContentDigest,
}

impl ArtifactSection {
    pub const fn section_id(&self) -> ArtifactSectionId {
        self.section_id
    }

    pub const fn schema(&self) -> &LogicalSchema {
        &self.schema
    }

    pub const fn schema_fingerprint(&self) -> LogicalSchemaFingerprint {
        self.schema_fingerprint
    }

    pub const fn stats(&self) -> &ArtifactSectionStats {
        &self.stats
    }

    pub fn partitions(&self) -> &[ArtifactPartition] {
        &self.partitions
    }

    pub const fn section_digest(&self) -> ContentDigest {
        self.section_digest
    }
}

/// Storage-owned manifest for report and rejected artifacts (contract 8.1).
///
/// The accepted snapshot keeps the existing `SnapshotManifest` and never
/// receives an `ArtifactManifest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub(crate) version: u16,
    pub(crate) artifact_id: Uuid,
    pub(crate) kind: ArtifactKindForSerde,
    pub(crate) sections: Vec<ArtifactSection>,
    pub(crate) manifest_digest: ContentDigest,
}

/// Serde helper so `ArtifactManifest` JSON uses the frozen kebab-case tags
/// while Rust code sees the core `ArtifactKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArtifactKindForSerde(pub(crate) stillflow_core::ArtifactKind);

impl Serialize for ArtifactKindForSerde {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let name = match self.0 {
            stillflow_core::ArtifactKind::VerificationBundle => "verification-bundle",
            stillflow_core::ArtifactKind::AcceptedSnapshot => "accepted-snapshot",
            stillflow_core::ArtifactKind::ValidationReport => "validation-report",
            stillflow_core::ArtifactKind::RejectedRows => "rejected-rows",
            stillflow_core::ArtifactKind::DeduplicationReport => "deduplication-report",
        };
        serializer.serialize_str(name)
    }
}

impl<'de> Deserialize<'de> for ArtifactKindForSerde {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let kind = match value.as_str() {
            "verification-bundle" => stillflow_core::ArtifactKind::VerificationBundle,
            "accepted-snapshot" => stillflow_core::ArtifactKind::AcceptedSnapshot,
            "validation-report" => stillflow_core::ArtifactKind::ValidationReport,
            "rejected-rows" => stillflow_core::ArtifactKind::RejectedRows,
            "deduplication-report" => stillflow_core::ArtifactKind::DeduplicationReport,
            other => {
                return Err(DeError::custom(format!("unknown artifact kind {other}")));
            }
        };
        Ok(Self(kind))
    }
}

impl ArtifactManifest {
    /// Assembles a manifest from canonically ordered sections.
    ///
    /// Section digests are recomputed and verified, section statistics must
    /// equal the sums of their partitions, duplicate section ids are refused,
    /// and the section vector is stored in the fixed tag order.
    pub(crate) fn try_new(
        artifact_id: Uuid,
        kind: stillflow_core::ArtifactKind,
        mut sections: Vec<ArtifactSection>,
    ) -> Result<Self, StorageError> {
        sections.sort_by_key(|section| section.section_id.tag());
        let mut previous_tag: Option<u8> = None;
        for section in &sections {
            let tag = section.section_id.tag();
            if previous_tag == Some(tag) {
                return Err(StorageError::InvalidManifest(
                    "artifact manifest contains a duplicate section id",
                ));
            }
            previous_tag = Some(tag);

            let expected_stats = ArtifactSectionStats::try_from_partitions(&section.partitions)?;
            if section.stats != expected_stats {
                return Err(StorageError::InvalidManifest(
                    "artifact section statistics do not match its partitions",
                ));
            }
            let expected_digest = compute_section_digest(
                artifact_id,
                section.section_id,
                &section.schema,
                section.schema_fingerprint.as_bytes(),
                &section.stats,
                &section.partitions,
            )?;
            if expected_digest != section.section_digest {
                return Err(StorageError::InvalidManifest(
                    "artifact section digest mismatch",
                ));
            }
        }

        let manifest_digest =
            compute_manifest_digest(ARTIFACT_MANIFEST_VERSION, artifact_id, kind, &sections)?;
        Ok(Self {
            version: ARTIFACT_MANIFEST_VERSION,
            artifact_id,
            kind: ArtifactKindForSerde(kind),
            sections,
            manifest_digest,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub const fn kind(&self) -> stillflow_core::ArtifactKind {
        self.kind.0
    }

    pub fn sections(&self) -> &[ArtifactSection] {
        &self.sections
    }

    pub const fn manifest_digest(&self) -> ContentDigest {
        self.manifest_digest
    }

    pub(crate) fn section(&self, section_id: ArtifactSectionId) -> Option<&ArtifactSection> {
        self.sections
            .iter()
            .find(|section| section.section_id == section_id)
    }
}

/// Incremental builder for one frozen section 8.1.1 digest preimage.
pub(crate) struct Preimage {
    hasher: Sha256,
}

impl Preimage {
    pub(crate) fn new(domain: &'static str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain.as_bytes());
        hasher.update([0x00]);
        Self { hasher }
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.hasher.update([value]);
    }

    pub(crate) fn u16(&mut self, value: u16) {
        self.hasher.update(value.to_le_bytes());
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.hasher.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.hasher.update(value.to_le_bytes());
    }

    pub(crate) fn uuid(&mut self, value: Uuid) {
        self.hasher.update(value.as_bytes());
    }

    /// Emits a byte string with its mandatory `u32` little-endian length.
    pub(crate) fn len_bytes(&mut self, value: &[u8]) {
        self.u32(u32::try_from(value.len()).unwrap_or(u32::MAX));
        self.hasher.update(value);
    }

    /// Emits a storage `ContentDigest` as a length-prefixed byte string.
    pub(crate) fn digest(&mut self, value: &ContentDigest) {
        self.len_bytes(value.as_bytes());
    }

    /// Emits a raw `[u8; 32]` digest (core provenance identity) as a
    /// length-prefixed byte string.
    pub(crate) fn digest_bytes(&mut self, value: &[u8; 32]) {
        self.len_bytes(value);
    }

    pub(crate) fn finalize(self) -> ContentDigest {
        ContentDigest::from_bytes(self.hasher.finalize().into())
    }
}

/// Computes the complete Arrow IPC record-batch message body
/// (`canonical_batch_bytes`, contract 8.1.1): the Message flatbuffer metadata
/// block plus its body, little-endian, uncompressed, default alignment, with
/// no stream framing, transport headers, or end-of-stream markers.
pub(crate) fn canonical_batch_bytes(batch: &RecordBatch) -> Result<Vec<u8>, StorageError> {
    const CONTINUATION_MARKER: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
            .map_err(|_| StorageError::Serialization("initialize Arrow IPC stream"))?;
        // The schema message is written during `try_new`; everything this
        // writer appends afterwards is the single record-batch message.
        writer
            .write(batch)
            .map_err(|_| StorageError::Serialization("encode Arrow IPC record batch"))?;
        // Dropping without `finish()` omits the end-of-stream marker.
    }
    let schema_message_end = {
        // Locate the boundary between schema message and batch message by
        // re-walking the encapsulated stream header layout.
        if buffer.len() < 8 || buffer[0..4] != [0xFF, 0xFF, 0xFF, 0xFF] {
            return Err(StorageError::Serialization(
                "unexpected Arrow IPC schema framing",
            ));
        }
        let schema_metadata_len =
            u32::from_le_bytes(buffer[4..8].try_into().expect("fixed-size slice")) as usize;
        // The metadata block is padded so the body starts on an 8-byte
        // boundary.
        8 + schema_metadata_len.div_ceil(8) * 8
    };
    let message_start = schema_message_end;
    let message = buffer
        .get(message_start..)
        .ok_or(StorageError::Serialization(
            "Arrow IPC record-batch message is missing",
        ))?;
    if message.len() < 8 || message[0..4] != CONTINUATION_MARKER {
        return Err(StorageError::Serialization(
            "unexpected Arrow IPC record-batch framing",
        ));
    }
    // Strip the encapsulation header (continuation marker plus message-length
    // prefix); the remainder is the flatbuffer metadata block, its alignment
    // padding, and the body.
    Ok(message[8..].to_vec())
}

/// Partition digest formula (contract 8.1.1), generalized over the
/// section-slot byte. Report and rejected artifacts pass their frozen
/// `ArtifactSectionId` tag; the accepted snapshot passes the
/// `ArtifactKind::AcceptedSnapshot` tag (0x02) because it owns no section.
pub(crate) fn compute_partition_digest_with_tag(
    artifact_id: Uuid,
    section_tag: u8,
    sequence: u32,
    row_count: u64,
    stored_byte_count: u64,
    canonical_batches: &[Vec<u8>],
) -> ContentDigest {
    let batch_count = u32::try_from(canonical_batches.len()).unwrap_or(u32::MAX);
    let mut preimage = Preimage::new(PARTITION_DOMAIN);
    preimage.uuid(artifact_id);
    preimage.u8(section_tag);
    preimage.u32(sequence);
    preimage.u64(row_count);
    preimage.u64(stored_byte_count);
    preimage.u32(batch_count);
    for batch in canonical_batches {
        preimage.len_bytes(batch);
    }
    preimage.finalize()
}

/// Partition digest formula (contract 8.1.1).
pub(crate) fn compute_partition_digest(
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
    sequence: u32,
    row_count: u64,
    stored_byte_count: u64,
    canonical_batches: &[Vec<u8>],
) -> ContentDigest {
    compute_partition_digest_with_tag(
        artifact_id,
        section_id.tag(),
        sequence,
        row_count,
        stored_byte_count,
        canonical_batches,
    )
}

/// Section digest formula (contract 8.1.1); partitions must be sorted by
/// strictly increasing sequence.
pub(crate) fn compute_section_digest(
    artifact_id: Uuid,
    section_id: ArtifactSectionId,
    schema: &LogicalSchema,
    schema_fingerprint: &[u8; 32],
    stats: &ArtifactSectionStats,
    partitions: &[ArtifactPartition],
) -> Result<ContentDigest, StorageError> {
    let schema_bytes = schema.canonical_bytes().map_err(|_| {
        StorageError::InvalidManifest("logical schema cannot be canonically encoded")
    })?;
    let mut preimage = Preimage::new(SECTION_DOMAIN);
    preimage.uuid(artifact_id);
    preimage.u8(section_id.tag());
    preimage.len_bytes(&schema_bytes);
    preimage.digest_bytes(schema_fingerprint);
    preimage.u64(stats.row_count);
    preimage.u64(stats.stored_byte_count);
    preimage.u32(stats.partition_count);
    let mut previous_sequence: Option<u32> = None;
    for partition in partitions {
        if let Some(previous) = previous_sequence {
            if partition.sequence <= previous {
                return Err(StorageError::InvalidManifest(
                    "artifact partitions are not sorted by strictly increasing sequence",
                ));
            }
        }
        previous_sequence = Some(partition.sequence);
        preimage.u32(partition.sequence);
        preimage.u64(partition.row_count);
        preimage.u64(partition.stored_byte_count);
        preimage.digest(&partition.digest);
    }
    Ok(preimage.finalize())
}

/// Manifest digest formula (contract 8.1.1); the `manifest_digest` field
/// itself is excluded from the preimage.
pub(crate) fn compute_manifest_digest(
    version: u16,
    artifact_id: Uuid,
    kind: stillflow_core::ArtifactKind,
    sections: &[ArtifactSection],
) -> Result<ContentDigest, StorageError> {
    let mut preimage = Preimage::new(MANIFEST_DOMAIN);
    preimage.u16(version);
    preimage.uuid(artifact_id);
    preimage.u8(kind.tag());
    let section_count = u32::try_from(sections.len())
        .map_err(|_| StorageError::ArithmeticOverflow("manifest section count"))?;
    preimage.u32(section_count);
    for section in sections {
        preimage.u8(section.section_id.tag());
        let schema_bytes = section.schema.canonical_bytes().map_err(|_| {
            StorageError::InvalidManifest("logical schema cannot be canonically encoded")
        })?;
        preimage.len_bytes(&schema_bytes);
        preimage.digest_bytes(section.schema_fingerprint.as_bytes());
        preimage.u64(section.stats.row_count);
        preimage.u64(section.stats.stored_byte_count);
        preimage.u32(section.stats.partition_count);
        preimage.digest(&section.section_digest);
    }
    Ok(preimage.finalize())
}

/// Committed provenance content digest for report and rejected artifacts
/// (contract 8.1.1). The parameter list mirrors the frozen preimage order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_artifact_provenance_digest(
    run_id: Uuid,
    bundle_id: Uuid,
    artifact_id: Uuid,
    kind: stillflow_core::ArtifactKind,
    canonical_plan_digest: &[u8; 32],
    input_version_digest: &[u8; 32],
    sections: &[ArtifactSection],
    manifest_digest: &ContentDigest,
) -> Result<[u8; 32], StorageError> {
    let mut preimage = Preimage::new(ARTIFACT_PROVENANCE_DOMAIN);
    preimage.uuid(run_id);
    preimage.uuid(bundle_id);
    preimage.uuid(artifact_id);
    preimage.u8(kind.tag());
    preimage.digest_bytes(canonical_plan_digest);
    preimage.digest_bytes(input_version_digest);
    let section_count = u32::try_from(sections.len())
        .map_err(|_| StorageError::ArithmeticOverflow("provenance section count"))?;
    preimage.u32(section_count);
    for section in sections {
        preimage.u8(section.section_id.tag());
        preimage.digest(&section.section_digest);
    }
    preimage.digest(manifest_digest);
    Ok(*preimage.finalize().as_bytes())
}

/// One accepted snapshot partition's LOGICAL digest inputs (contract 8.1.1):
/// the canonical Arrow IPC batch bytes replace the physical Parquet file as
/// the digest domain, and `stored_byte_count` is the canonical logical
/// payload byte count, not the Parquet file length. Physical file digests and
/// lengths remain in the E3 `SnapshotPartition`/`SnapshotManifest` values and
/// stay fully separate from these logical values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcceptedCanonicalPartition {
    pub sequence: u32,
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub digest: ContentDigest,
}

/// Computes one accepted partition's logical digest with the frozen
/// partition formula. The accepted snapshot owns no `ArtifactSectionId`, so
/// the section-slot byte carries the `ArtifactKind::AcceptedSnapshot` tag
/// (0x02) and the artifact slot carries the snapshot id; this is the literal
/// application of contract 8.1.1 line "the same `ArtifactPartition.digest`
/// formula used for report artifacts, applied to each accepted
/// `SnapshotPartition`".
pub(crate) fn accepted_partition_canonical_digest(
    snapshot_id: Uuid,
    sequence: u32,
    row_count: u64,
    stored_byte_count: u64,
    canonical_batches: &[Vec<u8>],
) -> ContentDigest {
    compute_partition_digest_with_tag(
        snapshot_id,
        ArtifactKind::AcceptedSnapshot.tag(),
        sequence,
        row_count,
        stored_byte_count,
        canonical_batches,
    )
}

/// Accepted-snapshot manifest digest (contract 8.1.1), computed from the
/// committed snapshot identity and the LOGICAL per-partition canonical
/// records. Parquet compression, footer, and writer configuration cannot
/// influence this digest; only decoded logical batch bytes can.
pub(crate) fn accepted_snapshot_manifest_digest(
    snapshot: &DatasetSnapshot,
    partitions: &[AcceptedCanonicalPartition],
) -> Result<[u8; 32], StorageError> {
    let mut preimage = Preimage::new(ACCEPTED_SNAPSHOT_DOMAIN);
    preimage.uuid(snapshot.id());
    preimage.uuid(snapshot.dataset_id());
    preimage.uuid(snapshot.session_id());
    preimage.uuid(snapshot.source_asset_id());
    preimage.digest_bytes(snapshot.schema_fingerprint().as_bytes());
    let mut row_count: u64 = 0;
    let mut stored_byte_count: u64 = 0;
    let mut previous_sequence: Option<u32> = None;
    for partition in partitions {
        if let Some(previous) = previous_sequence {
            if partition.sequence <= previous {
                return Err(StorageError::InvalidManifest(
                    "snapshot partitions are not sorted by strictly increasing sequence",
                ));
            }
        }
        previous_sequence = Some(partition.sequence);
        row_count = row_count
            .checked_add(partition.row_count)
            .ok_or(StorageError::ArithmeticOverflow("snapshot row count"))?;
        stored_byte_count = stored_byte_count
            .checked_add(partition.stored_byte_count)
            .ok_or(StorageError::ArithmeticOverflow(
                "snapshot stored byte count",
            ))?;
    }
    preimage.u64(row_count);
    preimage.u64(stored_byte_count);
    let partition_count = u32::try_from(partitions.len())
        .map_err(|_| StorageError::ArithmeticOverflow("snapshot partition count"))?;
    preimage.u32(partition_count);
    for partition in partitions {
        preimage.u32(partition.sequence);
        preimage.u64(partition.row_count);
        preimage.u64(partition.stored_byte_count);
        preimage.digest(&partition.digest);
    }
    Ok(*preimage.finalize().as_bytes())
}

/// Bundle-provenance content digest (contract 8.1.1). The child sequence is
/// fixed: accepted snapshot, validation report, optional rejected rows, then
/// deduplication report. The parameter list mirrors the frozen preimage.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_bundle_provenance_digest(
    run_id: Uuid,
    bundle_id: Uuid,
    bundle_artifact_id: Uuid,
    accepted_snapshot_id: Uuid,
    validation_report_artifact_id: Uuid,
    rejected_rows_artifact_id: Option<Uuid>,
    deduplication_report_artifact_id: Uuid,
    children: &[(Uuid, [u8; 32], [u8; 32])],
) -> [u8; 32] {
    let mut preimage = Preimage::new(BUNDLE_PROVENANCE_DOMAIN);
    preimage.uuid(run_id);
    preimage.uuid(bundle_id);
    preimage.uuid(bundle_artifact_id);
    preimage.uuid(accepted_snapshot_id);
    preimage.uuid(validation_report_artifact_id);
    match rejected_rows_artifact_id {
        None => preimage.u8(0x00),
        Some(id) => {
            preimage.u8(0x01);
            preimage.uuid(id);
        }
    }
    preimage.uuid(deduplication_report_artifact_id);
    // Contract 8.1.1 (bundle-provenance formula) defines NO child-count slot,
    // unlike every other formula whose counts are spelled inline. The child
    // sequence itself is fixed: accepted snapshot, validation report, optional
    // rejected rows, then deduplication report.
    for (child_artifact_id, child_manifest_digest, child_content_digest) in children {
        preimage.uuid(*child_artifact_id);
        preimage.digest_bytes(child_manifest_digest);
        preimage.digest_bytes(child_content_digest);
    }
    *preimage.finalize().as_bytes()
}

fn text_field(id: ColumnId, name: &str) -> LogicalField {
    LogicalField::new(id, name, LogicalType::Utf8, false).expect("frozen report field")
}

fn u32_field(id: ColumnId, name: &str) -> LogicalField {
    LogicalField::new(id, name, LogicalType::UInt32, false).expect("frozen report field")
}

fn u64_field(id: ColumnId, name: &str) -> LogicalField {
    LogicalField::new(id, name, LogicalType::UInt64, false).expect("frozen report field")
}

fn section_schema(fields: Vec<LogicalField>) -> LogicalSchema {
    LogicalSchema::new(fields).expect("frozen report section schema")
}

/// Frozen `ValidationRuleSummary` section schema (contract 8.3).
pub fn validation_rule_summary_section_schema() -> LogicalSchema {
    use stillflow_core::{
        VALIDATION_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_ERROR_COUNT_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_FAIL_COUNT_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_FALSE_COUNT_COLUMN_ID, VALIDATION_RULE_SUMMARY_INPUT_ID_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_MESSAGE_COLUMN_ID, VALIDATION_RULE_SUMMARY_NODE_ID_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_NULL_COUNT_COLUMN_ID, VALIDATION_RULE_SUMMARY_PASS_COUNT_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
        VALIDATION_RULE_SUMMARY_WARNING_COUNT_COLUMN_ID,
    };
    section_schema(vec![
        text_field(VALIDATION_RULE_SUMMARY_INPUT_KIND_COLUMN_ID, "input_kind"),
        text_field(VALIDATION_RULE_SUMMARY_INPUT_ID_COLUMN_ID, "input_id"),
        text_field(
            VALIDATION_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
            "input_version_digest",
        ),
        text_field(
            VALIDATION_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
            "plan_fingerprint",
        ),
        text_field(
            VALIDATION_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            "canonical_plan_digest",
        ),
        text_field(VALIDATION_RULE_SUMMARY_NODE_ID_COLUMN_ID, "node_id"),
        u32_field(
            VALIDATION_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
            "rule_ordinal",
        ),
        text_field(VALIDATION_RULE_SUMMARY_MESSAGE_COLUMN_ID, "message"),
        u64_field(
            VALIDATION_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
            "evaluated_count",
        ),
        u64_field(VALIDATION_RULE_SUMMARY_PASS_COUNT_COLUMN_ID, "pass_count"),
        u64_field(VALIDATION_RULE_SUMMARY_FAIL_COUNT_COLUMN_ID, "fail_count"),
        u64_field(
            VALIDATION_RULE_SUMMARY_WARNING_COUNT_COLUMN_ID,
            "warning_count",
        ),
        u64_field(VALIDATION_RULE_SUMMARY_ERROR_COUNT_COLUMN_ID, "error_count"),
        u64_field(VALIDATION_RULE_SUMMARY_NULL_COUNT_COLUMN_ID, "null_count"),
        u64_field(VALIDATION_RULE_SUMMARY_FALSE_COUNT_COLUMN_ID, "false_count"),
    ])
}

/// Frozen `ValidationFinding` section schema (contract 8.3).
pub fn validation_finding_section_schema() -> LogicalSchema {
    use stillflow_core::{
        VALIDATION_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID, VALIDATION_FINDING_INPUT_ID_COLUMN_ID,
        VALIDATION_FINDING_INPUT_KIND_COLUMN_ID, VALIDATION_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
        VALIDATION_FINDING_NODE_ID_COLUMN_ID, VALIDATION_FINDING_PLAN_FINGERPRINT_COLUMN_ID,
        VALIDATION_FINDING_PREDICATE_OUTCOME_COLUMN_ID, VALIDATION_FINDING_RULE_ORDINAL_COLUMN_ID,
        VALIDATION_FINDING_SEVERITY_COLUMN_ID, VALIDATION_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
    };
    section_schema(vec![
        text_field(VALIDATION_FINDING_INPUT_KIND_COLUMN_ID, "input_kind"),
        text_field(VALIDATION_FINDING_INPUT_ID_COLUMN_ID, "input_id"),
        text_field(
            VALIDATION_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
            "input_version_digest",
        ),
        u64_field(
            VALIDATION_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
            "source_row_ordinal",
        ),
        text_field(
            VALIDATION_FINDING_PLAN_FINGERPRINT_COLUMN_ID,
            "plan_fingerprint",
        ),
        text_field(
            VALIDATION_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            "canonical_plan_digest",
        ),
        text_field(VALIDATION_FINDING_NODE_ID_COLUMN_ID, "node_id"),
        u32_field(VALIDATION_FINDING_RULE_ORDINAL_COLUMN_ID, "rule_ordinal"),
        text_field(VALIDATION_FINDING_SEVERITY_COLUMN_ID, "severity"),
        text_field(
            VALIDATION_FINDING_PREDICATE_OUTCOME_COLUMN_ID,
            "predicate_outcome",
        ),
    ])
}

/// Frozen `RejectedRows` control suffix appended to the logical Scan output
/// schema (contract 8.4 and 8.6).
pub fn rejected_rows_control_fields() -> Vec<LogicalField> {
    vec![
        text_field(REJECTED_INPUT_KIND_COLUMN_ID, "input_kind"),
        text_field(REJECTED_INPUT_ID_COLUMN_ID, "input_id"),
        text_field(
            REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID,
            "input_version_digest",
        ),
        u64_field(REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID, "source_row_ordinal"),
        text_field(REJECTED_KIND_COLUMN_ID, "rejection_kind"),
        text_field(REJECTED_PLAN_FINGERPRINT_COLUMN_ID, "plan_fingerprint"),
        text_field(
            REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            "canonical_plan_digest",
        ),
        text_field(REJECTED_NODE_ID_COLUMN_ID, "node_id"),
        u32_field(REJECTED_RULE_ORDINAL_COLUMN_ID, "rule_ordinal"),
    ]
}

/// Frozen `RejectedRows` section schema: the exact logical Scan output fields
/// followed by the nine reserved control fields (contract 8.4).
pub fn rejected_rows_section_schema(source: &LogicalSchema) -> Result<LogicalSchema, StorageError> {
    let total = source.fields.len() + rejected_rows_control_fields().len();
    if total > stillflow_core::MAX_SCHEMA_FIELDS {
        return Err(StorageError::InvalidDraft(
            "source schema leaves no room for the rejected-row control fields",
        ));
    }
    // Source schemas must not already contain the reserved control names or
    // ids (contract 8.6); the storage boundary refuses them explicitly.
    const RESERVED_CONTROL_NAMES: [&str; 9] = [
        "input_kind",
        "input_id",
        "input_version_digest",
        "source_row_ordinal",
        "rejection_kind",
        "plan_fingerprint",
        "canonical_plan_digest",
        "node_id",
        "rule_ordinal",
    ];
    let reserved_ids = rejected_rows_control_fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    for field in &source.fields {
        if RESERVED_CONTROL_NAMES.contains(&field.name.as_str()) || reserved_ids.contains(&field.id)
        {
            return Err(StorageError::InvalidDraft(
                "source schema collides with reserved rejected-row controls",
            ));
        }
    }
    let mut fields = source.fields.clone();
    fields.extend(rejected_rows_control_fields());
    Ok(section_schema(fields))
}

/// Frozen `DedupRuleSummary` section schema (contract 8.5).
pub fn dedup_rule_summary_section_schema() -> LogicalSchema {
    use stillflow_core::{
        DEDUP_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
        DEDUP_RULE_SUMMARY_DUPLICATE_COUNT_COLUMN_ID, DEDUP_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
        DEDUP_RULE_SUMMARY_INPUT_ID_COLUMN_ID, DEDUP_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
        DEDUP_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
        DEDUP_RULE_SUMMARY_KEY_COLUMN_COUNT_COLUMN_ID, DEDUP_RULE_SUMMARY_NODE_ID_COLUMN_ID,
        DEDUP_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID, DEDUP_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
        DEDUP_RULE_SUMMARY_UNIQUE_COUNT_COLUMN_ID,
    };
    section_schema(vec![
        text_field(DEDUP_RULE_SUMMARY_INPUT_KIND_COLUMN_ID, "input_kind"),
        text_field(DEDUP_RULE_SUMMARY_INPUT_ID_COLUMN_ID, "input_id"),
        text_field(
            DEDUP_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
            "input_version_digest",
        ),
        text_field(
            DEDUP_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
            "plan_fingerprint",
        ),
        text_field(
            DEDUP_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            "canonical_plan_digest",
        ),
        text_field(DEDUP_RULE_SUMMARY_NODE_ID_COLUMN_ID, "node_id"),
        u32_field(DEDUP_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID, "rule_ordinal"),
        u32_field(
            DEDUP_RULE_SUMMARY_KEY_COLUMN_COUNT_COLUMN_ID,
            "key_column_count",
        ),
        u64_field(
            DEDUP_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
            "evaluated_count",
        ),
        u64_field(DEDUP_RULE_SUMMARY_UNIQUE_COUNT_COLUMN_ID, "unique_count"),
        u64_field(
            DEDUP_RULE_SUMMARY_DUPLICATE_COUNT_COLUMN_ID,
            "duplicate_count",
        ),
    ])
}

/// Frozen `DuplicateFinding` section schema (contract 8.5).
pub fn duplicate_finding_section_schema() -> LogicalSchema {
    use stillflow_core::{
        DUPLICATE_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
        DUPLICATE_FINDING_ENCODED_KEY_BYTE_COUNT_COLUMN_ID,
        DUPLICATE_FINDING_FIRST_SOURCE_ROW_ORDINAL_COLUMN_ID, DUPLICATE_FINDING_INPUT_ID_COLUMN_ID,
        DUPLICATE_FINDING_INPUT_KIND_COLUMN_ID, DUPLICATE_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
        DUPLICATE_FINDING_KEY_COLUMN_COUNT_COLUMN_ID, DUPLICATE_FINDING_NODE_ID_COLUMN_ID,
        DUPLICATE_FINDING_PLAN_FINGERPRINT_COLUMN_ID, DUPLICATE_FINDING_RULE_ORDINAL_COLUMN_ID,
        DUPLICATE_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
    };
    section_schema(vec![
        text_field(DUPLICATE_FINDING_INPUT_KIND_COLUMN_ID, "input_kind"),
        text_field(DUPLICATE_FINDING_INPUT_ID_COLUMN_ID, "input_id"),
        text_field(
            DUPLICATE_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
            "input_version_digest",
        ),
        u64_field(
            DUPLICATE_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
            "source_row_ordinal",
        ),
        u64_field(
            DUPLICATE_FINDING_FIRST_SOURCE_ROW_ORDINAL_COLUMN_ID,
            "first_source_row_ordinal",
        ),
        text_field(
            DUPLICATE_FINDING_PLAN_FINGERPRINT_COLUMN_ID,
            "plan_fingerprint",
        ),
        text_field(
            DUPLICATE_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            "canonical_plan_digest",
        ),
        text_field(DUPLICATE_FINDING_NODE_ID_COLUMN_ID, "node_id"),
        u32_field(DUPLICATE_FINDING_RULE_ORDINAL_COLUMN_ID, "rule_ordinal"),
        u32_field(
            DUPLICATE_FINDING_KEY_COLUMN_COUNT_COLUMN_ID,
            "key_column_count",
        ),
        u32_field(
            DUPLICATE_FINDING_ENCODED_KEY_BYTE_COUNT_COLUMN_ID,
            "encoded_key_byte_count",
        ),
    ])
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
    use chrono::{DateTime, Utc};
    use sha2::Sha256;
    use std::sync::Arc;

    use stillflow_core::{
        logical_schema_to_arrow, ColumnId, LogicalField, LogicalType, MAX_SCHEMA_FIELDS,
    };

    use super::*;

    fn le16(value: u16) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn le32(value: u32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn le64(value: u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn len_prefixed(bytes: &[u8]) -> Vec<u8> {
        let mut out = le32(u32::try_from(bytes.len()).expect("length"));
        out.extend_from_slice(bytes);
        out
    }

    fn domain(prefix: &str) -> Vec<u8> {
        let mut out = prefix.as_bytes().to_vec();
        out.push(0x00);
        out
    }

    fn manual_digest(parts: &[&[u8]]) -> ContentDigest {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part);
        }
        ContentDigest::from_bytes(hasher.finalize().into())
    }

    fn int_schema() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(11)),
                "value",
                LogicalType::Int64,
                false,
            )
            .expect("field")])
            .expect("schema"),
        )
    }

    fn int_batch(schema: &LogicalSchema, values: Vec<i64>) -> RecordBatch {
        let arrow = logical_schema_to_arrow(schema).expect("arrow schema");
        RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(values))]).expect("batch")
    }

    #[test]
    fn canonical_batch_bytes_are_deterministic_and_framing_free() {
        let schema = int_schema();
        let batch = int_batch(&schema, vec![1, 2, 3]);
        let first = canonical_batch_bytes(&batch).expect("canonical bytes");
        let second = canonical_batch_bytes(&batch).expect("canonical bytes");
        assert_eq!(first, second);
        assert!(!first.is_empty());
        // No encapsulation header: the message must not begin with the
        // continuation marker or a little-endian length that would decode to
        // the flatbuffer size including a continuation prefix.
        assert_ne!(&first[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);

        // Independent re-derivation: the full stream (finished) minus its
        // schema prefix and trailing end-of-stream marker leaves exactly the
        // encapsulated record-batch message.
        let mut schema_only = Vec::new();
        {
            let writer =
                StreamWriter::try_new(&mut schema_only, batch.schema().as_ref()).expect("stream");
            drop(writer); // no finish(): schema message only
        }
        let mut full = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut full, batch.schema().as_ref()).expect("stream");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }
        assert!(full.starts_with(&schema_only));
        const END_OF_STREAM_BYTES: usize = 8; // continuation marker plus zero length
        let region = &full[schema_only.len()..full.len() - END_OF_STREAM_BYTES];
        assert_eq!(&region[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        assert_eq!(&region[8..], &first[..]);
    }

    #[test]
    fn partition_digest_matches_the_manual_preimage_and_is_mutation_sensitive() {
        let artifact_id = Uuid::from_u128(0xA100);
        let section_id = ArtifactSectionId::ValidationFinding;
        let batches = vec![vec![1_u8, 2, 3], vec![4_u8, 5]];
        let digest = compute_partition_digest(artifact_id, section_id, 7, 9, 5, &batches);

        let mut expected = domain(PARTITION_DOMAIN);
        expected.extend_from_slice(artifact_id.as_bytes());
        expected.push(section_id.tag());
        expected.extend(le32(7));
        expected.extend(le64(9));
        expected.extend(le64(5));
        expected.extend(le32(2));
        for batch in &batches {
            expected.extend(len_prefixed(batch));
        }
        assert_eq!(digest, manual_digest(&[&expected]));

        let mutated = compute_partition_digest(artifact_id, section_id, 8, 9, 5, &batches);
        assert_ne!(mutated, digest);
        let other_section = compute_partition_digest(
            artifact_id,
            ArtifactSectionId::DuplicateFinding,
            7,
            9,
            5,
            &batches,
        );
        assert_ne!(other_section, digest);
    }

    #[test]
    fn section_digest_matches_the_manual_preimage_and_requires_sorted_partitions() {
        let artifact_id = Uuid::from_u128(0xA200);
        let schema = validation_finding_section_schema();
        let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fingerprint");
        let partitions = vec![
            ArtifactPartition::try_new(0, 3, 30, manual_digest(&[&[1]])).expect("partition"),
            ArtifactPartition::try_new(1, 4, 40, manual_digest(&[&[2]])).expect("partition"),
        ];
        let stats = ArtifactSectionStats::try_from_partitions(&partitions).expect("stats");
        let digest = compute_section_digest(
            artifact_id,
            ArtifactSectionId::RejectedRows,
            &schema,
            fingerprint.as_bytes(),
            &stats,
            &partitions,
        )
        .expect("digest");

        let schema_bytes = schema.canonical_bytes().expect("canonical");
        let mut expected = domain(SECTION_DOMAIN);
        expected.extend_from_slice(artifact_id.as_bytes());
        expected.push(ArtifactSectionId::RejectedRows.tag());
        expected.extend(len_prefixed(&schema_bytes));
        expected.extend(len_prefixed(fingerprint.as_bytes()));
        expected.extend(le64(7));
        expected.extend(le64(70));
        expected.extend(le32(2));
        for partition in &partitions {
            expected.extend(le32(partition.sequence()));
            expected.extend(le64(partition.row_count()));
            expected.extend(le64(partition.stored_byte_count()));
            expected.extend(len_prefixed(partition.digest().as_bytes()));
        }
        assert_eq!(digest, manual_digest(&[&expected]));

        // Unsorted partitions are refused rather than silently reordered.
        let reversed: Vec<_> = partitions.iter().rev().cloned().collect();
        assert!(compute_section_digest(
            artifact_id,
            ArtifactSectionId::RejectedRows,
            &schema,
            fingerprint.as_bytes(),
            &stats,
            &reversed,
        )
        .is_err());
    }

    #[test]
    fn manifest_digest_matches_the_manual_preimage() {
        let artifact_id = Uuid::from_u128(0xA300);
        let summary_schema = validation_rule_summary_section_schema();
        let finding_schema = validation_finding_section_schema();
        let mut sections = Vec::new();
        for (section_id, schema) in [
            (ArtifactSectionId::ValidationRuleSummary, &summary_schema),
            (ArtifactSectionId::ValidationFinding, &finding_schema),
        ] {
            let fingerprint = LogicalSchemaFingerprint::try_from_schema(schema).expect("fp");
            let partitions =
                vec![ArtifactPartition::try_new(0, 2, 20, manual_digest(&[&[9]])).expect("p")];
            let stats = ArtifactSectionStats::try_from_partitions(&partitions).expect("stats");
            let section_digest = compute_section_digest(
                artifact_id,
                section_id,
                schema,
                fingerprint.as_bytes(),
                &stats,
                &partitions,
            )
            .expect("section digest");
            sections.push(ArtifactSection {
                section_id,
                schema: schema.clone(),
                schema_fingerprint: fingerprint,
                stats,
                partitions,
                section_digest,
            });
        }
        let manifest = ArtifactManifest::try_new(
            artifact_id,
            stillflow_core::ArtifactKind::ValidationReport,
            sections.clone(),
        )
        .expect("manifest");

        let mut expected = domain(MANIFEST_DOMAIN);
        expected.extend(le16(ARTIFACT_MANIFEST_VERSION));
        expected.extend_from_slice(artifact_id.as_bytes());
        expected.push(stillflow_core::ArtifactKind::ValidationReport.tag());
        expected.extend(le32(2));
        for section in &sections {
            expected.push(section.section_id.tag());
            let schema_bytes = section.schema.canonical_bytes().expect("canonical");
            expected.extend(len_prefixed(&schema_bytes));
            expected.extend(len_prefixed(section.schema_fingerprint.as_bytes()));
            expected.extend(le64(section.stats.row_count));
            expected.extend(le64(section.stats.stored_byte_count));
            expected.extend(le32(section.stats.partition_count));
            expected.extend(len_prefixed(section.section_digest.as_bytes()));
        }
        assert_eq!(manifest.manifest_digest(), manual_digest(&[&expected]));
        assert_eq!(manifest.version(), ARTIFACT_MANIFEST_VERSION);
        assert_eq!(
            manifest.kind(),
            stillflow_core::ArtifactKind::ValidationReport
        );
    }

    #[test]
    fn manifest_assembly_rejects_tampered_or_inconsistent_sections() {
        let artifact_id = Uuid::from_u128(0xA310);
        let schema = validation_rule_summary_section_schema();
        let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fp");
        let make_section = |tag| {
            let partitions =
                vec![ArtifactPartition::try_new(0, 2, 20, manual_digest(&[&[9]])).expect("p")];
            let stats = ArtifactSectionStats::try_from_partitions(&partitions).expect("stats");
            let section_digest = compute_section_digest(
                artifact_id,
                tag,
                &schema,
                fingerprint.as_bytes(),
                &stats,
                &partitions,
            )
            .expect("digest");
            ArtifactSection {
                section_id: tag,
                schema: schema.clone(),
                schema_fingerprint: fingerprint,
                stats,
                partitions,
                section_digest,
            }
        };

        // Duplicate section ids are refused.
        let duplicated = vec![make_section(ArtifactSectionId::ValidationFinding); 2];
        assert!(ArtifactManifest::try_new(
            artifact_id,
            stillflow_core::ArtifactKind::ValidationReport,
            duplicated,
        )
        .is_err());

        // A tampered section digest is recomputed and caught.
        let mut tampered = make_section(ArtifactSectionId::ValidationFinding);
        tampered.section_digest = manual_digest(&[&[0xDE]]);
        assert!(ArtifactManifest::try_new(
            artifact_id,
            stillflow_core::ArtifactKind::ValidationReport,
            vec![tampered],
        )
        .is_err());

        // Statistics must equal the partition sums.
        let mut wrong_stats = make_section(ArtifactSectionId::ValidationFinding);
        wrong_stats.stats.row_count += 1;
        assert!(ArtifactManifest::try_new(
            artifact_id,
            stillflow_core::ArtifactKind::ValidationReport,
            vec![wrong_stats],
        )
        .is_err());
    }

    #[test]
    fn report_section_schemas_use_only_reserved_column_ids() {
        let cases: Vec<(LogicalSchema, usize)> = vec![
            (validation_rule_summary_section_schema(), 15),
            (validation_finding_section_schema(), 10),
            (dedup_rule_summary_section_schema(), 11),
            (duplicate_finding_section_schema(), 11),
        ];
        for (schema, field_count) in cases {
            assert_eq!(schema.fields.len(), field_count);
            for field in &schema.fields {
                let raw = field.id.as_uuid().as_u128();
                assert_eq!(
                    raw >> 8,
                    0x00E4_C000_0000_0040_0080_0000_0000_0000,
                    "report fields must use reserved ids"
                );
                assert!(!field.nullable);
                assert!(field.metadata.is_empty());
            }
        }
    }

    #[test]
    fn rejected_rows_schema_appends_controls_after_exact_source_fields() {
        let source = int_schema();
        let rejected = rejected_rows_section_schema(&source).expect("rejected schema");
        assert_eq!(rejected.fields.len(), source.fields.len() + 9);
        for (index, field) in source.fields.iter().enumerate() {
            assert_eq!(rejected.fields[index], *field);
        }
        let controls = rejected_rows_control_fields();
        for (index, control) in controls.iter().enumerate() {
            assert_eq!(rejected.fields[source.fields.len() + index], *control);
        }

        // A source schema already using a reserved control name is refused.
        let colliding = LogicalSchema::new(vec![
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(21)),
                "value",
                LogicalType::Int64,
                false,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(22)),
                "node_id",
                LogicalType::Utf8,
                false,
            )
            .expect("field"),
        ])
        .expect("schema");
        assert!(rejected_rows_section_schema(&colliding).is_err());

        // A source schema without room for the nine controls is refused.
        let oversized = LogicalSchema::new(
            (0..MAX_SCHEMA_FIELDS - 8)
                .map(|index| {
                    LogicalField::new(
                        ColumnId::from_uuid(Uuid::from_u128(1000 + index as u128)),
                        format!("c{index}"),
                        LogicalType::Int64,
                        false,
                    )
                    .expect("field")
                })
                .collect::<Vec<_>>(),
        )
        .expect("schema");
        assert!(rejected_rows_section_schema(&oversized).is_err());
    }

    #[test]
    fn provenance_preimages_match_manual_construction() {
        let run_id = Uuid::from_u128(0xB000);
        let bundle_id = Uuid::from_u128(0xB001);
        let artifact_id = Uuid::from_u128(0xB002);
        let plan_digest = [0x11_u8; 32];
        let version_digest = [0x22_u8; 32];

        let schema = validation_finding_section_schema();
        let fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fp");
        let partitions =
            vec![ArtifactPartition::try_new(0, 1, 10, manual_digest(&[&[7]])).expect("p")];
        let stats = ArtifactSectionStats::try_from_partitions(&partitions).expect("stats");
        let section_digest = compute_section_digest(
            artifact_id,
            ArtifactSectionId::ValidationFinding,
            &schema,
            fingerprint.as_bytes(),
            &stats,
            &partitions,
        )
        .expect("section digest");
        let section = ArtifactSection {
            section_id: ArtifactSectionId::ValidationFinding,
            schema: schema.clone(),
            schema_fingerprint: fingerprint,
            stats,
            partitions,
            section_digest,
        };
        let manifest = ArtifactManifest::try_new(
            artifact_id,
            stillflow_core::ArtifactKind::DeduplicationReport,
            vec![section],
        )
        .expect("manifest");

        let content = compute_artifact_provenance_digest(
            run_id,
            bundle_id,
            artifact_id,
            stillflow_core::ArtifactKind::DeduplicationReport,
            &plan_digest,
            &version_digest,
            manifest.sections(),
            &manifest.manifest_digest(),
        )
        .expect("content digest");

        let mut expected = domain(ARTIFACT_PROVENANCE_DOMAIN);
        expected.extend_from_slice(run_id.as_bytes());
        expected.extend_from_slice(bundle_id.as_bytes());
        expected.extend_from_slice(artifact_id.as_bytes());
        expected.push(stillflow_core::ArtifactKind::DeduplicationReport.tag());
        expected.extend(len_prefixed(&plan_digest));
        expected.extend(len_prefixed(&version_digest));
        expected.extend(le32(1));
        expected.push(ArtifactSectionId::ValidationFinding.tag());
        expected.extend(len_prefixed(section_digest.as_bytes()));
        expected.extend(len_prefixed(manifest.manifest_digest().as_bytes()));
        assert_eq!(content, manual_digest(&[&expected]).as_bytes().to_owned());
        assert_ne!(content, [0_u8; 32]);

        let accepted_snapshot = DatasetSnapshot::try_new(
            Uuid::from_u128(0xB010),
            Uuid::from_u128(0xB011),
            Uuid::from_u128(0xB012),
            Uuid::from_u128(0xB013),
            int_schema().as_ref().clone(),
            stillflow_core::SnapshotStats::try_new(4, 40, 1).expect("stats"),
            std::collections::BTreeSet::from([Uuid::from_u128(0xB014)]),
            Some(90),
            DateTime::<Utc>::from_timestamp(1_700_000_000, 0).expect("time"),
        )
        .expect("snapshot");
        let snapshot_partition_digest = manual_digest(&[&[42]]);
        let snapshot_partitions = vec![AcceptedCanonicalPartition {
            sequence: 0,
            row_count: 4,
            stored_byte_count: 40,
            digest: snapshot_partition_digest,
        }];
        let accepted_digest =
            accepted_snapshot_manifest_digest(&accepted_snapshot, &snapshot_partitions)
                .expect("accepted digest");
        let mut accepted_expected = domain(ACCEPTED_SNAPSHOT_DOMAIN);
        accepted_expected.extend_from_slice(accepted_snapshot.id().as_bytes());
        accepted_expected.extend_from_slice(accepted_snapshot.dataset_id().as_bytes());
        accepted_expected.extend_from_slice(accepted_snapshot.session_id().as_bytes());
        accepted_expected.extend_from_slice(accepted_snapshot.source_asset_id().as_bytes());
        accepted_expected.extend(len_prefixed(
            accepted_snapshot.schema_fingerprint().as_bytes(),
        ));
        accepted_expected.extend(le64(4));
        accepted_expected.extend(le64(40));
        accepted_expected.extend(le32(1));
        accepted_expected.extend(le32(0));
        accepted_expected.extend(le64(4));
        accepted_expected.extend(le64(40));
        accepted_expected.extend(len_prefixed(snapshot_partition_digest.as_bytes()));
        assert_eq!(
            accepted_digest,
            manual_digest(&[&accepted_expected]).as_bytes().to_owned()
        );

        let bundle_digest = compute_bundle_provenance_digest(
            run_id,
            bundle_id,
            Uuid::from_u128(0xB020),
            accepted_snapshot.id(),
            Uuid::from_u128(0xB021),
            None,
            Uuid::from_u128(0xB022),
            &[
                (accepted_snapshot.id(), accepted_digest, accepted_digest),
                (
                    manifest.artifact_id(),
                    *manifest.manifest_digest().as_bytes(),
                    content,
                ),
            ],
        );
        let mut bundle_expected = domain(BUNDLE_PROVENANCE_DOMAIN);
        bundle_expected.extend_from_slice(run_id.as_bytes());
        bundle_expected.extend_from_slice(bundle_id.as_bytes());
        bundle_expected.extend_from_slice(Uuid::from_u128(0xB020).as_bytes());
        bundle_expected.extend_from_slice(accepted_snapshot.id().as_bytes());
        bundle_expected.extend_from_slice(Uuid::from_u128(0xB021).as_bytes());
        bundle_expected.push(0x00);
        bundle_expected.extend_from_slice(Uuid::from_u128(0xB022).as_bytes());
        // The frozen bundle-provenance formula has no child-count slot.
        for (id, manifest_part, content_part) in [
            (accepted_snapshot.id(), accepted_digest, accepted_digest),
            (
                manifest.artifact_id(),
                *manifest.manifest_digest().as_bytes(),
                content,
            ),
        ] {
            bundle_expected.extend_from_slice(id.as_bytes());
            bundle_expected.extend(len_prefixed(&manifest_part));
            bundle_expected.extend(len_prefixed(&content_part));
        }
        assert_eq!(
            bundle_digest,
            manual_digest(&[&bundle_expected]).as_bytes().to_owned()
        );
    }

    #[test]
    fn report_limits_follow_the_contract_products() {
        // Contract section 12: MAX_REPORT_ROWS and MAX_REPORT_BYTES are exact
        // products of the partition ceiling and pack ceilings.
        assert_eq!(
            MAX_REPORT_ROWS,
            MAX_REPORT_PARTITIONS as u64 * REPORT_PACK_ROWS as u64
        );
        assert_eq!(
            MAX_REPORT_BYTES,
            MAX_REPORT_PARTITIONS as u64 * REPORT_PACK_BYTES as u64
        );
        // The bundle-wide ceiling is exactly twice each per-report ceiling.
        assert_eq!(MAX_BUNDLE_REPORT_ROWS, 2 * MAX_REPORT_ROWS);
        assert_eq!(MAX_BUNDLE_REPORT_BYTES, 2 * MAX_REPORT_BYTES);
        assert_eq!(MAX_BUNDLE_REPORT_PARTITIONS, 2 * MAX_REPORT_PARTITIONS);
    }

    // Keep unused array builders referenced so clippy stays quiet about the
    // fixture helpers below.
    #[allow(dead_code)]
    fn unused_fixture_helpers(values: Vec<u64>, ordinals: Vec<u32>, text: Vec<&str>) {
        let _ = UInt64Array::from(values);
        let _ = UInt32Array::from(ordinals);
        let _ = StringArray::from(text);
    }

    fn hex_digest(digest: ContentDigest) -> String {
        digest
            .as_bytes()
            .iter()
            .fold(String::new(), |mut out, byte| {
                use std::fmt::Write as _;
                let _ = write!(out, "{byte:02x}");
                out
            })
    }

    /// Walks an encapsulated IPC stream to the record-batch message and
    /// returns its metadata length prefix. Test-only oracle helper over raw
    /// `arrow-ipc` output; shares no code with `canonical_batch_bytes`.
    fn batch_message_metadata_len(stream: &[u8]) -> usize {
        assert_eq!(&stream[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        let schema_meta_len = u32::from_le_bytes(stream[4..8].try_into().expect("prefix")) as usize;
        let schema_end = 8 + schema_meta_len.div_ceil(8) * 8;
        let message = &stream[schema_end..];
        assert_eq!(&message[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        u32::from_le_bytes(message[4..8].try_into().expect("prefix")) as usize
    }

    /// Hardcoded literal from a scratch binary over raw arrow-ipc 59.2.0
    /// default (64-byte aligned) output — independent of this crate's
    /// helpers. The vector's metadata block carries nonzero alignment
    /// padding inside the length-prefixed region (asserted below), so the
    /// literal fails any implementation that strips alignment padding,
    /// retains framing bytes, or mis-slices the schema-message boundary.
    #[test]
    fn canonical_batch_bytes_match_hardcoded_arrow_ipc_literal() {
        let schema = int_schema();
        let batch = int_batch(&schema, vec![1, 2, 3]);
        let canonical = canonical_batch_bytes(&batch).expect("canonical bytes");
        assert_eq!(canonical.len(), 312);
        let mut hasher = Sha256::new();
        hasher.update(&canonical);
        assert_eq!(
            hex_digest(ContentDigest::from_bytes(hasher.finalize().into())),
            "017255f9f7ec953af183a352180bb78e90bd0444cdf78f6227c89d8fe1661374"
        );

        // The default writer pads the flatbuffer tail INSIDE the
        // length-prefixed metadata block. Re-encoding with 8-byte alignment
        // yields a shorter block; the difference is exactly the padding that
        // must stay part of the hashed region.
        use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
        let mut aligned8 = Vec::new();
        {
            let options = IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5)
                .expect("options");
            let mut writer =
                StreamWriter::try_new_with_options(&mut aligned8, batch.schema().as_ref(), options)
                    .expect("stream");
            writer.write(&batch).expect("write");
            drop(writer);
        }
        let mut default_stream = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut default_stream, batch.schema().as_ref()).expect("s");
            writer.write(&batch).expect("write");
            drop(writer);
        }
        let len64 = batch_message_metadata_len(&default_stream);
        let len8 = batch_message_metadata_len(&aligned8);
        assert_eq!(len64, 184);
        assert_eq!(len8, 136);
        assert_eq!(len64 - len8, 48, "metadata padding must be nonzero");
    }

    /// Contract-literal bundle-provenance digests produced by an external
    /// oracle implementing section 8.1.1 byte-for-byte. The frozen formula
    /// has NO child-count slot; these literals fail any implementation that
    /// adds one.
    #[test]
    fn bundle_provenance_digest_matches_contract_literal_without_child_count() {
        let run_id = Uuid::from_u128(0xB001);
        let bundle_id = Uuid::from_u128(0xB002);
        let bundle_artifact_id = Uuid::from_u128(0xB003);
        let accepted_id = Uuid::from_u128(0xB004);
        let validation_id = Uuid::from_u128(0xB005);
        let rejected_id = Uuid::from_u128(0xB006);
        let dedup_id = Uuid::from_u128(0xB007);
        let manifest_of = |k: u8| {
            let mut value = [0u8; 32];
            value.fill(0x10 + k);
            value
        };
        let content_of = |k: u8| {
            let mut value = [0u8; 32];
            value.fill(0x20 + k);
            value
        };

        let children_without_rejected: Vec<(Uuid, [u8; 32], [u8; 32])> = vec![
            (accepted_id, manifest_of(0), content_of(0)),
            (validation_id, manifest_of(1), content_of(1)),
            (dedup_id, manifest_of(3), content_of(3)),
        ];
        assert_eq!(
            hex_digest(ContentDigest::from_bytes(compute_bundle_provenance_digest(
                run_id,
                bundle_id,
                bundle_artifact_id,
                accepted_id,
                validation_id,
                None,
                dedup_id,
                &children_without_rejected,
            ))),
            "08270b9647a16768f7cd07e811a7593dbeef7cc42cf36d437fe2bb966c95da38"
        );

        let children_with_rejected: Vec<(Uuid, [u8; 32], [u8; 32])> = vec![
            (accepted_id, manifest_of(0), content_of(0)),
            (validation_id, manifest_of(1), content_of(1)),
            (rejected_id, manifest_of(2), content_of(2)),
            (dedup_id, manifest_of(3), content_of(3)),
        ];
        assert_eq!(
            hex_digest(ContentDigest::from_bytes(compute_bundle_provenance_digest(
                run_id,
                bundle_id,
                bundle_artifact_id,
                accepted_id,
                validation_id,
                Some(rejected_id),
                dedup_id,
                &children_with_rejected,
            ))),
            "d39cfe24ac422b8dbfed7e31b4fe3f68ddd1907dbf41f2572f1d8d1e3ec2d17a"
        );
    }

    /// Accepted partition digests cover decoded logical batch bytes only:
    /// identical logical data under different Parquet compression settings
    /// produces identical digests, and any logical value flip changes the
    /// digest (contract 8.1.1 excludes Parquet framing from the domain).
    #[test]
    fn accepted_partition_digest_is_independent_of_parquet_writer_settings() {
        use arrow_ipc::writer::StreamWriter as IpcWriter;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let schema = int_schema();
        let encode_parquet = |values: Vec<i64>, compressed: bool, path: &std::path::Path| {
            let properties = WriterProperties::builder()
                .set_compression(if compressed {
                    Compression::SNAPPY
                } else {
                    Compression::UNCOMPRESSED
                })
                .build();
            let file = std::fs::File::create(path).expect("create file");
            let mut writer = ArrowWriter::try_new(
                file,
                logical_schema_to_arrow(schema.as_ref()).expect("arrow schema"),
                Some(properties),
            )
            .expect("writer");
            writer.write(&int_batch(&schema, values)).expect("write");
            writer.into_inner().expect("finish");
            std::fs::read(path).expect("read back")
        };
        let decode_canonical = |file: &[u8]| -> Vec<u8> {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            let temp = tempfile::TempDir::new().expect("temp dir");
            let path = temp.path().join("partition.parquet");
            std::fs::write(&path, file).expect("write file");
            let reader_file = std::fs::File::open(&path).expect("open file");
            let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file).expect("builder");
            let reader = builder.with_batch_size(1024).build().expect("reader");
            let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("batch")).collect();
            assert_eq!(batches.len(), 1);
            let mut buffer = Vec::new();
            {
                let mut writer =
                    IpcWriter::try_new(&mut buffer, batches[0].schema().as_ref()).expect("ipc");
                writer.write(&batches[0]).expect("write");
                drop(writer);
            }
            buffer
        };

        let temp = tempfile::TempDir::new().expect("temp dir");
        let snappy = encode_parquet(vec![1, 2, 3], true, &temp.path().join("snappy.parquet"));
        let plain = encode_parquet(vec![1, 2, 3], false, &temp.path().join("plain.parquet"));
        assert_ne!(snappy, plain, "physical encodings must differ");

        let digest_for = |file: &[u8]| {
            let canonical = decode_canonical(file);
            accepted_partition_canonical_digest(
                Uuid::from_u128(0xB100),
                0,
                3,
                u64::try_from(canonical.len()).expect("canonical length"),
                std::slice::from_ref(&canonical),
            )
        };
        assert_eq!(digest_for(&snappy), digest_for(&plain));

        let flipped_file =
            encode_parquet(vec![1, 2, 4], true, &temp.path().join("flipped.parquet"));
        assert_ne!(digest_for(&flipped_file), digest_for(&snappy));
    }
}
