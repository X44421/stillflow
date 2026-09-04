use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use stillflow_core::{
    DatasetSnapshot, LogicalSchema, LogicalSchemaFingerprint, SnapshotStats, MAX_BATCH_ROWS,
};

use crate::{ContentDigest, StorageError};

pub const STORAGE_SCHEMA_VERSION: u16 = 10;
pub const MAX_INPUT_ENVELOPES: u32 = 16_384;
pub const MAX_SNAPSHOT_PARTITIONS: u32 = 16_384;
pub const MAX_SNAPSHOT_ROWS: u64 = 1_000_000_000;
pub const MAX_SNAPSHOT_STORED_BYTES: u64 = 1_u64 << 40;
pub const MAX_MAINTENANCE_CANDIDATES: u32 = 1_024;
pub const MAX_ACTIVE_READERS: u16 = 64;
pub const MAX_ACTIVE_PUBLISHERS: u16 = 8;
pub const SQLITE_BUSY_TIMEOUT_MILLIS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageLimits {
    max_input_envelopes: u32,
    max_partitions: u32,
    max_rows: u64,
    max_stored_bytes: u64,
    max_active_readers: u16,
    max_active_publishers: u16,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_input_envelopes: MAX_INPUT_ENVELOPES,
            max_partitions: MAX_SNAPSHOT_PARTITIONS,
            max_rows: MAX_SNAPSHOT_ROWS,
            max_stored_bytes: MAX_SNAPSHOT_STORED_BYTES,
            max_active_readers: MAX_ACTIVE_READERS,
            max_active_publishers: MAX_ACTIVE_PUBLISHERS,
        }
    }
}

impl StorageLimits {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_input_envelopes: u32,
        max_partitions: u32,
        max_rows: u64,
        max_stored_bytes: u64,
        max_active_readers: u16,
        max_active_publishers: u16,
    ) -> Result<Self, StorageError> {
        if max_input_envelopes == 0 || max_input_envelopes > MAX_INPUT_ENVELOPES {
            return Err(StorageError::InvalidConfiguration(
                "input envelope limit is outside the supported range",
            ));
        }
        if max_partitions == 0 || max_partitions > MAX_SNAPSHOT_PARTITIONS {
            return Err(StorageError::InvalidConfiguration(
                "partition limit is outside the supported range",
            ));
        }
        if max_rows == 0 || max_rows > MAX_SNAPSHOT_ROWS {
            return Err(StorageError::InvalidConfiguration(
                "row limit is outside the supported range",
            ));
        }
        if max_stored_bytes == 0 || max_stored_bytes > MAX_SNAPSHOT_STORED_BYTES {
            return Err(StorageError::InvalidConfiguration(
                "stored byte limit is outside the supported range",
            ));
        }
        if max_active_readers == 0 || max_active_readers > MAX_ACTIVE_READERS {
            return Err(StorageError::InvalidConfiguration(
                "active reader limit is outside the supported range",
            ));
        }
        if max_active_publishers == 0 || max_active_publishers > MAX_ACTIVE_PUBLISHERS {
            return Err(StorageError::InvalidConfiguration(
                "active publisher limit is outside the supported range",
            ));
        }

        Ok(Self {
            max_input_envelopes,
            max_partitions,
            max_rows,
            max_stored_bytes,
            max_active_readers,
            max_active_publishers,
        })
    }

    pub const fn max_input_envelopes(&self) -> u32 {
        self.max_input_envelopes
    }

    pub const fn max_partitions(&self) -> u32 {
        self.max_partitions
    }

    pub const fn max_rows(&self) -> u64 {
        self.max_rows
    }

    pub const fn max_stored_bytes(&self) -> u64 {
        self.max_stored_bytes
    }

    pub const fn max_active_readers(&self) -> u16 {
        self.max_active_readers
    }

    pub const fn max_active_publishers(&self) -> u16 {
        self.max_active_publishers
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDraft {
    id: Uuid,
    dataset_id: Uuid,
    session_id: Uuid,
    source_asset_id: Uuid,
    schema: LogicalSchema,
    schema_fingerprint: LogicalSchemaFingerprint,
    lineage: BTreeSet<Uuid>,
    quality_score: Option<u8>,
    created_at: DateTime<Utc>,
}

impl SnapshotDraft {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: Uuid,
        dataset_id: Uuid,
        session_id: Uuid,
        source_asset_id: Uuid,
        schema: LogicalSchema,
        lineage: BTreeSet<Uuid>,
        quality_score: Option<u8>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        if id.is_nil() || dataset_id.is_nil() || session_id.is_nil() || source_asset_id.is_nil() {
            return Err(StorageError::InvalidDraft(
                "snapshot identities must not be nil",
            ));
        }
        if lineage.iter().any(Uuid::is_nil) {
            return Err(StorageError::InvalidDraft(
                "lineage identities must not be nil",
            ));
        }
        if quality_score.is_some_and(|score| score > 100) {
            return Err(StorageError::InvalidDraft(
                "quality score is outside the supported range",
            ));
        }
        schema
            .validate()
            .map_err(|_| StorageError::InvalidDraft("logical schema is invalid"))?;
        let schema_fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
            .map_err(|_| StorageError::InvalidDraft("logical schema fingerprint failed"))?;

        Ok(Self {
            id,
            dataset_id,
            session_id,
            source_asset_id,
            schema,
            schema_fingerprint,
            lineage,
            quality_score,
            created_at,
        })
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }

    pub const fn dataset_id(&self) -> Uuid {
        self.dataset_id
    }

    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub const fn source_asset_id(&self) -> Uuid {
        self.source_asset_id
    }

    pub fn schema(&self) -> &LogicalSchema {
        &self.schema
    }

    pub const fn schema_fingerprint(&self) -> LogicalSchemaFingerprint {
        self.schema_fingerprint
    }

    pub fn lineage(&self) -> &BTreeSet<Uuid> {
        &self.lineage
    }

    pub const fn quality_score(&self) -> Option<u8> {
        self.quality_score
    }

    pub const fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPartition {
    sequence: u32,
    row_count: u64,
    stored_byte_count: u64,
    digest: ContentDigest,
}

impl SnapshotPartition {
    pub(crate) fn try_new(
        sequence: u32,
        row_count: u64,
        stored_byte_count: u64,
        digest: ContentDigest,
    ) -> Result<Self, StorageError> {
        if row_count == 0 || stored_byte_count == 0 {
            return Err(StorageError::InvalidManifest(
                "physical partitions must be non-empty",
            ));
        }
        if row_count > MAX_BATCH_ROWS as u64 {
            return Err(StorageError::InvalidManifest(
                "partition row count exceeds the batch limit",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
    snapshot: DatasetSnapshot,
    partitions: Vec<SnapshotPartition>,
}

impl SnapshotManifest {
    pub(crate) fn try_new(
        snapshot: DatasetSnapshot,
        partitions: Vec<SnapshotPartition>,
    ) -> Result<Self, StorageError> {
        let expected_count = u32::try_from(partitions.len())
            .map_err(|_| StorageError::InvalidManifest("partition count overflow"))?;
        if snapshot.stats().partition_count() != expected_count {
            return Err(StorageError::InvalidManifest("partition count mismatch"));
        }

        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for (index, partition) in partitions.iter().enumerate() {
            let expected = u32::try_from(index)
                .map_err(|_| StorageError::InvalidManifest("partition sequence overflow"))?;
            if partition.sequence != expected {
                return Err(StorageError::InvalidManifest(
                    "partition sequences are not contiguous",
                ));
            }
            rows = rows
                .checked_add(partition.row_count)
                .ok_or(StorageError::ArithmeticOverflow("manifest row count"))?;
            bytes = bytes.checked_add(partition.stored_byte_count).ok_or(
                StorageError::ArithmeticOverflow("manifest stored byte count"),
            )?;
        }
        if snapshot.stats().row_count() != rows {
            return Err(StorageError::InvalidManifest("row count mismatch"));
        }
        if snapshot.stats().stored_byte_count() != bytes {
            return Err(StorageError::InvalidManifest("stored byte count mismatch"));
        }

        Ok(Self {
            snapshot,
            partitions,
        })
    }

    pub fn snapshot(&self) -> &DatasetSnapshot {
        &self.snapshot
    }

    pub fn partitions(&self) -> &[SnapshotPartition] {
        &self.partitions
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub(crate) examined: u32,
    pub(crate) recovered: u32,
    pub(crate) ignored: u32,
}

impl RecoveryReport {
    pub const fn examined(&self) -> u32 {
        self.examined
    }

    pub const fn recovered(&self) -> u32 {
        self.recovered
    }

    pub const fn ignored(&self) -> u32 {
        self.ignored
    }

    pub(crate) fn add(&mut self, examined: u32, recovered: u32, ignored: u32) {
        self.examined = self.examined.saturating_add(examined);
        self.recovered = self.recovered.saturating_add(recovered);
        self.ignored = self.ignored.saturating_add(ignored);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GarbageCollectionReport {
    pub(crate) examined: u32,
    pub(crate) deleted: u32,
    pub(crate) retained: u32,
}

impl GarbageCollectionReport {
    pub const fn examined(&self) -> u32 {
        self.examined
    }

    pub const fn deleted(&self) -> u32 {
        self.deleted
    }

    pub const fn retained(&self) -> u32 {
        self.retained
    }

    pub(crate) fn add(&mut self, examined: u32, deleted: u32, retained: u32) {
        self.examined = self.examined.saturating_add(examined);
        self.deleted = self.deleted.saturating_add(deleted);
        self.retained = self.retained.saturating_add(retained);
    }
}

pub(crate) fn build_snapshot(
    draft: &SnapshotDraft,
    stats: SnapshotStats,
) -> Result<DatasetSnapshot, StorageError> {
    DatasetSnapshot::try_new(
        draft.id,
        draft.dataset_id,
        draft.session_id,
        draft.source_asset_id,
        draft.schema.clone(),
        stats,
        draft.lineage.clone(),
        draft.quality_score,
        draft.created_at.to_owned(),
    )
    .map_err(StorageError::from)
}
