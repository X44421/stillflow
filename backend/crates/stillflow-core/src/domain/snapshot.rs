use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use uuid::Uuid;

use crate::{LogicalError, LogicalSchema, LogicalSchemaFingerprint};

/// Current serialized version of the stable dataset-snapshot contract.
pub const DATASET_SNAPSHOT_VERSION: u16 = 1;

/// Conserved logical and physical totals for an immutable snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotStats {
    row_count: u64,
    stored_byte_count: u64,
    partition_count: u32,
}

impl SnapshotStats {
    pub fn try_new(
        row_count: u64,
        stored_byte_count: u64,
        partition_count: u32,
    ) -> Result<Self, SnapshotError> {
        let is_empty = partition_count == 0;
        if is_empty != (row_count == 0) || is_empty != (stored_byte_count == 0) {
            return Err(SnapshotError::ContradictoryStats);
        }

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

/// Immutable logical snapshot descriptor independent of SQLite and Parquet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshot {
    version: u16,
    id: Uuid,
    dataset_id: Uuid,
    session_id: Uuid,
    source_asset_id: Uuid,
    schema: LogicalSchema,
    schema_fingerprint: LogicalSchemaFingerprint,
    stats: SnapshotStats,
    lineage: BTreeSet<Uuid>,
    quality_score: Option<u8>,
    created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetSnapshotData {
    version: u16,
    id: Uuid,
    dataset_id: Uuid,
    session_id: Uuid,
    source_asset_id: Uuid,
    schema: LogicalSchema,
    schema_fingerprint: LogicalSchemaFingerprint,
    row_count: u64,
    stored_byte_count: u64,
    partition_count: u32,
    lineage: BTreeSet<Uuid>,
    quality_score: Option<u8>,
    created_at: DateTime<Utc>,
}

impl Serialize for DatasetSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DatasetSnapshotData {
            version: self.version,
            id: self.id,
            dataset_id: self.dataset_id,
            session_id: self.session_id,
            source_asset_id: self.source_asset_id,
            schema: self.schema.clone(),
            schema_fingerprint: self.schema_fingerprint,
            row_count: self.stats.row_count,
            stored_byte_count: self.stats.stored_byte_count,
            partition_count: self.stats.partition_count,
            lineage: self.lineage.clone(),
            quality_score: self.quality_score,
            created_at: self.created_at.to_owned(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DatasetSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = DatasetSnapshotData::deserialize(deserializer)?;
        let stats =
            SnapshotStats::try_new(data.row_count, data.stored_byte_count, data.partition_count)
                .map_err(DeError::custom)?;
        Self::try_from_parts(
            data.version,
            data.id,
            data.dataset_id,
            data.session_id,
            data.source_asset_id,
            data.schema,
            data.schema_fingerprint,
            stats,
            data.lineage,
            data.quality_score,
            data.created_at,
        )
        .map_err(DeError::custom)
    }
}

impl DatasetSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: Uuid,
        dataset_id: Uuid,
        session_id: Uuid,
        source_asset_id: Uuid,
        schema: LogicalSchema,
        stats: SnapshotStats,
        lineage: BTreeSet<Uuid>,
        quality_score: Option<u8>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, SnapshotError> {
        let schema_fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
            .map_err(|_| SnapshotError::SchemaFingerprint)?;
        Self::try_from_parts(
            DATASET_SNAPSHOT_VERSION,
            id,
            dataset_id,
            session_id,
            source_asset_id,
            schema,
            schema_fingerprint,
            stats,
            lineage,
            quality_score,
            created_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_from_parts(
        version: u16,
        id: Uuid,
        dataset_id: Uuid,
        session_id: Uuid,
        source_asset_id: Uuid,
        schema: LogicalSchema,
        schema_fingerprint: LogicalSchemaFingerprint,
        stats: SnapshotStats,
        lineage: BTreeSet<Uuid>,
        quality_score: Option<u8>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, SnapshotError> {
        if version != DATASET_SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }
        validate_id(id, "snapshot")?;
        validate_id(dataset_id, "dataset")?;
        validate_id(session_id, "session")?;
        validate_id(source_asset_id, "source asset")?;
        for lineage_id in &lineage {
            validate_id(*lineage_id, "lineage")?;
        }
        if quality_score.is_some_and(|score| score > 100) {
            return Err(SnapshotError::InvalidQualityScore);
        }
        stats.validate()?;
        schema.validate()?;
        let actual_fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
            .map_err(|_| SnapshotError::SchemaFingerprint)?;
        if actual_fingerprint != schema_fingerprint {
            return Err(SnapshotError::SchemaFingerprintMismatch);
        }

        Ok(Self {
            version,
            id,
            dataset_id,
            session_id,
            source_asset_id,
            schema,
            schema_fingerprint,
            stats,
            lineage,
            quality_score,
            created_at,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
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

    pub const fn stats(&self) -> SnapshotStats {
        self.stats
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

impl SnapshotStats {
    fn validate(&self) -> Result<(), SnapshotError> {
        Self::try_new(self.row_count, self.stored_byte_count, self.partition_count).map(|_| ())
    }
}

fn validate_id(id: Uuid, identity: &'static str) -> Result<(), SnapshotError> {
    if id.is_nil() {
        return Err(SnapshotError::NilIdentity(identity));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnapshotError {
    #[error("unsupported dataset snapshot version {0}")]
    UnsupportedVersion(u16),
    #[error("{0} identity must not be nil")]
    NilIdentity(&'static str),
    #[error("snapshot totals contradict the partition count")]
    ContradictoryStats,
    #[error("quality score must be between zero and one hundred")]
    InvalidQualityScore,
    #[error("logical schema is invalid: {0}")]
    Logical(#[from] LogicalError),
    #[error("logical schema fingerprint could not be computed")]
    SchemaFingerprint,
    #[error("logical schema fingerprint does not match the complete schema")]
    SchemaFingerprintMismatch,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::{ColumnId, LogicalField, LogicalType};

    fn schema() -> LogicalSchema {
        LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(11)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("valid field")])
        .expect("valid schema")
    }

    fn snapshot() -> DatasetSnapshot {
        DatasetSnapshot::try_new(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
            schema(),
            SnapshotStats::try_new(5, 128, 1).expect("valid stats"),
            BTreeSet::from([Uuid::from_u128(9)]),
            Some(98),
            DateTime::from_timestamp(1_700_000_000, 123).expect("valid timestamp"),
        )
        .expect("valid snapshot")
    }

    #[test]
    fn stable_snapshot_roundtrips_with_logical_schema_only() {
        let snapshot = snapshot();
        let json = serde_json::to_string(&snapshot).expect("serialize");
        let restored: DatasetSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored, snapshot);
        assert!(!json.contains("storageRef"));
        assert!(!json.contains("schemaFields"));
        assert!(!json.contains("snap://"));
    }

    #[test]
    fn rejects_invalid_versions_identities_quality_and_stats() {
        let valid = snapshot();
        assert!(matches!(
            DatasetSnapshot::try_from_parts(
                DATASET_SNAPSHOT_VERSION + 1,
                valid.id,
                valid.dataset_id,
                valid.session_id,
                valid.source_asset_id,
                valid.schema.clone(),
                valid.schema_fingerprint,
                valid.stats,
                valid.lineage.clone(),
                valid.quality_score,
                valid.created_at.to_owned(),
            ),
            Err(SnapshotError::UnsupportedVersion(_))
        ));
        assert!(matches!(
            DatasetSnapshot::try_new(
                Uuid::nil(),
                valid.dataset_id,
                valid.session_id,
                valid.source_asset_id,
                valid.schema.clone(),
                valid.stats,
                valid.lineage.clone(),
                valid.quality_score,
                valid.created_at.to_owned(),
            ),
            Err(SnapshotError::NilIdentity("snapshot"))
        ));
        assert!(matches!(
            DatasetSnapshot::try_new(
                valid.id,
                valid.dataset_id,
                valid.session_id,
                valid.source_asset_id,
                valid.schema.clone(),
                valid.stats,
                BTreeSet::from([Uuid::nil()]),
                valid.quality_score,
                valid.created_at.to_owned(),
            ),
            Err(SnapshotError::NilIdentity("lineage"))
        ));
        assert!(matches!(
            DatasetSnapshot::try_new(
                valid.id,
                valid.dataset_id,
                valid.session_id,
                valid.source_asset_id,
                valid.schema,
                valid.stats,
                valid.lineage,
                Some(101),
                valid.created_at.to_owned(),
            ),
            Err(SnapshotError::InvalidQualityScore)
        ));
        assert!(matches!(
            SnapshotStats::try_new(1, 0, 1),
            Err(SnapshotError::ContradictoryStats)
        ));
    }

    #[test]
    fn deserialization_revalidates_fingerprint_and_schema() {
        let snapshot = snapshot();
        let mut value = serde_json::to_value(&snapshot).expect("serialize");
        value["schemaFingerprint"] = serde_json::to_value(
            LogicalSchemaFingerprint::try_from_schema(&LogicalSchema::empty())
                .expect("fingerprint"),
        )
        .expect("serialize fingerprint");
        assert!(serde_json::from_value::<DatasetSnapshot>(value).is_err());

        let mut value = serde_json::to_value(snapshot).expect("serialize");
        value["schema"]["metadata"] = serde_json::json!(BTreeMap::from([(
            "password".to_owned(),
            "secret".to_owned()
        )]));
        assert!(serde_json::from_value::<DatasetSnapshot>(value).is_err());
    }
}
