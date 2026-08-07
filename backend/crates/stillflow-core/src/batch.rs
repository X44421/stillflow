use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    ColumnId, ConnectorError, ErrorCategory, LogicalError, LogicalField, LogicalSchema,
    LogicalType, TimeUnit,
};

/// Current in-memory batch-envelope contract version.
pub const BATCH_ENVELOPE_VERSION: u16 = 1;

/// Maximum number of rows carried by one public batch envelope.
pub const MAX_BATCH_ROWS: usize = 65_536;

/// Maximum conservative Arrow array memory carried by one public batch envelope.
pub const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Versioned algorithm used for non-security logical-schema fingerprints.
pub const LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM: &str = "stillflow-schema-fnv1a64x4-v1";

const SCHEMA_VERSION_KEY: &str = "stillflow.schema.version";
const SCHEMA_FINGERPRINT_KEY: &str = "stillflow.schema.fingerprint";
const SCHEMA_METADATA_KEY: &str = "stillflow.schema.metadata";
const COLUMN_ID_KEY: &str = "stillflow.column.id";
const FIELD_METADATA_KEY: &str = "stillflow.field.metadata";

/// Deterministic 256-bit index for a validated logical schema.
///
/// This value is not a cryptographic checksum. Callers must compare complete
/// logical schemas after a fingerprint match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogicalSchemaFingerprint([u8; 32]);

impl LogicalSchemaFingerprint {
    pub fn try_from_schema(schema: &LogicalSchema) -> Result<Self, BatchError> {
        schema.validate()?;
        let bytes = serde_json::to_vec(schema)
            .map_err(|error| BatchError::SchemaSerialization(error.to_string()))?;
        Ok(Self(fingerprint_bytes(&bytes)))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub const fn algorithm() -> &'static str {
        LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM
    }

    fn from_hex(value: &str) -> Result<Self, BatchError> {
        if value.len() != 64 {
            return Err(BatchError::InvalidReservedMetadata(SCHEMA_FINGERPRINT_KEY));
        }

        let mut result = [0_u8; 32];
        let mut chunks = value.as_bytes().chunks_exact(2);
        for target in &mut result {
            let Some(chunk) = chunks.next() else {
                return Err(BatchError::InvalidReservedMetadata(SCHEMA_FINGERPRINT_KEY));
            };
            let text = std::str::from_utf8(chunk)
                .map_err(|_| BatchError::InvalidReservedMetadata(SCHEMA_FINGERPRINT_KEY))?;
            *target = u8::from_str_radix(text, 16)
                .map_err(|_| BatchError::InvalidReservedMetadata(SCHEMA_FINGERPRINT_KEY))?;
        }
        if !chunks.remainder().is_empty() {
            return Err(BatchError::InvalidReservedMetadata(SCHEMA_FINGERPRINT_KEY));
        }
        Ok(Self(result))
    }
}

impl fmt::Display for LogicalSchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Versioned, bounded, lineage-aware Arrow execution payload.
#[derive(Clone)]
pub struct BatchEnvelope {
    version: u16,
    schema: Arc<LogicalSchema>,
    schema_fingerprint: LogicalSchemaFingerprint,
    source_asset_id: Uuid,
    sequence: u64,
    row_count: usize,
    byte_count: usize,
    payload: RecordBatch,
}

impl fmt::Debug for BatchEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchEnvelope")
            .field("version", &self.version)
            .field("schema_fingerprint", &self.schema_fingerprint)
            .field("source_asset_id", &self.source_asset_id)
            .field("sequence", &self.sequence)
            .field("row_count", &self.row_count)
            .field("byte_count", &self.byte_count)
            .finish_non_exhaustive()
    }
}

impl BatchEnvelope {
    pub fn try_new(
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        payload: RecordBatch,
    ) -> Result<Self, BatchError> {
        Self::try_from_parts(
            BATCH_ENVELOPE_VERSION,
            schema,
            source_asset_id,
            sequence,
            payload,
        )
    }

    pub fn try_from_parts(
        version: u16,
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        payload: RecordBatch,
    ) -> Result<Self, BatchError> {
        if version != BATCH_ENVELOPE_VERSION {
            return Err(BatchError::UnsupportedEnvelopeVersion(version));
        }
        if source_asset_id.is_nil() {
            return Err(BatchError::NilSourceAssetId);
        }

        schema.validate()?;
        let schema_fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)?;
        let expected_arrow_schema =
            logical_schema_to_arrow_with_fingerprint(&schema, schema_fingerprint)?;
        if payload.schema().as_ref() != expected_arrow_schema.as_ref() {
            return Err(BatchError::PhysicalSchemaMismatch);
        }

        let row_count = payload.num_rows();
        let byte_count = payload.get_array_memory_size();
        validate_batch_bounds(row_count, byte_count)?;

        Ok(Self {
            version,
            schema,
            schema_fingerprint,
            source_asset_id,
            sequence,
            row_count,
            byte_count,
            payload,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn schema(&self) -> &LogicalSchema {
        &self.schema
    }

    pub fn shared_schema(&self) -> &Arc<LogicalSchema> {
        &self.schema
    }

    pub const fn schema_fingerprint(&self) -> LogicalSchemaFingerprint {
        self.schema_fingerprint
    }

    pub const fn source_asset_id(&self) -> Uuid {
        self.source_asset_id
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    pub const fn byte_count(&self) -> usize {
        self.byte_count
    }

    pub fn payload(&self) -> &RecordBatch {
        &self.payload
    }

    pub fn into_payload(self) -> RecordBatch {
        self.payload
    }
}

/// Converts a validated logical schema to its canonical Apache Arrow 59 schema.
pub fn logical_schema_to_arrow(schema: &LogicalSchema) -> Result<SchemaRef, BatchError> {
    let fingerprint = LogicalSchemaFingerprint::try_from_schema(schema)?;
    logical_schema_to_arrow_with_fingerprint(schema, fingerprint)
}

fn logical_schema_to_arrow_with_fingerprint(
    schema: &LogicalSchema,
    fingerprint: LogicalSchemaFingerprint,
) -> Result<SchemaRef, BatchError> {
    schema.validate()?;
    let fields = schema
        .fields
        .iter()
        .map(logical_field_to_arrow)
        .collect::<Result<Vec<_>, _>>()?;
    let mut metadata = HashMap::new();
    metadata.insert(SCHEMA_VERSION_KEY.to_owned(), schema.version.to_string());
    metadata.insert(SCHEMA_FINGERPRINT_KEY.to_owned(), fingerprint.to_string());
    metadata.insert(
        SCHEMA_METADATA_KEY.to_owned(),
        encode_metadata(&schema.metadata)?,
    );
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

/// Rebuilds a logical schema from the canonical Apache Arrow 59 metadata mapping.
pub fn logical_schema_from_arrow(schema: &Schema) -> Result<LogicalSchema, BatchError> {
    let version = required_metadata(schema.metadata(), SCHEMA_VERSION_KEY)?
        .parse::<u16>()
        .map_err(|_| BatchError::InvalidReservedMetadata(SCHEMA_VERSION_KEY))?;
    let declared_fingerprint = LogicalSchemaFingerprint::from_hex(required_metadata(
        schema.metadata(),
        SCHEMA_FINGERPRINT_KEY,
    )?)?;
    let metadata = decode_metadata(
        required_metadata(schema.metadata(), SCHEMA_METADATA_KEY)?,
        SCHEMA_METADATA_KEY,
    )?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| logical_field_from_arrow(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let logical = LogicalSchema::from_parts(version, fields, metadata)?;
    let actual_fingerprint = LogicalSchemaFingerprint::try_from_schema(&logical)?;
    if declared_fingerprint != actual_fingerprint {
        return Err(BatchError::SchemaFingerprintMismatch);
    }
    Ok(logical)
}

fn logical_field_to_arrow(field: &LogicalField) -> Result<Field, BatchError> {
    field.validate()?;
    let mut metadata = HashMap::new();
    metadata.insert(COLUMN_ID_KEY.to_owned(), field.id.to_string());
    metadata.insert(
        FIELD_METADATA_KEY.to_owned(),
        encode_metadata(&field.metadata)?,
    );
    Ok(Field::new(
        field.name.clone(),
        logical_type_to_arrow(&field.data_type)?,
        field.nullable,
    )
    .with_metadata(metadata))
}

fn logical_field_from_arrow(field: &Field) -> Result<LogicalField, BatchError> {
    let id = Uuid::parse_str(required_metadata(field.metadata(), COLUMN_ID_KEY)?)
        .map(ColumnId::from_uuid)
        .map_err(|_| BatchError::InvalidReservedMetadata(COLUMN_ID_KEY))?;
    let metadata = decode_metadata(
        required_metadata(field.metadata(), FIELD_METADATA_KEY)?,
        FIELD_METADATA_KEY,
    )?;
    LogicalField::new(
        id,
        field.name().clone(),
        logical_type_from_arrow(field.data_type())?,
        field.is_nullable(),
    )?
    .with_metadata(metadata)
    .map_err(BatchError::from)
}

fn logical_type_to_arrow(data_type: &LogicalType) -> Result<DataType, BatchError> {
    Ok(match data_type {
        LogicalType::Null => DataType::Null,
        LogicalType::Boolean => DataType::Boolean,
        LogicalType::Int8 => DataType::Int8,
        LogicalType::Int16 => DataType::Int16,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Int64 => DataType::Int64,
        LogicalType::UInt8 => DataType::UInt8,
        LogicalType::UInt16 => DataType::UInt16,
        LogicalType::UInt32 => DataType::UInt32,
        LogicalType::UInt64 => DataType::UInt64,
        LogicalType::Float32 => DataType::Float32,
        LogicalType::Float64 => DataType::Float64,
        LogicalType::Utf8 => DataType::Utf8,
        LogicalType::Binary => DataType::Binary,
        LogicalType::Date32 => DataType::Date32,
        LogicalType::Timestamp { unit, timezone } => {
            DataType::Timestamp(arrow_time_unit(*unit), timezone.as_deref().map(Into::into))
        }
        LogicalType::List(element) => DataType::new_list(logical_type_to_arrow(element)?, true),
        LogicalType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(logical_field_to_arrow)
                .map(|field| field.map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
    })
}

fn logical_type_from_arrow(data_type: &DataType) -> Result<LogicalType, BatchError> {
    Ok(match data_type {
        DataType::Null => LogicalType::Null,
        DataType::Boolean => LogicalType::Boolean,
        DataType::Int8 => LogicalType::Int8,
        DataType::Int16 => LogicalType::Int16,
        DataType::Int32 => LogicalType::Int32,
        DataType::Int64 => LogicalType::Int64,
        DataType::UInt8 => LogicalType::UInt8,
        DataType::UInt16 => LogicalType::UInt16,
        DataType::UInt32 => LogicalType::UInt32,
        DataType::UInt64 => LogicalType::UInt64,
        DataType::Float32 => LogicalType::Float32,
        DataType::Float64 => LogicalType::Float64,
        DataType::Utf8 => LogicalType::Utf8,
        DataType::Binary => LogicalType::Binary,
        DataType::Date32 => LogicalType::Date32,
        DataType::Timestamp(unit, timezone) => LogicalType::Timestamp {
            unit: logical_time_unit(*unit),
            timezone: timezone.as_ref().map(|value| value.to_string()),
        },
        DataType::List(element) => {
            if element.name() != Field::LIST_FIELD_DEFAULT_NAME
                || !element.is_nullable()
                || !element.metadata().is_empty()
            {
                return Err(BatchError::NonCanonicalListElement);
            }
            LogicalType::List(Box::new(logical_type_from_arrow(element.data_type())?))
        }
        DataType::Struct(fields) => LogicalType::Struct(
            fields
                .iter()
                .map(|field| logical_field_from_arrow(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        unsupported => return Err(BatchError::UnsupportedArrowType(unsupported.to_string())),
    })
}

fn arrow_time_unit(unit: TimeUnit) -> ArrowTimeUnit {
    match unit {
        TimeUnit::Second => ArrowTimeUnit::Second,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

fn logical_time_unit(unit: ArrowTimeUnit) -> TimeUnit {
    match unit {
        ArrowTimeUnit::Second => TimeUnit::Second,
        ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn encode_metadata(metadata: &BTreeMap<String, String>) -> Result<String, BatchError> {
    serde_json::to_string(metadata)
        .map_err(|error| BatchError::SchemaSerialization(error.to_string()))
}

fn decode_metadata(value: &str, key: &'static str) -> Result<BTreeMap<String, String>, BatchError> {
    serde_json::from_str(value).map_err(|_| BatchError::InvalidReservedMetadata(key))
}

fn required_metadata<'a>(
    metadata: &'a HashMap<String, String>,
    key: &'static str,
) -> Result<&'a str, BatchError> {
    metadata
        .get(key)
        .map(String::as_str)
        .ok_or(BatchError::MissingReservedMetadata(key))
}

fn validate_batch_bounds(row_count: usize, byte_count: usize) -> Result<(), BatchError> {
    if row_count > MAX_BATCH_ROWS {
        return Err(BatchError::RowLimitExceeded {
            rows: row_count,
            maximum: MAX_BATCH_ROWS,
        });
    }
    if byte_count > MAX_BATCH_BYTES {
        return Err(BatchError::ByteLimitExceeded {
            bytes: byte_count,
            maximum: MAX_BATCH_BYTES,
        });
    }
    Ok(())
}

fn fingerprint_bytes(bytes: &[u8]) -> [u8; 32] {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut lanes = [
        0xcbf2_9ce4_8422_2325,
        0x6c62_272e_07bb_0142,
        0x9e37_79b9_7f4a_7c15,
        0xd6e8_feb8_6659_fd93,
    ];
    for byte in bytes {
        for (index, lane) in lanes.iter_mut().enumerate() {
            *lane ^= u64::from(*byte) ^ ((index as u64) << 8);
            *lane = (*lane).wrapping_mul(PRIME);
        }
    }

    let mut result = [0_u8; 32];
    for (target, lane) in result.chunks_exact_mut(8).zip(lanes) {
        target.copy_from_slice(&lane.to_be_bytes());
    }
    result
}

/// Typed failures for batch construction and logical/physical schema mapping.
#[derive(Debug, Error)]
pub enum BatchError {
    #[error("unsupported batch envelope version {0}")]
    UnsupportedEnvelopeVersion(u16),
    #[error("batch source asset id must not be nil")]
    NilSourceAssetId,
    #[error("logical schema serialization failed: {0}")]
    SchemaSerialization(String),
    #[error("Arrow schema is missing reserved metadata key {0}")]
    MissingReservedMetadata(&'static str),
    #[error("Arrow schema has invalid reserved metadata key {0}")]
    InvalidReservedMetadata(&'static str),
    #[error("Arrow schema fingerprint does not match its logical schema")]
    SchemaFingerprintMismatch,
    #[error("Arrow list element metadata is not canonical")]
    NonCanonicalListElement,
    #[error("unsupported Arrow physical type {0}")]
    UnsupportedArrowType(String),
    #[error("Arrow payload schema does not match the logical schema")]
    PhysicalSchemaMismatch,
    #[error("batch has {rows} rows; maximum is {maximum}")]
    RowLimitExceeded { rows: usize, maximum: usize },
    #[error("batch uses {bytes} bytes; maximum is {maximum}")]
    ByteLimitExceeded { bytes: usize, maximum: usize },
    #[error(transparent)]
    Logical(#[from] LogicalError),
}

impl BatchError {
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::PhysicalSchemaMismatch | Self::SchemaFingerprintMismatch => {
                ErrorCategory::SchemaDrift
            }
            _ => ErrorCategory::InvalidData,
        }
    }
}

impl From<BatchError> for ConnectorError {
    fn from(error: BatchError) -> Self {
        ConnectorError::with_category(
            error.category(),
            false,
            error.to_string(),
            Vec::new(),
            BTreeMap::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::NullArray;

    use super::*;

    fn id(value: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(value))
    }

    fn field(value: u128, name: &str, data_type: LogicalType) -> LogicalField {
        LogicalField::new(id(value), name, data_type, true).expect("valid field")
    }

    fn schema_with_type(data_type: LogicalType) -> LogicalSchema {
        LogicalSchema::new(vec![field(1, "value", data_type)]).expect("valid schema")
    }

    #[test]
    fn all_supported_types_roundtrip_through_arrow() {
        let atomic = vec![
            LogicalType::Null,
            LogicalType::Boolean,
            LogicalType::Int8,
            LogicalType::Int16,
            LogicalType::Int32,
            LogicalType::Int64,
            LogicalType::UInt8,
            LogicalType::UInt16,
            LogicalType::UInt32,
            LogicalType::UInt64,
            LogicalType::Float32,
            LogicalType::Float64,
            LogicalType::Utf8,
            LogicalType::Binary,
            LogicalType::Date32,
            LogicalType::Timestamp {
                unit: TimeUnit::Second,
                timezone: None,
            },
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: None,
            },
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("Asia/Singapore".to_owned()),
            },
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("UTC".to_owned()),
            },
        ];

        for data_type in atomic {
            let logical = schema_with_type(data_type);
            let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");
            let restored = logical_schema_from_arrow(&arrow).expect("from Arrow");
            assert_eq!(restored, logical);
        }
    }

    #[test]
    fn nested_schema_metadata_and_column_ids_roundtrip() {
        let nested = LogicalField::new(id(2), "nested", LogicalType::Utf8, false)
            .expect("valid nested field")
            .with_metadata(BTreeMap::from([("role".to_owned(), "label".to_owned())]))
            .expect("safe field metadata");
        let top = field(
            1,
            "items",
            LogicalType::List(Box::new(LogicalType::Struct(vec![nested]))),
        )
        .with_metadata(BTreeMap::from([("unit".to_owned(), "rows".to_owned())]))
        .expect("safe field metadata");
        let logical = LogicalSchema::from_parts(
            crate::LOGICAL_SCHEMA_VERSION,
            vec![top, field(3, "active", LogicalType::Boolean)],
            BTreeMap::from([("owner".to_owned(), "quality".to_owned())]),
        )
        .expect("valid schema");

        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");
        let restored = logical_schema_from_arrow(&arrow).expect("from Arrow");
        assert_eq!(restored, logical);
        assert_eq!(
            restored
                .fields
                .iter()
                .map(|field| field.id)
                .collect::<Vec<_>>(),
            vec![id(1), id(3)]
        );
        assert_eq!(
            LogicalSchemaFingerprint::try_from_schema(&restored).expect("fingerprint"),
            LogicalSchemaFingerprint::try_from_schema(&logical).expect("fingerprint")
        );
    }

    #[test]
    fn rejects_missing_metadata_unsupported_types_and_bad_fingerprint() {
        let logical = schema_with_type(LogicalType::Utf8);
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");

        let mut missing_metadata = arrow.metadata().clone();
        missing_metadata.remove(SCHEMA_VERSION_KEY);
        let missing = Schema::new_with_metadata(arrow.fields().clone(), missing_metadata);
        assert!(matches!(
            logical_schema_from_arrow(&missing),
            Err(BatchError::MissingReservedMetadata(SCHEMA_VERSION_KEY))
        ));

        let unsupported_field = arrow.field(0).clone().with_data_type(DataType::LargeUtf8);
        let unsupported =
            Schema::new_with_metadata(vec![unsupported_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&unsupported),
            Err(BatchError::UnsupportedArrowType(_))
        ));

        let mut bad_fingerprint = arrow.metadata().clone();
        bad_fingerprint.insert(SCHEMA_FINGERPRINT_KEY.to_owned(), "00".repeat(32));
        let bad = Schema::new_with_metadata(arrow.fields().clone(), bad_fingerprint);
        assert!(matches!(
            logical_schema_from_arrow(&bad),
            Err(BatchError::SchemaFingerprintMismatch)
        ));
    }

    #[test]
    fn rejects_invalid_reserved_metadata_and_noncanonical_lists() {
        let logical = schema_with_type(LogicalType::List(Box::new(LogicalType::Int64)));
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");

        let mut bad_version_metadata = arrow.metadata().clone();
        bad_version_metadata.insert(SCHEMA_VERSION_KEY.to_owned(), "2".to_owned());
        let bad_version = Schema::new_with_metadata(arrow.fields().clone(), bad_version_metadata);
        assert!(matches!(
            logical_schema_from_arrow(&bad_version),
            Err(BatchError::Logical(LogicalError::UnsupportedSchemaVersion(
                2
            )))
        ));

        let mut bad_schema_metadata = arrow.metadata().clone();
        bad_schema_metadata.insert(SCHEMA_METADATA_KEY.to_owned(), "[".to_owned());
        let bad_schema_json =
            Schema::new_with_metadata(arrow.fields().clone(), bad_schema_metadata);
        assert!(matches!(
            logical_schema_from_arrow(&bad_schema_json),
            Err(BatchError::InvalidReservedMetadata(SCHEMA_METADATA_KEY))
        ));

        let mut bad_id_metadata = arrow.field(0).metadata().clone();
        bad_id_metadata.insert(COLUMN_ID_KEY.to_owned(), "not-a-uuid".to_owned());
        let bad_id_field = arrow.field(0).clone().with_metadata(bad_id_metadata);
        let bad_id = Schema::new_with_metadata(vec![bad_id_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&bad_id),
            Err(BatchError::InvalidReservedMetadata(COLUMN_ID_KEY))
        ));

        let mut bad_field_metadata = arrow.field(0).metadata().clone();
        bad_field_metadata.insert(FIELD_METADATA_KEY.to_owned(), "[".to_owned());
        let bad_metadata_field = arrow.field(0).clone().with_metadata(bad_field_metadata);
        let bad_field_json =
            Schema::new_with_metadata(vec![bad_metadata_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&bad_field_json),
            Err(BatchError::InvalidReservedMetadata(FIELD_METADATA_KEY))
        ));

        let DataType::List(element) = arrow.field(0).data_type() else {
            panic!("canonical list field");
        };
        let noncanonical_element = Arc::new(element.as_ref().clone().with_nullable(false));
        let noncanonical_field = arrow
            .field(0)
            .clone()
            .with_data_type(DataType::List(noncanonical_element));
        let noncanonical =
            Schema::new_with_metadata(vec![noncanonical_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&noncanonical),
            Err(BatchError::NonCanonicalListElement)
        ));
    }

    #[test]
    fn envelope_accepts_empty_typed_batch_and_rejects_invalid_identity_or_version() {
        let logical = Arc::new(schema_with_type(LogicalType::Null));
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");
        let batch = RecordBatch::new_empty(arrow);
        let envelope =
            BatchEnvelope::try_new(Arc::clone(&logical), Uuid::from_u128(7), 0, batch.clone())
                .expect("valid empty envelope");
        assert_eq!(envelope.row_count(), 0);
        assert_eq!(envelope.sequence(), 0);

        assert!(matches!(
            BatchEnvelope::try_from_parts(
                BATCH_ENVELOPE_VERSION + 1,
                Arc::clone(&logical),
                Uuid::from_u128(7),
                0,
                batch.clone(),
            ),
            Err(BatchError::UnsupportedEnvelopeVersion(_))
        ));
        let invalid_schema = Arc::new(LogicalSchema {
            version: crate::LOGICAL_SCHEMA_VERSION + 1,
            fields: logical.fields.clone(),
            metadata: logical.metadata.clone(),
        });
        assert!(matches!(
            BatchEnvelope::try_new(invalid_schema, Uuid::from_u128(7), 0, batch.clone(),),
            Err(BatchError::Logical(LogicalError::UnsupportedSchemaVersion(
                _
            )))
        ));
        assert!(matches!(
            BatchEnvelope::try_new(logical, Uuid::nil(), 0, batch),
            Err(BatchError::NilSourceAssetId)
        ));
    }

    #[test]
    fn envelope_rejects_physical_schema_and_resource_bound_violations() {
        let logical = Arc::new(schema_with_type(LogicalType::Null));
        let wrong = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Null,
            true,
        )])));
        assert!(matches!(
            BatchEnvelope::try_new(Arc::clone(&logical), Uuid::from_u128(7), 0, wrong),
            Err(BatchError::PhysicalSchemaMismatch)
        ));

        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");
        let oversized =
            RecordBatch::try_new(arrow, vec![Arc::new(NullArray::new(MAX_BATCH_ROWS + 1))])
                .expect("valid Arrow batch");
        assert!(matches!(
            BatchEnvelope::try_new(logical, Uuid::from_u128(7), 0, oversized),
            Err(BatchError::RowLimitExceeded { .. })
        ));
        assert!(matches!(
            validate_batch_bounds(1, MAX_BATCH_BYTES + 1),
            Err(BatchError::ByteLimitExceeded { .. })
        ));
    }

    #[test]
    fn schema_fingerprint_is_stable_and_versioned() {
        let schema = schema_with_type(LogicalType::Int64);
        let first = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fingerprint");
        let second = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fingerprint");
        assert_eq!(first, second);
        assert_eq!(first.to_string().len(), 64);
        assert_eq!(
            LogicalSchemaFingerprint::algorithm(),
            LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM
        );
    }
}
