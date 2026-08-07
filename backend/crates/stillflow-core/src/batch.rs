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
    LogicalType, TimeUnit, MAX_SCHEMA_FIELDS, MAX_SCHEMA_NESTING_DEPTH,
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
        Self::from_validated_schema(schema)
    }

    fn from_validated_schema(schema: &LogicalSchema) -> Result<Self, BatchError> {
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

/// Reusable validated schema and lineage context for envelope construction.
#[derive(Clone)]
pub struct BatchEnvelopeFactory {
    version: u16,
    schema: Arc<LogicalSchema>,
    schema_fingerprint: LogicalSchemaFingerprint,
    arrow_schema: SchemaRef,
    source_asset_id: Uuid,
}

impl fmt::Debug for BatchEnvelopeFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchEnvelopeFactory")
            .field("version", &self.version)
            .field("schema_fingerprint", &self.schema_fingerprint)
            .field("source_asset_id", &self.source_asset_id)
            .finish_non_exhaustive()
    }
}

impl BatchEnvelopeFactory {
    pub fn try_new(schema: Arc<LogicalSchema>, source_asset_id: Uuid) -> Result<Self, BatchError> {
        Self::try_from_parts(BATCH_ENVELOPE_VERSION, schema, source_asset_id)
    }

    pub fn try_from_parts(
        version: u16,
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
    ) -> Result<Self, BatchError> {
        if version != BATCH_ENVELOPE_VERSION {
            return Err(BatchError::UnsupportedEnvelopeVersion(version));
        }
        if source_asset_id.is_nil() {
            return Err(BatchError::NilSourceAssetId);
        }

        schema.validate()?;
        let schema_fingerprint = LogicalSchemaFingerprint::from_validated_schema(schema.as_ref())?;
        let arrow_schema = logical_schema_to_arrow_validated(schema.as_ref(), schema_fingerprint)?;

        Ok(Self {
            version,
            schema,
            schema_fingerprint,
            arrow_schema,
            source_asset_id,
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

    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    pub const fn source_asset_id(&self) -> Uuid {
        self.source_asset_id
    }

    pub fn try_build(
        &self,
        sequence: u64,
        payload: RecordBatch,
    ) -> Result<BatchEnvelope, BatchError> {
        let shares_arrow_schema = Arc::ptr_eq(payload.schema_ref(), &self.arrow_schema);
        if !shares_arrow_schema && payload.schema_ref().as_ref() != self.arrow_schema.as_ref() {
            return Err(BatchError::PhysicalSchemaMismatch);
        }
        let payload = if shares_arrow_schema {
            payload
        } else {
            payload
                .with_schema(Arc::clone(&self.arrow_schema))
                .map_err(|_| BatchError::PhysicalSchemaMismatch)?
        };

        let row_count = payload.num_rows();
        let byte_count = payload.get_array_memory_size();
        validate_batch_bounds(row_count, byte_count)?;

        Ok(BatchEnvelope {
            version: self.version,
            schema: Arc::clone(&self.schema),
            schema_fingerprint: self.schema_fingerprint,
            source_asset_id: self.source_asset_id,
            sequence,
            row_count,
            byte_count,
            payload,
        })
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
        BatchEnvelopeFactory::try_from_parts(version, schema, source_asset_id)?
            .try_build(sequence, payload)
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
    schema.validate()?;
    let fingerprint = LogicalSchemaFingerprint::from_validated_schema(schema)?;
    logical_schema_to_arrow_validated(schema, fingerprint)
}

fn logical_schema_to_arrow_validated(
    schema: &LogicalSchema,
    fingerprint: LogicalSchemaFingerprint,
) -> Result<SchemaRef, BatchError> {
    let fields = schema
        .fields
        .iter()
        .map(logical_field_to_arrow_validated)
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
    let version_text = required_metadata(schema.metadata(), SCHEMA_VERSION_KEY)?;
    let fingerprint_text = required_metadata(schema.metadata(), SCHEMA_FINGERPRINT_KEY)?;
    let schema_metadata_text = required_metadata(schema.metadata(), SCHEMA_METADATA_KEY)?;
    ensure_exact_metadata_keys(
        schema.metadata(),
        &[
            SCHEMA_VERSION_KEY,
            SCHEMA_FINGERPRINT_KEY,
            SCHEMA_METADATA_KEY,
        ],
        BatchError::NonCanonicalSchemaMetadata,
    )?;

    let version = version_text
        .parse::<u16>()
        .map_err(|_| BatchError::InvalidReservedMetadata(SCHEMA_VERSION_KEY))?;
    if version.to_string() != version_text {
        return Err(BatchError::NonCanonicalSchemaMetadata);
    }

    let declared_fingerprint = LogicalSchemaFingerprint::from_hex(fingerprint_text)?;
    if declared_fingerprint.to_string() != fingerprint_text {
        return Err(BatchError::NonCanonicalSchemaMetadata);
    }

    let metadata = decode_canonical_metadata(
        schema_metadata_text,
        SCHEMA_METADATA_KEY,
        BatchError::NonCanonicalSchemaMetadata,
    )?;
    validate_arrow_schema_shape(schema)?;

    let fields = schema
        .fields()
        .iter()
        .map(|field| logical_field_from_arrow(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let logical = LogicalSchema::from_parts(version, fields, metadata)?;
    let actual_fingerprint = LogicalSchemaFingerprint::from_validated_schema(&logical)?;
    let actual_fingerprint_text = actual_fingerprint.to_string();
    if declared_fingerprint != actual_fingerprint || fingerprint_text != actual_fingerprint_text {
        return Err(BatchError::SchemaFingerprintMismatch);
    }
    Ok(logical)
}

fn logical_field_to_arrow_validated(field: &LogicalField) -> Result<Field, BatchError> {
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
    let id_text = required_metadata(field.metadata(), COLUMN_ID_KEY)?;
    let field_metadata_text = required_metadata(field.metadata(), FIELD_METADATA_KEY)?;
    ensure_exact_metadata_keys(
        field.metadata(),
        &[COLUMN_ID_KEY, FIELD_METADATA_KEY],
        BatchError::NonCanonicalFieldMetadata,
    )?;

    let id = Uuid::parse_str(id_text)
        .map(ColumnId::from_uuid)
        .map_err(|_| BatchError::InvalidReservedMetadata(COLUMN_ID_KEY))?;
    if id.to_string() != id_text {
        return Err(BatchError::NonCanonicalFieldMetadata);
    }
    let metadata = decode_canonical_metadata(
        field_metadata_text,
        FIELD_METADATA_KEY,
        BatchError::NonCanonicalFieldMetadata,
    )?;
    Ok(LogicalField {
        id,
        name: field.name().clone(),
        data_type: logical_type_from_arrow(field.data_type())?,
        nullable: field.is_nullable(),
        metadata,
    })
}

fn validate_arrow_schema_shape(schema: &Schema) -> Result<(), BatchError> {
    let mut fields_seen = 0_usize;
    let mut stack = Vec::new();
    push_arrow_fields(schema.fields(), 1, &mut fields_seen, &mut stack)?;

    while let Some((data_type, depth)) = stack.pop() {
        if depth > MAX_SCHEMA_NESTING_DEPTH {
            return Err(LogicalError::SchemaNestingDepthExceeded {
                depth,
                maximum: MAX_SCHEMA_NESTING_DEPTH,
            }
            .into());
        }
        match data_type {
            DataType::List(element) => {
                if element.name() != Field::LIST_FIELD_DEFAULT_NAME
                    || !element.is_nullable()
                    || !element.metadata().is_empty()
                {
                    return Err(BatchError::NonCanonicalListElement);
                }
                stack.push((element.data_type(), next_arrow_depth(depth)?));
            }
            DataType::Struct(fields) => {
                push_arrow_fields(
                    fields,
                    next_arrow_depth(depth)?,
                    &mut fields_seen,
                    &mut stack,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn push_arrow_fields<'a>(
    fields: &'a arrow_schema::Fields,
    type_depth: usize,
    fields_seen: &mut usize,
    stack: &mut Vec<(&'a DataType, usize)>,
) -> Result<(), BatchError> {
    for field in fields {
        *fields_seen =
            (*fields_seen)
                .checked_add(1)
                .ok_or(LogicalError::SchemaFieldLimitExceeded {
                    fields: usize::MAX,
                    maximum: MAX_SCHEMA_FIELDS,
                })?;
        if *fields_seen > MAX_SCHEMA_FIELDS {
            return Err(LogicalError::SchemaFieldLimitExceeded {
                fields: *fields_seen,
                maximum: MAX_SCHEMA_FIELDS,
            }
            .into());
        }
        required_metadata(field.metadata(), COLUMN_ID_KEY)?;
        required_metadata(field.metadata(), FIELD_METADATA_KEY)?;
        ensure_exact_metadata_keys(
            field.metadata(),
            &[COLUMN_ID_KEY, FIELD_METADATA_KEY],
            BatchError::NonCanonicalFieldMetadata,
        )?;
    }
    for field in fields.iter().rev() {
        stack.push((field.data_type(), type_depth));
    }
    Ok(())
}

fn next_arrow_depth(depth: usize) -> Result<usize, BatchError> {
    depth.checked_add(1).ok_or_else(|| {
        LogicalError::SchemaNestingDepthExceeded {
            depth: usize::MAX,
            maximum: MAX_SCHEMA_NESTING_DEPTH,
        }
        .into()
    })
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
                .map(logical_field_to_arrow_validated)
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

fn decode_canonical_metadata(
    value: &str,
    key: &'static str,
    noncanonical: BatchError,
) -> Result<BTreeMap<String, String>, BatchError> {
    let metadata = decode_metadata(value, key)?;
    if encode_metadata(&metadata)? != value {
        return Err(noncanonical);
    }
    Ok(metadata)
}

fn ensure_exact_metadata_keys(
    metadata: &HashMap<String, String>,
    expected: &[&str],
    noncanonical: BatchError,
) -> Result<(), BatchError> {
    if metadata.len() != expected.len() || expected.iter().any(|key| !metadata.contains_key(*key)) {
        return Err(noncanonical);
    }
    Ok(())
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
    #[error("Arrow schema metadata is not canonical")]
    NonCanonicalSchemaMetadata,
    #[error("Arrow field metadata is not canonical")]
    NonCanonicalFieldMetadata,
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
    fn strict_decoder_rejects_noncanonical_schema_metadata() {
        let logical = schema_with_type(LogicalType::Utf8);
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");

        let mut extra = arrow.metadata().clone();
        extra.insert("foreign".to_owned(), "metadata".to_owned());
        assert!(matches!(
            logical_schema_from_arrow(&Schema::new_with_metadata(arrow.fields().clone(), extra)),
            Err(BatchError::NonCanonicalSchemaMetadata)
        ));

        let mut padded_version = arrow.metadata().clone();
        padded_version.insert(SCHEMA_VERSION_KEY.to_owned(), "01".to_owned());
        assert!(matches!(
            logical_schema_from_arrow(&Schema::new_with_metadata(
                arrow.fields().clone(),
                padded_version
            )),
            Err(BatchError::NonCanonicalSchemaMetadata)
        ));

        let mut uppercase_fingerprint = arrow.metadata().clone();
        uppercase_fingerprint.insert(SCHEMA_FINGERPRINT_KEY.to_owned(), "AA".repeat(32));
        assert!(matches!(
            logical_schema_from_arrow(&Schema::new_with_metadata(
                arrow.fields().clone(),
                uppercase_fingerprint
            )),
            Err(BatchError::NonCanonicalSchemaMetadata)
        ));

        let mut spaced_json = arrow.metadata().clone();
        spaced_json.insert(SCHEMA_METADATA_KEY.to_owned(), "{ }".to_owned());
        assert!(matches!(
            logical_schema_from_arrow(&Schema::new_with_metadata(
                arrow.fields().clone(),
                spaced_json
            )),
            Err(BatchError::NonCanonicalSchemaMetadata)
        ));
    }

    #[test]
    fn strict_decoder_rejects_noncanonical_field_metadata() {
        let logical = schema_with_type(LogicalType::Utf8);
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");

        let mut extra = arrow.field(0).metadata().clone();
        extra.insert("foreign".to_owned(), "metadata".to_owned());
        let extra_field = arrow.field(0).clone().with_metadata(extra);
        let extra_schema = Schema::new_with_metadata(vec![extra_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&extra_schema),
            Err(BatchError::NonCanonicalFieldMetadata)
        ));

        let mut compact_id = arrow.field(0).metadata().clone();
        compact_id.insert(
            COLUMN_ID_KEY.to_owned(),
            required_metadata(arrow.field(0).metadata(), COLUMN_ID_KEY)
                .expect("column id")
                .replace('-', ""),
        );
        let compact_field = arrow.field(0).clone().with_metadata(compact_id);
        let compact_schema =
            Schema::new_with_metadata(vec![compact_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&compact_schema),
            Err(BatchError::NonCanonicalFieldMetadata)
        ));

        let mut spaced_json = arrow.field(0).metadata().clone();
        spaced_json.insert(FIELD_METADATA_KEY.to_owned(), "{ }".to_owned());
        let spaced_field = arrow.field(0).clone().with_metadata(spaced_json);
        let spaced_schema = Schema::new_with_metadata(vec![spaced_field], arrow.metadata().clone());
        assert!(matches!(
            logical_schema_from_arrow(&spaced_schema),
            Err(BatchError::NonCanonicalFieldMetadata)
        ));
    }

    #[test]
    fn strict_decoder_rejects_arrow_nesting_beyond_the_logical_limit() {
        let logical = schema_with_type(LogicalType::Int64);
        let arrow = logical_schema_to_arrow(&logical).expect("to Arrow");
        let mut over_limit_type = DataType::Int64;
        for _ in 1..=MAX_SCHEMA_NESTING_DEPTH {
            over_limit_type = DataType::new_list(over_limit_type, true);
        }
        let over_limit_field = arrow.field(0).clone().with_data_type(over_limit_type);
        let over_limit =
            Schema::new_with_metadata(vec![over_limit_field], arrow.metadata().clone());

        assert!(matches!(
            logical_schema_from_arrow(&over_limit),
            Err(BatchError::Logical(
                LogicalError::SchemaNestingDepthExceeded {
                    depth,
                    maximum: MAX_SCHEMA_NESTING_DEPTH
                }
            )) if depth == MAX_SCHEMA_NESTING_DEPTH + 1
        ));
    }

    #[test]
    fn factory_reuses_validated_logical_and_arrow_schemas() {
        let logical = Arc::new(schema_with_type(LogicalType::Int64));
        let source = Uuid::from_u128(7);
        let factory = BatchEnvelopeFactory::try_new(Arc::clone(&logical), source).expect("factory");
        let first_batch = RecordBatch::new_empty(Arc::clone(factory.arrow_schema()));
        let structurally_equal_schema = Arc::new(factory.arrow_schema().as_ref().clone());
        let second_batch = RecordBatch::new_empty(structurally_equal_schema);

        let first = factory.try_build(0, first_batch).expect("first envelope");
        let second = factory.try_build(1, second_batch).expect("second envelope");

        assert!(Arc::ptr_eq(first.shared_schema(), second.shared_schema()));
        assert!(Arc::ptr_eq(
            first.payload().schema_ref(),
            second.payload().schema_ref()
        ));
        assert!(Arc::ptr_eq(
            first.payload().schema_ref(),
            factory.arrow_schema()
        ));
        assert_eq!(first.schema_fingerprint(), second.schema_fingerprint());
        assert_eq!(first.source_asset_id(), second.source_asset_id());
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
