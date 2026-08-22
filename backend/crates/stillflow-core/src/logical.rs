use std::collections::{BTreeMap, HashSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::ensure_no_secret_fields;

/// Current wire-format version for logical schemas.
pub const LOGICAL_SCHEMA_VERSION: u16 = 1;

/// Maximum version 1 nesting depth across list elements and struct fields.
pub const MAX_SCHEMA_NESTING_DEPTH: usize = 64;

/// Maximum number of logical fields, including nested struct fields.
pub const MAX_SCHEMA_FIELDS: usize = 4_096;

/// Maximum cumulative UTF-8 bytes in schema names, timezones and metadata.
pub const MAX_SCHEMA_TEXT_BYTES: usize = 1024 * 1024;

/// Stable identity of a logical column, independent of its display name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnId(Uuid);

impl ColumnId {
    /// Generates an identity for a newly created column.
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical timestamp precision ordered from coarsest to finest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// Engine-independent logical value types used by schemas and expressions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum LogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    List(Box<LogicalType>),
    Struct(Vec<LogicalField>),
}

impl LogicalType {
    /// Computes the deterministic least upper bound defined by schema version 1.
    pub fn least_upper_bound(&self, other: &Self) -> Result<Self, LogicalError> {
        use LogicalType::{
            Binary, Boolean, Date32, Float32, Float64, List, Null, Struct, Timestamp, Utf8,
        };

        if self == other {
            return Ok(self.clone());
        }
        if matches!(self, Null) {
            return Ok(other.clone());
        }
        if matches!(other, Null) {
            return Ok(self.clone());
        }

        if let (Some(left), Some(right)) = (signed_rank(self), signed_rank(other)) {
            return Ok(signed_type(left.max(right)));
        }
        if let (Some(left), Some(right)) = (unsigned_rank(self), unsigned_rank(other)) {
            return Ok(unsigned_type(left.max(right)));
        }
        if let (Some(left), Some(right)) = (float_rank(self), float_rank(other)) {
            return Ok(if left.max(right) == 32 {
                Float32
            } else {
                Float64
            });
        }
        if is_numeric(self) && is_numeric(other) {
            return Ok(Float64);
        }

        match (self, other) {
            (
                Timestamp {
                    unit: left_unit,
                    timezone: left_timezone,
                },
                Timestamp {
                    unit: right_unit,
                    timezone: right_timezone,
                },
            ) if left_timezone == right_timezone => Ok(Timestamp {
                unit: (*left_unit).max(*right_unit),
                timezone: left_timezone.clone(),
            }),
            (List(left), List(right)) => Ok(List(Box::new(left.least_upper_bound(right)?))),
            (Struct(left), Struct(right)) => join_struct_fields(left, right).map(Struct),
            (Boolean, Boolean) | (Utf8, Utf8) | (Binary, Binary) | (Date32, Date32) => {
                Ok(self.clone())
            }
            _ => Err(LogicalError::IncompatibleTypes {
                left: Box::new(self.clone()),
                right: Box::new(other.clone()),
            }),
        }
    }

    pub fn validate(&self) -> Result<(), LogicalError> {
        validate_type(self)
    }
}

fn signed_rank(data_type: &LogicalType) -> Option<u8> {
    match data_type {
        LogicalType::Int8 => Some(8),
        LogicalType::Int16 => Some(16),
        LogicalType::Int32 => Some(32),
        LogicalType::Int64 => Some(64),
        _ => None,
    }
}

fn signed_type(rank: u8) -> LogicalType {
    match rank {
        8 => LogicalType::Int8,
        16 => LogicalType::Int16,
        32 => LogicalType::Int32,
        _ => LogicalType::Int64,
    }
}

fn unsigned_rank(data_type: &LogicalType) -> Option<u8> {
    match data_type {
        LogicalType::UInt8 => Some(8),
        LogicalType::UInt16 => Some(16),
        LogicalType::UInt32 => Some(32),
        LogicalType::UInt64 => Some(64),
        _ => None,
    }
}

fn unsigned_type(rank: u8) -> LogicalType {
    match rank {
        8 => LogicalType::UInt8,
        16 => LogicalType::UInt16,
        32 => LogicalType::UInt32,
        _ => LogicalType::UInt64,
    }
}

fn float_rank(data_type: &LogicalType) -> Option<u8> {
    match data_type {
        LogicalType::Float32 => Some(32),
        LogicalType::Float64 => Some(64),
        _ => None,
    }
}

fn is_numeric(data_type: &LogicalType) -> bool {
    signed_rank(data_type).is_some()
        || unsigned_rank(data_type).is_some()
        || float_rank(data_type).is_some()
}

fn join_struct_fields(
    left: &[LogicalField],
    right: &[LogicalField],
) -> Result<Vec<LogicalField>, LogicalError> {
    if left.len() != right.len() {
        return Err(LogicalError::IncompatibleTypes {
            left: Box::new(LogicalType::Struct(left.to_vec())),
            right: Box::new(LogicalType::Struct(right.to_vec())),
        });
    }

    left.iter()
        .zip(right)
        .map(|(left_field, right_field)| {
            if left_field.id != right_field.id
                || left_field.name != right_field.name
                || left_field.metadata != right_field.metadata
            {
                return Err(LogicalError::IncompatibleTypes {
                    left: Box::new(LogicalType::Struct(left.to_vec())),
                    right: Box::new(LogicalType::Struct(right.to_vec())),
                });
            }
            Ok(LogicalField {
                id: left_field.id,
                name: left_field.name.clone(),
                data_type: left_field
                    .data_type
                    .least_upper_bound(&right_field.data_type)?,
                nullable: left_field.nullable || right_field.nullable,
                metadata: left_field.metadata.clone(),
            })
        })
        .collect()
}

/// One ordered field in a logical schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalField {
    pub id: ColumnId,
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl LogicalField {
    pub fn new(
        id: ColumnId,
        name: impl Into<String>,
        data_type: LogicalType,
        nullable: bool,
    ) -> Result<Self, LogicalError> {
        let field = Self {
            id,
            name: name.into(),
            data_type,
            nullable,
            metadata: BTreeMap::new(),
        };
        field.validate()?;
        Ok(field)
    }

    pub fn with_metadata(
        mut self,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, LogicalError> {
        self.metadata = metadata;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), LogicalError> {
        validate_fields(std::slice::from_ref(self))
    }
}

/// Versioned logical schema with stable column identities and ordered fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalSchema {
    pub version: u16,
    pub fields: Vec<LogicalField>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogicalSchemaData {
    version: u16,
    fields: Vec<LogicalField>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for LogicalSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = LogicalSchemaData::deserialize(deserializer)?;
        Self::from_parts(data.version, data.fields, data.metadata).map_err(serde::de::Error::custom)
    }
}

impl LogicalSchema {
    pub fn new(fields: Vec<LogicalField>) -> Result<Self, LogicalError> {
        Self::from_parts(LOGICAL_SCHEMA_VERSION, fields, BTreeMap::new())
    }

    pub fn empty() -> Self {
        Self {
            version: LOGICAL_SCHEMA_VERSION,
            fields: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn from_parts(
        version: u16,
        fields: Vec<LogicalField>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, LogicalError> {
        let schema = Self {
            version,
            fields,
            metadata,
        };
        schema.validate()?;
        Ok(schema)
    }

    pub fn validate(&self) -> Result<(), LogicalError> {
        if self.version != LOGICAL_SCHEMA_VERSION {
            return Err(LogicalError::UnsupportedSchemaVersion(self.version));
        }
        validate_schema(&self.fields, &self.metadata)
    }

    pub fn field(&self, id: ColumnId) -> Option<&LogicalField> {
        self.fields.iter().find(|field| field.id == id)
    }

    pub fn rename_column(
        &mut self,
        id: ColumnId,
        new_name: impl Into<String>,
    ) -> Result<(), LogicalError> {
        let index = self
            .fields
            .iter()
            .position(|field| field.id == id)
            .ok_or(LogicalError::UnknownColumn(id))?;
        let field = self
            .fields
            .get_mut(index)
            .ok_or(LogicalError::UnknownColumn(id))?;
        let previous = std::mem::replace(&mut field.name, new_name.into());
        if let Err(error) = self.validate() {
            if let Some(field) = self.fields.iter_mut().find(|field| field.id == id) {
                field.name = previous;
            }
            return Err(error);
        }
        Ok(())
    }

    /// Freezes the schema into the canonical descriptor byte encoding of
    /// contract section 8.1.1 (`canonical_schema_bytes`).
    ///
    /// The encoding is total over every [`LogicalType`] variant, including
    /// nested `List` and `Struct`. Multi-byte integers are little-endian,
    /// UUIDs use their 16 `Uuid::as_bytes()` bytes, and metadata maps are
    /// emitted sorted by UTF-8 key bytes (`BTreeMap` iteration order). The
    /// exact required length is computed before any allocation; exceeding an
    /// addressable length fails with [`LogicalError::CanonicalEncodingOverflow`]
    /// instead of panicking or truncating.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, LogicalError> {
        let mut counter = CountingSink::default();
        self.encode_canonical_into(&mut counter)?;
        let mut out = Vec::new();
        out.try_reserve_exact(counter.length)
            .map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
        let mut sink = VecSink { out };
        self.encode_canonical_into(&mut sink)?;
        debug_assert_eq!(sink.out.len(), counter.length);
        Ok(sink.out)
    }

    fn encode_canonical_into<S: EncodingSink>(&self, sink: &mut S) -> Result<(), LogicalError> {
        sink.put(&self.version.to_le_bytes())?;
        let field_count = u32::try_from(self.fields.len())
            .map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
        sink.put(&field_count.to_le_bytes())?;
        let mut tasks = vec![EncodeTask::Fields(&self.fields)];
        run_encode_tasks(&mut tasks, sink)?;
        encode_metadata_block(&self.metadata, sink)
    }
}

/// Fixed `logical_type_tag` values of the frozen schema-descriptor table
/// (contract section 8.1.1). This namespace mirrors, but is distinct from,
/// the section 6.4 value tags.
mod canonical_type_tags {
    pub const NULL: u8 = 0x00;
    pub const BOOLEAN: u8 = 0x01;
    pub const INT8: u8 = 0x02;
    pub const INT16: u8 = 0x03;
    pub const INT32: u8 = 0x04;
    pub const INT64: u8 = 0x05;
    pub const UINT8: u8 = 0x06;
    pub const UINT16: u8 = 0x07;
    pub const UINT32: u8 = 0x08;
    pub const UINT64: u8 = 0x09;
    pub const FLOAT32: u8 = 0x0A;
    pub const FLOAT64: u8 = 0x0B;
    pub const UTF8: u8 = 0x0C;
    pub const BINARY: u8 = 0x0D;
    pub const DATE32: u8 = 0x0E;
    pub const TIMESTAMP: u8 = 0x0F;
    pub const LIST: u8 = 0x10;
    pub const STRUCT: u8 = 0x11;
}

/// Fixed timestamp unit-tag values of the frozen descriptor table
/// (contract section 8.1.1: `0=Second`, `1=Millisecond`, `2=Microsecond`,
/// `3=Nanosecond`).
fn canonical_time_unit_tag(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0x00,
        TimeUnit::Millisecond => 0x01,
        TimeUnit::Microsecond => 0x02,
        TimeUnit::Nanosecond => 0x03,
    }
}

trait EncodingSink {
    fn put(&mut self, bytes: &[u8]) -> Result<(), LogicalError>;
}

#[derive(Default)]
struct CountingSink {
    length: usize,
}

impl EncodingSink for CountingSink {
    fn put(&mut self, bytes: &[u8]) -> Result<(), LogicalError> {
        self.length = self
            .length
            .checked_add(bytes.len())
            .ok_or(LogicalError::CanonicalEncodingOverflow)?;
        Ok(())
    }
}

struct VecSink {
    out: Vec<u8>,
}

impl EncodingSink for VecSink {
    fn put(&mut self, bytes: &[u8]) -> Result<(), LogicalError> {
        self.out.extend_from_slice(bytes);
        Ok(())
    }
}

enum EncodeTask<'a> {
    Field(&'a LogicalField),
    Fields(&'a [LogicalField]),
    Type(&'a LogicalType),
    Metadata(&'a BTreeMap<String, String>),
}

fn run_encode_tasks<S: EncodingSink>(
    tasks: &mut Vec<EncodeTask<'_>>,
    sink: &mut S,
) -> Result<(), LogicalError> {
    while let Some(task) = tasks.pop() {
        match task {
            EncodeTask::Fields(fields) => {
                tasks.extend(fields.iter().rev().map(EncodeTask::Field));
            }
            EncodeTask::Field(field) => {
                let name = field.name.as_bytes();
                let name_length = u32::try_from(name.len())
                    .map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
                sink.put(&name_length.to_le_bytes())?;
                sink.put(name)?;
                sink.put(field.id.as_uuid().as_bytes())?;
                sink.put(&[u8::from(field.nullable)])?;
                // Stack order emits the type encoding before the field
                // metadata block.
                tasks.push(EncodeTask::Metadata(&field.metadata));
                tasks.push(EncodeTask::Type(&field.data_type));
            }
            EncodeTask::Type(data_type) => encode_canonical_type(data_type, tasks, sink)?,
            EncodeTask::Metadata(metadata) => encode_metadata_block(metadata, sink)?,
        }
    }
    Ok(())
}

fn encode_canonical_type<'a, S: EncodingSink>(
    data_type: &'a LogicalType,
    tasks: &mut Vec<EncodeTask<'a>>,
    sink: &mut S,
) -> Result<(), LogicalError> {
    match data_type {
        LogicalType::Null => sink.put(&[canonical_type_tags::NULL]),
        LogicalType::Boolean => sink.put(&[canonical_type_tags::BOOLEAN]),
        LogicalType::Int8 => sink.put(&[canonical_type_tags::INT8]),
        LogicalType::Int16 => sink.put(&[canonical_type_tags::INT16]),
        LogicalType::Int32 => sink.put(&[canonical_type_tags::INT32]),
        LogicalType::Int64 => sink.put(&[canonical_type_tags::INT64]),
        LogicalType::UInt8 => sink.put(&[canonical_type_tags::UINT8]),
        LogicalType::UInt16 => sink.put(&[canonical_type_tags::UINT16]),
        LogicalType::UInt32 => sink.put(&[canonical_type_tags::UINT32]),
        LogicalType::UInt64 => sink.put(&[canonical_type_tags::UINT64]),
        LogicalType::Float32 => sink.put(&[canonical_type_tags::FLOAT32]),
        LogicalType::Float64 => sink.put(&[canonical_type_tags::FLOAT64]),
        LogicalType::Utf8 => sink.put(&[canonical_type_tags::UTF8]),
        LogicalType::Binary => sink.put(&[canonical_type_tags::BINARY]),
        LogicalType::Date32 => sink.put(&[canonical_type_tags::DATE32]),
        LogicalType::Timestamp { unit, timezone } => {
            sink.put(&[
                canonical_type_tags::TIMESTAMP,
                canonical_time_unit_tag(*unit),
            ])?;
            match timezone {
                None => sink.put(&[0x00]),
                Some(timezone) => {
                    let bytes = timezone.as_bytes();
                    let length = u32::try_from(bytes.len())
                        .map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
                    sink.put(&[0x01])?;
                    sink.put(&length.to_le_bytes())?;
                    sink.put(bytes)
                }
            }
        }
        LogicalType::List(element) => {
            sink.put(&[canonical_type_tags::LIST])?;
            tasks.push(EncodeTask::Type(element));
            Ok(())
        }
        LogicalType::Struct(fields) => {
            sink.put(&[canonical_type_tags::STRUCT])?;
            let field_count =
                u32::try_from(fields.len()).map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
            sink.put(&field_count.to_le_bytes())?;
            tasks.push(EncodeTask::Fields(fields));
            Ok(())
        }
    }
}

fn encode_metadata_block<S: EncodingSink>(
    metadata: &BTreeMap<String, String>,
    sink: &mut S,
) -> Result<(), LogicalError> {
    let count =
        u32::try_from(metadata.len()).map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
    sink.put(&count.to_le_bytes())?;
    // BTreeMap iterates in ascending key-byte order, which is exactly the
    // frozen metadata ordering.
    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        let key_length =
            u32::try_from(key_bytes.len()).map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
        let value_bytes = value.as_bytes();
        let value_length = u32::try_from(value_bytes.len())
            .map_err(|_| LogicalError::CanonicalEncodingOverflow)?;
        sink.put(&key_length.to_le_bytes())?;
        sink.put(key_bytes)?;
        sink.put(&value_length.to_le_bytes())?;
        sink.put(value_bytes)?;
    }
    Ok(())
}

#[derive(Default)]
struct SchemaBudget {
    fields: usize,
    text_bytes: usize,
}

impl SchemaBudget {
    fn add_field(&mut self) -> Result<(), LogicalError> {
        self.fields = self
            .fields
            .checked_add(1)
            .ok_or(LogicalError::SchemaFieldLimitExceeded {
                fields: usize::MAX,
                maximum: MAX_SCHEMA_FIELDS,
            })?;
        if self.fields > MAX_SCHEMA_FIELDS {
            return Err(LogicalError::SchemaFieldLimitExceeded {
                fields: self.fields,
                maximum: MAX_SCHEMA_FIELDS,
            });
        }
        Ok(())
    }

    fn add_text(&mut self, bytes: usize) -> Result<(), LogicalError> {
        self.text_bytes =
            self.text_bytes
                .checked_add(bytes)
                .ok_or(LogicalError::SchemaTextLimitExceeded {
                    bytes: usize::MAX,
                    maximum: MAX_SCHEMA_TEXT_BYTES,
                })?;
        if self.text_bytes > MAX_SCHEMA_TEXT_BYTES {
            return Err(LogicalError::SchemaTextLimitExceeded {
                bytes: self.text_bytes,
                maximum: MAX_SCHEMA_TEXT_BYTES,
            });
        }
        Ok(())
    }
}

enum ValidationNode<'a> {
    Type {
        data_type: &'a LogicalType,
        depth: usize,
    },
    Fields {
        fields: &'a [LogicalField],
        type_depth: usize,
    },
}

fn validate_type(data_type: &LogicalType) -> Result<(), LogicalError> {
    validate_nodes(
        vec![ValidationNode::Type {
            data_type,
            depth: 1,
        }],
        SchemaBudget::default(),
    )
}

fn validate_fields(fields: &[LogicalField]) -> Result<(), LogicalError> {
    validate_nodes(
        vec![ValidationNode::Fields {
            fields,
            type_depth: 1,
        }],
        SchemaBudget::default(),
    )
}

fn validate_schema(
    fields: &[LogicalField],
    metadata: &BTreeMap<String, String>,
) -> Result<(), LogicalError> {
    let mut budget = SchemaBudget::default();
    validate_metadata(metadata, &mut budget)?;
    validate_nodes(
        vec![ValidationNode::Fields {
            fields,
            type_depth: 1,
        }],
        budget,
    )
}

fn validate_nodes(
    mut stack: Vec<ValidationNode<'_>>,
    mut budget: SchemaBudget,
) -> Result<(), LogicalError> {
    while let Some(node) = stack.pop() {
        match node {
            ValidationNode::Type { data_type, depth } => {
                if depth > MAX_SCHEMA_NESTING_DEPTH {
                    return Err(LogicalError::SchemaNestingDepthExceeded {
                        depth,
                        maximum: MAX_SCHEMA_NESTING_DEPTH,
                    });
                }
                match data_type {
                    LogicalType::Timestamp {
                        timezone: Some(timezone),
                        ..
                    } => {
                        if timezone.trim().is_empty() {
                            return Err(LogicalError::EmptyTimezone);
                        }
                        budget.add_text(timezone.len())?;
                    }
                    LogicalType::List(element) => {
                        let child_depth = next_depth(depth)?;
                        stack.push(ValidationNode::Type {
                            data_type: element,
                            depth: child_depth,
                        });
                    }
                    LogicalType::Struct(fields) => {
                        stack.push(ValidationNode::Fields {
                            fields,
                            type_depth: next_depth(depth)?,
                        });
                    }
                    _ => {}
                }
            }
            ValidationNode::Fields { fields, type_depth } => {
                let mut ids = HashSet::new();
                let mut names = HashSet::new();
                for field in fields {
                    if field.name.trim().is_empty() {
                        return Err(LogicalError::EmptyColumnName(field.id));
                    }
                    budget.add_field()?;
                    budget.add_text(field.name.len())?;
                    validate_metadata(&field.metadata, &mut budget)?;
                    if !ids.insert(field.id) {
                        return Err(LogicalError::DuplicateColumnId(field.id));
                    }
                    if !names.insert(field.name.as_str()) {
                        return Err(LogicalError::DuplicateColumnName(field.name.clone()));
                    }
                }
                for field in fields.iter().rev() {
                    stack.push(ValidationNode::Type {
                        data_type: &field.data_type,
                        depth: type_depth,
                    });
                }
            }
        }
    }
    Ok(())
}

fn next_depth(depth: usize) -> Result<usize, LogicalError> {
    depth
        .checked_add(1)
        .ok_or(LogicalError::SchemaNestingDepthExceeded {
            depth: usize::MAX,
            maximum: MAX_SCHEMA_NESTING_DEPTH,
        })
}

fn validate_metadata(
    metadata: &BTreeMap<String, String>,
    budget: &mut SchemaBudget,
) -> Result<(), LogicalError> {
    for (key, value) in metadata {
        budget.add_text(key.len())?;
        budget.add_text(value.len())?;
    }
    let value = serde_json::to_value(metadata).map_err(|_| LogicalError::UnsafeMetadata)?;
    ensure_no_secret_fields(&value).map_err(|_| LogicalError::UnsafeMetadata)
}

/// Validation failures for logical schemas, types, and expressions.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LogicalError {
    #[error("unsupported logical schema version {0}")]
    UnsupportedSchemaVersion(u16),
    #[error("column {0} has an empty name")]
    EmptyColumnName(ColumnId),
    #[error("duplicate column id {0}")]
    DuplicateColumnId(ColumnId),
    #[error("duplicate column name `{0}`")]
    DuplicateColumnName(String),
    #[error("unknown column id {0}")]
    UnknownColumn(ColumnId),
    #[error("logical types are incompatible: {left:?} and {right:?}")]
    IncompatibleTypes {
        left: Box<LogicalType>,
        right: Box<LogicalType>,
    },
    #[error("timestamp timezone must not be empty")]
    EmptyTimezone,
    #[error("schema nesting depth {depth} exceeds maximum {maximum}")]
    SchemaNestingDepthExceeded { depth: usize, maximum: usize },
    #[error("schema field count {fields} exceeds maximum {maximum}")]
    SchemaFieldLimitExceeded { fields: usize, maximum: usize },
    #[error("schema text uses {bytes} bytes; maximum is {maximum}")]
    SchemaTextLimitExceeded { bytes: usize, maximum: usize },
    #[error("schema metadata contains a forbidden secret-like field or value")]
    UnsafeMetadata,
    #[error("canonical schema encoding exceeded an addressable length")]
    CanonicalEncodingOverflow,
    #[error("floating-point literal must be finite")]
    NonFiniteFloat,
    #[error("coalesce expression must contain at least one operand")]
    EmptyCoalesce,
    #[error("expression literal contains a forbidden secret-like value")]
    UnsafeLiteral,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(value))
    }

    fn column(value: u128, name: &str, data_type: LogicalType) -> LogicalField {
        LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(value)),
            name,
            data_type,
            false,
        )
        .expect("valid field")
    }

    fn atomic_types() -> Vec<LogicalType> {
        vec![
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
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_owned()),
            },
        ]
    }

    #[test]
    fn rename_preserves_column_identity() {
        let id = ColumnId::from_uuid(Uuid::from_u128(1));
        let mut schema =
            LogicalSchema::new(vec![column(1, "old", LogicalType::Utf8)]).expect("schema");
        schema.rename_column(id, "new").expect("rename");
        assert_eq!(schema.fields[0].id, id);
        assert_eq!(schema.fields[0].name, "new");
    }

    #[test]
    fn rejects_duplicate_ids_and_names() {
        let duplicate_id = vec![
            column(1, "left", LogicalType::Utf8),
            column(1, "right", LogicalType::Utf8),
        ];
        assert!(matches!(
            LogicalSchema::new(duplicate_id),
            Err(LogicalError::DuplicateColumnId(_))
        ));

        let duplicate_name = vec![
            column(1, "same", LogicalType::Utf8),
            column(2, "same", LogicalType::Utf8),
        ];
        assert!(matches!(
            LogicalSchema::new(duplicate_name),
            Err(LogicalError::DuplicateColumnName(_))
        ));
    }

    #[test]
    fn rejects_invalid_version_during_deserialization() {
        let value = serde_json::json!({ "version": 2, "fields": [] });
        serde_json::from_value::<LogicalSchema>(value).expect_err("version must fail");
    }

    #[test]
    fn widening_is_a_partial_semilattice_for_atomic_types() {
        let types = atomic_types();

        for left in &types {
            assert_eq!(
                left.least_upper_bound(left).expect("idempotent join"),
                *left
            );
            for right in &types {
                match (left.least_upper_bound(right), right.least_upper_bound(left)) {
                    (Ok(left_result), Ok(right_result)) => {
                        assert_eq!(left_result, right_result, "commutativity")
                    }
                    (Err(_), Err(_)) => {}
                    results => panic!("commutativity defined on one side only: {results:?}"),
                }

                for third in &types {
                    let first = left
                        .least_upper_bound(right)
                        .and_then(|joined| joined.least_upper_bound(third));
                    let second = right
                        .least_upper_bound(third)
                        .and_then(|joined| left.least_upper_bound(&joined));
                    match (first, second) {
                        (Ok(first), Ok(second)) => {
                            assert_eq!(first, second, "associativity")
                        }
                        (Err(_), Err(_)) => {}
                        results => panic!("associativity defined on one side only: {results:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn nested_widening_preserves_shape_and_joins_nullability() {
        let left = LogicalType::Struct(vec![column(1, "value", LogicalType::Int16)]);
        let mut right_field = column(1, "value", LogicalType::Int64);
        right_field.nullable = true;
        let right = LogicalType::Struct(vec![right_field]);
        let result = left.least_upper_bound(&right).expect("compatible structs");
        let LogicalType::Struct(fields) = result else {
            panic!("struct expected");
        };
        assert_eq!(fields[0].data_type, LogicalType::Int64);
        assert!(fields[0].nullable);
    }

    fn nested_list(depth: usize) -> LogicalType {
        let mut data_type = LogicalType::Int64;
        for _ in 1..depth {
            data_type = LogicalType::List(Box::new(data_type));
        }
        data_type
    }

    #[test]
    fn enforces_schema_nesting_depth_without_recursive_validation() {
        nested_list(MAX_SCHEMA_NESTING_DEPTH)
            .validate()
            .expect("exact nesting limit");
        assert!(matches!(
            nested_list(MAX_SCHEMA_NESTING_DEPTH + 1).validate(),
            Err(LogicalError::SchemaNestingDepthExceeded {
                depth,
                maximum: MAX_SCHEMA_NESTING_DEPTH
            }) if depth == MAX_SCHEMA_NESTING_DEPTH + 1
        ));
    }

    #[test]
    fn enforces_total_field_limit_including_nested_structs() {
        let schema_with_nested_fields = |nested_fields: usize| {
            let nested = (1..=nested_fields)
                .map(|value| LogicalField {
                    id: id(u128::try_from(value).expect("field id")),
                    name: format!("field-{value}"),
                    data_type: LogicalType::Null,
                    nullable: false,
                    metadata: BTreeMap::new(),
                })
                .collect::<Vec<_>>();
            LogicalSchema {
                version: LOGICAL_SCHEMA_VERSION,
                fields: vec![LogicalField {
                    id: id(10_000),
                    name: "root".to_owned(),
                    data_type: LogicalType::Struct(nested),
                    nullable: false,
                    metadata: BTreeMap::new(),
                }],
                metadata: BTreeMap::new(),
            }
        };

        schema_with_nested_fields(MAX_SCHEMA_FIELDS - 1)
            .validate()
            .expect("exact field limit");
        assert!(matches!(
            schema_with_nested_fields(MAX_SCHEMA_FIELDS).validate(),
            Err(LogicalError::SchemaFieldLimitExceeded {
                fields,
                maximum: MAX_SCHEMA_FIELDS
            }) if fields == MAX_SCHEMA_FIELDS + 1
        ));
    }

    #[test]
    fn enforces_cumulative_schema_text_limit() {
        let exact = LogicalField {
            id: id(1),
            name: "x".repeat(MAX_SCHEMA_TEXT_BYTES),
            data_type: LogicalType::Null,
            nullable: false,
            metadata: BTreeMap::new(),
        };
        LogicalSchema::new(vec![exact]).expect("exact text limit");

        let excessive = LogicalField {
            id: id(1),
            name: "x".repeat(MAX_SCHEMA_TEXT_BYTES + 1),
            data_type: LogicalType::Null,
            nullable: false,
            metadata: BTreeMap::new(),
        };
        assert!(matches!(
            LogicalSchema::new(vec![excessive]),
            Err(LogicalError::SchemaTextLimitExceeded {
                bytes,
                maximum: MAX_SCHEMA_TEXT_BYTES
            }) if bytes == MAX_SCHEMA_TEXT_BYTES + 1
        ));
    }

    #[test]
    fn counts_timezone_and_metadata_text_bytes() {
        let mut field_metadata = BTreeMap::new();
        field_metadata.insert("a".to_owned(), "b".to_owned());
        let mut schema_metadata = BTreeMap::new();
        schema_metadata.insert("c".to_owned(), "d".to_owned());
        let field = LogicalField {
            id: id(1),
            name: "x".repeat(MAX_SCHEMA_TEXT_BYTES - 7),
            data_type: LogicalType::Timestamp {
                unit: TimeUnit::Second,
                timezone: Some("UTC".to_owned()),
            },
            nullable: false,
            metadata: field_metadata,
        };
        LogicalSchema::from_parts(LOGICAL_SCHEMA_VERSION, vec![field], schema_metadata)
            .expect("all text sources total the exact limit");
    }

    #[test]
    fn failed_rename_restores_the_previous_valid_name() {
        let column_id = id(1);
        let mut schema =
            LogicalSchema::new(vec![column(1, "value", LogicalType::Utf8)]).expect("schema");
        assert!(matches!(
            schema.rename_column(column_id, "x".repeat(MAX_SCHEMA_TEXT_BYTES + 1)),
            Err(LogicalError::SchemaTextLimitExceeded { .. })
        ));
        assert_eq!(schema.field(column_id).expect("field").name, "value");
    }

    #[test]
    fn schema_json_is_stable_and_roundtrips() {
        let mut metadata = BTreeMap::new();
        metadata.insert("origin".to_owned(), "fixture".to_owned());
        let schema = LogicalSchema::from_parts(
            LOGICAL_SCHEMA_VERSION,
            vec![column(1, "value", LogicalType::Int64)],
            metadata,
        )
        .expect("schema");
        let first = serde_json::to_vec(&schema).expect("serialize");
        let restored: LogicalSchema = serde_json::from_slice(&first).expect("deserialize");
        let second = serde_json::to_vec(&restored).expect("serialize again");
        assert_eq!(first, second);
    }

    fn uuid_bytes(value: u128) -> Vec<u8> {
        Uuid::from_u128(value).as_bytes().to_vec()
    }

    fn metadata_block(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut out = u32::try_from(entries.len())
            .expect("entry count")
            .to_le_bytes()
            .to_vec();
        for (key, value) in entries {
            out.extend_from_slice(&u32::try_from(key.len()).expect("key length").to_le_bytes());
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("value length")
                    .to_le_bytes(),
            );
            out.extend_from_slice(value.as_bytes());
        }
        out
    }

    fn field_header(name: &str, id: u128, nullable: bool) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(
            &u32::try_from(name.len())
                .expect("name length")
                .to_le_bytes(),
        );
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&uuid_bytes(id));
        out.push(u8::from(nullable));
        out
    }

    #[test]
    fn canonical_bytes_match_the_frozen_encoding_for_scalar_fields() {
        let schema = LogicalSchema::new(vec![column(1, "a", LogicalType::Int64)]).expect("schema");
        let mut expected = Vec::new();
        expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&field_header("a", 1, false));
        expected.push(0x05);
        expected.extend_from_slice(&metadata_block(&[]));
        expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(schema.canonical_bytes().expect("canonical"), expected);

        let empty = LogicalSchema::empty();
        let mut empty_expected = Vec::new();
        empty_expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        empty_expected.extend_from_slice(&0_u32.to_le_bytes());
        empty_expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(empty.canonical_bytes().expect("empty"), empty_expected);
    }

    #[test]
    fn canonical_bytes_cover_every_atomic_descriptor_tag() {
        let variants: Vec<(LogicalType, u8)> = vec![
            (LogicalType::Null, 0x00),
            (LogicalType::Boolean, 0x01),
            (LogicalType::Int8, 0x02),
            (LogicalType::Int16, 0x03),
            (LogicalType::Int32, 0x04),
            (LogicalType::Int64, 0x05),
            (LogicalType::UInt8, 0x06),
            (LogicalType::UInt16, 0x07),
            (LogicalType::UInt32, 0x08),
            (LogicalType::UInt64, 0x09),
            (LogicalType::Float32, 0x0A),
            (LogicalType::Float64, 0x0B),
            (LogicalType::Utf8, 0x0C),
            (LogicalType::Binary, 0x0D),
            (LogicalType::Date32, 0x0E),
        ];
        for (data_type, tag) in variants {
            let schema =
                LogicalSchema::new(vec![column(1, "v", data_type.clone())]).expect("schema");
            let bytes = schema.canonical_bytes().expect("canonical");
            // Layout tail: tag byte, then the empty field-metadata block and
            // the empty schema-metadata block (four bytes each).
            assert_eq!(bytes[bytes.len() - 9], tag, "tag for {data_type:?}");
            let mut tail = metadata_block(&[]);
            tail.extend_from_slice(&metadata_block(&[]));
            assert_eq!(&bytes[bytes.len() - 8..], &tail[..]);
        }
    }

    #[test]
    fn canonical_bytes_encode_timestamp_unit_and_timezone_presence() {
        let schema = LogicalSchema::new(vec![column(
            2,
            "t",
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: Some("UTC".to_owned()),
            },
        )])
        .expect("schema");
        let mut expected = Vec::new();
        expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&field_header("t", 2, false));
        expected.extend_from_slice(&[0x0F, 0x01, 0x01]);
        expected.extend_from_slice(&3_u32.to_le_bytes());
        expected.extend_from_slice(b"UTC");
        expected.extend_from_slice(&metadata_block(&[]));
        expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(schema.canonical_bytes().expect("canonical"), expected);

        let absent = LogicalSchema::new(vec![column(
            2,
            "t",
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
        )])
        .expect("schema");
        let mut absent_expected = Vec::new();
        absent_expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        absent_expected.extend_from_slice(&1_u32.to_le_bytes());
        absent_expected.extend_from_slice(&field_header("t", 2, false));
        absent_expected.extend_from_slice(&[0x0F, 0x03, 0x00]);
        absent_expected.extend_from_slice(&metadata_block(&[]));
        absent_expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(
            absent.canonical_bytes().expect("canonical"),
            absent_expected
        );

        // Every frozen unit tag is pinned positionally, including the two
        // reserved-in-E4-C0 encodings: Second = 0x00 and Microsecond = 0x02.
        for (unit, tag) in [
            (TimeUnit::Second, 0x00_u8),
            (TimeUnit::Microsecond, 0x02_u8),
        ] {
            let schema = LogicalSchema::new(vec![column(
                2,
                "t",
                LogicalType::Timestamp {
                    unit,
                    timezone: None,
                },
            )])
            .expect("schema");
            let mut expected = Vec::new();
            expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
            expected.extend_from_slice(&1_u32.to_le_bytes());
            expected.extend_from_slice(&field_header("t", 2, false));
            // Type descriptor only: unit tag plus timezone-presence byte.
            expected.extend_from_slice(&[0x0F, tag, 0x00]);
            expected.extend_from_slice(&metadata_block(&[]));
            expected.extend_from_slice(&metadata_block(&[]));
            assert_eq!(
                schema.canonical_bytes().expect("canonical"),
                expected,
                "unit tag for {unit:?}"
            );
        }
    }

    #[test]
    fn canonical_bytes_recurse_into_lists_and_structs() {
        let nested = LogicalType::List(Box::new(LogicalType::List(Box::new(LogicalType::Int64))));
        let schema = LogicalSchema::new(vec![column(3, "n", nested)]).expect("schema");
        let mut expected = Vec::new();
        expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&field_header("n", 3, false));
        expected.extend_from_slice(&[0x10, 0x10, 0x05]);
        expected.extend_from_slice(&metadata_block(&[]));
        expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(schema.canonical_bytes().expect("canonical"), expected);

        let mut field_metadata = BTreeMap::new();
        field_metadata.insert("z".to_owned(), "1".to_owned());
        field_metadata.insert("a".to_owned(), "2".to_owned());
        let inner = LogicalField {
            id: ColumnId::from_uuid(Uuid::from_u128(4)),
            name: "s".to_owned(),
            data_type: LogicalType::Utf8,
            nullable: true,
            metadata: field_metadata,
        };
        inner.validate().expect("inner field");
        let struct_schema = LogicalSchema::new(vec![LogicalField {
            id: ColumnId::from_uuid(Uuid::from_u128(5)),
            name: "outer".to_owned(),
            data_type: LogicalType::Struct(vec![inner]),
            nullable: false,
            metadata: BTreeMap::new(),
        }])
        .expect("schema");
        let mut struct_expected = Vec::new();
        struct_expected.extend_from_slice(&LOGICAL_SCHEMA_VERSION.to_le_bytes());
        struct_expected.extend_from_slice(&1_u32.to_le_bytes());
        struct_expected.extend_from_slice(&field_header("outer", 5, false));
        struct_expected.push(0x11);
        struct_expected.extend_from_slice(&1_u32.to_le_bytes());
        struct_expected.extend_from_slice(&field_header("s", 4, true));
        struct_expected.push(0x0C);
        // Nested field metadata is emitted sorted by key bytes ("a" < "z").
        struct_expected.extend_from_slice(&metadata_block(&[("a", "2"), ("z", "1")]));
        struct_expected.extend_from_slice(&metadata_block(&[]));
        struct_expected.extend_from_slice(&metadata_block(&[]));
        assert_eq!(
            struct_schema.canonical_bytes().expect("canonical"),
            struct_expected
        );
    }

    #[test]
    fn canonical_bytes_are_sensitive_to_every_descriptor_input() {
        let base = LogicalSchema::new(vec![column(1, "a", LogicalType::Int64)]).expect("schema");
        let base_bytes = base.canonical_bytes().expect("canonical");

        let renamed = LogicalSchema::new(vec![column(1, "b", LogicalType::Int64)]).expect("schema");
        assert_ne!(renamed.canonical_bytes().expect("renamed"), base_bytes);

        let renumbered =
            LogicalSchema::new(vec![column(2, "a", LogicalType::Int64)]).expect("schema");
        assert_ne!(
            renumbered.canonical_bytes().expect("renumbered"),
            base_bytes
        );

        let retyped = LogicalSchema::new(vec![column(1, "a", LogicalType::Int32)]).expect("schema");
        assert_ne!(retyped.canonical_bytes().expect("retyped"), base_bytes);

        let mut nullable_field = column(1, "a", LogicalType::Int64);
        nullable_field.nullable = true;
        let nullable = LogicalSchema::new(vec![nullable_field]).expect("schema");
        assert_ne!(nullable.canonical_bytes().expect("nullable"), base_bytes);

        let mut metadata_field = column(1, "a", LogicalType::Int64);
        let mut metadata = BTreeMap::new();
        metadata.insert("k".to_owned(), "v".to_owned());
        metadata_field.metadata = metadata;
        let with_metadata = LogicalSchema::new(vec![metadata_field]).expect("schema");
        assert_ne!(
            with_metadata.canonical_bytes().expect("metadata"),
            base_bytes
        );

        let mut schema_metadata = BTreeMap::new();
        schema_metadata.insert("origin".to_owned(), "fixture".to_owned());
        let schema_level = LogicalSchema::from_parts(
            LOGICAL_SCHEMA_VERSION,
            vec![column(1, "a", LogicalType::Int64)],
            schema_metadata,
        )
        .expect("schema");
        assert_ne!(
            schema_level.canonical_bytes().expect("schema metadata"),
            base_bytes
        );
    }

    #[test]
    fn canonical_bytes_encode_maximum_nesting_depth() {
        let schema = LogicalSchema::new(vec![column(
            1,
            "deep",
            nested_list(MAX_SCHEMA_NESTING_DEPTH),
        )])
        .expect("schema");
        let bytes = schema.canonical_bytes().expect("canonical");
        let expected_tags = vec![0x10; MAX_SCHEMA_NESTING_DEPTH - 1]
            .into_iter()
            .chain(std::iter::once(0x05))
            .collect::<Vec<_>>();
        let tail_start = bytes.len() - 8 - expected_tags.len();
        assert_eq!(&bytes[tail_start..bytes.len() - 8], &expected_tags);
    }
}
