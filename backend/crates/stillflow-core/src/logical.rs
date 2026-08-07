use std::collections::{BTreeMap, BTreeSet};
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
        let previous = std::mem::replace(&mut self.fields[index].name, new_name.into());
        if let Err(error) = self.validate() {
            self.fields[index].name = previous;
            return Err(error);
        }
        Ok(())
    }
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
                let mut ids = BTreeSet::new();
                let mut names = BTreeSet::new();
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
                    if !names.insert(field.name.clone()) {
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
        let exact_name = "x".repeat(MAX_SCHEMA_TEXT_BYTES);
        let exact = LogicalField {
            id: id(1),
            name: exact_name,
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
}
