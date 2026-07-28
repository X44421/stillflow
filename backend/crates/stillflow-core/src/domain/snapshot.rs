use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ConnectorError;
use crate::ConnectorResult;

/// Lossless JSON encoding of an Arrow [`DataType`] for snapshot persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum EncodedDataType {
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
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    FixedSizeBinary {
        byte_width: i32,
    },
    Date32,
    Date64,
    Timestamp {
        unit: EncodedTimeUnit,
        timezone: Option<String>,
    },
    Time32 {
        unit: EncodedTimeUnit,
    },
    Time64 {
        unit: EncodedTimeUnit,
    },
    Duration {
        unit: EncodedTimeUnit,
    },
    Interval {
        unit: EncodedIntervalUnit,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    List {
        element: Box<EncodedField>,
    },
    LargeList {
        element: Box<EncodedField>,
    },
    FixedSizeList {
        element: Box<EncodedField>,
        size: i32,
    },
    Struct {
        fields: Vec<EncodedField>,
    },
    Union {
        fields: Vec<EncodedField>,
        mode: EncodedUnionMode,
        type_ids: Vec<i8>,
    },
    Dictionary {
        key: Box<EncodedDataType>,
        value: Box<EncodedDataType>,
    },
    Map {
        key: Box<EncodedField>,
        value: Box<EncodedField>,
        keys_sorted: bool,
    },
    BinaryView,
    Utf8View,
    ListView {
        element: Box<EncodedField>,
    },
    LargeListView {
        element: Box<EncodedField>,
    },
    Decimal32 {
        precision: u8,
        scale: i8,
    },
    Decimal64 {
        precision: u8,
        scale: i8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EncodedTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EncodedIntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EncodedUnionMode {
    Sparse,
    Dense,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EncodedField {
    name: String,
    data_type: EncodedDataType,
    nullable: bool,
}

impl EncodedField {
    fn from_field(field: &Field) -> Self {
        Self {
            name: field.name().to_owned(),
            data_type: EncodedDataType::from_data_type(field.data_type()),
            nullable: field.is_nullable(),
        }
    }

    fn to_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.to_data_type(),
            self.nullable,
        )
    }
}

impl EncodedDataType {
    fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::FixedSizeBinary(byte_width) => Self::FixedSizeBinary {
                byte_width: *byte_width,
            },
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Timestamp(unit, timezone) => Self::Timestamp {
                unit: EncodedTimeUnit::from_time_unit(*unit),
                timezone: timezone.as_ref().map(|tz| tz.to_string()),
            },
            DataType::Time32(unit) => Self::Time32 {
                unit: EncodedTimeUnit::from_time_unit(*unit),
            },
            DataType::Time64(unit) => Self::Time64 {
                unit: EncodedTimeUnit::from_time_unit(*unit),
            },
            DataType::Duration(unit) => Self::Duration {
                unit: EncodedTimeUnit::from_time_unit(*unit),
            },
            DataType::Interval(unit) => Self::Interval {
                unit: EncodedIntervalUnit::from_interval_unit(*unit),
            },
            DataType::Decimal128(precision, scale) => Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal256(precision, scale) => Self::Decimal256 {
                precision: *precision,
                scale: *scale,
            },
            DataType::List(field) => Self::List {
                element: Box::new(EncodedField::from_field(field)),
            },
            DataType::LargeList(field) => Self::LargeList {
                element: Box::new(EncodedField::from_field(field)),
            },
            DataType::FixedSizeList(field, size) => Self::FixedSizeList {
                element: Box::new(EncodedField::from_field(field)),
                size: *size,
            },
            DataType::Struct(fields) => Self::Struct {
                fields: fields
                    .iter()
                    .map(|field| EncodedField::from_field(field.as_ref()))
                    .collect(),
            },
            DataType::Union(union_fields, mode) => Self::Union {
                fields: union_fields
                    .iter()
                    .map(|(_, field)| EncodedField::from_field(field.as_ref()))
                    .collect(),
                mode: EncodedUnionMode::from_union_mode(*mode),
                type_ids: union_fields.iter().map(|(id, _)| id).collect(),
            },
            DataType::Dictionary(key, value) => Self::Dictionary {
                key: Box::new(EncodedDataType::from_data_type(key)),
                value: Box::new(EncodedDataType::from_data_type(value)),
            },
            DataType::Map(field, keys_sorted) => {
                let DataType::Struct(fields) = field.data_type() else {
                    panic!("map field must contain a struct");
                };
                Self::Map {
                    key: Box::new(EncodedField::from_field(fields[0].as_ref())),
                    value: Box::new(EncodedField::from_field(fields[1].as_ref())),
                    keys_sorted: *keys_sorted,
                }
            }
            DataType::RunEndEncoded(_, _) => {
                panic!("RunEndEncoded is not supported in snapshot schema encoding")
            }
            DataType::BinaryView => Self::BinaryView,
            DataType::Utf8View => Self::Utf8View,
            DataType::ListView(field) => Self::ListView {
                element: Box::new(EncodedField::from_field(field)),
            },
            DataType::LargeListView(field) => Self::LargeListView {
                element: Box::new(EncodedField::from_field(field)),
            },
            DataType::Decimal32(precision, scale) => Self::Decimal32 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal64(precision, scale) => Self::Decimal64 {
                precision: *precision,
                scale: *scale,
            },
        }
    }

    fn to_data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float16 => DataType::Float16,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Binary => DataType::Binary,
            Self::LargeBinary => DataType::LargeBinary,
            Self::FixedSizeBinary { byte_width } => DataType::FixedSizeBinary(*byte_width),
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Timestamp { unit, timezone } => DataType::Timestamp(
                unit.to_time_unit(),
                timezone.as_ref().map(|tz| Arc::from(tz.as_str())),
            ),
            Self::Time32 { unit } => DataType::Time32(unit.to_time_unit()),
            Self::Time64 { unit } => DataType::Time64(unit.to_time_unit()),
            Self::Duration { unit } => DataType::Duration(unit.to_time_unit()),
            Self::Interval { unit } => DataType::Interval(unit.to_interval_unit()),
            Self::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Decimal256 { precision, scale } => DataType::Decimal256(*precision, *scale),
            Self::List { element } => DataType::List(Arc::new(element.to_field())),
            Self::LargeList { element } => DataType::LargeList(Arc::new(element.to_field())),
            Self::FixedSizeList { element, size } => {
                DataType::FixedSizeList(Arc::new(element.to_field()), *size)
            }
            Self::Struct { fields } => DataType::Struct(
                fields
                    .iter()
                    .map(|field| Arc::new(field.to_field()))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Self::Union {
                fields,
                mode,
                type_ids,
            } => {
                let union_fields = type_ids
                    .iter()
                    .zip(fields.iter())
                    .map(|(&id, field)| (id, Arc::new(field.to_field())))
                    .collect();
                DataType::Union(union_fields, mode.to_union_mode())
            }
            Self::Dictionary { key, value } => {
                DataType::Dictionary(Box::new(key.to_data_type()), Box::new(value.to_data_type()))
            }
            Self::Map {
                key,
                value,
                keys_sorted,
            } => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![Arc::new(key.to_field()), Arc::new(value.to_field())].into(),
                    ),
                    false,
                )),
                *keys_sorted,
            ),
            Self::BinaryView => DataType::BinaryView,
            Self::Utf8View => DataType::Utf8View,
            Self::ListView { element } => DataType::ListView(Arc::new(element.to_field())),
            Self::LargeListView { element } => {
                DataType::LargeListView(Arc::new(element.to_field()))
            }
            Self::Decimal32 { precision, scale } => DataType::Decimal32(*precision, *scale),
            Self::Decimal64 { precision, scale } => DataType::Decimal64(*precision, *scale),
        }
    }
}

impl EncodedTimeUnit {
    fn from_time_unit(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }

    fn to_time_unit(&self) -> TimeUnit {
        match self {
            Self::Second => TimeUnit::Second,
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl EncodedIntervalUnit {
    fn from_interval_unit(unit: arrow_schema::IntervalUnit) -> Self {
        match unit {
            arrow_schema::IntervalUnit::YearMonth => Self::YearMonth,
            arrow_schema::IntervalUnit::DayTime => Self::DayTime,
            arrow_schema::IntervalUnit::MonthDayNano => Self::MonthDayNano,
        }
    }

    fn to_interval_unit(&self) -> arrow_schema::IntervalUnit {
        match self {
            Self::YearMonth => arrow_schema::IntervalUnit::YearMonth,
            Self::DayTime => arrow_schema::IntervalUnit::DayTime,
            Self::MonthDayNano => arrow_schema::IntervalUnit::MonthDayNano,
        }
    }
}

impl EncodedUnionMode {
    fn from_union_mode(mode: arrow_schema::UnionMode) -> Self {
        match mode {
            arrow_schema::UnionMode::Sparse => Self::Sparse,
            arrow_schema::UnionMode::Dense => Self::Dense,
        }
    }

    fn to_union_mode(&self) -> arrow_schema::UnionMode {
        match self {
            Self::Sparse => arrow_schema::UnionMode::Sparse,
            Self::Dense => arrow_schema::UnionMode::Dense,
        }
    }
}

/// Serializable snapshot of one Arrow schema field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaFieldSnapshot {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl SchemaFieldSnapshot {
    fn from_field(field: &Field) -> Self {
        let encoded = EncodedDataType::from_data_type(field.data_type());
        Self {
            name: field.name().to_owned(),
            data_type: serde_json::to_string(&encoded).expect("encode data type"),
            nullable: field.is_nullable(),
        }
    }

    fn to_field(&self) -> ConnectorResult<Field> {
        let encoded: EncodedDataType = serde_json::from_str(&self.data_type).map_err(|error| {
            ConnectorError::invalid_configuration(format!(
                "invalid schema field data type encoding: {error}"
            ))
        })?;
        Ok(Field::new(
            self.name.clone(),
            encoded.to_data_type(),
            self.nullable,
        ))
    }
}

/// Immutable materialized output plus lineage and quality metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSnapshot {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub storage_ref: String,
    pub row_count: u64,
    pub quality_score: Option<u8>,
    pub lineage: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_fields: Vec<SchemaFieldSnapshot>,
    #[serde(skip)]
    pub schema: Option<Arc<Schema>>,
    pub created_at: DateTime<Utc>,
}

impl DatasetSnapshot {
    pub fn new(
        dataset_id: Uuid,
        session_id: Uuid,
        storage_ref: impl Into<String>,
        row_count: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            dataset_id,
            session_id,
            storage_ref: storage_ref.into(),
            row_count,
            quality_score: None,
            lineage: Vec::new(),
            schema_fields: Vec::new(),
            schema: None,
            created_at: Utc::now(),
        }
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> ConnectorResult<Self> {
        self.schema_fields = schema
            .fields()
            .iter()
            .map(|field| SchemaFieldSnapshot::from_field(field.as_ref()))
            .collect();
        self.schema = Some(schema);
        Ok(self)
    }

    pub fn resolved_schema(&self) -> ConnectorResult<Option<Arc<Schema>>> {
        if let Some(schema) = &self.schema {
            return Ok(Some(schema.clone()));
        }
        if self.schema_fields.is_empty() {
            return Ok(None);
        }
        let fields: Vec<Field> = self
            .schema_fields
            .iter()
            .map(SchemaFieldSnapshot::to_field)
            .collect::<ConnectorResult<_>>()?;
        Ok(Some(Arc::new(Schema::new(fields))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrips_through_schema_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let snapshot = DatasetSnapshot::new(Uuid::new_v4(), Uuid::new_v4(), "snap://1", 10)
            .with_schema(schema.clone())
            .expect("schema");
        let json = serde_json::to_string(&snapshot).expect("serialize");
        let restored: DatasetSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.schema_fields.len(), 3);
        let resolved = restored
            .resolved_schema()
            .expect("resolve")
            .expect("schema");
        assert_eq!(resolved.as_ref(), schema.as_ref());
    }
}
