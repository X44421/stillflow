use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use polars::prelude::{DataType as PolarsDataType, Field as PolarsField};
use polars::prelude::{Schema as PolarsSchema, TimeUnit as PolarsTimeUnit};
use polars_arrow::datatypes::{
    ArrowDataType as PolarsArrowDataType, ArrowSchema as PolarsArrowSchema,
    Field as PolarsArrowField, TimeUnit as PolarsArrowTimeUnit,
};
use stillflow_core::{
    ColumnId, ConnectorError, ConnectorResult, ErrorCategory, LogicalField, LogicalSchema,
    LogicalType, TimeUnit,
};
use uuid::Uuid;

pub(crate) fn logical_schema_from_polars_arrow(
    asset_id: Uuid,
    schema: &PolarsArrowSchema,
) -> ConnectorResult<LogicalSchema> {
    let fields = schema
        .iter_values()
        .enumerate()
        .map(|(position, field)| {
            logical_field_from_polars_arrow(asset_id, "", position, field, field.is_nullable)
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    LogicalSchema::new(fields).map_err(logical_error)
}

fn logical_field_from_polars_arrow(
    asset_id: Uuid,
    parent_path: &str,
    position: usize,
    field: &PolarsArrowField,
    nullable: bool,
) -> ConnectorResult<LogicalField> {
    let name = field.name.as_str();
    let path = field_path(parent_path, position, name);
    let data_type = logical_type_from_polars_arrow(asset_id, &path, &field.dtype)?;
    LogicalField::new(stable_column_id(asset_id, &path), name, data_type, nullable)
        .map_err(logical_error)
}

fn logical_type_from_polars_arrow(
    asset_id: Uuid,
    parent_path: &str,
    data_type: &PolarsArrowDataType,
) -> ConnectorResult<LogicalType> {
    let result = match data_type {
        PolarsArrowDataType::Null => LogicalType::Null,
        PolarsArrowDataType::Boolean => LogicalType::Boolean,
        PolarsArrowDataType::Int8 => LogicalType::Int8,
        PolarsArrowDataType::Int16 => LogicalType::Int16,
        PolarsArrowDataType::Int32 => LogicalType::Int32,
        PolarsArrowDataType::Int64 => LogicalType::Int64,
        PolarsArrowDataType::UInt8 => LogicalType::UInt8,
        PolarsArrowDataType::UInt16 => LogicalType::UInt16,
        PolarsArrowDataType::UInt32 => LogicalType::UInt32,
        PolarsArrowDataType::UInt64 => LogicalType::UInt64,
        PolarsArrowDataType::Float32 => LogicalType::Float32,
        PolarsArrowDataType::Float64 => LogicalType::Float64,
        PolarsArrowDataType::Utf8
        | PolarsArrowDataType::LargeUtf8
        | PolarsArrowDataType::Utf8View => LogicalType::Utf8,
        PolarsArrowDataType::Binary
        | PolarsArrowDataType::LargeBinary
        | PolarsArrowDataType::BinaryView => LogicalType::Binary,
        PolarsArrowDataType::Date32 => LogicalType::Date32,
        PolarsArrowDataType::Timestamp(unit, timezone) => LogicalType::Timestamp {
            unit: logical_time_unit(*unit),
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        PolarsArrowDataType::List(field) | PolarsArrowDataType::LargeList(field) => {
            LogicalType::List(Box::new(logical_type_from_polars_arrow(
                asset_id,
                &field_path(parent_path, 0, field.name.as_str()),
                &field.dtype,
            )?))
        }
        PolarsArrowDataType::Struct(fields) => LogicalType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(position, field)| {
                    logical_field_from_polars_arrow(
                        asset_id,
                        parent_path,
                        position,
                        field,
                        field.is_nullable,
                    )
                })
                .collect::<ConnectorResult<Vec<_>>>()?,
        ),
        _ => {
            return Err(data_error(
                "source contains a logical type unsupported by schema version 1",
            ));
        }
    };
    Ok(result)
}

pub(crate) fn polars_schema_from_logical(
    schema: &LogicalSchema,
) -> ConnectorResult<Arc<PolarsSchema>> {
    schema.validate().map_err(logical_error)?;
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            Ok((
                field.name.as_str().into(),
                polars_type_from_logical(&field.data_type)?,
            ))
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(Arc::new(PolarsSchema::from_iter(fields)))
}

fn polars_type_from_logical(data_type: &LogicalType) -> ConnectorResult<PolarsDataType> {
    let result = match data_type {
        LogicalType::Null => PolarsDataType::Null,
        LogicalType::Boolean => PolarsDataType::Boolean,
        LogicalType::Int8 => PolarsDataType::Int8,
        LogicalType::Int16 => PolarsDataType::Int16,
        LogicalType::Int32 => PolarsDataType::Int32,
        LogicalType::Int64 => PolarsDataType::Int64,
        LogicalType::UInt8 => PolarsDataType::UInt8,
        LogicalType::UInt16 => PolarsDataType::UInt16,
        LogicalType::UInt32 => PolarsDataType::UInt32,
        LogicalType::UInt64 => PolarsDataType::UInt64,
        LogicalType::Float32 => PolarsDataType::Float32,
        LogicalType::Float64 => PolarsDataType::Float64,
        LogicalType::Utf8 => PolarsDataType::String,
        LogicalType::Binary => PolarsDataType::Binary,
        LogicalType::Date32 => PolarsDataType::Date,
        LogicalType::Timestamp { unit, timezone } => {
            PolarsDataType::Datetime(polars_time_unit(*unit), timezone.as_deref().map(Into::into))
        }
        LogicalType::List(element) => {
            PolarsDataType::List(Box::new(polars_type_from_logical(element)?))
        }
        LogicalType::Struct(fields) => PolarsDataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok(PolarsField::new(
                        field.name.as_str().into(),
                        polars_type_from_logical(&field.data_type)?,
                    ))
                })
                .collect::<ConnectorResult<Vec<_>>>()?,
        ),
    };
    Ok(result)
}

pub(crate) struct Projection {
    pub(crate) schema: LogicalSchema,
    pub(crate) source_indices: Vec<usize>,
    pub(crate) names: Vec<String>,
}

pub(crate) fn project_schema(
    schema: &LogicalSchema,
    projection: Option<&[ColumnId]>,
) -> ConnectorResult<Projection> {
    schema.validate().map_err(logical_error)?;
    let Some(ids) = projection else {
        return Ok(Projection {
            schema: schema.clone(),
            source_indices: (0..schema.fields.len()).collect(),
            names: schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect(),
        });
    };

    if ids.is_empty() || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len() {
        return Err(ConnectorError::invalid_configuration(
            "projection must contain unique known column ids",
        ));
    }

    let mut fields = Vec::with_capacity(ids.len());
    let mut indices = Vec::with_capacity(ids.len());
    for id in ids {
        let Some((index, field)) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.id == *id)
        else {
            return Err(ConnectorError::invalid_configuration(
                "projection contains an unknown column id",
            ));
        };
        indices.push(index);
        fields.push(field.clone());
    }
    let projected = LogicalSchema::from_parts(schema.version, fields, schema.metadata.clone())
        .map_err(logical_error)?;
    Ok(Projection {
        names: projected
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect(),
        schema: projected,
        source_indices: indices,
    })
}

pub(crate) fn stable_column_id(asset_id: Uuid, path: &str) -> ColumnId {
    ColumnId::from_uuid(Uuid::new_v5(&asset_id, path.as_bytes()))
}

pub(crate) fn field_path(parent: &str, position: usize, name: &str) -> String {
    format!("{parent}/{position}:{}:{name}", name.len())
}

fn logical_time_unit(unit: PolarsArrowTimeUnit) -> TimeUnit {
    match unit {
        PolarsArrowTimeUnit::Second => TimeUnit::Second,
        PolarsArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        PolarsArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        PolarsArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn polars_time_unit(unit: TimeUnit) -> PolarsTimeUnit {
    match unit {
        TimeUnit::Second | TimeUnit::Millisecond => PolarsTimeUnit::Milliseconds,
        TimeUnit::Microsecond => PolarsTimeUnit::Microseconds,
        TimeUnit::Nanosecond => PolarsTimeUnit::Nanoseconds,
    }
}

fn logical_error(error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::with_category(
        ErrorCategory::InvalidData,
        false,
        format!("source schema is invalid: {error}"),
        Vec::new(),
        BTreeMap::new(),
    )
}

fn data_error(message: &'static str) -> ConnectorError {
    ConnectorError::with_category(
        ErrorCategory::InvalidData,
        false,
        message,
        Vec::new(),
        BTreeMap::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_retains_request_order_and_rejects_unknown_ids() {
        let asset = Uuid::from_u128(1);
        let first = LogicalField::new(
            stable_column_id(asset, "/0:1:a"),
            "a",
            LogicalType::Int64,
            false,
        )
        .expect("first field");
        let second = LogicalField::new(
            stable_column_id(asset, "/1:1:b"),
            "b",
            LogicalType::Utf8,
            true,
        )
        .expect("second field");
        let schema = LogicalSchema::new(vec![first.clone(), second.clone()]).expect("schema");
        let projected = project_schema(&schema, Some(&[second.id, first.id])).expect("projection");
        assert_eq!(projected.names, ["b", "a"]);
        assert_eq!(projected.source_indices, [1, 0]);
        assert!(
            project_schema(&schema, Some(&[ColumnId::from_uuid(Uuid::from_u128(999))])).is_err()
        );
    }
}
