use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit};
use stillflow_core::{
    logical_schema_to_arrow, ColumnId, ConnectorError, ConnectorResult, ErrorCategory,
    LogicalField, LogicalSchema, LogicalType, TimeUnit,
};
use uuid::Uuid;

pub(crate) fn logical_schema_from_source_arrow(
    asset_id: Uuid,
    schema: &Schema,
) -> ConnectorResult<LogicalSchema> {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            logical_field(asset_id, "", position, field.as_ref(), field.is_nullable())
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    LogicalSchema::new(fields).map_err(|_| schema_error(
        ErrorCategory::InvalidData,
        "Parquet schema is outside the supported logical bounds",
    ))
}

fn logical_field(
    asset_id: Uuid,
    parent: &str,
    position: usize,
    field: &Field,
    nullable: bool,
) -> ConnectorResult<LogicalField> {
    let path = field_path(parent, position, field.name());
    LogicalField::new(
        ColumnId::from_uuid(Uuid::new_v5(&asset_id, path.as_bytes())),
        field.name(),
        logical_type(asset_id, &path, field.data_type())?,
        nullable,
    )
    .map_err(|_| schema_error(
        ErrorCategory::InvalidData,
        "Parquet field is outside the supported logical bounds",
    ))
}

fn logical_type(
    asset_id: Uuid,
    parent: &str,
    data_type: &DataType,
) -> ConnectorResult<LogicalType> {
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
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => LogicalType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => LogicalType::Binary,
        DataType::Date32 => LogicalType::Date32,
        DataType::Timestamp(unit, timezone) => LogicalType::Timestamp {
            unit: time_unit(*unit),
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        DataType::List(element) | DataType::LargeList(element) => {
            LogicalType::List(Box::new(logical_type(
                asset_id,
                &field_path(parent, 0, element.name()),
                element.data_type(),
            )?))
        }
        DataType::Struct(fields) => LogicalType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(position, field)| {
                    logical_field(
                        asset_id,
                        parent,
                        position,
                        field.as_ref(),
                        field.is_nullable(),
                    )
                })
                .collect::<ConnectorResult<Vec<_>>>()?,
        ),
        _ => {
            return Err(schema_error(
                ErrorCategory::InvalidData,
                "Parquet contains a logical type unsupported by schema version 1",
            ));
        }
    })
}

fn field_path(parent: &str, position: usize, name: &str) -> String {
    format!("{parent}/{position}:{}:{name}", name.len())
}

fn time_unit(unit: ArrowTimeUnit) -> TimeUnit {
    match unit {
        ArrowTimeUnit::Second => TimeUnit::Second,
        ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

pub(crate) struct ProjectionPlan {
    pub(crate) output_schema: LogicalSchema,
    pub(crate) mask_indices: Vec<usize>,
    output_positions: Vec<usize>,
    canonical_arrow: Arc<Schema>,
}

impl ProjectionPlan {
    pub(crate) fn new(
        source: &LogicalSchema,
        schema_override: Option<&LogicalSchema>,
        projection: Option<&[ColumnId]>,
    ) -> ConnectorResult<Self> {
        let full = if let Some(override_schema) = schema_override {
            validate_override(source, override_schema)?;
            override_schema.clone()
        } else {
            source.clone()
        };
        let desired_indices = match projection {
            None => (0..full.fields.len()).collect::<Vec<_>>(),
            Some(ids) => {
                if ids.is_empty()
                    || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len()
                {
                    return Err(ConnectorError::invalid_configuration(
                        "projection must contain unique known column ids",
                    ));
                }
                ids.iter()
                    .map(|id| {
                        full.fields
                            .iter()
                            .position(|field| field.id == *id)
                            .ok_or_else(|| ConnectorError::invalid_configuration(
                                "projection contains an unknown column id",
                            ))
                    })
                    .collect::<ConnectorResult<Vec<_>>>()?
            }
        };
        let mut mask_indices = desired_indices.clone();
        mask_indices.sort_unstable();
        mask_indices.dedup();
        let output_positions = desired_indices
            .iter()
            .map(|source_index| {
                mask_indices.binary_search(source_index).map_err(|_| schema_error(
                    ErrorCategory::Internal,
                    "Parquet projection mapping is inconsistent",
                ))
            })
            .collect::<ConnectorResult<Vec<_>>>()?;
        let output_fields = desired_indices
            .iter()
            .map(|index| full.fields.get(*index).cloned().ok_or_else(|| schema_error(
                ErrorCategory::Internal,
                "Parquet projection index is outside its schema",
            )))
            .collect::<ConnectorResult<Vec<_>>>()?;
        let output_schema = LogicalSchema::from_parts(
            full.version,
            output_fields,
            full.metadata.clone(),
        )
        .map_err(|_| schema_error(
            ErrorCategory::InvalidData,
            "projected Parquet schema is invalid",
        ))?;
        let canonical_arrow = logical_schema_to_arrow(&output_schema).map_err(|_| schema_error(
            ErrorCategory::InvalidData,
            "projected Parquet schema cannot establish the Arrow boundary",
        ))?;
        Ok(Self {
            output_schema,
            mask_indices,
            output_positions,
            canonical_arrow,
        })
    }

    pub(crate) fn adapt_batch(&self, batch: RecordBatch) -> ConnectorResult<RecordBatch> {
        let mut columns = Vec::with_capacity(self.output_positions.len());
        for (output_index, input_index) in self.output_positions.iter().copied().enumerate() {
            let source = batch.columns().get(input_index).ok_or_else(|| schema_error(
                ErrorCategory::InvalidData,
                "Parquet batch is missing a projected column",
            ))?;
            let target = self
                .canonical_arrow
                .fields()
                .get(output_index)
                .ok_or_else(|| schema_error(
                    ErrorCategory::Internal,
                    "canonical Parquet projection is inconsistent",
                ))?;
            let array = if source.data_type() == target.data_type() {
                Arc::clone(source)
            } else {
                cast(source, target.data_type()).map_err(|_| schema_error(
                    ErrorCategory::SchemaDrift,
                    "Parquet batch cannot satisfy the selected logical schema",
                ))?
            };
            columns.push(array);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&self.canonical_arrow), columns, &options)
            .map_err(|_| schema_error(
                ErrorCategory::InvalidData,
                "Parquet batch has an invalid projected schema",
            ))
    }
}

fn validate_override(source: &LogicalSchema, target: &LogicalSchema) -> ConnectorResult<()> {
    target.validate().map_err(|_| ConnectorError::invalid_configuration(
        "schema override is not a valid logical schema",
    ))?;
    if source.fields.len() != target.fields.len()
        || source
            .fields
            .iter()
            .zip(&target.fields)
            .any(|(source, target)| !field_compatible(source, target))
    {
        return Err(schema_error(
            ErrorCategory::SchemaDrift,
            "Parquet schema cannot satisfy the requested schema override",
        ));
    }
    Ok(())
}

fn field_compatible(source: &LogicalField, target: &LogicalField) -> bool {
    source.name == target.name
        && (!source.nullable || target.nullable)
        && type_compatible(&source.data_type, &target.data_type)
}

fn type_compatible(source: &LogicalType, target: &LogicalType) -> bool {
    match (source, target) {
        (LogicalType::Struct(source), LogicalType::Struct(target)) => {
            source.len() == target.len()
                && source
                    .iter()
                    .zip(target)
                    .all(|(source, target)| field_compatible(source, target))
        }
        (LogicalType::List(source), LogicalType::List(target)) => {
            type_compatible(source, target)
        }
        _ => source
            .least_upper_bound(target)
            .is_ok_and(|joined| joined == *target),
    }
}

fn schema_error(category: ErrorCategory, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(
        category,
        false,
        message,
        Vec::new(),
        std::collections::BTreeMap::new(),
    )
}
