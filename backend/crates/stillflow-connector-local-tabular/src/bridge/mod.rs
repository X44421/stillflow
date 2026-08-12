//! Narrow Polars-to-arrow-rs bridge through the Arrow C Data Interface.

#[allow(unsafe_code)]
mod ffi;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    make_array, Array, ArrayRef, ListArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow_cast::cast;
use arrow_schema::SchemaRef;
use polars::prelude::{CompatLevel, DataFrame};
use stillflow_core::{
    ConnectorError, ConnectorResult, ErrorCategory, LogicalField, LogicalSchema, LogicalType,
};

pub(crate) fn dataframe_to_record_batch(
    frame: DataFrame,
    logical_schema: &LogicalSchema,
    target_schema: &SchemaRef,
) -> ConnectorResult<RecordBatch> {
    logical_schema.validate().map_err(|_| {
        bridge_error(
            ErrorCategory::Internal,
            "the bridge received an invalid logical schema",
        )
    })?;
    if frame.width() != logical_schema.fields.len() {
        return Err(bridge_error(
            ErrorCategory::SchemaDrift,
            "decoded columns do not match the established schema",
        ));
    }

    let height = frame.height();
    if logical_schema.fields.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(height));
        return RecordBatch::try_new_with_options(Arc::clone(target_schema), Vec::new(), &options)
            .map_err(|_| {
                bridge_error(
                    ErrorCategory::Internal,
                    "the empty Arrow record batch violated a bridge invariant",
                )
            });
    }

    let polars_batch = frame.rechunk_to_record_batch(CompatLevel::oldest());
    let (polars_schema, polars_arrays) = polars_batch.into_schema_and_arrays();

    let mut arrays = Vec::<ArrayRef>::with_capacity(polars_arrays.len());
    for (index, (field, array)) in polars_schema.iter_values().zip(polars_arrays).enumerate() {
        let imported = ffi::import_array(field, array)?;
        let imported = make_array(imported);
        let target_field = target_schema.fields().get(index).ok_or_else(|| {
            bridge_error(
                ErrorCategory::Internal,
                "the canonical Arrow schema is missing a decoded field",
            )
        })?;
        let logical_field = logical_schema.fields.get(index).ok_or_else(|| {
            bridge_error(
                ErrorCategory::Internal,
                "the logical schema is missing a decoded field",
            )
        })?;
        let target_type = target_field.data_type();
        let normalized = if imported.data_type() == target_type {
            imported
        } else {
            cast(imported.as_ref(), target_type).map_err(|_| {
                bridge_error(
                    ErrorCategory::InvalidData,
                    "decoded values cannot be represented by the established schema",
                )
            })?
        };
        validate_required_values(logical_field, normalized.as_ref(), None)?;
        arrays.push(normalized);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(height));
    RecordBatch::try_new_with_options(Arc::clone(target_schema), arrays, &options).map_err(|_| {
        bridge_error(
            ErrorCategory::Internal,
            "the Arrow record batch violated a bridge invariant",
        )
    })
}

fn validate_required_values(
    field: &LogicalField,
    array: &dyn Array,
    parent_present: Option<&[bool]>,
) -> ConnectorResult<()> {
    if parent_present.is_some_and(|present| present.len() != array.len()) {
        return Err(bridge_error(
            ErrorCategory::Internal,
            "nested Arrow validity length does not match its parent",
        ));
    }
    if !field.nullable
        && (0..array.len()).any(|row| parent_is_present(parent_present, row) && array.is_null(row))
    {
        return Err(bridge_error(
            ErrorCategory::SchemaDrift,
            "decoded data is missing a required field",
        ));
    }
    let present = (0..array.len())
        .map(|row| parent_is_present(parent_present, row) && array.is_valid(row))
        .collect::<Vec<_>>();
    validate_nested_values(&field.data_type, array, &present)
}

fn parent_is_present(parent_present: Option<&[bool]>, row: usize) -> bool {
    parent_present
        .and_then(|present| present.get(row))
        .copied()
        .unwrap_or(true)
}

fn validate_nested_values(
    data_type: &LogicalType,
    array: &dyn Array,
    present: &[bool],
) -> ConnectorResult<()> {
    match data_type {
        LogicalType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical struct array has an unexpected representation",
                    )
                })?;
            if fields.len() != values.num_columns() {
                return Err(bridge_error(
                    ErrorCategory::Internal,
                    "canonical struct fields do not match their child arrays",
                ));
            }
            for (field, child) in fields.iter().zip(values.columns()) {
                validate_required_values(field, child.as_ref(), Some(present))?;
            }
        }
        LogicalType::List(element) => {
            let values = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                bridge_error(
                    ErrorCategory::Internal,
                    "canonical list array has an unexpected representation",
                )
            })?;
            let offsets = values.value_offsets();
            let mut element_parent = vec![false; values.values().len()];
            for (row, row_present) in present.iter().copied().enumerate() {
                if !row_present {
                    continue;
                }
                let end_index = row.checked_add(1).ok_or_else(|| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical list row index exceeds the supported range",
                    )
                })?;
                let start_offset = offsets.get(row).ok_or_else(|| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical list is missing a start offset",
                    )
                })?;
                let end_offset = offsets.get(end_index).ok_or_else(|| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical list is missing an end offset",
                    )
                })?;
                let start = usize::try_from(*start_offset).map_err(|_| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical list contains a negative offset",
                    )
                })?;
                let end = usize::try_from(*end_offset).map_err(|_| {
                    bridge_error(
                        ErrorCategory::Internal,
                        "canonical list contains a negative offset",
                    )
                })?;
                let Some(active) = element_parent.get_mut(start..end) else {
                    return Err(bridge_error(
                        ErrorCategory::Internal,
                        "canonical list offsets exceed the child array",
                    ));
                };
                active.fill(true);
            }
            let element_present = element_parent
                .into_iter()
                .enumerate()
                .map(|(index, parent)| parent && values.values().is_valid(index))
                .collect::<Vec<_>>();
            validate_nested_values(element, values.values().as_ref(), &element_present)?;
        }
        _ => {}
    }
    Ok(())
}

fn bridge_error(category: ErrorCategory, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, false, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array, StringArray};
    use polars::prelude::{DataFrame, IntoColumn, NamedFrom, Series};
    use stillflow_core::{logical_schema_to_arrow, ColumnId, LogicalField, LogicalType};
    use uuid::Uuid;

    use super::*;

    fn schema() -> LogicalSchema {
        LogicalSchema::new(vec![
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(1)),
                "id",
                LogicalType::Int64,
                true,
            )
            .expect("id field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(2)),
                "label",
                LogicalType::Utf8,
                true,
            )
            .expect("label field"),
        ])
        .expect("logical schema")
    }

    fn bridge(frame: DataFrame, schema: &LogicalSchema) -> ConnectorResult<RecordBatch> {
        let arrow_schema = logical_schema_to_arrow(schema).expect("canonical Arrow schema");
        dataframe_to_record_batch(frame, schema, &arrow_schema)
    }

    #[test]
    fn imports_null_and_variable_width_arrays_and_releases_on_drop() {
        let frame = DataFrame::new(vec![
            Series::new("id".into(), [Some(1_i64), None, Some(3)]).into_column(),
            Series::new("label".into(), [Some("a"), Some("variable"), None]).into_column(),
        ])
        .expect("frame");
        let batch = bridge(frame, &schema()).expect("bridge");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int64Array>(),
            Some(&Int64Array::from(vec![Some(1), None, Some(3)]))
        );
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("canonical utf8");
        assert_eq!(labels.value(1), "variable");
        drop(batch);
    }

    #[test]
    fn imports_empty_frames() {
        let frame = DataFrame::new(vec![
            Series::new_empty("id".into(), &polars::prelude::DataType::Int64).into_column(),
            Series::new_empty("label".into(), &polars::prelude::DataType::String).into_column(),
        ])
        .expect("empty frame");
        let batch = bridge(frame, &schema()).expect("empty bridge");
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn imports_sliced_frames_and_supports_immediate_drop() {
        let frame = DataFrame::new(vec![
            Series::new("id".into(), [1_i64, 2, 3, 4]).into_column(),
            Series::new("label".into(), ["a", "b", "c", "d"]).into_column(),
        ])
        .expect("frame")
        .slice(1, 2);
        let batch = bridge(frame, &schema()).expect("sliced bridge");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("canonical integers");
        assert_eq!(ids.values(), &[2, 3]);
        drop(batch);

        let frame = DataFrame::new(vec![
            Series::new("id".into(), [5_i64]).into_column(),
            Series::new("label".into(), ["early"]).into_column(),
        ])
        .expect("early-drop frame");
        drop(bridge(frame, &schema()).expect("early-drop bridge"));
    }

    #[test]
    fn imports_chunked_frames_without_changing_row_order() {
        let mut frame = DataFrame::new(vec![
            Series::new("id".into(), [1_i64, 2]).into_column(),
            Series::new("label".into(), ["a", "b"]).into_column(),
        ])
        .expect("first frame");
        let second = DataFrame::new(vec![
            Series::new("id".into(), [3_i64, 4]).into_column(),
            Series::new("label".into(), ["c", "d"]).into_column(),
        ])
        .expect("second frame");
        frame.vstack_mut(&second).expect("chunked frame");
        assert!(frame.column("id").expect("id column").n_chunks() > 1);

        let batch = bridge(frame, &schema()).expect("chunked bridge");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("canonical integers");
        assert_eq!(ids.values(), &[1, 2, 3, 4]);
    }

    #[test]
    fn rejects_nulls_in_required_fields() {
        let required = LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(3)),
            "id",
            LogicalType::Int64,
            false,
        )
        .expect("required field")])
        .expect("required schema");
        let frame = DataFrame::new(vec![
            Series::new("id".into(), [Some(1_i64), None]).into_column()
        ])
        .expect("nullable frame");
        let error = bridge(frame, &required).expect_err("required null");
        assert_eq!(error.category(), ErrorCategory::SchemaDrift);
    }
}
