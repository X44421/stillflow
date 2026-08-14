//! Engine-owned Polars 0.46 ↔ Arrow 59 C Data Interface bridge.

use std::mem::{align_of, size_of, ManuallyDrop};
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_cast::cast;
use arrow_schema::SchemaRef;
use polars::prelude::{CompatLevel, DataFrame};
use polars_arrow::array::Array as PolarsArray;
use polars_arrow::datatypes::Field as PolarsField;
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, import_array_from_c, import_field_from_c,
    ArrowArray as PolarsArrowArray, ArrowSchema as PolarsArrowSchema,
};
use stillflow_core::LogicalSchema;

use crate::error::EngineError;

const _: () = assert!(size_of::<PolarsArrowArray>() == size_of::<FFI_ArrowArray>());
const _: () = assert!(align_of::<PolarsArrowArray>() == align_of::<FFI_ArrowArray>());
const _: () = assert!(size_of::<PolarsArrowSchema>() == size_of::<FFI_ArrowSchema>());
const _: () = assert!(align_of::<PolarsArrowSchema>() == align_of::<FFI_ArrowSchema>());

pub(crate) fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, EngineError> {
    if batch.num_columns() == 0 {
        let marker = polars::prelude::Column::full_null(
            "__stillflow_row_marker".into(),
            batch.num_rows(),
            &polars::prelude::DataType::Null,
        );
        return DataFrame::new(vec![marker])
            .and_then(|frame| frame.select(Vec::<&str>::new()))
            .map_err(|_| EngineError::Ffi);
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut exported = Vec::new();
    for (index, array) in batch.columns().iter().enumerate() {
        match export_arrow_array(array.as_ref()) {
            Ok(pair) => exported.push(pair),
            Err(error) => {
                drop(exported);
                return Err(error);
            }
        }
        let _ = index;
    }

    for (index, (array, schema)) in exported.into_iter().enumerate() {
        match import_into_polars(array, schema) {
            Ok(series) => columns.push(series),
            Err(error) => {
                drop(columns);
                return Err(error);
            }
        }
        let _ = index;
    }

    DataFrame::new(columns)
        .map_err(|_| EngineError::Internal("polars dataframe construction failed"))
}

pub(crate) fn dataframe_to_record_batch(
    frame: DataFrame,
    logical_schema: &LogicalSchema,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, EngineError> {
    if frame.width() != logical_schema.fields.len() {
        return Err(EngineError::Ffi);
    }
    let height = frame.height();
    if logical_schema.fields.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(height));
        return RecordBatch::try_new_with_options(Arc::clone(target_schema), Vec::new(), &options)
            .map_err(|_| EngineError::Ffi);
    }

    let polars_batch = frame.rechunk_to_record_batch(CompatLevel::oldest());
    let (polars_schema, polars_arrays) = polars_batch.into_schema_and_arrays();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(polars_arrays.len());
    let pairs: Vec<_> = polars_schema.iter_values().zip(polars_arrays).collect();
    for (index, (field, array)) in pairs.into_iter().enumerate() {
        let exported = match export_polars_array(field, array) {
            Ok(exported_array) => exported_array,
            Err(error) => {
                drop(arrays);
                return Err(error);
            }
        };
        let target_field = target_schema.field(index);
        let normalized = if exported.data_type() == target_field.data_type() {
            exported
        } else {
            cast(exported.as_ref(), target_field.data_type()).map_err(|_| {
                EngineError::Internal("exported array could not be cast to the canonical type")
            })?
        };
        arrays.push(normalized);
    }

    RecordBatch::try_new(Arc::clone(target_schema), arrays).map_err(|_| EngineError::Ffi)
}

fn export_arrow_array(
    array: &dyn Array,
) -> Result<(PolarsArrowArray, PolarsArrowSchema), EngineError> {
    let (ffi_array, ffi_schema) =
        to_ffi(&array.to_data()).map_err(|_| EngineError::Internal("arrow C ABI export failed"))?;
    let ffi_array = ManuallyDrop::new(ffi_array);
    let ffi_schema = ManuallyDrop::new(ffi_schema);
    let polars_array = unsafe {
        std::ptr::read((&*ffi_array as *const FFI_ArrowArray).cast::<PolarsArrowArray>())
    };
    let polars_schema = unsafe {
        std::ptr::read((&*ffi_schema as *const FFI_ArrowSchema).cast::<PolarsArrowSchema>())
    };
    Ok((polars_array, polars_schema))
}

fn import_into_polars(
    array: PolarsArrowArray,
    schema: PolarsArrowSchema,
) -> Result<polars::prelude::Column, EngineError> {
    let field = unsafe { import_field_from_c(&schema) }
        .map_err(|_| EngineError::Internal("polars field import failed"))?;
    let imported = unsafe { import_array_from_c(array, field.dtype.clone()) }
        .map_err(|_| EngineError::Internal("polars array import failed"))?;
    let name = field.name.clone();
    polars::prelude::Series::from_arrow(name, imported)
        .map(polars::prelude::Column::from)
        .map_err(|_| EngineError::Internal("polars series import failed"))
}

fn export_polars_array(
    field: &PolarsField,
    array: Box<dyn PolarsArray>,
) -> Result<ArrayRef, EngineError> {
    let array = export_array_to_c(array);
    let schema = export_field_to_c(field);
    let normalize_null = matches!(field.dtype, polars_arrow::datatypes::ArrowDataType::Null);
    let data = unsafe { import_exported(array, schema, normalize_null) }?;
    Ok(make_array(data))
}

unsafe fn import_exported(
    array: PolarsArrowArray,
    schema: PolarsArrowSchema,
    normalize_null_buffers: bool,
) -> Result<arrow_data::ArrayData, EngineError> {
    let mut array = array;
    let mut schema = schema;
    let mut arrow_array = unsafe {
        FFI_ArrowArray::from_raw((&mut array as *mut PolarsArrowArray).cast::<FFI_ArrowArray>())
    };
    let arrow_schema = unsafe {
        FFI_ArrowSchema::from_raw((&mut schema as *mut PolarsArrowSchema).cast::<FFI_ArrowSchema>())
    };
    if normalize_null_buffers {
        arrow_array.n_buffers = 0;
        arrow_array.buffers = std::ptr::null_mut();
    }
    unsafe { from_ffi(arrow_array, &arrow_schema) }.map_err(|_| EngineError::Ffi)
}
