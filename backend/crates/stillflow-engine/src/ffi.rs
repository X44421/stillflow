//! Engine-owned Polars 0.46 ↔ Arrow 59 C Data Interface bridge.

#[cfg(test)]
use std::cell::Cell;
use std::mem::{align_of, size_of, ManuallyDrop};
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, NullArray, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use polars::prelude::DataFrame;
use polars_arrow::ffi::{
    import_array_from_c, import_field_from_c, ArrowArray as PolarsArrowArray,
    ArrowSchema as PolarsArrowSchema,
};
use stillflow_core::{LogicalSchema, LogicalType, ScalarValue, TimeUnit};

use crate::error::EngineError;

const _: () = assert!(size_of::<PolarsArrowArray>() == size_of::<FFI_ArrowArray>());
const _: () = assert!(align_of::<PolarsArrowArray>() == align_of::<FFI_ArrowArray>());
const _: () = assert!(size_of::<PolarsArrowSchema>() == size_of::<FFI_ArrowSchema>());
const _: () = assert!(align_of::<PolarsArrowSchema>() == align_of::<FFI_ArrowSchema>());

#[cfg(test)]
thread_local! {
    static IMPORT_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn ffi_import_count() -> u64 {
    IMPORT_COUNT.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_ffi_import_count() {
    IMPORT_COUNT.with(|cell| cell.set(0));
}

pub(crate) fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, EngineError> {
    #[cfg(test)]
    IMPORT_COUNT.with(|cell| cell.set(cell.get().saturating_add(1)));
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
    for array in batch.columns() {
        match export_arrow_array(array.as_ref()) {
            Ok(pair) => exported.push(pair),
            Err(error) => {
                drop(exported);
                return Err(error);
            }
        }
    }

    let arrow_schema = batch.schema();
    for (index, (array, schema)) in exported.into_iter().enumerate() {
        match import_into_polars(array, schema) {
            Ok(column) => {
                columns.push(column.with_name(arrow_schema.field(index).name().into()));
            }
            Err(error) => {
                drop(columns);
                return Err(error);
            }
        }
    }

    DataFrame::new(columns)
        .map_err(|_| EngineError::Internal("polars dataframe construction failed"))
}

pub(crate) fn dataframe_to_record_batch(
    frame: DataFrame,
    logical_schema: &LogicalSchema,
    target_schema: &SchemaRef,
    deferred: &[(String, ScalarValue)],
) -> Result<RecordBatch, EngineError> {
    if frame.width() != logical_schema.fields.len() {
        return Err(EngineError::Internal(
            "polars frame width does not match schema",
        ));
    }
    let height = frame.height();
    if logical_schema.fields.is_empty() {
        drop(frame);
        let options = RecordBatchOptions::new().with_row_count(Some(height));
        return RecordBatch::try_new_with_options(Arc::clone(target_schema), Vec::new(), &options)
            .map_err(|_| EngineError::Ffi);
    }

    let mut extracted: Vec<Option<ArrayRef>> = Vec::with_capacity(logical_schema.fields.len());
    for field in &logical_schema.fields {
        if deferred.iter().any(|(name, _)| name == field.name.as_str()) {
            extracted.push(None);
            continue;
        }
        let column = frame
            .column(field.name.as_str())
            .map_err(|_| EngineError::Internal("polars column missing during export"))?;
        extracted.push(Some(column_to_arrow(column, &field.data_type)?));
    }
    drop(frame);

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(logical_schema.fields.len());
    for (field, existing) in logical_schema.fields.iter().zip(extracted) {
        if let Some(array) = existing {
            arrays.push(array);
            continue;
        }
        let value = deferred
            .iter()
            .find(|(name, _)| name == field.name.as_str())
            .map(|(_, value)| value)
            .ok_or(EngineError::Internal(
                "deferred literal missing during export",
            ))?;
        arrays.push(array_from_literal(value, &field.data_type, height)?);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(height));
    RecordBatch::try_new_with_options(Arc::clone(target_schema), arrays, &options)
        .map_err(|_| EngineError::Internal("canonical record batch reconstruction failed"))
}

fn column_to_arrow(
    column: &polars::prelude::Column,
    data_type: &LogicalType,
) -> Result<ArrayRef, EngineError> {
    match data_type {
        LogicalType::Null => Ok(Arc::new(NullArray::new(column.len()))),
        LogicalType::Boolean => bool_from_polars(column),
        LogicalType::Int8 => map_i8(column),
        LogicalType::Int16 => map_i16(column),
        LogicalType::Int32 => map_i32(column),
        LogicalType::Int64 => map_i64(column),
        LogicalType::UInt8 => map_u8(column),
        LogicalType::UInt16 => map_u16(column),
        LogicalType::UInt32 => map_u32(column),
        LogicalType::UInt64 => map_u64(column),
        LogicalType::Float32 => map_f32(column),
        LogicalType::Float64 => map_f64(column),
        LogicalType::Utf8 => utf8_from_polars_column(column),
        LogicalType::Binary => binary_from_polars_column(column),
        LogicalType::Date32 => date_from_polars(column),
        LogicalType::Timestamp { unit, timezone } => {
            timestamp_from_polars(column, *unit, timezone.as_deref())
        }
        LogicalType::List(_) | LogicalType::Struct(_) => Err(EngineError::TypeError(
            "list and struct execution is paused",
        )),
    }
}

fn primitive_from_iter<T, I>(len: usize, values: I) -> ArrayRef
where
    T: ArrowPrimitiveType,
    I: IntoIterator<Item = Option<T::Native>>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(len);
    for value in values {
        match value {
            Some(native) => builder.append_value(native),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn map_i8(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.i8().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<Int8Type, _>(ca.len(), ca.iter()))
}

fn map_i16(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.i16().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<Int16Type, _>(ca.len(), ca.iter()))
}

fn map_i32(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.i32().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<Int32Type, _>(ca.len(), ca.iter()))
}

fn map_i64(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column
        .i64()
        .map_err(|_| EngineError::Internal("export expected int64"))?;
    Ok(primitive_from_iter::<Int64Type, _>(ca.len(), ca.iter()))
}

fn map_u8(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.u8().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<UInt8Type, _>(ca.len(), ca.iter()))
}

fn map_u16(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.u16().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<UInt16Type, _>(ca.len(), ca.iter()))
}

fn map_u32(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.u32().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<UInt32Type, _>(ca.len(), ca.iter()))
}

fn map_u64(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.u64().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<UInt64Type, _>(ca.len(), ca.iter()))
}

fn map_f32(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.f32().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<Float32Type, _>(ca.len(), ca.iter()))
}

fn map_f64(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.f64().map_err(|_| EngineError::Ffi)?;
    Ok(primitive_from_iter::<Float64Type, _>(ca.len(), ca.iter()))
}

fn bool_from_polars(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.bool().map_err(|_| EngineError::Ffi)?;
    let mut builder = BooleanBuilder::with_capacity(ca.len());
    for value in ca.iter() {
        match value {
            Some(flag) => builder.append_value(flag),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn date_from_polars(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.date().map_err(|_| EngineError::Ffi)?;
    let physical = ca.physical();
    Ok(primitive_from_iter::<Date32Type, _>(
        physical.len(),
        physical.iter(),
    ))
}

fn timestamp_from_polars(
    column: &polars::prelude::Column,
    unit: TimeUnit,
    timezone: Option<&str>,
) -> Result<ArrayRef, EngineError> {
    let ca = column.datetime().map_err(|_| EngineError::Ffi)?;
    let physical = ca.physical();
    Ok(match unit {
        TimeUnit::Millisecond => timestamp_from_iter::<TimestampMillisecondType, _>(
            physical.len(),
            physical.iter(),
            timezone,
        ),
        TimeUnit::Microsecond => timestamp_from_iter::<TimestampMicrosecondType, _>(
            physical.len(),
            physical.iter(),
            timezone,
        ),
        TimeUnit::Nanosecond => timestamp_from_iter::<TimestampNanosecondType, _>(
            physical.len(),
            physical.iter(),
            timezone,
        ),
        TimeUnit::Second => {
            return Err(EngineError::TypeError("timestamp second unit is paused"));
        }
    })
}

fn timestamp_from_iter<T, I>(len: usize, values: I, timezone: Option<&str>) -> ArrayRef
where
    T: arrow_array::types::ArrowTimestampType<Native = i64>,
    I: IntoIterator<Item = Option<i64>>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(len);
    for value in values {
        match value {
            Some(native) => builder.append_value(native),
            None => builder.append_null(),
        }
    }
    Arc::new(
        builder
            .finish()
            .with_timezone_opt(timezone.map(str::to_owned)),
    )
}

fn utf8_from_polars_column(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    if let Some(scalar) = column.as_scalar_column() {
        use polars::prelude::AnyValue;
        let len = scalar.len();
        return match scalar.scalar().value() {
            AnyValue::Null => {
                let mut builder = StringBuilder::with_capacity(len, 0);
                for _ in 0..len {
                    builder.append_null();
                }
                Ok(Arc::new(builder.finish()))
            }
            AnyValue::String(text) => utf8_repeat(text, len),
            AnyValue::StringOwned(text) => utf8_repeat(text.as_str(), len),
            _ => Err(EngineError::Internal("export expected utf8")),
        };
    }
    let ca = column
        .str()
        .map_err(|_| EngineError::Internal("export expected utf8"))?;
    let data_hint = ca
        .iter()
        .flatten()
        .map(str::len)
        .fold(0_usize, usize::saturating_add);
    let mut builder = StringBuilder::with_capacity(ca.len(), data_hint);
    for value in ca.iter() {
        match value {
            Some(text) => builder.append_value(text),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn array_from_literal(
    value: &ScalarValue,
    data_type: &LogicalType,
    len: usize,
) -> Result<ArrayRef, EngineError> {
    match (value, data_type) {
        (ScalarValue::Null, LogicalType::Utf8) => {
            let mut builder = StringBuilder::with_capacity(len, 0);
            for _ in 0..len {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        (ScalarValue::Utf8(text), LogicalType::Utf8) => utf8_repeat(text, len),
        _ => Err(EngineError::Internal(
            "deferred literal is not an authorized utf8 value",
        )),
    }
}

fn utf8_repeat(text: &str, len: usize) -> Result<ArrayRef, EngineError> {
    let bytes = text.as_bytes();
    let total = bytes.len().saturating_mul(len);
    let mut values = Vec::new();
    values.reserve_exact(total);
    for _ in 0..len {
        values.extend_from_slice(bytes);
    }
    values.shrink_to_fit();
    let step = i32::try_from(bytes.len())
        .map_err(|_| EngineError::BoundExceeded("utf8 literal is wider than the offset type"))?;
    let mut offsets = Vec::new();
    offsets.reserve_exact(len.saturating_add(1));
    let mut offset = 0_i32;
    offsets.push(0);
    for _ in 0..len {
        offset = offset
            .checked_add(step)
            .ok_or(EngineError::BoundExceeded("utf8 literal offset overflow"))?;
        offsets.push(offset);
    }
    offsets.shrink_to_fit();
    StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(values),
        None,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|_| EngineError::Internal("utf8 literal array is invalid"))
}

fn binary_from_polars_column(column: &polars::prelude::Column) -> Result<ArrayRef, EngineError> {
    let ca = column.binary().map_err(|_| EngineError::Ffi)?;
    let data_hint = ca
        .iter()
        .flatten()
        .map(<[u8]>::len)
        .fold(0_usize, usize::saturating_add);
    let mut builder = BinaryBuilder::with_capacity(ca.len(), data_hint);
    for value in ca.iter() {
        match value {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
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
