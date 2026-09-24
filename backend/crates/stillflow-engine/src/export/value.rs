//! Shared export schema checks and Arrow value rendering.

use arrow_array::{
    Array, Float32Array, Float64Array, GenericListArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use chrono::DateTime;
use stillflow_core::{BatchEnvelope, ExportFormat, LogicalField, LogicalType, TimeUnit};

use super::type_error;
use crate::EngineError;

// ---------------------------------------------------------------------------
// Format matrix
// ---------------------------------------------------------------------------

/// Applies the frozen format matrix (ADR-004 §3) to the declared schema
/// before any byte is written: binary columns are Parquet-only; nested
/// list/struct columns are Parquet/JSONL-only. Fails closed, typed.
pub(super) fn check_format_columns(
    format: ExportFormat,
    fields: &[LogicalField],
) -> Result<(), EngineError> {
    for field in fields {
        check_format_leaf(format, &field.data_type)?;
    }
    Ok(())
}

fn check_format_leaf(format: ExportFormat, logical: &LogicalType) -> Result<(), EngineError> {
    match logical {
        LogicalType::Binary if format != ExportFormat::Parquet => Err(type_error(
            "binary columns are only legal in Parquet exports",
        )),
        LogicalType::List(inner) => {
            if format == ExportFormat::Csv || format == ExportFormat::Tsv {
                return Err(type_error(
                    "nested list columns are not legal in CSV/TSV exports",
                ));
            }
            check_format_leaf(format, inner)
        }
        LogicalType::Struct(fields) => {
            if format == ExportFormat::Csv || format == ExportFormat::Tsv {
                return Err(type_error(
                    "nested struct columns are not legal in CSV/TSV exports",
                ));
            }
            for field in fields {
                check_format_leaf(format, &field.data_type)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Scans one verified batch for non-finite float values, including values
/// nested inside lists and structs; NaN and ±inf fail typed in every format
/// before any byte of the batch is written (ADR-004 §3).
pub(super) fn ensure_finite_values(
    logical: &LogicalType,
    array: &dyn Array,
) -> Result<(), EngineError> {
    match logical {
        LogicalType::Float32 => {
            let values = downcast::<Float32Array>(array)?;
            for row in 0..values.len() {
                if !values.is_null(row) && !values.value(row).is_finite() {
                    return Err(type_error("export float value is not finite"));
                }
            }
        }
        LogicalType::Float64 => {
            let values = downcast::<Float64Array>(array)?;
            for row in 0..values.len() {
                if !values.is_null(row) && !values.value(row).is_finite() {
                    return Err(type_error("export float value is not finite"));
                }
            }
        }
        LogicalType::List(inner) => {
            let list = downcast::<GenericListArray<i32>>(array)?;
            ensure_finite_values(inner, list.values().as_ref())?;
        }
        LogicalType::Struct(fields) => {
            let struct_array = downcast::<StructArray>(array)?;
            for (column, field) in struct_array.columns().iter().zip(fields.iter()) {
                ensure_finite_values(&field.data_type, column.as_ref())?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn ensure_batch_finite(
    fields: &[LogicalField],
    envelope: &BatchEnvelope,
) -> Result<(), EngineError> {
    let columns = envelope.payload().columns();
    if columns.len() != fields.len() {
        return Err(type_error(
            "export batch column count drifted from the schema",
        ));
    }
    for (field, column) in fields.iter().zip(columns.iter()) {
        ensure_finite_values(&field.data_type, column.as_ref())?;
    }
    Ok(())
}

pub(super) fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T, EngineError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| type_error("export column physical type drifted from the canonical schema"))
}

// ---------------------------------------------------------------------------
// Shared value rendering
// ---------------------------------------------------------------------------

pub(super) fn timestamp_value(
    unit: TimeUnit,
    array: &dyn Array,
    row: usize,
) -> Result<i64, EngineError> {
    let value = match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array)?.value(row),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.value(row),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.value(row),
        TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.value(row),
    };
    Ok(value)
}

/// Renders one Date32 (days since the Unix epoch) as `%Y-%m-%d` (ADR-004 §3).
pub(super) fn format_date(days: i32) -> Result<String, EngineError> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or(EngineError::Internal("epoch date is not representable"))?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .ok_or_else(|| type_error("export date value is not representable"))?;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// Renders one timestamp of the given logical unit as a UTC RFC 3339 instant
/// with the `Z` suffix, preserving the unit's fractional precision (ADR-004
/// §3). Original offsets are not reconstructed in text.
pub(super) fn format_timestamp_value(unit: TimeUnit, value: i64) -> Result<String, EngineError> {
    let date_time = match unit {
        TimeUnit::Second => DateTime::from_timestamp(value, 0),
        TimeUnit::Millisecond => DateTime::from_timestamp_millis(value),
        TimeUnit::Microsecond => DateTime::from_timestamp_micros(value),
        TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(value)),
    }
    .ok_or_else(|| type_error("export timestamp value is not representable"))?;
    Ok(match unit {
        TimeUnit::Second => date_time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        TimeUnit::Millisecond => date_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        TimeUnit::Microsecond => date_time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
        TimeUnit::Nanosecond => date_time.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string(),
    })
}
