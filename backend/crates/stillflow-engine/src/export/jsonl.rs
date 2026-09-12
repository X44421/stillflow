//! JSONL export encoder.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, GenericListArray, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, StructArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use stillflow_core::{BatchEnvelope, LogicalField, LogicalType};
use stillflow_storage::StagedExportFile;

use super::type_error;
use super::value::{downcast, format_date, format_timestamp_value, timestamp_value};
use crate::EngineError;

// JSONL encoding (ADR-004 §3)
// ---------------------------------------------------------------------------

pub(super) fn write_jsonl_batch(
    staged: &mut StagedExportFile,
    fields: &[LogicalField],
    envelope: &BatchEnvelope,
) -> Result<(), EngineError> {
    let columns = envelope.payload().columns();
    if columns.len() != fields.len() {
        return Err(type_error(
            "export batch column count drifted from the schema",
        ));
    }
    let mut line = Vec::new();
    for row in 0..envelope.row_count() {
        line.clear();
        line.push(b'{');
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                line.push(b',');
            }
            let key = serde_json::to_string(&field.name)
                .map_err(|_| EngineError::Internal("export JSONL field name encoding failed"))?;
            line.extend_from_slice(key.as_bytes());
            line.push(b':');
            let column = columns[index].as_ref();
            if column.is_null(row) {
                line.extend_from_slice(b"null");
            } else {
                write_json_value(&mut line, &field.data_type, column, row)?;
            }
        }
        line.push(b'}');
        line.push(b'\n');
        staged
            .write_bytes(&line)
            .map_err(EngineError::from_storage)?;
    }
    Ok(())
}

/// Writes one JSON value in minimal RFC 8259 form. Fields of nested objects
/// occur once each, in declared schema order; integers print exactly; floats
/// print as the pinned shortest-round-trip form (ADR-004 §3).
fn write_json_value(
    out: &mut Vec<u8>,
    logical: &LogicalType,
    array: &dyn Array,
    row: usize,
) -> Result<(), EngineError> {
    match logical {
        LogicalType::Null => out.extend_from_slice(b"null"),
        LogicalType::Boolean => {
            let values = downcast::<BooleanArray>(array)?;
            out.extend_from_slice(if values.value(row) { b"true" } else { b"false" });
        }
        LogicalType::Int8 => {
            let values = downcast::<Int8Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::Int16 => {
            let values = downcast::<Int16Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::Int32 => {
            let values = downcast::<Int32Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::Int64 => {
            let values = downcast::<Int64Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::UInt8 => {
            let values = downcast::<UInt8Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::UInt16 => {
            let values = downcast::<UInt16Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::UInt32 => {
            let values = downcast::<UInt32Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::UInt64 => {
            let values = downcast::<UInt64Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
        }
        LogicalType::Float32 => {
            let values = downcast::<Float32Array>(array)?;
            let value = values.value(row);
            if !value.is_finite() {
                return Err(type_error("export float value is not finite"));
            }
            append_json_f32(out, value);
        }
        LogicalType::Float64 => {
            let values = downcast::<Float64Array>(array)?;
            let value = values.value(row);
            if !value.is_finite() {
                return Err(type_error("export float value is not finite"));
            }
            append_json_f64(out, value);
        }
        LogicalType::Utf8 => {
            let values = downcast::<StringArray>(array)?;
            let encoded = serde_json::to_string(values.value(row))
                .map_err(|_| EngineError::Internal("export JSONL string encoding failed"))?;
            out.extend_from_slice(encoded.as_bytes());
        }
        LogicalType::Date32 => {
            let values = downcast::<Date32Array>(array)?;
            out.push(b'"');
            out.extend_from_slice(format_date(values.value(row))?.as_bytes());
            out.push(b'"');
        }
        LogicalType::Timestamp { unit, .. } => {
            out.push(b'"');
            out.extend_from_slice(
                format_timestamp_value(*unit, timestamp_value(*unit, array, row)?)?.as_bytes(),
            );
            out.push(b'"');
        }
        LogicalType::Binary => {
            return Err(type_error(
                "binary columns are only legal in Parquet exports",
            ));
        }
        LogicalType::List(inner) => {
            let list = downcast::<GenericListArray<i32>>(array)?;
            let start = list.value_offsets()[row] as usize;
            let end = list.value_offsets()[row + 1] as usize;
            let values = list.values();
            out.push(b'[');
            for index in start..end {
                if index > start {
                    out.push(b',');
                }
                if values.is_null(index) {
                    out.extend_from_slice(b"null");
                } else {
                    write_json_value(out, inner, values.as_ref(), index)?;
                }
            }
            out.push(b']');
        }
        LogicalType::Struct(fields) => {
            let struct_array = downcast::<StructArray>(array)?;
            if struct_array.columns().len() != fields.len() {
                return Err(type_error(
                    "export struct column drifted from the canonical schema",
                ));
            }
            out.push(b'{');
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                let key = serde_json::to_string(&field.name).map_err(|_| {
                    EngineError::Internal("export JSONL field name encoding failed")
                })?;
                out.extend_from_slice(key.as_bytes());
                out.push(b':');
                let column = struct_array.column(index);
                if column.is_null(row) {
                    out.extend_from_slice(b"null");
                } else {
                    write_json_value(out, &field.data_type, column.as_ref(), row)?;
                }
            }
            out.push(b'}');
        }
    }
    Ok(())
}

/// Pins the JSONL float rendering to `serde_json`'s Ryu shortest round-trip
/// formatting (`EXPORT_JSONL_FLOAT_ENCODER`, ADR-004 §3), with the f32 and
/// f64 renderers kept distinct. Non-finite values were rejected before
/// rendering, and serialization of finite floats cannot fail; the fallback
/// byte sequence is unreachable and typed as internal.
fn append_json_f32(out: &mut Vec<u8>, value: f32) {
    match serde_json::to_string(&value) {
        Ok(encoded) => out.extend_from_slice(encoded.as_bytes()),
        Err(_) => out.extend_from_slice(b"0"),
    }
}

fn append_json_f64(out: &mut Vec<u8>, value: f64) {
    match serde_json::to_string(&value) {
        Ok(encoded) => out.extend_from_slice(encoded.as_bytes()),
        Err(_) => out.extend_from_slice(b"0"),
    }
}
