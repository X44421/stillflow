//! CSV and TSV export encoders.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use stillflow_core::{BatchEnvelope, ExportFormat, LogicalField, LogicalType};
use stillflow_storage::StagedExportFile;

use super::type_error;
use super::value::{downcast, format_date, format_timestamp_value, timestamp_value};
use crate::EngineError;

// ---------------------------------------------------------------------------
// CSV / TSV encoding (ADR-004 §3)
// ---------------------------------------------------------------------------

pub(super) fn text_delimiter(format: ExportFormat) -> Result<u8, EngineError> {
    format
        .text_delimiter()
        .ok_or_else(|| type_error("text delimiter requested for a non-text export format"))
}

pub(super) fn write_text_header(
    staged: &mut StagedExportFile,
    fields: &[LogicalField],
    format: ExportFormat,
) -> Result<(), EngineError> {
    let delimiter = text_delimiter(format)?;
    let mut line = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            line.push(delimiter);
        }
        encode_text_field(&mut line, field.name.as_bytes(), delimiter);
    }
    line.push(b'\n');
    staged.write_bytes(&line).map_err(EngineError::from_storage)
}

pub(super) fn write_text_batch(
    staged: &mut StagedExportFile,
    fields: &[LogicalField],
    envelope: &BatchEnvelope,
    format: ExportFormat,
) -> Result<(), EngineError> {
    let delimiter = text_delimiter(format)?;
    let columns = envelope.payload().columns();
    if columns.len() != fields.len() {
        return Err(type_error(
            "export batch column count drifted from the schema",
        ));
    }
    let mut line = Vec::new();
    let mut rendered = Vec::new();
    for row in 0..envelope.row_count() {
        line.clear();
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                line.push(delimiter);
            }
            let column = columns[index].as_ref();
            if column.is_null(row) {
                continue; // null: unquoted empty field
            }
            rendered.clear();
            render_text_value(&mut rendered, &field.data_type, column, row)?;
            encode_text_field(&mut line, &rendered, delimiter);
        }
        line.push(b'\n');
        staged
            .write_bytes(&line)
            .map_err(EngineError::from_storage)?;
    }
    Ok(())
}

/// Quoting law: quote iff empty or containing the delimiter, a double quote,
/// LF, or CR; embedded double quotes are doubled (ADR-004 §3).
fn encode_text_field(line: &mut Vec<u8>, value: &[u8], delimiter: u8) {
    let needs_quote = value.is_empty()
        || value
            .iter()
            .any(|byte| *byte == delimiter || *byte == b'"' || *byte == b'\n' || *byte == b'\r');
    if needs_quote {
        line.push(b'"');
        for &byte in value {
            if byte == b'"' {
                line.push(b'"');
            }
            line.push(byte);
        }
        line.push(b'"');
    } else {
        line.extend_from_slice(value);
    }
}

fn render_text_value(
    out: &mut Vec<u8>,
    logical: &LogicalType,
    array: &dyn Array,
    row: usize,
) -> Result<(), EngineError> {
    match logical {
        LogicalType::Null => Ok(()),
        LogicalType::Boolean => {
            let values = downcast::<BooleanArray>(array)?;
            out.extend_from_slice(if values.value(row) { b"true" } else { b"false" });
            Ok(())
        }
        LogicalType::Int8 => {
            let values = downcast::<Int8Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::Int16 => {
            let values = downcast::<Int16Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::Int32 => {
            let values = downcast::<Int32Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::Int64 => {
            let values = downcast::<Int64Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::UInt8 => {
            let values = downcast::<UInt8Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::UInt16 => {
            let values = downcast::<UInt16Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::UInt32 => {
            let values = downcast::<UInt32Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::UInt64 => {
            let values = downcast::<UInt64Array>(array)?;
            out.extend_from_slice(values.value(row).to_string().as_bytes());
            Ok(())
        }
        LogicalType::Float32 => {
            let values = downcast::<Float32Array>(array)?;
            let value = values.value(row);
            if !value.is_finite() {
                return Err(type_error("export float value is not finite"));
            }
            out.extend_from_slice(value.to_string().as_bytes());
            Ok(())
        }
        LogicalType::Float64 => {
            let values = downcast::<Float64Array>(array)?;
            let value = values.value(row);
            if !value.is_finite() {
                return Err(type_error("export float value is not finite"));
            }
            out.extend_from_slice(value.to_string().as_bytes());
            Ok(())
        }
        LogicalType::Utf8 => {
            let values = downcast::<StringArray>(array)?;
            out.extend_from_slice(values.value(row).as_bytes());
            Ok(())
        }
        LogicalType::Date32 => {
            let values = downcast::<Date32Array>(array)?;
            let rendered = format_date(values.value(row))?;
            out.extend_from_slice(rendered.as_bytes());
            Ok(())
        }
        LogicalType::Timestamp { unit, .. } => {
            let rendered = format_timestamp_value(*unit, timestamp_value(*unit, array, row)?)?;
            out.extend_from_slice(rendered.as_bytes());
            Ok(())
        }
        LogicalType::Binary | LogicalType::List(_) | LogicalType::Struct(_) => Err(type_error(
            "export column shape is not legal in CSV/TSV exports",
        )),
    }
}

// ---------------------------------------------------------------------------
