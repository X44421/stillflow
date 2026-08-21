//! CSV export from a committed snapshot. Experimental E4 probe:
//! Snapshot is a sufficient Export input without re-reading the source.

use std::io::Write;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use stillflow_storage::SnapshotStore;
use uuid::Uuid;

use crate::error::EngineError;

pub fn export_snapshot_to_csv(
    store: &SnapshotStore,
    snapshot_id: Uuid,
    writer: &mut impl Write,
) -> Result<(), EngineError> {
    let mut reader = store
        .read_batches(snapshot_id)
        .map_err(EngineError::from_storage)?;
    let schema = reader.manifest().snapshot().schema().clone();
    write_csv_row(
        writer,
        schema.fields.iter().map(|field| field.name.as_str()),
    )?;
    for envelope in &mut reader {
        let envelope = envelope.map_err(EngineError::from_storage)?;
        let batch = envelope.payload();
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                cells.push(csv_cell(column.as_ref(), row)?);
            }
            write_csv_row(writer, cells.iter().map(String::as_str))?;
        }
    }
    Ok(())
}

fn write_csv_row<'a>(
    writer: &mut impl Write,
    cells: impl Iterator<Item = &'a str>,
) -> Result<(), EngineError> {
    let mut first = true;
    for cell in cells {
        if !first {
            writer
                .write_all(b",")
                .map_err(|_| EngineError::Internal("csv write failed"))?;
        }
        first = false;
        write_csv_cell(writer, cell)?;
    }
    writer
        .write_all(b"\n")
        .map_err(|_| EngineError::Internal("csv write failed"))?;
    Ok(())
}

fn write_csv_cell(writer: &mut impl Write, cell: &str) -> Result<(), EngineError> {
    let needs_quotes = cell.contains([',', '"', '\n', '\r']);
    if needs_quotes {
        let escaped = cell.replace('"', "\"\"");
        writer
            .write_all(b"\"")
            .map_err(|_| EngineError::Internal("csv write failed"))?;
        writer
            .write_all(escaped.as_bytes())
            .map_err(|_| EngineError::Internal("csv write failed"))?;
        writer
            .write_all(b"\"")
            .map_err(|_| EngineError::Internal("csv write failed"))?;
        return Ok(());
    }
    writer
        .write_all(cell.as_bytes())
        .map_err(|_| EngineError::Internal("csv write failed"))?;
    Ok(())
}

fn csv_cell(array: &dyn Array, row: usize) -> Result<String, EngineError> {
    if array.is_null(row) {
        return Ok(String::new());
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(if values.value(row) {
            "true".to_owned()
        } else {
            "false".to_owned()
        });
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(row).to_owned());
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(bytes_to_hex(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(values.value(row).to_string());
    }
    Err(EngineError::Internal("unsupported csv export type"))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
