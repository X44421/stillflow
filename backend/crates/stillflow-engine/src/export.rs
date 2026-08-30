//! Deterministic streaming ExportArtifact runtime (ADR-004 §§2–8).
//!
//! The runtime reads exactly one committed immutable Snapshot through the
//! existing `SnapshotStore` reader, runs the complete visible-manifest
//! verification battery over every input partition before the first encoded
//! byte, streams CSV/TSV/JSONL/Parquet output into storage-owned staging, and
//! publishes an all-or-none Export Manifest through the single
//! [`stillflow_storage::ExportWriter`] publication path. Cancellation,
//! deadlines, and bounds are checkpointed at the ADR-004 §8 points. No second
//! scheduler, digest authority, Snapshot reader, or publication path exists
//! here.

use std::time::Duration;

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, GenericListArray, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use stillflow_core::{
    logical_schema_to_arrow, BatchEnvelope, ExportDestination, ExportError, ExportFormat,
    ExportInputIdentity, ExportPolicy, ExportResult, ExportResultFile, ExportShape, LogicalField,
    LogicalSchema, LogicalSchemaFingerprint, LogicalType, RequestContext, TimeUnit,
    EXPORT_DEFAULT_DEADLINE_SECONDS, MAX_BATCH_ROWS, MAX_EXPORT_PARTITIONS, MAX_EXPORT_ROWS,
};
use stillflow_storage::{
    ExportPlan, ExportProvenance, ExportWriter, SnapshotStore, StagedExportFile, StorageError,
};
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::map_context_error;
use crate::{EngineError, ENGINE_CONTRACT_VERSION, ENGINE_MAX_DEADLINE};

/// Caller-facing request of one export (ADR-004 §1, §2, §6).
///
/// The export id is caller-injected; the created-at instant is injected by the
/// caller so publication provenance carries no wall-clock read of this module.
#[derive(Debug, Clone)]
pub struct ExportRequest {
    pub export_id: Uuid,
    pub snapshot_id: Uuid,
    pub format: ExportFormat,
    pub policy: ExportPolicy,
    pub destination: ExportDestination,
    pub created_at: DateTime<Utc>,
    pub context: RequestContext,
}

/// Runs one export against one committed Snapshot (ADR-004 §§2–8).
///
/// The function is synchronous and checkpointed: cancellation and deadline
/// checks occur before input verification, after each verified input
/// partition, after each output append, and before every publication step. On
/// any failure the caller receives a typed error, staging is removed
/// best-effort by the writer, and journaled residue is removed by the
/// definitive storage recovery sweep; no visible artifact can result.
pub fn run_export(
    store: &SnapshotStore,
    request: ExportRequest,
) -> Result<ExportResult, EngineError> {
    if request.export_id.is_nil() {
        return Err(export_error(ExportError::NilIdentity("export")));
    }
    let mut context = request.context.clone();
    if context.deadline().is_none() {
        context = RequestContext::with_cancellation_and_deadline(
            context.cancellation().clone(),
            Instant::now() + Duration::from_secs(EXPORT_DEFAULT_DEADLINE_SECONDS),
        );
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining > ENGINE_MAX_DEADLINE)
    {
        return Err(EngineError::BoundExceeded(
            "export deadline exceeds ENGINE_MAX_DEADLINE",
        ));
    }
    // Checkpoint: before input verification (ADR-004 §8).
    checkpoint(&context)?;

    // Committed input: only a visible snapshot manifest loads; live, preview,
    // tombstoned, and open-writer states fail typed here (ADR-004 §2).
    let manifest = store
        .load_manifest(request.snapshot_id)
        .map_err(EngineError::from_storage)?;
    let snapshot = manifest.snapshot();
    let input = ExportInputIdentity::try_new(
        snapshot.id(),
        snapshot.dataset_id(),
        snapshot.session_id(),
        snapshot.source_asset_id(),
        snapshot.schema_fingerprint(),
        snapshot.version(),
    )
    .map_err(export_error)?;
    let plan = ExportPlan::try_new(
        request.export_id,
        input,
        request.destination.clone(),
        request.format,
        request.policy,
    )
    .map_err(EngineError::from_storage)?;

    check_format_columns(request.format, snapshot.schema().fields.as_slice())?;

    // Phase 1 — complete input verification before the first output byte:
    // every partition passes the full read_batches battery (no-follow,
    // regular file, stored length, digest, canonical schema, row count,
    // single-batch shape) and payloads are dropped unencoded.
    let mut verified_rows = 0_u64;
    {
        let mut verified_partitions = 0_u64;
        let reader = store
            .read_batches(request.snapshot_id)
            .map_err(EngineError::from_storage)?;
        for batch in reader {
            let envelope = batch.map_err(EngineError::from_storage)?;
            verified_rows = add_rows(verified_rows, envelope.row_count())?;
            if verified_rows > MAX_EXPORT_ROWS {
                return Err(EngineError::BoundExceeded(
                    "export row bound exceeded before encoding",
                ));
            }
            verified_partitions =
                verified_partitions
                    .checked_add(1)
                    .ok_or(EngineError::BoundExceeded(
                        "export partition count overflow",
                    ))?;
            if request.policy.shape == ExportShape::PartitionedSet
                && verified_partitions > u64::from(MAX_EXPORT_PARTITIONS)
            {
                return Err(EngineError::BoundExceeded(
                    "export partitioned set exceeds MAX_EXPORT_PARTITIONS",
                ));
            }
            // Checkpoint: after each verified input partition (ADR-004 §8).
            checkpoint(&context)?;
        }
    }

    // Phase 2 — bounded streaming encoding and publication.
    let mut export_writer = store
        .begin_export(plan, request.created_at)
        .map_err(EngineError::from_storage)?;
    let encoded_rows = encode_stream(
        store,
        &mut export_writer,
        &request,
        &context,
        snapshot.schema(),
    )?;

    // Checkpoint: before the publication commit (ADR-004 §8).
    checkpoint(&context)?;
    let committed = export_writer
        .commit(ExportProvenance {
            created_at: request.created_at,
            row_count: encoded_rows,
            engine_contract_version: ENGINE_CONTRACT_VERSION,
        })
        .map_err(EngineError::from_storage)?;

    let finished = Instant::now();
    let deadline_overshoot = context
        .deadline()
        .and_then(|deadline| finished.checked_duration_since(deadline));

    let files = committed
        .files()
        .iter()
        .map(|file| {
            ExportResultFile::try_new(
                file.name().to_owned(),
                file.byte_count(),
                file.digest().to_owned(),
            )
            .map_err(export_error)
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    ExportResult::try_new(
        committed.export_id(),
        input,
        committed.format(),
        committed.shape(),
        committed.row_count(),
        files,
        committed.set_digest().to_owned(),
        committed.manifest_version(),
        committed.destination_root().to_path_buf(),
        committed.destination_relative().to_vec(),
        deadline_overshoot,
    )
    .map_err(export_error)
}

fn checkpoint(context: &RequestContext) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)
}

fn add_rows(total: u64, rows: usize) -> Result<u64, EngineError> {
    let rows =
        u64::try_from(rows).map_err(|_| EngineError::BoundExceeded("export row overflow"))?;
    total
        .checked_add(rows)
        .ok_or(EngineError::BoundExceeded("export row count overflow"))
}

fn export_error(error: ExportError) -> EngineError {
    EngineError::from_connector(error.into_connector_error())
}

fn type_error(message: &'static str) -> EngineError {
    EngineError::TypeError(message)
}

// ---------------------------------------------------------------------------
// Format matrix
// ---------------------------------------------------------------------------

/// Applies the frozen format matrix (ADR-004 §3) to the declared schema
/// before any byte is written: binary columns are Parquet-only; nested
/// list/struct columns are Parquet/JSONL-only. Fails closed, typed.
fn check_format_columns(format: ExportFormat, fields: &[LogicalField]) -> Result<(), EngineError> {
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
fn ensure_finite_values(logical: &LogicalType, array: &dyn Array) -> Result<(), EngineError> {
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

fn ensure_batch_finite(
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

// ---------------------------------------------------------------------------
// Encoding loop
// ---------------------------------------------------------------------------

/// Streams every verified partition into the writer and returns the encoded
/// row total. Single-file artifacts concatenate partitions in stored order
/// into one staged stream; partitioned artifacts install one
/// `part-<seq:010>.<ext>` member per input partition, in input partition
/// order, with no repartitioning (ADR-004 §4, §5).
fn encode_stream(
    store: &SnapshotStore,
    export_writer: &mut ExportWriter,
    request: &ExportRequest,
    context: &RequestContext,
    schema: &LogicalSchema,
) -> Result<u64, EngineError> {
    let fields = schema.fields.as_slice();
    let mut total_rows = 0_u64;
    let mut next_part = 0_u32;

    match (request.format, request.policy.shape) {
        (ExportFormat::Parquet, ExportShape::SingleFile) => {
            let mut staged = export_writer
                .create_staged_file()
                .map_err(EngineError::from_storage)?;
            {
                let mut writer = open_parquet_writer(&mut staged, schema)?;
                let reader = store
                    .read_batches(request.snapshot_id)
                    .map_err(EngineError::from_storage)?;
                for batch in reader {
                    let envelope = batch.map_err(EngineError::from_storage)?;
                    ensure_batch_finite(fields, &envelope)?;
                    total_rows = add_rows(total_rows, envelope.row_count())?;
                    if total_rows > MAX_EXPORT_ROWS {
                        return Err(EngineError::BoundExceeded("export row bound exceeded"));
                    }
                    writer
                        .write(envelope.payload())
                        .map_err(map_parquet_error)?;
                    // Checkpoint: after each output append (ADR-004 §8).
                    checkpoint(context)?;
                }
                writer.into_inner().map_err(map_parquet_error)?;
            }
            staged
                .refresh_accounting()
                .map_err(EngineError::from_storage)?;
            // Checkpoint: before every publication step (ADR-004 §8).
            checkpoint(context)?;
            install(export_writer, staged)?;
        }
        (ExportFormat::Parquet, ExportShape::PartitionedSet) => {
            let reader = store
                .read_batches(request.snapshot_id)
                .map_err(EngineError::from_storage)?;
            for batch in reader {
                let envelope = batch.map_err(EngineError::from_storage)?;
                ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                let mut staged = open_partition(export_writer, &mut next_part)?;
                {
                    let mut writer = open_parquet_writer(&mut staged, schema)?;
                    writer
                        .write(envelope.payload())
                        .map_err(map_parquet_error)?;
                    writer.into_inner().map_err(map_parquet_error)?;
                }
                staged
                    .refresh_accounting()
                    .map_err(EngineError::from_storage)?;
                checkpoint(context)?;
                install(export_writer, staged)?;
                checkpoint(context)?;
            }
        }
        (format, ExportShape::SingleFile) => {
            let mut staged = export_writer
                .create_staged_file()
                .map_err(EngineError::from_storage)?;
            if format == ExportFormat::Csv || format == ExportFormat::Tsv {
                write_text_header(&mut staged, fields, format)?;
            }
            let reader = store
                .read_batches(request.snapshot_id)
                .map_err(EngineError::from_storage)?;
            for batch in reader {
                let envelope = batch.map_err(EngineError::from_storage)?;
                ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                match format {
                    ExportFormat::Csv | ExportFormat::Tsv => {
                        write_text_batch(&mut staged, fields, &envelope, format)?;
                    }
                    ExportFormat::Jsonl => {
                        write_jsonl_batch(&mut staged, fields, &envelope)?;
                    }
                    ExportFormat::Parquet => unreachable!("handled by the Parquet arm"),
                }
                // Checkpoint: after each output append (ADR-004 §8).
                checkpoint(context)?;
            }
            checkpoint(context)?;
            install(export_writer, staged)?;
        }
        (format, ExportShape::PartitionedSet) => {
            let reader = store
                .read_batches(request.snapshot_id)
                .map_err(EngineError::from_storage)?;
            for batch in reader {
                let envelope = batch.map_err(EngineError::from_storage)?;
                ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                let mut staged = open_partition(export_writer, &mut next_part)?;
                match format {
                    ExportFormat::Csv | ExportFormat::Tsv => {
                        write_text_header(&mut staged, fields, format)?;
                        write_text_batch(&mut staged, fields, &envelope, format)?;
                    }
                    ExportFormat::Jsonl => {
                        write_jsonl_batch(&mut staged, fields, &envelope)?;
                    }
                    ExportFormat::Parquet => unreachable!("handled by the Parquet arm"),
                }
                checkpoint(context)?;
                install(export_writer, staged)?;
                checkpoint(context)?;
            }
        }
    }

    Ok(total_rows)
}

fn open_partition(
    export_writer: &mut ExportWriter,
    next_part: &mut u32,
) -> Result<StagedExportFile, EngineError> {
    if u64::from(*next_part) >= u64::from(MAX_EXPORT_PARTITIONS) {
        return Err(EngineError::BoundExceeded(
            "export partitioned set exceeds MAX_EXPORT_PARTITIONS",
        ));
    }
    let staged = export_writer
        .create_staged_file()
        .map_err(EngineError::from_storage)?;
    *next_part = next_part
        .checked_add(1)
        .ok_or(EngineError::BoundExceeded("export part sequence overflow"))?;
    Ok(staged)
}

fn install(export_writer: &mut ExportWriter, staged: StagedExportFile) -> Result<(), EngineError> {
    export_writer
        .install_staged_file(staged)
        .map_err(EngineError::from_storage)?;
    Ok(())
}

fn open_parquet_writer<'a>(
    staged: &'a mut StagedExportFile,
    schema: &LogicalSchema,
) -> Result<ArrowWriter<&'a mut StagedExportFile>, EngineError> {
    let arrow_schema = logical_schema_to_arrow(schema)
        .map_err(|_| type_error("export canonical Arrow schema derivation failed"))?;
    let fingerprint = LogicalSchemaFingerprint::try_from_schema(schema)
        .map_err(|_| type_error("export schema fingerprint derivation failed"))?
        .to_string();
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(MAX_BATCH_ROWS))
        .set_key_value_metadata(Some(vec![
            KeyValue::new("stillflow:schema_fingerprint".to_owned(), Some(fingerprint)),
            KeyValue::new(
                "stillflow:export_manifest_version".to_owned(),
                Some(stillflow_core::EXPORT_MANIFEST_VERSION.to_string()),
            ),
            KeyValue::new(
                "stillflow:export_format_contract_version".to_owned(),
                Some(stillflow_core::EXPORT_FORMAT_CONTRACT_VERSION.to_string()),
            ),
            KeyValue::new(
                "stillflow:export_encoder_version".to_owned(),
                Some(stillflow_core::EXPORT_ENCODER_VERSION.to_owned()),
            ),
            KeyValue::new(
                "stillflow:engine_contract_version".to_owned(),
                Some(ENGINE_CONTRACT_VERSION.to_string()),
            ),
        ]))
        .build();
    ArrowWriter::try_new(staged, arrow_schema, Some(properties))
        .map_err(|_| type_error("export Parquet writer initialization failed"))
}

/// Recovers the typed staged-file error from a Parquet sink failure so
/// staging-budget violations stay typed (ADR-004 §5), and maps every other
/// encoder failure onto the internal category.
fn map_parquet_error(error: ParquetError) -> EngineError {
    if let ParquetError::External(source) = error {
        if let Ok(io_error) = source.downcast::<std::io::Error>() {
            if let Some(inner) = io_error.into_inner() {
                if let Ok(storage) = inner.downcast::<StorageError>() {
                    return EngineError::from_storage(*storage);
                }
            }
        }
    }
    EngineError::Internal("export Parquet encoding failed")
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T, EngineError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| type_error("export column physical type drifted from the canonical schema"))
}

// ---------------------------------------------------------------------------
// CSV / TSV encoding (ADR-004 §3)
// ---------------------------------------------------------------------------

fn text_delimiter(format: ExportFormat) -> Result<u8, EngineError> {
    format
        .text_delimiter()
        .ok_or_else(|| type_error("text delimiter requested for a non-text export format"))
}

fn write_text_header(
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

fn write_text_batch(
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
// JSONL encoding (ADR-004 §3)
// ---------------------------------------------------------------------------

fn write_jsonl_batch(
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

// ---------------------------------------------------------------------------
// Shared value rendering
// ---------------------------------------------------------------------------

fn timestamp_value(unit: TimeUnit, array: &dyn Array, row: usize) -> Result<i64, EngineError> {
    let value = match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array)?.value(row),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.value(row),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.value(row),
        TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.value(row),
    };
    Ok(value)
}

/// Renders one Date32 (days since the Unix epoch) as `%Y-%m-%d` (ADR-004 §3).
fn format_date(days: i32) -> Result<String, EngineError> {
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
fn format_timestamp_value(unit: TimeUnit, value: i64) -> Result<String, EngineError> {
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
