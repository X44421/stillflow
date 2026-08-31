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
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{
        ArrayRef, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMillisecondArray,
    };
    use chrono::{DateTime, Utc};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::TempDir;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use stillflow_core::{
        logical_schema_to_arrow, BatchEnvelope, ColumnId, ExportDestination, ExportFormat,
        ExportPolicy, ExportShape, LogicalField, LogicalSchema, LogicalType, RequestContext,
        TimeUnit,
    };
    use stillflow_storage::{SnapshotDraft, SnapshotStore, StorageLimits};

    use crate::export::{run_export, ExportRequest};
    use crate::EngineError;

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("valid timestamp")
    }
    fn store(temp: &TempDir) -> SnapshotStore {
        SnapshotStore::open(temp.path(), StorageLimits::default()).expect("open store")
    }
    fn destination_root(temp: &TempDir) -> PathBuf {
        let root = temp.path().join("published");
        std::fs::create_dir_all(&root).expect("destination root");
        root
    }
    fn simple_schema() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(11)),
                "value",
                LogicalType::Int64,
                false,
            )
            .expect("valid field")])
            .expect("valid schema"),
        )
    }
    fn draft(snapshot_id: Uuid, source_asset_id: Uuid, schema: &LogicalSchema) -> SnapshotDraft {
        SnapshotDraft::try_new(
            snapshot_id,
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            source_asset_id,
            schema.clone(),
            BTreeSet::from([Uuid::from_u128(9)]),
            Some(97),
            at(1_700_000_000),
        )
        .expect("valid draft")
    }
    fn envelope(
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        values: Vec<i64>,
    ) -> BatchEnvelope {
        let arrow_schema = logical_schema_to_arrow(&schema).expect("Arrow schema");
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))])
            .expect("record batch");
        BatchEnvelope::try_new(schema, source_asset_id, sequence, batch).expect("envelope")
    }
    fn publish(
        temp: &TempDir,
        snapshot_id: Uuid,
        source_asset_id: Uuid,
        schema: Arc<LogicalSchema>,
        partitions: Vec<Vec<i64>>,
    ) -> stillflow_storage::SnapshotManifest {
        let s = store(temp);
        let mut writer = s
            .begin_snapshot(
                draft(snapshot_id, source_asset_id, &schema),
                at(1_700_000_001),
            )
            .expect("begin snapshot");
        for (seq, values) in partitions.into_iter().enumerate() {
            writer
                .append(&envelope(
                    Arc::clone(&schema),
                    source_asset_id,
                    seq as u64,
                    values,
                ))
                .expect("append");
        }
        writer.commit().expect("commit")
    }
    /// Publishes a snapshot of exactly `total_rows` rows as full
    /// `MAX_BATCH_ROWS` envelopes (one partition per envelope), which is the
    /// densest legal snapshot shape for boundary tests.
    fn publish_rows(
        temp: &TempDir,
        snapshot_id: Uuid,
        source_asset_id: Uuid,
        schema: Arc<LogicalSchema>,
        total_rows: u64,
    ) -> stillflow_storage::SnapshotManifest {
        let per_batch = stillflow_core::MAX_BATCH_ROWS as u64;
        let mut partitions = Vec::new();
        let mut remaining = total_rows;
        let mut value = 0_i64;
        while remaining > 0 {
            let take = usize::try_from(remaining.min(per_batch)).expect("rows fit usize");
            partitions.push(vec![value; take]);
            remaining -= take as u64;
            value += 1;
        }
        publish(temp, snapshot_id, source_asset_id, schema, partitions)
    }
    #[allow(clippy::too_many_arguments)]
    fn export_request(
        export_id: Uuid,
        snapshot_id: Uuid,
        root: &Path,
        relative: &[&str],
        format: ExportFormat,
        shape: ExportShape,
        context: RequestContext,
        created_at: DateTime<Utc>,
    ) -> ExportRequest {
        ExportRequest {
            export_id,
            snapshot_id,
            format,
            policy: ExportPolicy { shape },
            destination: ExportDestination::local(
                root,
                relative.iter().map(|s| (*s).to_owned()).collect(),
                format,
                shape,
            )
            .expect("local destination"),
            created_at,
            context,
        }
    }
    fn artifact_path(root: &Path, relative: &[&str]) -> PathBuf {
        let mut p = root.to_path_buf();
        for c in relative {
            p.push(c);
        }
        p
    }

    #[test]
    fn only_committed_visible_snapshot_is_accepted() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let s = store(&temp);
        let mut w = s
            .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
            .expect("begin");
        w.append(&envelope(Arc::clone(&schema), source, 0, vec![1, 2]))
            .expect("append");
        let manifest = w.commit().expect("commit");
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let nil_req = export_request(
            Uuid::nil(),
            snap,
            &root,
            &["reports", "nil.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&s, nil_req).expect_err("nil export id must fail");
        assert!(matches!(err, EngineError::Connector(_)));
        let missing_req = export_request(
            Uuid::from_u128(100),
            Uuid::from_u128(999),
            &root,
            &["reports", "missing.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&s, missing_req).expect_err("missing snapshot must fail");
        assert!(matches!(err, EngineError::Storage(_)));
        let draft_id = Uuid::from_u128(555);
        let _draft_writer = s
            .begin_snapshot(draft(draft_id, source, &schema), at(1_700_000_002))
            .expect("begin draft");
        let live_req = export_request(
            Uuid::from_u128(101),
            draft_id,
            &root,
            &["reports", "live.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&s, live_req).expect_err("live draft must fail");
        assert!(matches!(err, EngineError::Storage(_)));
        let ok_req = export_request(
            Uuid::from_u128(102),
            snap,
            &root,
            &["reports", "ok.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let result = run_export(&s, ok_req).expect("visible snapshot must succeed");
        assert_eq!(result.row_count(), 2);
        assert!(artifact_path(&root, &["reports", "ok.csv"]).exists());
        let _ = manifest;
    }

    #[test]
    fn input_verification_before_first_byte_fails_closed() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let manifest = publish(
            &temp,
            snap,
            source,
            Arc::clone(&schema),
            vec![vec![1, 2], vec![3]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let partitions_dir = temp.path().join("partitions").join(snap.to_string());
        let _deleted = (|| {
            let entries = std::fs::read_dir(&partitions_dir).ok()?;
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_file() {
                    let _ = std::fs::remove_file(&path);
                    return Some(true);
                }
            }
            None
        })()
        .unwrap_or(false);
        let req = export_request(
            Uuid::from_u128(200),
            snap,
            &root,
            &["reports", "corrupt.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&store(&temp), req).expect_err("corrupt input must fail");
        assert!(!artifact_path(&root, &["reports", "corrupt.csv"]).exists());
        assert!(matches!(
            err,
            EngineError::Storage(_) | EngineError::Internal(_) | EngineError::Connector(_)
        ));
        let _ = manifest;
    }

    fn csv_tsv_schema() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(101)),
                    "id",
                    LogicalType::Int64,
                    false,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(102)),
                    "name",
                    LogicalType::Utf8,
                    true,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(103)),
                    "note",
                    LogicalType::Utf8,
                    true,
                )
                .expect("field"),
            ])
            .expect("schema"),
        )
    }
    fn csv_tsv_envelope(
        schema: Arc<LogicalSchema>,
        source: Uuid,
        rows: Vec<(i64, Option<String>, Option<String>)>,
    ) -> BatchEnvelope {
        let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow schema");
        let mut ids = Vec::new();
        let mut names = Vec::new();
        let mut notes = Vec::new();
        for (id, name, note) in rows {
            ids.push(id);
            names.push(name);
            notes.push(note);
        }
        let id_arr = Arc::new(Int64Array::from(ids)) as ArrayRef;
        let name_arr = Arc::new(StringArray::from(names)) as ArrayRef;
        let note_arr = Arc::new(StringArray::from(notes)) as ArrayRef;
        let batch =
            RecordBatch::try_new(arrow_schema, vec![id_arr, name_arr, note_arr]).expect("batch");
        BatchEnvelope::try_new(schema, source, 0, batch).expect("envelope")
    }

    #[test]
    fn csv_golden_header_null_empty_quote_delimiter_lf_cr() {
        let temp = TempDir::new().expect("temp dir");
        let schema = csv_tsv_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let s = store(&temp);
        let mut writer = s
            .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
            .expect("begin");
        writer
            .append(&csv_tsv_envelope(
                Arc::clone(&schema),
                source,
                vec![
                    (1, Some("a".to_owned()), None),
                    (2, Some("".to_owned()), Some("".to_owned())),
                    (3, Some("a,b".to_owned()), Some("x".to_owned())),
                    (4, Some("a\"b".to_owned()), Some("y".to_owned())),
                    (5, Some("a\nb".to_owned()), Some("z".to_owned())),
                    (6, Some("a\rb".to_owned()), Some("w".to_owned())),
                ],
            ))
            .expect("append");
        let manifest = writer.commit().expect("commit");
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let req = export_request(
            Uuid::from_u128(300),
            snap,
            &root,
            &["reports", "golden.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let result = run_export(&s, req).expect("csv export");
        let bytes =
            std::fs::read(artifact_path(&root, &["reports", "golden.csv"])).expect("read csv");
        let text = String::from_utf8(bytes).expect("utf8");
        let expected = concat!(
            "id,name,note\n",
            "1,a,\n",
            "2,\"\",\"\"\n",
            "3,\"a,b\",x\n",
            "4,\"a\"\"b\",y\n",
            "5,\"a\nb\",z\n",
            "6,\"a\rb\",w\n"
        );
        assert_eq!(text, expected);
        assert_eq!(result.row_count(), 6);
        let _ = manifest;
    }

    #[test]
    fn tsv_golden_same_edges_with_tab_delimiter() {
        let temp = TempDir::new().expect("temp dir");
        let schema = csv_tsv_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let s = store(&temp);
        let mut writer = s
            .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
            .expect("begin");
        writer
            .append(&csv_tsv_envelope(
                Arc::clone(&schema),
                source,
                vec![
                    (1, Some("a".to_owned()), None),
                    (2, Some("".to_owned()), Some("".to_owned())),
                    (3, Some("a\tb".to_owned()), Some("x".to_owned())),
                    (4, Some("a\"b".to_owned()), Some("y".to_owned())),
                ],
            ))
            .expect("append");
        let manifest = writer.commit().expect("commit");
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let req = export_request(
            Uuid::from_u128(301),
            snap,
            &root,
            &["reports", "golden.tsv"],
            ExportFormat::Tsv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        run_export(&s, req).expect("tsv export");
        let bytes =
            std::fs::read(artifact_path(&root, &["reports", "golden.tsv"])).expect("read tsv");
        let text = String::from_utf8(bytes).expect("utf8");
        let expected = "id\tname\tnote\n1\ta\t\n2\t\"\"\t\"\"\n3\t\"a\tb\"\tx\n4\t\"a\"\"b\"\ty\n";
        assert_eq!(text, expected);
        let _ = manifest;
    }

    fn jsonl_schema() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(201)),
                    "id",
                    LogicalType::Int64,
                    false,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(202)),
                    "name",
                    LogicalType::Utf8,
                    true,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(203)),
                    "score",
                    LogicalType::Float64,
                    true,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(204)),
                    "ts",
                    LogicalType::Timestamp {
                        unit: TimeUnit::Millisecond,
                        timezone: None,
                    },
                    true,
                )
                .expect("field"),
            ])
            .expect("schema"),
        )
    }

    #[test]
    fn jsonl_golden_field_order_escaping_numeric_timestamp() {
        let temp = TempDir::new().expect("temp dir");
        let schema = jsonl_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow");
        let id_arr = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let name_arr = Arc::new(StringArray::from(vec![Some("a\"b\n"), Some("c")])) as ArrayRef;
        let score_arr = Arc::new(Float64Array::from(vec![Some(1.5), None])) as ArrayRef;
        let ts_arr = Arc::new(TimestampMillisecondArray::from(vec![
            Some(1609459200123),
            None,
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema, vec![id_arr, name_arr, score_arr, ts_arr])
            .expect("batch");
        let envelope =
            BatchEnvelope::try_new(Arc::clone(&schema), source, 0, batch).expect("envelope");
        let s = store(&temp);
        let mut writer = s
            .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
            .expect("begin");
        writer.append(&envelope).expect("append");
        let manifest = writer.commit().expect("commit");
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let req = export_request(
            Uuid::from_u128(302),
            snap,
            &root,
            &["reports", "golden.jsonl"],
            ExportFormat::Jsonl,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        run_export(&s, req).expect("jsonl export");
        let bytes =
            std::fs::read(artifact_path(&root, &["reports", "golden.jsonl"])).expect("read");
        let text = String::from_utf8(bytes).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(text.ends_with('\n'));
        let v1: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
        assert_eq!(v1["id"], 1);
        assert_eq!(v1["name"], "a\"b\n");
        assert_eq!(v1["score"], 1.5);
        assert_eq!(v1["ts"], "2021-01-01T00:00:00.123Z");
        let _ = manifest;
    }

    fn parquet_schema_simple() -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(301)),
                    "id",
                    LogicalType::Int64,
                    false,
                )
                .expect("field"),
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(302)),
                    "data",
                    LogicalType::Binary,
                    true,
                )
                .expect("field"),
            ])
            .expect("schema"),
        )
    }

    #[test]
    fn parquet_golden_canonical_schema_snappy_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let schema = parquet_schema_simple();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow");
        let id_arr = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let data_arr = Arc::new(BinaryArray::from(vec![Some(b"hello" as &[u8]), None])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema, vec![id_arr, data_arr]).expect("batch");
        let envelope =
            BatchEnvelope::try_new(Arc::clone(&schema), source, 0, batch).expect("envelope");
        let s = store(&temp);
        let mut writer = s
            .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
            .expect("begin");
        writer.append(&envelope).expect("append");
        let manifest = writer.commit().expect("commit");
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let req = export_request(
            Uuid::from_u128(303),
            snap,
            &root,
            &["reports", "golden.parquet"],
            ExportFormat::Parquet,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        run_export(&s, req).expect("parquet export");
        let path = artifact_path(&root, &["reports", "golden.parquet"]);
        assert!(path.exists());
        let file = std::fs::File::open(&path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("reader");
        let metadata = reader.metadata();
        let row_group = metadata.row_group(0);
        for col in row_group.columns() {
            assert_eq!(col.compression(), parquet::basic::Compression::SNAPPY);
        }
        let kvs = metadata.file_metadata().key_value_metadata().expect("kvs");
        let keys: Vec<_> = kvs.iter().map(|kv| kv.key.as_str()).collect();
        assert!(keys.contains(&"stillflow:schema_fingerprint"));
        assert!(keys.contains(&"stillflow:export_manifest_version"));
        assert!(keys.contains(&"stillflow:export_format_contract_version"));
        assert!(keys.contains(&"stillflow:export_encoder_version"));
        assert!(keys.contains(&"stillflow:engine_contract_version"));
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 2);
        let _ = manifest;
    }

    #[test]
    fn format_matrix_binary_non_finite() {
        let temp = TempDir::new().expect("temp dir");
        let binary_schema = Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(401)),
                "bin",
                LogicalType::Binary,
                false,
            )
            .expect("field")])
            .expect("schema"),
        );
        let float_schema = Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(403)),
                "f",
                LogicalType::Float64,
                false,
            )
            .expect("field")])
            .expect("schema"),
        );
        let source = Uuid::from_u128(4);
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let snap_bin = Uuid::from_u128(10);
        let arrow_schema = logical_schema_to_arrow(&binary_schema).expect("arrow");
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(BinaryArray::from(vec![b"hi" as &[u8]])) as ArrayRef],
        )
        .expect("batch");
        let env =
            BatchEnvelope::try_new(Arc::clone(&binary_schema), source, 0, batch).expect("env");
        let mut w = store(&temp)
            .begin_snapshot(draft(snap_bin, source, &binary_schema), at(1_700_000_001))
            .expect("begin");
        w.append(&env).expect("append");
        w.commit().expect("commit");
        let req = export_request(
            Uuid::from_u128(400),
            snap_bin,
            &root,
            &["reports", "bin.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&store(&temp), req).expect_err("binary csv must fail");
        assert!(matches!(err, EngineError::TypeError(_)));
        let req = export_request(
            Uuid::from_u128(401),
            snap_bin,
            &root,
            &["reports", "bin.parquet"],
            ExportFormat::Parquet,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        run_export(&store(&temp), req).expect("binary parquet must succeed");
        let snap_float = Uuid::from_u128(12);
        let arrow_schema = logical_schema_to_arrow(&float_schema).expect("arrow");
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef],
        )
        .expect("batch");
        let env = BatchEnvelope::try_new(Arc::clone(&float_schema), source, 0, batch).expect("env");
        let mut w = store(&temp)
            .begin_snapshot(draft(snap_float, source, &float_schema), at(1_700_000_003))
            .expect("begin");
        w.append(&env).expect("append");
        w.commit().expect("commit");
        for (fid, fmt) in [
            (404, ExportFormat::Csv),
            (405, ExportFormat::Jsonl),
            (406, ExportFormat::Parquet),
        ] {
            let ext = fmt.extension();
            let req = export_request(
                Uuid::from_u128(fid),
                snap_float,
                &root,
                &["reports", &format!("nan.{ext}")],
                fmt,
                ExportShape::SingleFile,
                RequestContext::new(),
                created_at,
            );
            let err = run_export(&store(&temp), req).expect_err("non-finite must fail");
            assert!(matches!(err, EngineError::TypeError(_)));
        }
    }

    #[test]
    fn deterministic_repeated_export_byte_equality() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let manifest = publish(
            &temp,
            snap,
            source,
            Arc::clone(&schema),
            vec![vec![3, 1], vec![2]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let s = store(&temp);
        let req1 = export_request(
            Uuid::from_u128(500),
            snap,
            &root,
            &["reports", "det.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let r1 = run_export(&s, req1).expect("first export");
        let bytes1 = std::fs::read(artifact_path(&root, &["reports", "det.csv"])).expect("read1");
        let req2 = export_request(
            Uuid::from_u128(501),
            snap,
            &root,
            &["reports", "det2.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let r2 = run_export(&s, req2).expect("second export");
        let bytes2 = std::fs::read(artifact_path(&root, &["reports", "det2.csv"])).expect("read2");
        assert_eq!(bytes1, bytes2);
        assert_eq!(r1.files()[0].digest(), r2.files()[0].digest());
        let text = String::from_utf8(bytes1).expect("utf8");
        assert_eq!(text, "value\n3\n1\n2\n");
        let _ = manifest;
    }

    #[test]
    fn single_file_and_partitioned_naming() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let manifest = publish(
            &temp,
            snap,
            source,
            Arc::clone(&schema),
            vec![vec![1], vec![2], vec![3]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let snapshot_store = store(&temp);
        let req = export_request(
            Uuid::from_u128(600),
            snap,
            &root,
            &["reports", "single.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let r = run_export(&snapshot_store, req).expect("single");
        assert_eq!(r.files()[0].name(), "single.csv");
        assert_eq!(r.files().len(), 1);
        assert!(artifact_path(&root, &["reports", "single.csv"]).exists());
        let temp2 = TempDir::new().expect("temp2");
        let schema2 = simple_schema();
        let snap2 = Uuid::from_u128(2);
        let _m2 = publish(
            &temp2,
            snap2,
            source,
            Arc::clone(&schema2),
            vec![vec![10], vec![20]],
        );
        let root2 = destination_root(&temp2);
        let snapshot_store2 = store(&temp2);
        let req = export_request(
            Uuid::from_u128(601),
            snap2,
            &root2,
            &["reports", "parts"],
            ExportFormat::Csv,
            ExportShape::PartitionedSet,
            RequestContext::new(),
            created_at,
        );
        let r = run_export(&snapshot_store2, req).expect("partitioned");
        assert_eq!(r.files().len(), 2);
        assert_eq!(r.files()[0].name(), "part-0000000000.csv");
        assert_eq!(r.files()[1].name(), "part-0000000001.csv");
        let _ = manifest;
    }

    #[test]
    fn cancellation_at_checkpoints_leaves_no_artifact() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let _m = publish(
            &temp,
            snap,
            source,
            Arc::clone(&schema),
            vec![vec![1, 2, 3]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let s = store(&temp);
        let token = CancellationToken::new();
        token.cancel();
        let ctx = RequestContext::with_cancellation(token);
        let req = export_request(
            Uuid::from_u128(800),
            snap,
            &root,
            &["reports", "cancel_before.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            ctx,
            created_at,
        );
        let err = run_export(&s, req).expect_err("cancelled before verification");
        assert!(!format!("{err:?}").is_empty());
        assert!(!artifact_path(&root, &["reports", "cancel_before.csv"]).exists());
        let ctx = RequestContext::with_deadline(Instant::now() - Duration::from_millis(1));
        let req = export_request(
            Uuid::from_u128(801),
            snap,
            &root,
            &["reports", "deadline_before.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            ctx,
            created_at,
        );
        let err = run_export(&s, req).expect_err("deadline before verification");
        assert!(!format!("{err:?}").is_empty());
    }

    #[test]
    fn deterministic_retry_after_cancellation_or_failure() {
        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let snap = Uuid::from_u128(1);
        let _m = publish(&temp, snap, source, Arc::clone(&schema), vec![vec![42]]);
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);
        let s = store(&temp);
        let token = CancellationToken::new();
        token.cancel();
        let ctx = RequestContext::with_cancellation(token);
        let req = export_request(
            Uuid::from_u128(900),
            snap,
            &root,
            &["reports", "retry.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            ctx,
            created_at,
        );
        assert!(run_export(&s, req).is_err());
        assert!(!artifact_path(&root, &["reports", "retry.csv"]).exists());
        let _ = s.recover(at(1_700_100_500), Duration::from_secs(60), 16);
        let req = export_request(
            Uuid::from_u128(900),
            snap,
            &root,
            &["reports", "retry.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let r1 = run_export(&s, req).expect("retry after cancel must succeed");
        let bytes1 = std::fs::read(artifact_path(&root, &["reports", "retry.csv"])).expect("read");
        let req = export_request(
            Uuid::from_u128(901),
            snap,
            &root,
            &["reports", "retry2.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let r2 = run_export(&s, req).expect("second retry");
        let bytes2 =
            std::fs::read(artifact_path(&root, &["reports", "retry2.csv"])).expect("read2");
        assert_eq!(bytes1, bytes2);
        assert_eq!(r1.files()[0].digest(), r2.files()[0].digest());
    }

    #[test]
    fn no_api_job_surface_introduced() {
        // Compile-time guard: the api crate must keep existing, and its root
        // must never reference an export/job surface symbol. Routes are not
        // implemented yet, so lib.rs is the whole api source today; extend
        // this guard when more api files appear.
        let api_lib = include_str!("../../stillflow-api/src/lib.rs");
        for surface in [
            "run_export",
            "ExportRequest",
            "ExportArtifact",
            "ExportFormat",
            "ExportPolicy",
        ] {
            assert!(
                !api_lib.contains(surface),
                "stillflow-api must not reference export surface symbol {surface}"
            );
        }
    }

    #[test]
    fn bounds_rows_partitions_deadline() {
        use stillflow_core::export::{MAX_EXPORT_PARTITIONS, MAX_EXPORT_ROWS};

        assert_eq!(MAX_EXPORT_ROWS, 10_000_000);
        assert_eq!(MAX_EXPORT_PARTITIONS, 1_024);
        assert_eq!(
            crate::ENGINE_MAX_DEADLINE,
            Duration::from_secs(30 * 60),
            "engine deadline cap is ADR-004 §8"
        );

        let temp = TempDir::new().expect("temp dir");
        let schema = simple_schema();
        let source = Uuid::from_u128(4);
        let created_at = at(1_700_100_000);
        let root = destination_root(&temp);

        // An already-expired deadline fails typed before any store access:
        // the snapshot id does not exist, so only the deadline can fire.
        let expired = RequestContext::with_deadline(Instant::now() - Duration::from_secs(1));
        let req = export_request(
            Uuid::from_u128(700),
            Uuid::from_u128(999),
            &root,
            &["reports", "deadline.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            expired,
            created_at,
        );
        match run_export(&store(&temp), req) {
            Err(EngineError::Timeout) => {}
            other => panic!("expected Timeout before any I/O, got {other:?}"),
        }

        // Row bound: 10_000_001 valid rows fail before the first output byte.
        let snap = Uuid::from_u128(701);
        publish_rows(
            &temp,
            snap,
            source,
            Arc::clone(&schema),
            MAX_EXPORT_ROWS + 1,
        );
        let req = export_request(
            Uuid::from_u128(702),
            snap,
            &root,
            &["reports", "rows.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        match run_export(&store(&temp), req) {
            Err(EngineError::BoundExceeded(message)) => {
                assert!(message.contains("row bound"), "unexpected bound: {message}");
            }
            other => panic!("expected row BoundExceeded, got {other:?}"),
        }
        assert!(!artifact_path(&root, &["reports", "rows.csv"]).exists());

        // The exact cap is accepted: 10_000_000 rows export cleanly.
        let snap = Uuid::from_u128(703);
        publish_rows(&temp, snap, source, Arc::clone(&schema), MAX_EXPORT_ROWS);
        let req = export_request(
            Uuid::from_u128(704),
            snap,
            &root,
            &["reports", "rows_exact.csv"],
            ExportFormat::Csv,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let result = run_export(&store(&temp), req).expect("exact row cap exports");
        assert_eq!(result.files().len(), 1);
        assert!(artifact_path(&root, &["reports", "rows_exact.csv"]).exists());

        // Partition bound: a 1_025-partition partitioned set fails in the
        // phase-1 verification battery, before encoding any byte.
        let snap = Uuid::from_u128(705);
        let partitions: Vec<Vec<i64>> = (0..=i64::from(MAX_EXPORT_PARTITIONS))
            .map(|i| vec![i])
            .collect();
        publish(&temp, snap, source, Arc::clone(&schema), partitions);
        let req = export_request(
            Uuid::from_u128(706),
            snap,
            &root,
            &["reports", "parts_over"],
            ExportFormat::Csv,
            ExportShape::PartitionedSet,
            RequestContext::new(),
            created_at,
        );
        match run_export(&store(&temp), req) {
            Err(EngineError::BoundExceeded(message)) => {
                assert!(message.contains("partition"), "unexpected bound: {message}");
            }
            other => panic!("expected partition BoundExceeded, got {other:?}"),
        }
        assert!(!artifact_path(&root, &["reports", "parts_over"]).exists());
    }
}
