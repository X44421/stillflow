//! Deterministic streaming ExportArtifact runtime (ADR-004 §§2–8).
//!
//! The runtime validates committed Snapshot input, delegates format encoding
//! to focused modules, and publishes through the storage-owned ExportWriter.
//! This module owns orchestration, bounds, checkpoints, and publication only.

mod jsonl;
mod parquet;
mod text;
mod value;

#[cfg(test)]
mod tests;

use std::time::Duration;

use chrono::{DateTime, Utc};
use stillflow_core::{
    ExportDestination, ExportError, ExportFormat, ExportInputIdentity, ExportPolicy, ExportResult,
    ExportResultFile, ExportShape, LogicalSchema, RequestContext, EXPORT_DEFAULT_DEADLINE_SECONDS,
    MAX_EXPORT_PARTITIONS, MAX_EXPORT_ROWS,
};
use stillflow_storage::{
    ExportPlan, ExportProvenance, ExportWriter, SnapshotStore, StagedExportFile,
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
    run_export_inner(store, request, None)
}

/// Runs one export while binding the committed manifest to the authoritative
/// Job Run that will publish its terminal `ExportArtifactRef`.
pub fn run_export_with_run(
    store: &SnapshotStore,
    request: ExportRequest,
    run_id: Uuid,
) -> Result<ExportResult, EngineError> {
    if run_id.is_nil() {
        return Err(EngineError::InvalidPlan(
            "export Run identity must not be nil",
        ));
    }
    run_export_inner(store, request, Some(run_id))
}

fn run_export_inner(
    store: &SnapshotStore,
    request: ExportRequest,
    run_id: Option<Uuid>,
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
    let plan = ExportPlan::try_new_with_run(
        request.export_id,
        run_id,
        input,
        request.destination.clone(),
        request.format,
        request.policy,
    )
    .map_err(EngineError::from_storage)?;

    value::check_format_columns(request.format, snapshot.schema().fields.as_slice())?;

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
                let mut writer = parquet::open_parquet_writer(&mut staged, schema)?;
                let reader = store
                    .read_batches(request.snapshot_id)
                    .map_err(EngineError::from_storage)?;
                for batch in reader {
                    let envelope = batch.map_err(EngineError::from_storage)?;
                    value::ensure_batch_finite(fields, &envelope)?;
                    total_rows = add_rows(total_rows, envelope.row_count())?;
                    if total_rows > MAX_EXPORT_ROWS {
                        return Err(EngineError::BoundExceeded("export row bound exceeded"));
                    }
                    writer
                        .write(envelope.payload())
                        .map_err(parquet::map_parquet_error)?;
                    // Checkpoint: after each output append (ADR-004 §8).
                    checkpoint(context)?;
                }
                writer.into_inner().map_err(parquet::map_parquet_error)?;
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
                value::ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                let mut staged = open_partition(export_writer, &mut next_part)?;
                {
                    let mut writer = parquet::open_parquet_writer(&mut staged, schema)?;
                    writer
                        .write(envelope.payload())
                        .map_err(parquet::map_parquet_error)?;
                    writer.into_inner().map_err(parquet::map_parquet_error)?;
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
                text::write_text_header(&mut staged, fields, format)?;
            }
            let reader = store
                .read_batches(request.snapshot_id)
                .map_err(EngineError::from_storage)?;
            for batch in reader {
                let envelope = batch.map_err(EngineError::from_storage)?;
                value::ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                match format {
                    ExportFormat::Csv | ExportFormat::Tsv => {
                        text::write_text_batch(&mut staged, fields, &envelope, format)?;
                    }
                    ExportFormat::Jsonl => {
                        jsonl::write_jsonl_batch(&mut staged, fields, &envelope)?;
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
                value::ensure_batch_finite(fields, &envelope)?;
                total_rows = add_rows(total_rows, envelope.row_count())?;
                if total_rows > MAX_EXPORT_ROWS {
                    return Err(EngineError::BoundExceeded("export row bound exceeded"));
                }
                let mut staged = open_partition(export_writer, &mut next_part)?;
                match format {
                    ExportFormat::Csv | ExportFormat::Tsv => {
                        text::write_text_header(&mut staged, fields, format)?;
                        text::write_text_batch(&mut staged, fields, &envelope, format)?;
                    }
                    ExportFormat::Jsonl => {
                        jsonl::write_jsonl_batch(&mut staged, fields, &envelope)?;
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
