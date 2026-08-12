use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow_select::take::take;
use stillflow_core::{
    BatchEnvelopeFactory, ConnectorError, ConnectorResult, ErrorCategory, PreviewData,
    PreviewRequest,
};

use crate::access::StoreAccess;
use crate::parquet::prepare_parquet;

const PREVIEW_BATCH_ROWS: usize = 1_024;

pub(crate) async fn preview_parquet(
    access: &StoreAccess,
    request: PreviewRequest,
) -> ConnectorResult<PreviewData> {
    let lookahead = request.row_limit.checked_add(1).ok_or_else(|| {
        ConnectorError::invalid_configuration("preview row limit exceeds the supported range")
    })?;
    let mut reader = prepare_parquet(
        access,
        &request.asset,
        request.schema_override.as_ref(),
        request.projection.as_deref(),
        PREVIEW_BATCH_ROWS,
        Some(lookahead),
        &request.context,
    )
    .await?;
    let schema = reader.output_schema();
    let factory = BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), request.asset.id)
        .map_err(|_| preview_error("preview schema cannot establish the batch boundary"))?;
    let mut batches = Vec::new();
    let mut source_rows = 0_usize;
    let mut returned_rows = 0_usize;
    let mut returned_bytes = 0_usize;
    let mut bytes_truncated = false;
    let mut byte_closed = false;
    let mut sequence = 0_u64;

    while let Some(batch) = reader.next_record_batch().await? {
        request.context.ensure_active()?;
        let rows_before = source_rows;
        source_rows = source_rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| preview_error("preview source row count overflow"))?;
        let rows_within_limit = request
            .row_limit
            .saturating_sub(rows_before)
            .min(batch.num_rows());
        if byte_closed || rows_within_limit == 0 {
            continue;
        }
        let candidate = compact_range(&batch, 0, rows_within_limit)?;
        let remaining = request.byte_limit.saturating_sub(returned_bytes);
        let fit = largest_fitting_prefix(&candidate, remaining)?;
        if fit < rows_within_limit {
            let single = compact_range(&candidate, fit, 1)?;
            if single.get_array_memory_size() > request.byte_limit {
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    "one decoded preview row exceeds the byte limit",
                ));
            }
            bytes_truncated = true;
            byte_closed = true;
        }
        if fit == 0 {
            continue;
        }
        let payload = if fit == candidate.num_rows() {
            candidate
        } else {
            compact_range(&candidate, 0, fit)?
        };
        let envelope = factory.try_build(sequence, payload).map_err(|_| {
            preview_error("bounded Parquet preview violated an envelope invariant")
        })?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| preview_error("preview batch sequence overflow"))?;
        returned_rows = returned_rows
            .checked_add(envelope.row_count())
            .ok_or_else(|| preview_error("preview row count overflow"))?;
        returned_bytes = returned_bytes
            .checked_add(envelope.byte_count())
            .ok_or_else(|| preview_error("preview byte count overflow"))?;
        batches.push(envelope);
    }
    Ok(PreviewData {
        schema,
        batches,
        rows_returned: returned_rows,
        rows_truncated: source_rows > request.row_limit,
        bytes_returned: returned_bytes,
        bytes_truncated,
        warnings: Vec::new(),
    })
}

fn largest_fitting_prefix(batch: &RecordBatch, byte_limit: usize) -> ConnectorResult<usize> {
    if batch.get_array_memory_size() <= byte_limit {
        return Ok(batch.num_rows());
    }
    let mut low = 0_usize;
    let mut high = batch.num_rows();
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        let candidate = compact_range(batch, 0, middle)?;
        if candidate.get_array_memory_size() <= byte_limit {
            low = middle;
        } else {
            high = middle.saturating_sub(1);
        }
    }
    Ok(low)
}

fn compact_range(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> ConnectorResult<RecordBatch> {
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= batch.num_rows())
        .ok_or_else(|| preview_error("preview compaction range is invalid"))?;
    let indices = (offset..end)
        .map(|index| {
            u32::try_from(index).map_err(|_| preview_error("preview row index exceeds u32"))
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    let indices = UInt32Array::from(indices);
    let arrays = batch
        .columns()
        .iter()
        .map(|array| {
            take(array.as_ref(), &indices, None).map_err(|_| {
                preview_error("preview arrays could not be compacted to the byte bound")
            })
        })
        .collect::<ConnectorResult<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(length));
    RecordBatch::try_new_with_options(batch.schema(), arrays, &options)
        .map_err(|_| preview_error("compacted preview batch has an invalid schema"))
}

fn preview_error(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::Internal, message)
}

fn source_error(category: ErrorCategory, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, false, message, Vec::new(), BTreeMap::new())
}
