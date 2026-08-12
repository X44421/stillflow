use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow_select::take::take;
use stillflow_core::{
    BatchEnvelopeFactory, ConnectorError, ConnectorResult, ErrorCategory, PreviewData,
    PreviewRequest,
};

use crate::config::WorkbookConfig;
use crate::path::RootSet;
use crate::read::{prepare_reader, PrepareOptions};

const PREVIEW_BATCH_ROWS: usize = 1_024;

pub(crate) async fn preview_asset(
    config: &WorkbookConfig,
    roots: &RootSet,
    request: PreviewRequest,
) -> ConnectorResult<PreviewData> {
    let lookahead_rows = request.row_limit.checked_add(1).ok_or_else(|| {
        ConnectorError::invalid_configuration("preview row limit exceeds the supported range")
    })?;
    let mut reader = prepare_reader(
        config,
        roots,
        &request.asset,
        PrepareOptions {
            schema_override: request.schema_override.as_ref(),
            projection_ids: request.projection.as_deref(),
            batch_size: PREVIEW_BATCH_ROWS,
            max_rows: Some(lookahead_rows),
            context: &request.context,
        },
    )?;
    let schema = reader.output_schema();
    let envelope_factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), request.asset.id).map_err(
            |_| preview_error("preview schema cannot establish the public batch boundary"),
        )?;
    let warnings = std::mem::take(&mut reader.warnings);
    let mut batches = Vec::new();
    let mut source_rows = 0_usize;
    let mut returned_rows = 0_usize;
    let mut returned_bytes = 0_usize;
    let mut bytes_truncated = false;
    let mut byte_closed = false;
    let mut output_sequence = 0_u64;

    while let Some(envelope) = reader.next_envelope()? {
        request.context.ensure_active()?;
        let rows_before = source_rows;
        source_rows = source_rows
            .checked_add(envelope.row_count())
            .ok_or_else(|| preview_error("preview source row count overflow"))?;
        let rows_within_limit = request
            .row_limit
            .saturating_sub(rows_before)
            .min(envelope.row_count());
        if byte_closed || rows_within_limit == 0 {
            continue;
        }

        let candidate = compact_range(envelope.payload(), 0, rows_within_limit)?;
        let remaining_bytes = request.byte_limit.saturating_sub(returned_bytes);
        let fit = largest_fitting_prefix(&candidate, remaining_bytes)?;
        if fit < rows_within_limit {
            let single = compact_range(&candidate, fit, 1)?;
            if single.get_array_memory_size() > request.byte_limit {
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    "one decoded workbook preview row exceeds the byte limit",
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
        let output = envelope_factory
            .try_build(output_sequence, payload)
            .map_err(|_| preview_error("bounded preview batch violated an envelope invariant"))?;
        output_sequence = output_sequence
            .checked_add(1)
            .ok_or_else(|| preview_error("preview batch sequence overflow"))?;
        returned_rows = returned_rows
            .checked_add(output.row_count())
            .ok_or_else(|| preview_error("preview row count overflow"))?;
        returned_bytes = returned_bytes
            .checked_add(output.byte_count())
            .ok_or_else(|| preview_error("preview byte count overflow"))?;
        batches.push(output);
    }

    request.context.ensure_active()?;
    Ok(PreviewData {
        schema,
        batches,
        rows_returned: returned_rows,
        rows_truncated: source_rows > request.row_limit,
        bytes_returned: returned_bytes,
        bytes_truncated,
        warnings,
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
            high = middle - 1;
        }
    }
    Ok(low)
}

fn compact_range(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> ConnectorResult<RecordBatch> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > batch.num_rows())
    {
        return Err(preview_error("preview compaction range is invalid"));
    }
    let indices = (offset..offset + length)
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

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;

    use super::*;

    #[test]
    fn finds_the_exact_compacted_prefix_for_a_byte_limit() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "a",
            "medium value",
            "a substantially longer third value",
        ]));
        let batch = RecordBatch::try_from_iter(vec![("value", values)]).expect("record batch");
        let two = compact_range(&batch, 0, 2).expect("two rows");
        let three = compact_range(&batch, 0, 3).expect("three rows");
        assert!(three.get_array_memory_size() > two.get_array_memory_size());
        assert_eq!(
            largest_fitting_prefix(&batch, two.get_array_memory_size()).expect("prefix"),
            2
        );
    }
}
