use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;

use futures::StreamExt;
use stillflow_core::{
    BatchEnvelope, LogicalSchema, LogicalSchemaFingerprint, ReadRequest, RequestContext,
    MAX_BATCH_BYTES,
};
use tokio::time::Instant;

use crate::engine::ExecutionEngine;
use crate::error::{map_context_error, EngineError};
use crate::ffi::{dataframe_to_record_batch, record_batch_to_dataframe};
use crate::memory::{MemoryReport, MemoryTracker};
use crate::predict::{largest_feasible_k, PredictedSchema};
use crate::preflight::{self, PreparedPlan};
use crate::remainder::CanonicalRebatcher;
use crate::{
    PreviewRequest, PreviewResult, PREVIEW_DEFAULT_DEADLINE, PREVIEW_MAX_BYTE_LIMIT,
    PREVIEW_MAX_DEADLINE, PREVIEW_MAX_ROW_LIMIT, PREVIEW_MAX_SOURCE_BYTES_OBSERVED,
    PREVIEW_MAX_SOURCE_BYTES_SCANNED, PREVIEW_MAX_SOURCE_ROWS_OBSERVED,
    PREVIEW_MAX_SOURCE_ROWS_SCANNED,
};

pub(crate) async fn preview(
    engine: &ExecutionEngine,
    request: PreviewRequest,
) -> Result<(PreviewResult, MemoryReport), EngineError> {
    let mut context = request.context.clone();
    if context.deadline().is_none() {
        context = RequestContext::with_cancellation_and_deadline(
            context.cancellation().clone(),
            Instant::now() + PREVIEW_DEFAULT_DEADLINE,
        );
    }
    context.ensure_active().map_err(map_context_error)?;
    if request.batch_size < ReadRequest::MIN_BATCH_SIZE
        || request.batch_size > ReadRequest::MAX_BATCH_SIZE
    {
        return Err(EngineError::BoundExceeded(
            "batch_size is outside 1..=65536",
        ));
    }
    if request.row_limit == 0 {
        return Err(EngineError::InvalidPlan(
            "preview row_limit must be greater than zero",
        ));
    }
    if request.row_limit > PREVIEW_MAX_ROW_LIMIT {
        return Err(EngineError::BoundExceeded(
            "preview row_limit exceeds PREVIEW_MAX_ROW_LIMIT",
        ));
    }
    if request.byte_limit == 0 {
        return Err(EngineError::InvalidPlan(
            "preview byte_limit must be greater than zero",
        ));
    }
    if request.byte_limit > PREVIEW_MAX_BYTE_LIMIT {
        return Err(EngineError::BoundExceeded(
            "preview byte_limit exceeds PREVIEW_MAX_BYTE_LIMIT",
        ));
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining > PREVIEW_MAX_DEADLINE)
    {
        return Err(EngineError::BoundExceeded(
            "preview deadline exceeds PREVIEW_MAX_DEADLINE",
        ));
    }

    let permit = Arc::clone(&engine.run_gate)
        .try_acquire_owned()
        .map_err(|_| EngineError::Busy)?;
    let _permit = permit;

    let prepared = preflight::preflight(
        &engine.registry,
        &request.plan,
        &request.connection,
        &request.asset,
        request.schema_override.as_ref(),
        &context,
        Some(request.target_node_id),
    )
    .await?;
    let plan_fingerprint = request
        .plan
        .fingerprint()
        .map_err(|_| EngineError::InvalidPlan("logical plan fingerprint failed"))?;

    context.ensure_active().map_err(map_context_error)?;
    let read = ReadRequest {
        context: context.clone(),
        asset: request.asset.clone(),
        schema_override: Some(prepared.expected_connector.clone()),
        projection: prepared
            .push_projection
            .then(|| prepared.scan_projection.clone()),
        filter: None,
        checkpoint: None,
        batch_size: request.batch_size,
    };
    let mut stream = engine
        .registry
        .read_batches(&request.connection, read)
        .await
        .map_err(EngineError::from_connector)?;

    let target_arrow = stillflow_core::logical_schema_to_arrow(&prepared.target_schema)
        .map_err(|_| EngineError::Internal("preview target arrow schema failed"))?;
    let expected_fingerprint =
        LogicalSchemaFingerprint::try_from_schema(&prepared.expected_connector)
            .map_err(|_| EngineError::Internal("connector schema fingerprint failed"))?;
    let predicted = PredictedSchema::from_scan_output(&prepared.scan_output);
    let mut tracker = MemoryTracker::new_preview();
    let mut accumulator = PreviewAccumulator::new(
        Arc::new(prepared.target_schema.clone()),
        request.asset.id,
        request.batch_size,
        request.row_limit,
        request.byte_limit,
        &mut tracker,
    )?;

    let mut source_rows_scanned = 0_usize;
    let mut source_bytes_scanned = 0_usize;
    let mut source_rows_observed = 0_usize;
    let mut source_bytes_observed = 0_usize;
    let mut rows_truncated = false;
    let mut bytes_truncated = false;
    let mut scan_truncated = false;
    let mut source_exhausted = false;
    let mut visible_prefix_closed = false;
    let mut target_rows_seen = 0_usize;

    loop {
        context.ensure_active().map_err(map_context_error)?;
        let Some(item) = stream.next().await else {
            source_exhausted = true;
            break;
        };
        let envelope = item.map_err(EngineError::from_connector)?;
        source_rows_observed = source_rows_observed.saturating_add(envelope.row_count());
        source_bytes_observed = source_bytes_observed.saturating_add(envelope.byte_count());
        if source_rows_observed > PREVIEW_MAX_SOURCE_ROWS_OBSERVED
            || source_bytes_observed > PREVIEW_MAX_SOURCE_BYTES_OBSERVED
        {
            return Err(EngineError::BoundExceeded(
                "preview source observed counters exceeded their ceilings",
            ));
        }

        if source_rows_scanned >= PREVIEW_MAX_SOURCE_ROWS_SCANNED {
            scan_truncated = true;
            break;
        }
        if source_bytes_scanned.saturating_add(envelope.byte_count())
            > PREVIEW_MAX_SOURCE_BYTES_SCANNED
        {
            scan_truncated = true;
            break;
        }
        if envelope.schema() != &prepared.expected_connector
            || envelope.schema_fingerprint() != expected_fingerprint
        {
            return Err(EngineError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }

        let consume_rows = envelope
            .row_count()
            .min(PREVIEW_MAX_SOURCE_ROWS_SCANNED - source_rows_scanned);
        let mid_envelope_scan_close = consume_rows < envelope.row_count();
        source_bytes_scanned = source_bytes_scanned.saturating_add(envelope.byte_count());
        tracker.hold_envelope(envelope.byte_count())?;

        let consumed = if consume_rows == envelope.row_count() {
            envelope.clone()
        } else {
            BatchEnvelope::try_from_parts(
                envelope.version(),
                envelope.shared_schema().clone(),
                envelope.source_asset_id(),
                envelope.sequence(),
                envelope.payload().slice(0, consume_rows),
            )
            .map_err(|_| EngineError::Internal("preview scan prefix envelope failed"))?
        };

        let mut offset = 0_usize;
        let rows = consumed.payload().num_rows();
        while offset < rows {
            context.ensure_active().map_err(map_context_error)?;
            let k = largest_feasible_k(
                rows,
                offset,
                consumed.payload().columns(),
                &predicted,
                &prepared.target_steps,
            )?;
            let (batch, consumed_rows) = lower_chunk(
                consumed.payload().slice(offset, k),
                &prepared,
                &target_arrow,
                &mut tracker,
            )?;
            source_rows_scanned = source_rows_scanned.saturating_add(consumed_rows);
            offset = offset.saturating_add(consumed_rows);
            target_rows_seen = target_rows_seen.saturating_add(batch.num_rows());
            tracker.record_chunk(consumed_rows, false);

            if visible_prefix_closed {
                if target_rows_seen > request.row_limit {
                    rows_truncated = true;
                    break;
                }
                continue;
            }

            #[cfg(test)]
            let batch_rows = batch.num_rows();
            #[cfg(test)]
            let rows_before_push = accumulator.rows();
            let push_outcome = accumulator.push(batch, &mut tracker)?;
            #[cfg(test)]
            tracker.record_chunk_output(
                consumed_rows,
                batch_rows,
                accumulator.rows().saturating_sub(rows_before_push),
            );
            match push_outcome {
                PushOutcome::Appended => {}
                PushOutcome::RowClosed => {
                    visible_prefix_closed = true;
                }
                PushOutcome::ByteClosed => {
                    bytes_truncated = true;
                    visible_prefix_closed = true;
                }
            }
            if target_rows_seen > request.row_limit {
                rows_truncated = true;
                visible_prefix_closed = true;
                break;
            }
        }
        tracker.drop_envelope()?;

        if mid_envelope_scan_close {
            scan_truncated = true;
            break;
        }
        if source_rows_scanned >= PREVIEW_MAX_SOURCE_ROWS_SCANNED
            || source_bytes_scanned >= PREVIEW_MAX_SOURCE_BYTES_SCANNED
        {
            context.ensure_active().map_err(map_context_error)?;
            match stream.next().await {
                Some(Ok(next)) => {
                    source_rows_observed = source_rows_observed.saturating_add(next.row_count());
                    source_bytes_observed = source_bytes_observed.saturating_add(next.byte_count());
                    if source_rows_observed > PREVIEW_MAX_SOURCE_ROWS_OBSERVED
                        || source_bytes_observed > PREVIEW_MAX_SOURCE_BYTES_OBSERVED
                    {
                        return Err(EngineError::BoundExceeded(
                            "preview source observed counters exceeded their ceilings",
                        ));
                    }
                    scan_truncated = true;
                }
                Some(Err(error)) => return Err(EngineError::from_connector(error)),
                None => source_exhausted = true,
            }
            break;
        }
        if rows_truncated || scan_truncated || source_exhausted {
            break;
        }
    }

    context.ensure_active().map_err(map_context_error)?;
    let response_batches = accumulator.finish(&mut tracker)?;
    context.ensure_active().map_err(map_context_error)?;

    let mut rows_returned = 0_usize;
    let mut bytes_returned = 0_usize;
    let mut expected_sequence = 0_u64;
    for batch in &response_batches {
        if batch.sequence() != expected_sequence {
            return Err(EngineError::Internal(
                "preview response envelope sequence is not contiguous",
            ));
        }
        expected_sequence = expected_sequence.saturating_add(1);
        if batch.schema() != &prepared.target_schema || batch.source_asset_id() != request.asset.id
        {
            return Err(EngineError::Internal(
                "preview response envelope schema or lineage invariant failed",
            ));
        }
        rows_returned = rows_returned.saturating_add(batch.row_count());
        bytes_returned = bytes_returned.saturating_add(batch.byte_count());
    }
    if rows_returned > request.row_limit || bytes_returned > request.byte_limit {
        return Err(EngineError::Internal(
            "preview response exceeded a frozen output cap",
        ));
    }
    if source_rows_scanned > PREVIEW_MAX_SOURCE_ROWS_SCANNED
        || source_bytes_scanned > PREVIEW_MAX_SOURCE_BYTES_SCANNED
        || source_rows_observed > PREVIEW_MAX_SOURCE_ROWS_OBSERVED
        || source_bytes_observed > PREVIEW_MAX_SOURCE_BYTES_OBSERVED
    {
        return Err(EngineError::Internal(
            "preview source counters exceeded a frozen scan cap",
        ));
    }
    if source_exhausted && (rows_truncated || scan_truncated) {
        return Err(EngineError::Internal(
            "source_exhausted contradicts truncation flags",
        ));
    }

    let report = tracker.report();
    Ok((
        PreviewResult {
            plan_fingerprint,
            target_node_id: request.target_node_id,
            schema: prepared.target_schema.clone(),
            rows_returned,
            bytes_returned,
            source_rows_scanned,
            source_bytes_scanned,
            source_rows_observed,
            source_bytes_observed,
            rows_truncated,
            bytes_truncated,
            scan_truncated,
            source_exhausted,
            batches: response_batches,
        },
        report,
    ))
}

#[cfg(test)]
thread_local! {
    static FORCE_EXPORT_RETRIES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn set_forced_export_retries(value: usize) {
    FORCE_EXPORT_RETRIES.with(|cell| cell.set(value));
}

#[cfg(test)]
fn take_forced_export_retry() -> bool {
    FORCE_EXPORT_RETRIES.with(|cell| {
        let value = cell.get();
        if value == 0 {
            false
        } else {
            cell.set(value - 1);
            true
        }
    })
}

fn lower_chunk(
    slice: arrow_array::RecordBatch,
    prepared: &PreparedPlan,
    target_arrow: &arrow_schema::SchemaRef,
    tracker: &mut MemoryTracker,
) -> Result<(arrow_array::RecordBatch, usize), EngineError> {
    let mut n = slice.num_rows();
    loop {
        let attempt = if n == slice.num_rows() {
            slice.clone()
        } else {
            slice.slice(0, n)
        };
        let frame = record_batch_to_dataframe(&attempt)?;
        let frame_bytes = frame.estimated_size();
        tracker.hold_polars(frame_bytes)?;
        let (transformed, deferred) =
            crate::lower::transform(frame, &prepared.scan_output, &prepared.target_steps)?;
        let transformed_bytes = transformed.estimated_size();
        tracker.hold_polars(transformed_bytes)?;
        let batch = dataframe_to_record_batch(
            transformed,
            &prepared.target_schema,
            target_arrow,
            &deferred,
        )?;
        tracker.drop_polars()?;
        let exact = batch.get_array_memory_size();
        #[cfg(test)]
        let force_retry = take_forced_export_retry();
        #[cfg(not(test))]
        let force_retry = false;
        if exact <= MAX_BATCH_BYTES && !force_retry {
            tracker.hold_incoming(exact)?;
            return Ok((batch, n));
        }
        drop(batch);
        if n == 1 {
            return Err(EngineError::BoundExceeded(
                "single-row preview export transition exceeds MAX_BATCH_BYTES",
            ));
        }
        n = (n / 2).max(1);
    }
}

struct PreviewAccumulator {
    rebatcher: CanonicalRebatcher,
    batches: Vec<BatchEnvelope>,
    finalized_rows: usize,
    finalized_bytes: usize,
    row_limit: usize,
    byte_limit: usize,
    pack_limit: usize,
}

enum PushOutcome {
    Appended,
    RowClosed,
    ByteClosed,
}

impl PreviewAccumulator {
    fn new(
        schema: Arc<LogicalSchema>,
        source_asset_id: uuid::Uuid,
        pack_limit: usize,
        row_limit: usize,
        byte_limit: usize,
        tracker: &mut MemoryTracker,
    ) -> Result<Self, EngineError> {
        let rebatcher = CanonicalRebatcher::new(schema, source_asset_id, pack_limit)?;
        tracker.hold_remainder(rebatcher.remainder_bytes())?;
        Ok(Self {
            rebatcher,
            batches: Vec::new(),
            finalized_rows: 0,
            finalized_bytes: 0,
            row_limit,
            byte_limit,
            pack_limit,
        })
    }

    fn absorb(
        &mut self,
        published: Vec<BatchEnvelope>,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        for envelope in published {
            self.finalized_rows = self.finalized_rows.saturating_add(envelope.row_count());
            self.finalized_bytes = self.finalized_bytes.saturating_add(envelope.byte_count());
            #[cfg(test)]
            tracker.record_finalized_response_envelope(envelope.byte_count());
            self.batches.push(envelope);
        }
        tracker.hold_remainder(
            self.finalized_bytes
                .saturating_add(self.rebatcher.remainder_bytes()),
        )
    }

    fn flush_builder(&mut self, tracker: &mut MemoryTracker) -> Result<(), EngineError> {
        let mut published = Vec::new();
        self.rebatcher
            .flush_to(self.finalized_bytes, &mut published, tracker)?;
        self.absorb(published, tracker)
    }

    #[cfg(test)]
    fn rows(&self) -> usize {
        self.finalized_rows.saturating_add(self.rebatcher.rows())
    }

    fn push(
        &mut self,
        mut incoming: arrow_array::RecordBatch,
        tracker: &mut MemoryTracker,
    ) -> Result<PushOutcome, EngineError> {
        while incoming.num_rows() > 0 {
            let incoming_rows_before = incoming.num_rows();
            let row_room = self
                .row_limit
                .saturating_sub(self.finalized_rows)
                .saturating_sub(self.rebatcher.rows());
            if row_room == 0 {
                self.flush_builder(tracker)?;
                return Ok(PushOutcome::RowClosed);
            }
            let pack_room = self.pack_limit.saturating_sub(self.rebatcher.rows());
            if pack_room == 0 {
                self.flush_builder(tracker)?;
                continue;
            }
            let high = incoming.num_rows().min(row_room).min(pack_room);
            let mut low = 0_usize;
            let mut high = high;
            while low < high {
                let mid = low + (high - low).div_ceil(2);
                let exact = self.rebatcher.exact_bytes_after_append(&incoming, mid)?;
                let builder_budget = self.byte_limit.saturating_sub(self.finalized_bytes);
                if exact <= MAX_BATCH_BYTES
                    && self.finalized_bytes.saturating_add(exact) <= self.byte_limit
                    && self
                        .rebatcher
                        .can_reserve_for_budget(&incoming, mid, builder_budget)?
                {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            let k = low;
            if k == 0 {
                let one = self.rebatcher.exact_bytes_after_append(&incoming, 1)?;
                if one > MAX_BATCH_BYTES {
                    if self.rebatcher.rows() > 0 {
                        self.flush_builder(tracker)?;
                        continue;
                    }
                    return Err(EngineError::BoundExceeded(
                        "a single transformed row exceeds MAX_BATCH_BYTES",
                    ));
                }
                if self.finalized_bytes == 0 && self.rebatcher.rows() == 0 {
                    return Err(EngineError::BoundExceeded(
                        "the first preview row exceeds the response byte limit",
                    ));
                }
                self.flush_builder(tracker)?;
                return Ok(PushOutcome::ByteClosed);
            }

            let accepted = incoming.slice(0, k);
            let remaining = incoming.slice(k, incoming.num_rows() - k);
            let mut published = Vec::new();
            self.rebatcher.push_with_base(
                accepted,
                self.finalized_bytes,
                tracker,
                |envelope, _tracker| {
                    published.push(envelope);
                    Ok(())
                },
            )?;
            self.absorb(published, tracker)?;
            incoming = remaining;

            if k < incoming_rows_before {
                if k == pack_room {
                    // The single-envelope pack limit closed this prefix; the
                    // canonical builder was flushed by `push` and the next
                    // loop iteration must continue with the remaining rows.
                    continue;
                }
                self.flush_builder(tracker)?;
                if k >= row_room {
                    return Ok(PushOutcome::RowClosed);
                }
                let next_exact = self.rebatcher.exact_bytes_after_append(&incoming, 1)?;
                if next_exact > MAX_BATCH_BYTES {
                    // The current canonical envelope reached its single-
                    // envelope bound. The next row may still fit in a fresh
                    // envelope and must not be reported as public byte
                    // truncation.
                    continue;
                }
                if self.finalized_bytes.saturating_add(next_exact) > self.byte_limit {
                    return Ok(PushOutcome::ByteClosed);
                }
                // A smaller prefix can also be caused by an internal
                // reserve/reallocation budget. That is a response-envelope
                // segmentation point, not public byte truncation: the
                // remaining rows from this lowered chunk must still be
                // processed.
                continue;
            }
            if self.finalized_rows.saturating_add(self.rebatcher.rows()) >= self.row_limit {
                self.flush_builder(tracker)?;
                return Ok(PushOutcome::RowClosed);
            }
        }
        Ok(PushOutcome::Appended)
    }

    fn finish(mut self, tracker: &mut MemoryTracker) -> Result<Vec<BatchEnvelope>, EngineError> {
        self.flush_builder(tracker)?;
        self.rebatcher
            .finish_to(self.finalized_bytes, &mut self.batches, tracker)?;
        let total = self.batches.iter().fold(0_usize, |sum, envelope| {
            sum.saturating_add(envelope.byte_count())
        });
        tracker.hold_remainder(total)?;
        Ok(self.batches)
    }
}

#[cfg(test)]
mod estimator_tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use stillflow_core::{BatchEnvelopeFactory, LogicalField, LogicalType};

    #[test]
    fn exact_estimator_matches_final_envelope_for_supported_arrays() {
        let id = stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(1));
        let schema = Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(id, "i", LogicalType::Int64, true).unwrap(),
                LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(2)),
                    "b",
                    LogicalType::Boolean,
                    true,
                )
                .unwrap(),
                LogicalField::new(
                    stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(3)),
                    "s",
                    LogicalType::Utf8,
                    true,
                )
                .unwrap(),
            ])
            .unwrap(),
        );
        let factory =
            BatchEnvelopeFactory::try_new(schema.clone(), uuid::Uuid::from_u128(9)).unwrap();
        let batch = RecordBatch::try_new(
            factory.arrow_schema().clone(),
            vec![
                std::sync::Arc::new(Int64Array::from(vec![
                    Some(1_i64),
                    None,
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                    None,
                    Some(8),
                    Some(9),
                ])),
                std::sync::Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                    Some(true),
                    Some(false),
                ])),
                std::sync::Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("ccc"),
                    Some("dddd"),
                    Some("e"),
                    Some("ffff"),
                    None,
                    Some("h"),
                    Some("ii"),
                ])),
            ],
        )
        .unwrap();
        let mut tracker = MemoryTracker::new_preview();
        let mut rebatcher = CanonicalRebatcher::new(schema, uuid::Uuid::from_u128(9), 4).unwrap();
        let exact = rebatcher.exact_bytes_after_append(&batch, 4).unwrap();
        let mut published = Vec::new();
        let incoming = batch.slice(0, 4);
        rebatcher
            .push(incoming, &mut tracker, |envelope, _| {
                published.push(envelope);
                Ok(())
            })
            .unwrap();
        let actual = published[0].byte_count();
        assert_eq!(exact, actual);
    }

    #[test]
    fn n_shrink_retry_halves_until_feasible() {
        let id = stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(12));
        let schema = Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(id, "v", LogicalType::Int64, false).unwrap()
            ])
            .unwrap(),
        );
        let arrow = stillflow_core::logical_schema_to_arrow(&schema).unwrap();
        let prepared = PreparedPlan {
            push_projection: true,
            scan_projection: vec![id],
            expected_connector: schema.as_ref().clone(),
            scan_output: schema.as_ref().clone(),
            materialize_schema: schema.as_ref().clone(),
            steps: Vec::new(),
            target_steps: Vec::new(),
            target_schema: schema.as_ref().clone(),
            materialize_id: stillflow_plan::PlanNodeId::from_uuid(uuid::Uuid::from_u128(13)),
        };
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![std::sync::Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
        )
        .unwrap();
        set_forced_export_retries(1);
        let mut tracker = MemoryTracker::new_preview();
        let (result, consumed) = lower_chunk(batch, &prepared, &arrow, &mut tracker).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(consumed, 2);
        assert!(result.get_array_memory_size() <= MAX_BATCH_BYTES);
        set_forced_export_retries(0);
    }

    #[test]
    fn exact_estimator_tracks_validity_and_utf8_offsets() {
        let id = stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(10));
        let schema = Arc::new(
            LogicalSchema::new(vec![
                LogicalField::new(id, "s", LogicalType::Utf8, true).unwrap()
            ])
            .unwrap(),
        );
        let factory =
            BatchEnvelopeFactory::try_new(schema.clone(), uuid::Uuid::from_u128(11)).unwrap();
        let batch = RecordBatch::try_new(
            factory.arrow_schema().clone(),
            vec![std::sync::Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("ccc"),
                Some("dddd"),
                Some("e"),
                Some("ffff"),
                None,
                Some("h"),
                Some("ii"),
            ]))],
        )
        .unwrap();
        let mut tracker = MemoryTracker::new_preview();
        let rebatcher =
            CanonicalRebatcher::new(schema.clone(), uuid::Uuid::from_u128(11), 9).unwrap();
        for k in [7, 8, 9] {
            let incoming = batch.slice(0, k);
            let exact = rebatcher.exact_bytes_after_append(&incoming, k).unwrap();
            let mut published = Vec::new();
            let mut fresh =
                CanonicalRebatcher::new(schema.clone(), uuid::Uuid::from_u128(11), 9).unwrap();
            fresh
                .push(incoming, &mut tracker, |envelope, _| {
                    published.push(envelope);
                    Ok(())
                })
                .unwrap();
            fresh.finish_to(0, &mut published, &mut tracker).unwrap();
            assert_eq!(exact, published[0].byte_count(), "k={k}");
        }
        let _ = rebatcher.rows();
    }
}
