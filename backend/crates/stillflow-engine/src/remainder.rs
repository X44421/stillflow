use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_select::concat::concat;
use stillflow_core::{BatchEnvelope, BatchEnvelopeFactory, LogicalSchema, MAX_BATCH_BYTES};

use crate::error::EngineError;
use crate::memory::MemoryTracker;

pub(crate) struct CanonicalRebatcher {
    factory: BatchEnvelopeFactory,
    pack_limit: usize,
    remainder: Option<RecordBatch>,
    next_sequence: u64,
}

impl CanonicalRebatcher {
    pub(crate) fn new(
        schema: Arc<LogicalSchema>,
        source_asset_id: uuid::Uuid,
        pack_limit: usize,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            factory: BatchEnvelopeFactory::try_new(schema, source_asset_id)
                .map_err(|_| EngineError::Internal("output envelope factory failed"))?,
            pack_limit,
            remainder: None,
            next_sequence: 0,
        })
    }

    pub(crate) fn remainder_live(&self) -> bool {
        self.remainder
            .as_ref()
            .is_some_and(|batch| batch.num_rows() > 0)
    }

    pub(crate) fn remainder_bytes(&self) -> usize {
        self.remainder
            .as_ref()
            .map(RecordBatch::get_array_memory_size)
            .unwrap_or(0)
    }

    pub(crate) fn push(
        &mut self,
        incoming: RecordBatch,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let mut remaining = incoming;
        while remaining.num_rows() > 0 {
            let k = self.max_prefix(&remaining)?;
            if k == 0 {
                if self.remainder_live() {
                    self.flush(&mut publish, tracker)?;
                    continue;
                }
                return Err(EngineError::BoundExceeded(
                    "a single transformed row exceeds MAX_BATCH_BYTES",
                ));
            }
            let take = remaining.slice(0, k);
            remaining = remaining.slice(k, remaining.num_rows() - k);
            self.append_take(take)?;
            tracker.hold_remainder(self.remainder_bytes())?;
            if self.should_flush() {
                self.flush(&mut publish, tracker)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        if self.remainder_live() {
            self.flush(&mut publish, tracker)?;
        }
        Ok(())
    }

    fn max_prefix(&self, incoming: &RecordBatch) -> Result<usize, EngineError> {
        let current_rows = self
            .remainder
            .as_ref()
            .map(RecordBatch::num_rows)
            .unwrap_or(0);
        let current_bytes = self.remainder_bytes();
        let n = incoming.num_rows();
        if n == 0 {
            return Ok(0);
        }
        let mut low = 0_usize;
        let mut high = n.min(self.pack_limit.saturating_sub(current_rows));
        if high == 0 {
            return Ok(0);
        }
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let slice = incoming.slice(0, mid);
            let combined = current_bytes.saturating_add(slice.get_array_memory_size());
            if current_rows + mid <= self.pack_limit && combined <= MAX_BATCH_BYTES {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(low)
    }

    fn append_take(&mut self, take: RecordBatch) -> Result<(), EngineError> {
        self.remainder = Some(match self.remainder.take() {
            Some(existing) => concat_batches(&existing, &take)?,
            None => take,
        });
        Ok(())
    }

    fn should_flush(&self) -> bool {
        let Some(remainder) = &self.remainder else {
            return false;
        };
        remainder.num_rows() >= self.pack_limit
            || remainder.get_array_memory_size() >= MAX_BATCH_BYTES
    }

    fn flush(
        &mut self,
        publish: &mut impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        let Some(batch) = self.remainder.take() else {
            return Ok(());
        };
        let envelope = self
            .factory
            .try_build(self.next_sequence, batch)
            .map_err(|_| EngineError::BoundExceeded("output envelope exceeded batch bounds"))?;
        tracker.hold_remainder(envelope.byte_count())?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(EngineError::Internal("output envelope sequence overflow"))?;
        publish(envelope, tracker)?;
        tracker.hold_remainder(0)?;
        Ok(())
    }
}

fn concat_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch, EngineError> {
    if left.num_columns() != right.num_columns() {
        return Err(EngineError::Internal("remainder concat column mismatch"));
    }
    let columns = left
        .columns()
        .iter()
        .zip(right.columns())
        .map(|(left_col, right_col)| {
            concat(&[left_col.as_ref(), right_col.as_ref()])
                .map_err(|_| EngineError::Internal("remainder concat failed"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(left.schema(), columns).map_err(|_| EngineError::Ffi)
}
