use std::sync::Arc;

use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, NullArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use stillflow_core::{
    BatchEnvelope, BatchEnvelopeFactory, LogicalSchema, LogicalType, TimeUnit, MAX_BATCH_BYTES,
};

use crate::error::EngineError;
use crate::memory::{AllocatorPhase, MemoryTracker};

pub(crate) struct CanonicalRebatcher {
    factory: BatchEnvelopeFactory,
    pack_limit: usize,
    rows: usize,
    sinks: Vec<ColumnSink>,
    next_sequence: u64,
}

impl CanonicalRebatcher {
    pub(crate) fn new(
        schema: Arc<LogicalSchema>,
        source_asset_id: uuid::Uuid,
        pack_limit: usize,
    ) -> Result<Self, EngineError> {
        let _guard = crate::memory::enter_phase(AllocatorPhase::Remainder);
        let sinks = schema
            .fields
            .iter()
            .map(|field| ColumnSink::new(&field.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            factory: BatchEnvelopeFactory::try_new(schema, source_asset_id)
                .map_err(|_| EngineError::Internal("output envelope factory failed"))?,
            pack_limit,
            rows: 0,
            sinks,
            next_sequence: 0,
        })
    }

    pub(crate) fn remainder_live(&self) -> bool {
        self.rows > 0
    }

    pub(crate) fn remainder_bytes(&self) -> usize {
        self.sinks
            .iter()
            .map(|sink| sink.allocated_capacity_bytes())
            .fold(0_usize, usize::saturating_add)
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Exact finalized `RecordBatch::get_array_memory_size()` if the current
    /// builder were frozen now.
    #[allow(dead_code)]
    pub(crate) fn exact_current_bytes(&self) -> usize {
        self.sinks
            .iter()
            .map(|sink| sink.exact_array_bytes(self.rows))
            .fold(0_usize, usize::saturating_add)
    }

    /// Allocation-free exact byte count of the envelope that would result from
    /// appending the first `k` rows of `incoming` to the current builder.
    pub(crate) fn exact_bytes_after_append(
        &self,
        incoming: &RecordBatch,
        k: usize,
    ) -> Result<usize, EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        Ok(self
            .sinks
            .iter()
            .zip(incoming.columns())
            .map(|(sink, array)| sink.exact_array_bytes_after_append(array.as_ref(), k))
            .fold(0_usize, usize::saturating_add))
    }

    /// Freeze the current builder into the next finalized envelope.
    pub(crate) fn flush_to(
        &mut self,
        base_bytes: usize,
        batches: &mut Vec<BatchEnvelope>,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        self.flush(
            base_bytes,
            &mut |envelope, _tracker| {
                batches.push(envelope);
                Ok(())
            },
            tracker,
        )
    }

    pub(crate) fn finish_to(
        self,
        base_bytes: usize,
        batches: &mut Vec<BatchEnvelope>,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        self.finish_with_base(base_bytes, tracker, |envelope, _tracker| {
            batches.push(envelope);
            Ok(())
        })
    }

    pub(crate) fn push(
        &mut self,
        incoming: RecordBatch,
        tracker: &mut MemoryTracker,
        publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        self.push_with_base(incoming, 0, tracker, publish)
    }

    pub(crate) fn push_with_base(
        &mut self,
        incoming: RecordBatch,
        base_bytes: usize,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let _remainder_guard = crate::memory::enter_phase(AllocatorPhase::Remainder);
        tracker.hold_remainder(base_bytes.saturating_add(self.remainder_bytes()))?;
        tracker.hold_incoming(incoming.get_array_memory_size())?;
        let mut remaining = incoming;
        while remaining.num_rows() > 0 {
            let k = self.max_prefix(&remaining)?;
            if k == 0 {
                // Section 10.4 canonical rebatcher: the single-envelope cap
                // closed (or `pack_limit` was reached), so freeze a non-empty
                // builder and retry this row against the empty builder.
                if self.remainder_live() {
                    self.flush(base_bytes, &mut publish, tracker)?;
                    continue;
                }
                return Err(EngineError::BoundExceeded(
                    "a single transformed row exceeds MAX_BATCH_BYTES",
                ));
            }
            self.append_rows(&remaining, k, tracker)?;
            remaining = remaining.slice(k, remaining.num_rows() - k);
            tracker.hold_incoming(if remaining.num_rows() == 0 {
                0
            } else {
                remaining.get_array_memory_size()
            })?;
            tracker.hold_remainder(base_bytes.saturating_add(self.remainder_bytes()))?;
            // Section 10.4 canonical rebatcher (and issue #46 §14): when the
            // appended remainder now meets the row cap or the single-envelope
            // byte cap, freeze eagerly. Both conditions are exact post-append
            // cap states of the admission-sound enforced capacity — never
            // transient-driven.
            if self.rows >= self.pack_limit || self.remainder_bytes() >= MAX_BATCH_BYTES {
                self.flush(base_bytes, &mut publish, tracker)?;
            }
        }
        tracker.drop_incoming()?;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        tracker: &mut MemoryTracker,
        publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        self.finish_with_base(0, tracker, publish)
    }

    pub(crate) fn finish_with_base(
        mut self,
        base_bytes: usize,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let _remainder_guard = crate::memory::enter_phase(AllocatorPhase::Remainder);
        if self.remainder_live() {
            self.flush(base_bytes, &mut publish, tracker)?;
        }
        drop(self);
        tracker.hold_remainder(base_bytes)?;
        Ok(())
    }

    /// Peak remainder-builder bytes during a realloc of the builder buffers
    /// while appending `k` rows, including the old-buffer/new-buffer
    /// transient. E3 §10.1(5)/§10.3: this is a memory-safety quantity only.
    /// It never gates admission, public row/byte caps, or batch boundaries;
    /// it feeds the independent peak pre-check that selects how the builder
    /// buffers physically grow (section 10.2 peak law).
    pub(crate) fn realloc_transient_peak(
        &self,
        incoming: &RecordBatch,
        k: usize,
    ) -> Result<usize, EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        let mut sum_subsequent_old = self.remainder_bytes();
        let mut sum_prior_new = 0_usize;
        let mut peak = 0_usize;
        for (sink, array) in self.sinks.iter().zip(incoming.columns()) {
            let old_cap = sink.allocated_capacity_bytes();
            sum_subsequent_old = sum_subsequent_old.saturating_sub(old_cap);
            let (transient_peak, new_cap) =
                sink.calculate_growth_peak_and_new_capacity(array.as_ref(), k);
            let step_peak = sum_prior_new
                .saturating_add(transient_peak)
                .saturating_add(sum_subsequent_old);
            peak = peak.max(step_peak);
            sum_prior_new = sum_prior_new.saturating_add(new_cap);
            peak = peak.max(sum_prior_new.saturating_add(sum_subsequent_old));
        }
        Ok(peak)
    }

    /// Admission oracle for the canonical remainder builder: the exact
    /// allocation-free prediction of `BatchEnvelope.byte_count()` for the
    /// envelope that would result from appending the first `k` rows
    /// (section 10.4). Because the builder shares the canonical buffer
    /// layout of the finalized arrays, the enforced post-append builder
    /// capacity equals this prediction minus per-array object sizes, so an
    /// admitted prefix can never push the enforced `remainder_bytes()`
    /// capacity over `MAX_BATCH_BYTES`.
    fn max_prefix(&self, incoming: &RecordBatch) -> Result<usize, EngineError> {
        let n = incoming.num_rows();
        if n == 0 {
            return Ok(0);
        }
        let high = n.min(self.pack_limit.saturating_sub(self.rows));
        if high == 0 {
            return Ok(0);
        }
        let mut low = 0_usize;
        let mut high = high;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if self.exact_bytes_after_append(incoming, mid)? <= MAX_BATCH_BYTES {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(low)
    }

    /// Append `k` admitted rows. The realloc transient is constrained by the
    /// independent peak pre-check below: when the predicted one-shot
    /// Append `k` admitted rows. The realloc transient is constrained by the
    /// independent peak pre-check below, which verifies the predicted
    /// old-buffer/new-buffer transient against the remaining engine-peak
    /// headroom (section 10.2/§10.3 peak law). The pre-check never changes
    /// which rows are admitted or when the builder freezes; the exact-need
    /// `reserve_exact` growth is the minimum-transient physical strategy,
    /// because old and new buffers necessarily coexist while copying.
    fn append_rows(
        &mut self,
        incoming: &RecordBatch,
        k: usize,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        let transient_peak = self.realloc_transient_peak(incoming, k)?;
        tracker.pre_check_realloc_peak(transient_peak, self.remainder_bytes())?;
        for (sink, array) in self.sinks.iter_mut().zip(incoming.columns()) {
            sink.append(array.as_ref(), k)?;
        }
        self.rows = self.rows.saturating_add(k);
        Ok(())
    }

    fn flush(
        &mut self,
        base_bytes: usize,
        publish: &mut impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        if self.rows == 0 {
            return Ok(());
        }
        let rows = self.rows;
        let columns = self
            .sinks
            .iter_mut()
            .map(|sink| sink.finish(rows))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(self.factory.arrow_schema().clone(), columns)
            .map_err(|_| EngineError::Internal("remainder freeze produced an invalid batch"))?;
        self.rows = 0;
        let envelope = self
            .factory
            .try_build(self.next_sequence, batch)
            .map_err(|_| EngineError::BoundExceeded("output envelope exceeded batch bounds"))?;
        tracker.hold_remainder(base_bytes.saturating_add(envelope.byte_count()))?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(EngineError::Internal("output envelope sequence overflow"))?;
        publish(envelope, tracker)?;
        tracker.hold_remainder(base_bytes.saturating_add(self.remainder_bytes()))?;
        Ok(())
    }
}

enum ColumnSink {
    Null,
    Boolean(ExactBooleanSink),
    Int8(ExactPrimitiveSink<Int8Type>),
    Int16(ExactPrimitiveSink<Int16Type>),
    Int32(ExactPrimitiveSink<Int32Type>),
    Int64(ExactPrimitiveSink<Int64Type>),
    UInt8(ExactPrimitiveSink<UInt8Type>),
    UInt16(ExactPrimitiveSink<UInt16Type>),
    UInt32(ExactPrimitiveSink<UInt32Type>),
    UInt64(ExactPrimitiveSink<UInt64Type>),
    Float32(ExactPrimitiveSink<Float32Type>),
    Float64(ExactPrimitiveSink<Float64Type>),
    Utf8(VariableBytes),
    Binary(VariableBytes),
    Date32(ExactPrimitiveSink<Date32Type>),
    TimestampMs(ExactPrimitiveSink<TimestampMillisecondType>, Option<String>),
    TimestampUs(ExactPrimitiveSink<TimestampMicrosecondType>, Option<String>),
    TimestampNs(ExactPrimitiveSink<TimestampNanosecondType>, Option<String>),
}

impl ColumnSink {
    fn new(data_type: &LogicalType) -> Result<Self, EngineError> {
        Ok(match data_type {
            LogicalType::Null => Self::Null,
            LogicalType::Boolean => Self::Boolean(ExactBooleanSink::new()),
            LogicalType::Int8 => Self::Int8(ExactPrimitiveSink::new()),
            LogicalType::Int16 => Self::Int16(ExactPrimitiveSink::new()),
            LogicalType::Int32 => Self::Int32(ExactPrimitiveSink::new()),
            LogicalType::Int64 => Self::Int64(ExactPrimitiveSink::new()),
            LogicalType::UInt8 => Self::UInt8(ExactPrimitiveSink::new()),
            LogicalType::UInt16 => Self::UInt16(ExactPrimitiveSink::new()),
            LogicalType::UInt32 => Self::UInt32(ExactPrimitiveSink::new()),
            LogicalType::UInt64 => Self::UInt64(ExactPrimitiveSink::new()),
            LogicalType::Float32 => Self::Float32(ExactPrimitiveSink::new()),
            LogicalType::Float64 => Self::Float64(ExactPrimitiveSink::new()),
            LogicalType::Utf8 => Self::Utf8(VariableBytes::new()),
            LogicalType::Binary => Self::Binary(VariableBytes::new()),
            LogicalType::Date32 => Self::Date32(ExactPrimitiveSink::new()),
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone,
            } => Self::TimestampMs(ExactPrimitiveSink::new(), timezone.clone()),
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone,
            } => Self::TimestampUs(ExactPrimitiveSink::new(), timezone.clone()),
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone,
            } => Self::TimestampNs(ExactPrimitiveSink::new(), timezone.clone()),
            LogicalType::Timestamp {
                unit: TimeUnit::Second,
                ..
            } => return Err(EngineError::TypeError("timestamp second unit is paused")),
            LogicalType::List(_) | LogicalType::Struct(_) => {
                return Err(EngineError::TypeError(
                    "list and struct execution is paused",
                ));
            }
        })
    }

    fn allocated_capacity_bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(b) => b.allocated_capacity_bytes(),
            Self::Int8(b) => b.allocated_capacity_bytes(),
            Self::UInt8(b) => b.allocated_capacity_bytes(),
            Self::Int16(b) => b.allocated_capacity_bytes(),
            Self::UInt16(b) => b.allocated_capacity_bytes(),
            Self::Int32(b) => b.allocated_capacity_bytes(),
            Self::UInt32(b) => b.allocated_capacity_bytes(),
            Self::Float32(b) => b.allocated_capacity_bytes(),
            Self::Date32(b) => b.allocated_capacity_bytes(),
            Self::Int64(b) => b.allocated_capacity_bytes(),
            Self::UInt64(b) => b.allocated_capacity_bytes(),
            Self::Float64(b) => b.allocated_capacity_bytes(),
            Self::TimestampMs(b, _) => b.allocated_capacity_bytes(),
            Self::TimestampUs(b, _) => b.allocated_capacity_bytes(),
            Self::TimestampNs(b, _) => b.allocated_capacity_bytes(),
            Self::Utf8(sink) | Self::Binary(sink) => sink.allocated_capacity_bytes(),
        }
    }

    #[allow(dead_code)]
    fn exact_array_bytes(&self, rows: usize) -> usize {
        match self {
            Self::Null => std::mem::size_of::<NullArray>(),
            Self::Boolean(b) => b.exact_array_bytes(rows),
            Self::Int8(b) => b.exact_array_bytes(rows),
            Self::UInt8(b) => b.exact_array_bytes(rows),
            Self::Int16(b) => b.exact_array_bytes(rows),
            Self::UInt16(b) => b.exact_array_bytes(rows),
            Self::Int32(b) => b.exact_array_bytes(rows),
            Self::UInt32(b) => b.exact_array_bytes(rows),
            Self::Float32(b) => b.exact_array_bytes(rows),
            Self::Date32(b) => b.exact_array_bytes(rows),
            Self::Int64(b) => b.exact_array_bytes(rows),
            Self::UInt64(b) => b.exact_array_bytes(rows),
            Self::Float64(b) => b.exact_array_bytes(rows),
            Self::TimestampMs(b, _) => b.exact_array_bytes(rows),
            Self::TimestampUs(b, _) => b.exact_array_bytes(rows),
            Self::TimestampNs(b, _) => b.exact_array_bytes(rows),
            Self::Utf8(sink) => sink.exact_array_bytes(rows, std::mem::size_of::<StringArray>()),
            Self::Binary(sink) => sink.exact_array_bytes(rows, std::mem::size_of::<BinaryArray>()),
        }
    }

    fn exact_array_bytes_after_append(&self, array: &dyn Array, k: usize) -> usize {
        let k = k.min(array.len());
        match self {
            Self::Null => std::mem::size_of::<NullArray>(),
            Self::Boolean(b) => b.exact_array_bytes_after_append(array, k),
            Self::Int8(b) => b.exact_array_bytes_after_append(array, k),
            Self::UInt8(b) => b.exact_array_bytes_after_append(array, k),
            Self::Int16(b) => b.exact_array_bytes_after_append(array, k),
            Self::UInt16(b) => b.exact_array_bytes_after_append(array, k),
            Self::Int32(b) => b.exact_array_bytes_after_append(array, k),
            Self::UInt32(b) => b.exact_array_bytes_after_append(array, k),
            Self::Float32(b) => b.exact_array_bytes_after_append(array, k),
            Self::Date32(b) => b.exact_array_bytes_after_append(array, k),
            Self::Int64(b) => b.exact_array_bytes_after_append(array, k),
            Self::UInt64(b) => b.exact_array_bytes_after_append(array, k),
            Self::Float64(b) => b.exact_array_bytes_after_append(array, k),
            Self::TimestampMs(b, _) => b.exact_array_bytes_after_append(array, k),
            Self::TimestampUs(b, _) => b.exact_array_bytes_after_append(array, k),
            Self::TimestampNs(b, _) => b.exact_array_bytes_after_append(array, k),
            Self::Utf8(sink) => {
                sink.exact_array_bytes_after_append(array, k, std::mem::size_of::<StringArray>())
            }
            Self::Binary(sink) => {
                sink.exact_array_bytes_after_append(array, k, std::mem::size_of::<BinaryArray>())
            }
        }
    }

    fn calculate_growth_peak_and_new_capacity(
        &self,
        array: &dyn Array,
        k: usize,
    ) -> (usize, usize) {
        match self {
            Self::Null => (0, 0),
            Self::Boolean(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Int8(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::UInt8(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Int16(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::UInt16(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Int32(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::UInt32(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Float32(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Date32(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Int64(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::UInt64(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Float64(b) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::TimestampMs(b, _) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::TimestampUs(b, _) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::TimestampNs(b, _) => b.calculate_growth_peak_and_new_capacity(array, k),
            Self::Utf8(sink) | Self::Binary(sink) => {
                sink.calculate_growth_peak_and_new_capacity(array, k)
            }
        }
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        match self {
            Self::Null => Ok(()),
            Self::Boolean(b) => b.append(array, k),
            Self::Int8(b) => b.append(array, k),
            Self::Int16(b) => b.append(array, k),
            Self::Int32(b) => b.append(array, k),
            Self::Int64(b) => b.append(array, k),
            Self::UInt8(b) => b.append(array, k),
            Self::UInt16(b) => b.append(array, k),
            Self::UInt32(b) => b.append(array, k),
            Self::UInt64(b) => b.append(array, k),
            Self::Float32(b) => b.append(array, k),
            Self::Float64(b) => b.append(array, k),
            Self::Utf8(sink) => append_utf8(sink, array, k),
            Self::Binary(sink) => append_binary(sink, array, k),
            Self::Date32(b) => b.append(array, k),
            Self::TimestampMs(b, _) => b.append(array, k),
            Self::TimestampUs(b, _) => b.append(array, k),
            Self::TimestampNs(b, _) => b.append(array, k),
        }
    }

    fn finish(&mut self, _rows: usize) -> Result<ArrayRef, EngineError> {
        Ok(match self {
            Self::Null => Arc::new(NullArray::new(_rows)),
            Self::Boolean(b) => b.finish()?,
            Self::Int8(b) => b.finish()?,
            Self::Int16(b) => b.finish()?,
            Self::Int32(b) => b.finish()?,
            Self::Int64(b) => b.finish()?,
            Self::UInt8(b) => b.finish()?,
            Self::UInt16(b) => b.finish()?,
            Self::UInt32(b) => b.finish()?,
            Self::UInt64(b) => b.finish()?,
            Self::Float32(b) => b.finish()?,
            Self::Float64(b) => b.finish()?,
            Self::Utf8(sink) => sink.finish_utf8()?,
            Self::Binary(sink) => sink.finish_binary()?,
            Self::Date32(b) => b.finish()?,
            Self::TimestampMs(b, timezone) => {
                let array = b.finish()?;
                let prim = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                    .ok_or(EngineError::Internal("timestamp cast failed"))?;
                Arc::new(prim.clone().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampUs(b, timezone) => {
                let array = b.finish()?;
                let prim = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>()
                    .ok_or(EngineError::Internal("timestamp cast failed"))?;
                Arc::new(prim.clone().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampNs(b, timezone) => {
                let array = b.finish()?;
                let prim = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
                    .ok_or(EngineError::Internal("timestamp cast failed"))?;
                Arc::new(prim.clone().with_timezone_opt(timezone.clone()))
            }
        })
    }
}

fn reserve_bytes_exact(bytes: &mut Vec<u8>, needed_bytes: usize) {
    if needed_bytes > bytes.len() {
        bytes.reserve_exact(needed_bytes - bytes.len());
    }
}

#[derive(Default)]
struct BitPackedSink {
    bytes: Vec<u8>,
    bit_len: usize,
}

impl BitPackedSink {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bit_len: 0,
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    fn calculate_growth_peak_and_new_capacity(&self, additional_bits: usize) -> (usize, usize) {
        let cur_cap_bytes = self.bytes.capacity();
        let needed_bytes = self.bit_len.saturating_add(additional_bits).div_ceil(8);
        if needed_bytes > cur_cap_bytes {
            (cur_cap_bytes.saturating_add(needed_bytes), needed_bytes)
        } else {
            (cur_cap_bytes, cur_cap_bytes)
        }
    }

    fn capacity_bytes_after_append(&self, additional_bits: usize) -> usize {
        let needed_bytes = self.bit_len.saturating_add(additional_bits).div_ceil(8);
        self.bytes.capacity().max(needed_bytes)
    }

    fn prepare_append(&mut self, additional_bits: usize) {
        let needed_bytes = self.bit_len.saturating_add(additional_bits).div_ceil(8);
        reserve_bytes_exact(&mut self.bytes, needed_bytes);
    }

    fn append_bit(&mut self, bit: bool) {
        let byte_idx = self.bit_len / 8;
        let bit_idx = self.bit_len % 8;
        if bit_idx == 0 {
            self.bytes.push(0);
        }
        if bit {
            self.bytes[byte_idx] |= 1 << bit_idx;
        }
        self.bit_len += 1;
    }

    fn finish(&mut self) -> BooleanBuffer {
        let len = self.bit_len;
        let buffer = Buffer::from_vec(std::mem::take(&mut self.bytes));
        self.bit_len = 0;
        BooleanBuffer::new(buffer, 0, len)
    }
}

/// Validity bitmap sink whose allocation layout is canonical with the
/// finalized envelope (E3 §10.4 rule 3). While every appended row is valid
/// no backing allocation exists, because the finalized array owns no validity
/// buffer at all; once a null appears the buffer is materialized with an
/// exact `ceil(rows / 8)` capacity — precisely the capacity the finalized
/// validity bitmap will own. This keeps the enforced builder capacity and
/// the exact estimator consistent: an admission decision made on
/// `candidate_envelope_bytes` can never be defeated by validity scratch the
/// finalized envelope will not contain.
struct LazyValiditySink {
    bits: BitPackedSink,
    leading_valid: usize,
    has_null: bool,
}

impl LazyValiditySink {
    fn new() -> Self {
        Self {
            bits: BitPackedSink::new(),
            leading_valid: 0,
            has_null: false,
        }
    }

    fn has_null(&self) -> bool {
        self.has_null
    }

    fn rows(&self) -> usize {
        if self.has_null {
            self.bits.bit_len
        } else {
            self.leading_valid
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.bits.allocated_capacity_bytes()
    }

    fn capacity_bytes_after_append(&self, additional_bits: usize) -> usize {
        let needed_bytes = self.rows().saturating_add(additional_bits).div_ceil(8);
        self.bits.allocated_capacity_bytes().max(needed_bytes)
    }

    fn calculate_growth_peak_and_new_capacity(&self, additional_bits: usize) -> (usize, usize) {
        let current = self.bits.allocated_capacity_bytes();
        let needed_bytes = self.rows().saturating_add(additional_bits).div_ceil(8);
        if needed_bytes > current {
            (current.saturating_add(needed_bytes), needed_bytes)
        } else {
            (current, current)
        }
    }

    fn materialize(&mut self) {
        let nbytes = self.leading_valid.div_ceil(8);
        self.bits.bytes = vec![0xFF_u8; nbytes];
        self.bits.bit_len = self.leading_valid;
        self.has_null = true;
    }

    fn prepare_append(&mut self, additional_bits: usize, any_null: bool) {
        if !self.has_null {
            if !any_null {
                return;
            }
            self.materialize();
        }
        let needed_bytes = self
            .bits
            .bit_len
            .saturating_add(additional_bits)
            .div_ceil(8);
        reserve_bytes_exact(&mut self.bits.bytes, needed_bytes);
    }

    fn append_bit(&mut self, valid: bool) {
        if !self.has_null {
            if valid {
                self.leading_valid += 1;
            } else {
                self.materialize();
                self.bits.append_bit(false);
            }
            return;
        }
        self.bits.append_bit(valid);
    }

    fn finish(&mut self) -> Option<BooleanBuffer> {
        if !self.has_null {
            // No validity buffer exists for an all-valid envelope, but the
            // implicit leading-valid run must still reset for the next fill.
            self.leading_valid = 0;
            return None;
        }
        let buffer = self.bits.finish();
        self.has_null = false;
        self.leading_valid = 0;
        Some(buffer)
    }
}

struct ExactPrimitiveSink<T: arrow_array::ArrowPrimitiveType> {
    values: Vec<T::Native>,
    validity: LazyValiditySink,
}

impl<T: arrow_array::ArrowPrimitiveType> ExactPrimitiveSink<T> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            validity: LazyValiditySink::new(),
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(std::mem::size_of::<T::Native>())
            .saturating_add(self.validity.allocated_capacity_bytes())
    }

    #[allow(dead_code)]
    fn exact_array_bytes(&self, _rows: usize) -> usize {
        let values_bytes = self
            .values
            .capacity()
            .saturating_mul(std::mem::size_of::<T::Native>());
        let validity_bytes = if self.validity.has_null() {
            self.validity.allocated_capacity_bytes()
        } else {
            0
        };
        std::mem::size_of::<PrimitiveArray<T>>()
            .saturating_add(values_bytes)
            .saturating_add(validity_bytes)
    }

    fn exact_array_bytes_after_append(&self, array: &dyn Array, k: usize) -> usize {
        let values = match array.as_any().downcast_ref::<PrimitiveArray<T>>() {
            Some(values) => values,
            None => return self.exact_array_bytes(0),
        };
        let k = k.min(values.len());
        let slot = std::mem::size_of::<T::Native>();
        let new_values_bytes = self
            .values
            .capacity()
            .max(self.values.len().saturating_add(k))
            .saturating_mul(slot);
        let validity_bytes = if !self.validity.has_null() && !prefix_has_null(values, k) {
            0
        } else {
            self.validity.capacity_bytes_after_append(k)
        };
        std::mem::size_of::<PrimitiveArray<T>>()
            .saturating_add(new_values_bytes)
            .saturating_add(validity_bytes)
    }

    fn calculate_growth_peak_and_new_capacity(
        &self,
        array: &dyn Array,
        k: usize,
    ) -> (usize, usize) {
        let values = match array.as_any().downcast_ref::<PrimitiveArray<T>>() {
            Some(values) => values,
            None => {
                return (
                    self.allocated_capacity_bytes(),
                    self.allocated_capacity_bytes(),
                )
            }
        };
        let val_slot = std::mem::size_of::<T::Native>();
        let cur_val_cap = self.values.capacity();
        let needed_val = self.values.len().saturating_add(k.min(values.len()));
        let cur_val_bytes = cur_val_cap.saturating_mul(val_slot);
        let (val_transient, new_val_bytes) = if needed_val > cur_val_cap {
            let new_bytes = needed_val.saturating_mul(val_slot);
            (cur_val_bytes.saturating_add(new_bytes), new_bytes)
        } else {
            (cur_val_bytes, cur_val_bytes)
        };

        let validity_bits = k.min(values.len());
        let (validity_transient, new_validity_bytes) =
            if !self.validity.has_null() && !prefix_has_null(values, validity_bits) {
                (self.validity.allocated_capacity_bytes(), 0)
            } else {
                self.validity
                    .calculate_growth_peak_and_new_capacity(validity_bits)
            };

        (
            val_transient.saturating_add(validity_transient),
            new_val_bytes.saturating_add(new_validity_bytes),
        )
    }

    fn prepare_append(&mut self, k: usize, any_null: bool) {
        self.values.reserve_exact(k);
        self.validity.prepare_append(k, any_null);
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        let values = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or(EngineError::Internal("remainder expected primitive array"))?;
        let len = k.min(values.len());
        let any_null = prefix_has_null(values, len);
        self.prepare_append(len, any_null);
        for index in 0..len {
            if values.is_null(index) {
                self.validity.append_bit(false);
                self.values.push(T::Native::default());
            } else {
                self.validity.append_bit(true);
                self.values.push(values.value(index));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, EngineError> {
        let values = ScalarBuffer::from(std::mem::take(&mut self.values));
        let nulls = self.validity.finish().map(NullBuffer::new);
        Ok(Arc::new(PrimitiveArray::<T>::new(values, nulls)))
    }
}

struct ExactBooleanSink {
    values: BitPackedSink,
    validity: LazyValiditySink,
}

impl ExactBooleanSink {
    fn new() -> Self {
        Self {
            values: BitPackedSink::new(),
            validity: LazyValiditySink::new(),
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.values
            .allocated_capacity_bytes()
            .saturating_add(self.validity.allocated_capacity_bytes())
    }

    #[allow(dead_code)]
    fn exact_array_bytes(&self, _rows: usize) -> usize {
        let values_bytes = self.values.allocated_capacity_bytes();
        let validity_bytes = if self.validity.has_null() {
            self.validity.allocated_capacity_bytes()
        } else {
            0
        };
        std::mem::size_of::<BooleanArray>()
            .saturating_add(values_bytes)
            .saturating_add(validity_bytes)
    }

    fn exact_array_bytes_after_append(&self, array: &dyn Array, k: usize) -> usize {
        let values = match array.as_any().downcast_ref::<BooleanArray>() {
            Some(values) => values,
            None => return self.exact_array_bytes(0),
        };
        let k = k.min(values.len());
        let values_bytes = self.values.capacity_bytes_after_append(k);
        let validity_bytes = if !self.validity.has_null() && !prefix_has_null(values, k) {
            0
        } else {
            self.validity.capacity_bytes_after_append(k)
        };
        std::mem::size_of::<BooleanArray>()
            .saturating_add(values_bytes)
            .saturating_add(validity_bytes)
    }

    fn calculate_growth_peak_and_new_capacity(
        &self,
        array: &dyn Array,
        k: usize,
    ) -> (usize, usize) {
        let values = match array.as_any().downcast_ref::<BooleanArray>() {
            Some(values) => values,
            None => {
                return (
                    self.allocated_capacity_bytes(),
                    self.allocated_capacity_bytes(),
                )
            }
        };
        let bits = k.min(values.len());
        let (val_transient, val_new) = self.values.calculate_growth_peak_and_new_capacity(bits);
        let (valid_transient, valid_new) =
            if !self.validity.has_null() && !prefix_has_null(values, bits) {
                (self.validity.allocated_capacity_bytes(), 0)
            } else {
                self.validity.calculate_growth_peak_and_new_capacity(bits)
            };
        (
            val_transient.saturating_add(valid_transient),
            val_new.saturating_add(valid_new),
        )
    }

    fn prepare_append(&mut self, k: usize, any_null: bool) {
        self.values.prepare_append(k);
        self.validity.prepare_append(k, any_null);
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        let values = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(EngineError::Ffi)?;
        let len = k.min(values.len());
        let any_null = prefix_has_null(values, len);
        self.prepare_append(len, any_null);
        for index in 0..len {
            if values.is_null(index) {
                self.validity.append_bit(false);
                self.values.append_bit(false);
            } else {
                self.validity.append_bit(true);
                self.values.append_bit(values.value(index));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, EngineError> {
        let values_buf = self.values.finish();
        let nulls = self.validity.finish().map(NullBuffer::new);
        Ok(Arc::new(BooleanArray::new(values_buf, nulls)))
    }
}

struct VariableBytes {
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: LazyValiditySink,
    data_bytes: usize,
}

impl VariableBytes {
    fn new() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
            validity: LazyValiditySink::new(),
            data_bytes: 0,
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.offsets
            .capacity()
            .saturating_mul(4)
            .saturating_add(self.values.capacity())
            .saturating_add(self.validity.allocated_capacity_bytes())
    }

    #[allow(dead_code)]
    fn exact_array_bytes(&self, _rows: usize, object_size: usize) -> usize {
        let validity_bytes = if self.validity.has_null() {
            self.validity.allocated_capacity_bytes()
        } else {
            0
        };
        object_size
            .saturating_add(self.offsets.capacity().saturating_mul(4))
            .saturating_add(self.values.capacity())
            .saturating_add(validity_bytes)
    }

    fn exact_array_bytes_after_append(
        &self,
        array: &dyn Array,
        k: usize,
        object_size: usize,
    ) -> usize {
        let k = k.min(array.len());
        let additional_data = array_data_bytes_for_slice(array, k);
        let all_valid_after = !self.validity.has_null() && !prefix_has_null(array, k);
        let validity_bytes = if all_valid_after {
            0
        } else {
            self.validity.capacity_bytes_after_append(k)
        };
        object_size
            .saturating_add(
                self.offsets
                    .capacity()
                    .max(self.offsets.len().saturating_add(k))
                    .saturating_mul(4),
            )
            .saturating_add(
                self.values
                    .capacity()
                    .max(self.values.len().saturating_add(additional_data)),
            )
            .saturating_add(validity_bytes)
    }

    fn calculate_growth_peak_and_new_capacity(
        &self,
        array: &dyn Array,
        k: usize,
    ) -> (usize, usize) {
        let additional_data = array_data_bytes_for_slice(array, k);

        let cur_offsets_cap = self.offsets.capacity();
        let needed_offsets = self.offsets.len().saturating_add(k);
        let cur_offsets_bytes = cur_offsets_cap.saturating_mul(4);
        let (offsets_transient, new_offsets_bytes) = if needed_offsets > cur_offsets_cap {
            let new_bytes = needed_offsets.saturating_mul(4);
            (cur_offsets_bytes.saturating_add(new_bytes), new_bytes)
        } else {
            (cur_offsets_bytes, cur_offsets_bytes)
        };

        let (validity_transient, new_validity_bytes) =
            if !self.validity.has_null() && !prefix_has_null(array, k) {
                (self.validity.allocated_capacity_bytes(), 0)
            } else {
                self.validity.calculate_growth_peak_and_new_capacity(k)
            };

        let cur_values_cap = self.values.capacity();
        let needed_values = self.values.len().saturating_add(additional_data);
        let cur_values_bytes = cur_values_cap;
        let (values_transient, new_values_bytes) = if needed_values > cur_values_cap {
            let new_bytes = needed_values;
            (cur_values_bytes.saturating_add(new_bytes), new_bytes)
        } else {
            (cur_values_bytes, cur_values_bytes)
        };

        (
            offsets_transient
                .saturating_add(validity_transient)
                .saturating_add(values_transient),
            new_offsets_bytes
                .saturating_add(new_validity_bytes)
                .saturating_add(new_values_bytes),
        )
    }

    fn prepare_append(&mut self, rows: usize, data_bytes: usize, any_null: bool) {
        self.offsets.reserve_exact(rows);
        self.validity.prepare_append(rows, any_null);
        self.values.reserve_exact(data_bytes);
    }

    fn append_value(&mut self, payload: Option<&[u8]>) -> Result<(), EngineError> {
        match payload {
            None => {
                self.validity.append_bit(false);
            }
            Some(bytes) => {
                self.values.extend_from_slice(bytes);
                self.validity.append_bit(true);
                self.data_bytes = self.data_bytes.saturating_add(bytes.len());
            }
        }
        let offset = i32::try_from(self.values.len())
            .map_err(|_| EngineError::BoundExceeded("variable-width remainder offset overflow"))?;
        self.offsets.push(offset);
        Ok(())
    }

    fn finish_parts(
        &mut self,
    ) -> Result<(OffsetBuffer<i32>, Buffer, Option<NullBuffer>), EngineError> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(std::mem::take(&mut self.offsets)));
        let values = Buffer::from_vec(std::mem::take(&mut self.values));
        let nulls = self.validity.finish().map(NullBuffer::new);
        *self = Self::new();
        Ok((offsets, values, nulls))
    }

    fn finish_utf8(&mut self) -> Result<ArrayRef, EngineError> {
        let (offsets, values, nulls) = self.finish_parts()?;
        StringArray::try_new(offsets, values, nulls)
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|_| EngineError::Internal("remainder freeze produced invalid utf8"))
    }

    fn finish_binary(&mut self) -> Result<ArrayRef, EngineError> {
        let (offsets, values, nulls) = self.finish_parts()?;
        BinaryArray::try_new(offsets, values, nulls)
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|_| EngineError::Internal("remainder freeze produced invalid binary"))
    }
}

fn prefix_has_null(array: &dyn Array, k: usize) -> bool {
    let end = k.min(array.len());
    (0..end).any(|index| array.is_null(index))
}

fn array_data_bytes_for_slice(array: &dyn Array, k: usize) -> usize {
    let end = k.min(array.len());
    if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
        return utf8_range_bytes(utf8, 0, end);
    }
    if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        let mut data = 0_usize;
        for i in 0..end {
            if !binary.is_null(i) {
                data = data.saturating_add(binary.value(i).len());
            }
        }
        return data;
    }
    if let Some(view) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        let mut data = 0_usize;
        for i in 0..end {
            if !view.is_null(i) {
                data = data.saturating_add(view.value(i).len());
            }
        }
        return data;
    }
    if let Some(large) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        let mut data = 0_usize;
        for i in 0..end {
            if !large.is_null(i) {
                data = data.saturating_add(large.value(i).len());
            }
        }
        return data;
    }
    0
}

fn utf8_range_bytes(array: &StringArray, offset: usize, k: usize) -> usize {
    if k == 0 {
        return 0;
    }
    let offsets = array.value_offsets();
    let start = offsets[offset] as usize;
    let end = offsets[offset + k] as usize;
    end.saturating_sub(start)
}

fn append_utf8(sink: &mut VariableBytes, array: &dyn Array, k: usize) -> Result<(), EngineError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        let len = k.min(values.len());
        return append_utf8_values(sink, len, values, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        let len = k.min(values.len());
        return append_utf8_values(sink, len, values, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        let len = k.min(values.len());
        return append_utf8_values(sink, len, values, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    Err(EngineError::Internal("remainder expected utf8 array"))
}

fn append_utf8_values<'a>(
    sink: &mut VariableBytes,
    len: usize,
    array: &dyn Array,
    value_at: impl Fn(usize) -> Option<&'a [u8]>,
) -> Result<(), EngineError> {
    let data_bytes = (0..len)
        .filter_map(|index| value_at(index).map(<[u8]>::len))
        .fold(0_usize, usize::saturating_add);
    let any_null = prefix_has_null(array, len);
    sink.prepare_append(len, data_bytes, any_null);
    for index in 0..len {
        sink.append_value(value_at(index))?;
    }
    Ok(())
}

fn append_binary(sink: &mut VariableBytes, array: &dyn Array, k: usize) -> Result<(), EngineError> {
    let values = array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(EngineError::Ffi)?;
    let len = k.min(values.len());
    let data_bytes = (0..len)
        .filter(|&index| !values.is_null(index))
        .map(|index| values.value(index).len())
        .fold(0_usize, usize::saturating_add);
    let any_null = prefix_has_null(values, len);
    sink.prepare_append(len, data_bytes, any_null);
    for index in 0..len {
        if values.is_null(index) {
            sink.append_value(None)?;
        } else {
            sink.append_value(Some(values.value(index)))?;
        }
    }
    Ok(())
}
