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

    pub(crate) fn push(
        &mut self,
        incoming: RecordBatch,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let _remainder_guard = crate::memory::enter_phase(AllocatorPhase::Remainder);
        tracker.hold_incoming(incoming.get_array_memory_size())?;
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
            self.append_rows(&remaining, k)?;
            remaining = remaining.slice(k, remaining.num_rows() - k);
            tracker.hold_incoming(if remaining.num_rows() == 0 {
                0
            } else {
                remaining.get_array_memory_size()
            })?;
            tracker.hold_remainder(self.remainder_bytes())?;
            if self.should_flush() {
                self.flush(&mut publish, tracker)?;
            }
        }
        tracker.drop_incoming()?;
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        tracker: &mut MemoryTracker,
        mut publish: impl FnMut(BatchEnvelope, &mut MemoryTracker) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let _remainder_guard = crate::memory::enter_phase(AllocatorPhase::Remainder);
        if self.remainder_live() {
            self.flush(&mut publish, tracker)?;
        }
        drop(self);
        tracker.hold_remainder(0)?;
        Ok(())
    }

    fn can_reserve_for(&self, incoming: &RecordBatch, k: usize) -> Result<bool, EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        let mut sum_subsequent_old = self.remainder_bytes();
        let mut sum_prior_new = 0_usize;
        for (sink, array) in self.sinks.iter().zip(incoming.columns()) {
            let old_cap = sink.allocated_capacity_bytes();
            sum_subsequent_old = sum_subsequent_old.saturating_sub(old_cap);
            let (transient_peak, new_cap) =
                sink.calculate_growth_peak_and_new_capacity(array.as_ref(), k);
            let step_peak = sum_prior_new
                .saturating_add(transient_peak)
                .saturating_add(sum_subsequent_old);
            if step_peak > MAX_BATCH_BYTES {
                return Ok(false);
            }
            sum_prior_new = sum_prior_new.saturating_add(new_cap);
            if sum_prior_new.saturating_add(sum_subsequent_old) > MAX_BATCH_BYTES {
                return Ok(false);
            }
        }
        Ok(sum_prior_new <= MAX_BATCH_BYTES)
    }

    fn max_prefix(&self, incoming: &RecordBatch) -> Result<usize, EngineError> {
        let n = incoming.num_rows();
        if n == 0 {
            return Ok(0);
        }
        let mut low = 0_usize;
        let mut high = n.min(self.pack_limit.saturating_sub(self.rows));
        if high == 0 {
            return Ok(0);
        }
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if self.can_reserve_for(incoming, mid)? {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(low)
    }

    fn append_rows(&mut self, incoming: &RecordBatch, k: usize) -> Result<(), EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        for (sink, array) in self.sinks.iter_mut().zip(incoming.columns()) {
            sink.append(array.as_ref(), k)?;
        }
        self.rows = self.rows.saturating_add(k);
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.rows >= self.pack_limit || self.remainder_bytes() >= MAX_BATCH_BYTES
    }

    fn flush(
        &mut self,
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
        tracker.hold_remainder(envelope.byte_count())?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(EngineError::Internal("output envelope sequence overflow"))?;
        publish(envelope, tracker)?;
        tracker.hold_remainder(self.remainder_bytes())?;
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

    fn calculate_growth_peak_and_new_capacity(
        &self,
        array: &dyn Array,
        k: usize,
    ) -> (usize, usize) {
        match self {
            Self::Null => (0, 0),
            Self::Boolean(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Int8(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::UInt8(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Int16(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::UInt16(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Int32(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::UInt32(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Float32(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Date32(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Int64(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::UInt64(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::Float64(b) => b.calculate_growth_peak_and_new_capacity(k),
            Self::TimestampMs(b, _) => b.calculate_growth_peak_and_new_capacity(k),
            Self::TimestampUs(b, _) => b.calculate_growth_peak_and_new_capacity(k),
            Self::TimestampNs(b, _) => b.calculate_growth_peak_and_new_capacity(k),
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

    fn prepare_append(&mut self, additional_bits: usize) {
        let needed_bytes = self.bit_len.saturating_add(additional_bits).div_ceil(8);
        if needed_bytes > self.bytes.len() {
            self.bytes.reserve_exact(needed_bytes - self.bytes.len());
        }
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

struct ExactPrimitiveSink<T: arrow_array::ArrowPrimitiveType> {
    values: Vec<T::Native>,
    validity: BitPackedSink,
    all_valid: bool,
}

impl<T: arrow_array::ArrowPrimitiveType> ExactPrimitiveSink<T> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            validity: BitPackedSink::new(),
            all_valid: true,
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(std::mem::size_of::<T::Native>())
            .saturating_add(self.validity.allocated_capacity_bytes())
    }

    fn calculate_growth_peak_and_new_capacity(&self, k: usize) -> (usize, usize) {
        let val_slot = std::mem::size_of::<T::Native>();
        let cur_val_cap = self.values.capacity();
        let needed_val = self.values.len().saturating_add(k);
        let cur_val_bytes = cur_val_cap.saturating_mul(val_slot);
        let (val_transient, new_val_bytes) = if needed_val > cur_val_cap {
            let new_bytes = needed_val.saturating_mul(val_slot);
            (cur_val_bytes.saturating_add(new_bytes), new_bytes)
        } else {
            (cur_val_bytes, cur_val_bytes)
        };

        let (validity_transient, new_validity_bytes) =
            self.validity.calculate_growth_peak_and_new_capacity(k);

        (
            val_transient.saturating_add(validity_transient),
            new_val_bytes.saturating_add(new_validity_bytes),
        )
    }

    fn prepare_append(&mut self, k: usize) {
        self.values.reserve_exact(k);
        self.validity.prepare_append(k);
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        let values = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or(EngineError::Internal("remainder expected primitive array"))?;
        let len = k.min(values.len());
        self.prepare_append(len);
        for index in 0..len {
            if values.is_null(index) {
                self.all_valid = false;
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
        let nulls = if self.all_valid {
            None
        } else {
            let validity_buf = self.validity.finish();
            Some(NullBuffer::new(validity_buf))
        };
        self.all_valid = true;
        Ok(Arc::new(PrimitiveArray::<T>::new(values, nulls)))
    }
}

struct ExactBooleanSink {
    values: BitPackedSink,
    validity: BitPackedSink,
    all_valid: bool,
}

impl ExactBooleanSink {
    fn new() -> Self {
        Self {
            values: BitPackedSink::new(),
            validity: BitPackedSink::new(),
            all_valid: true,
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.values
            .allocated_capacity_bytes()
            .saturating_add(self.validity.allocated_capacity_bytes())
    }

    fn calculate_growth_peak_and_new_capacity(&self, k: usize) -> (usize, usize) {
        let (val_transient, val_new) = self.values.calculate_growth_peak_and_new_capacity(k);
        let (valid_transient, valid_new) = self.validity.calculate_growth_peak_and_new_capacity(k);
        (
            val_transient.saturating_add(valid_transient),
            val_new.saturating_add(valid_new),
        )
    }

    fn prepare_append(&mut self, k: usize) {
        self.values.prepare_append(k);
        self.validity.prepare_append(k);
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        let values = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(EngineError::Ffi)?;
        let len = k.min(values.len());
        self.prepare_append(len);
        for index in 0..len {
            if values.is_null(index) {
                self.all_valid = false;
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
        let nulls = if self.all_valid {
            None
        } else {
            let validity_buf = self.validity.finish();
            Some(NullBuffer::new(validity_buf))
        };
        self.all_valid = true;
        Ok(Arc::new(BooleanArray::new(values_buf, nulls)))
    }
}

struct VariableBytes {
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: BitPackedSink,
    all_valid: bool,
    data_bytes: usize,
}

impl VariableBytes {
    fn new() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
            validity: BitPackedSink::new(),
            all_valid: true,
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
            self.validity.calculate_growth_peak_and_new_capacity(k);

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

    fn prepare_append(&mut self, rows: usize, data_bytes: usize) {
        self.offsets.reserve_exact(rows);
        self.validity.prepare_append(rows);
        self.values.reserve_exact(data_bytes);
    }

    fn append_value(&mut self, payload: Option<&[u8]>) -> Result<(), EngineError> {
        match payload {
            None => {
                self.all_valid = false;
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
        let nulls = if self.all_valid {
            None
        } else {
            let validity_buf = self.validity.finish();
            Some(NullBuffer::new(validity_buf))
        };
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
        return append_utf8_values(sink, len, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        let len = k.min(values.len());
        return append_utf8_values(sink, len, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        let len = k.min(values.len());
        return append_utf8_values(sink, len, |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    Err(EngineError::Internal("remainder expected utf8 array"))
}

fn append_utf8_values<'a>(
    sink: &mut VariableBytes,
    len: usize,
    value_at: impl Fn(usize) -> Option<&'a [u8]>,
) -> Result<(), EngineError> {
    let data_bytes = (0..len)
        .filter_map(|index| value_at(index).map(<[u8]>::len))
        .fold(0_usize, usize::saturating_add);
    sink.prepare_append(len, data_bytes);
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
    sink.prepare_append(len, data_bytes);
    for index in 0..len {
        if values.is_null(index) {
            sink.append_value(None)?;
        } else {
            sink.append_value(Some(values.value(index)))?;
        }
    }
    Ok(())
}
