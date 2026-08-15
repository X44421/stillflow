use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, NullArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use stillflow_core::{
    BatchEnvelope, BatchEnvelopeFactory, LogicalSchema, LogicalType, TimeUnit, MAX_BATCH_BYTES,
};

use crate::error::EngineError;
use crate::memory::{AllocatorPhase, MemoryTracker};
use crate::predict::{fixed_physical_bytes, utf8_physical_bytes};

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
        if self.rows == 0 {
            return 0;
        }
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
            if !self.can_reserve_for(&remaining, k)? {
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
        Ok(())
    }

    fn can_reserve_for(&self, incoming: &RecordBatch, k: usize) -> Result<bool, EngineError> {
        if incoming.num_columns() != self.sinks.len() {
            return Err(EngineError::Internal("remainder column mismatch"));
        }
        let total_current_cap = self.remainder_bytes();
        let mut total_resulting_cap = 0_usize;
        for (sink, array) in self.sinks.iter().zip(incoming.columns()) {
            let sink_current_cap = sink.allocated_capacity_bytes();
            let other_live_cap = total_current_cap.saturating_sub(sink_current_cap);
            let (transient_peak, new_cap) =
                sink.calculate_growth_peak_and_new_capacity(array.as_ref(), k);
            if other_live_cap.saturating_add(transient_peak) > MAX_BATCH_BYTES {
                return Ok(false);
            }
            total_resulting_cap = total_resulting_cap.saturating_add(new_cap);
            if total_resulting_cap > MAX_BATCH_BYTES {
                return Ok(false);
            }
        }
        Ok(true)
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
            if self.fits_prefix(incoming, mid) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(low)
    }

    fn fits_prefix(&self, incoming: &RecordBatch, k: usize) -> bool {
        self.rows.saturating_add(k) <= self.pack_limit
            && self.predicted_bytes_with(incoming, k) <= MAX_BATCH_BYTES
    }

    fn predicted_bytes_with(&self, incoming: &RecordBatch, k: usize) -> usize {
        incoming
            .columns()
            .iter()
            .zip(self.sinks.iter())
            .map(|(array, sink)| {
                sink.allocated_capacity_bytes()
                    .saturating_add(incoming_physical_bytes(array.as_ref(), k))
            })
            .fold(0_usize, usize::saturating_add)
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
        tracker.hold_remainder(0)?;
        Ok(())
    }
}

enum ColumnSink {
    Null,
    Boolean(BooleanBuilder),
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(VariableBytes),
    Binary(VariableBytes),
    Date32(PrimitiveBuilder<Date32Type>),
    TimestampMs(PrimitiveBuilder<TimestampMillisecondType>, Option<String>),
    TimestampUs(PrimitiveBuilder<TimestampMicrosecondType>, Option<String>),
    TimestampNs(PrimitiveBuilder<TimestampNanosecondType>, Option<String>),
}

impl ColumnSink {
    fn new(data_type: &LogicalType) -> Result<Self, EngineError> {
        Ok(match data_type {
            LogicalType::Null => Self::Null,
            LogicalType::Boolean => Self::Boolean(BooleanBuilder::new()),
            LogicalType::Int8 => Self::Int8(PrimitiveBuilder::new()),
            LogicalType::Int16 => Self::Int16(PrimitiveBuilder::new()),
            LogicalType::Int32 => Self::Int32(PrimitiveBuilder::new()),
            LogicalType::Int64 => Self::Int64(PrimitiveBuilder::new()),
            LogicalType::UInt8 => Self::UInt8(PrimitiveBuilder::new()),
            LogicalType::UInt16 => Self::UInt16(PrimitiveBuilder::new()),
            LogicalType::UInt32 => Self::UInt32(PrimitiveBuilder::new()),
            LogicalType::UInt64 => Self::UInt64(PrimitiveBuilder::new()),
            LogicalType::Float32 => Self::Float32(PrimitiveBuilder::new()),
            LogicalType::Float64 => Self::Float64(PrimitiveBuilder::new()),
            LogicalType::Utf8 => Self::Utf8(VariableBytes::new()),
            LogicalType::Binary => Self::Binary(VariableBytes::new()),
            LogicalType::Date32 => Self::Date32(PrimitiveBuilder::new()),
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone,
            } => Self::TimestampMs(PrimitiveBuilder::new(), timezone.clone()),
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone,
            } => Self::TimestampUs(PrimitiveBuilder::new(), timezone.clone()),
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone,
            } => Self::TimestampNs(PrimitiveBuilder::new(), timezone.clone()),
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
            Self::Boolean(b) => b.capacity().div_ceil(8).saturating_mul(2),
            Self::Int8(b) => b
                .capacity()
                .saturating_mul(1)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::UInt8(b) => b
                .capacity()
                .saturating_mul(1)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Int16(b) => b
                .capacity()
                .saturating_mul(2)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::UInt16(b) => b
                .capacity()
                .saturating_mul(2)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Int32(b) => b
                .capacity()
                .saturating_mul(4)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::UInt32(b) => b
                .capacity()
                .saturating_mul(4)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Float32(b) => b
                .capacity()
                .saturating_mul(4)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Date32(b) => b
                .capacity()
                .saturating_mul(4)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Int64(b) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::UInt64(b) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::Float64(b) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::TimestampMs(b, _) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::TimestampUs(b, _) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
            Self::TimestampNs(b, _) => b
                .capacity()
                .saturating_mul(8)
                .saturating_add(b.capacity().div_ceil(8)),
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
            Self::Boolean(b) => boolean_growth(b, k),
            Self::Int8(b) => primitive_growth(b, k),
            Self::UInt8(b) => primitive_growth(b, k),
            Self::Int16(b) => primitive_growth(b, k),
            Self::UInt16(b) => primitive_growth(b, k),
            Self::Int32(b) => primitive_growth(b, k),
            Self::UInt32(b) => primitive_growth(b, k),
            Self::Float32(b) => primitive_growth(b, k),
            Self::Date32(b) => primitive_growth(b, k),
            Self::Int64(b) => primitive_growth(b, k),
            Self::UInt64(b) => primitive_growth(b, k),
            Self::Float64(b) => primitive_growth(b, k),
            Self::TimestampMs(b, _) => primitive_growth(b, k),
            Self::TimestampUs(b, _) => primitive_growth(b, k),
            Self::TimestampNs(b, _) => primitive_growth(b, k),
            Self::Utf8(sink) | Self::Binary(sink) => {
                sink.calculate_growth_peak_and_new_capacity(array, k)
            }
        }
    }

    fn append(&mut self, array: &dyn Array, k: usize) -> Result<(), EngineError> {
        let slice = array.slice(0, k);
        match self {
            Self::Null => Ok(()),
            Self::Boolean(builder) => append_bool(builder, slice.as_ref()),
            Self::Int8(builder) => append_prim(builder, slice.as_ref()),
            Self::Int16(builder) => append_prim(builder, slice.as_ref()),
            Self::Int32(builder) => append_prim(builder, slice.as_ref()),
            Self::Int64(builder) => append_prim(builder, slice.as_ref()),
            Self::UInt8(builder) => append_prim(builder, slice.as_ref()),
            Self::UInt16(builder) => append_prim(builder, slice.as_ref()),
            Self::UInt32(builder) => append_prim(builder, slice.as_ref()),
            Self::UInt64(builder) => append_prim(builder, slice.as_ref()),
            Self::Float32(builder) => append_prim(builder, slice.as_ref()),
            Self::Float64(builder) => append_prim(builder, slice.as_ref()),
            Self::Utf8(sink) => append_utf8(sink, slice.as_ref()),
            Self::Binary(sink) => append_binary(sink, slice.as_ref()),
            Self::Date32(builder) => append_prim(builder, slice.as_ref()),
            Self::TimestampMs(builder, _) => append_prim(builder, slice.as_ref()),
            Self::TimestampUs(builder, _) => append_prim(builder, slice.as_ref()),
            Self::TimestampNs(builder, _) => append_prim(builder, slice.as_ref()),
        }
    }

    fn finish(&mut self, rows: usize) -> Result<ArrayRef, EngineError> {
        Ok(match self {
            Self::Null => Arc::new(NullArray::new(rows)),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Int8(builder) => Arc::new(builder.finish()),
            Self::Int16(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt8(builder) => Arc::new(builder.finish()),
            Self::UInt16(builder) => Arc::new(builder.finish()),
            Self::UInt32(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Utf8(sink) => sink.finish_utf8()?,
            Self::Binary(sink) => sink.finish_binary()?,
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::TimestampMs(builder, timezone) => {
                Arc::new(builder.finish().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampUs(builder, timezone) => {
                Arc::new(builder.finish().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampNs(builder, timezone) => {
                Arc::new(builder.finish().with_timezone_opt(timezone.clone()))
            }
        })
    }
}

struct VariableBytes {
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: Vec<bool>,
    all_valid: bool,
    data_bytes: usize,
}

impl VariableBytes {
    fn new() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
            validity: Vec::new(),
            all_valid: true,
            data_bytes: 0,
        }
    }

    fn allocated_capacity_bytes(&self) -> usize {
        self.offsets
            .capacity()
            .saturating_mul(4)
            .saturating_add(self.values.capacity())
            .saturating_add(self.validity.capacity().div_ceil(8))
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

        let cur_validity_cap = self.validity.capacity();
        let needed_validity = self.validity.len().saturating_add(k);
        let cur_validity_bytes = cur_validity_cap.div_ceil(8);
        let (validity_transient, new_validity_bytes) = if needed_validity > cur_validity_cap {
            let new_bytes = needed_validity.div_ceil(8);
            (cur_validity_bytes.saturating_add(new_bytes), new_bytes)
        } else {
            (cur_validity_bytes, cur_validity_bytes)
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

    fn prepare_append(&mut self, rows: usize, data_bytes: usize) {
        self.offsets.reserve_exact(rows);
        self.validity.reserve_exact(rows);
        self.values.reserve_exact(data_bytes);
    }

    fn append_value(&mut self, payload: Option<&[u8]>) -> Result<(), EngineError> {
        match payload {
            None => {
                self.all_valid = false;
                self.validity.push(false);
            }
            Some(bytes) => {
                self.values.extend_from_slice(bytes);
                self.validity.push(true);
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
        self.offsets.shrink_to_fit();
        self.values.shrink_to_fit();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(std::mem::take(&mut self.offsets)));
        let values = Buffer::from_vec(std::mem::take(&mut self.values));
        let nulls = if self.all_valid {
            None
        } else {
            Some(NullBuffer::from(std::mem::take(&mut self.validity)))
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

fn incoming_physical_bytes(array: &dyn Array, k: usize) -> usize {
    let end = k.min(array.len());
    if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
        return utf8_physical_bytes(end, utf8_range_bytes(utf8, 0, end));
    }
    if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        let mut data = 0_usize;
        for index in 0..end {
            if !binary.is_null(index) {
                data = data.saturating_add(binary.value(index).len());
            }
        }
        return utf8_physical_bytes(end, data);
    }
    if matches!(array.data_type(), arrow_schema::DataType::Null) {
        return end.div_ceil(8);
    }
    let slot = match array.data_type() {
        arrow_schema::DataType::Boolean
        | arrow_schema::DataType::Int8
        | arrow_schema::DataType::UInt8 => 1,
        arrow_schema::DataType::Int16 | arrow_schema::DataType::UInt16 => 2,
        arrow_schema::DataType::Int32
        | arrow_schema::DataType::UInt32
        | arrow_schema::DataType::Float32
        | arrow_schema::DataType::Date32 => 4,
        _ => 8,
    };
    fixed_physical_bytes(end, slot)
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

fn boolean_growth(builder: &BooleanBuilder, k: usize) -> (usize, usize) {
    let cur_items = builder.len();
    let cur_cap = builder.capacity();
    let cur_cap_bytes = cur_cap.div_ceil(8).saturating_mul(2);
    let needed_items = cur_items.saturating_add(k);
    if needed_items > cur_cap {
        let new_cap_bytes = needed_items.div_ceil(8).saturating_mul(2);
        (cur_cap_bytes.saturating_add(new_cap_bytes), new_cap_bytes)
    } else {
        (cur_cap_bytes, cur_cap_bytes)
    }
}

fn primitive_growth<T: arrow_array::ArrowPrimitiveType>(
    builder: &PrimitiveBuilder<T>,
    k: usize,
) -> (usize, usize) {
    let slot = std::mem::size_of::<T::Native>();
    let cur_items = builder.len();
    let cur_cap = builder.capacity();
    let cur_cap_bytes = cur_cap
        .saturating_mul(slot)
        .saturating_add(cur_cap.div_ceil(8));
    let needed_items = cur_items.saturating_add(k);
    if needed_items > cur_cap {
        let new_cap_bytes = needed_items
            .saturating_mul(slot)
            .saturating_add(needed_items.div_ceil(8));
        (cur_cap_bytes.saturating_add(new_cap_bytes), new_cap_bytes)
    } else {
        (cur_cap_bytes, cur_cap_bytes)
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

fn append_bool(builder: &mut BooleanBuilder, array: &dyn Array) -> Result<(), EngineError> {
    let values = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or(EngineError::Ffi)?;
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(values.value(index));
        }
    }
    Ok(())
}

fn append_prim<T: arrow_array::ArrowPrimitiveType>(
    builder: &mut PrimitiveBuilder<T>,
    array: &dyn Array,
) -> Result<(), EngineError> {
    let values = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or(EngineError::Internal("remainder expected primitive array"))?;
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(values.value(index));
        }
    }
    Ok(())
}

fn append_utf8(sink: &mut VariableBytes, array: &dyn Array) -> Result<(), EngineError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return append_utf8_values(sink, values.len(), |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        return append_utf8_values(sink, values.len(), |index| {
            (!values.is_null(index)).then(|| values.value(index).as_bytes())
        });
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        return append_utf8_values(sink, values.len(), |index| {
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

fn append_binary(sink: &mut VariableBytes, array: &dyn Array) -> Result<(), EngineError> {
    let values = array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(EngineError::Ffi)?;
    let data_bytes = (0..values.len())
        .filter(|index| !values.is_null(*index))
        .map(|index| values.value(index).len())
        .fold(0_usize, usize::saturating_add);
    sink.prepare_append(values.len(), data_bytes);
    for index in 0..values.len() {
        if values.is_null(index) {
            sink.append_value(None)?;
        } else {
            sink.append_value(Some(values.value(index)))?;
        }
    }
    Ok(())
}
