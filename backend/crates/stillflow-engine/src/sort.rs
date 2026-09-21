//! Shared stable-ordering foundation (#370 §5).
//!
//! A `Sort` step is positional, not per-row: it must see the whole input before
//! it can emit anything, because any row it released early could be displaced
//! by a later one. This module owns that one materialization.
//!
//! The buffer is bounded and fails closed. It never spills, never emits a
//! partial result, and charges every retained byte through the engine's
//! existing `MemoryTracker` accounting before the allocation is made
//! (`docs/contracts/issue-370-nx-s0-shared-sort-contract.md` §5).

use std::cmp::Ordering;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampNanosecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use stillflow_core::LogicalSchema;
use stillflow_plan::{NullPlacement, SortDirection, SortKey};

use crate::error::EngineError;
use crate::memory::MemoryTracker;
use crate::MAX_OPERATOR_STATE_BYTES;

/// Declared bound on the bytes one sort may buffer (#370 §5.6).
///
/// It reuses the existing `MAX_OPERATOR_STATE_BYTES` engine law rather than
/// inventing a budget, and it widens nothing. It is deliberately conservative:
/// an input larger than this fails closed with `BoundExceeded` instead of
/// taking an uncontracted spill path.
pub const MAX_SORT_INPUT_BYTES: usize = MAX_OPERATOR_STATE_BYTES;

/// Per-row allowance charged while buffering, covering the ordering decision
/// structure that exists at the same time as the payload.
const SORT_ORDER_BYTES_PER_ROW: usize = 8;

/// One resolved ordering key: the payload column index plus its declaration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SortLane {
    pub column_index: usize,
    pub direction: SortDirection,
    pub nulls: NullPlacement,
}

impl SortLane {
    fn compare(&self, left: &RecordBatch, right: &RecordBatch) -> Ordering {
        let left_column = left.column(self.column_index);
        let right_column = right.column(self.column_index);
        let left_null = left_column.is_null(0);
        let right_null = right_column.is_null(0);
        if left_null || right_null {
            return match (left_null, right_null) {
                (true, true) => Ordering::Equal,
                // NULL placement is absolute: it is honoured for both
                // directions and is never flipped by the direction (§4.3).
                (true, false) => placement_ordering(self.nulls, true),
                (false, true) => placement_ordering(self.nulls, false),
                (false, false) => Ordering::Equal,
            };
        }
        let ordering = compare_values(left_column, right_column);
        match self.direction {
            SortDirection::Ascending => ordering,
            SortDirection::Descending => ordering.reverse(),
        }
    }
}

fn placement_ordering(nulls: NullPlacement, left_is_null: bool) -> Ordering {
    match (nulls, left_is_null) {
        (NullPlacement::First, true) | (NullPlacement::Last, false) => Ordering::Less,
        (NullPlacement::First, false) | (NullPlacement::Last, true) => Ordering::Greater,
    }
}

/// Resolves the declared keys against the target schema.
///
/// A key whose column is absent is rejected here rather than at comparison
/// time; its payload type is checked by [`SortBuffer::validate_types`] once the
/// real payload is known.
pub(crate) fn resolve_lanes(
    schema: &LogicalSchema,
    keys: &[SortKey],
) -> Result<Vec<SortLane>, EngineError> {
    let mut lanes = Vec::with_capacity(keys.len());
    for key in keys {
        let column_index = schema
            .fields
            .iter()
            .position(|field| field.id == key.column)
            .ok_or(EngineError::UnknownColumn(key.column))?;
        lanes.push(SortLane {
            column_index,
            direction: key.direction,
            nulls: key.nulls,
        });
    }
    Ok(lanes)
}

/// Buffers every transformed chunk, then yields one stable ordering.
pub(crate) struct SortBuffer {
    lanes: Vec<SortLane>,
    chunks: Vec<RecordBatch>,
    bytes: usize,
    types_checked: bool,
}

impl SortBuffer {
    pub(crate) fn new(lanes: Vec<SortLane>) -> Self {
        Self {
            lanes,
            chunks: Vec::new(),
            bytes: 0,
            types_checked: false,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Test-only admission that skips the memory charge, so comparator and
    /// ordering laws can be exercised without a live tracker budget.
    #[cfg(test)]
    fn push_unmetered(&mut self, batch: RecordBatch) -> Result<(), EngineError> {
        if !self.types_checked {
            check_ordered_types(&batch, &self.lanes)?;
            self.types_checked = true;
        }
        self.chunks.push(batch);
        Ok(())
    }

    /// Admits one transformed chunk, charging its retained bytes before it is
    /// kept. A chunk that would exceed the declared bound fails closed and
    /// releases everything already buffered.
    pub(crate) fn push(
        &mut self,
        batch: RecordBatch,
        tracker: &mut MemoryTracker,
    ) -> Result<(), EngineError> {
        if !self.types_checked {
            check_ordered_types(&batch, &self.lanes)?;
            self.types_checked = true;
        }
        let charge = batch
            .get_array_memory_size()
            .saturating_add(batch.num_rows().saturating_mul(SORT_ORDER_BYTES_PER_ROW));
        if self.bytes.saturating_add(charge) > MAX_SORT_INPUT_BYTES {
            // The declared bound is checked before the state it limits is
            // allocated, and the failure publishes nothing (§5.2).
            self.release(tracker)?;
            return Err(EngineError::BoundExceeded(
                "sort input exceeds MAX_SORT_INPUT_BYTES",
            ));
        }
        tracker.hold_polars(charge)?;
        self.bytes = self.bytes.saturating_add(charge);
        self.chunks.push(batch);
        Ok(())
    }

    /// Performs the single stable ordering and returns the ordered rows plus
    /// the payload bytes the caller must keep charged while it drains them.
    ///
    /// The tie-break is the input position, which is the connector's declared
    /// stream order — never hash-map, partition or thread order (§4.2).
    pub(crate) fn finish(&mut self) -> (Vec<RecordBatch>, usize) {
        let chunks = std::mem::take(&mut self.chunks);
        let payload = chunks
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(0_usize, usize::saturating_add);
        let mut rows: Vec<RecordBatch> = Vec::new();
        for chunk in &chunks {
            for row in 0..chunk.num_rows() {
                rows.push(chunk.slice(row, 1));
            }
        }
        let lanes = &self.lanes;
        rows.sort_by(|left, right| compare_rows(left, right, lanes));
        (rows, payload)
    }

    /// Releases the buffer's own charge. Called on success, cancellation and
    /// failure alike, so no temporary resource outlives the run (§5.5). The
    /// ordered rows returned by [`Self::finish`] remain valid because they are
    /// zero-copy views that the caller has taken charge of.
    pub(crate) fn release(&mut self, tracker: &mut MemoryTracker) -> Result<(), EngineError> {
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.bytes = 0;
        tracker.drop_polars()
    }
}

fn compare_rows(left: &RecordBatch, right: &RecordBatch, lanes: &[SortLane]) -> Ordering {
    for lane in lanes {
        let ordering = lane.compare(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn check_ordered_types(batch: &RecordBatch, lanes: &[SortLane]) -> Result<(), EngineError> {
    for lane in lanes {
        if !is_ordered_arrow_type(batch.column(lane.column_index).data_type()) {
            return Err(EngineError::TypeError(
                "sort key column type is not ordered",
            ));
        }
    }
    Ok(())
}

fn is_ordered_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(_, _)
    )
}

/// Compares row 0 of two single-row payloads of the same ordered type.
fn compare_values(left: &ArrayRef, right: &ArrayRef) -> Ordering {
    match left.data_type() {
        DataType::Int8 => compare_primitive::<Int8Type>(left, right),
        DataType::Int16 => compare_primitive::<Int16Type>(left, right),
        DataType::Int32 => compare_primitive::<Int32Type>(left, right),
        DataType::Int64 => compare_primitive::<Int64Type>(left, right),
        DataType::UInt8 => compare_primitive::<UInt8Type>(left, right),
        DataType::UInt16 => compare_primitive::<UInt16Type>(left, right),
        DataType::UInt32 => compare_primitive::<UInt32Type>(left, right),
        DataType::UInt64 => compare_primitive::<UInt64Type>(left, right),
        // Floats compare through a sign-flipped bit pattern so the order is
        // total and NaN is equal to itself (§4.5).
        DataType::Float32 => compare_total_f32(left, right),
        DataType::Float64 => compare_total_f64(left, right),
        DataType::Date32 => compare_primitive::<Date32Type>(left, right),
        // Every timestamp unit shares one physical i64 representation, so the
        // unit does not affect comparison and no unit metadata is read.
        DataType::Timestamp(_, _) => compare_primitive::<TimestampNanosecondType>(left, right),
        _ => Ordering::Equal,
    }
}

fn compare_primitive<T>(left: &ArrayRef, right: &ArrayRef) -> Ordering
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
{
    let left = left.as_primitive::<T>();
    let right = right.as_primitive::<T>();
    left.value(0)
        .partial_cmp(&right.value(0))
        .unwrap_or(Ordering::Equal)
}

fn compare_total_f64(left: &ArrayRef, right: &ArrayRef) -> Ordering {
    let left = left.as_primitive::<Float64Type>().value(0).to_bits();
    let right = right.as_primitive::<Float64Type>().value(0).to_bits();
    let flip = |bits: u64| {
        if bits & (1_u64 << 63) != 0 {
            !bits
        } else {
            bits | (1_u64 << 63)
        }
    };
    flip(left).cmp(&flip(right))
}

fn compare_total_f32(left: &ArrayRef, right: &ArrayRef) -> Ordering {
    let left = left.as_primitive::<Float32Type>().value(0).to_bits();
    let right = right.as_primitive::<Float32Type>().value(0).to_bits();
    let flip = |bits: u32| {
        if bits & (1_u32 << 31) != 0 {
            !bits
        } else {
            bits | (1_u32 << 31)
        }
    };
    flip(left).cmp(&flip(right))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use stillflow_core::{ColumnId, LogicalField, LogicalSchema, LogicalType};
    use stillflow_plan::{NullPlacement, SortDirection, SortKey};

    use super::*;

    fn column_id(value: u128) -> ColumnId {
        ColumnId::from_uuid(uuid::Uuid::from_u128(value))
    }

    fn key(column: ColumnId, direction: SortDirection, nulls: NullPlacement) -> SortKey {
        SortKey {
            column,
            direction,
            nulls,
        }
    }

    /// One `value` column plus a unique `seq` column so a reordering is visible.
    fn batch(values: &[Option<i64>]) -> RecordBatch {
        let seq: Vec<i64> = (0..values.len() as i64).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("seq", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(seq)) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    fn lanes(direction: SortDirection, nulls: NullPlacement) -> Vec<SortLane> {
        vec![SortLane {
            column_index: 0,
            direction,
            nulls,
        }]
    }

    /// Extracts the `seq` column of the ordered rows, which names the original
    /// input position of each row.
    fn ordered_seq(rows: &[RecordBatch]) -> Vec<i64> {
        rows.iter()
            .map(|row| {
                row.column(1)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0)
            })
            .collect()
    }

    #[test]
    fn equal_keys_keep_their_input_order() {
        let mut buffer = SortBuffer::new(lanes(SortDirection::Ascending, NullPlacement::Last));
        buffer
            .push_unmetered(batch(&[Some(7), Some(7), Some(7), Some(1)]))
            .expect("ordered payload");
        let (rows, _) = buffer.finish();
        assert_eq!(
            ordered_seq(&rows),
            vec![3, 0, 1, 2],
            "the three equal keys must keep input order"
        );
    }

    #[test]
    fn null_placement_is_absolute_for_both_directions() {
        for (direction, nulls, expected) in [
            (
                SortDirection::Ascending,
                NullPlacement::First,
                vec![0, 1, 2, 3],
            ),
            (
                SortDirection::Ascending,
                NullPlacement::Last,
                vec![2, 3, 0, 1],
            ),
            (
                SortDirection::Descending,
                NullPlacement::First,
                vec![0, 1, 3, 2],
            ),
            (
                SortDirection::Descending,
                NullPlacement::Last,
                vec![3, 2, 0, 1],
            ),
        ] {
            let mut buffer = SortBuffer::new(lanes(direction, nulls));
            buffer
                .push_unmetered(batch(&[None, None, Some(1), Some(2)]))
                .expect("ordered payload");
            let (rows, _) = buffer.finish();
            assert_eq!(
                ordered_seq(&rows),
                expected,
                "{direction:?}/{nulls:?} placed NULLs or values wrongly"
            );
        }
    }

    #[test]
    fn an_unordered_payload_type_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef],
        )
        .expect("batch");
        let mut buffer = SortBuffer::new(vec![SortLane {
            column_index: 0,
            direction: SortDirection::Ascending,
            nulls: NullPlacement::Last,
        }]);
        let mut tracker = MemoryTracker::new_preview();
        let error = buffer
            .push(batch, &mut tracker)
            .expect_err("utf8 is not ordered");
        assert!(matches!(error, EngineError::TypeError(_)));
    }

    #[test]
    fn an_input_over_the_declared_bound_fails_closed_and_releases_state() {
        // Wide ordered payloads: 8 Int64 columns over 200k rows is ~12.8 MiB of
        // array storage, comfortably past the 5 MiB declared bound, while the
        // single key column keeps the batch well typed.
        let rows = 200_000_usize;
        let columns: Vec<ArrayRef> = (0..8)
            .map(|_| Arc::new(Int64Array::from(vec![0_i64; rows])) as ArrayRef)
            .collect();
        let fields: Vec<Field> = (0..8)
            .map(|index| Field::new(format!("c{index}"), DataType::Int64, true))
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");
        assert!(
            batch.get_array_memory_size() > MAX_SORT_INPUT_BYTES,
            "the fixture must actually exceed the bound"
        );

        let mut buffer = SortBuffer::new(lanes(SortDirection::Ascending, NullPlacement::Last));
        let mut tracker = MemoryTracker::new_preview();
        let error = buffer
            .push(batch, &mut tracker)
            .expect_err("an input over the bound must fail closed");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
        assert!(
            buffer.is_empty(),
            "a refused input must leave no buffered state behind"
        );
    }

    #[test]
    fn lanes_resolve_against_the_schema_and_reject_absent_columns() {
        let schema = LogicalSchema::new(vec![LogicalField {
            id: column_id(2),
            name: "value".to_owned(),
            data_type: LogicalType::Int64,
            nullable: true,
            metadata: Default::default(),
        }])
        .expect("schema");
        let keys = vec![key(
            column_id(2),
            SortDirection::Ascending,
            NullPlacement::Last,
        )];
        let resolved = resolve_lanes(&schema, &keys).expect("resolved");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].column_index, 0);

        let missing = vec![key(
            column_id(9),
            SortDirection::Ascending,
            NullPlacement::Last,
        )];
        let error = resolve_lanes(&schema, &missing).expect_err("absent column");
        assert!(matches!(error, EngineError::UnknownColumn(_)));
    }
}
