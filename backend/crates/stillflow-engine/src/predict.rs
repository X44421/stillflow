use arrow_array::Array;
use stillflow_core::{ColumnId, Expr, LogicalSchema, LogicalType, ScalarValue, MAX_BATCH_BYTES};
use stillflow_plan::Rule;

use crate::error::EngineError;
use crate::types::fixed_slot_bytes;
use crate::{
    MAX_BOOL_UTF8_BYTES, MAX_FLOAT_UTF8_BYTES, MAX_INT_UTF8_BYTES, UTF8_OFFSET_SLOT_BYTES,
    UTF8_VIEW_SLOT_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnOrigin {
    Source { ordinal: usize },
    Derived,
}

#[derive(Debug, Clone)]
pub(crate) struct PredictedColumn {
    pub id: ColumnId,
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub max_value_bytes: usize,
    pub origin: ColumnOrigin,
}

#[derive(Debug, Clone)]
pub(crate) struct PredictedSchema {
    columns: Vec<PredictedColumn>,
}

impl PredictedSchema {
    pub(crate) fn from_scan_output(schema: &LogicalSchema) -> Self {
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| PredictedColumn {
                id: field.id,
                name: field.name.clone(),
                data_type: field.data_type.clone(),
                nullable: field.nullable,
                max_value_bytes: 0,
                origin: ColumnOrigin::Source { ordinal },
            })
            .collect();
        Self { columns }
    }

    fn column(&self, id: ColumnId) -> Result<&PredictedColumn, EngineError> {
        self.columns
            .iter()
            .find(|column| column.id == id)
            .ok_or(EngineError::UnknownColumn(id))
    }

    fn column_mut(&mut self, id: ColumnId) -> Result<&mut PredictedColumn, EngineError> {
        self.columns
            .iter_mut()
            .find(|column| column.id == id)
            .ok_or(EngineError::UnknownColumn(id))
    }
}

pub(crate) fn utf8_physical_bytes(k: usize, data_bytes: usize) -> usize {
    data_bytes
        .saturating_add(k.saturating_mul(UTF8_VIEW_SLOT_BYTES))
        .saturating_add(k.saturating_add(1).saturating_mul(UTF8_OFFSET_SLOT_BYTES))
        .saturating_add(k.div_ceil(8))
}

pub(crate) fn fixed_physical_bytes(k: usize, slot: usize) -> usize {
    k.saturating_mul(slot).saturating_add(k.div_ceil(8))
}

pub(crate) fn predict(
    k: usize,
    offset: usize,
    arrays: &[arrow_array::ArrayRef],
    schema: &PredictedSchema,
    steps: &[crate::preflight::CompiledStep],
) -> Result<usize, EngineError> {
    if k == 0 {
        return Ok(0);
    }
    let mut working = schema.clone();
    refresh_source_widths(&mut working, arrays, offset, k)?;
    let mut peak = 0_usize;
    let mut live_before = column_physical_sum(&working, arrays, offset, k)?;
    for step in steps {
        let (temporary, live_after, next) =
            predict_step(k, offset, arrays, &working, live_before, step)?;
        let step_peak = live_before
            .saturating_add(temporary)
            .saturating_add(live_after);
        peak = peak.max(step_peak);
        working = next;
        live_before = live_after;
    }
    Ok(peak.max(live_before))
}

pub(crate) fn largest_feasible_k(
    row_count: usize,
    offset: usize,
    arrays: &[arrow_array::ArrayRef],
    schema: &PredictedSchema,
    steps: &[crate::preflight::CompiledStep],
) -> Result<usize, EngineError> {
    let remaining = row_count.saturating_sub(offset);
    if remaining == 0 {
        return Ok(0);
    }
    if predict(1, offset, arrays, schema, steps)? > MAX_BATCH_BYTES {
        return Err(EngineError::BoundExceeded(
            "single-row predicted expansion exceeds MAX_BATCH_BYTES",
        ));
    }
    let mut low = 1_usize;
    let mut high = remaining;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if predict(mid, offset, arrays, schema, steps)? <= MAX_BATCH_BYTES {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    Ok(low)
}

fn refresh_source_widths(
    schema: &mut PredictedSchema,
    arrays: &[arrow_array::ArrayRef],
    offset: usize,
    k: usize,
) -> Result<(), EngineError> {
    for column in &mut schema.columns {
        if let ColumnOrigin::Source { ordinal } = column.origin {
            let array = arrays.get(ordinal).ok_or(EngineError::Ffi)?;
            column.max_value_bytes =
                max_variable_width(array.as_ref(), offset, k, &column.data_type)?;
        }
    }
    Ok(())
}

fn column_physical_sum(
    schema: &PredictedSchema,
    arrays: &[arrow_array::ArrayRef],
    offset: usize,
    k: usize,
) -> Result<usize, EngineError> {
    let mut total = 0_usize;
    for column in &schema.columns {
        total = total.saturating_add(column_physical_bytes(column, arrays, offset, k)?);
    }
    Ok(total)
}

fn column_physical_bytes(
    column: &PredictedColumn,
    arrays: &[arrow_array::ArrayRef],
    offset: usize,
    k: usize,
) -> Result<usize, EngineError> {
    match (&column.origin, fixed_slot_bytes(&column.data_type)) {
        (ColumnOrigin::Source { ordinal }, Some(slot)) => Ok(fixed_physical_bytes(k, slot).max(
            slice_validity_bytes(arrays.get(*ordinal).map(|array| array.as_ref()), offset, k),
        )),
        (ColumnOrigin::Source { ordinal }, None) => {
            let array = arrays.get(*ordinal).ok_or(EngineError::Ffi)?;
            let data_bytes = variable_data_bytes(array.as_ref(), offset, k)?;
            Ok(utf8_physical_bytes(k, data_bytes))
        }
        (ColumnOrigin::Derived, Some(slot)) => Ok(fixed_physical_bytes(k, slot)),
        (ColumnOrigin::Derived, None) => Ok(utf8_physical_bytes(
            k,
            k.saturating_mul(column.max_value_bytes),
        )),
    }
}

fn predict_step(
    k: usize,
    offset: usize,
    arrays: &[arrow_array::ArrayRef],
    working: &PredictedSchema,
    live_before: usize,
    step: &crate::preflight::CompiledStep,
) -> Result<(usize, usize, PredictedSchema), EngineError> {
    match step {
        crate::preflight::CompiledStep::Project { columns } => {
            let mut next = working.clone();
            next.columns.retain(|column| columns.contains(&column.id));
            next.columns.sort_by_key(|column| {
                columns
                    .iter()
                    .position(|id| *id == column.id)
                    .unwrap_or(usize::MAX)
            });
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((0, live_after, next))
        }
        crate::preflight::CompiledStep::Filter { .. } => {
            Ok((live_before, live_before, working.clone()))
        }
        crate::preflight::CompiledStep::Rules { rules } => {
            let mut next = working.clone();
            let mut temporary = 0_usize;
            let mut live = live_before;
            for rule in rules {
                let (rule_temp, after, updated) =
                    predict_rule(k, offset, arrays, &next, live, rule)?;
                temporary = temporary.max(rule_temp);
                live = after;
                next = updated;
            }
            Ok((temporary, live, next))
        }
    }
}

fn predict_rule(
    k: usize,
    offset: usize,
    arrays: &[arrow_array::ArrayRef],
    working: &PredictedSchema,
    live_before: usize,
    rule: &Rule,
) -> Result<(usize, usize, PredictedSchema), EngineError> {
    let mut next = working.clone();
    match rule {
        Rule::Rename { column, to } => {
            next.column_mut(*column)?.name = to.clone();
            Ok((0, live_before, next))
        }
        Rule::DropColumn { column } => {
            next.columns.retain(|item| item.id != *column);
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((0, live_after, next))
        }
        Rule::Trim { column } => {
            let current = working.column(*column)?;
            if !matches!(current.data_type, LogicalType::Utf8) {
                return Err(EngineError::TypeError("trim requires a utf8 column"));
            }
            let temporary = utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes));
            Ok((temporary, live_before, next))
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            if next
                .columns
                .iter()
                .any(|column| column.id == *id || column.name == *name)
            {
                return Err(EngineError::InvalidPlan(
                    "derived column id or name is not unique",
                ));
            }
            let max_value_bytes = expression_max_value_bytes(working, expression, data_type)?;
            let new_column = PredictedColumn {
                id: *id,
                name: name.clone(),
                data_type: data_type.clone(),
                nullable: *nullable,
                max_value_bytes,
                origin: ColumnOrigin::Derived,
            };
            let temporary = column_physical_bytes(&new_column, arrays, offset, k)?;
            next.columns.push(new_column);
            let live_after = live_before.saturating_add(temporary);
            Ok((temporary, live_after, next))
        }
        Rule::ReplaceLiteral {
            column,
            from: _,
            to,
        } => {
            let current = next.column_mut(*column)?;
            match (to, &current.data_type) {
                (ScalarValue::Null, _) => {
                    current.nullable = true;
                    Ok((k.div_ceil(8), live_before, next))
                }
                (ScalarValue::Utf8(value), LogicalType::Utf8) => {
                    current.max_value_bytes = current.max_value_bytes.max(value.len());
                    let temporary =
                        utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes));
                    Ok((temporary, live_before, next))
                }
                (ScalarValue::Utf8(_), _) | (_, LogicalType::Binary) => Err(
                    EngineError::TypeError("replace literal is not authorized for this type"),
                ),
                _ => Ok((
                    fixed_slot_bytes(&current.data_type)
                        .map(|slot| fixed_physical_bytes(k, slot))
                        .unwrap_or(0),
                    live_before,
                    next,
                )),
            }
        }
        Rule::FillNull { column, value } => {
            let current = next.column_mut(*column)?;
            match (value, &current.data_type) {
                (ScalarValue::Null, _) => {
                    Err(EngineError::TypeError("fill-null value must not be null"))
                }
                (_, LogicalType::Binary) => Err(EngineError::TypeError(
                    "fill-null is not authorized on binary",
                )),
                (ScalarValue::Utf8(value), LogicalType::Utf8) => {
                    current.nullable = false;
                    current.max_value_bytes = current.max_value_bytes.max(value.len());
                    let temporary =
                        utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes));
                    Ok((temporary, live_before, next))
                }
                _ => {
                    current.nullable = false;
                    Ok((
                        fixed_slot_bytes(&current.data_type)
                            .map(|slot| fixed_physical_bytes(k, slot))
                            .unwrap_or(0),
                        live_before,
                        next,
                    ))
                }
            }
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            if matches!(
                (working.column(*column)?.data_type.clone(), data_type),
                (
                    LogicalType::Date32 | LogicalType::Timestamp { .. },
                    LogicalType::Utf8
                )
            ) {
                return Err(EngineError::TypeError(
                    "cast from date32 or timestamp to utf8 is paused",
                ));
            }
            let current = next.column_mut(*column)?;
            current.data_type = data_type.clone();
            if matches!(on_failure, stillflow_plan::CastFailurePolicy::SetNull) {
                current.nullable = true;
            }
            current.max_value_bytes = match data_type {
                LogicalType::Utf8 => current.max_value_bytes.max(MAX_INT_UTF8_BYTES),
                _ => 0,
            };
            let temporary = match fixed_slot_bytes(data_type) {
                Some(slot) => fixed_physical_bytes(k, slot),
                None => utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes)),
            };
            Ok((temporary, live_before, next))
        }
        Rule::FilterRows { .. } => Ok((live_before, live_before, next)),
        Rule::Validate { .. } => Err(EngineError::UnsupportedRule {
            node: uuid::Uuid::nil(),
            kind: "validate",
        }),
        Rule::Deduplicate { .. } => Err(EngineError::UnsupportedRule {
            node: uuid::Uuid::nil(),
            kind: "deduplicate",
        }),
    }
}

fn expression_max_value_bytes(
    working: &PredictedSchema,
    expression: &Expr,
    data_type: &LogicalType,
) -> Result<usize, EngineError> {
    if let Some(slot) = fixed_slot_bytes(data_type) {
        let _ = slot;
        return Ok(0);
    }
    Ok(match expression {
        Expr::Literal(ScalarValue::Utf8(value)) => value.len(),
        Expr::Literal(_) => 0,
        Expr::Column(id) => working.column(*id)?.max_value_bytes,
        Expr::Cast {
            data_type: LogicalType::Utf8,
            ..
        } => match data_type_of_cast_source(working, expression)? {
            LogicalType::Boolean => MAX_BOOL_UTF8_BYTES,
            LogicalType::Float32 | LogicalType::Float64 => MAX_FLOAT_UTF8_BYTES,
            LogicalType::Date32 | LogicalType::Timestamp { .. } => {
                return Err(EngineError::TypeError(
                    "cast from date32 or timestamp to utf8 is paused",
                ));
            }
            _ => MAX_INT_UTF8_BYTES,
        },
        Expr::Cast { .. } => 0,
        Expr::Coalesce { expressions } => {
            let mut width = 0_usize;
            for expr in expressions {
                width = width.max(expression_max_value_bytes(working, expr, data_type)?);
            }
            width
        }
        _ => 0,
    })
}

fn data_type_of_cast_source(
    working: &PredictedSchema,
    expression: &Expr,
) -> Result<LogicalType, EngineError> {
    match expression {
        Expr::Cast { expression, .. } => match expression.as_ref() {
            Expr::Column(id) => Ok(working.column(*id)?.data_type.clone()),
            Expr::Literal(ScalarValue::Boolean(_)) => Ok(LogicalType::Boolean),
            Expr::Literal(ScalarValue::Int64(_)) => Ok(LogicalType::Int64),
            Expr::Literal(ScalarValue::UInt64(_)) => Ok(LogicalType::UInt64),
            Expr::Literal(ScalarValue::Float64(_)) => Ok(LogicalType::Float64),
            Expr::Literal(ScalarValue::Utf8(_)) => Ok(LogicalType::Utf8),
            Expr::Literal(ScalarValue::Null) => Ok(LogicalType::Null),
            other => data_type_of_cast_source(working, other),
        },
        Expr::Column(id) => Ok(working.column(*id)?.data_type.clone()),
        _ => Ok(LogicalType::Utf8),
    }
}

fn max_variable_width(
    array: &dyn Array,
    offset: usize,
    k: usize,
    data_type: &LogicalType,
) -> Result<usize, EngineError> {
    if fixed_slot_bytes(data_type).is_some() {
        return Ok(0);
    }
    let end = offset.saturating_add(k).min(array.len());
    let mut max = 0_usize;
    for index in offset..end {
        max = max.max(value_width(array, index));
    }
    Ok(max)
}

fn variable_data_bytes(array: &dyn Array, offset: usize, k: usize) -> Result<usize, EngineError> {
    let end = offset.saturating_add(k).min(array.len());
    let mut total = 0_usize;
    for index in offset..end {
        total = total.saturating_add(value_width(array, index));
    }
    Ok(total)
}

fn value_width(array: &dyn Array, index: usize) -> usize {
    if array.is_null(index) {
        return 0;
    }
    if let Some(utf8) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        return utf8.value(index).len();
    }
    if let Some(utf8) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        return utf8.value(index).len();
    }
    if let Some(binary) = array.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        return binary.value(index).len();
    }
    if let Some(binary) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
    {
        return binary.value(index).len();
    }
    0
}

fn slice_validity_bytes(array: Option<&dyn Array>, offset: usize, k: usize) -> usize {
    let Some(array) = array else {
        return k.div_ceil(8);
    };
    if array.null_count() == 0 && offset == 0 {
        return 0;
    }
    let _ = offset;
    k.div_ceil(8)
}
