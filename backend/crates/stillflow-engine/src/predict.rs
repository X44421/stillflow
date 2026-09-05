use arrow_array::{Array, ListArray, StringArray, StructArray};
use stillflow_core::{
    ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, ScalarValue, MAX_BATCH_BYTES,
};
use stillflow_plan::Rule;

use crate::error::EngineError;
use crate::predict_metrics::{self, CloneSite, RuleKind};
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

    pub(crate) fn to_logical_schema(&self) -> Result<LogicalSchema, EngineError> {
        predict_metrics::record_to_logical_schema();
        let fields = self
            .columns
            .iter()
            .map(|col| {
                LogicalField::new(
                    col.id,
                    col.name.clone(),
                    col.data_type.clone(),
                    col.nullable,
                )
                .map_err(|_| EngineError::Internal("invalid field in predicted schema"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        LogicalSchema::new(fields).map_err(|_| EngineError::Internal("invalid predicted schema"))
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
    predict_metrics::record_predict_probe();
    let _probe_timer = predict_metrics::scoped_timer(predict_metrics::add_predict_wall);
    if k == 0 {
        return Ok(0);
    }
    let mut working = {
        predict_metrics::record_clone_site(CloneSite::WorkingInit);
        schema.clone()
    };
    refresh_source_widths(&mut working, arrays, offset, k)?;
    let mut peak = 0_usize;
    let mut live_before = column_physical_sum(&working, arrays, offset, k)?;
    peak = peak.max(live_before);
    for step in steps {
        match step {
            crate::preflight::CompiledStep::Rules { rules } => {
                for rule in rules {
                    let (temporary, live_after, next) =
                        predict_rule(k, offset, arrays, &working, live_before, rule)?;
                    peak = peak.max(
                        live_before
                            .saturating_add(temporary)
                            .saturating_add(live_after),
                    );
                    working = next;
                    live_before = live_after;
                }
            }
            other => {
                let (temporary, live_after, next) =
                    predict_step(k, offset, arrays, &working, live_before, other)?;
                peak = peak.max(
                    live_before
                        .saturating_add(temporary)
                        .saturating_add(live_after),
                );
                working = next;
                live_before = live_after;
            }
        }
    }
    let predict_export = predict_export_transition(&working, arrays, offset, k)?;
    Ok(peak.max(predict_export))
}

pub(crate) fn largest_feasible_k(
    row_count: usize,
    offset: usize,
    arrays: &[arrow_array::ArrayRef],
    schema: &PredictedSchema,
    steps: &[crate::preflight::CompiledStep],
) -> Result<usize, EngineError> {
    predict_metrics::record_lfk_call();
    let _lfk_timer = predict_metrics::scoped_timer(predict_metrics::add_lfk_wall);
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
    predict_metrics::record_refresh_source_widths();
    let _refresh_timer = predict_metrics::scoped_timer(predict_metrics::add_refresh_wall);
    let mut refreshed = 0_u64;
    for column in &mut schema.columns {
        if let ColumnOrigin::Source { ordinal } = column.origin {
            let array = arrays.get(ordinal).ok_or(EngineError::Ffi)?;
            column.max_value_bytes =
                max_variable_width(array.as_ref(), offset, k, &column.data_type)?;
            refreshed += 1;
        }
    }
    predict_metrics::record_source_columns_refreshed(refreshed);
    Ok(())
}

fn column_physical_sum(
    schema: &PredictedSchema,
    arrays: &[arrow_array::ArrayRef],
    offset: usize,
    k: usize,
) -> Result<usize, EngineError> {
    predict_metrics::record_column_physical_sum(schema.columns.len());
    let _sum_timer = predict_metrics::scoped_timer(predict_metrics::add_sum_wall);
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
    predict_metrics::record_column_physical_bytes();
    match (&column.origin, fixed_slot_bytes(&column.data_type)) {
        (ColumnOrigin::Source { ordinal }, Some(slot)) => Ok(fixed_physical_bytes(k, slot).max(
            slice_validity_bytes(arrays.get(*ordinal).map(|array| array.as_ref()), offset, k),
        )),
        (ColumnOrigin::Source { ordinal }, None) => {
            let array = arrays.get(*ordinal).ok_or(EngineError::Ffi)?;
            nested_or_variable_bytes(array.as_ref(), offset, k, &column.data_type)
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
            predict_metrics::record_clone_site(CloneSite::Project);
            let mut next = working.clone();
            next.columns.retain(|column| columns.contains(&column.id));
            next.columns.sort_by_key(|column| {
                columns
                    .iter()
                    .position(|id| *id == column.id)
                    .unwrap_or(usize::MAX)
            });
            predict_metrics::record_project_full_recompute();
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((0, live_after, next))
        }
        crate::preflight::CompiledStep::Filter { .. } => {
            predict_metrics::record_clone_site(CloneSite::Filter);
            Ok((live_before, live_before, working.clone()))
        }
        crate::preflight::CompiledStep::Rules { rules } => {
            let _ = rules;
            Err(EngineError::Internal(
                "apply-rules must be expanded per rule in predict",
            ))
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
    predict_metrics::record_clone_site(CloneSite::Rule);
    let mut next = working.clone();
    match rule {
        Rule::Rename { column, to } => {
            next.column_mut(*column)?.name = to.clone();
            Ok((0, live_before, next))
        }
        Rule::DropColumn { column } => {
            next.columns.retain(|item| item.id != *column);
            predict_metrics::record_rule_full_recompute(RuleKind::DropColumn);
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((0, live_after, next))
        }
        Rule::Trim { column } => {
            let current = working.column(*column)?;
            if !matches!(current.data_type, LogicalType::Utf8) {
                return Err(EngineError::TypeError("trim requires a utf8 column"));
            }
            let temporary = utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes));
            next.column_mut(*column)?.origin = ColumnOrigin::Derived;
            predict_metrics::record_rule_full_recompute(RuleKind::Trim);
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((temporary, live_after, next))
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
            predict_metrics::record_derive_temp_bytes();
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
            current.origin = ColumnOrigin::Derived;
            match (to, &current.data_type) {
                (ScalarValue::Null, _) => {
                    current.nullable = true;
                    predict_metrics::record_rule_full_recompute(RuleKind::ReplaceLiteral);
                    let live_after = column_physical_sum(&next, arrays, offset, k)?;
                    Ok((k.div_ceil(8), live_after, next))
                }
                (ScalarValue::Utf8(value), LogicalType::Utf8) => {
                    current.max_value_bytes = current.max_value_bytes.max(value.len());
                    let temporary =
                        utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes));
                    predict_metrics::record_rule_full_recompute(RuleKind::ReplaceLiteral);
                    let live_after = column_physical_sum(&next, arrays, offset, k)?;
                    Ok((temporary, live_after, next))
                }
                (ScalarValue::Utf8(_), _) | (_, LogicalType::Binary) => Err(
                    EngineError::TypeError("replace literal is not authorized for this type"),
                ),
                _ => {
                    let temporary = fixed_slot_bytes(&current.data_type)
                        .map(|slot| fixed_physical_bytes(k, slot))
                        .unwrap_or(0);
                    predict_metrics::record_rule_full_recompute(RuleKind::ReplaceLiteral);
                    let live_after = column_physical_sum(&next, arrays, offset, k)?;
                    Ok((temporary, live_after, next))
                }
            }
        }
        Rule::FillNull { column, value } => {
            let current = next.column_mut(*column)?;
            current.origin = ColumnOrigin::Derived;
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
                    predict_metrics::record_rule_full_recompute(RuleKind::FillNull);
                    let live_after = column_physical_sum(&next, arrays, offset, k)?;
                    Ok((temporary, live_after, next))
                }
                _ => {
                    current.nullable = false;
                    let temporary = fixed_slot_bytes(&current.data_type)
                        .map(|slot| fixed_physical_bytes(k, slot))
                        .unwrap_or(0);
                    predict_metrics::record_rule_full_recompute(RuleKind::FillNull);
                    let live_after = column_physical_sum(&next, arrays, offset, k)?;
                    Ok((temporary, live_after, next))
                }
            }
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            let source_type = working.column(*column)?.data_type.clone();
            if matches!(
                (&source_type, data_type),
                (
                    LogicalType::Date32 | LogicalType::Timestamp { .. },
                    LogicalType::Utf8
                )
            ) {
                return Err(EngineError::TypeError(
                    "cast from date32 or timestamp to utf8 is paused",
                ));
            }
            if matches!((&source_type, data_type), (LogicalType::Binary, dt) if dt != &LogicalType::Binary)
                || matches!((&source_type, data_type), (src, LogicalType::Binary) if src != &LogicalType::Binary)
            {
                return Err(EngineError::TypeError(
                    "cast to/from binary is not authorized",
                ));
            }
            let current = next.column_mut(*column)?;
            current.origin = ColumnOrigin::Derived;
            current.data_type = data_type.clone();
            if matches!(on_failure, stillflow_plan::CastFailurePolicy::SetNull) {
                current.nullable = true;
            }
            current.max_value_bytes = match data_type {
                LogicalType::Utf8 => match source_type {
                    LogicalType::Boolean => current.max_value_bytes.max(MAX_BOOL_UTF8_BYTES),
                    LogicalType::Float32 | LogicalType::Float64 => {
                        current.max_value_bytes.max(MAX_FLOAT_UTF8_BYTES)
                    }
                    LogicalType::Utf8 => current.max_value_bytes,
                    _ => current.max_value_bytes.max(MAX_INT_UTF8_BYTES),
                },
                _ => 0,
            };
            let temporary = match fixed_slot_bytes(data_type) {
                Some(slot) => fixed_physical_bytes(k, slot),
                None => utf8_physical_bytes(k, k.saturating_mul(current.max_value_bytes)),
            };
            predict_metrics::record_rule_full_recompute(RuleKind::Cast);
            let live_after = column_physical_sum(&next, arrays, offset, k)?;
            Ok((temporary, live_after, next))
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
            expression: inner,
        } => {
            let schema = working.to_logical_schema()?;
            let src_type = crate::typing::type_check_expr(inner, &schema)?;
            match src_type {
                LogicalType::Boolean => MAX_BOOL_UTF8_BYTES,
                LogicalType::Float32 | LogicalType::Float64 => MAX_FLOAT_UTF8_BYTES,
                LogicalType::Date32 | LogicalType::Timestamp { .. } => {
                    return Err(EngineError::TypeError(
                        "cast from date32 or timestamp to utf8 is paused",
                    ));
                }
                _ => MAX_INT_UTF8_BYTES,
            }
        }
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

fn predict_export_transition(
    schema: &PredictedSchema,
    arrays: &[arrow_array::ArrayRef],
    offset: usize,
    k: usize,
) -> Result<usize, EngineError> {
    let _export_timer = predict_metrics::scoped_timer(predict_metrics::add_export_wall);
    let mut column_byte_calls = 0_usize;
    let mut total_polars_bytes = 0_usize;
    for col in &schema.columns {
        total_polars_bytes =
            total_polars_bytes.saturating_add(column_physical_bytes(col, arrays, offset, k)?);
        column_byte_calls += 1;
    }
    let mut max_transition_peak = total_polars_bytes;
    let mut finished_arrow = 0_usize;
    let mut remaining_polars = total_polars_bytes;
    for col in &schema.columns {
        let col_bytes = column_physical_bytes(col, arrays, offset, k)?;
        column_byte_calls += 1;
        remaining_polars = remaining_polars.saturating_sub(col_bytes);
        let builder_transient = col_bytes.saturating_mul(2);
        let current_transition = remaining_polars
            .saturating_add(finished_arrow)
            .saturating_add(builder_transient);
        max_transition_peak = max_transition_peak.max(current_transition);
        finished_arrow = finished_arrow.saturating_add(col_bytes);
    }
    max_transition_peak = max_transition_peak.max(finished_arrow);
    predict_metrics::record_export_transition(schema.columns.len(), column_byte_calls);
    Ok(max_transition_peak)
}

fn nested_or_variable_bytes(
    array: &dyn Array,
    offset: usize,
    k: usize,
    data_type: &LogicalType,
) -> Result<usize, EngineError> {
    if let Some(slot) = fixed_slot_bytes(data_type) {
        return Ok(fixed_physical_bytes(k, slot));
    }
    match data_type {
        LogicalType::List(inner) => list_physical_bytes(array, offset, k, inner),
        LogicalType::Struct(fields) => struct_physical_bytes(array, offset, k, fields),
        _ => Ok(utf8_physical_bytes(
            k,
            variable_data_bytes(array, offset, k)?,
        )),
    }
}

fn list_physical_bytes(
    array: &dyn Array,
    offset: usize,
    k: usize,
    inner: &LogicalType,
) -> Result<usize, EngineError> {
    predict_metrics::record_list_scan();
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(EngineError::Ffi)?;
    let end = offset.saturating_add(k).min(list.len());
    let offsets = list.value_offsets();
    let child_start = offsets[offset] as usize;
    let child_end = offsets[end] as usize;
    let child_k = child_end.saturating_sub(child_start);
    let child = nested_or_variable_bytes(list.values().as_ref(), child_start, child_k, inner)?;
    Ok(k.div_ceil(8)
        .saturating_add(k.saturating_add(1).saturating_mul(4))
        .saturating_add(child))
}

fn struct_physical_bytes(
    array: &dyn Array,
    offset: usize,
    k: usize,
    fields: &[LogicalField],
) -> Result<usize, EngineError> {
    predict_metrics::record_struct_scan();
    let structure = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(EngineError::Ffi)?;
    let mut total = k.div_ceil(8);
    for (field, child) in fields.iter().zip(structure.columns()) {
        total = total.saturating_add(nested_or_variable_bytes(
            child.as_ref(),
            offset,
            k,
            &field.data_type,
        )?);
    }
    Ok(total)
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
    let mut value_bytes = 0_u64;
    for index in offset..end {
        let width = value_width(array, index);
        value_bytes = value_bytes.saturating_add(width as u64);
        max = max.max(width);
    }
    predict_metrics::record_max_variable_width_scan(end.saturating_sub(offset) as u64, value_bytes);
    Ok(max)
}

fn variable_data_bytes(array: &dyn Array, offset: usize, k: usize) -> Result<usize, EngineError> {
    let end = offset.saturating_add(k).min(array.len());
    if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
        if k == 0 || end <= offset {
            predict_metrics::record_variable_data_bytes(0, 0);
            return Ok(0);
        }
        let offsets = utf8.value_offsets();
        let start = offsets[offset] as usize;
        let stop = offsets[end] as usize;
        let span = stop.saturating_sub(start);
        predict_metrics::record_variable_data_bytes(end.saturating_sub(offset) as u64, span as u64);
        return Ok(span);
    }
    let mut total = 0_usize;
    for index in offset..end {
        total = total.saturating_add(value_width(array, index));
    }
    predict_metrics::record_variable_data_bytes(end.saturating_sub(offset) as u64, total as u64);
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
