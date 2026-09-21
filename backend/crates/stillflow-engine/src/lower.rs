use polars::prelude::{
    col, lit, when, Column, DataFrame, DataType, Expr as PolarsExpr, GetOutput, IntoLazy,
    LazyFrame, Series, StringChunked, StrptimeOptions, NULL,
};
use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, ScalarValue,
    UnaryOperator,
};
use stillflow_plan::{CastFailurePolicy, Rule};

use crate::error::EngineError;
use crate::preflight::CompiledStep;
use crate::types::polars_data_type;

/// A rule whose expression can fail *at execution time* rather than at plan
/// build time, recorded in plan order.
///
/// Collapsing the chunk into one `collect()` means a single Polars failure can
/// no longer be attributed by the collector that raised it. These checkpoints
/// restore the old attribution: the earliest checkpoint that could have failed
/// is the one reported, which matches the previous step-by-step behaviour where
/// the first failing step reported first.
#[derive(Debug, Clone, Copy)]
enum RuleFailure {
    Cast { column: ColumnId },
    ParseTemporal { column: ColumnId },
    DeriveColumn,
    NormalizeText,
}

/// Lowers one chunk's steps into **one** Polars lazy plan and collects it once.
///
/// Before this, every step built its own `.lazy().<one op>().collect()`, so the
/// Polars optimizer only ever saw a single operator at a time and could not
/// apply predicate/projection pushdown, expression simplification or common
/// subplan elimination across the chunk's steps. Building the whole chunk plan
/// and collecting once lets the optimizer see all of them.
///
/// The lazy build is incremental but the plan is a single DAG: each
/// `with_column` appends to that plan, so post-optimization semantics are
/// sequence-equivalent to the previous step-by-step execution.
///
/// Scope is deliberately one chunk. The engine's bounded-batch memory model
/// (`MAX_LIVE_COLUMNAR_PAYLOADS`, `MAX_BATCH_BYTES`, `MAX_ENGINE_PEAK_BYTES`)
/// and its `MemoryTracker` accounting are unchanged: this does not introduce a
/// whole-dataset `LazyFrame` and does not hand memory ownership to Polars
/// streaming.
pub(crate) fn transform(
    frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
    deferred_in: Vec<(String, ScalarValue)>,
) -> Result<(DataFrame, Vec<(String, ScalarValue)>), EngineError> {
    let mut lazy = frame.lazy();
    let mut schema = schema.clone();
    let mut deferred = deferred_in;
    let mut checkpoints: Vec<RuleFailure> = Vec::new();
    for step in steps {
        match step {
            CompiledStep::Project { columns } => {
                let names = names_for(&schema, columns)?;
                lazy = lazy.select(
                    names
                        .iter()
                        .map(|name| col(name.as_str()))
                        .collect::<Vec<_>>(),
                );
                deferred.retain(|(name, _)| names.iter().any(|keep| keep == name));
                schema = crate::preflight::project_schema(&schema, columns)?;
            }
            CompiledStep::Filter { predicate } => {
                let expr = lower_expr(predicate, &schema)?;
                lazy = lazy.filter(expr);
            }
            CompiledStep::Rules { rules } => {
                for rule in rules {
                    lazy = apply_rule(lazy, &mut schema, &mut deferred, rule, &mut checkpoints)?;
                }
            }
            // #370 §5: a positional cross-batch step has no per-chunk
            // lowering. It is resolved once, after the whole input has been
            // consumed, by `preview`'s sort buffer.
            CompiledStep::Sort { .. } => {}
        }
    }
    // Measurement-only: one gather per chunk after consolidation.
    crate::chunk_metrics::record_chunk_gather();
    let frame = lazy.collect().map_err(|_| chunk_failure(&checkpoints))?;
    Ok((frame, deferred))
}

/// Maps a single chunk-wide Polars failure back to the rule class that most
/// likely raised it, preserving the pre-consolidation error contract.
fn chunk_failure(checkpoints: &[RuleFailure]) -> EngineError {
    match checkpoints.first() {
        Some(RuleFailure::Cast { column }) | Some(RuleFailure::ParseTemporal { column }) => {
            EngineError::CastFailure {
                column: *column,
                sequence: 0,
                row: 0,
            }
        }
        Some(RuleFailure::DeriveColumn) => EngineError::TypeError("derive-column failed"),
        Some(RuleFailure::NormalizeText) => EngineError::TypeError("text normalization failed"),
        // No rule could fail at execution time, so the failure came from a
        // filter or projection expression.
        None => EngineError::TypeError("chunk evaluation failed"),
    }
}

fn apply_rule(
    frame: LazyFrame,
    schema: &mut LogicalSchema,
    deferred: &mut Vec<(String, ScalarValue)>,
    rule: &Rule,
    checkpoints: &mut Vec<RuleFailure>,
) -> Result<LazyFrame, EngineError> {
    match rule {
        Rule::Rename { column, to } => {
            let from = field_name(schema, *column)?;
            for (name, _) in deferred.iter_mut() {
                if name == &from {
                    *name = to.clone();
                }
            }
            schema
                .rename_column(*column, to.clone())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            Ok(frame.rename([from.as_str()], [to.as_str()], true))
        }
        Rule::DropColumn { column } => {
            let name = field_name(schema, *column)?;
            deferred.retain(|(deferred_name, _)| deferred_name != &name);
            let keep: Vec<ColumnId> = schema
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .map(|field| field.id)
                .collect();
            *schema = crate::preflight::project_schema(schema, &keep)?;
            Ok(frame.drop([name.as_str()]))
        }
        Rule::Trim { column } => {
            let name = field_name(schema, *column)?;
            Ok(frame.with_column(
                col(name.as_str())
                    .str()
                    .strip_chars(lit(NULL))
                    .alias(name.as_str()),
            ))
        }
        Rule::NormalizeText { column, operation } => {
            let name = field_name(schema, *column)?;
            let operation = *operation;
            checkpoints.push(RuleFailure::NormalizeText);
            Ok(frame.with_column(
                col(name.as_str())
                    .map(
                        move |column: Column| {
                            let values = column.str()?;
                            let normalized: StringChunked = values
                                .into_iter()
                                .map(|value| {
                                    value.map(|text| {
                                        crate::text_normalize::normalize(text, operation)
                                    })
                                })
                                .collect();
                            let series: Series = normalized.into();
                            Ok(Some(series.into()))
                        },
                        GetOutput::from_type(DataType::String),
                    )
                    .alias(name.as_str()),
            ))
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            let dtype = polars_data_type(data_type)?;
            let derived = match expression {
                // The literal arms keep their original typing — a NULL literal
                // still becomes a typed NULL column and a Utf8/Null literal is
                // still routed through the deferred export path — but they no
                // longer read `frame.height()`. A broadcast literal has the
                // input's height by construction, which is what removes the
                // last thing forcing this chunk to materialize early.
                Expr::Literal(value)
                    if matches!(data_type, LogicalType::Utf8)
                        && matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null) =>
                {
                    deferred.push((name.clone(), value.clone()));
                    frame.with_column(lit(NULL).cast(dtype).alias(name.as_str()))
                }
                Expr::Literal(ScalarValue::Null) => {
                    frame.with_column(lit(NULL).cast(dtype).alias(name.as_str()))
                }
                Expr::Literal(value) => {
                    frame.with_column(literal(value)?.cast(dtype.clone()).alias(name.as_str()))
                }
                _ => {
                    let expr = lower_expr(expression, schema)?;
                    checkpoints.push(RuleFailure::DeriveColumn);
                    frame.with_column(expr.cast(dtype).alias(name.as_str()))
                }
            };
            let mut fields = schema.fields.clone();
            fields.push(
                LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                    .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?,
            );
            *schema = LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("derive produced an invalid schema"))?;
            Ok(derived)
        }
        Rule::ReplaceLiteral { column, from, to } => {
            let name = field_name(schema, *column)?;
            let expr = match from {
                ScalarValue::Null => col(name.as_str()).fill_null(literal(to)?),
                _ => when(col(name.as_str()).eq(literal(from)?))
                    .then(literal(to)?)
                    .otherwise(col(name.as_str())),
            };
            Ok(frame.with_column(expr.alias(name.as_str())))
        }
        Rule::FillNull { column, value } => {
            let name = field_name(schema, *column)?;
            Ok(frame.with_column(
                col(name.as_str())
                    .fill_null(literal(value)?)
                    .alias(name.as_str()),
            ))
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            let name = field_name(schema, *column)?;
            let dtype = polars_data_type(data_type)?;
            let expr = if matches!(on_failure, CastFailurePolicy::Error) {
                // Only a strict cast can fail at execution time; `setNull`
                // maps the failure to NULL and never raises.
                checkpoints.push(RuleFailure::Cast { column: *column });
                col(name.as_str()).strict_cast(dtype.clone())
            } else {
                col(name.as_str()).cast(dtype)
            };
            Ok(frame.with_column(expr.alias(name.as_str())))
        }
        Rule::ParseTemporal {
            column,
            data_type,
            on_failure,
            format,
        } => {
            let name = field_name(schema, *column)?;
            let target = polars_data_type(data_type)?;
            // Parse the wall time naively: the declared format never implies a
            // timezone, and no offset is inferred from the environment.
            let parse_dtype = match &target {
                DataType::Datetime(unit, _) => DataType::Datetime(*unit, None),
                other => other.clone(),
            };
            let options = StrptimeOptions {
                format: Some(format.as_str().into()),
                // `error` rejects an unparseable value; `setNull` maps it to
                // NULL. `exact` keeps the match anchored to the whole string so
                // a partial match never silently succeeds.
                strict: matches!(on_failure, CastFailurePolicy::Error),
                exact: true,
                cache: true,
            };
            if matches!(on_failure, CastFailurePolicy::Error) {
                checkpoints.push(RuleFailure::ParseTemporal { column: *column });
            }
            Ok(frame.with_column(
                col(name.as_str())
                    .str()
                    .strptime(parse_dtype, options, lit("raise"))
                    .alias(name.as_str()),
            ))
        }
        Rule::FilterRows { predicate } => {
            let expr = lower_expr(predicate, schema)?;
            Ok(frame.filter(expr))
        }
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

fn names_for(schema: &LogicalSchema, columns: &[ColumnId]) -> Result<Vec<String>, EngineError> {
    columns.iter().map(|id| field_name(schema, *id)).collect()
}

fn field_name(schema: &LogicalSchema, id: ColumnId) -> Result<String, EngineError> {
    schema
        .field(id)
        .map(|field| field.name.clone())
        .ok_or(EngineError::UnknownColumn(id))
}

pub(crate) fn lower_expr(expr: &Expr, schema: &LogicalSchema) -> Result<PolarsExpr, EngineError> {
    Ok(match expr {
        Expr::Column(id) => col(field_name(schema, *id)?),
        Expr::Literal(value) => literal(value)?,
        Expr::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => lower_expr(expression, schema)?.not(),
        Expr::Unary {
            operator: UnaryOperator::Negate,
            ..
        } => {
            return Err(EngineError::TypeError(
                "checked arithmetic is paused until overflow semantics are implemented",
            ));
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => {
            let left_type = crate::typing::type_check_expr(left, schema)?;
            let right_type = crate::typing::type_check_expr(right, schema)?;
            let mut left_expr = lower_expr(left, schema)?;
            let mut right_expr = lower_expr(right, schema)?;
            if left_type != right_type {
                let lub = left_type
                    .least_upper_bound(&right_type)
                    .map_err(|_| EngineError::TypeError("incompatible binary operand types"))?;
                if left_type != lub {
                    let lub_dtype = polars_data_type(&lub)?;
                    left_expr = left_expr.strict_cast(lub_dtype);
                }
                if right_type != lub {
                    let lub_dtype = polars_data_type(&lub)?;
                    right_expr = right_expr.strict_cast(lub_dtype);
                }
            }
            match operator {
                BinaryOperator::Equal => left_expr.eq(right_expr),
                BinaryOperator::NotEqual => left_expr.neq(right_expr),
                BinaryOperator::LessThan => left_expr.lt(right_expr),
                BinaryOperator::LessThanOrEqual => left_expr.lt_eq(right_expr),
                BinaryOperator::GreaterThan => left_expr.gt(right_expr),
                BinaryOperator::GreaterThanOrEqual => left_expr.gt_eq(right_expr),
                BinaryOperator::And => left_expr.and(right_expr),
                BinaryOperator::Or => left_expr.or(right_expr),
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    return Err(EngineError::TypeError(
                        "checked arithmetic is paused until overflow semantics are implemented",
                    ));
                }
                BinaryOperator::Contains => {
                    // A literal substring test: the right operand is a value,
                    // never a pattern, so no regex feature is involved.
                    left_expr.str().contains_literal(right_expr)
                }
            }
        }
        Expr::IsNull {
            expression,
            negated,
        } => {
            let inner = lower_expr(expression, schema)?;
            if *negated {
                inner.is_not_null()
            } else {
                inner.is_null()
            }
        }
        Expr::Cast {
            expression,
            data_type,
        } => lower_expr(expression, schema)?.strict_cast(polars_data_type(data_type)?),
        Expr::Substring {
            expression,
            start,
            length,
        } => lower_expr(expression, schema)?
            .str()
            // Polars `slice` is 0-based over characters and clamps
            // out-of-range requests, which is the contract's §3.3 wording.
            // `start >= 1` is enforced by `Expr::validate_shape`, so the
            // subtraction cannot underflow.
            .slice(lit((*start - 1) as i64), lit(i64::from(*length))),
        Expr::Conditional {
            predicate,
            then,
            otherwise,
        } => when(lower_expr(predicate, schema)?)
            .then(lower_expr(then, schema)?)
            .otherwise(lower_expr(otherwise, schema)?),
        Expr::Concat { expressions } => {
            // Ordered Utf8 concatenation (#368 §3.1). Polars propagates NULL
            // through `+` on strings, which is exactly the contract's NULL law.
            let mut iter = expressions.iter();
            let first = iter
                .next()
                .ok_or(EngineError::InvalidPlan("concat requires operands"))?;
            let mut concatenated = lower_expr(first, schema)?;
            for expression in iter {
                concatenated = concatenated + lower_expr(expression, schema)?;
            }
            concatenated
        }
        Expr::Coalesce { expressions } => {
            if expressions.is_empty() {
                return Ok(lit(NULL));
            }
            let target_lub = crate::typing::type_check_expr(expr, schema)?;
            let target_dtype = polars_data_type(&target_lub)?;
            let mut lowered = Vec::new();
            for e in expressions {
                let arm_type = crate::typing::type_check_expr(e, schema)?;
                let mut arm_expr = lower_expr(e, schema)?;
                if arm_type != target_lub {
                    arm_expr = arm_expr.strict_cast(target_dtype.clone());
                }
                lowered.push(arm_expr);
            }
            coalesce_exprs(lowered)
        }
    })
}

fn coalesce_exprs(mut exprs: Vec<PolarsExpr>) -> PolarsExpr {
    let Some(first) = exprs.pop() else {
        return lit(NULL);
    };
    exprs.into_iter().rev().fold(first, |tail, head| {
        when(head.clone().is_not_null()).then(head).otherwise(tail)
    })
}

fn literal(value: &ScalarValue) -> Result<PolarsExpr, EngineError> {
    Ok(match value {
        ScalarValue::Null => lit(NULL),
        ScalarValue::Boolean(value) => lit(*value),
        ScalarValue::Int64(value) => lit(*value),
        ScalarValue::UInt64(value) => lit(*value),
        ScalarValue::Float64(value) => lit(value.get()),
        ScalarValue::Utf8(value) => lit(value.clone()),
    })
}
