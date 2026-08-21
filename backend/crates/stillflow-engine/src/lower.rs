use polars::prelude::{col, lit, when, DataFrame, Expr as PolarsExpr, IntoLazy, NULL};
use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, ScalarValue,
    UnaryOperator,
};
use stillflow_plan::{CastFailurePolicy, Rule};

use crate::error::EngineError;
use crate::preflight::CompiledStep;
use crate::types::polars_data_type;

pub(crate) fn transform(
    frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
) -> Result<(DataFrame, Vec<(String, ScalarValue)>), EngineError> {
    let mut frame = frame;
    let mut schema = schema.clone();
    let mut deferred = Vec::new();
    for step in steps {
        match step {
            CompiledStep::Project { columns } => {
                let names = names_for(&schema, columns)?;
                frame = frame
                    .select(names.iter().map(String::as_str))
                    .map_err(|_| EngineError::UnknownColumn(columns[0]))?;
                deferred.retain(|(name, _)| names.iter().any(|keep| keep == name));
                schema = crate::preflight::project_schema(&schema, columns)?;
            }
            CompiledStep::Filter { predicate } => {
                let expr = lower_expr(predicate, &schema)?;
                frame = frame
                    .lazy()
                    .filter(expr)
                    .collect()
                    .map_err(|_| EngineError::TypeError("filter evaluation failed"))?;
            }
            CompiledStep::Rules { rules, .. } => {
                for rule in rules {
                    frame = apply_rule(frame, &mut schema, &mut deferred, rule)?;
                }
            }
        }
    }
    Ok((frame, deferred))
}

pub(crate) fn apply_rule(
    frame: DataFrame,
    schema: &mut LogicalSchema,
    deferred: &mut Vec<(String, ScalarValue)>,
    rule: &Rule,
) -> Result<DataFrame, EngineError> {
    match rule {
        Rule::Rename { column, to } => {
            let from = field_name(schema, *column)?;
            for (name, _) in deferred.iter_mut() {
                if name == &from {
                    *name = to.clone();
                }
            }
            let renamed = frame
                .lazy()
                .rename([from.as_str()], [to.as_str()], true)
                .collect()
                .map_err(|_| EngineError::Internal("rename failed"))?;
            schema
                .rename_column(*column, to.clone())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            Ok(renamed)
        }
        Rule::DropColumn { column } => {
            let name = field_name(schema, *column)?;
            deferred.retain(|(deferred_name, _)| deferred_name != &name);
            let dropped = frame
                .drop(name.as_str())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            let keep: Vec<ColumnId> = schema
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .map(|field| field.id)
                .collect();
            *schema = crate::preflight::project_schema(schema, &keep)?;
            Ok(dropped)
        }
        Rule::Trim { column } => {
            let name = field_name(schema, *column)?;
            frame
                .lazy()
                .with_column(
                    col(name.as_str())
                        .str()
                        .strip_chars(lit(NULL))
                        .alias(name.as_str()),
                )
                .collect()
                .map_err(|_| EngineError::TypeError("trim failed"))
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            let derived = match expression {
                Expr::Literal(value)
                    if matches!(data_type, LogicalType::Utf8)
                        && matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null) =>
                {
                    let height = frame.height();
                    let mut derived = frame;
                    let dtype = polars_data_type(data_type)?;
                    derived
                        .with_column(polars::prelude::Column::full_null(
                            name.as_str().into(),
                            height,
                            &dtype,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    deferred.push((name.clone(), value.clone()));
                    derived
                }
                Expr::Literal(ScalarValue::Null) => {
                    let height = frame.height();
                    let mut derived = frame;
                    let dtype = polars_data_type(data_type)?;
                    derived
                        .with_column(polars::prelude::Column::full_null(
                            name.as_str().into(),
                            height,
                            &dtype,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    derived
                }
                Expr::Literal(value) => {
                    let height = frame.height();
                    let mut derived = frame;
                    derived
                        .with_column(polars::prelude::Column::new_scalar(
                            name.as_str().into(),
                            literal_scalar(value)?,
                            height,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    derived
                }
                _ => {
                    let expr = lower_expr(expression, schema)?;
                    let dtype = polars_data_type(data_type)?;
                    frame
                        .lazy()
                        .with_column(expr.cast(dtype).alias(name.as_str()))
                        .collect()
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?
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
            frame
                .lazy()
                .with_column(expr.alias(name.as_str()))
                .collect()
                .map_err(|_| EngineError::TypeError("replace-literal failed"))
        }
        Rule::FillNull { column, value } => {
            let name = field_name(schema, *column)?;
            frame
                .lazy()
                .with_column(
                    col(name.as_str())
                        .fill_null(literal(value)?)
                        .alias(name.as_str()),
                )
                .collect()
                .map_err(|_| EngineError::TypeError("fill-null failed"))
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            let name = field_name(schema, *column)?;
            let dtype = polars_data_type(data_type)?;
            let expr = if matches!(on_failure, CastFailurePolicy::Error) {
                col(name.as_str()).strict_cast(dtype.clone())
            } else {
                col(name.as_str()).cast(dtype)
            };
            frame
                .lazy()
                .with_column(expr.alias(name.as_str()))
                .collect()
                .map_err(|_| EngineError::CastFailure {
                    column: *column,
                    sequence: 0,
                    row: 0,
                })
        }
        Rule::FilterRows { predicate } => {
            let expr = lower_expr(predicate, schema)?;
            frame
                .lazy()
                .filter(expr)
                .collect()
                .map_err(|_| EngineError::TypeError("filter-rows failed"))
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

fn lower_expr(expr: &Expr, schema: &LogicalSchema) -> Result<PolarsExpr, EngineError> {
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
                    return Err(EngineError::TypeError(
                        "contains is paused until the regex polars feature is approved",
                    ));
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

fn literal_scalar(value: &ScalarValue) -> Result<polars::prelude::Scalar, EngineError> {
    use polars::prelude::{AnyValue, DataType, Scalar};
    Ok(match value {
        ScalarValue::Null => Scalar::new(DataType::Null, AnyValue::Null),
        ScalarValue::Boolean(value) => Scalar::from(*value),
        ScalarValue::Int64(value) => Scalar::from(*value),
        ScalarValue::UInt64(value) => Scalar::from(*value),
        ScalarValue::Float64(value) => Scalar::from(value.get()),
        ScalarValue::Utf8(value) => Scalar::from(polars::prelude::PlSmallStr::from(value.as_str())),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateOutcome {
    True,
    False,
    Null,
}

pub(crate) fn predicate_outcomes(
    frame: &DataFrame,
    predicate: &Expr,
    schema: &LogicalSchema,
) -> Result<Vec<PredicateOutcome>, EngineError> {
    use polars::prelude::AnyValue;
    let expr = lower_expr(predicate, schema)?;
    let evaluated = frame
        .clone()
        .lazy()
        .select([expr.alias("__stillflow_e4_pred")])
        .collect()
        .map_err(|_| EngineError::TypeError("validate predicate evaluation failed"))?;
    let column = evaluated
        .column("__stillflow_e4_pred")
        .map_err(|_| EngineError::Internal("validate predicate column missing"))?;
    let mut out = Vec::with_capacity(frame.height());
    for row in 0..frame.height() {
        match column
            .get(row)
            .map_err(|_| EngineError::Internal("validate predicate row"))?
        {
            AnyValue::Boolean(true) => out.push(PredicateOutcome::True),
            AnyValue::Boolean(false) => out.push(PredicateOutcome::False),
            AnyValue::Null => out.push(PredicateOutcome::Null),
            _ => {
                return Err(EngineError::TypeError("validate predicate was not boolean"));
            }
        }
    }
    Ok(out)
}

pub(crate) fn filter_rows(frame: DataFrame, keep: &[bool]) -> Result<DataFrame, EngineError> {
    use polars::prelude::{BooleanChunked, NewChunkedArray};
    let mask = BooleanChunked::from_iter_values("keep".into(), keep.iter().copied());
    frame
        .filter(&mask)
        .map_err(|_| EngineError::Internal("row filter failed"))
}
