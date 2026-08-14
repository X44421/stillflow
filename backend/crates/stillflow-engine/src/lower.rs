use polars::prelude::{col, lit, when, DataFrame, Expr as PolarsExpr, IntoLazy, NULL};
use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, ScalarValue, UnaryOperator,
};
use stillflow_plan::{CastFailurePolicy, Rule};

use crate::error::EngineError;
use crate::preflight::CompiledStep;
use crate::types::polars_data_type;

pub(crate) fn transform(
    frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
) -> Result<DataFrame, EngineError> {
    let mut frame = frame;
    let mut schema = schema.clone();
    for step in steps {
        match step {
            CompiledStep::Project { columns } => {
                let names = names_for(&schema, columns)?;
                frame = frame
                    .select(names.iter().map(String::as_str))
                    .map_err(|_| EngineError::UnknownColumn(columns[0]))?;
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
            CompiledStep::Rules { rules } => {
                for rule in rules {
                    frame = apply_rule(frame, &mut schema, rule)?;
                }
            }
        }
    }
    Ok(frame)
}

fn apply_rule(
    frame: DataFrame,
    schema: &mut LogicalSchema,
    rule: &Rule,
) -> Result<DataFrame, EngineError> {
    match rule {
        Rule::Rename { column, to } => {
            let from = field_name(schema, *column)?;
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
            let expr = lower_expr(expression, schema)?;
            let dtype = polars_data_type(data_type)?;
            let derived = frame
                .lazy()
                .with_column(expr.cast(dtype).alias(name.as_str()))
                .collect()
                .map_err(|_| EngineError::TypeError("derive-column failed"))?;
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
            expression,
        } => {
            let inner = lower_expr(expression, schema)?;
            lit(0) - inner
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => {
            let left = lower_expr(left, schema)?;
            let right = lower_expr(right, schema)?;
            match operator {
                BinaryOperator::Equal => left.eq(right),
                BinaryOperator::NotEqual => left.neq(right),
                BinaryOperator::LessThan => left.lt(right),
                BinaryOperator::LessThanOrEqual => left.lt_eq(right),
                BinaryOperator::GreaterThan => left.gt(right),
                BinaryOperator::GreaterThanOrEqual => left.gt_eq(right),
                BinaryOperator::And => left.and(right),
                BinaryOperator::Or => left.or(right),
                BinaryOperator::Add => left + right,
                BinaryOperator::Subtract => left - right,
                BinaryOperator::Multiply => left * right,
                BinaryOperator::Divide => left / right,
                BinaryOperator::Modulo => left % right,
                BinaryOperator::Contains => left.str().contains_literal(right),
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
            let mut lowered = Vec::new();
            for expr in expressions {
                lowered.push(lower_expr(expr, schema)?);
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
