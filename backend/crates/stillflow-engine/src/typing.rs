use stillflow_core::{
    BinaryOperator, Expr, LogicalSchema, LogicalType, ScalarValue, TimeUnit, UnaryOperator,
};

use crate::error::EngineError;

pub(crate) fn type_check_expr(
    expr: &Expr,
    schema: &LogicalSchema,
) -> Result<LogicalType, EngineError> {
    expr.validate_shape()
        .map_err(|_| EngineError::InvalidPlan("expression failed shape validation"))?;
    infer_type(expr, schema)
}

pub(crate) fn require_boolean(expr: &Expr, schema: &LogicalSchema) -> Result<(), EngineError> {
    match type_check_expr(expr, schema)? {
        LogicalType::Boolean => Ok(()),
        _ => Err(EngineError::TypeError("predicate must be boolean")),
    }
}

pub(crate) fn reject_paused_expr(expr: &Expr) -> Result<(), EngineError> {
    match expr {
        Expr::Unary {
            operator: UnaryOperator::Negate,
            ..
        } => Err(EngineError::TypeError(
            "checked arithmetic is paused until overflow semantics are implemented",
        )),
        Expr::Unary { expression, .. } | Expr::IsNull { expression, .. } => {
            reject_paused_expr(expression)
        }
        Expr::Cast {
            expression,
            data_type,
        } => {
            reject_paused_type(data_type)?;
            reject_paused_expr(expression)
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => {
            match operator {
                BinaryOperator::Contains => {
                    return Err(EngineError::TypeError(
                        "contains is paused until the regex polars feature is approved",
                    ));
                }
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    return Err(EngineError::TypeError(
                        "checked arithmetic is paused until overflow semantics are implemented",
                    ));
                }
                _ => {}
            }
            reject_paused_expr(left)?;
            reject_paused_expr(right)
        }
        Expr::Coalesce { expressions } => {
            for nested in expressions {
                reject_paused_expr(nested)?;
            }
            Ok(())
        }
        Expr::Column(_) | Expr::Literal(_) => Ok(()),
    }
}

pub(crate) fn reject_paused_type(data_type: &LogicalType) -> Result<(), EngineError> {
    match data_type {
        LogicalType::List(_) | LogicalType::Struct(_) => Err(EngineError::TypeError(
            "list and struct execution is paused",
        )),
        LogicalType::Timestamp {
            unit: TimeUnit::Second,
            ..
        } => Err(EngineError::TypeError("timestamp second unit is paused")),
        _ => data_type
            .validate()
            .map_err(|_| EngineError::TypeError("logical type is invalid")),
    }
}

fn infer_type(expr: &Expr, schema: &LogicalSchema) -> Result<LogicalType, EngineError> {
    match expr {
        Expr::Column(id) => {
            let field = schema.field(*id).ok_or(EngineError::UnknownColumn(*id))?;
            reject_paused_type(&field.data_type)?;
            Ok(field.data_type.clone())
        }
        Expr::Literal(ScalarValue::Boolean(_)) => Ok(LogicalType::Boolean),
        Expr::Literal(ScalarValue::Int64(_)) => Ok(LogicalType::Int64),
        Expr::Literal(ScalarValue::UInt64(_)) => Ok(LogicalType::UInt64),
        Expr::Literal(ScalarValue::Float64(_)) => Ok(LogicalType::Float64),
        Expr::Literal(ScalarValue::Utf8(_)) => Ok(LogicalType::Utf8),
        Expr::Literal(ScalarValue::Null) => Ok(LogicalType::Null),
        Expr::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => {
            require_boolean(expression, schema)?;
            Ok(LogicalType::Boolean)
        }
        Expr::Unary {
            operator: UnaryOperator::Negate,
            ..
        } => Err(EngineError::TypeError(
            "checked arithmetic is paused until overflow semantics are implemented",
        )),
        Expr::IsNull { expression, .. } => {
            let _ = infer_type(expression, schema)?;
            Ok(LogicalType::Boolean)
        }
        Expr::Cast {
            expression,
            data_type,
        } => {
            let from = infer_type(expression, schema)?;
            reject_paused_type(data_type)?;
            if matches!(from, LogicalType::Date32 | LogicalType::Timestamp { .. })
                && matches!(data_type, LogicalType::Utf8)
            {
                return Err(EngineError::TypeError(
                    "cast from date32 or timestamp to utf8 is paused",
                ));
            }
            if matches!(data_type, LogicalType::Binary) && !matches!(from, LogicalType::Binary) {
                return Err(EngineError::TypeError("cast to binary is not authorized"));
            }
            Ok(data_type.clone())
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => infer_binary(*operator, left, right, schema),
        Expr::Coalesce { expressions } => {
            if expressions.is_empty() {
                return Err(EngineError::InvalidPlan("coalesce is empty"));
            }
            let mut joined = infer_type(&expressions[0], schema)?;
            for expr in expressions.iter().skip(1) {
                let next = infer_type(expr, schema)?;
                joined = joined
                    .least_upper_bound(&next)
                    .map_err(|_| EngineError::TypeError("coalesce arms are not type-compatible"))?;
            }
            reject_paused_type(&joined)?;
            Ok(joined)
        }
    }
}

fn infer_binary(
    operator: BinaryOperator,
    left: &Expr,
    right: &Expr,
    schema: &LogicalSchema,
) -> Result<LogicalType, EngineError> {
    let left_type = infer_type(left, schema)?;
    let right_type = infer_type(right, schema)?;
    match operator {
        BinaryOperator::And | BinaryOperator::Or => {
            if !matches!(left_type, LogicalType::Boolean)
                || !matches!(right_type, LogicalType::Boolean)
            {
                return Err(EngineError::TypeError("logical operands must be boolean"));
            }
            Ok(LogicalType::Boolean)
        }
        BinaryOperator::Contains => Err(EngineError::TypeError(
            "contains is paused until the regex polars feature is approved",
        )),
        BinaryOperator::Add
        | BinaryOperator::Subtract
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => Err(EngineError::TypeError(
            "checked arithmetic is paused until overflow semantics are implemented",
        )),
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            comparable_pair(&left_type, &right_type)?;
            Ok(LogicalType::Boolean)
        }
        BinaryOperator::LessThan
        | BinaryOperator::LessThanOrEqual
        | BinaryOperator::GreaterThan
        | BinaryOperator::GreaterThanOrEqual => {
            ordered_pair(&left_type, &right_type)?;
            Ok(LogicalType::Boolean)
        }
    }
}

fn comparable_pair(left: &LogicalType, right: &LogicalType) -> Result<(), EngineError> {
    if left.least_upper_bound(right).is_ok() {
        return Ok(());
    }
    Err(EngineError::TypeError(
        "comparison operands are not comparable",
    ))
}

fn ordered_pair(left: &LogicalType, right: &LogicalType) -> Result<(), EngineError> {
    let joined = left
        .least_upper_bound(right)
        .map_err(|_| EngineError::TypeError("ordered comparison operands are not compatible"))?;
    match joined {
        LogicalType::Int8
        | LogicalType::Int16
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::UInt8
        | LogicalType::UInt16
        | LogicalType::UInt32
        | LogicalType::UInt64
        | LogicalType::Float32
        | LogicalType::Float64
        | LogicalType::Date32
        | LogicalType::Timestamp { .. } => {
            reject_paused_type(&joined)?;
            Ok(())
        }
        _ => Err(EngineError::TypeError(
            "ordered comparison requires numeric, date32, or timestamp operands",
        )),
    }
}
