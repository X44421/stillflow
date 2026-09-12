use stillflow_core::{Expr, LogicalSchema, LogicalType};

use crate::error::EngineError;
use stillflow_plan::semantics::{self, ColumnResolver, SemanticError, SemanticKind};

/// The frozen mapping from a shared semantic failure to the engine's error
/// surface. The decision is made once by the shared analyzer; this mapping
/// preserves the engine's current error classes and messages per decision.
pub(crate) fn semantic_error(error: SemanticError) -> EngineError {
    match error.kind() {
        SemanticKind::ShapeInvalid => {
            EngineError::InvalidPlan("expression failed shape validation")
        }
        SemanticKind::ExprBounds => {
            EngineError::BoundExceeded("expression exceeds node or depth limits")
        }
        SemanticKind::UnknownColumn => EngineError::UnknownColumn(
            error
                .column_id()
                .unwrap_or_else(|| stillflow_core::ColumnId::from_uuid(uuid::Uuid::nil())),
        ),
        SemanticKind::NotRequiresBoolean => EngineError::TypeError("predicate must be boolean"),
        SemanticKind::LogicalOperandsMustBeBoolean => {
            EngineError::TypeError("logical operands must be boolean")
        }
        SemanticKind::ContainsPaused => {
            EngineError::TypeError("contains is paused until the regex polars feature is approved")
        }
        SemanticKind::CheckedArithmeticPaused => EngineError::TypeError(
            "checked arithmetic is paused until overflow semantics are implemented",
        ),
        SemanticKind::ListStructPaused => {
            EngineError::TypeError("list and struct execution is paused")
        }
        SemanticKind::TimestampSecondPaused => {
            EngineError::TypeError("timestamp second unit is paused")
        }
        SemanticKind::InvalidLogicalType => EngineError::TypeError("logical type is invalid"),
        SemanticKind::DateToUtf8CastPaused => {
            EngineError::TypeError("cast from date32 or timestamp to utf8 is paused")
        }
        SemanticKind::BinaryCastUnauthorized => {
            EngineError::TypeError("cast to/from binary is not authorized")
        }
        SemanticKind::ComparisonIncomparable => {
            EngineError::TypeError("comparison operands are not comparable")
        }
        SemanticKind::OrderedComparisonIncompatible => {
            EngineError::TypeError("ordered comparison operands are not compatible")
        }
        SemanticKind::OrderedComparisonRequiresNumeric => EngineError::TypeError(
            "ordered comparison requires numeric, date32, or timestamp operands",
        ),
        SemanticKind::CoalesceArmsIncompatible => {
            EngineError::TypeError("coalesce arms are not type-compatible")
        }
        // The rule-level kinds below never reach the typing entry (the
        // engine's rule propagation is the IncrementalSchema layer); the
        // fallback keeps the mapping total without inventing a decision.
        SemanticKind::TrimRequiresUtf8
        | SemanticKind::LiteralIncompatibleWithColumn
        | SemanticKind::BinaryReplaceOnlyNullToNull
        | SemanticKind::FillNullValueMustNotBeNull
        | SemanticKind::FillNullNotAuthorizedOnBinary
        | SemanticKind::DropLastRemainingField
        | SemanticKind::DerivedTypeMismatch
        | SemanticKind::DerivedIdentityNotUnique
        | SemanticKind::DerivedNullabilityNarrower
        | SemanticKind::DerivedFieldInvalid
        | SemanticKind::ProjectionEmpty
        | SemanticKind::ProjectionDuplicate
        | SemanticKind::SchemaRebuildInvalid
        | SemanticKind::RuleNotAdmitted => EngineError::InvalidPlan(error.kind().compile_message()),
    }
}

/// Shared semantic analysis of one expression through the engine's error
/// surface: result type and nullability in one pass (NX-S1, #336).
pub(crate) fn analyze_expr<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    resolver: &R,
) -> Result<(LogicalType, bool), EngineError> {
    semantics::analyze_expr(expr, resolver)
        .map(|analysis| (analysis.data_type, analysis.nullable))
        .map_err(semantic_error)
}

pub(crate) fn type_check_expr(
    expr: &Expr,
    schema: &LogicalSchema,
) -> Result<LogicalType, EngineError> {
    type_check_expr_in(expr, schema)
}

pub(crate) fn type_check_expr_in<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    schema: &R,
) -> Result<LogicalType, EngineError> {
    analyze_expr(expr, schema).map(|(data_type, _)| data_type)
}

pub(crate) fn require_boolean_in<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    schema: &R,
) -> Result<(), EngineError> {
    match type_check_expr_in(expr, schema)? {
        LogicalType::Boolean => Ok(()),
        _ => Err(EngineError::TypeError("predicate must be boolean")),
    }
}

pub(crate) fn reject_paused_expr(expr: &Expr) -> Result<(), EngineError> {
    semantics::capability::reject_paused_capability(expr).map_err(semantic_error)
}

pub(crate) fn reject_paused_type(
    data_type: &stillflow_core::LogicalType,
) -> Result<(), EngineError> {
    semantics::capability::reject_paused_type(data_type).map_err(semantic_error)
}

/// Counts every `Expr::Column` occurrence in an expression tree. Used by the
/// deterministic index-policy estimator (each occurrence is one resolution
/// performed by the expression passes).
pub(crate) fn count_expr_column_refs(expr: &Expr) -> usize {
    let mut count = 0_usize;
    let mut pending = vec![expr];
    while let Some(current) = pending.pop() {
        match current {
            Expr::Column(_) => count += 1,
            Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => pending.push(expression),
            Expr::Binary { left, right, .. } => {
                pending.push(left);
                pending.push(right);
            }
            Expr::Coalesce { expressions } => pending.extend(expressions.iter()),
        }
    }
    count
}
