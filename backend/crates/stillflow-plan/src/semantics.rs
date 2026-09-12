//! Shared semantic analysis for the NodeGraph compiler and the engine
//! preflight (NX-S1, #336; frozen by the NX-C0 contract §5).
//!
//! This module is the only implementation of these semantics for both entry
//! points:
//!
//! - column identity resolution against the working schema, by `ColumnId`
//!   only, through [`ColumnResolver`];
//! - expression result type and nullability inference ([`analyze_expr`]);
//! - expression shape and reference validation ([`validate_expr_refs`]);
//! - the post-rule schema effect of every `Rule` variant that the product
//!   path admits ([`rule_effect`]) and of a projection ([`project_effect`]);
//! - classification of a semantic failure into the frozen `NG_*` vocabulary
//!   ([`SemanticError::code`]) with the compile path's message
//!   ([`SemanticError::compile_message`]).
//!
//! The capability gate ([`capability`]) answers "is this executable on this
//! head" and stays a separate authority from semantic analysis. The paused
//! set it enforces is frozen behavior (NX-C0 §5.2): semantic success never
//! implies executability, and the engine's runtime gate remains the final
//! authority. This module must not depend on `stillflow-engine`, connector
//! crates, or any physical executor.

use std::collections::BTreeSet;

use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, NodeGraphErrorCode,
    ScalarValue, UnaryOperator, MAX_EXPR_DEPTH, MAX_EXPR_NODES,
};

use crate::rule::{CastFailurePolicy, Rule};

/// Resolution of a `ColumnId` against the working schema. Implemented by
/// [`LogicalSchema`] (the linear authoritative backend) and by the engine's
/// private indexed backends, which return the identical field for any valid
/// schema because validated schemas have unique column ids.
pub trait ColumnResolver {
    fn resolve_column(&self, id: ColumnId) -> Option<&LogicalField>;
}

impl ColumnResolver for LogicalSchema {
    fn resolve_column(&self, id: ColumnId) -> Option<&LogicalField> {
        self.field(id)
    }
}

/// The result of analyzing one expression: its logical type and the
/// nullability of its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprAnalysis {
    pub data_type: LogicalType,
    pub nullable: bool,
}

/// The frozen failure kinds of the shared semantic analysis. Each kind maps
/// to exactly one `NG_*` code and one compile-path message; the engine maps
/// kinds to its own error surface without re-deciding the semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticKind {
    ShapeInvalid,
    ExprBounds,
    UnknownColumn,
    NotRequiresBoolean,
    LogicalOperandsMustBeBoolean,
    ContainsPaused,
    CheckedArithmeticPaused,
    ListStructPaused,
    TimestampSecondPaused,
    InvalidLogicalType,
    DateToUtf8CastPaused,
    BinaryCastUnauthorized,
    ComparisonIncomparable,
    OrderedComparisonIncompatible,
    OrderedComparisonRequiresNumeric,
    CoalesceArmsIncompatible,
    TrimRequiresUtf8,
    LiteralIncompatibleWithColumn,
    BinaryReplaceOnlyNullToNull,
    FillNullValueMustNotBeNull,
    FillNullNotAuthorizedOnBinary,
    DropLastRemainingField,
    DerivedTypeMismatch,
    DerivedIdentityNotUnique,
    DerivedNullabilityNarrower,
    DerivedFieldInvalid,
    ProjectionEmpty,
    ProjectionDuplicate,
    SchemaRebuildInvalid,
    RuleNotAdmitted,
}

impl SemanticKind {
    /// The frozen `NG_*` classification of this failure for the compile path
    /// (NX-C0 §10.4: classes are preserved, only location is added later).
    pub const fn code(self) -> NodeGraphErrorCode {
        match self {
            Self::ShapeInvalid
            | Self::FillNullValueMustNotBeNull
            | Self::DropLastRemainingField
            | Self::DerivedIdentityNotUnique
            | Self::DerivedFieldInvalid
            | Self::ProjectionEmpty
            | Self::ProjectionDuplicate
            | Self::SchemaRebuildInvalid
            | Self::RuleNotAdmitted => NodeGraphErrorCode::InvalidConfig,
            Self::UnknownColumn => NodeGraphErrorCode::UnknownColumn,
            Self::ExprBounds => NodeGraphErrorCode::LimitNestingDepth,
            Self::NotRequiresBoolean
            | Self::LogicalOperandsMustBeBoolean
            | Self::ContainsPaused
            | Self::CheckedArithmeticPaused
            | Self::ListStructPaused
            | Self::TimestampSecondPaused
            | Self::InvalidLogicalType
            | Self::DateToUtf8CastPaused
            | Self::BinaryCastUnauthorized
            | Self::ComparisonIncomparable
            | Self::OrderedComparisonIncompatible
            | Self::OrderedComparisonRequiresNumeric
            | Self::CoalesceArmsIncompatible
            | Self::TrimRequiresUtf8
            | Self::LiteralIncompatibleWithColumn
            | Self::BinaryReplaceOnlyNullToNull
            | Self::FillNullNotAuthorizedOnBinary
            | Self::DerivedTypeMismatch
            | Self::DerivedNullabilityNarrower => NodeGraphErrorCode::IncompatibleType,
        }
    }

    /// The compile path's frozen message for this failure.
    pub const fn compile_message(self) -> &'static str {
        match self {
            Self::ShapeInvalid => "expression shape is invalid",
            Self::ExprBounds => "expression exceeds the contract limit",
            Self::UnknownColumn => "column is absent from the working schema",
            Self::NotRequiresBoolean => "not requires a boolean expression",
            Self::LogicalOperandsMustBeBoolean => "logical operands must be boolean",
            Self::ContainsPaused => "contains is not authorized",
            Self::CheckedArithmeticPaused => "checked arithmetic is not authorized",
            Self::ListStructPaused => "list and struct execution is paused",
            Self::TimestampSecondPaused => "timestamp second unit is paused",
            Self::InvalidLogicalType => "logical type is invalid",
            Self::DateToUtf8CastPaused => "date or timestamp to utf8 cast is paused",
            Self::BinaryCastUnauthorized => "cast to or from binary is not authorized",
            Self::ComparisonIncomparable => "comparison operands are not comparable",
            Self::OrderedComparisonIncompatible => "ordered comparison operands are not compatible",
            Self::OrderedComparisonRequiresNumeric => {
                "ordered comparison requires numeric or date values"
            }
            Self::CoalesceArmsIncompatible => "coalesce arms are not type-compatible",
            Self::TrimRequiresUtf8 => "trim requires a utf8 column",
            Self::LiteralIncompatibleWithColumn => "literal is not type-compatible with the column",
            Self::BinaryReplaceOnlyNullToNull => "binary replace-literal only permits null-to-null",
            Self::FillNullValueMustNotBeNull => "fill-null value must not be null",
            Self::FillNullNotAuthorizedOnBinary => "fill-null is not authorized on binary",
            Self::DropLastRemainingField => "drop-column cannot remove the final field",
            Self::DerivedTypeMismatch => "derived column type does not match the expression",
            Self::DerivedIdentityNotUnique => "derived column id or name is not unique",
            Self::DerivedNullabilityNarrower => {
                "derived column nullability is narrower than the expression"
            }
            Self::DerivedFieldInvalid => "derived field is invalid",
            Self::ProjectionEmpty => "projection must contain at least one column",
            Self::ProjectionDuplicate => "projection contains duplicate columns",
            Self::SchemaRebuildInvalid => "node produced an invalid logical schema",
            Self::RuleNotAdmitted => "rule is not admitted in the product path",
        }
    }
}

/// A typed semantic failure carrying the failing column when one is
/// identifiable. It never echoes a caller-supplied value (NX-C0 §7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticError {
    kind: SemanticKind,
    column_id: Option<ColumnId>,
}

impl SemanticError {
    pub const fn new(kind: SemanticKind) -> Self {
        Self {
            kind,
            column_id: None,
        }
    }

    pub const fn for_column(kind: SemanticKind, column_id: ColumnId) -> Self {
        Self {
            kind,
            column_id: Some(column_id),
        }
    }

    pub const fn kind(&self) -> SemanticKind {
        self.kind
    }

    pub const fn code(&self) -> NodeGraphErrorCode {
        self.kind.code()
    }

    pub const fn compile_message(&self) -> &'static str {
        self.kind.compile_message()
    }

    pub const fn column_id(&self) -> Option<ColumnId> {
        self.column_id
    }
}

/// Validates expression-local shape and bounds and resolves every column
/// reference against the working schema, without inferring types. The first
/// offending column in depth-first order is reported, matching the engine's
/// reference-validation entry point.
pub fn validate_expr_refs<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    resolver: &R,
) -> Result<(), SemanticError> {
    expr.validate_shape()
        .map_err(|_| SemanticError::new(SemanticKind::ShapeInvalid))?;
    let mut nodes = 0_usize;
    let mut pending = vec![(expr, 1_usize)];
    while let Some((current, depth)) = pending.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_EXPR_NODES || depth > MAX_EXPR_DEPTH {
            return Err(SemanticError::new(SemanticKind::ExprBounds));
        }
        match current {
            Expr::Column(id) => {
                if resolver.resolve_column(*id).is_none() {
                    return Err(SemanticError::for_column(SemanticKind::UnknownColumn, *id));
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => pending.push((expression, depth + 1)),
            Expr::Binary { left, right, .. } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                for expression in expressions {
                    pending.push((expression, depth + 1));
                }
            }
        }
    }
    Ok(())
}

/// Analyzes one expression against the working schema: shape, bounds,
/// references, result type, nullability, and the capability gate at the same
/// decision points both entry points enforce today.
pub fn analyze_expr<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    resolver: &R,
) -> Result<ExprAnalysis, SemanticError> {
    expr.validate_shape()
        .map_err(|_| SemanticError::new(SemanticKind::ShapeInvalid))?;
    check_shape_bounds(expr)?;
    let (data_type, nullable) = infer_expr(expr, resolver)?;
    Ok(ExprAnalysis {
        data_type,
        nullable,
    })
}

fn check_shape_bounds(expr: &Expr) -> Result<(), SemanticError> {
    let mut nodes = 0_usize;
    let mut max_depth = 0_usize;
    let mut pending = vec![(expr, 1_usize)];
    while let Some((current, depth)) = pending.pop() {
        nodes = nodes.saturating_add(1);
        max_depth = max_depth.max(depth);
        if nodes > MAX_EXPR_NODES || max_depth > MAX_EXPR_DEPTH {
            return Err(SemanticError::new(SemanticKind::ExprBounds));
        }
        match current {
            Expr::Column(_) | Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => pending.push((expression, depth + 1)),
            Expr::Binary { left, right, .. } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                for expression in expressions {
                    pending.push((expression, depth + 1));
                }
            }
        }
    }
    Ok(())
}

fn infer_expr<R: ColumnResolver + ?Sized>(
    expr: &Expr,
    resolver: &R,
) -> Result<(LogicalType, bool), SemanticError> {
    match expr {
        Expr::Column(id) => {
            let field = resolver
                .resolve_column(*id)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *id))?;
            capability::reject_paused_type(&field.data_type)?;
            Ok((field.data_type.clone(), field.nullable))
        }
        Expr::Literal(ScalarValue::Boolean(_)) => Ok((LogicalType::Boolean, false)),
        Expr::Literal(ScalarValue::Int64(_)) => Ok((LogicalType::Int64, false)),
        Expr::Literal(ScalarValue::UInt64(_)) => Ok((LogicalType::UInt64, false)),
        Expr::Literal(ScalarValue::Float64(_)) => Ok((LogicalType::Float64, false)),
        Expr::Literal(ScalarValue::Utf8(_)) => Ok((LogicalType::Utf8, false)),
        Expr::Literal(ScalarValue::Null) => Ok((LogicalType::Null, true)),
        Expr::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => {
            let (inner_type, inner_nullable) = infer_expr(expression, resolver)?;
            if inner_type != LogicalType::Boolean {
                return Err(SemanticError::new(SemanticKind::NotRequiresBoolean));
            }
            Ok((LogicalType::Boolean, inner_nullable))
        }
        Expr::Unary {
            operator: UnaryOperator::Negate,
            ..
        } => Err(SemanticError::new(SemanticKind::CheckedArithmeticPaused)),
        Expr::IsNull { expression, .. } => {
            let _ = infer_expr(expression, resolver)?;
            Ok((LogicalType::Boolean, false))
        }
        Expr::Cast {
            expression,
            data_type,
        } => {
            let (from, inner_nullable) = infer_expr(expression, resolver)?;
            capability::reject_paused_type(data_type)?;
            capability::reject_paused_cast(&from, data_type)?;
            Ok((data_type.clone(), inner_nullable))
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => {
            let (left_type, left_nullable) = infer_expr(left, resolver)?;
            let (right_type, right_nullable) = infer_expr(right, resolver)?;
            let nullable = left_nullable || right_nullable;
            match operator {
                BinaryOperator::And | BinaryOperator::Or => {
                    if left_type == LogicalType::Boolean && right_type == LogicalType::Boolean {
                        Ok((LogicalType::Boolean, nullable))
                    } else {
                        Err(SemanticError::new(
                            SemanticKind::LogicalOperandsMustBeBoolean,
                        ))
                    }
                }
                BinaryOperator::Contains => Err(SemanticError::new(SemanticKind::ContainsPaused)),
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    Err(SemanticError::new(SemanticKind::CheckedArithmeticPaused))
                }
                BinaryOperator::Equal | BinaryOperator::NotEqual => {
                    comparable_pair(&left_type, &right_type)?;
                    Ok((LogicalType::Boolean, nullable))
                }
                BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual => {
                    ordered_pair(&left_type, &right_type)?;
                    Ok((LogicalType::Boolean, nullable))
                }
            }
        }
        Expr::Coalesce { expressions } => {
            let mut joined: Option<LogicalType> = None;
            let mut nullable = true;
            for expression in expressions {
                let (next, arm_nullable) = infer_expr(expression, resolver)?;
                joined = match joined {
                    Some(current) => current
                        .least_upper_bound(&next)
                        .map_err(|_| SemanticError::new(SemanticKind::CoalesceArmsIncompatible))?
                        .into(),
                    None => Some(next),
                };
                nullable &= arm_nullable;
            }
            let joined = joined.ok_or_else(|| SemanticError::new(SemanticKind::ShapeInvalid))?;
            capability::reject_paused_type(&joined)?;
            Ok((joined, nullable))
        }
    }
}

fn comparable_pair(left: &LogicalType, right: &LogicalType) -> Result<(), SemanticError> {
    left.least_upper_bound(right)
        .map(|_| ())
        .map_err(|_| SemanticError::new(SemanticKind::ComparisonIncomparable))
}

fn ordered_pair(left: &LogicalType, right: &LogicalType) -> Result<(), SemanticError> {
    let joined = left
        .least_upper_bound(right)
        .map_err(|_| SemanticError::new(SemanticKind::OrderedComparisonIncompatible))?;
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
            capability::reject_paused_type(&joined)?;
            Ok(())
        }
        _ => Err(SemanticError::new(
            SemanticKind::OrderedComparisonRequiresNumeric,
        )),
    }
}

/// The complete schema after applying one product-admitted rule. The engine's
/// incremental propagation layer must produce the identical schema for every
/// accepted rule and is differentially tested against this function.
pub fn rule_effect(schema: &LogicalSchema, rule: &Rule) -> Result<LogicalSchema, SemanticError> {
    match rule {
        Rule::Rename { column, to } => {
            let mut fields = schema.fields.clone();
            let field = fields
                .iter_mut()
                .find(|field| field.id == *column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            field.name = to.clone();
            rebuild(schema, fields)
        }
        Rule::Trim { column } => {
            let field = schema
                .field(*column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            if field.data_type != LogicalType::Utf8 {
                return Err(SemanticError::for_column(
                    SemanticKind::TrimRequiresUtf8,
                    *column,
                ));
            }
            Ok(schema.clone())
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            let field = schema
                .field(*column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            capability::reject_paused_type(data_type)?;
            capability::reject_paused_cast(&field.data_type, data_type)?;
            let mut fields = schema.fields.clone();
            let output = fields
                .iter_mut()
                .find(|field| field.id == *column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            output.data_type = data_type.clone();
            if matches!(on_failure, CastFailurePolicy::SetNull) {
                output.nullable = true;
            }
            rebuild(schema, fields)
        }
        Rule::ReplaceLiteral { column, from, to } => {
            let field = schema
                .field(*column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            validate_literal_for_column(&field.data_type, from)?;
            validate_literal_for_column(&field.data_type, to)?;
            if field.data_type == LogicalType::Binary
                && !matches!((from, to), (ScalarValue::Null, ScalarValue::Null))
            {
                return Err(SemanticError::for_column(
                    SemanticKind::BinaryReplaceOnlyNullToNull,
                    *column,
                ));
            }
            if matches!(to, ScalarValue::Null) {
                let mut fields = schema.fields.clone();
                let output = fields
                    .iter_mut()
                    .find(|field| field.id == *column)
                    .ok_or_else(|| {
                        SemanticError::for_column(SemanticKind::UnknownColumn, *column)
                    })?;
                output.nullable = true;
                rebuild(schema, fields)
            } else {
                Ok(schema.clone())
            }
        }
        Rule::FillNull { column, value } => {
            if matches!(value, ScalarValue::Null) {
                return Err(SemanticError::for_column(
                    SemanticKind::FillNullValueMustNotBeNull,
                    *column,
                ));
            }
            let field = schema
                .field(*column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            if field.data_type == LogicalType::Binary {
                return Err(SemanticError::for_column(
                    SemanticKind::FillNullNotAuthorizedOnBinary,
                    *column,
                ));
            }
            validate_literal_for_column(&field.data_type, value)?;
            let mut fields = schema.fields.clone();
            let output = fields
                .iter_mut()
                .find(|field| field.id == *column)
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?;
            output.nullable = false;
            rebuild(schema, fields)
        }
        Rule::DropColumn { column } => {
            if schema.fields.len() <= 1 {
                return Err(SemanticError::new(SemanticKind::DropLastRemainingField));
            }
            if schema.field(*column).is_none() {
                return Err(SemanticError::for_column(
                    SemanticKind::UnknownColumn,
                    *column,
                ));
            }
            let fields = schema
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .cloned()
                .collect();
            rebuild(schema, fields)
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            let analysis = analyze_expr(expression, schema)?;
            capability::reject_paused_type(data_type)?;
            if analysis.data_type != LogicalType::Null && analysis.data_type != *data_type {
                return Err(SemanticError::new(SemanticKind::DerivedTypeMismatch));
            }
            if schema.field(*id).is_some() || schema.fields.iter().any(|field| field.name == *name)
            {
                return Err(SemanticError::new(SemanticKind::DerivedIdentityNotUnique));
            }
            capability::reject_paused_casts_in_expr(expression, schema)?;
            if !*nullable && analysis.nullable {
                return Err(SemanticError::new(SemanticKind::DerivedNullabilityNarrower));
            }
            let mut fields = schema.fields.clone();
            fields.push(
                LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                    .map_err(|_| SemanticError::new(SemanticKind::DerivedFieldInvalid))?,
            );
            rebuild(schema, fields)
        }
        Rule::FilterRows { .. } | Rule::Validate { .. } | Rule::Deduplicate { .. } => {
            Err(SemanticError::new(SemanticKind::RuleNotAdmitted))
        }
    }
}

/// The complete schema after projecting to the given ordered columns.
pub fn project_effect(
    schema: &LogicalSchema,
    columns: &[ColumnId],
) -> Result<LogicalSchema, SemanticError> {
    if columns.is_empty() {
        return Err(SemanticError::new(SemanticKind::ProjectionEmpty));
    }
    let mut seen = BTreeSet::new();
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if !seen.insert(*column) {
            return Err(SemanticError::new(SemanticKind::ProjectionDuplicate));
        }
        fields.push(
            schema
                .field(*column)
                .cloned()
                .ok_or_else(|| SemanticError::for_column(SemanticKind::UnknownColumn, *column))?,
        );
    }
    rebuild(schema, fields)
}

fn validate_literal_for_column(
    column_type: &LogicalType,
    value: &ScalarValue,
) -> Result<(), SemanticError> {
    let compatible = matches!(
        (column_type, value),
        (_, ScalarValue::Null)
            | (LogicalType::Boolean, ScalarValue::Boolean(_))
            | (LogicalType::Int64, ScalarValue::Int64(_))
            | (LogicalType::UInt64, ScalarValue::UInt64(_))
            | (LogicalType::Float64, ScalarValue::Float64(_))
            | (LogicalType::Utf8, ScalarValue::Utf8(_))
            | (
                LogicalType::Int8 | LogicalType::Int16 | LogicalType::Int32,
                ScalarValue::Int64(_)
            )
            | (
                LogicalType::UInt8 | LogicalType::UInt16 | LogicalType::UInt32,
                ScalarValue::UInt64(_),
            )
            | (LogicalType::Float32, ScalarValue::Float64(_))
    );
    if compatible {
        Ok(())
    } else {
        Err(SemanticError::new(
            SemanticKind::LiteralIncompatibleWithColumn,
        ))
    }
}

fn rebuild(
    source: &LogicalSchema,
    fields: Vec<LogicalField>,
) -> Result<LogicalSchema, SemanticError> {
    LogicalSchema::from_parts(source.version, fields, source.metadata.clone())
        .map_err(|_| SemanticError::new(SemanticKind::SchemaRebuildInvalid))
}

/// The compile-time capability gate: the paused set is frozen behavior
/// (NX-C0 §5.2) and must never be unlocked, relaxed, or reordered here. The
/// engine's runtime gate remains the final authority; semantic success never
/// implies executability.
pub mod capability {
    use stillflow_core::{BinaryOperator, LogicalType, TimeUnit, UnaryOperator};

    use super::{Expr, SemanticError, SemanticKind};

    pub fn reject_paused_type(data_type: &LogicalType) -> Result<(), SemanticError> {
        match data_type {
            LogicalType::List(_) | LogicalType::Struct(_) => {
                Err(SemanticError::new(SemanticKind::ListStructPaused))
            }
            LogicalType::Timestamp {
                unit: TimeUnit::Second,
                ..
            } => Err(SemanticError::new(SemanticKind::TimestampSecondPaused)),
            _ => data_type
                .validate()
                .map_err(|_| SemanticError::new(SemanticKind::InvalidLogicalType)),
        }
    }

    pub fn reject_paused_cast(from: &LogicalType, to: &LogicalType) -> Result<(), SemanticError> {
        if matches!(from, LogicalType::Date32 | LogicalType::Timestamp { .. })
            && matches!(to, LogicalType::Utf8)
        {
            return Err(SemanticError::new(SemanticKind::DateToUtf8CastPaused));
        }
        if (matches!(to, LogicalType::Binary) && !matches!(from, LogicalType::Binary))
            || (matches!(from, LogicalType::Binary) && !matches!(to, LogicalType::Binary))
        {
            return Err(SemanticError::new(SemanticKind::BinaryCastUnauthorized));
        }
        Ok(())
    }

    /// Walks an expression rejecting the paused capability set regardless of
    /// position: checked arithmetic, `contains`, and paused cast targets.
    pub fn reject_paused_capability(expr: &Expr) -> Result<(), SemanticError> {
        match expr {
            Expr::Unary {
                operator: UnaryOperator::Negate,
                ..
            } => Err(SemanticError::new(SemanticKind::CheckedArithmeticPaused)),
            Expr::Unary { expression, .. } | Expr::IsNull { expression, .. } => {
                reject_paused_capability(expression)
            }
            Expr::Cast {
                expression,
                data_type,
            } => {
                reject_paused_type(data_type)?;
                reject_paused_capability(expression)
            }
            Expr::Binary {
                left,
                operator,
                right,
            } => {
                match operator {
                    BinaryOperator::Contains => {
                        return Err(SemanticError::new(SemanticKind::ContainsPaused));
                    }
                    BinaryOperator::Add
                    | BinaryOperator::Subtract
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => {
                        return Err(SemanticError::new(SemanticKind::CheckedArithmeticPaused));
                    }
                    _ => {}
                }
                reject_paused_capability(left)?;
                reject_paused_capability(right)
            }
            Expr::Coalesce { expressions } => {
                for nested in expressions {
                    reject_paused_capability(nested)?;
                }
                Ok(())
            }
            Expr::Column(_) | Expr::Literal(_) => Ok(()),
        }
    }

    /// Rejects paused casts at any position inside an expression, using the
    /// shared analyzer for the cast source type.
    pub fn reject_paused_casts_in_expr<R: super::ColumnResolver + ?Sized>(
        expr: &Expr,
        resolver: &R,
    ) -> Result<(), SemanticError> {
        match expr {
            Expr::Cast {
                expression,
                data_type,
            } => {
                let analysis = super::analyze_expr(expression, resolver)?;
                reject_paused_cast(&analysis.data_type, data_type)?;
                reject_paused_casts_in_expr(expression, resolver)
            }
            Expr::Unary { expression, .. } | Expr::IsNull { expression, .. } => {
                reject_paused_casts_in_expr(expression, resolver)
            }
            Expr::Binary { left, right, .. } => {
                reject_paused_casts_in_expr(left, resolver)?;
                reject_paused_casts_in_expr(right, resolver)
            }
            Expr::Coalesce { expressions } => {
                for expression in expressions {
                    reject_paused_casts_in_expr(expression, resolver)?;
                }
                Ok(())
            }
            Expr::Column(_) | Expr::Literal(_) => Ok(()),
        }
    }
}
