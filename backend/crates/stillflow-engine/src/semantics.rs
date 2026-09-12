//! Public semantic-analysis seam (NX-S1, #336).
//!
//! The engine's preflight typing, nullability, capability, and rule-schema
//! propagation delegate to the shared analyzer in
//! [`stillflow_plan::semantics`](stillflow_plan::semantics) (NX-C0 §5.1). This
//! module exposes the engine-equivalent entry points so the frozen
//! differential battery (`tests/nx_s1_differential.rs`) can compare the two
//! entry points without a connector, API, service, filesystem, or network
//! dependency, and so embedders can reproduce engine semantics exactly.
//!
//! Rule-schema propagation here is the engine's production incremental layer
//! (the NX-C0 §5.4 performance representation), not a second authority: it is
//! differentially tested against the shared analyzer by the battery.

use stillflow_core::{ColumnId, Expr, LogicalSchema, LogicalType};
use stillflow_plan::Rule;

use crate::error::EngineError;

/// The engine's semantic result for one expression: the logical type and the
/// nullability of the expression result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprSemantics {
    pub data_type: LogicalType,
    pub nullable: bool,
}

/// Analyzes one expression exactly as the engine preflight does (shared
/// analyzer, engine error mapping).
pub fn analyze_expr(expr: &Expr, schema: &LogicalSchema) -> Result<ExprSemantics, EngineError> {
    crate::typing::analyze_expr(expr, schema).map(|(data_type, nullable)| ExprSemantics {
        data_type,
        nullable,
    })
}

/// Applies one rule through the engine's production incremental propagation
/// layer, exactly as preflight does for execution (`verification = false`,
/// so `Rule::Validate`/`Rule::Deduplicate` stay rejected).
pub fn rule_effect(schema: &LogicalSchema, rule: &Rule) -> Result<LogicalSchema, EngineError> {
    crate::preflight::apply_rule_schema(schema.clone(), rule, false)
}

/// Projects a schema to the given ordered columns exactly as preflight does
/// (deterministic index policy over the lookups this single call serves).
pub fn project_effect(
    schema: &LogicalSchema,
    columns: &[ColumnId],
) -> Result<LogicalSchema, EngineError> {
    crate::preflight::project_schema(schema, columns)
}
