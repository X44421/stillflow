//! Deterministic logical rules, plans, validation, and canonical fingerprints.
//!
//! This crate contains no physical execution-engine objects. Polars and DuckDB
//! adapters lower these contracts in downstream crates.

mod node_graph_compiler;
mod plan;
mod rule;
pub mod semantics;

pub use node_graph_compiler::{
    compile_node_graph, internal_plan_node_id, validate_node_graph, AuthorizedSourceContext,
    CompileDiagnostic, CompileTarget, CompiledNodeGraph, NodeGraphCompileError, NodeGraphCompiler,
    MAX_COMPILE_WORK, MAX_DIAGNOSTICS, MAX_DIAGNOSTIC_BYTES, NODE_GRAPH_COMPILER_VERSION,
};
pub use plan::{
    JoinKey, JoinType, LogicalPlan, PlanError, PlanFingerprint, PlanNode, PlanNodeId, PlanNodeKind,
    PLAN_FINGERPRINT_ALGORITHM, PLAN_VERSION,
};
pub use rule::{CastFailurePolicy, Rule, RuleError, ValidationSeverity};
pub use semantics::{
    analyze_expr, capability, project_effect, rule_effect, validate_expr_refs, ColumnResolver,
    ExprAnalysis, SemanticError, SemanticKind,
};
