//! Deterministic logical rules, plans, validation, and canonical fingerprints.
//!
//! This crate contains no physical execution-engine objects. Polars and DuckDB
//! adapters lower these contracts in downstream crates.

mod plan;
mod rule;

pub use plan::{
    JoinKey, JoinType, LogicalPlan, PlanError, PlanFingerprint, PlanNode, PlanNodeId, PlanNodeKind,
    PLAN_FINGERPRINT_ALGORITHM, PLAN_VERSION,
};
pub use rule::{CastFailurePolicy, Rule, RuleError, ValidationSeverity};
