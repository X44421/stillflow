use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use stillflow_core::{ColumnId, Expr, LogicalError};
use thiserror::Error;
use uuid::Uuid;

use crate::rule::{Rule, RuleError};

/// Current logical plan wire-format version.
pub const PLAN_VERSION: u16 = 1;

/// Versioned algorithm name for the non-security plan cache fingerprint.
pub const PLAN_FINGERPRINT_ALGORITHM: &str = "stillflow-fnv1a64x4-v1";

/// Stable identity of a node inside a logical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlanNodeId(Uuid);

impl PlanNodeId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for PlanNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical join behavior. Inputs remain positional: left then right.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// One ordered pair of logical join-key expressions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinKey {
    pub left: Expr,
    pub right: Expr,
}

/// Closed set of version 1 logical plan operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum PlanNodeKind {
    Scan {
        source_asset_id: Uuid,
        projection: Vec<ColumnId>,
        predicate: Option<Expr>,
    },
    Project {
        columns: Vec<ColumnId>,
    },
    Filter {
        predicate: Expr,
    },
    ApplyRules {
        rules: Vec<Rule>,
    },
    Join {
        join_type: JoinType,
        keys: Vec<JoinKey>,
    },
    Union,
    Materialize {
        output_label: String,
    },
}

impl PlanNodeKind {
    fn name(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "scan",
            Self::Project { .. } => "project",
            Self::Filter { .. } => "filter",
            Self::ApplyRules { .. } => "applyRules",
            Self::Join { .. } => "join",
            Self::Union => "union",
            Self::Materialize { .. } => "materialize",
        }
    }
}

/// Logical operator and its positional input node identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanNode {
    pub inputs: Vec<PlanNodeId>,
    pub kind: PlanNodeKind,
}

impl PlanNode {
    pub fn new(kind: PlanNodeKind, inputs: Vec<PlanNodeId>) -> Self {
        Self { inputs, kind }
    }
}

/// Versioned logical plan stored as an explicitly ordered map-backed DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalPlan {
    pub version: u16,
    pub root: PlanNodeId,
    pub nodes: BTreeMap<PlanNodeId, PlanNode>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogicalPlanData {
    version: u16,
    root: PlanNodeId,
    nodes: BTreeMap<PlanNodeId, PlanNode>,
}

impl<'de> Deserialize<'de> for LogicalPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = LogicalPlanData::deserialize(deserializer)?;
        Self::from_parts(data.version, data.root, data.nodes).map_err(serde::de::Error::custom)
    }
}

impl LogicalPlan {
    pub fn new(root: PlanNodeId, nodes: BTreeMap<PlanNodeId, PlanNode>) -> Result<Self, PlanError> {
        Self::from_parts(PLAN_VERSION, root, nodes)
    }

    pub fn from_parts(
        version: u16,
        root: PlanNodeId,
        nodes: BTreeMap<PlanNodeId, PlanNode>,
    ) -> Result<Self, PlanError> {
        let plan = Self {
            version,
            root,
            nodes,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Validates references, arity, local node invariants, and acyclicity.
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.version != PLAN_VERSION {
            return Err(PlanError::UnsupportedVersion(self.version));
        }
        if !self.nodes.contains_key(&self.root) {
            return Err(PlanError::MissingRoot(self.root));
        }

        let mut indegree: HashMap<PlanNodeId, usize> = self
            .nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, 0))
            .collect();
        let mut outgoing: HashMap<PlanNodeId, Vec<PlanNodeId>> = HashMap::new();

        for (node_id, node) in &self.nodes {
            validate_node(*node_id, node)?;
            for input in &node.inputs {
                if !self.nodes.contains_key(input) {
                    return Err(PlanError::UnknownInput {
                        node: *node_id,
                        input: *input,
                    });
                }
                if input == node_id {
                    return Err(PlanError::SelfEdge(*node_id));
                }
                *indegree.entry(*node_id).or_insert(0) += 1;
                outgoing.entry(*input).or_default().push(*node_id);
            }
        }

        let mut ready: VecDeque<PlanNodeId> = indegree
            .iter()
            .filter_map(|(node_id, degree)| (*degree == 0).then_some(*node_id))
            .collect();
        let mut visited = 0_usize;

        while let Some(node_id) = ready.pop_front() {
            visited += 1;
            if let Some(dependents) = outgoing.get(&node_id) {
                for dependent in dependents {
                    let Some(degree) = indegree.get_mut(dependent) else {
                        return Err(PlanError::GraphInvariant);
                    };
                    if *degree == 0 {
                        return Err(PlanError::GraphInvariant);
                    }
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push_back(*dependent);
                    }
                }
            }
        }

        if visited != self.nodes.len() {
            return Err(PlanError::Cycle);
        }
        Ok(())
    }

    /// Returns compact deterministic JSON after validating the complete plan.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, PlanError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| PlanError::Serialization(error.to_string()))
    }

    /// Returns a deterministic non-security cache fingerprint.
    pub fn fingerprint(&self) -> Result<PlanFingerprint, PlanError> {
        Ok(PlanFingerprint(fingerprint_bytes(&self.canonical_bytes()?)))
    }
}

fn validate_node(node_id: PlanNodeId, node: &PlanNode) -> Result<(), PlanError> {
    let (minimum_inputs, maximum_inputs) = match &node.kind {
        PlanNodeKind::Scan { .. } => (0, 0),
        PlanNodeKind::Project { .. }
        | PlanNodeKind::Filter { .. }
        | PlanNodeKind::ApplyRules { .. }
        | PlanNodeKind::Materialize { .. } => (1, 1),
        PlanNodeKind::Join { .. } => (2, 2),
        PlanNodeKind::Union => (2, usize::MAX),
    };
    if node.inputs.len() < minimum_inputs || node.inputs.len() > maximum_inputs {
        return Err(PlanError::InvalidArity {
            node: node_id,
            kind: node.kind.name(),
            minimum: minimum_inputs,
            maximum: maximum_inputs,
            actual: node.inputs.len(),
        });
    }

    match &node.kind {
        PlanNodeKind::Scan {
            source_asset_id,
            projection,
            predicate,
        } => {
            if source_asset_id.is_nil() {
                return Err(PlanError::NilSourceAsset(node_id));
            }
            validate_columns(node_id, "projection", projection)?;
            if let Some(predicate) = predicate {
                predicate.validate_shape()?;
            }
        }
        PlanNodeKind::Project { columns } => {
            validate_columns(node_id, "columns", columns)?;
        }
        PlanNodeKind::Filter { predicate } => predicate.validate_shape()?,
        PlanNodeKind::ApplyRules { rules } => {
            if rules.is_empty() {
                return Err(PlanError::EmptyCollection {
                    node: node_id,
                    field: "rules",
                });
            }
            for rule in rules {
                rule.validate()?;
            }
        }
        PlanNodeKind::Join { keys, .. } => {
            if keys.is_empty() {
                return Err(PlanError::EmptyCollection {
                    node: node_id,
                    field: "join keys",
                });
            }
            for key in keys {
                key.left.validate_shape()?;
                key.right.validate_shape()?;
            }
        }
        PlanNodeKind::Union => {}
        PlanNodeKind::Materialize { output_label } => {
            if output_label.trim().is_empty() {
                return Err(PlanError::EmptyCollection {
                    node: node_id,
                    field: "output label",
                });
            }
        }
    }
    Ok(())
}

fn validate_columns(
    node: PlanNodeId,
    field: &'static str,
    columns: &[ColumnId],
) -> Result<(), PlanError> {
    if columns.is_empty() {
        return Err(PlanError::EmptyCollection { node, field });
    }
    let mut unique = BTreeSet::new();
    for column in columns {
        if !unique.insert(*column) {
            return Err(PlanError::DuplicateColumn {
                node,
                field,
                column: *column,
            });
        }
    }
    Ok(())
}

fn fingerprint_bytes(bytes: &[u8]) -> [u8; 32] {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut lanes = [
        0xcbf2_9ce4_8422_2325,
        0x6c62_272e_07bb_0142,
        0x9e37_79b9_7f4a_7c15,
        0xd6e8_feb8_6659_fd93,
    ];
    for byte in bytes {
        for (index, lane) in lanes.iter_mut().enumerate() {
            *lane ^= u64::from(*byte) ^ ((index as u64) << 8);
            *lane = (*lane).wrapping_mul(PRIME);
        }
    }

    let mut result = [0_u8; 32];
    for (index, lane) in lanes.into_iter().enumerate() {
        let start = index * 8;
        result[start..start + 8].copy_from_slice(&lane.to_be_bytes());
    }
    result
}

/// Deterministic 256-bit plan cache index.
///
/// This value is not a cryptographic integrity checksum. Cache implementations
/// must compare canonical bytes after a fingerprint hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlanFingerprint([u8; 32]);

impl PlanFingerprint {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub const fn algorithm() -> &'static str {
        PLAN_FINGERPRINT_ALGORITHM
    }
}

impl fmt::Display for PlanFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Typed logical-plan validation and canonicalization failures.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("unsupported logical plan version {0}")]
    UnsupportedVersion(u16),
    #[error("logical plan root {0} is absent")]
    MissingRoot(PlanNodeId),
    #[error("node {node} references absent input {input}")]
    UnknownInput { node: PlanNodeId, input: PlanNodeId },
    #[error("node {0} references itself")]
    SelfEdge(PlanNodeId),
    #[error("node {node} ({kind}) has {actual} inputs; expected between {minimum} and {maximum}")]
    InvalidArity {
        node: PlanNodeId,
        kind: &'static str,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    #[error("node {node} has an empty required field: {field}")]
    EmptyCollection {
        node: PlanNodeId,
        field: &'static str,
    },
    #[error("node {node} has duplicate column {column} in {field}")]
    DuplicateColumn {
        node: PlanNodeId,
        field: &'static str,
        column: ColumnId,
    },
    #[error("scan node {0} has a nil source asset id")]
    NilSourceAsset(PlanNodeId),
    #[error("logical plan contains a directed cycle")]
    Cycle,
    #[error("logical plan graph bookkeeping invariant failed")]
    GraphInvariant,
    #[error(transparent)]
    Logical(#[from] LogicalError),
    #[error(transparent)]
    Rule(#[from] RuleError),
    #[error("logical plan serialization failed: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(value: u128) -> PlanNodeId {
        PlanNodeId::from_uuid(Uuid::from_u128(value))
    }

    fn column_id(value: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(value))
    }

    fn scan() -> PlanNode {
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: Uuid::from_u128(100),
                projection: vec![column_id(1)],
                predicate: None,
            },
            Vec::new(),
        )
    }

    fn two_node_plan(reverse_insert: bool) -> LogicalPlan {
        let scan_id = node_id(1);
        let project_id = node_id(2);
        let project = PlanNode::new(
            PlanNodeKind::Project {
                columns: vec![column_id(1)],
            },
            vec![scan_id],
        );
        let mut nodes = BTreeMap::new();
        if reverse_insert {
            nodes.insert(project_id, project.clone());
            nodes.insert(scan_id, scan());
        } else {
            nodes.insert(scan_id, scan());
            nodes.insert(project_id, project);
        }
        LogicalPlan::new(project_id, nodes).expect("valid plan")
    }

    #[test]
    fn rejects_missing_root_unknown_input_bad_arity_and_cycle() {
        let missing_root = LogicalPlan {
            version: PLAN_VERSION,
            root: node_id(9),
            nodes: BTreeMap::new(),
        };
        assert!(matches!(
            missing_root.validate(),
            Err(PlanError::MissingRoot(_))
        ));

        let mut unknown_nodes = BTreeMap::new();
        unknown_nodes.insert(
            node_id(1),
            PlanNode::new(
                PlanNodeKind::Project {
                    columns: vec![column_id(1)],
                },
                vec![node_id(99)],
            ),
        );
        let unknown = LogicalPlan {
            version: PLAN_VERSION,
            root: node_id(1),
            nodes: unknown_nodes,
        };
        assert!(matches!(
            unknown.validate(),
            Err(PlanError::UnknownInput { .. })
        ));

        let mut arity_nodes = BTreeMap::new();
        arity_nodes.insert(
            node_id(1),
            PlanNode::new(
                PlanNodeKind::Project {
                    columns: vec![column_id(1)],
                },
                Vec::new(),
            ),
        );
        let arity = LogicalPlan {
            version: PLAN_VERSION,
            root: node_id(1),
            nodes: arity_nodes,
        };
        assert!(matches!(
            arity.validate(),
            Err(PlanError::InvalidArity { .. })
        ));

        let mut cycle_nodes = BTreeMap::new();
        cycle_nodes.insert(
            node_id(1),
            PlanNode::new(
                PlanNodeKind::Project {
                    columns: vec![column_id(1)],
                },
                vec![node_id(2)],
            ),
        );
        cycle_nodes.insert(
            node_id(2),
            PlanNode::new(
                PlanNodeKind::Project {
                    columns: vec![column_id(1)],
                },
                vec![node_id(1)],
            ),
        );
        let cycle = LogicalPlan {
            version: PLAN_VERSION,
            root: node_id(2),
            nodes: cycle_nodes,
        };
        assert!(matches!(cycle.validate(), Err(PlanError::Cycle)));
    }

    #[test]
    fn validates_deep_dag_without_recursive_stack_growth() {
        const NODE_COUNT: u128 = 10_000;
        let mut nodes = BTreeMap::new();
        nodes.insert(node_id(1), scan());
        for value in 2..=NODE_COUNT {
            nodes.insert(
                node_id(value),
                PlanNode::new(
                    PlanNodeKind::Project {
                        columns: vec![column_id(1)],
                    },
                    vec![node_id(value - 1)],
                ),
            );
        }
        let plan = LogicalPlan {
            version: PLAN_VERSION,
            root: node_id(NODE_COUNT),
            nodes,
        };
        plan.validate().expect("deep DAG must validate");
    }

    #[test]
    fn canonical_bytes_and_fingerprint_ignore_map_insertion_order() {
        let first = two_node_plan(false);
        let second = two_node_plan(true);
        assert_eq!(
            first.canonical_bytes().expect("canonical"),
            second.canonical_bytes().expect("canonical")
        );
        assert_eq!(
            first.fingerprint().expect("fingerprint"),
            second.fingerprint().expect("fingerprint")
        );
        assert_eq!(PlanFingerprint::algorithm(), PLAN_FINGERPRINT_ALGORITHM);
        assert_eq!(
            first.fingerprint().expect("fingerprint").to_string().len(),
            64
        );
    }

    #[test]
    fn semantic_change_changes_canonical_bytes_and_fingerprint() {
        let first = two_node_plan(false);
        let mut changed = first.clone();
        let node = changed.nodes.get_mut(&node_id(2)).expect("project node");
        node.kind = PlanNodeKind::Filter {
            predicate: Expr::Column(column_id(1)),
        };
        assert_ne!(
            first.canonical_bytes().expect("canonical"),
            changed.canonical_bytes().expect("canonical")
        );
        assert_ne!(
            first.fingerprint().expect("fingerprint"),
            changed.fingerprint().expect("fingerprint")
        );
    }

    #[test]
    fn expression_input_order_and_rule_order_are_canonical_semantics() {
        let scan_id = node_id(1);
        let filter_id = node_id(2);
        let predicate = |value| Expr::Binary {
            left: Box::new(Expr::Column(column_id(1))),
            operator: stillflow_core::BinaryOperator::Equal,
            right: Box::new(Expr::Literal(stillflow_core::ScalarValue::Int64(value))),
        };
        let mut first_nodes = BTreeMap::new();
        first_nodes.insert(scan_id, scan());
        first_nodes.insert(
            filter_id,
            PlanNode::new(
                PlanNodeKind::Filter {
                    predicate: predicate(1),
                },
                vec![scan_id],
            ),
        );
        let first = LogicalPlan::new(filter_id, first_nodes).expect("filter plan");
        let mut changed_expression = first.clone();
        changed_expression
            .nodes
            .get_mut(&filter_id)
            .expect("filter")
            .kind = PlanNodeKind::Filter {
            predicate: predicate(2),
        };
        assert_ne!(
            first.canonical_bytes().expect("canonical"),
            changed_expression.canonical_bytes().expect("canonical")
        );

        let left_id = node_id(10);
        let right_id = node_id(11);
        let join_id = node_id(12);
        let mut join_nodes = BTreeMap::new();
        join_nodes.insert(left_id, scan());
        join_nodes.insert(
            right_id,
            PlanNode::new(
                PlanNodeKind::Scan {
                    source_asset_id: Uuid::from_u128(101),
                    projection: vec![column_id(1)],
                    predicate: None,
                },
                Vec::new(),
            ),
        );
        join_nodes.insert(
            join_id,
            PlanNode::new(
                PlanNodeKind::Join {
                    join_type: JoinType::Inner,
                    keys: vec![JoinKey {
                        left: Expr::Column(column_id(1)),
                        right: Expr::Column(column_id(1)),
                    }],
                },
                vec![left_id, right_id],
            ),
        );
        let ordered = LogicalPlan::new(join_id, join_nodes).expect("join plan");
        let mut reversed = ordered.clone();
        reversed.nodes.get_mut(&join_id).expect("join").inputs = vec![right_id, left_id];
        assert_ne!(
            ordered.canonical_bytes().expect("canonical"),
            reversed.canonical_bytes().expect("canonical")
        );

        let rules_id = node_id(20);
        let mut rule_nodes = BTreeMap::new();
        rule_nodes.insert(scan_id, scan());
        rule_nodes.insert(
            rules_id,
            PlanNode::new(
                PlanNodeKind::ApplyRules {
                    rules: vec![Rule::Trim {
                        column: column_id(1),
                    }],
                },
                vec![scan_id],
            ),
        );
        let trim = LogicalPlan::new(rules_id, rule_nodes).expect("rule plan");
        let mut drop = trim.clone();
        drop.nodes.get_mut(&rules_id).expect("rules").kind = PlanNodeKind::ApplyRules {
            rules: vec![Rule::DropColumn {
                column: column_id(1),
            }],
        };
        assert_ne!(
            trim.canonical_bytes().expect("canonical"),
            drop.canonical_bytes().expect("canonical")
        );
    }

    #[test]
    fn validated_plan_roundtrips() {
        let plan = two_node_plan(false);
        let json = serde_json::to_vec(&plan).expect("serialize");
        let restored: LogicalPlan = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(restored, plan);
    }
}
