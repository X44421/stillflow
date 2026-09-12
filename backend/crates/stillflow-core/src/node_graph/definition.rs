//! Node definition records, the machine-readable config-constraint
//! vocabulary, and the shared config parsing helpers (NX-N1, #337; frozen by
//! the NX-C0 contract §6).
//!
//! Each built-in node owns one definition module under [`crate::node_graph`]
//! (`definitions/`), producing its typed config validation, catalog entry,
//! constraints, support conditions, and validity samples from one source.
//! The registry only performs stable lookup and ordering; it never writes a
//! node's rules.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{
    ColumnId, Expr, LogicalType, NodeConfig, NodeGraphError, NodeGraphErrorCode, NodeId, PortId,
    ScalarValue, ValidatedNodeConfig, MAX_STRING_BYTES,
};

/// The closed set of JSON shapes a config field can carry. The variants are
/// the catalog wire tokens of NX-C0 §6.2; the Rust enum is named only for
/// implementers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfigValueKind {
    Uuid,
    ColumnId,
    ColumnIdList,
    String,
    Boolean,
    Expression,
    LogicalType,
    ScalarValue,
    CastFailurePolicy,
}

/// Machine-readable constraints describing what the node's validator
/// enforces for one config field (NX-C0 §6.2). Constraint data is
/// descriptive of the validator, never a substitute for it: a constraint the
/// validator does not enforce, or a validator rule the constraints cannot
/// express, is a defect for the positive/negative sample test.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigConstraints {
    /// Closed value set in wire values (camelCase), not Rust variant names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    /// UTF-8 byte bounds on a string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    /// List length bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_items: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_items: Option<usize>,
    /// No duplicate entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_items: Option<bool>,
    /// Caller order is semantic and preserved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordered: Option<bool>,
    /// Rejects empty strings and empty lists after trimming rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_empty: Option<bool>,
    /// Rejects `null` even where the kind would allow it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_null: Option<bool>,
    /// Per-value byte ceiling from the frozen limit table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_bound: Option<usize>,
}

impl ConfigConstraints {
    pub const fn default_for() -> Self {
        Self {
            enum_values: None,
            min_length: None,
            max_length: None,
            min_items: None,
            max_items: None,
            unique_items: None,
            ordered: None,
            non_empty: None,
            non_null: None,
            byte_bound: None,
        }
    }

    pub fn with_enum_values(mut self, values: &[&str]) -> Self {
        self.enum_values = Some(values.iter().map(|value| value.to_string()).collect());
        self
    }

    pub const fn with_list_bounds(mut self, min_items: usize, unique_items: bool) -> Self {
        self.min_items = Some(min_items);
        self.unique_items = Some(unique_items);
        self
    }

    pub const fn ordered(mut self) -> Self {
        self.ordered = Some(true);
        self
    }

    pub const fn non_empty(mut self) -> Self {
        self.non_empty = Some(true);
        self
    }

    pub const fn non_null(mut self) -> Self {
        self.non_null = Some(true);
        self
    }

    pub const fn with_byte_bound(mut self, bound: usize) -> Self {
        self.byte_bound = Some(bound);
        self
    }

    /// The names of the constraints this record advertises, used by the
    /// mechanical catalog↔validator consistency test.
    pub fn advertised(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.enum_values.is_some() {
            names.push("enumValues");
        }
        if self.min_length.is_some() {
            names.push("minLength");
        }
        if self.max_length.is_some() {
            names.push("maxLength");
        }
        if self.min_items.is_some() {
            names.push("minItems");
        }
        if self.max_items.is_some() {
            names.push("maxItems");
        }
        if self.unique_items.is_some() {
            names.push("uniqueItems");
        }
        if self.ordered.is_some() {
            names.push("ordered");
        }
        if self.non_empty.is_some() {
            names.push("nonEmpty");
        }
        if self.non_null.is_some() {
            names.push("nonNull");
        }
        if self.byte_bound.is_some() {
            names.push("byteBound");
        }
        names
    }
}

/// One declared config field. `name`, `value_kind`, and `required` keep
/// their positions and meanings; constraints are carried additively.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    pub name: String,
    pub value_kind: ConfigValueKind,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<ConfigConstraints>,
}

impl ConfigField {
    pub fn advertised_constraints(&self) -> Vec<&'static str> {
        self.constraints
            .as_ref()
            .map(ConfigConstraints::advertised)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSchema {
    pub fields: Vec<ConfigField>,
    pub additional_properties: bool,
}

/// A machine-readable statement about when a node is applicable to the
/// working schema (NX-C0 §6.3). Conditions are declared by the definition
/// and enforced by the compiler with the same `NG_*` codes the shared
/// semantics produce; no condition is enforced only by the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SupportCondition {
    RequiresType(LogicalType),
    RequiresNonNullable,
    RequiresNullable,
    ForbidsType(LogicalType),
    RequiresExecutableType,
    MinFields(usize),
}

impl SupportCondition {
    /// The constraint name advertised in the catalog for this condition.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::RequiresType(_) => "requiresType",
            Self::RequiresNonNullable => "requiresNonNullable",
            Self::RequiresNullable => "requiresNullable",
            Self::ForbidsType(_) => "forbidsType",
            Self::RequiresExecutableType => "requiresExecutableType",
            Self::MinFields(_) => "minFields",
        }
    }
}

/// Support conditions for one input port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortSupportConditions {
    pub port: PortId,
    pub conditions: Vec<SupportCondition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeLoweringTarget {
    Scan,
    Project,
    Filter,
    ApplyRules,
    Materialize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeSupportStatus {
    Supported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCatalogEntry {
    pub type_id: String,
    pub config_version: u16,
    pub display_name: String,
    pub description: String,
    pub config_schema: ConfigSchema,
    pub input_ports: Vec<PortId>,
    pub output_ports: Vec<PortId>,
    pub lowering_target: NodeLoweringTarget,
    pub support_status: NodeSupportStatus,
    /// Declared input support conditions. Empty (omitted on the wire) when
    /// the node declares none; every built-in declares
    /// `requiresExecutableType` on its input.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_conditions: Vec<PortSupportConditions>,
}

/// The role a node plays in graph validation. Adding a node type never
/// extends a central enum; it picks one of these roles in its own module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Source,
    Transform,
    Output,
}

/// One node's declarative definition: identity, ports, catalog metadata,
/// constraints, support conditions, and the node-owned config validator.
/// Adding a node type means adding one such record in its own module plus an
/// entry in the registry's definition list — never a change to the generic
/// graph traversal.
/// The definition kind: an atomic node validates through its own function;
/// a composite node resolves through its frozen expansion (NX-C1 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DefinitionKind {
    Atomic(fn(&NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError>),
    Composite(super::composite::CompositeStepList),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDefinition {
    type_id: String,
    config_version: u16,
    display_name: String,
    description: String,
    config_schema: ConfigSchema,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
    lowering_target: NodeLoweringTarget,
    support_status: NodeSupportStatus,
    support_conditions: Vec<PortSupportConditions>,
    role: NodeRole,
    kind: DefinitionKind,
}

impl NodeDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        type_id: &str,
        config_version: u16,
        display_name: &str,
        description: &str,
        config_schema: ConfigSchema,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        lowering_target: NodeLoweringTarget,
        role: NodeRole,
        support_conditions: Vec<PortSupportConditions>,
        validate: fn(&NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError>,
    ) -> Self {
        Self {
            type_id: type_id.to_owned(),
            config_version,
            display_name: display_name.to_owned(),
            description: description.to_owned(),
            config_schema,
            input_ports,
            output_ports,
            lowering_target,
            support_status: NodeSupportStatus::Supported,
            support_conditions,
            role,
            kind: DefinitionKind::Atomic(validate),
        }
    }

    /// The composite definition constructor (NX-C1 §3.1): the expansion is
    /// data, resolved against the registry during traversal.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_composite(
        type_id: &str,
        config_version: u16,
        display_name: &str,
        description: &str,
        config_schema: ConfigSchema,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        lowering_target: NodeLoweringTarget,
        support_conditions: Vec<PortSupportConditions>,
        expansion: Vec<super::composite::CompositeStep>,
    ) -> Self {
        Self {
            type_id: type_id.to_owned(),
            config_version,
            display_name: display_name.to_owned(),
            description: description.to_owned(),
            config_schema,
            input_ports,
            output_ports,
            lowering_target,
            support_status: NodeSupportStatus::Supported,
            support_conditions,
            role: NodeRole::Transform,
            kind: DefinitionKind::Composite(super::composite::CompositeStepList {
                steps: expansion,
            }),
        }
    }

    pub(crate) fn composite_expansion(&self) -> Option<&[super::composite::CompositeStep]> {
        match &self.kind {
            DefinitionKind::Atomic(_) => None,
            DefinitionKind::Composite(expansion) => Some(&expansion.steps),
        }
    }

    pub fn type_id(&self) -> &str {
        &self.type_id
    }

    pub const fn config_version(&self) -> u16 {
        self.config_version
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn config_schema(&self) -> &ConfigSchema {
        &self.config_schema
    }

    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }

    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }

    pub const fn lowering_target(&self) -> NodeLoweringTarget {
        self.lowering_target
    }

    pub const fn support_status(&self) -> NodeSupportStatus {
        self.support_status
    }

    pub fn support_conditions(&self) -> &[PortSupportConditions] {
        &self.support_conditions
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub(crate) fn is_source(&self) -> bool {
        self.role == NodeRole::Source
    }

    pub(crate) fn is_output(&self) -> bool {
        self.role == NodeRole::Output
    }

    pub fn catalog_entry(&self) -> NodeCatalogEntry {
        NodeCatalogEntry {
            type_id: self.type_id.clone(),
            config_version: self.config_version,
            display_name: self.display_name.clone(),
            description: self.description.clone(),
            config_schema: self.config_schema.clone(),
            input_ports: self.input_ports.clone(),
            output_ports: self.output_ports.clone(),
            lowering_target: self.lowering_target,
            support_status: self.support_status,
            support_conditions: self.support_conditions.clone(),
        }
    }

    pub fn validate_node_config(
        &self,
        node: &NodeConfig,
    ) -> Result<ValidatedNodeConfig, NodeGraphError> {
        if node.type_id() != self.type_id {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::UnknownNodeType,
                Some(node.id()),
                "node type does not match its resolved definition",
            ));
        }
        if node.config_version() != self.config_version {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::UnsupportedConfigVersion,
                Some(node.id()),
                format!(
                    "config version {} is not supported for {}",
                    node.config_version, self.type_id
                ),
            ));
        }
        match &self.kind {
            DefinitionKind::Atomic(validate) => validate(node),
            // The composite path needs the registry to resolve its steps, so
            // the traversal calls it directly; reaching here means a caller
            // bypassed the traversal.
            DefinitionKind::Composite(_) => Err(NodeGraphError::new(
                NodeGraphErrorCode::Internal,
                Some(node.id()),
                "composite definitions resolve through the graph traversal",
            )),
        }
    }
}

/// Declares one config field with optional constraints.
pub(crate) fn config_field(
    name: &str,
    value_kind: ConfigValueKind,
    required: bool,
    constraints: Option<ConfigConstraints>,
) -> ConfigField {
    ConfigField {
        name: name.to_owned(),
        value_kind,
        required,
        constraints,
    }
}

/// Declares the `requiresExecutableType` support condition for one input
/// port; every built-in input carries it (NX-C0 §6.3).
pub(crate) fn executable_input(port: &str) -> PortSupportConditions {
    PortSupportConditions {
        port: PortId::new(port).expect("static port id"),
        conditions: vec![SupportCondition::RequiresExecutableType],
    }
}

// ---- shared config parsing and validation helpers ----

pub(super) fn parse_config<T: DeserializeOwned>(
    node: &NodeConfig,
    required: &[(&str, bool)],
    optional: &[&str],
) -> Result<T, NodeGraphError> {
    let object = node.config.as_object().ok_or_else(|| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "node config must be an object",
        )
    })?;
    let allowed: std::collections::BTreeSet<&str> = required
        .iter()
        .map(|(name, _)| *name)
        .chain(optional.iter().copied())
        .collect();
    for key in object.keys() {
        if !allowed.contains(key.as_str()) {
            return Err(invalid_config(node, key));
        }
    }
    for (name, required_field) in required {
        if *required_field && !object.contains_key(*name) {
            return Err(invalid_config(node, name));
        }
    }
    serde_json::from_value(node.config.clone()).map_err(|_| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "node config has an invalid field shape or primitive type",
        )
    })
}

pub(super) fn invalid_config(node: &NodeConfig, field: &str) -> NodeGraphError {
    NodeGraphError::new(
        NodeGraphErrorCode::InvalidConfig,
        Some(node.id()),
        format!("node config field {field} is missing or invalid"),
    )
    .with_field_path(field)
}

pub(super) fn validate_name(
    node: &NodeConfig,
    value: &str,
    field: &str,
) -> Result<(), NodeGraphError> {
    validate_string(value, Some(node.id()), field)?;
    if value.trim().is_empty() {
        return Err(invalid_config(node, field));
    }
    Ok(())
}

pub(super) fn validate_column(
    node: &NodeConfig,
    column: ColumnId,
    field: &str,
) -> Result<(), NodeGraphError> {
    if column.as_uuid().is_nil() {
        return Err(invalid_config(node, field));
    }
    Ok(())
}

pub(super) fn validate_columns(
    node: &NodeConfig,
    columns: &[ColumnId],
    field: &str,
) -> Result<(), NodeGraphError> {
    let mut unique = std::collections::BTreeSet::new();
    for column in columns {
        validate_column(node, *column, field)?;
        if !unique.insert(*column) {
            return Err(invalid_config(node, field));
        }
    }
    Ok(())
}

pub(super) fn validate_expression(
    node: &NodeConfig,
    expression: &Expr,
    field: &str,
) -> Result<(), NodeGraphError> {
    expression
        .validate_shape()
        .map_err(|_| invalid_config(node, field))?;
    let mut pending = vec![(expression, 1_usize)];
    let mut count = 0_usize;
    while let Some((current, depth)) = pending.pop() {
        count = count.checked_add(1).ok_or_else(|| {
            NodeGraphError::new(
                NodeGraphErrorCode::LimitCompileWork,
                Some(node.id()),
                "expression node count overflowed",
            )
        })?;
        if count > super::MAX_EXPR_NODES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitCompileWork,
                Some(node.id()),
                "expression node bound exceeded",
            ));
        }
        if depth > super::MAX_EXPR_DEPTH {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitNestingDepth,
                Some(node.id()),
                "expression depth bound exceeded",
            ));
        }
        match current {
            Expr::Column(column) => {
                if column.as_uuid().is_nil() {
                    return Err(invalid_config(node, field));
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => {
                pending.push((expression, depth + 1));
            }
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

pub(super) fn validate_logical_type(
    node: &NodeConfig,
    data_type: &LogicalType,
    field: &str,
) -> Result<(), NodeGraphError> {
    data_type
        .validate()
        .map_err(|_| invalid_config(node, field))
}

pub(super) fn validate_scalar(
    node: &NodeConfig,
    value: &ScalarValue,
    field: &str,
) -> Result<(), NodeGraphError> {
    Expr::Literal(value.clone())
        .validate_shape()
        .map_err(|_| invalid_config(node, field))
}

pub(super) fn validate_string(
    value: &str,
    node_id: Option<NodeId>,
    field: &str,
) -> Result<(), NodeGraphError> {
    if value.len() > MAX_STRING_BYTES {
        return Err(NodeGraphError::new(
            NodeGraphErrorCode::LimitStringBytes,
            node_id,
            format!("{field} exceeds {} UTF-8 bytes", MAX_STRING_BYTES),
        ));
    }
    Ok(())
}
