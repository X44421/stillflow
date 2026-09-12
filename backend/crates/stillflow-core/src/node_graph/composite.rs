//! Declarative composite node packages (NX-N2, #341; frozen by the NX-C1
//! contract §6/§7).
//!
//! A package is an audited, explicitly deployed declaration that expands one
//! product node into a bounded linear sequence of existing atomic operators.
//! Packages carry an immutable content digest, exact versions, an empty
//! dependency list, and a closed operator set; they never execute code, and
//! published plans never depend on them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::definition::{
    ConfigField, ConfigSchema, ConfigValueKind, NodeDefinition, PortSupportConditions,
};
use super::{
    CastFailurePolicy, ColumnId, Expr, LogicalType, NodeConfig, NodeGraphError, NodeGraphErrorCode,
    NodeRegistry, PortId, ScalarValue, ValidatedNodeConfig,
};

/// The closed set of atomic operators a package step may target (NX-C1
/// §6.3). `source` and `output` are not admissible; the set is closed over
/// the built-ins and every entry must be declared in the package's
/// `operators` list.
pub const ADMISSIBLE_OPERATORS: [&str; 9] = [
    "stillflow.node.select",
    "stillflow.node.filter",
    "stillflow.node.rename",
    "stillflow.node.trim",
    "stillflow.node.cast",
    "stillflow.node.replace-literal",
    "stillflow.node.fill-null",
    "stillflow.node.drop-column",
    "stillflow.node.derive-column",
];

/// Frozen package bounds (NX-C1 §8).
pub const MAX_PACKAGE_BYTES: usize = 16 * 1024;
pub const MAX_PACKAGE_OPERATORS: usize = 8;
pub const MAX_EXPANSION_STEPS: usize = 4;

/// The expansion payload carried by a composite `NodeDefinition`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositeStepList {
    pub steps: Vec<CompositeStep>,
}

/// One expansion step: an atomic operator plus frozen bindings from the
/// composite config to the step's config fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompositeStep {
    pub operator: String,
    pub bindings: BTreeMap<String, CompositeBinding>,
}

/// The frozen binding forms (NX-C1 §5): `identity` binds the composite's
/// `column` parameter; `literal` embeds a constant JSON value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "form", content = "value", rename_all = "camelCase")]
pub enum CompositeBinding {
    Identity,
    Literal(Value),
}

/// The package document. Unknown fields are rejected; `contentDigest` is the
/// SHA-256 of the canonical JSON encoding of every field except itself and
/// is verified at the deployment surface (NX-C1 §6.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodePackage {
    pub package_format: u16,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub content_digest: String,
    pub depends_on: Vec<String>,
    pub operators: Vec<String>,
    pub type_id: String,
    pub config_version: u16,
    pub definition: NodePackageDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodePackageDefinition {
    pub display_name: String,
    pub description: String,
    pub config_schema: ConfigSchema,
    pub expansion: Vec<CompositeStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_conditions: Vec<PortSupportConditions>,
}

/// A typed value of a composite config field or binding, parsed per the
/// closed `ConfigValueKind` vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeValue {
    Uuid(uuid::Uuid),
    ColumnId(ColumnId),
    ColumnIdList(Vec<ColumnId>),
    String(String),
    Boolean(bool),
    Expression(Expr),
    LogicalType(LogicalType),
    Scalar(ScalarValue),
    CastFailurePolicy(CastFailurePolicy),
}

fn parse_composite_value(
    node: &NodeConfig,
    kind: ConfigValueKind,
    value: &Value,
) -> Result<CompositeValue, NodeGraphError> {
    let invalid = || {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "composite config field is missing or invalid",
        )
    };
    let parsed = match kind {
        ConfigValueKind::Uuid => value
            .as_str()
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
            .map(CompositeValue::Uuid),
        ConfigValueKind::ColumnId => value
            .as_str()
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
            .map(|parsed| CompositeValue::ColumnId(ColumnId::from_uuid(parsed))),
        ConfigValueKind::ColumnIdList => value.as_array().and_then(|items| {
            let mut columns = Vec::with_capacity(items.len());
            for item in items {
                columns.push(ColumnId::from_uuid(
                    uuid::Uuid::parse_str(item.as_str()?).ok()?,
                ));
            }
            Some(CompositeValue::ColumnIdList(columns))
        }),
        ConfigValueKind::String => value
            .as_str()
            .map(|raw| CompositeValue::String(raw.to_owned())),
        ConfigValueKind::Boolean => value.as_bool().map(CompositeValue::Boolean),
        ConfigValueKind::Expression => serde_json::from_value::<Expr>(value.clone())
            .ok()
            .map(CompositeValue::Expression),
        ConfigValueKind::LogicalType => serde_json::from_value::<LogicalType>(value.clone())
            .ok()
            .map(CompositeValue::LogicalType),
        ConfigValueKind::ScalarValue => serde_json::from_value::<ScalarValue>(value.clone())
            .ok()
            .map(CompositeValue::Scalar),
        ConfigValueKind::CastFailurePolicy => {
            serde_json::from_value::<CastFailurePolicy>(value.clone())
                .ok()
                .map(CompositeValue::CastFailurePolicy)
        }
    };
    parsed.ok_or_else(invalid)
}

/// Validates a composite node's config against the package's declared
/// config schema and resolves the expansion into atomic validated configs
/// through `registry`. This is the one composite branch in the traversal;
/// adding an atomic node type still requires no traversal change (NX-N1).
pub(crate) fn resolve_composite(
    node: &NodeConfig,
    expansion: &[CompositeStep],
    config_schema: &ConfigSchema,
    registry: &NodeRegistry,
) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let object = node.config.as_object().ok_or_else(|| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "node config must be an object",
        )
    })?;
    let mut values: BTreeMap<String, CompositeValue> = BTreeMap::new();
    for field in &config_schema.fields {
        match object.get(&field.name) {
            Some(value) => {
                if value.is_null() {
                    return Err(field_error(node, &field.name));
                }
                values.insert(
                    field.name.clone(),
                    parse_composite_value(node, field.value_kind, value)?,
                );
            }
            None if field.required => return Err(field_error(node, &field.name)),
            None => {}
        }
    }
    for key in object.keys() {
        if !config_schema
            .fields
            .iter()
            .any(|field| field.name == key.as_str())
        {
            return Err(field_error(node, key));
        }
    }

    // The identity binding addresses the composite's `column` parameter.
    let identity_column = match values.get("column") {
        Some(CompositeValue::ColumnId(column)) => *column,
        _ => {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(node.id()),
                "composite expansion requires a column parameter",
            )
            .with_field_path("column"))
        }
    };

    let mut steps = Vec::with_capacity(expansion.len());
    for (ordinal, step) in expansion.iter().enumerate() {
        let atomic = registry.lookup(&step.operator, 1).map_err(|error| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(node.id()),
                format!(
                    "composite step {} targets an unknown operator: {}",
                    ordinal,
                    error.message()
                ),
            )
        })?;
        let mut step_config = serde_json::Map::new();
        for (field_name, binding) in &step.bindings {
            let value = match binding {
                CompositeBinding::Identity => {
                    if field_name != "column" {
                        return Err(NodeGraphError::new(
                            NodeGraphErrorCode::InvalidConfig,
                            Some(node.id()),
                            "identity bindings resolve to the column parameter",
                        ));
                    }
                    identity_column.as_uuid().to_string().into()
                }
                CompositeBinding::Literal(value) => value.clone(),
            };
            step_config.insert(field_name.clone(), value);
        }
        let synthetic = NodeConfig::new(
            node.id(),
            &step.operator,
            1,
            Value::Object(step_config),
            BTreeMap::new(),
        )?;
        let validated = atomic.validate_node_config(&synthetic).map_err(|error| {
            NodeGraphError::new(
                error.code(),
                Some(node.id()),
                format!("composite step {} rejected: {}", ordinal, error.message()),
            )
            .with_field_path(format!("steps[{ordinal}]"))
        })?;
        steps.push(validated);
    }
    Ok(ValidatedNodeConfig::Composite { steps })
}

fn field_error(node: &NodeConfig, field: &str) -> NodeGraphError {
    NodeGraphError::new(
        NodeGraphErrorCode::InvalidConfig,
        Some(node.id()),
        format!("node config field {field} is missing or invalid"),
    )
    .with_field_path(field)
}

/// Builds the `NodeDefinition` for a validated package. The composite
/// validator resolves against the registry at traversal time, so the
/// definition carries the expansion data instead of a function pointer.
pub(crate) fn package_definition(package: &NodePackage) -> NodeDefinition {
    NodeDefinition::new_composite(
        &package.type_id,
        package.config_version,
        &package.definition.display_name,
        &package.definition.description,
        package.definition.config_schema.clone(),
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        super::definition::NodeLoweringTarget::ApplyRules,
        package.definition.support_conditions.clone(),
        package.definition.expansion.clone(),
    )
}

/// Validates a package document against the NX-C1 frozen rules (§6). The
/// content digest is structural here and cryptographically verified at the
/// deployment surface, which owns SHA-256.
pub(crate) fn validate_package(package: &NodePackage) -> Result<(), NodeGraphError> {
    let invalid = |message: String| {
        Err(NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            None,
            message,
        ))
    };
    if package.package_format != 1 {
        return invalid("package format is not supported".to_owned());
    }
    // Canonical content bound (NX-C1 §8): the package minus its digest field
    // is measured the same way the digest law measures it.
    let mut content = serde_json::to_value(package).map_err(|_| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            None,
            "package document is not representable",
        )
    })?;
    if let Some(fields) = content.as_object_mut() {
        fields.remove("contentDigest");
    }
    if serde_json::to_string(&content)
        .map(|encoded| encoded.len())
        .unwrap_or(MAX_PACKAGE_BYTES + 1)
        > MAX_PACKAGE_BYTES
    {
        return invalid("package document exceeds the frozen byte bound".to_owned());
    }
    if !package
        .namespace
        .chars()
        .next()
        .map(|first| first.is_ascii_lowercase())
        .unwrap_or(false)
        || package.namespace.len() > 64
        || !package.namespace.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return invalid("package namespace is invalid".to_owned());
    }
    if !valid_semver(&package.version) {
        return invalid("package version is not strict semver".to_owned());
    }
    if !package.depends_on.is_empty() {
        return invalid("package dependencies are not authorized".to_owned());
    }
    if package.operators.len() > MAX_PACKAGE_OPERATORS {
        return invalid("package operator list exceeds the frozen bound".to_owned());
    }
    for operator in &package.operators {
        if !ADMISSIBLE_OPERATORS.contains(&operator.as_str()) {
            return invalid(format!("package operator {operator} is not admissible"));
        }
    }
    if !package.type_id.starts_with("stillflow.composite.")
        || package.type_id != format!("stillflow.composite.{}", package.name)
    {
        return invalid("package type id must be stillflow.composite.<name>".to_owned());
    }
    if package.config_version != 1 {
        return invalid("package config version must be 1".to_owned());
    }
    if package.definition.expansion.is_empty()
        || package.definition.expansion.len() > MAX_EXPANSION_STEPS
    {
        return invalid("package expansion exceeds the frozen step bound".to_owned());
    }
    for step in &package.definition.expansion {
        if !package.operators.contains(&step.operator) {
            return invalid(format!(
                "expansion step targets operator {} outside the package's declared operators",
                step.operator
            ));
        }
    }
    if package.content_digest.len() != 71 || !package.content_digest.starts_with("sha256-") {
        return invalid("package content digest must be sha256-<64 hex>".to_owned());
    }
    // The config schema must validate a `column` parameter: the identity
    // binding requires exactly one required ColumnId field named `column`
    // (NX-C1 §5, single-column schema contract).
    let column_fields: Vec<&ConfigField> = package
        .definition
        .config_schema
        .fields
        .iter()
        .filter(|field| field.name == "column")
        .collect();
    if column_fields.len() != 1
        || !column_fields[0].required
        || column_fields[0].value_kind != ConfigValueKind::ColumnId
    {
        return invalid(
            "composite packages require one required `column` (columnId) field".to_owned(),
        );
    }
    Ok(())
}

fn valid_semver(version: &str) -> bool {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    parts.iter().all(|part| {
        !part.is_empty()
            && part.len() <= 6
            && part.chars().all(|character| character.is_ascii_digit())
    })
}

/// The deployed package manifest: the compiled-in, operator-audited list
/// (NX-C1 §6.2). Deployment is explicit and static; there is no runtime
/// upload path.
pub fn deployed_packages() -> Vec<NodePackage> {
    vec![trim_clean_package()]
}

/// The frozen first composite sample (NX-C1 §7): trim then empty-string→
/// null on one UTF-8 column.
pub fn trim_clean_package() -> NodePackage {
    NodePackage {
        package_format: 1,
        namespace: "ops-clean".to_owned(),
        name: "trim-clean".to_owned(),
        version: "1.0.0".to_owned(),
        content_digest: "sha256-997734d5099ca8d632aaeb652e59fa2b167da7c347b32b7df451365ace367e60"
            .to_owned(),
        depends_on: Vec::new(),
        operators: vec![
            "stillflow.node.trim".to_owned(),
            "stillflow.node.replace-literal".to_owned(),
        ],
        type_id: "stillflow.composite.trim-clean".to_owned(),
        config_version: 1,
        definition: NodePackageDefinition {
            display_name: "Trim clean".to_owned(),
            description: "Trim one UTF-8 column, then map empty strings to null.".to_owned(),
            config_schema: ConfigSchema {
                fields: vec![ConfigField {
                    name: "column".to_owned(),
                    value_kind: ConfigValueKind::ColumnId,
                    required: true,
                    constraints: None,
                }],
                additional_properties: false,
            },
            expansion: vec![
                CompositeStep {
                    operator: "stillflow.node.trim".to_owned(),
                    bindings: BTreeMap::from([("column".to_owned(), CompositeBinding::Identity)]),
                },
                CompositeStep {
                    operator: "stillflow.node.replace-literal".to_owned(),
                    bindings: BTreeMap::from([
                        ("column".to_owned(), CompositeBinding::Identity),
                        (
                            "from".to_owned(),
                            CompositeBinding::Literal(
                                serde_json::json!({"kind": "utf8", "value": ""}),
                            ),
                        ),
                        (
                            "to".to_owned(),
                            CompositeBinding::Literal(serde_json::json!({"kind": "null"})),
                        ),
                    ]),
                },
            ],
            support_conditions: vec![PortSupportConditions {
                port: PortId::from_static("in"),
                conditions: vec![
                    super::definition::SupportCondition::RequiresExecutableType,
                    super::definition::SupportCondition::RequiresType(LogicalType::Utf8),
                ],
            }],
        },
    }
}
