//! `stillflow.node.normalize-text` — apply one deterministic text
//! normalization operation to one UTF-8 column.
//!
//! Several operations are composed by chaining nodes, because the product law
//! keeps exactly one rule per node (`MAX_RULES_PER_NODE = 1`); the chain order
//! is the operation order. No operation executes user code.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, TextOperation};

use super::definition::{
    config_field, parse_config, validate_column, ConfigConstraints, ConfigSchema, ConfigValueKind,
    NodeDefinition, NodeLoweringTarget, NodeRole, PortSupportConditions, SupportCondition,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

/// The closed wire vocabulary of operations, in catalog order.
pub(crate) const OPERATION_VALUES: [&str; 5] = [
    "trim",
    "collapseWhitespace",
    "lowercase",
    "uppercase",
    "unicodeNfc",
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalizeTextConfigData {
    column: ColumnId,
    operation: TextOperation,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: NormalizeTextConfigData =
        parse_config(node, &[("column", true), ("operation", true)], &[])?;
    validate_column(node, data.column, "column")?;
    Ok(ValidatedNodeConfig::NormalizeText {
        column: data.column,
        operation: data.operation,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.normalize-text",
        1,
        "Normalize text",
        "Apply one deterministic text normalization to one UTF-8 column.",
        ConfigSchema {
            fields: vec![
                config_field("column", ConfigValueKind::ColumnId, true, None),
                config_field(
                    "operation",
                    ConfigValueKind::String,
                    true,
                    Some(ConfigConstraints::default_for().with_enum_values(&OPERATION_VALUES)),
                ),
            ],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        NodeLoweringTarget::ApplyRules,
        NodeRole::Transform,
        vec![PortSupportConditions {
            port: PortId::from_static("in"),
            conditions: vec![
                SupportCondition::RequiresExecutableType,
                SupportCondition::RequiresType(crate::LogicalType::Utf8),
            ],
        }],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({
            "column": "00000000-0000-0000-0000-000000000002",
            "operation": "collapseWhitespace"
        })),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002"}),
            "operation",
            "required",
        ),
        invalid_sample(json!({"operation": "trim"}), "column", "required"),
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000002",
                "operation": "shout"
            }),
            "operation",
            "enumValues",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000000", "operation": "trim"}),
            "column",
            "valueKind",
        ),
    ]
}
