//! `stillflow.node.drop-column` — remove one logical column.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::ColumnId;

use super::definition::{
    config_field, parse_config, validate_column, ConfigSchema, ConfigValueKind, NodeDefinition,
    NodeLoweringTarget, NodeRole, PortSupportConditions, SupportCondition,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnConfigData {
    column: ColumnId,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: ColumnConfigData = parse_config(node, &[("column", true)], &[])?;
    validate_column(node, data.column, "column")?;
    Ok(ValidatedNodeConfig::DropColumn {
        column: data.column,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.drop-column",
        1,
        "Drop column",
        "Remove one logical column.",
        ConfigSchema {
            fields: vec![config_field(
                "column",
                ConfigValueKind::ColumnId,
                true,
                None,
            )],
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
                // The node may not remove the final remaining field; the
                // compiler enforces the same width bound.
                SupportCondition::MinFields(2),
            ],
        }],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({"column": "00000000-0000-0000-0000-000000000002"})),
        invalid_sample(json!({}), "column", "required"),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000000"}),
            "column",
            "valueKind",
        ),
    ]
}
