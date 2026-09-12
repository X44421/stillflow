//! `stillflow.node.rename` — rename one logical column without changing its
//! identity.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, MAX_STRING_BYTES};

use super::definition::{
    config_field, executable_input, parse_config, validate_column, validate_name,
    ConfigConstraints, ConfigSchema, ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenameConfigData {
    column: ColumnId,
    to: String,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: RenameConfigData = parse_config(node, &[("column", true), ("to", true)], &[])?;
    validate_column(node, data.column, "column")?;
    validate_name(node, &data.to, "to")?;
    Ok(ValidatedNodeConfig::Rename {
        column: data.column,
        to: data.to,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.rename",
        1,
        "Rename",
        "Rename one logical column without changing its identity.",
        ConfigSchema {
            fields: vec![
                config_field("column", ConfigValueKind::ColumnId, true, None),
                config_field(
                    "to",
                    ConfigValueKind::String,
                    true,
                    Some(
                        ConfigConstraints::default_for()
                            .non_empty()
                            .with_byte_bound(MAX_STRING_BYTES),
                    ),
                ),
            ],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        NodeLoweringTarget::ApplyRules,
        NodeRole::Transform,
        vec![executable_input("in")],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({"column": "00000000-0000-0000-0000-000000000002", "to": "label"})),
        invalid_sample(json!({"to": "label"}), "column", "required"),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002"}),
            "to",
            "required",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "to": "   "}),
            "to",
            "nonEmpty",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "to": 1}),
            "to",
            "valueKind",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000000", "to": "label"}),
            "column",
            "valueKind",
        ),
    ]
}
