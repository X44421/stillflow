//! `stillflow.node.output` — declare the logical materialization label.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::MAX_STRING_BYTES;

use super::definition::{
    config_field, parse_config, validate_name, ConfigConstraints, ConfigSchema, ConfigValueKind,
    NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutputConfigData {
    output_label: String,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: OutputConfigData = parse_config(node, &[("outputLabel", true)], &[])?;
    validate_name(node, &data.output_label, "outputLabel")?;
    Ok(ValidatedNodeConfig::Output {
        output_label: data.output_label,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.output",
        1,
        "Output",
        "Declare the logical materialization label.",
        ConfigSchema {
            fields: vec![config_field(
                "outputLabel",
                ConfigValueKind::String,
                true,
                Some(
                    ConfigConstraints::default_for()
                        .non_empty()
                        .with_byte_bound(MAX_STRING_BYTES),
                ),
            )],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        Vec::new(),
        NodeLoweringTarget::Materialize,
        NodeRole::Output,
        Vec::new(),
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({"outputLabel": "cleaned"})),
        invalid_sample(json!({}), "outputLabel", "required"),
        invalid_sample(json!({"outputLabel": ""}), "outputLabel", "nonEmpty"),
        invalid_sample(json!({"outputLabel": 1}), "outputLabel", "valueKind"),
    ]
}
