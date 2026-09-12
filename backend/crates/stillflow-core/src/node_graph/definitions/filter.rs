//! `stillflow.node.filter` — filter rows with a logical predicate.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::Expr;

use super::definition::{
    config_field, executable_input, parse_config, validate_expression, ConfigSchema,
    ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterConfigData {
    predicate: Expr,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: FilterConfigData = parse_config(node, &[("predicate", true)], &[])?;
    validate_expression(node, &data.predicate, "predicate")?;
    Ok(ValidatedNodeConfig::Filter {
        predicate: data.predicate,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.filter",
        1,
        "Filter",
        "Filter rows with a logical predicate.",
        ConfigSchema {
            fields: vec![config_field(
                "predicate",
                ConfigValueKind::Expression,
                true,
                None,
            )],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        NodeLoweringTarget::Filter,
        NodeRole::Transform,
        vec![executable_input("in")],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    let predicate = serde_json::json!({
        "kind": "binary",
        "value": {
            "left": {"kind": "column", "value": "00000000-0000-0000-0000-000000000002"},
            "operator": "equal",
            "right": {
                "kind": "literal",
                "value": {"kind": "int64", "value": 1}
            }
        }
    });
    vec![
        valid_sample(json!({"predicate": predicate})),
        invalid_sample(json!({}), "predicate", "required"),
        invalid_sample(json!({"predicate": 1}), "predicate", "valueKind"),
        invalid_sample(
            json!({"predicate": predicate, "extra": true}),
            "extra",
            "additionalProperties",
        ),
    ]
}
