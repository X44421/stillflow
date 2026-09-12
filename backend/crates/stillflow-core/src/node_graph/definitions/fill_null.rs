//! `stillflow.node.fill-null` — fill null values with one non-null literal.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, ScalarValue};

use super::definition::{
    config_field, executable_input, invalid_config, parse_config, validate_column, validate_scalar,
    ConfigConstraints, ConfigSchema, ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FillNullConfigData {
    column: ColumnId,
    value: ScalarValue,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: FillNullConfigData = parse_config(node, &[("column", true), ("value", true)], &[])?;
    validate_column(node, data.column, "column")?;
    if matches!(data.value, ScalarValue::Null) {
        return Err(invalid_config(node, "value"));
    }
    validate_scalar(node, &data.value, "value")?;
    Ok(ValidatedNodeConfig::FillNull {
        column: data.column,
        value: data.value,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.fill-null",
        1,
        "Fill null",
        "Fill null values with one non-null literal.",
        ConfigSchema {
            fields: vec![
                config_field("column", ConfigValueKind::ColumnId, true, None),
                config_field(
                    "value",
                    ConfigValueKind::ScalarValue,
                    true,
                    Some(ConfigConstraints::default_for().non_null()),
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
        valid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "value": {"kind": "int64", "value": 0}}),
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "value": {"kind": "null"}}),
            "value",
            "nonNull",
        ),
        invalid_sample(
            json!({"value": {"kind": "int64", "value": 0}}),
            "column",
            "required",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002"}),
            "value",
            "required",
        ),
    ]
}
