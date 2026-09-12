//! `stillflow.node.replace-literal` — replace one typed literal in a column.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, ScalarValue};

use super::definition::{
    config_field, executable_input, parse_config, validate_column, validate_scalar, ConfigSchema,
    ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReplaceLiteralConfigData {
    column: ColumnId,
    from: ScalarValue,
    to: ScalarValue,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: ReplaceLiteralConfigData =
        parse_config(node, &[("column", true), ("from", true), ("to", true)], &[])?;
    validate_column(node, data.column, "column")?;
    validate_scalar(node, &data.from, "from")?;
    validate_scalar(node, &data.to, "to")?;
    Ok(ValidatedNodeConfig::ReplaceLiteral {
        column: data.column,
        from: data.from,
        to: data.to,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.replace-literal",
        1,
        "Replace literal",
        "Replace one typed literal in a column.",
        ConfigSchema {
            fields: vec![
                config_field("column", ConfigValueKind::ColumnId, true, None),
                config_field("from", ConfigValueKind::ScalarValue, true, None),
                config_field("to", ConfigValueKind::ScalarValue, true, None),
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
    let column = "00000000-0000-0000-0000-000000000002";
    vec![
        valid_sample(
            json!({"column": column, "from": {"kind": "utf8", "value": "x"}, "to": {"kind": "utf8", "value": "y"}}),
        ),
        invalid_sample(
            json!({"to": {"kind": "utf8", "value": "y"}}),
            "column",
            "required",
        ),
        invalid_sample(
            json!({"column": column, "to": {"kind": "utf8", "value": "y"}}),
            "from",
            "required",
        ),
        invalid_sample(
            json!({"column": column, "from": {"kind": "utf8", "value": "x"}}),
            "to",
            "required",
        ),
        invalid_sample(
            json!({"column": column, "from": {"kind": "utf8", "value": "x"}, "to": 1}),
            "to",
            "valueKind",
        ),
        invalid_sample(
            json!({"column": column, "from": 1, "to": {"kind": "utf8", "value": "y"}}),
            "from",
            "valueKind",
        ),
    ]
}
