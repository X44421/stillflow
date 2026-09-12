//! `stillflow.node.select` — keep an ordered set of existing columns.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use super::definition::{
    config_field, executable_input, invalid_config, parse_config, validate_columns,
    ConfigConstraints, ConfigSchema, ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{ColumnId, NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SelectConfigData {
    columns: Vec<ColumnId>,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: SelectConfigData = parse_config(node, &[("columns", true)], &[])?;
    if data.columns.is_empty() {
        return Err(invalid_config(node, "columns"));
    }
    validate_columns(node, &data.columns, "columns")?;
    Ok(ValidatedNodeConfig::Select {
        columns: data.columns,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.select",
        1,
        "Select",
        "Keep an ordered set of existing columns.",
        ConfigSchema {
            fields: vec![config_field(
                "columns",
                ConfigValueKind::ColumnIdList,
                true,
                Some(
                    ConfigConstraints::default_for()
                        .with_list_bounds(1, true)
                        .ordered(),
                ),
            )],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        NodeLoweringTarget::Project,
        NodeRole::Transform,
        vec![executable_input("in")],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({"columns": ["00000000-0000-0000-0000-000000000002"]})),
        invalid_sample(json!({}), "columns", "required"),
        invalid_sample(json!({"columns": []}), "columns", "minItems"),
        invalid_sample(
            json!({"columns": [
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000002"
            ]}),
            "columns",
            "uniqueItems",
        ),
        invalid_sample(
            json!({"columns": ["00000000-0000-0000-0000-000000000000"]}),
            "columns",
            "valueKind",
        ),
    ]
}
