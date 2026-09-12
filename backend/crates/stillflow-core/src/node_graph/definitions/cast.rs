//! `stillflow.node.cast` — cast one logical column with an explicit failure
//! policy.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, LogicalType};

use super::definition::{
    config_field, executable_input, parse_config, validate_column, validate_logical_type,
    ConfigConstraints, ConfigSchema, ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CastConfigData {
    column: ColumnId,
    data_type: LogicalType,
    on_failure: crate::CastFailurePolicy,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: CastConfigData = parse_config(
        node,
        &[("column", true), ("dataType", true), ("onFailure", true)],
        &[],
    )?;
    validate_column(node, data.column, "column")?;
    validate_logical_type(node, &data.data_type, "dataType")?;
    Ok(ValidatedNodeConfig::Cast {
        column: data.column,
        data_type: data.data_type,
        on_failure: data.on_failure,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.cast",
        1,
        "Cast",
        "Cast one logical column with an explicit failure policy.",
        ConfigSchema {
            fields: vec![
                config_field("column", ConfigValueKind::ColumnId, true, None),
                config_field("dataType", ConfigValueKind::LogicalType, true, None),
                config_field(
                    "onFailure",
                    ConfigValueKind::CastFailurePolicy,
                    true,
                    Some(ConfigConstraints::default_for().with_enum_values(&["setNull", "error"])),
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
        valid_sample(json!({
            "column": "00000000-0000-0000-0000-000000000002",
            "dataType": {"kind": "utf8"},
            "onFailure": "setNull"
        })),
        invalid_sample(
            json!({"dataType": {"kind": "utf8"}, "onFailure": "error"}),
            "column",
            "required",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "onFailure": "error"}),
            "dataType",
            "required",
        ),
        invalid_sample(
            json!({"column": "00000000-0000-0000-0000-000000000002", "dataType": {"kind": "utf8"}}),
            "onFailure",
            "required",
        ),
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000002",
                "dataType": {"kind": "utf8"},
                "onFailure": "panic"
            }),
            "onFailure",
            "enumValues",
        ),
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000002",
                "dataType": {"kind": "nonsense"},
                "onFailure": "error"
            }),
            "dataType",
            "valueKind",
        ),
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000000",
                "dataType": {"kind": "utf8"},
                "onFailure": "error"
            }),
            "column",
            "valueKind",
        ),
    ]
}
