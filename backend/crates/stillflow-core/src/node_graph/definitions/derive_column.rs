//! `stillflow.node.derive-column` — append one caller-identified column
//! from a logical expression.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, Expr, LogicalType, MAX_STRING_BYTES};

use super::definition::{
    config_field, executable_input, parse_config, validate_column, validate_expression,
    validate_logical_type, validate_name, ConfigConstraints, ConfigSchema, ConfigValueKind,
    NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeriveColumnConfigData {
    id: ColumnId,
    name: String,
    data_type: LogicalType,
    nullable: bool,
    expression: Expr,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: DeriveColumnConfigData = parse_config(
        node,
        &[
            ("id", true),
            ("name", true),
            ("dataType", true),
            ("nullable", true),
            ("expression", true),
        ],
        &[],
    )?;
    validate_column(node, data.id, "id")?;
    validate_name(node, &data.name, "name")?;
    validate_logical_type(node, &data.data_type, "dataType")?;
    validate_expression(node, &data.expression, "expression")?;
    Ok(ValidatedNodeConfig::DeriveColumn {
        id: data.id,
        name: data.name,
        data_type: data.data_type,
        nullable: data.nullable,
        expression: data.expression,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.derive-column",
        1,
        "Derive column",
        "Append one caller-identified column from a logical expression.",
        ConfigSchema {
            fields: vec![
                config_field("id", ConfigValueKind::ColumnId, true, None),
                config_field(
                    "name",
                    ConfigValueKind::String,
                    true,
                    Some(
                        ConfigConstraints::default_for()
                            .non_empty()
                            .with_byte_bound(MAX_STRING_BYTES),
                    ),
                ),
                config_field("dataType", ConfigValueKind::LogicalType, true, None),
                config_field("nullable", ConfigValueKind::Boolean, true, None),
                config_field("expression", ConfigValueKind::Expression, true, None),
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
    let expression = json!({
        "kind": "column",
        "value": "00000000-0000-0000-0000-000000000002"
    });
    vec![
        valid_sample(json!({
            "id": "00000000-0000-0000-0000-000000000010",
            "name": "copy",
            "dataType": {"kind": "int64"},
            "nullable": true,
            "expression": expression
        })),
        invalid_sample(
            json!({
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": expression
            }),
            "id",
            "required",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": expression
            }),
            "name",
            "required",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "copy",
                "nullable": true,
                "expression": expression
            }),
            "dataType",
            "required",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "copy",
                "dataType": {"kind": "int64"},
                "expression": expression
            }),
            "nullable",
            "required",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": true
            }),
            "expression",
            "required",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "   ",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": expression
            }),
            "name",
            "nonEmpty",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": expression
            }),
            "id",
            "valueKind",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": 1
            }),
            "expression",
            "valueKind",
        ),
        invalid_sample(
            json!({
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": "yes",
                "expression": expression
            }),
            "nullable",
            "valueKind",
        ),
    ]
}
