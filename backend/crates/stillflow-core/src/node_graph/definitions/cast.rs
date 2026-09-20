//! `stillflow.node.cast` — cast one logical column with an explicit failure
//! policy.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::{ColumnId, LogicalType};

use super::definition::{
    config_field, executable_input, invalid_config, parse_config, validate_column,
    validate_logical_type, ConfigConstraints, ConfigSchema, ConfigValueKind, NodeDefinition,
    NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CastConfigData {
    column: ColumnId,
    data_type: LogicalType,
    on_failure: crate::CastFailurePolicy,
    format: Option<String>,
    timezone: Option<String>,
}

fn is_temporal(data_type: &LogicalType) -> bool {
    matches!(
        data_type,
        LogicalType::Date32 | LogicalType::Timestamp { .. }
    )
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: CastConfigData = parse_config(
        node,
        &[("column", true), ("dataType", true), ("onFailure", true)],
        &["format", "timezone"],
    )?;
    validate_column(node, data.column, "column")?;
    validate_logical_type(node, &data.data_type, "dataType")?;

    // A format is a temporal parsing instruction: it is refused for any other
    // target so the configuration can never imply an implicit conversion.
    if let Some(format) = &data.format {
        if !is_temporal(&data.data_type) {
            return Err(invalid_config(node, "format"));
        }
        if format.trim().is_empty() {
            return Err(invalid_config(node, "format"));
        }
    }
    // A declared timezone is refused for every target. Applying one correctly
    // needs the `timezones` Polars feature, which changes an unrelated frozen
    // ingestion behaviour (zoned columns fail closed at the CSV bridge today);
    // refusing it here is what keeps "no implicit timezone" true.
    if data.timezone.is_some() {
        return Err(invalid_config(node, "timezone"));
    }

    Ok(ValidatedNodeConfig::Cast {
        column: data.column,
        data_type: data.data_type,
        on_failure: data.on_failure,
        format: data.format,
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
                config_field(
                    "format",
                    ConfigValueKind::String,
                    false,
                    Some(
                        ConfigConstraints::default_for()
                            .non_empty()
                            .with_byte_bound(crate::MAX_STRING_BYTES),
                    ),
                ),
                config_field(
                    "timezone",
                    ConfigValueKind::String,
                    false,
                    Some(
                        ConfigConstraints::default_for()
                            .non_empty()
                            .with_byte_bound(crate::MAX_STRING_BYTES),
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
        // Optional temporal parsing: a declared format is accepted for a date
        // target, and a timezone only for a timestamp target.
        valid_sample(json!({
            "column": "00000000-0000-0000-0000-000000000002",
            "dataType": {"kind": "date32"},
            "onFailure": "error",
            "format": "%Y-%m-%d"
        })),
        valid_sample(json!({
            "column": "00000000-0000-0000-0000-000000000002",
            "dataType": {"kind": "timestamp", "value": {"unit": "millisecond", "timezone": null}},
            "onFailure": "setNull",
            "format": "%Y-%m-%dT%H:%M:%S"
        })),
        // The optional temporal fields advertise `nonEmpty`; an empty format or
        // timezone is refused. The cross-field laws (a format is only valid for
        // a temporal target, and a timezone is refused for every target) are
        // covered by the real-data suite, which cannot be expressed as a
        // per-field constraint.
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000002",
                "dataType": {"kind": "date32"},
                "onFailure": "error",
                "format": ""
            }),
            "format",
            "nonEmpty",
        ),
        invalid_sample(
            json!({
                "column": "00000000-0000-0000-0000-000000000002",
                "dataType": {"kind": "timestamp", "value": {"unit": "millisecond", "timezone": null}},
                "onFailure": "error",
                "format": "%Y-%m-%dT%H:%M:%S",
                "timezone": ""
            }),
            "timezone",
            "nonEmpty",
        ),
    ]
}
