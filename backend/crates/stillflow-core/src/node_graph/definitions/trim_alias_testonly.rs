//! Test-only definition proving that adding a node type needs no change to
//! the generic graph traversal. It reuses the trim validator under a
//! different type id; it is compiled only under `cfg(test)` and never
//! enters the production catalog (NX-N1 acceptance).

use serde::Deserialize;
#[cfg(test)]
use crate::ColumnId;

use super::definition::{
    config_field, executable_input, parse_config, validate_column, ConfigSchema, ConfigValueKind,
    NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnConfigData {
    column: ColumnId,
}

fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: ColumnConfigData = parse_config(node, &[("column", true)], &[])?;
    validate_column(node, data.column, "column")?;
    Ok(ValidatedNodeConfig::Trim {
        column: data.column,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.test.trim-alias",
        1,
        "Trim alias (test only)",
        "Test-only alias proving definition-driven extensibility.",
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
        vec![executable_input("in")],
        validate,
    )
}

