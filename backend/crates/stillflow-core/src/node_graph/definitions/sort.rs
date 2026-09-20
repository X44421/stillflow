//! `stillflow.node.sort` — stable ordering over explicit key columns.
//!
//! Ordering is a property of the relation, not of a row, so this node lowers to
//! a first-class `PlanNodeKind::Sort` rather than to a per-row rule
//! (contract `docs/contracts/issue-370-nx-s0-shared-sort-contract.md` §1–§2).

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;

use crate::ColumnId;

use super::definition::{
    config_field, invalid_config, parse_config, ConfigConstraints, ConfigSchema, ConfigValueKind,
    NodeDefinition, NodeLoweringTarget, NodeRole, PortSupportConditions, SupportCondition,
};
use super::{NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};
use crate::node_graph::{NodeGraphErrorCode, NullPlacement, SortDirection, SortKey};

/// The contract's schema law for the number of ordering keys.
const MIN_SORT_KEYS: usize = 1;
const MAX_SORT_KEYS: usize = 8;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SortConfigData {
    keys: Vec<SortKeyData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SortKeyData {
    column: ColumnId,
    direction: SortDirection,
    nulls: NullPlacement,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: SortConfigData = parse_config(node, &[("keys", true)], &[])?;

    if data.keys.len() < MIN_SORT_KEYS || data.keys.len() > MAX_SORT_KEYS {
        return Err(NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            format!("sort keys must number between {MIN_SORT_KEYS} and {MAX_SORT_KEYS}"),
        )
        .with_field_path("keys"));
    }

    let mut keys = Vec::with_capacity(data.keys.len());
    for key in data.keys {
        // The advertised `ColumnIdList` constraint promises a non-nil column
        // and no repeats, so the validator must enforce exactly that.
        if key.column.as_uuid().is_nil() {
            return Err(invalid_config(node, "keys"));
        }
        if keys
            .iter()
            .any(|existing: &SortKey| existing.column == key.column)
        {
            return Err(invalid_config(node, "keys"));
        }
        keys.push(SortKey {
            column: key.column,
            direction: key.direction,
            nulls: key.nulls,
        });
    }

    Ok(ValidatedNodeConfig::Sort { keys })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.sort",
        1,
        "Sort",
        "Stable ordering over explicit key columns.",
        ConfigSchema {
            fields: vec![config_field(
                "keys",
                ConfigValueKind::ColumnIdList,
                true,
                Some(ConfigConstraints {
                    min_items: Some(MIN_SORT_KEYS),
                    max_items: Some(MAX_SORT_KEYS),
                    ..ConfigConstraints::default()
                }),
            )],
            additional_properties: false,
        },
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
        NodeLoweringTarget::Sort,
        NodeRole::Transform,
        vec![PortSupportConditions {
            port: PortId::from_static("in"),
            conditions: vec![
                SupportCondition::RequiresExecutableType,
                // Ordering needs at least one key column to exist.
                SupportCondition::MinFields(1),
            ],
        }],
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({
            "keys": [
                {
                    "column": "00000000-0000-0000-0000-000000000002",
                    "direction": "ascending",
                    "nulls": "last"
                }
            ]
        })),
        // A second key, exercising the multi-key shape.
        valid_sample(json!({
            "keys": [
                {
                    "column": "00000000-0000-0000-0000-000000000002",
                    "direction": "descending",
                    "nulls": "first"
                },
                {
                    "column": "00000000-0000-0000-0000-000000000003",
                    "direction": "ascending",
                    "nulls": "last"
                }
            ]
        })),
        invalid_sample(json!({}), "keys", "required"),
        invalid_sample(json!({"keys": []}), "keys", "minItems"),
        invalid_sample(
            json!({"keys": [
                {"column": "00000000-0000-0000-0000-000000000002", "direction": "ascending"},
            ]}),
            "keys",
            "required",
        ),
        invalid_sample(
            json!({"keys": [
                {"column": "00000000-0000-0000-0000-000000000000", "direction": "ascending", "nulls": "last"},
            ]}),
            "keys",
            "valueKind",
        ),
    ]
}
