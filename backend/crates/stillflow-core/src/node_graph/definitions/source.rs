//! `stillflow.node.source` — authorized source asset input.

use serde::Deserialize;
#[cfg(test)]
use serde_json::json;
use uuid::Uuid;

use super::definition::{
    config_field, invalid_config, parse_config, validate_columns, ConfigConstraints, ConfigSchema,
    ConfigValueKind, NodeDefinition, NodeLoweringTarget, NodeRole,
};
use super::{ColumnId, NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceConfigData {
    source_asset_id: Uuid,
    #[serde(default)]
    projection: Option<Vec<ColumnId>>,
}

pub(super) fn validate(node: &NodeConfig) -> Result<ValidatedNodeConfig, NodeGraphError> {
    let data: SourceConfigData = parse_config(node, &[("sourceAssetId", true)], &["projection"])?;
    if data.source_asset_id.is_nil() {
        return Err(invalid_config(node, "sourceAssetId"));
    }
    if node
        .config
        .get("projection")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(invalid_config(node, "projection"));
    }
    if let Some(projection) = &data.projection {
        if projection.is_empty() {
            return Err(invalid_config(node, "projection"));
        }
        validate_columns(node, projection, "projection")?;
    }
    Ok(ValidatedNodeConfig::Source {
        source_asset_id: data.source_asset_id,
        projection: data.projection,
    })
}

pub(super) fn definition() -> NodeDefinition {
    NodeDefinition::new(
        "stillflow.node.source",
        1,
        "Source",
        "Authorized source asset input.",
        ConfigSchema {
            fields: vec![
                config_field("sourceAssetId", ConfigValueKind::Uuid, true, None),
                config_field(
                    "projection",
                    ConfigValueKind::ColumnIdList,
                    false,
                    Some(
                        ConfigConstraints::default_for()
                            .with_list_bounds(1, true)
                            .ordered()
                            .non_null(),
                    ),
                ),
            ],
            additional_properties: false,
        },
        Vec::new(),
        vec![PortId::from_static("out")],
        NodeLoweringTarget::Scan,
        NodeRole::Source,
        Vec::new(),
        validate,
    )
}

#[cfg(test)]
pub(super) fn samples() -> Vec<super::ConfigSample> {
    use super::{invalid_sample, valid_sample};
    vec![
        valid_sample(json!({"sourceAssetId": "00000000-0000-0000-0000-000000000001"})),
        valid_sample(json!({
            "sourceAssetId": "00000000-0000-0000-0000-000000000001",
            "projection": ["00000000-0000-0000-0000-000000000002"]
        })),
        invalid_sample(
            json!({"projection": ["00000000-0000-0000-0000-000000000002"]}),
            "sourceAssetId",
            "required",
        ),
        invalid_sample(
            json!({"sourceAssetId": "00000000-0000-0000-0000-000000000001", "projection": []}),
            "projection",
            "minItems",
        ),
        invalid_sample(
            json!({
                "sourceAssetId": "00000000-0000-0000-0000-000000000001",
                "projection": [
                    "00000000-0000-0000-0000-000000000002",
                    "00000000-0000-0000-0000-000000000002"
                ]
            }),
            "projection",
            "uniqueItems",
        ),
        invalid_sample(
            json!({"sourceAssetId": "00000000-0000-0000-0000-000000000001", "projection": null}),
            "projection",
            "nonNull",
        ),
        invalid_sample(
            json!({"sourceAssetId": "00000000-0000-0000-0000-000000000000"}),
            "sourceAssetId",
            "valueKind",
        ),
        invalid_sample(
            json!({"sourceAssetId": "00000000-0000-0000-0000-000000000001", "extra": true}),
            "extra",
            "additionalProperties",
        ),
    ]
}
