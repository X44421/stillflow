//! Per-node definition modules (NX-N1, #337). Each module owns its typed
//! config parsing, validation, catalog constraints, support conditions, and
//! validity samples. Adding a node type means adding one module here plus a
//! registry-list entry; the generic graph traversal and the registry never
//! gain node-specific branches.

pub(crate) mod cast;
pub(crate) mod derive_column;
pub(crate) mod drop_column;
pub(crate) mod fill_null;
pub(crate) mod filter;
pub(crate) mod output;
pub(crate) mod rename;
pub(crate) mod replace_literal;
pub(crate) mod select;
pub(crate) mod source;
pub(crate) mod trim;

#[cfg(test)]
pub(crate) mod trim_alias_testonly;

pub(crate) use crate::node_graph::definition;
pub(crate) use crate::{ColumnId, NodeConfig, NodeGraphError, PortId, ValidatedNodeConfig};

use super::definition::NodeDefinition;

/// The closed production definition list, in registration order. The
/// registry sorts and closes it; only this list changes when a node type is
/// added.
pub(crate) fn production_definitions() -> Vec<NodeDefinition> {
    vec![
        source::definition(),
        select::definition(),
        filter::definition(),
        rename::definition(),
        trim::definition(),
        cast::definition(),
        replace_literal::definition(),
        fill_null::definition(),
        drop_column::definition(),
        derive_column::definition(),
        output::definition(),
    ]
}

#[cfg(test)]
pub(crate) fn test_only_definitions() -> Vec<NodeDefinition> {
    vec![trim_alias_testonly::definition()]
}

/// One validity sample produced by a node's definition source. Positive
/// samples must pass the validator; negative samples must fail with the
/// expected code and either exercise an advertised catalog constraint or one
/// of the cross-cutting laws that sit outside the §6.2 vocabulary
/// (secret sentinel, expression bounds, kind-shape/nil semantics,
/// additionalProperties).
#[cfg(test)]
pub(crate) struct ConfigSample {
    pub config: serde_json::Value,
    pub valid: bool,
    pub field: &'static str,
    pub constraint: &'static str,
}

#[cfg(test)]
pub(crate) fn valid_sample(config: serde_json::Value) -> ConfigSample {
    ConfigSample {
        config,
        valid: true,
        field: "",
        constraint: "",
    }
}

#[cfg(test)]
pub(crate) fn invalid_sample(
    config: serde_json::Value,
    field: &'static str,
    constraint: &'static str,
) -> ConfigSample {
    ConfigSample {
        config,
        valid: false,
        field,
        constraint,
    }
}

#[cfg(test)]
mod consistency_tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::super::definition::NodeDefinition;
    use super::super::{
        NodeConfig, NodeGraph, NodeGraphErrorCode, NodeId, NodeRegistry, PortId,
        ValidatedNodeConfig,
    };
    use super::{production_definitions, ConfigSample};

    fn sample_definitions() -> Vec<(&'static str, NodeDefinition, Vec<ConfigSample>)> {
        vec![
            (
                "source",
                super::source::definition(),
                super::source::samples(),
            ),
            (
                "select",
                super::select::definition(),
                super::select::samples(),
            ),
            (
                "filter",
                super::filter::definition(),
                super::filter::samples(),
            ),
            (
                "rename",
                super::rename::definition(),
                super::rename::samples(),
            ),
            ("trim", super::trim::definition(), super::trim::samples()),
            ("cast", super::cast::definition(), super::cast::samples()),
            (
                "replace-literal",
                super::replace_literal::definition(),
                super::replace_literal::samples(),
            ),
            (
                "fill-null",
                super::fill_null::definition(),
                super::fill_null::samples(),
            ),
            (
                "drop-column",
                super::drop_column::definition(),
                super::drop_column::samples(),
            ),
            (
                "derive-column",
                super::derive_column::definition(),
                super::derive_column::samples(),
            ),
            (
                "output",
                super::output::definition(),
                super::output::samples(),
            ),
        ]
    }

    fn constraint_names(definition: &NodeDefinition, field: &str) -> Vec<String> {
        let mut names: Vec<String> = definition
            .config_schema()
            .fields
            .iter()
            .find(|candidate| candidate.name == field)
            .map(|candidate| {
                candidate
                    .advertised_constraints()
                    .iter()
                    .map(|name| name.to_string())
                    .collect()
            })
            .unwrap_or_default();
        // The value kind and the required flag are themselves advertised
        // properties of the field record.
        names.push("valueKind".to_owned());
        names.push("required".to_owned());
        names
    }

    /// NX-C0 §6.2: every value the constraints advertise must be accepted by
    /// the validator, and every rejection reason the validator produces must
    /// be expressible by a constraint (or a named cross-cutting law).
    #[test]
    fn catalog_constraints_and_validator_agree_for_all_builtins() {
        for (name, definition, samples) in sample_definitions() {
            let definition = &definition;
            assert_eq!(definition.config_version(), 1, "{name} config version");
            assert_eq!(definition.type_id(), format!("stillflow.node.{name}"));
            let mut advertised: Vec<&str> = Vec::new();
            for field in definition.config_schema().fields.iter() {
                advertised.extend(field.advertised_constraints());
                if field.required {
                    let missing = samples.iter().any(|sample| {
                        !sample.valid
                            && sample.field == field.name
                            && sample.constraint == "required"
                    });
                    assert!(
                        missing,
                        "{name}: required field {} lacks a missing-required negative sample",
                        field.name
                    );
                }
            }
            for sample in &samples {
                let node = NodeConfig::new(
                    NodeId::from_uuid(uuid::Uuid::from_u128(0xA1)),
                    definition.type_id(),
                    1,
                    sample.config.clone(),
                    BTreeMap::new(),
                )
                .expect("sample node config");
                let outcome = definition.validate_node_config(&node);
                if sample.valid {
                    assert!(
                        outcome.is_ok(),
                        "{name}: positive sample rejected: {:?}",
                        outcome.err().map(|error| error.message().to_owned())
                    );
                    continue;
                }
                let error = outcome.expect_err("{name}: negative sample accepted");
                let expected_code = match sample.constraint {
                    "expressionBounds" => NodeGraphErrorCode::LimitNestingDepth,
                    _ => NodeGraphErrorCode::InvalidConfig,
                };
                assert_eq!(
                    error.code(),
                    expected_code,
                    "{name}: sample on field {} (constraint {}) produced a different class",
                    sample.field,
                    sample.constraint
                );
                if sample.constraint != "additionalProperties" {
                    let names = constraint_names(definition, sample.field);
                    assert!(
                        names.iter().any(|candidate| candidate == sample.constraint),
                        "{name}: constraint {} is not advertised on field {} (advertised: {names:?})",
                        sample.constraint,
                        sample.field
                    );
                }
            }
            // Round-trip in the other direction: every advertised constraint
            // has at least one negative sample exercising it.
            // Descriptive constraints state accepted behavior with no
            // rejection of their own at this layer. `byteBound` and the
            // secret sentinel are enforced earlier, by `NodeConfig::new`'s
            // construction validation, and stay advertised so clients see
            // the frozen limit.
            const DESCRIPTIVE_ONLY: [&str; 5] =
                ["ordered", "maxItems", "minLength", "maxLength", "byteBound"];
            for constraint in advertised {
                // Descriptive constraints state accepted behavior and have
                // no rejection of their own; the others must each be
                // exercised by a negative sample.
                if DESCRIPTIVE_ONLY.contains(&constraint) {
                    continue;
                }
                let exercised = samples
                    .iter()
                    .any(|sample| !sample.valid && sample.constraint == constraint);
                assert!(
                    exercised,
                    "{name}: advertised constraint {constraint} has no negative sample"
                );
            }
        }
    }

    #[test]
    fn registry_rejects_duplicates_and_sorts_stably() {
        let production = production_definitions();
        let type_ids: Vec<_> = production
            .iter()
            .map(|definition| definition.type_id().to_owned())
            .collect();

        // Shuffled registration order never changes the catalog.
        let mut shuffled = production.clone();
        shuffled.reverse();
        let mut rotated = production.clone();
        rotated.rotate_left(3);
        let expected: Vec<String> = {
            let mut sorted = type_ids.clone();
            sorted.sort();
            sorted
        };
        for registry in [
            NodeRegistry::from_definitions(shuffled).expect("shuffled"),
            NodeRegistry::from_definitions(rotated).expect("rotated"),
            NodeRegistry::new(),
        ] {
            let ids: Vec<_> = registry
                .catalog()
                .iter()
                .map(|entry| entry.type_id.clone())
                .collect();
            assert_eq!(ids, expected);
            assert_eq!(registry.catalog().len(), 11);
        }

        // Duplicate (typeId, configVersion) pairs fail closed.
        let mut duplicated = production.clone();
        duplicated.push(production_definitions()[4].clone());
        let error = NodeRegistry::from_definitions(duplicated).expect_err("duplicate");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
    }

    #[test]
    fn support_conditions_match_the_frozen_declaration() {
        let registry = NodeRegistry::new();
        for definition in registry.definitions() {
            for port_conditions in definition.support_conditions() {
                assert!(
                    definition
                        .input_ports()
                        .iter()
                        .any(|port| port == &port_conditions.port),
                    "{} declares conditions for a non-input port",
                    definition.type_id()
                );
            }
        }
        let trim = registry.lookup("stillflow.node.trim", 1).expect("trim");
        let conditions = &trim.support_conditions()[0];
        assert_eq!(conditions.port, PortId::from_static("in"));
        assert_eq!(conditions.conditions.len(), 2);
        assert!(conditions
            .conditions
            .iter()
            .any(|condition| matches!(condition, super::super::SupportCondition::RequiresType(_))));
        let drop = registry
            .lookup("stillflow.node.drop-column", 1)
            .expect("drop");
        assert!(drop.support_conditions()[0]
            .conditions
            .iter()
            .any(|condition| matches!(condition, super::super::SupportCondition::MinFields(2))));
    }

    /// The test-only definition registers through the same public path,
    /// never enters the production catalog, and the generic graph traversal
    /// resolves it with no node-specific branch.
    #[test]
    fn test_only_definition_extends_without_traversal_changes() {
        let production = NodeRegistry::new();
        assert!(production.lookup("stillflow.test.trim-alias", 1).is_err());

        let mut definitions = production_definitions();
        definitions.extend(super::test_only_definitions());
        let extended = NodeRegistry::from_definitions(definitions).expect("extended registry");
        assert_eq!(extended.catalog().len(), 12);

        let column = "00000000-0000-0000-0000-000000000002";
        let graph = NodeGraph::new(
            uuid::Uuid::from_u128(0xBEEF),
            NodeId::from_uuid(uuid::Uuid::from_u128(1)),
            NodeId::from_uuid(uuid::Uuid::from_u128(5)),
            vec![
                NodeConfig::new(
                    NodeId::from_uuid(uuid::Uuid::from_u128(1)),
                    "stillflow.node.source",
                    1,
                    json!({"sourceAssetId": "00000000-0000-0000-0000-000000000007"}),
                    BTreeMap::new(),
                )
                .expect("source"),
                NodeConfig::new(
                    NodeId::from_uuid(uuid::Uuid::from_u128(2)),
                    "stillflow.test.trim-alias",
                    1,
                    json!({"column": column}),
                    BTreeMap::new(),
                )
                .expect("alias node"),
                NodeConfig::new(
                    NodeId::from_uuid(uuid::Uuid::from_u128(5)),
                    "stillflow.node.output",
                    1,
                    json!({"outputLabel": "cleaned"}),
                    BTreeMap::new(),
                )
                .expect("output"),
            ],
            vec![
                super::super::NodeEdge {
                    from: super::super::NodePort {
                        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(1)),
                        port: PortId::from_static("out"),
                    },
                    to: super::super::NodePort {
                        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(2)),
                        port: PortId::from_static("in"),
                    },
                },
                super::super::NodeEdge {
                    from: super::super::NodePort {
                        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(2)),
                        port: PortId::from_static("out"),
                    },
                    to: super::super::NodePort {
                        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(5)),
                        port: PortId::from_static("in"),
                    },
                },
            ],
            BTreeMap::new(),
        )
        .expect("graph");
        let configs = graph
            .validated_configs(&extended)
            .expect("generic traversal");
        assert!(matches!(
            configs.get(&NodeId::from_uuid(uuid::Uuid::from_u128(2))),
            Some(ValidatedNodeConfig::Trim { .. })
        ));
    }
}
