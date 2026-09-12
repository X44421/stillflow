//! NX-N2 (#341) compiler-level acceptance: the composite expansion's
//! determinism, internal identity, equivalence with the hand-written atomic
//! chain, and fail-closed behavior without deployment (NX-C1 §4/§7/§9).

use std::collections::BTreeMap;

use serde_json::{json, Value};
use stillflow_core::{
    ColumnId, NodeConfig, NodeEdge, NodeGraph, NodeGraphErrorCode, NodeId, NodeRegistry, PortId,
};
use stillflow_plan::{
    internal_plan_node_id, AuthorizedSourceContext, CompileTarget, NodeGraphCompiler, PlanNodeId,
};

fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn column(value: u128) -> ColumnId {
    ColumnId::from_uuid(uuid(value))
}

fn edge(from: u128, to: u128) -> NodeEdge {
    NodeEdge {
        from: stillflow_core::NodePort {
            node_id: node(from),
            port: PortId::new("out").expect("port"),
        },
        to: stillflow_core::NodePort {
            node_id: node(to),
            port: PortId::new("in").expect("port"),
        },
    }
}

fn source_config(asset: u128) -> NodeConfig {
    NodeConfig::new(
        node(1),
        "stillflow.node.source",
        1,
        json!({"sourceAssetId": uuid(asset)}),
        BTreeMap::new(),
    )
    .expect("source")
}

fn output_config() -> NodeConfig {
    NodeConfig::new(
        node(5),
        "stillflow.node.output",
        1,
        json!({"outputLabel": "cleaned"}),
        BTreeMap::new(),
    )
    .expect("output")
}

fn source_schema() -> stillflow_core::LogicalSchema {
    stillflow_core::LogicalSchema::new(vec![stillflow_core::LogicalField::new(
        column(0x1000),
        "name",
        stillflow_core::LogicalType::Utf8,
        true,
    )
    .expect("field")])
    .expect("schema")
}

fn source(asset: u128) -> AuthorizedSourceContext {
    AuthorizedSourceContext::new(uuid(asset), source_schema()).expect("source")
}

/// `source → trim-clean(column) → output` and the hand-written atomic chain
/// `source → trim(column) → replace-literal(column, "", null) → output`.
fn composite_graph() -> NodeGraph {
    NodeGraph::new(
        uuid(0xF00),
        node(1),
        node(5),
        vec![
            source_config(700),
            NodeConfig::new(
                node(2),
                "stillflow.composite.trim-clean",
                1,
                json!({"column": column(0x1000)}),
                BTreeMap::new(),
            )
            .expect("composite"),
            output_config(),
        ],
        vec![edge(1, 2), edge(2, 5)],
        BTreeMap::new(),
    )
    .expect("graph")
}

fn atomic_graph() -> NodeGraph {
    NodeGraph::new(
        uuid(0xF01),
        node(1),
        node(5),
        vec![
            source_config(700),
            NodeConfig::new(
                node(2),
                "stillflow.node.trim",
                1,
                json!({"column": column(0x1000)}),
                BTreeMap::new(),
            )
            .expect("trim"),
            NodeConfig::new(
                node(3),
                "stillflow.node.replace-literal",
                1,
                json!({
                    "column": column(0x1000),
                    "from": {"kind": "utf8", "value": ""},
                    "to": {"kind": "null"}
                }),
                BTreeMap::new(),
            )
            .expect("replace"),
            output_config(),
        ],
        vec![edge(1, 2), edge(2, 3), edge(3, 5)],
        BTreeMap::new(),
    )
    .expect("graph")
}

fn rules_of(compiled: &stillflow_plan::CompiledNodeGraph) -> Vec<Value> {
    let mut rules = Vec::new();
    for plan_node in compiled.plan.nodes.values() {
        if let stillflow_plan::PlanNodeKind::ApplyRules { rules: step_rules } = &plan_node.kind {
            for rule in step_rules {
                rules.push(serde_json::to_value(rule).expect("rule"));
            }
        }
    }
    rules
}

/// The equivalence law (NX-C1 §7): the composite expansion and the
/// hand-written atomic chain carry identical rules, identical per-node
/// schemas, and identical output schema — the engine executes the same rule
/// sequence, so values, NULL handling, column order, and errors match by
/// construction.
#[test]
fn composite_expansion_equals_the_hand_written_chain() {
    let deployed = NodeRegistry::deployed();
    let compiled = NodeGraphCompiler::new(deployed.clone())
        .compile(&composite_graph(), &source(700), CompileTarget::Execution)
        .expect("composite compiles");
    let atomic = NodeGraphCompiler::new(deployed)
        .compile(&atomic_graph(), &source(700), CompileTarget::Execution)
        .expect("atomic compiles");

    assert_eq!(rules_of(&compiled), rules_of(&atomic));
    assert_eq!(compiled.output_schema, atomic.output_schema);

    // Per-node schemas: the composite's product schema (post-expansion)
    // equals the atomic chain's final transform schema; the internal step
    // schemas appear under the derived internal ids.
    let composite_product = PlanNodeId::from_uuid(uuid(2));
    let internal_0 = internal_plan_node_id(composite_product, 0);
    let internal_1 = internal_plan_node_id(composite_product, 1);
    let key_0 = stillflow_core::NodeId::from_uuid(internal_0.as_uuid());
    let key_1 = stillflow_core::NodeId::from_uuid(internal_1.as_uuid());
    let composite_product_key = node(2);
    assert_eq!(
        compiled.node_schemas[&key_0],
        atomic.node_schemas[&node(2)],
        "step 0 (trim) schema equals the atomic trim schema"
    );
    assert_eq!(
        compiled.node_schemas[&key_1],
        atomic.node_schemas[&node(3)],
        "step 1 (replace) schema equals the atomic replace schema"
    );
    assert_eq!(
        compiled.node_schemas[&composite_product_key],
        atomic.node_schemas[&node(3)]
    );
    // Nullability contract: the empty-string→null step widens the column.
    assert!(compiled.node_schemas[&key_1].fields[0].nullable);

    // Product mapping: the composite previews its output boundary (the last
    // internal node); atomic nodes stay identity-mapped.
    assert_eq!(compiled.node_plan_ids[&composite_product_key], internal_1);
    assert_eq!(
        compiled
            .preview_plan_node_id(node(2))
            .expect("preview target"),
        internal_1
    );

    // Plan shape: source + two internal rule nodes + output; the internal
    // ids carry the 0x8000|ordinal marker in the low 16 bits.
    assert_eq!(compiled.plan.nodes.len(), 4);
    assert_eq!(internal_0.as_uuid().as_u128() & 0xFFFF, 0x8000);
    assert_eq!(internal_1.as_uuid().as_u128() & 0xFFFF, 0x8001);
}

/// Determinism (NX-C1 §4.2 property 1): array permutation, repeated
/// compilation, and (by purity) restarts produce identical fingerprints.
#[test]
fn composite_expansion_is_deterministic_under_permutation() {
    let deployed = NodeRegistry::deployed();
    let first = NodeGraphCompiler::new(deployed.clone())
        .compile(&composite_graph(), &source(700), CompileTarget::Execution)
        .expect("first");
    let mut graph = composite_graph();
    graph.nodes.reverse();
    graph.edges.reverse();
    let second = NodeGraphCompiler::new(deployed)
        .compile(&graph, &source(700), CompileTarget::Execution)
        .expect("second");
    assert_eq!(
        first.fingerprint().expect("fingerprint"),
        second.fingerprint().expect("fingerprint")
    );
    assert_eq!(
        first.canonical_bytes().expect("bytes"),
        second.canonical_bytes().expect("bytes")
    );
}

/// Without deployment, the composite type is unknown and fails closed
/// (NX-C1 §9); the deployed registry never loosens atomic behavior.
#[test]
fn composite_types_fail_closed_without_deployment() {
    let error = NodeGraphCompiler::default()
        .compile(&composite_graph(), &source(700), CompileTarget::Execution)
        .expect_err("composite unknown without deployment");
    assert_eq!(error.code(), NodeGraphErrorCode::UnknownNodeType);
    assert_eq!(error.code().as_str(), "NG_UNKNOWN_NODE_TYPE");

    // The atomic chain still compiles on the production registry.
    NodeGraphCompiler::default()
        .compile(&atomic_graph(), &source(700), CompileTarget::Execution)
        .expect("atomic chain unaffected");
}

/// Internal ids never become product-mappable targets: an internal id is
/// not a valid preview target (NX-C1 §4.2 property 4).
#[test]
fn internal_ids_are_not_preview_targets() {
    let deployed = NodeRegistry::deployed();
    let compiled = NodeGraphCompiler::new(deployed)
        .compile(&composite_graph(), &source(700), CompileTarget::Execution)
        .expect("compiles");
    let internal_0 =
        NodeId::from_uuid(internal_plan_node_id(PlanNodeId::from_uuid(uuid(2)), 0).as_uuid());
    let error = compiled
        .preview_plan_node_id(internal_0)
        .expect_err("internal id is not an emitted product node");
    assert_eq!(error.code(), NodeGraphErrorCode::UnsupportedTarget);
}
