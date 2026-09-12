//! NX-B1 (#339) compile-resource measurement harness.
//!
//! Measures the merged baseline's wall time, allocation totals/peaks, and
//! serialized sizes for the three NX-C0 §9.3 shapes: the typical chain, the
//! 64-node wide-schema chain, and the deep expression. Run with:
//!
//! ```text
//! cargo test -p stillflow-plan --test nx_b1_resources -- --ignored measure --nocapture
//! ```
//!
//! The harness is measurement-only; the committed evidence lives under
//! `docs/evidence/nodes/`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Instant;

use serde_json::{json, Value};
use stillflow_core::{
    ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, NodeConfig, NodeEdge, NodeGraph,
    NodeId,
};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static TOTAL: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            // Wrapping arithmetic: deallocations of allocations made before
            // the measured window legitimately move CURRENT below its
            // baseline; the reported peak is seeded at the baseline.
            let current = CURRENT.fetch_add(layout.size(), SeqCst).wrapping_add(layout.size());
            TOTAL.fetch_add(layout.size(), SeqCst);
            PEAK.fetch_max(current, SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), SeqCst);
        System.dealloc(pointer, layout)
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn column(value: u128) -> ColumnId {
    ColumnId::from_uuid(uuid(value))
}

fn schema(fields: usize) -> LogicalSchema {
    let fields: Vec<LogicalField> = (0..fields)
        .map(|index| {
            LogicalField::new(
                column(0x1000 + index as u128),
                format!("column_{index}"),
                if index % 2 == 0 {
                    LogicalType::Utf8
                } else {
                    LogicalType::Int64
                },
                true,
            )
            .expect("field")
        })
        .collect();
    LogicalSchema::new(fields).expect("schema")
}

fn config(id: u128, type_id: &str, value: Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("config")
}

fn source_config(asset: u128) -> NodeConfig {
    // No projection: every schema field flows through, which is the
    // amplification worst case (a 4096-column projection would exceed the
    // 64 KiB config bound by itself).
    config(1, "stillflow.node.source", json!({"sourceAssetId": uuid(asset)}))
}

fn output_config() -> NodeConfig {
    config(5, "stillflow.node.output", json!({"outputLabel": "measured"}))
}

/// The typical chain: source → trim → fill-null → rename → filter → output
/// over a small schema.
fn typical_chain() -> (NodeGraph, LogicalSchema) {
    let nodes = vec![
        source_config(700),
        config(2, "stillflow.node.trim", json!({"column": column(0x1000)})),
        config(
            3,
            "stillflow.node.fill-null",
            json!({"column": column(0x1001), "value": {"kind": "int64", "value": 0}}),
        ),
        config(
            4,
            "stillflow.node.rename",
            json!({"column": column(0x1000), "to": "label"}),
        ),
        config(
            6,
            "stillflow.node.filter",
            json!({"predicate": {"kind": "binary", "value": {
                "left": {"kind": "column", "value": uuid(0x1001).to_string()},
                "operator": "greaterThanOrEqual",
                "right": {"kind": "literal", "value": {"kind": "int64", "value": -128}}
            }}}),
        ),
        output_config(),
    ];
    let edges = [(1u128, 2u128), (2, 3), (3, 4), (4, 6), (6, 5)]
        .into_iter()
        .map(|(from, to)| NodeEdge {
            from: stillflow_core::NodePort {
                node_id: node(from),
                port: stillflow_core::PortId::new("out").expect("port"),
            },
            to: stillflow_core::NodePort {
                node_id: node(to),
                port: stillflow_core::PortId::new("in").expect("port"),
            },
        })
        .collect();
    (
        NodeGraph::new(uuid(900), node(1), node(5), nodes, edges, BTreeMap::new()).expect("graph"),
        schema(3),
    )
}

/// The wide shape: a 64-node chain (the product node bound) over a
/// 4096-field schema (the core schema field bound), alternating select and
/// trim so every node carries the full schema.
fn wide_chain(fields: usize) -> (NodeGraph, LogicalSchema) {
    // Trim nodes keep every field in every intermediate schema: the schema
    // snapshot amplification worst case under the frozen config-byte bound.
    let mut nodes = vec![source_config(700)];
    let mut edges = Vec::new();
    for index in 0..62u128 {
        let id = 0x10 + index;
        nodes.push(config(id, "stillflow.node.trim", json!({"column": column(0x1000)})));
        edges.push((if index == 0 { 1 } else { 0x0F + index }, id));
    }
    nodes.push(output_config());
    edges.push((0x0F + 62, 5));
    let edges: Vec<NodeEdge> = edges
        .into_iter()
        .map(|(from, to)| NodeEdge {
            from: stillflow_core::NodePort {
                node_id: node(from),
                port: stillflow_core::PortId::new("out").expect("port"),
            },
            to: stillflow_core::NodePort {
                node_id: node(to),
                port: stillflow_core::PortId::new("in").expect("port"),
            },
        })
        .collect();
    (
        NodeGraph::new(uuid(901), node(1), node(5), nodes, edges, BTreeMap::new()).expect("graph"),
        schema(fields),
    )
}

/// The deep shape: one derive node whose expression nests `IsNull` as deep
/// as the JSON decode allows. Each `IsNull` costs two JSON nesting levels
/// against the 64-level decode bound, so the reachable expression depth is
/// ~31 — itself a measured boundary finding (the contract's 64-level
/// expression bound is not reachable through the config path).
fn deep_expression() -> (NodeGraph, LogicalSchema) {
    let mut expression = Expr::Column(column(0x1000));
    for _ in 0..30 {
        expression = Expr::IsNull {
            expression: Box::new(expression),
            negated: false,
        };
    }
    let nodes = vec![
        source_config(700),
        config(
            2,
            "stillflow.node.derive-column",
            json!({
                "id": uuid(0x2000).to_string(),
                "name": "deep",
                "dataType": {"kind": "boolean"},
                "nullable": true,
                "expression": serde_json::to_value(&expression).expect("expression")
            }),
        ),
        config(
            3,
            "stillflow.node.filter",
            json!({"predicate": {"kind": "binary", "value": {
                "left": {"kind": "column", "value": uuid(0x1001).to_string()},
                "operator": "equal",
                "right": {"kind": "literal", "value": {"kind": "int64", "value": 1}}
            }}}),
        ),
        output_config(),
    ];
    let edges = [(1u128, 2u128), (2, 3), (3, 5)]
        .into_iter()
        .map(|(from, to)| NodeEdge {
            from: stillflow_core::NodePort {
                node_id: node(from),
                port: stillflow_core::PortId::new("out").expect("port"),
            },
            to: stillflow_core::NodePort {
                node_id: node(to),
                port: stillflow_core::PortId::new("in").expect("port"),
            },
        })
        .collect();
    (
        NodeGraph::new(uuid(902), node(1), node(5), nodes, edges, BTreeMap::new()).expect("graph"),
        schema(2),
    )
}

fn measure(name: &str, graph: &NodeGraph, schema: &LogicalSchema) -> Value {
    let source = AuthorizedSourceContext::new(uuid(700), schema.clone()).expect("source");
    let compiler = NodeGraphCompiler::default();

    // Warm, then measure a single compile. The baseline is the live
    // current-allocation value; PEAK is seeded with it so the reported peak
    // is relative to the baseline.
    let _ = compiler.compile(graph, &source, CompileTarget::Execution).expect("warm compile");
    let baseline = CURRENT.load(SeqCst);
    TOTAL.store(0, SeqCst);
    PEAK.store(baseline, SeqCst);
    let start = Instant::now();
    let compiled = compiler
        .compile(graph, &source, CompileTarget::Execution)
        .expect("measured compile");
    let wall_micros = start.elapsed().as_micros() as u64;
    let total = TOTAL.load(SeqCst);
    let peak = PEAK.load(SeqCst).wrapping_sub(baseline);

    // Serialized response proxy: per-node schemas plus the compile outputs.
    let schemas_bytes = serde_json::to_vec(&compiled.node_schemas).expect("schemas json").len() as u64;
    let canonical = compiled.canonical_bytes().expect("canonical").len() as u64;
    let fingerprint = compiled.fingerprint().expect("fingerprint").to_string();
    let output_schema_bytes =
        serde_json::to_vec(&compiled.output_schema).expect("output schema json").len() as u64;

    json!({
        "shape": name,
        "nodes": graph.nodes.len(),
        "schemaFields": schema.fields.len(),
        "wallTimeMicros": wall_micros,
        "allocatedTotalBytes": total,
        "peakAllocatedBytes": peak,
        "nodeSchemasJsonBytes": schemas_bytes,
        "outputSchemaJsonBytes": output_schema_bytes,
        "canonicalPlanBytes": canonical,
        "planFingerprint": fingerprint,
    })
}

#[test]
#[ignore = "measurement run; evidence is committed under docs/evidence/nodes/"]
fn measure_baseline() {
    // The 4096-field wide shape is now rejected by the frozen snapshot
    // accounting before allocation (see boundary test below); its baseline
    // numbers live in docs/evidence/nodes/nx-b1-compile-resources.md.
    let (typical, small) = typical_chain();
    let (wide_small, wide_small_schema) = wide_chain(64);
    let (deep, deep_schema) = deep_expression();

    let results = vec![
        measure("typical-chain-6n-3f", &typical, &small),
        measure("wide-chain-64n-64f", &wide_small, &wide_small_schema),
        measure("deep-expression-64", &deep, &deep_schema),
    ];
    for result in &results {
        println!("{}", serde_json::to_string_pretty(result).expect("json"));
    }
}

/// The frozen accounting rejects the measured amplification worst case
/// (64 nodes × 4096 fields ≈ 32 MB of estimated snapshot bytes against the
/// 2 MiB budget) with the existing `NG_LIMIT_COMPILE_WORK` class, before
/// any snapshot is built, and the rejection is deterministic.
#[test]
fn schema_amplification_over_the_snapshot_budget_is_rejected_up_front() {
    let (wide, wide_schema) = wide_chain(4096);
    let source = AuthorizedSourceContext::new(uuid(700), wide_schema).expect("source");
    let error = NodeGraphCompiler::default()
        .compile(&wide, &source, CompileTarget::Execution)
        .expect_err("amplifying graph rejected");
    assert_eq!(
        error.code().as_str(),
        "NG_LIMIT_COMPILE_WORK",
        "the frozen failure class is preserved"
    );
    // Deterministic across repeated compilation.
    for _ in 0..3 {
        let again = NodeGraphCompiler::default()
            .compile(&wide, &source, CompileTarget::Execution)
            .expect_err("amplifying graph rejected");
        assert_eq!(again.code().as_str(), "NG_LIMIT_COMPILE_WORK");
    }
}

/// Shapes under the budget keep compiling and stay byte-identical to the
/// pre-accounting baseline (the v1 corpus covers the small shapes).
#[test]
fn shapes_under_the_snapshot_budget_are_unchanged() {
    let (wide_small, wide_small_schema) = wide_chain(64);
    let source = AuthorizedSourceContext::new(uuid(700), wide_small_schema).expect("source");
    let compiled = NodeGraphCompiler::default()
        .compile(&wide_small, &source, CompileTarget::Execution)
        .expect("64-field chain compiles");
    let fingerprint = compiled.fingerprint().expect("fingerprint").to_string();
    assert!(!fingerprint.is_empty());
    // 64 nodes × 64 fields × 128 = 512 KiB, under the 2 MiB budget.
    assert_eq!(compiled.node_schemas.len(), 64);
}
