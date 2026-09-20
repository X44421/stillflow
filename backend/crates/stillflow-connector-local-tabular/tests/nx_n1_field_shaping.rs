//! NX-N1 (#365) acceptance: field shaping (`select` / `rename` / `drop-column`)
//! and combined filtering (AND / OR / NOT, keep versus exclude, NULL predicate)
//! over **real CSV data**, driven through the production NodeGraph compile path
//! and executed by the engine.
//!
//! Contract: `docs/contracts/issue-362-nx-c0-twelve-category-capability-matrix.md`
//! §2 categories 1 and 5, §3.1 bucket A — the capability is configuration and
//! catalog only, so the deliverable is real-data evidence rather than new
//! execution semantics. Batch rename is realized as an ordered chain of single
//! `rename` nodes because the product bound is one rule per node
//! (`MAX_RULES_PER_NODE = 1`); the engine's internal 256 is never cited.
//!
//! Every assertion below is made against values produced by a real CSV read
//! through the local-tabular connector, not against a synthetic stub.

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, Int64Array, StringArray};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    BinaryOperator, ColumnId, ConnectorKind, CredentialRef, Expr, LogicalSchema, LogicalType,
    NodeConfig, NodeEdge, NodeGraph, NodeGraphErrorCode, NodeId, NodePort, NodeRegistry, PortId,
    RequestContext, ScalarValue, SourceAsset, SourceConnection, UnaryOperator,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

const CSV: &str = "name,city,age\n\
                   alice,beijing,30\n\
                   bob,shanghai,\n\
                   carol,beijing,25\n\
                   dave,hangzhou,40\n\
                   eve,shanghai,35\n";

// ---------------------------------------------------------------------------
// Identities and graph construction helpers
// ---------------------------------------------------------------------------

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn port(name: &str) -> PortId {
    PortId::new(name).expect("port id")
}

fn config(id: u128, type_id: &str, value: serde_json::Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("node config")
}

fn edge(from: u128, to: u128) -> NodeEdge {
    NodeEdge {
        from: NodePort {
            node_id: node(from),
            port: port("out"),
        },
        to: NodePort {
            node_id: node(to),
            port: port("in"),
        },
    }
}

fn column_expr(id: ColumnId) -> Expr {
    Expr::Column(id)
}

fn utf8(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value.to_owned()))
}

fn int64(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(value))
}

fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        operator,
        right: Box::new(right),
    }
}

fn not(expression: Expr) -> Expr {
    Expr::Unary {
        operator: UnaryOperator::Not,
        expression: Box::new(expression),
    }
}

/// `source -> transform* -> output`, the version-1 linear profile.
fn graph(transforms: Vec<(u128, &str, serde_json::Value)>, asset_id: Uuid) -> NodeGraph {
    let output_id = 900 + transforms.len() as u128;
    let mut nodes = vec![config(
        1,
        "stillflow.node.source",
        json!({ "sourceAssetId": asset_id }),
    )];
    for (id, type_id, value) in &transforms {
        nodes.push(config(*id, type_id, value.clone()));
    }
    nodes.push(config(
        output_id,
        "stillflow.node.output",
        json!({ "outputLabel": "out" }),
    ));

    let mut edges = Vec::new();
    let mut previous = 1_u128;
    for (id, _, _) in &transforms {
        edges.push(edge(previous, *id));
        previous = *id;
    }
    edges.push(edge(previous, output_id));

    NodeGraph::new(
        uuid(1),
        node(1),
        node(output_id),
        nodes,
        edges,
        BTreeMap::new(),
    )
    .expect("valid graph")
}

// ---------------------------------------------------------------------------
// Real-data fixture: a CSV on disk, discovered and inspected by the connector
// ---------------------------------------------------------------------------

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    let path = root.path().join("people.csv");
    fs::write(&path, CSV).expect("write fixture");

    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n1 fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n1").expect("credential ref"),
    )
    .expect("connection");

    let mut registry = ConnectorRegistry::new();
    registry
        .register(std::sync::Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");

    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let asset = assets
        .into_iter()
        .find(|asset| asset.name == "people.csv")
        .expect("fixture asset");

    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");

    Fixture {
        _root: root,
        connection,
        asset,
        schema: metadata.schema,
    }
}

impl Fixture {
    fn column(&self, name: &str) -> ColumnId {
        self.schema
            .fields
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("column {name} exists"))
            .id
    }

    /// Compiles the graph and previews the named transform node. The preview
    /// target must be a non-`Materialize` plan node (NG-C0 §9), so callers pass
    /// the last transform of the chain rather than the output node.
    async fn run(&self, graph: &NodeGraph, target_node: u128) -> PreviewResult {
        let compiled = NodeGraphCompiler::new(NodeRegistry::deployed())
            .compile(
                graph,
                &AuthorizedSourceContext::new(self.asset.id, self.schema.clone())
                    .expect("authorized source"),
                CompileTarget::Execution,
            )
            .expect("graph compiles");

        // The compiled per-node schema must equal what execution actually
        // produces (issue #365 acceptance: compile Schema == real output).
        let target = *compiled
            .node_plan_ids
            .get(&node(target_node))
            .expect("target plan node");
        let expected = compiled
            .node_schemas
            .get(&node(target_node))
            .expect("target schema")
            .clone();

        let mut request = PreviewRequest::new(
            compiled.plan.clone(),
            target,
            self.connection.clone(),
            self.asset.clone(),
        );
        request.row_limit = 100;
        request.byte_limit = 1_048_576;

        let result = ExecutionEngine::new(registry())
            .preview(request)
            .await
            .expect("preview executes");

        assert_eq!(
            result.schema, expected,
            "compiled schema must equal the real output schema"
        );
        assert_eq!(
            result.schema, compiled.output_schema,
            "the last transform's schema must equal the graph's output schema"
        );
        result
    }
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(std::sync::Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

// ---------------------------------------------------------------------------
// Cell readers over the real Arrow payload
// ---------------------------------------------------------------------------

fn strings(result: &PreviewResult, column: &ColumnId) -> Vec<Option<String>> {
    let index = field_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row).to_owned())
            });
        }
    }
    values
}

fn integers(result: &PreviewResult, column: &ColumnId) -> Vec<Option<i64>> {
    let index = field_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            });
        }
    }
    values
}

fn field_index(result: &PreviewResult, column: &ColumnId) -> usize {
    result
        .schema
        .fields
        .iter()
        .position(|field| field.id == *column)
        .expect("column present in the output schema")
}

fn names(result: &PreviewResult) -> Vec<String> {
    result
        .schema
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

fn ids(result: &PreviewResult) -> Vec<ColumnId> {
    result.schema.fields.iter().map(|field| field.id).collect()
}

// ---------------------------------------------------------------------------
// 1. select / drop-column / rename: identity, order and names
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_keeps_declared_order_and_field_identity() {
    let fixture = fixture().await;
    let (name, city) = (fixture.column("name"), fixture.column("city"));

    let graph = graph(
        vec![(
            2,
            "stillflow.node.select",
            json!({ "columns": [city, name] }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    assert_eq!(names(&result), vec!["city", "name"]);
    assert_eq!(ids(&result), vec![city, name]);
    assert_eq!(
        strings(&result, &city),
        vec![
            Some("beijing".to_owned()),
            Some("shanghai".to_owned()),
            Some("beijing".to_owned()),
            Some("hangzhou".to_owned()),
            Some("shanghai".to_owned()),
        ]
    );
    assert_eq!(strings(&result, &name)[0].as_deref(), Some("alice"));
}

#[tokio::test]
async fn rename_changes_only_the_name_and_keeps_the_column_id() {
    let fixture = fixture().await;
    let age = fixture.column("age");

    let graph = graph(
        vec![(
            2,
            "stillflow.node.rename",
            json!({ "column": age, "to": "years" }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    assert_eq!(names(&result), vec!["name", "city", "years"]);
    assert_eq!(ids(&result)[2], age, "rename preserves ColumnId");
    // Values are untouched by a rename.
    assert_eq!(
        integers(&result, &age),
        vec![Some(30), None, Some(25), Some(40), Some(35)]
    );
}

#[tokio::test]
async fn drop_column_removes_the_field_and_keeps_the_rest_in_order() {
    let fixture = fixture().await;
    let (name, city, age) = (
        fixture.column("name"),
        fixture.column("city"),
        fixture.column("age"),
    );

    let graph = graph(
        vec![(2, "stillflow.node.drop-column", json!({ "column": city }))],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    assert_eq!(names(&result), vec!["name", "age"]);
    assert_eq!(ids(&result), vec![name, age]);
}

#[tokio::test]
async fn batch_rename_is_an_ordered_chain_of_single_rule_nodes() {
    let fixture = fixture().await;
    let (name, city) = (fixture.column("name"), fixture.column("city"));

    // Two single-rule rename nodes: the frozen product bound of one rule per
    // node is respected instead of packing a mapping into one node.
    let graph = graph(
        vec![
            (
                2,
                "stillflow.node.rename",
                json!({ "column": name, "to": "full_name" }),
            ),
            (
                3,
                "stillflow.node.rename",
                json!({ "column": city, "to": "town" }),
            ),
        ],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 3).await;

    assert_eq!(names(&result), vec!["full_name", "town", "age"]);
    assert_eq!(ids(&result), vec![name, city, fixture.column("age")]);
    assert_eq!(strings(&result, &name)[0].as_deref(), Some("alice"));
}

// ---------------------------------------------------------------------------
// 2. Rejections: typed diagnostics, produced by the pure compile path
// ---------------------------------------------------------------------------

fn compile_error(graph: &NodeGraph, fixture: &Fixture) -> stillflow_plan::NodeGraphCompileError {
    NodeGraphCompiler::new(NodeRegistry::deployed())
        .compile(
            graph,
            &AuthorizedSourceContext::new(fixture.asset.id, fixture.schema.clone())
                .expect("authorized source"),
            CompileTarget::Execution,
        )
        .expect_err("graph must be rejected")
}

#[tokio::test]
async fn unknown_column_is_rejected_by_the_pure_compile_path() {
    let fixture = fixture().await;
    let graph = graph(
        vec![(
            2,
            "stillflow.node.select",
            json!({ "columns": [uuid(9999)] }),
        )],
        fixture.asset.id,
    );
    let error = compile_error(&graph, &fixture);
    assert_eq!(error.code(), NodeGraphErrorCode::UnknownColumn);
    assert_eq!(error.node_id(), Some(node(2)));
}

#[tokio::test]
async fn duplicate_target_name_is_rejected() {
    let fixture = fixture().await;
    let city = fixture.column("city");
    // `name` already exists, so renaming `city` to `name` must be refused
    // rather than silently suffixed.
    let graph = graph(
        vec![(
            2,
            "stillflow.node.rename",
            json!({ "column": city, "to": "name" }),
        )],
        fixture.asset.id,
    );
    let error = compile_error(&graph, &fixture);
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn empty_column_list_is_rejected() {
    let fixture = fixture().await;
    let graph = graph(
        vec![(2, "stillflow.node.select", json!({ "columns": [] }))],
        fixture.asset.id,
    );
    let error = compile_error(&graph, &fixture);
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn dropping_the_last_remaining_field_is_rejected() {
    let fixture = fixture().await;
    let (name, city, age) = (
        fixture.column("name"),
        fixture.column("city"),
        fixture.column("age"),
    );
    let graph = graph(
        vec![
            (2, "stillflow.node.drop-column", json!({ "column": name })),
            (3, "stillflow.node.drop-column", json!({ "column": city })),
            (4, "stillflow.node.drop-column", json!({ "column": age })),
        ],
        fixture.asset.id,
    );
    let error = compile_error(&graph, &fixture);
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn a_non_boolean_predicate_is_rejected() {
    let fixture = fixture().await;
    let age = fixture.column("age");
    // `age` alone is Int64, not Boolean.
    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": column_expr(age) }),
        )],
        fixture.asset.id,
    );
    let error = compile_error(&graph, &fixture);
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

// ---------------------------------------------------------------------------
// 3. Filtering: AND / OR / NOT over real rows, with a NULL-bearing column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn filter_comparison_drops_the_null_predicate_row() {
    let fixture = fixture().await;
    let age = fixture.column("age");
    let name = fixture.column("name");

    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": binary(column_expr(age), BinaryOperator::GreaterThanOrEqual, int64(30)) }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    // bob has a NULL age: a NULL predicate result removes the row.
    assert_eq!(
        strings(&result, &name),
        vec![
            Some("alice".to_owned()),
            Some("dave".to_owned()),
            Some("eve".to_owned()),
        ]
    );
}

#[tokio::test]
async fn filter_and_combination() {
    let fixture = fixture().await;
    let (name, city, age) = (
        fixture.column("name"),
        fixture.column("city"),
        fixture.column("age"),
    );

    let predicate = binary(
        binary(column_expr(city), BinaryOperator::Equal, utf8("beijing")),
        BinaryOperator::And,
        binary(
            column_expr(age),
            BinaryOperator::GreaterThanOrEqual,
            int64(30),
        ),
    );
    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": predicate }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;
    assert_eq!(strings(&result, &name), vec![Some("alice".to_owned())]);
}

#[tokio::test]
async fn filter_or_combination() {
    let fixture = fixture().await;
    let (name, city, age) = (
        fixture.column("name"),
        fixture.column("city"),
        fixture.column("age"),
    );

    let predicate = binary(
        binary(column_expr(city), BinaryOperator::Equal, utf8("hangzhou")),
        BinaryOperator::Or,
        binary(column_expr(age), BinaryOperator::Equal, int64(25)),
    );
    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": predicate }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;
    assert_eq!(
        strings(&result, &name),
        vec![Some("carol".to_owned()), Some("dave".to_owned())]
    );
}

#[tokio::test]
async fn filter_not_combination() {
    let fixture = fixture().await;
    let (name, city) = (fixture.column("name"), fixture.column("city"));

    let predicate = not(binary(
        column_expr(city),
        BinaryOperator::Equal,
        utf8("beijing"),
    ));
    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": predicate }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;
    assert_eq!(
        strings(&result, &name),
        vec![
            Some("bob".to_owned()),
            Some("dave".to_owned()),
            Some("eve".to_owned()),
        ]
    );
}

#[tokio::test]
async fn keep_and_exclude_modes_are_exact_complements() {
    let fixture = fixture().await;
    let (name, city) = (fixture.column("name"), fixture.column("city"));

    let keep = binary(column_expr(city), BinaryOperator::Equal, utf8("beijing"));
    let exclude = not(binary(
        column_expr(city),
        BinaryOperator::Equal,
        utf8("beijing"),
    ));

    let keep_graph = graph(
        vec![(2, "stillflow.node.filter", json!({ "predicate": keep }))],
        fixture.asset.id,
    );
    let exclude_graph = graph(
        vec![(2, "stillflow.node.filter", json!({ "predicate": exclude }))],
        fixture.asset.id,
    );

    let kept = strings(&fixture.run(&keep_graph, 2).await, &name);
    let excluded = strings(&fixture.run(&exclude_graph, 2).await, &name);

    assert_eq!(
        kept,
        vec![Some("alice".to_owned()), Some("carol".to_owned())]
    );
    assert_eq!(
        excluded,
        vec![
            Some("bob".to_owned()),
            Some("dave".to_owned()),
            Some("eve".to_owned()),
        ]
    );

    // Complementary: disjoint, and together they cover every source row.
    let mut union: Vec<String> = kept
        .iter()
        .chain(excluded.iter())
        .flatten()
        .cloned()
        .collect();
    union.sort();
    assert_eq!(union, vec!["alice", "bob", "carol", "dave", "eve"]);
    assert!(kept.iter().all(|row| !excluded.contains(row)));
}

// ---------------------------------------------------------------------------
// 4. Old node configurations keep working unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_single_rule_configurations_still_compile_and_run() {
    let fixture = fixture().await;
    let (name, age) = (fixture.column("name"), fixture.column("age"));

    // The exact single-rule shapes that existed before this wave.
    let graph = graph(
        vec![
            (
                2,
                "stillflow.node.select",
                json!({ "columns": [name, age] }),
            ),
            (
                3,
                "stillflow.node.rename",
                json!({ "column": name, "to": "who" }),
            ),
            (
                4,
                "stillflow.node.filter",
                json!({ "predicate": binary(column_expr(age), BinaryOperator::GreaterThan, int64(26)) }),
            ),
        ],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 4).await;

    assert_eq!(names(&result), vec!["who", "age"]);
    assert_eq!(ids(&result), vec![name, age]);
    assert_eq!(
        strings(&result, &name),
        vec![
            Some("alice".to_owned()),
            Some("dave".to_owned()),
            Some("eve".to_owned()),
        ]
    );
    assert_eq!(integers(&result, &age), vec![Some(30), Some(40), Some(35)]);
}

// ---------------------------------------------------------------------------
// 5. Schema typing of the fixture itself (guards the assertions above)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_schema_is_the_expected_logical_shape() {
    let fixture = fixture().await;
    assert_eq!(
        fixture
            .schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.data_type.clone(), field.nullable))
            .collect::<Vec<_>>(),
        vec![
            ("name", LogicalType::Utf8, false),
            ("city", LogicalType::Utf8, false),
            ("age", LogicalType::Int64, true),
        ],
        "the empty age cell must infer as a nullable Int64"
    );
}
