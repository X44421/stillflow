//! NX-N2 (#366) acceptance: deterministic text normalization on **real data**.
//!
//! The fixture is NDJSON so that a NULL value and an empty string are both
//! representable and distinguishable. Every assertion runs through the
//! production NodeGraph compile path and the engine, over values produced by a
//! real read through the local-tabular connector.
//!
//! Contract: `docs/contracts/issue-362-nx-c0-twelve-category-capability-matrix.md`
//! §2 category 2 and §3.2. Operations are composed by chaining nodes because
//! the product law keeps exactly one rule per node.

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, Int64Array, StringArray};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, LogicalSchema, LogicalType, NodeConfig, NodeEdge,
    NodeGraph, NodeGraphErrorCode, NodeId, NodePort, NodeRegistry, PortId, RequestContext,
    SourceAsset, SourceConnection,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

/// `note` carries the interesting text; `code` is deliberately non-text.
const NDJSON: &str = concat!(
    r#"{"label":"alice","note":"  hello   world  ","code":1}"#,
    "\n",
    r#"{"label":"bob","note":"ÄÖÜ","code":2}"#,
    "\n",
    r#"{"label":"carol","note":"e\u0301cole","code":3}"#,
    "\n",
    r#"{"label":"dave","note":null,"code":4}"#,
    "\n",
    r#"{"label":"erin","note":"","code":5}"#,
    "\n",
    r#"{"label":"frank","note":"中文　空格","code":6}"#,
    "\n",
);

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

/// `source -> transform* -> output`.
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

fn normalize(
    id: u128,
    column: ColumnId,
    operation: &str,
) -> (u128, &'static str, serde_json::Value) {
    (
        id,
        "stillflow.node.normalize-text",
        json!({ "column": column, "operation": operation }),
    )
}

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("notes.ndjson"), NDJSON).expect("write fixture");

    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n2 fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n2").expect("credential ref"),
    )
    .expect("connection");

    let registry = registry();
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
        .find(|asset| asset.name == "notes.ndjson")
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

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(std::sync::Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
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

    fn compile(
        &self,
        graph: &NodeGraph,
    ) -> Result<stillflow_plan::CompiledNodeGraph, stillflow_plan::NodeGraphCompileError> {
        NodeGraphCompiler::new(NodeRegistry::deployed()).compile(
            graph,
            &AuthorizedSourceContext::new(self.asset.id, self.schema.clone())
                .expect("authorized source"),
            CompileTarget::Execution,
        )
    }

    /// Compiles and previews the last transform of the chain.
    async fn run(&self, graph: &NodeGraph, target_node: u128) -> PreviewResult {
        let compiled = self.compile(graph).expect("graph compiles");
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
        result
    }
}

fn strings(result: &PreviewResult, column: &ColumnId) -> Vec<Option<String>> {
    let index = result
        .schema
        .fields
        .iter()
        .position(|field| field.id == *column)
        .expect("column present");
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
    let index = result
        .schema
        .fields
        .iter()
        .position(|field| field.id == *column)
        .expect("column present");
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

// ---------------------------------------------------------------------------
// Fixture guard: NULL and empty string are both present and distinguishable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_has_a_null_and_an_empty_string_and_a_non_text_column() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let code = fixture.column("code");

    let graph = graph(Vec::new(), fixture.asset.id);
    // With no transforms the source schema flows straight through; preview the
    // source node itself.
    let result = fixture.run(&graph, 1).await;

    assert_eq!(
        strings(&result, &note),
        vec![
            Some("  hello   world  ".to_owned()),
            Some("ÄÖÜ".to_owned()),
            // decomposed "e" + combining acute, exactly as written in the file
            Some("e\u{0301}cole".to_owned()),
            None,
            Some(String::new()),
            Some("中文\u{3000}空格".to_owned()),
        ],
        "the fixture must contain a NULL and a distinct empty string"
    );
    assert_eq!(integers(&result, &code).len(), 6);

    let note_field = fixture
        .schema
        .fields
        .iter()
        .find(|field| field.id == note)
        .expect("note field");
    assert_eq!(note_field.data_type, LogicalType::Utf8);
    assert!(note_field.nullable, "the NULL row makes note nullable");
}

// ---------------------------------------------------------------------------
// Each operation, over real values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trim_removes_surrounding_whitespace_only() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(vec![normalize(2, note, "trim")], fixture.asset.id);
    let result = fixture.run(&graph, 2).await;

    assert_eq!(
        strings(&result, &note),
        vec![
            Some("hello   world".to_owned()),
            Some("ÄÖÜ".to_owned()),
            Some("e\u{0301}cole".to_owned()),
            None,
            Some(String::new()),
            Some("中文\u{3000}空格".to_owned()),
        ],
        "inner runs are untouched by trim, and CJK text is unchanged"
    );
}

#[tokio::test]
async fn collapse_whitespace_normalizes_runs_including_ideographic_space() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(
        vec![normalize(2, note, "collapseWhitespace")],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    assert_eq!(
        strings(&result, &note),
        vec![
            // leading and trailing runs each become exactly one space
            Some(" hello world ".to_owned()),
            Some("ÄÖÜ".to_owned()),
            Some("e\u{0301}cole".to_owned()),
            None,
            Some(String::new()),
            // U+3000 is whitespace and collapses to one ASCII space
            Some("中文 空格".to_owned()),
        ]
    );
}

#[tokio::test]
async fn case_operations_are_unicode_aware() {
    let fixture = fixture().await;
    let note = fixture.column("note");

    let lower = graph(vec![normalize(2, note, "lowercase")], fixture.asset.id);
    assert_eq!(
        strings(&fixture.run(&lower, 2).await, &note)[1],
        Some("äöü".to_owned()),
        "non-ASCII letters participate in lower-casing"
    );

    let upper = graph(vec![normalize(2, note, "uppercase")], fixture.asset.id);
    assert_eq!(
        strings(&fixture.run(&upper, 2).await, &note)[1],
        Some("ÄÖÜ".to_owned())
    );
}

#[tokio::test]
async fn unicode_nfc_composes_a_decomposed_sequence() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(vec![normalize(2, note, "unicodeNfc")], fixture.asset.id);
    let result = fixture.run(&graph, 2).await;

    let values = strings(&result, &note);
    assert_eq!(
        values[2],
        Some("\u{00e9}cole".to_owned()),
        "decomposed e + combining acute must compose to U+00E9"
    );
    assert_eq!(
        values[2].as_ref().expect("value").chars().count(),
        5,
        "composition must reduce the character count"
    );
    // CJK and already-composed text are unchanged.
    assert_eq!(values[5], Some("中文\u{3000}空格".to_owned()));
    assert_eq!(values[1], Some("ÄÖÜ".to_owned()));
}

// ---------------------------------------------------------------------------
// NULL, empty string, and schema identity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn null_is_preserved_and_empty_stays_empty_but_whitespace_only_is_cleaned() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(vec![normalize(2, note, "trim")], fixture.asset.id);
    let values = strings(&fixture.run(&graph, 2).await, &note);

    assert_eq!(values[3], None, "NULL must stay NULL, never become a value");
    assert_eq!(
        values[4],
        Some(String::new()),
        "an empty string is not NULL and stays an empty string"
    );
}

#[tokio::test]
async fn output_schema_identity_is_unchanged_by_normalization() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(vec![normalize(2, note, "uppercase")], fixture.asset.id);
    let result = fixture.run(&graph, 2).await;

    let before = fixture
        .schema
        .fields
        .iter()
        .find(|field| field.id == note)
        .expect("note field")
        .clone();
    let after = result
        .schema
        .fields
        .iter()
        .find(|field| field.id == note)
        .expect("note field");

    assert_eq!(after.name, before.name);
    assert_eq!(after.data_type, before.data_type);
    assert_eq!(after.nullable, before.nullable, "nullability is preserved");
    assert_eq!(
        result
            .schema
            .fields
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        fixture
            .schema
            .fields
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        "field order and identity are unchanged"
    );
}

// ---------------------------------------------------------------------------
// Composition and rejections
// ---------------------------------------------------------------------------

#[tokio::test]
async fn operations_compose_by_chaining_nodes() {
    let fixture = fixture().await;
    let note = fixture.column("note");

    // The product law keeps one rule per node, so a multi-operation pipeline is
    // an ordered chain. collapse -> trim -> lowercase.
    let graph = graph(
        vec![
            normalize(2, note, "collapseWhitespace"),
            normalize(3, note, "trim"),
            normalize(4, note, "lowercase"),
        ],
        fixture.asset.id,
    );
    let values = strings(&fixture.run(&graph, 4).await, &note);

    assert_eq!(values[0], Some("hello world".to_owned()));
    assert_eq!(values[1], Some("äöü".to_owned()));
    assert_eq!(values[5], Some("中文 空格".to_owned()));
    assert_eq!(values[3], None, "the chain preserves NULL throughout");
}

#[tokio::test]
async fn a_non_text_column_is_rejected() {
    let fixture = fixture().await;
    let code = fixture.column("code");
    let graph = graph(vec![normalize(2, code, "trim")], fixture.asset.id);

    let error = fixture
        .compile(&graph)
        .expect_err("non-text must be rejected");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
    assert_eq!(error.node_id(), Some(node(2)));
}

#[tokio::test]
async fn an_unknown_operation_is_rejected_at_config_validation() {
    let fixture = fixture().await;
    let note = fixture.column("note");
    let graph = graph(vec![normalize(2, note, "shout")], fixture.asset.id);

    let error = fixture.compile(&graph).expect_err("unknown operation");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn an_unknown_column_is_rejected() {
    let fixture = fixture().await;
    let graph = graph(
        vec![normalize(2, ColumnId::from_uuid(uuid(9999)), "trim")],
        fixture.asset.id,
    );

    let error = fixture.compile(&graph).expect_err("unknown column");
    assert_eq!(error.code(), NodeGraphErrorCode::UnknownColumn);
}

// ---------------------------------------------------------------------------
// Regression: the pre-wave text nodes still behave
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_trim_and_replace_literal_still_work() {
    let fixture = fixture().await;
    let note = fixture.column("note");

    let graph = graph(
        vec![
            (2, "stillflow.node.trim", json!({ "column": note })),
            (
                3,
                "stillflow.node.replace-literal",
                json!({
                    "column": note,
                    "from": {"kind": "utf8", "value": "hello   world"},
                    "to": {"kind": "utf8", "value": "hi"}
                }),
            ),
        ],
        fixture.asset.id,
    );
    let values = strings(&fixture.run(&graph, 3).await, &note);

    assert_eq!(values[0], Some("hi".to_owned()));
    assert_eq!(values[2], Some("e\u{0301}cole".to_owned()));
    assert_eq!(values[3], None);
}
