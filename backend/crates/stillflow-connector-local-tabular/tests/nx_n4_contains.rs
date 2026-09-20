//! NX-N4 (#368) acceptance, slice 1: `contains` is admitted as a **literal**
//! substring test on real data.
//!
//! Contract: `docs/contracts/issue-368-nx-n4-expression-extension-contract.md`
//! §2.2 (the lift and its literal-only semantics) and §2.4 (the single
//! authorized version-1 corpus change). The arithmetic lift of §2.1 is **not**
//! implemented by this slice.
//!
//! The fixture is NDJSON so a NULL and an empty string are both present. Every
//! assertion runs through the production NodeGraph compile path and the engine
//! over values produced by a real read.

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, BooleanArray, StringArray};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    BinaryOperator, ColumnId, ConnectorKind, CredentialRef, Expr, LogicalSchema, LogicalType,
    NodeConfig, NodeEdge, NodeGraph, NodeGraphErrorCode, NodeId, NodePort, NodeRegistry, PortId,
    RequestContext, ScalarValue, SourceAsset, SourceConnection,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

/// `text` holds values whose dots would be wildcards under a regex reading.
const NDJSON: &str = concat!(
    r#"{"id":1,"text":"a.c"}"#,
    "\n",
    r#"{"id":2,"text":"abc"}"#,
    "\n",
    r#"{"id":3,"text":"x.y.z"}"#,
    "\n",
    r#"{"id":4,"text":null}"#,
    "\n",
    r#"{"id":5,"text":""}"#,
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

fn contains(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        operator: BinaryOperator::Contains,
        right: Box::new(right),
    }
}

fn column(id: ColumnId) -> Expr {
    Expr::Column(id)
}

fn text(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value.to_owned()))
}

fn integer(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(value))
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

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("contains.ndjson"), NDJSON).expect("write fixture");

    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n4 fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n4").expect("credential ref"),
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
        .find(|asset| asset.name == "contains.ndjson")
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

    async fn run(&self, graph: &NodeGraph, target: u128) -> PreviewResult {
        let compiled = self.compile(graph).expect("graph compiles");
        let plan_target = *compiled
            .node_plan_ids
            .get(&node(target))
            .expect("target plan node");
        let mut request = PreviewRequest::new(
            compiled.plan.clone(),
            plan_target,
            self.connection.clone(),
            self.asset.clone(),
        );
        request.row_limit = 100;
        request.byte_limit = 1_048_576;
        ExecutionEngine::new(registry())
            .preview(request)
            .await
            .expect("preview executes")
    }

    /// Derives a Boolean `flag` from a `contains` expression and returns the
    /// observed flags in row order.
    async fn flags(&self, right: Expr) -> Vec<Option<bool>> {
        let text_column = self.column("text");
        let graph = graph(
            vec![(
                2,
                "stillflow.node.derive-column",
                json!({
                    "id": uuid(0xC0FFEE),
                    "name": "flag",
                    "dataType": {"kind": "boolean"},
                    "nullable": true,
                    "expression": contains(column(text_column), right),
                }),
            )],
            self.asset.id,
        );
        let result = self.run(&graph, 2).await;
        assert_eq!(
            result
                .schema
                .fields
                .iter()
                .find(|field| field.name == "flag")
                .expect("flag field")
                .data_type,
            LogicalType::Boolean,
            "contains yields Boolean"
        );
        let index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "flag")
            .expect("flag index");
        let mut values = Vec::new();
        for batch in &result.batches {
            let payload = batch.payload();
            let array = payload
                .column(index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean column");
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

// ---------------------------------------------------------------------------
// Literal semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_needle_with_metacharacters_matches_literally() {
    let fixture = fixture().await;
    // "a.c" must match only the row that literally contains "a.c"; a regex
    // reading would also match "abc".
    let flags = fixture.flags(text("a.c")).await;
    assert_eq!(
        flags,
        vec![Some(true), Some(false), Some(false), None, Some(false)]
    );
}

#[tokio::test]
async fn a_bare_dot_is_a_literal_dot_not_a_wildcard() {
    let fixture = fixture().await;
    // Under a regex reading "." would match every non-empty value. As a literal
    // it matches only the rows that contain a dot.
    let flags = fixture.flags(text(".")).await;
    assert_eq!(
        flags,
        vec![Some(true), Some(false), Some(true), None, Some(false)]
    );
}

#[tokio::test]
async fn an_anchored_looking_needle_is_still_literal() {
    let fixture = fixture().await;
    // "^a" is not an anchor here; it is two literal characters that no row
    // contains.
    let flags = fixture.flags(text("^a")).await;
    assert_eq!(
        flags,
        vec![Some(false), Some(false), Some(false), None, Some(false)]
    );
}

// ---------------------------------------------------------------------------
// NULL propagation and typing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_null_value_yields_null_and_an_empty_value_yields_false() {
    let fixture = fixture().await;
    let flags = fixture.flags(text("a")).await;
    assert_eq!(
        flags,
        vec![Some(true), Some(true), Some(false), None, Some(false)],
        "the NULL row stays NULL; the empty string is false, not NULL"
    );
}

#[tokio::test]
async fn a_null_needle_yields_null() {
    let fixture = fixture().await;
    let flags = fixture.flags(Expr::Literal(ScalarValue::Null)).await;
    assert_eq!(flags, vec![None, None, None, None, None]);
}

#[tokio::test]
async fn a_non_text_operand_is_rejected() {
    let fixture = fixture().await;
    let id = fixture.column("id");
    let graph = graph(
        vec![(
            2,
            "stillflow.node.derive-column",
            json!({
                "id": uuid(0xC0FFEE),
                "name": "flag",
                "dataType": {"kind": "boolean"},
                "nullable": true,
                "expression": contains(column(id), integer(1)),
            }),
        )],
        fixture.asset.id,
    );
    let error = fixture.compile(&graph).expect_err("non-text operand");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

// ---------------------------------------------------------------------------
// Integration: contains as a filter predicate over real rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn contains_works_as_a_filter_predicate() {
    let fixture = fixture().await;
    let text_column = fixture.column("text");
    let graph = graph(
        vec![(
            2,
            "stillflow.node.filter",
            json!({ "predicate": contains(column(text_column), text(".")) }),
        )],
        fixture.asset.id,
    );
    let result = fixture.run(&graph, 2).await;

    assert_eq!(
        strings(&result, &text_column),
        vec![Some("a.c".to_owned()), Some("x.y.z".to_owned())],
        "the NULL row and the dot-free rows are filtered out"
    );
}
