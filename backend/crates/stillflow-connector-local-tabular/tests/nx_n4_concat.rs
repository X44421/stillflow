//! NX-N4 (#368) acceptance, slice 2: `Concat` on real data.
//!
//! Contract: `docs/contracts/issue-368-nx-n4-expression-extension-contract.md`
//! §3.1 (two to eight `Utf8` operands, NULL in any operand yields NULL, empty
//! strings are values) and §4 (result type and nullability laws).

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, StringArray};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, Expr, LogicalSchema, LogicalType, NodeConfig, NodeEdge,
    NodeGraph, NodeGraphErrorCode, NodeId, NodePort, NodeRegistry, PortId, RequestContext,
    ScalarValue, SourceAsset, SourceConnection,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

const NDJSON: &str = concat!(
    r#"{"id":1,"first":"Ada","last":"Lovelace"}"#,
    "\n",
    r#"{"id":2,"first":"张","last":"三"}"#,
    "\n",
    r#"{"id":3,"first":null,"last":"X"}"#,
    "\n",
    r#"{"id":4,"first":"","last":""}"#,
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

fn concat(expressions: Vec<Expr>) -> Expr {
    Expr::Concat { expressions }
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

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("names.ndjson"), NDJSON).expect("write fixture");

    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n4 concat fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n4-concat").expect("credential ref"),
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
        .find(|asset| asset.name == "names.ndjson")
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

    fn graph_with(&self, expression: Expr) -> NodeGraph {
        let nodes = vec![
            config(
                1,
                "stillflow.node.source",
                json!({ "sourceAssetId": self.asset.id }),
            ),
            config(
                2,
                "stillflow.node.derive-column",
                json!({
                    "id": uuid(0xBEEF),
                    "name": "joined",
                    "dataType": {"kind": "utf8"},
                    "nullable": true,
                    "expression": expression,
                }),
            ),
            config(5, "stillflow.node.output", json!({ "outputLabel": "out" })),
        ];
        NodeGraph::new(
            uuid(1),
            node(1),
            node(5),
            nodes,
            vec![edge(1, 2), edge(2, 5)],
            BTreeMap::new(),
        )
        .expect("valid graph")
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

    async fn joined(&self, expression: Expr) -> (LogicalType, Vec<Option<String>>) {
        let graph = self.graph_with(expression);
        let compiled = self.compile(&graph).expect("graph compiles");
        let target = *compiled.node_plan_ids.get(&node(2)).expect("target");
        let mut request = PreviewRequest::new(
            compiled.plan.clone(),
            target,
            self.connection.clone(),
            self.asset.clone(),
        );
        request.row_limit = 100;
        request.byte_limit = 1_048_576;
        let result: PreviewResult = ExecutionEngine::new(registry())
            .preview(request)
            .await
            .expect("preview executes");

        let field = result
            .schema
            .fields
            .iter()
            .find(|field| field.name == "joined")
            .expect("joined field");
        let data_type = field.data_type.clone();
        let index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "joined")
            .expect("joined index");
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
        (data_type, values)
    }
}

#[tokio::test]
async fn concat_joins_two_operands_in_order() {
    let fixture = fixture().await;
    let (first, last) = (fixture.column("first"), fixture.column("last"));
    let (data_type, values) = fixture
        .joined(concat(vec![column(first), text(" "), column(last)]))
        .await;

    assert_eq!(data_type, LogicalType::Utf8);
    assert_eq!(
        values,
        vec![
            Some("Ada Lovelace".to_owned()),
            // Unicode scalar concatenation, no byte splitting
            Some("张 三".to_owned()),
            None,
            Some(" ".to_owned()),
        ],
        "NULL propagates and empty strings stay empty"
    );
}

#[tokio::test]
async fn concat_accepts_the_maximum_arity() {
    let fixture = fixture().await;
    let first = fixture.column("first");
    let mut operands = vec![column(first)];
    for _ in 0..7 {
        operands.push(text("|"));
    }
    let (_, values) = fixture.joined(concat(operands)).await;
    assert_eq!(values[0], Some("Ada|||||||".to_owned()));
}

#[tokio::test]
async fn concat_rejects_one_operand() {
    let fixture = fixture().await;
    let first = fixture.column("first");
    let graph = fixture.graph_with(concat(vec![column(first)]));
    let error = fixture.compile(&graph).expect_err("arity 1 is invalid");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn concat_rejects_nine_operands() {
    let fixture = fixture().await;
    let first = fixture.column("first");
    let mut operands = vec![column(first)];
    for _ in 0..8 {
        operands.push(text("|"));
    }
    let graph = fixture.graph_with(concat(operands));
    let error = fixture.compile(&graph).expect_err("arity 9 is invalid");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn concat_rejects_a_non_text_operand() {
    let fixture = fixture().await;
    let (first, id) = (fixture.column("first"), fixture.column("id"));
    let graph = fixture.graph_with(concat(vec![column(first), integer(1), column(id)]));
    let error = fixture.compile(&graph).expect_err("non-utf8 operand");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn concat_nullability_follows_its_operands() {
    let fixture = fixture().await;
    let first = fixture.column("first");
    // `first` is nullable in the fixture, so the derived column must be
    // nullable; declaring it non-nullable is refused by the existing law.
    let graph = NodeGraph::new(
        uuid(1),
        node(1),
        node(5),
        vec![
            config(
                1,
                "stillflow.node.source",
                json!({ "sourceAssetId": fixture.asset.id }),
            ),
            config(
                2,
                "stillflow.node.derive-column",
                json!({
                    "id": uuid(0xBEEF),
                    "name": "joined",
                    "dataType": {"kind": "utf8"},
                    "nullable": false,
                    "expression": concat(vec![column(first), text("-")]),
                }),
            ),
            config(5, "stillflow.node.output", json!({ "outputLabel": "out" })),
        ],
        vec![edge(1, 2), edge(2, 5)],
        BTreeMap::new(),
    )
    .expect("valid graph");

    let error = fixture
        .compile(&graph)
        .expect_err("a narrower declared nullability must be refused");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}
