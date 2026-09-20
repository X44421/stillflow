//! NX-N4 (#368) acceptance, slice 3: `Conditional` on real data.
//!
//! Contract: `docs/contracts/issue-368-nx-n4-expression-extension-contract.md`
//! §3.2 (Boolean predicate, one branch type, NULL predicate takes `otherwise`)
//! and §4 (result type and nullability).

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, StringArray};
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

const NDJSON: &str = concat!(
    r#"{"id":1,"score":10}"#,
    "\n",
    r#"{"id":2,"score":50}"#,
    "\n",
    r#"{"id":3,"score":null}"#,
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
fn column(id: ColumnId) -> Expr {
    Expr::Column(id)
}
fn text(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value.to_owned()))
}
fn integer(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(value))
}
fn conditional(predicate: Expr, then: Expr, otherwise: Expr) -> Expr {
    Expr::Conditional {
        predicate: Box::new(predicate),
        then: Box::new(then),
        otherwise: Box::new(otherwise),
    }
}
fn at_least(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        operator: BinaryOperator::GreaterThanOrEqual,
        right: Box::new(right),
    }
}

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("scores.ndjson"), NDJSON).expect("write fixture");
    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n4 conditional fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n4-conditional").expect("credential ref"),
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
        .find(|asset| asset.name == "scores.ndjson")
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

    fn graph_with(&self, expression: Expr, nullable: bool) -> NodeGraph {
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
                    "id": uuid(0xF00D),
                    "name": "band",
                    "dataType": {"kind": "utf8"},
                    "nullable": nullable,
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

    async fn banded(
        &self,
        expression: Expr,
        nullable: bool,
    ) -> (LogicalType, bool, Vec<Option<String>>) {
        let graph = self.graph_with(expression, nullable);
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
            .find(|field| field.name == "band")
            .expect("band field");
        let data_type = field.data_type.clone();
        let nullable = field.nullable;
        let index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "band")
            .expect("band index");
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
        (data_type, nullable, values)
    }
}

#[tokio::test]
async fn a_null_predicate_takes_the_otherwise_branch() {
    let fixture = fixture().await;
    let score = fixture.column("score");
    let (data_type, nullable, values) = fixture
        .banded(
            conditional(
                at_least(column(score), integer(40)),
                text("senior"),
                text("junior"),
            ),
            false,
        )
        .await;

    assert_eq!(data_type, LogicalType::Utf8);
    assert_eq!(
        values,
        vec![
            Some("junior".to_owned()),
            Some("senior".to_owned()),
            // NULL predicate (NULL score) takes `otherwise`.
            Some("junior".to_owned()),
        ]
    );
    assert!(
        !nullable,
        "both branches are non-null, so the result is non-null even though the predicate is nullable"
    );
}

#[tokio::test]
async fn a_nullable_branch_makes_the_result_nullable() {
    let fixture = fixture().await;
    let score = fixture.column("score");
    let (_, nullable, _) = fixture
        .banded(
            conditional(
                at_least(column(score), integer(40)),
                text("senior"),
                Expr::Literal(ScalarValue::Null),
            ),
            true,
        )
        .await;
    assert!(nullable, "a nullable branch widens the result nullability");
}

#[tokio::test]
async fn a_nullable_branch_refuses_a_non_nullable_declaration() {
    let fixture = fixture().await;
    let score = fixture.column("score");
    let graph = fixture.graph_with(
        conditional(
            at_least(column(score), integer(40)),
            text("senior"),
            Expr::Literal(ScalarValue::Null),
        ),
        false,
    );
    let error = fixture
        .compile(&graph)
        .expect_err("a narrower declared nullability must be refused");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn mismatched_branch_types_are_rejected() {
    let fixture = fixture().await;
    let score = fixture.column("score");
    let graph = fixture.graph_with(
        conditional(
            at_least(column(score), integer(40)),
            text("senior"),
            integer(1),
        ),
        true,
    );
    let error = fixture.compile(&graph).expect_err("branch types differ");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn a_non_boolean_predicate_is_rejected() {
    let fixture = fixture().await;
    let score = fixture.column("score");
    let graph = fixture.graph_with(conditional(column(score), text("a"), text("b")), true);
    let error = fixture.compile(&graph).expect_err("predicate is numeric");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}
