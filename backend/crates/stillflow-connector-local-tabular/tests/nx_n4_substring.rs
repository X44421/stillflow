//! NX-N4 (#368) acceptance, slice 4: `Substring` on real data.
//!
//! Contract: `docs/contracts/issue-368-nx-n4-expression-extension-contract.md`
//! §3.3 (Utf8 input, literal 1-based `start`, non-negative `length`, clamping
//! rather than errors, Unicode scalar indexing) and §4 (result type and
//! nullability).

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
    SourceAsset, SourceConnection,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

/// `text` holds the interesting shapes: ASCII, CJK, a non-BMP scalar, an
/// empty string, and a NULL. `plain` is a non-nullable-enough Utf8 column
/// used to exercise the type gate.
const NDJSON: &str = concat!(
    r#"{"id":1,"text":"abcdef","plain":"abcdef","n":1}"#,
    "\n",
    r#"{"id":2,"text":"日本語テキスト","plain":"abcdef","n":2}"#,
    "\n",
    r#"{"id":3,"text":"a😀b","plain":"abcdef","n":3}"#,
    "\n",
    r#"{"id":4,"text":"","plain":"abcdef","n":4}"#,
    "\n",
    r#"{"id":5,"text":null,"plain":"abcdef","n":5}"#,
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

/// `Expr::Substring` with the contract's 1-based literal `start`.
fn substring(expression: Expr, start: u32, length: u32) -> Expr {
    Expr::Substring {
        expression: Box::new(expression),
        start,
        length,
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
    fs::write(root.path().join("texts.ndjson"), NDJSON).expect("write fixture");
    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n4 substring fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n4-substring").expect("credential ref"),
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
        .find(|asset| asset.name == "texts.ndjson")
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
                    "id": uuid(0xBEEF),
                    "name": "piece",
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

    /// Compiles, executes and returns the derived column as `Option<String>`s.
    async fn sliced(
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
            .find(|field| field.name == "piece")
            .expect("piece field");
        let data_type = field.data_type.clone();
        let nullable = field.nullable;
        let index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "piece")
            .expect("piece index");
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
async fn a_first_scalar_substring_is_one_based() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (data_type, _, values) = fixture.sliced(substring(column(text), 1, 3), true).await;

    assert_eq!(data_type, LogicalType::Utf8);
    assert_eq!(
        values,
        vec![
            Some("abc".to_owned()),
            // Three Unicode scalars, not three bytes.
            Some("日本語".to_owned()),
            Some("a😀b".to_owned()),
            Some(String::new()),
            None,
        ]
    );
}

#[tokio::test]
async fn an_offset_start_counts_scalars_not_bytes() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, _, values) = fixture.sliced(substring(column(text), 3, 2), true).await;

    assert_eq!(
        values,
        vec![
            Some("cd".to_owned()),
            // The 3rd scalar of the CJK text, not the 3rd byte.
            Some("語テ".to_owned()),
            // `a😀b` has three scalars, so start 3 is `b`: a non-BMP scalar
            // occupies exactly one index, not four bytes.
            Some("b".to_owned()),
            Some(String::new()),
            None,
        ]
    );
}

#[tokio::test]
async fn a_non_bmp_scalar_is_never_split() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, _, values) = fixture.sliced(substring(column(text), 2, 1), true).await;

    // Row 3 is `a😀b`: index 2 is the whole emoji, never one of its four
    // UTF-8 bytes and never an unpaired surrogate.
    assert_eq!(values[2], Some("😀".to_owned()));
}

#[tokio::test]
async fn a_start_beyond_the_end_is_clamped_to_the_empty_string() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, _, values) = fixture.sliced(substring(column(text), 99, 3), true).await;

    assert_eq!(
        values,
        vec![
            Some(String::new()),
            Some(String::new()),
            Some(String::new()),
            Some(String::new()),
            None,
        ],
        "an out-of-range start yields the empty string, never an error"
    );
}

#[tokio::test]
async fn a_length_beyond_the_remainder_yields_the_remainder() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, _, values) = fixture.sliced(substring(column(text), 2, 99), true).await;

    assert_eq!(
        values,
        vec![
            Some("bcdef".to_owned()),
            Some("本語テキスト".to_owned()),
            Some("😀b".to_owned()),
            Some(String::new()),
            None,
        ]
    );
}

#[tokio::test]
async fn a_zero_length_yields_the_empty_string() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, _, values) = fixture.sliced(substring(column(text), 1, 0), true).await;

    assert_eq!(
        values,
        vec![
            Some(String::new()),
            Some(String::new()),
            Some(String::new()),
            Some(String::new()),
            None,
        ]
    );
}

#[tokio::test]
async fn null_input_yields_null_and_never_a_value() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let (_, nullable, values) = fixture.sliced(substring(column(text), 1, 3), true).await;

    assert!(nullable, "a nullable input widens the result nullability");
    assert_eq!(values[4], None, "NULL input stays NULL");
}

#[tokio::test]
async fn a_non_utf8_input_is_rejected() {
    let fixture = fixture().await;
    let id = fixture.column("id");
    let graph = fixture.graph_with(substring(column(id), 1, 3), true);
    let error = fixture
        .compile(&graph)
        .expect_err("substring requires a utf8 input");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn a_zero_start_is_rejected_as_a_shape_violation() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let graph = fixture.graph_with(substring(column(text), 0, 3), true);
    // `start` is 1-based by contract §3.3; zero is not an out-of-range
    // request to clamp but an invalid literal.
    let error = fixture
        .compile(&graph)
        .expect_err("start must be at least 1");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn a_nullable_result_refuses_a_non_nullable_declaration() {
    let fixture = fixture().await;
    let text = fixture.column("text");
    let graph = fixture.graph_with(substring(column(text), 1, 3), false);
    let error = fixture
        .compile(&graph)
        .expect_err("a narrower declared nullability must be refused");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}
