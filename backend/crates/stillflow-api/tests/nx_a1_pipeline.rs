//! NX-A1 (#338) acceptance tests: the request-pipeline order, the timeout
//! law, and the safe diagnostic surface, verified against an in-process
//! `ApiService` with a counting connector stub (NX-C0 contract §7/§8).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use stillflow_api::{
    ApiError, ApiErrorCode, ApiRequest, NodeGraphCompileRequest, NodeGraphCompileTarget,
    NodeGraphPreviewRequest, RequestMetadata,
};
use stillflow_connectors::{
    AssetMetadata, ConnectionStatus, ConnectorCapabilities, ConnectorKind, ConnectorRegistry,
    ConnectorResult, InspectRequest, PreviewData, PreviewRequest as CorePreviewRequest,
    RawBatchStream, ReadRequest, SourceConnector, SourceConnectorRef, TestConnectionRequest,
};
use stillflow_core::{
    AssetKind, ColumnId, CredentialRef, LogicalField, LogicalSchema, LogicalType, NodeConfig,
    NodeEdge, NodeGraph, NodeId, PortId,
};
use stillflow_storage::ControlPlaneStore;

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn column(value: u128) -> ColumnId {
    ColumnId::from_uuid(uuid(value))
}

/// Connector stub whose inspect/read calls are counted, proving that
/// pure-graph rejections (stage 4) and timeout rejections never reach the
/// connector.
#[derive(Debug)]
struct CountingConnector {
    schema: LogicalSchema,
    inspect_count: AtomicUsize,
    read_count: AtomicUsize,
}

impl CountingConnector {
    fn new(schema: LogicalSchema) -> Self {
        Self {
            schema,
            inspect_count: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
        }
    }

    fn inspect_calls(&self) -> usize {
        self.inspect_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn read_calls(&self) -> usize {
        self.read_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl SourceConnector for CountingConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::LocalFile
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            schema_discovery: true,
            preview: true,
            streaming: true,
            ..ConnectorCapabilities::default()
        }
    }

    async fn test_connection(
        &self,
        _connection: &stillflow_core::SourceConnection,
        _request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus> {
        Ok(ConnectionStatus::Ok)
    }

    async fn discover(
        &self,
        _connection: &stillflow_core::SourceConnection,
        _request: stillflow_connectors::DiscoverRequest,
    ) -> ConnectorResult<Vec<stillflow_core::SourceAsset>> {
        Ok(Vec::new())
    }

    async fn inspect(
        &self,
        _connection: &stillflow_core::SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<AssetMetadata> {
        request.context.ensure_active()?;
        self.inspect_count.fetch_add(1, Ordering::SeqCst);
        Ok(AssetMetadata::new(self.schema.clone(), "fixture"))
    }

    async fn preview(
        &self,
        _connection: &stillflow_core::SourceConnection,
        request: CorePreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        request.context.ensure_active()?;
        Ok(PreviewData::empty(self.schema.clone()))
    }

    async fn checkpoint(
        &self,
        _connection: &stillflow_core::SourceConnection,
        _request: stillflow_connectors::CheckpointRequest,
    ) -> ConnectorResult<Option<stillflow_connectors::Checkpoint>> {
        Ok(None)
    }

    async fn read_batches(
        &self,
        _connection: &stillflow_core::SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<RawBatchStream> {
        request.context.ensure_active()?;
        self.read_count.fetch_add(1, Ordering::SeqCst);
        Ok(RawBatchStream::new(Box::pin(futures::stream::empty())))
    }
}

fn source_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalField::new(column(101), "name", LogicalType::Utf8, true).expect("field"),
        LogicalField::new(column(102), "age", LogicalType::Int64, true).expect("field"),
    ])
    .expect("schema")
}

struct Fixture {
    _root: tempfile::TempDir,
    service: stillflow_api::ApiService,
    connector: Arc<CountingConnector>,
    workspace_id: Uuid,
    connection_id: Uuid,
    asset_id: Uuid,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
    let workspace_id = uuid(0xA110);
    let connection_id = uuid(0xA111);
    let asset_id = uuid(0xA112);
    let at = Utc::now();
    store.create_workspace(workspace_id, at).expect("workspace");
    store
        .create_source_connection(
            workspace_id,
            connection_id,
            ConnectorKind::LocalFile,
            "fixture",
            json!({"root": "/data/fixture"}),
            CredentialRef::new("cred://local/fixture").expect("cred"),
            at,
        )
        .expect("connection");
    store
        .create_source_asset(
            workspace_id,
            connection_id,
            asset_id,
            AssetKind::File,
            "values.csv",
            json!({"path": "/values.csv"}),
            at,
        )
        .expect("asset");
    let connector = Arc::new(CountingConnector::new(source_schema()));
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    let service = stillflow_api::ApiService::new(store).with_connectors(Arc::new(registry));
    Fixture {
        _root: root,
        service,
        connector,
        workspace_id,
        connection_id,
        asset_id,
    }
}

fn meta(workspace_id: Uuid, request_id: Uuid) -> RequestMetadata {
    RequestMetadata::new(request_id, workspace_id)
}

fn graph_json(asset_id: Uuid, mutate: fn(&mut Value)) -> NodeGraph {
    let mut value = json!({
        "version": 1,
        "graphId": uuid(0xB001),
        "sourceNodeId": node(1),
        "outputNodeId": node(5),
        "nodes": [
            {
                "id": node(1), "typeId": "stillflow.node.source", "configVersion": 1,
                "config": {
                    "sourceAssetId": asset_id,
                    "projection": [column(101), column(102)]
                }
            },
            {
                "id": node(5), "typeId": "stillflow.node.output", "configVersion": 1,
                "config": {"outputLabel": "cleaned"}
            }
        ],
        "edges": [
            {"from": {"nodeId": node(1), "port": "out"}, "to": {"nodeId": node(5), "port": "in"}}
        ],
        "metadata": {}
    });
    mutate(&mut value);
    serde_json::from_value(value).expect("decodable graph")
}

fn compile_request(
    fixture: &Fixture,
    graph: NodeGraph,
    timeout_seconds: Option<u64>,
) -> ApiRequest<NodeGraphCompileRequest> {
    ApiRequest {
        meta: meta(fixture.workspace_id, uuid(0xC001)),
        body: NodeGraphCompileRequest {
            graph,
            connection_id: fixture.connection_id,
            asset_id: fixture.asset_id,
            target: NodeGraphCompileTarget::Execution,
            timeout_seconds,
            schema_detail: Default::default(),
        },
    }
}

#[tokio::test]
async fn stage4_failures_perform_zero_connector_calls() {
    let fixture = fixture();

    // Decodable but unknown node type: registry lookup fails at stage 4.
    let unknown_type = graph_json(fixture.asset_id, |value| {
        value["nodes"][1]["typeId"] = json!("stillflow.node.unknown");
    });
    let error = fixture
        .service
        .compile_node_graph(compile_request(&fixture, unknown_type, Some(30)))
        .await
        .expect_err("unknown node type rejected");
    assert_eq!(error.code, ApiErrorCode::InvalidRequest);
    assert_eq!(fixture.connector.inspect_calls(), 0, "no inspect calls");

    // Decodable but source binding mismatch: the graph points at a foreign
    // asset. The failure stays a not-found and the connector is untouched.
    let foreign_asset = graph_json(uuid(0xDEAD), |_value| {});
    let error = fixture
        .service
        .compile_node_graph(compile_request(&fixture, foreign_asset, Some(30)))
        .await
        .expect_err("foreign binding rejected");
    assert_eq!(error.code, ApiErrorCode::NotFound);
    assert_eq!(fixture.connector.inspect_calls(), 0, "no inspect calls");

    // Decodable but branching topology: rejected before the inspect.
    let branching = NodeGraph::new(
        uuid(0xB002),
        node(1),
        node(5),
        vec![
            NodeConfig::new(
                node(1),
                "stillflow.node.source",
                1,
                json!({"sourceAssetId": fixture.asset_id}),
                BTreeMap::new(),
            )
            .expect("source"),
            NodeConfig::new(
                node(5),
                "stillflow.node.output",
                1,
                json!({"outputLabel": "cleaned"}),
                BTreeMap::new(),
            )
            .expect("output"),
            NodeConfig::new(
                node(6),
                "stillflow.node.select",
                1,
                json!({"columns": [column(101)]}),
                BTreeMap::new(),
            )
            .expect("select"),
        ],
        vec![
            NodeEdge {
                from: stillflow_core::NodePort {
                    node_id: node(1),
                    port: PortId::new("out").expect("port"),
                },
                to: stillflow_core::NodePort {
                    node_id: node(5),
                    port: PortId::new("in").expect("port"),
                },
            },
            NodeEdge {
                from: stillflow_core::NodePort {
                    node_id: node(1),
                    port: PortId::new("out").expect("port"),
                },
                to: stillflow_core::NodePort {
                    node_id: node(6),
                    port: PortId::new("in").expect("port"),
                },
            },
        ],
        BTreeMap::new(),
    )
    .expect("decodable graph");
    let error = fixture
        .service
        .compile_node_graph(compile_request(&fixture, branching, Some(30)))
        .await
        .expect_err("branching rejected");
    assert_eq!(error.code, ApiErrorCode::InvalidRequest);
    assert_eq!(fixture.connector.inspect_calls(), 0, "no inspect calls");
    assert_eq!(fixture.connector.read_calls(), 0);

    // The valid graph still compiles and does reach the connector.
    let valid = graph_json(fixture.asset_id, |_value| {});
    fixture
        .service
        .compile_node_graph(compile_request(&fixture, valid, Some(30)))
        .await
        .expect("valid graph compiles");
    assert_eq!(fixture.connector.inspect_calls(), 1, "exactly one inspect");
}

#[tokio::test]
async fn timeout_law_is_accepted_or_rejected_never_clamped() {
    let fixture = fixture();

    // Compile path (§8.2 table, compile column).
    let valid = graph_json(fixture.asset_id, |_value| {});
    for timeout in [None, Some(30), Some(300)] {
        fixture
            .service
            .compile_node_graph(compile_request(&fixture, valid.clone(), timeout))
            .await
            .unwrap_or_else(|error| panic!("compile with {timeout:?} must succeed: {error:?}"));
    }
    let inspect_after_successes = fixture.connector.inspect_calls();
    for timeout in [Some(0), Some(301)] {
        let error = fixture
            .service
            .compile_node_graph(compile_request(&fixture, valid.clone(), timeout))
            .await
            .expect_err("over-bound timeout rejected");
        assert_eq!(error.code, ApiErrorCode::LimitExceeded, "{timeout:?}");
        assert_eq!(
            fixture.connector.inspect_calls(),
            inspect_after_successes,
            "boundary rejection performs no connector call"
        );
    }

    // Preview path: the operation is capped by the 30 s engine bound.
    let preview_request = |timeout: Option<u64>| ApiRequest {
        meta: meta(fixture.workspace_id, uuid(0xC002)),
        body: NodeGraphPreviewRequest {
            graph: graph_json(fixture.asset_id, |_value| {}),
            connection_id: fixture.connection_id,
            asset_id: fixture.asset_id,
            target_node_id: node(1),
            batch_size: 1024,
            row_limit: 10,
            byte_limit: 65536,
            timeout_seconds: timeout,
        },
    };
    // Absent: resolves to the 30 s operation default and gets past the
    // timeout law (the R-11 change); without an engine the preview then
    // fails with a conflict, not a limit rejection.
    let error = fixture
        .service
        .preview_node_graph(preview_request(None))
        .await
        .expect_err("no engine configured");
    assert_eq!(
        error.code,
        ApiErrorCode::Conflict,
        "absent preview timeout is accepted by the timeout law"
    );
    let inspect_before = fixture.connector.inspect_calls();
    for timeout in [Some(0), Some(31), Some(300), Some(301)] {
        let error = fixture
            .service
            .preview_node_graph(preview_request(timeout))
            .await
            .expect_err("over-cap preview timeout rejected");
        assert_eq!(error.code, ApiErrorCode::LimitExceeded, "{timeout:?}");
    }
    assert_eq!(
        fixture.connector.inspect_calls(),
        inspect_before,
        "preview boundary rejections perform no connector call"
    );
}

#[tokio::test]
async fn compile_errors_carry_the_frozen_diagnostic_shape() {
    let fixture = fixture();
    // A valid shape but unknown column: the semantic stage rejects with
    // NG_UNKNOWN_COLUMN and a located diagnostic.
    let mut value = json!({
        "version": 1,
        "graphId": uuid(0xB003),
        "sourceNodeId": node(1),
        "outputNodeId": node(5),
        "nodes": [
            {
                "id": node(1), "typeId": "stillflow.node.source", "configVersion": 1,
                "config": {
                    "sourceAssetId": fixture.asset_id,
                    "projection": [column(101), column(102)]
                }
            },
            {
                "id": node(2), "typeId": "stillflow.node.select", "configVersion": 1,
                "config": {"columns": [column(999)]}
            },
            {
                "id": node(5), "typeId": "stillflow.node.output", "configVersion": 1,
                "config": {"outputLabel": "cleaned"}
            }
        ],
        "edges": [
            {"from": {"nodeId": node(1), "port": "out"}, "to": {"nodeId": node(2), "port": "in"}},
            {"from": {"nodeId": node(2), "port": "out"}, "to": {"nodeId": node(5), "port": "in"}}
        ],
        "metadata": {}
    });
    let graph: NodeGraph = serde_json::from_value(value.clone()).expect("graph");
    let error = fixture
        .service
        .compile_node_graph(compile_request(&fixture, graph, Some(30)))
        .await
        .expect_err("unknown column rejected");
    assert_eq!(error.code, ApiErrorCode::InvalidRequest);
    assert_eq!(error.diagnostics.len(), 1, "exactly one primary diagnostic");
    let diagnostic = &error.diagnostics[0];
    assert_eq!(diagnostic.code, "NG_UNKNOWN_COLUMN");
    assert_eq!(diagnostic.node_id, Some(node(2)));
    assert!(
        diagnostic.message.contains("column is absent"),
        "{:?}",
        diagnostic.message
    );

    // The strict decoding law: unknown envelope fields are rejected (R-1).
    value["nodes"][0]["extra"] = json!(true);
    let decoded: Result<NodeGraph, _> = serde_json::from_value(value);
    assert!(
        decoded.is_err(),
        "unknown graph-level field must fail closed"
    );
    let _ = ApiError::invalid("unused");
}
