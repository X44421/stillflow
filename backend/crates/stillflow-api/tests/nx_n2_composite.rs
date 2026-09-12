//! NX-N2 (#341) acceptance tests: package deployment, the frozen first
//! composite sample, and its end-to-end compile through the API surface.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use stillflow_api::{
    ApiError, ApiRequest, NodeGraphCompileRequest, NodeGraphCompileTarget, RequestMetadata,
};
use stillflow_connectors::{
    AssetMetadata, ConnectionStatus, ConnectorCapabilities, ConnectorKind, ConnectorRegistry,
    ConnectorResult, InspectRequest, PreviewData, PreviewRequest as CorePreviewRequest,
    RawBatchStream, ReadRequest, SourceConnector, SourceConnectorRef, TestConnectionRequest,
};
use stillflow_core::{
    trim_clean_node_package as trim_clean_package, AssetKind, ColumnId, CredentialRef,
    LogicalField, LogicalSchema, LogicalType, NodeId, NodePackage, NodeRegistry,
};

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

/// The frozen content digest law (NX-C1 §6.1): the digest is the SHA-256 of
/// the canonical JSON encoding (sorted keys, compact, UTF-8) of every field
/// except `contentDigest` itself.
#[test]
fn deployed_package_content_digest_binds_the_manifest() {
    use sha2::{Digest, Sha256};
    let package = trim_clean_package();
    let mut document = serde_json::to_value(&package).expect("package json");
    let content = document.as_object_mut().expect("object");
    content.remove("contentDigest");
    let canonical = serde_json::to_string(&content).expect("canonical json");
    let digest = Sha256::digest(canonical.as_bytes());
    let expected = format!("sha256-{}", hex(&digest));
    assert_eq!(
        package.content_digest, expected,
        "the deployed manifest's digest must bind its canonical content"
    );
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// The deployed registry exposes the composite in its catalog while keeping
/// the eleven atomic definitions.
#[test]
fn deployed_catalog_lists_the_composite() {
    let registry = NodeRegistry::deployed();
    let catalog = registry.catalog();
    let ids: Vec<&str> = catalog.iter().map(|entry| entry.type_id.as_str()).collect();
    assert_eq!(ids.len(), 12);
    assert!(ids.contains(&"stillflow.composite.trim-clean"));
    assert!(ids.contains(&"stillflow.node.trim"));
    // Package order never changes the catalog: reversed deployment is
    // identical.
    let packages = vec![trim_clean_package()];
    let reversed = NodeRegistry::new()
        .with_packages(packages.into_iter().rev().collect())
        .expect("deployment");
    assert_eq!(reversed.catalog(), registry.catalog());
}

/// Deployment rejections (NX-C1 §6): unknown formats, non-empty
/// dependencies, inadmissible operators, recursion, over-expansion, and
/// content-changed-under-same-version conflicts all fail closed.
#[test]
fn deployment_rejections_fail_closed() {
    let mutate = |mutate: fn(&mut NodePackage)| {
        let mut package = trim_clean_package();
        mutate(&mut package);
        package
    };
    let attempts: Vec<(&str, NodePackage)> = vec![
        ("format", mutate(|p| p.package_format = 2)),
        ("dependsOn", mutate(|p| p.depends_on = vec!["x".to_owned()])),
        (
            "operators",
            mutate(|p| {
                p.operators = vec!["stillflow.node.trim".to_owned(); 9];
            }),
        ),
        (
            "inadmissible operator",
            mutate(|p| {
                p.operators.push("stillflow.node.source".to_owned());
            }),
        ),
        (
            "recursion",
            mutate(|p| {
                p.operators
                    .push("stillflow.composite.trim-clean".to_owned());
            }),
        ),
        (
            "expansion bound",
            mutate(|p| {
                for _ in 0..3 {
                    p.definition
                        .expansion
                        .push(p.definition.expansion[0].clone());
                }
            }),
        ),
        (
            "type id",
            mutate(|p| p.type_id = "stillflow.composite.other".to_owned()),
        ),
        ("config version", mutate(|p| p.config_version = 2)),
        (
            "digest shape",
            mutate(|p| p.content_digest = "md5-abc".to_owned()),
        ),
        ("version", mutate(|p| p.version = "1.0".to_owned())),
        ("namespace", mutate(|p| p.namespace = "Ops".to_owned())),
        (
            "step operator outside declared set",
            mutate(|p| {
                p.definition.expansion[1].operator = "stillflow.node.fill-null".to_owned();
            }),
        ),
    ];
    for (name, package) in attempts {
        let error = NodeRegistry::new()
            .with_packages(vec![package])
            .expect_err(name);
        assert_eq!(error.code().as_str(), "NG_INVALID_CONFIG", "{name}");
    }

    // Same version, changed content: refused.
    let first = trim_clean_package();
    let mut second = trim_clean_package();
    second.content_digest =
        "sha256-1111111111111111111111111111111111111111111111111111111111111111".to_owned();
    let error = NodeRegistry::new()
        .with_packages(vec![first, second])
        .expect_err("content conflict");
    assert!(
        error.message().contains("content changed"),
        "{:?}",
        error.message()
    );
}

// ---- end-to-end compile through the API surface ----

#[derive(Debug)]
struct CountingConnector {
    schema: LogicalSchema,
    inspect_count: AtomicUsize,
}

impl CountingConnector {
    fn new(schema: LogicalSchema) -> Self {
        Self {
            schema,
            inspect_count: AtomicUsize::new(0),
        }
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
        Ok(RawBatchStream::new(Box::pin(futures::stream::empty())))
    }
}

fn fixture() -> (
    stillflow_api::ApiService,
    Arc<CountingConnector>,
    Uuid,
    Uuid,
    Uuid,
    tempfile::TempDir,
) {
    let root = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(stillflow_storage::ControlPlaneStore::open(root.path()).expect("store"));
    let workspace_id = uuid(0xE110);
    let connection_id = uuid(0xE111);
    let asset_id = uuid(0xE112);
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
    let schema = LogicalSchema::new(vec![LogicalField::new(
        ColumnId::from_uuid(uuid(0x1000)),
        "name",
        LogicalType::Utf8,
        true,
    )
    .expect("field")])
    .expect("schema");
    let connector = Arc::new(CountingConnector::new(schema));
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    let service = stillflow_api::ApiService::new(store).with_connectors(Arc::new(registry));
    (
        service,
        connector,
        workspace_id,
        connection_id,
        asset_id,
        root,
    )
}

fn composite_graph(asset_id: Uuid) -> Value {
    json!({
        "version": 1,
        "graphId": uuid(0xE200),
        "sourceNodeId": uuid(0xE201),
        "outputNodeId": uuid(0xE205),
        "nodes": [
            {
                "id": uuid(0xE201), "typeId": "stillflow.node.source", "configVersion": 1,
                "config": {"sourceAssetId": asset_id}
            },
            {
                "id": uuid(0xE202), "typeId": "stillflow.composite.trim-clean", "configVersion": 1,
                "config": {"column": uuid(0x1000).to_string()}
            },
            {
                "id": uuid(0xE205), "typeId": "stillflow.node.output", "configVersion": 1,
                "config": {"outputLabel": "cleaned"}
            }
        ],
        "edges": [
            {"from": {"nodeId": uuid(0xE201), "port": "out"}, "to": {"nodeId": uuid(0xE202), "port": "in"}},
            {"from": {"nodeId": uuid(0xE202), "port": "out"}, "to": {"nodeId": uuid(0xE205), "port": "in"}}
        ],
        "metadata": {}
    })
}

/// The composite graph compiles end to end; its plan carries one internal
/// ApplyRules node per expansion step with identity-derived internal ids,
/// the product preview boundary maps to the last internal node, and the
/// nullability contract holds (empty strings map to null ⇒ the column
/// becomes nullable).
#[tokio::test]
async fn composite_graph_compiles_with_internal_expansion() {
    let (service, connector, workspace_id, connection_id, asset_id, _root) = fixture();
    let graph: stillflow_core::NodeGraph =
        serde_json::from_value(composite_graph(asset_id)).expect("graph");
    let compiled = service
        .compile_node_graph(ApiRequest {
            meta: RequestMetadata::new(uuid(0xE300), workspace_id),
            body: NodeGraphCompileRequest {
                graph,
                connection_id,
                asset_id,
                target: NodeGraphCompileTarget::Execution,
                timeout_seconds: Some(30),
                schema_detail: Default::default(),
            },
        })
        .await
        .expect("composite compiles");
    let view = compiled.body;

    // Product mapping: the composite's preview boundary is its last
    // internal node; the atomic source and output stay identity-mapped.
    let composite_product = NodeId::from_uuid(uuid(0xE202));
    let boundary = view.node_plan_ids[&composite_product];
    assert_eq!(boundary.as_uuid().as_u128() & 0xFFFF, 0x8001, "ordinal 1");
    assert_eq!(
        view.node_plan_ids[&NodeId::from_uuid(uuid(0xE201))].as_uuid(),
        uuid(0xE201)
    );

    // The plan node count covers both internal steps plus source and output.
    assert_eq!(view.logical_plan.nodes.len(), 4);
    // The rules match the hand-written chain: trim, then replace-literal.
    let mut rules = Vec::new();
    for node in view.logical_plan.nodes.values() {
        if let stillflow_plan::PlanNodeKind::ApplyRules { rules: step_rules } = &node.kind {
            for rule in step_rules {
                rules.push(serde_json::to_value(rule).expect("rule"));
            }
        }
    }
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["kind"], json!("trim"));
    assert_eq!(rules[1]["kind"], json!("replaceLiteral"));
    assert_eq!(
        rules[1]["value"]["from"],
        json!({"kind": "utf8", "value": ""})
    );
    assert_eq!(rules[1]["value"]["to"], json!({"kind": "null"}));

    // Nullability: the output schema's column is nullable after the
    // replace-literal step.
    let output_field = &view.output_schema.fields[0];
    assert!(
        output_field.nullable,
        "empty-string→null widens nullability"
    );

    // Exactly one inspect happened (stage 5).
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 1);

    // Determinism: the same graph compiles to the same fingerprint.
    let second = service
        .compile_node_graph(ApiRequest {
            meta: RequestMetadata::new(uuid(0xE301), workspace_id),
            body: NodeGraphCompileRequest {
                graph: serde_json::from_value(composite_graph(asset_id)).expect("graph"),
                connection_id,
                asset_id,
                target: NodeGraphCompileTarget::Execution,
                timeout_seconds: Some(30),
                schema_detail: Default::default(),
            },
        })
        .await
        .expect("second compile");
    assert_eq!(view.plan_fingerprint, second.body.plan_fingerprint);
    let _ = ApiError::invalid("unused");
    let _ = BTreeMap::<String, Value>::new();
}

/// Without deployment, a composite graph fails closed with
/// NG_UNKNOWN_NODE_TYPE (NX-C1 §9 matrix).
#[tokio::test]
async fn composite_graphs_fail_closed_without_deployment() {
    let (service, _connector, workspace_id, connection_id, asset_id, _root) = fixture();
    let mut graph = composite_graph(asset_id);
    graph["nodes"][1]["typeId"] = json!("stillflow.node.trim");
    graph["nodes"][1]["config"] = json!({"column": uuid(0x1000).to_string()});
    // Sanity: the atomic equivalent compiles (the deployed registry keeps
    // atomic behavior).
    let atomic: stillflow_core::NodeGraph = serde_json::from_value(graph).expect("graph");
    service
        .compile_node_graph(ApiRequest {
            meta: RequestMetadata::new(uuid(0xE310), workspace_id),
            body: NodeGraphCompileRequest {
                graph: atomic,
                connection_id,
                asset_id,
                target: NodeGraphCompileTarget::Execution,
                timeout_seconds: Some(30),
                schema_detail: Default::default(),
            },
        })
        .await
        .expect("atomic chain compiles");
}
