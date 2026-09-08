//! SVC-A1 HTTP entry e2e (contract §6). Every assertion crosses real TCP:
//! either reqwest against a started service or the spawned
//! `stillflow-server` binary. No in-memory ApiService calls here.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;

use stillflow_api::{ServiceConfig, BOOTSTRAP_MANIFEST, E5_A1_ROUTES};
use stillflow_core::{
    ColumnId, JobOperation, MaterializePolicyV1, OperationDescriptorV1, OperationKind, SnapshotRef,
    SourceAssetRef, VerificationPolicyV1,
};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind};
use stillflow_service::wire::{decode_stream, WireView};
use stillflow_service::{start_service, AuthModeConfig, ProcessConfig, StartedService};

const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

fn timestamp() -> Value {
    json!(chrono::Utc::now().to_rfc3339())
}

#[test]
fn wire_shape_submit_job_envelope_deserializes() {
    let workspace_id = Uuid::new_v4();
    let operation = materialize_op(workspace_id, Uuid::new_v4(), Uuid::new_v4());
    let body = json!({
        "sessionId": Uuid::new_v4(),
        "planVersionId": Uuid::new_v4(),
        "planId": Uuid::new_v4(),
        "jobId": Uuid::new_v4(),
        "operation": serde_json::to_value(&operation).expect("operation"),
        "inputs": [serde_json::to_value(operation.input()).expect("inputs")],
        "executionPolicy": {"deadlineSeconds": 300},
        "outputPolicy": {},
        "queuedAt": timestamp(),
        "eventId": Uuid::new_v4(),
        "correlationId": "svc-a1",
        "actorRef": "actor:svc-a1",
    });
    let envelope = envelope_with_key(workspace_id, body);
    let parsed: Result<stillflow_api::ApiRequest<stillflow_api::SubmitJobRequest>, _> =
        serde_json::from_value(envelope);
    assert!(
        parsed.is_ok(),
        "submit envelope wire shape: {:?}",
        parsed.err()
    );
}

fn process_config(root: &std::path::Path) -> ProcessConfig {
    let service = ServiceConfig {
        managed_root: root.join("managed").to_string_lossy().into_owned(),
        bind_host: "127.0.0.1".to_owned(),
        bind_port: 0,
        shutdown_grace_seconds: 5,
        ..ServiceConfig::default()
    };
    ProcessConfig {
        service,
        authorization_mode: AuthModeConfig::LocalTrusted,
        workspace_id: Uuid::new_v4(),
    }
}

async fn start(config: ProcessConfig) -> (StartedService, String, reqwest::Client) {
    let service = start_service(config).await.expect("service starts");
    let base = format!("http://127.0.0.1:{}", service.addr.port());
    (service, base, reqwest::Client::new())
}

fn envelope(workspace_id: Uuid, body: Value) -> Value {
    json!({
        "meta": {
            "apiVersion": 1,
            "requestId": Uuid::new_v4(),
            "workspaceId": workspace_id,
        },
        "body": body,
    })
}

fn envelope_with_key(workspace_id: Uuid, body: Value) -> Value {
    let mut request = envelope(workspace_id, body);
    request["meta"]["idempotencyKey"] = json!(format!("svc-a1-{}", Uuid::new_v4()));
    request
}

async fn post_json(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .expect("request sends")
}

async fn get_json(client: &reqwest::Client, base: &str, path: &str) -> reqwest::Response {
    client
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("request sends")
}

/// Asserts a typed-binary success response: 200, the frozen Arrow IPC stream
/// media type (contract §6.1), and a decodable stream with the §6.1 reader
/// rules applied.
async fn arrow_stream(
    response: reqwest::Response,
    what: &str,
) -> stillflow_service::wire::DecodedStream {
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .expect("content type")
        .to_owned();
    let bytes = response.bytes().await.expect("body bytes");
    assert_eq!(
        status,
        200,
        "{what} succeeds: {}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(content_type, ARROW_STREAM_MEDIA_TYPE, "{what} media type");
    decode_stream(&bytes).unwrap_or_else(|error| panic!("{what} decodes: {error}"))
}

fn scan_materialize_plan(asset_id: Uuid, projection: Vec<ColumnId>) -> (LogicalPlan, PlanNodeId) {
    let scan = PlanNodeId::from_uuid(Uuid::new_v4());
    let root = PlanNodeId::from_uuid(Uuid::new_v4());
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: asset_id,
                projection,
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        root,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "svc-a1".to_owned(),
            },
            vec![scan],
        ),
    );
    (LogicalPlan::new(root, nodes).expect("plan validates"), scan)
}

async fn discover_and_project(
    client: &reqwest::Client,
    base: &str,
    workspace_id: Uuid,
    connection_id: Uuid,
) -> (Uuid, Vec<ColumnId>) {
    let discovered = post_json(
        client,
        base,
        "/v1/assets/discover",
        envelope(
            workspace_id,
            json!({"connectionId": connection_id, "parentPath": null, "timeoutSeconds": null}),
        ),
    )
    .await;
    assert_eq!(discovered.status(), 200, "asset discover");
    let body: Value = discovered.json().await.expect("discover json");
    let asset_id: Uuid = body["body"][0]["id"]
        .as_str()
        .expect("asset id")
        .parse()
        .expect("asset uuid");
    let inspected = post_json(
        client,
        base,
        "/v1/assets/inspect",
        envelope(
            workspace_id,
            json!({"connectionId": connection_id, "assetId": asset_id, "timeoutSeconds": null}),
        ),
    )
    .await;
    assert_eq!(inspected.status(), 200, "asset inspect");
    let body: Value = inspected.json().await.expect("inspect json");
    let projection = body["body"]["schema"]["fields"]
        .as_array()
        .expect("schema fields")
        .iter()
        .map(|field| serde_json::from_value::<ColumnId>(field["id"].clone()).expect("column id"))
        .collect::<Vec<_>>();
    assert!(!projection.is_empty(), "inspected schema carries fields");
    (asset_id, projection)
}

fn node_graph_json(asset_id: Uuid, projection: &[ColumnId], branch: bool) -> Value {
    let source = Uuid::from_u128(0x101);
    let transform = Uuid::from_u128(0x102);
    let output = Uuid::from_u128(0x103);
    let branch_node = Uuid::from_u128(0x104);
    let mut nodes = vec![
        json!({
            "id": source,
            "typeId": "stillflow.node.source",
            "configVersion": 1,
            "config": {"sourceAssetId": asset_id, "projection": projection},
        }),
        json!({
            "id": transform,
            "typeId": "stillflow.node.trim",
            "configVersion": 1,
            "config": {"column": projection[1]},
        }),
        json!({
            "id": output,
            "typeId": "stillflow.node.output",
            "configVersion": 1,
            "config": {"outputLabel": "ng-a1"},
        }),
    ];
    let mut edges = vec![
        json!({"from": {"nodeId": source, "port": "out"}, "to": {"nodeId": transform, "port": "in"}}),
        json!({"from": {"nodeId": transform, "port": "out"}, "to": {"nodeId": output, "port": "in"}}),
    ];
    if branch {
        nodes.insert(
            2,
            json!({
                "id": branch_node,
                "typeId": "stillflow.node.select",
                "configVersion": 1,
                "config": {"columns": [projection[0]]},
            }),
        );
        edges.push(
            json!({"from": {"nodeId": source, "port": "out"}, "to": {"nodeId": branch_node, "port": "in"}}),
        );
    }
    json!({
        "version": 1,
        "graphId": Uuid::from_u128(0x110),
        "sourceNodeId": source,
        "outputNodeId": output,
        "nodes": nodes,
        "edges": edges,
    })
}

fn materialize_op(workspace_id: Uuid, connection_id: Uuid, asset_id: Uuid) -> JobOperation {
    JobOperation::try_new(
        OperationKind::Materialize,
        OperationDescriptorV1::Materialize {
            source_asset: SourceAssetRef {
                workspace_id,
                source_connection_id: connection_id,
                source_asset_id: asset_id,
                version_digest: [7; 32],
            },
            materialize_policy: MaterializePolicyV1 { batch_size: 1024 },
        },
    )
    .expect("materialize operation validates")
}

struct MaterializePlanFixture {
    workspace_id: Uuid,
    session_id: Uuid,
    plan_id: Uuid,
    version_id: Uuid,
    connection_id: Uuid,
    asset_id: Uuid,
}

async fn submit_materialize_job(
    client: &reqwest::Client,
    base: &str,
    fixture: &MaterializePlanFixture,
) -> Uuid {
    let MaterializePlanFixture {
        workspace_id,
        session_id,
        plan_id,
        version_id,
        connection_id,
        asset_id,
    } = *fixture;
    let job_id = Uuid::new_v4();
    let operation = materialize_op(workspace_id, connection_id, asset_id);
    let body = json!({
        "sessionId": session_id,
        "planVersionId": version_id,
        "planId": plan_id,
        "jobId": job_id,
        "operation": serde_json::to_value(&operation).expect("operation"),
        "inputs": [serde_json::to_value(operation.input()).expect("inputs")],
        "executionPolicy": {"deadlineSeconds": 300},
        "outputPolicy": {},
        "queuedAt": timestamp(),
        "eventId": Uuid::new_v4(),
        "correlationId": "svc-a1-loop",
        "actorRef": "actor:svc-a1",
    });
    let response = post_json(
        client,
        base,
        "/v1/jobs",
        envelope_with_key(workspace_id, body),
    )
    .await;
    let status = response.status();
    let text = response.text().await.expect("body");
    assert_eq!(status, 200, "job submit over TCP: {status} {text}");
    job_id
}

async fn wait_terminal(
    client: &reqwest::Client,
    base: &str,
    _workspace_id: Uuid,
    job_id: Uuid,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let response = get_json(
            client,
            base,
            &format!("/v1/jobs/{job_id}?workspaceId={_workspace_id}"),
        )
        .await;
        assert_eq!(response.status(), 200, "job read over TCP");
        let body: Value = response.json().await.expect("job json");
        let state = body["body"]["state"].as_str().expect("state").to_owned();
        if matches!(state.as_str(), "succeeded" | "failed" | "cancelled") {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "job {job_id} never reached terminal state"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t1_handshake_negotiates_over_real_tcp() {
    let root = tempfile::tempdir().expect("root");
    let (service, base, client) = start(process_config(root.path())).await;
    let response = post_json(
        &client,
        &base,
        "/v1/handshake",
        envelope(service.workspace_id, json!({"requestedVersion": 1})),
    )
    .await;
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("handshake json");
    assert_eq!(body["body"]["selectedVersion"], 1);
    assert_eq!(
        body["body"]["manifest"]["apiVersion"],
        BOOTSTRAP_MANIFEST.api_version
    );

    let rejected = post_json(
        &client,
        &base,
        "/v1/handshake",
        envelope(service.workspace_id, json!({"requestedVersion": 99})),
    )
    .await;
    assert_eq!(rejected.status(), 400, "unknown version fails closed");
    let body: Value = rejected.json().await.expect("error json");
    assert_eq!(body["error"]["code"], "unsupportedVersion");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_ng_a1_catalog_compile_preview_and_scope_fail_closed() {
    let root = tempfile::tempdir().expect("root");
    let fixture = tempfile::tempdir().expect("fixtures");
    std::fs::write(
        fixture.path().join("rows.csv"),
        b"id,label,ignored\n1,alpha,x\n2,beta,y\n",
    )
    .expect("csv fixture");
    let (service, base, client) = start(process_config(root.path())).await;
    let workspace_id = service.workspace_id;

    let catalog = get_json(
        &client,
        &base,
        &format!("/v1/node-types?workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(catalog.status(), 200, "node catalog endpoint");
    let catalog_body: Value = catalog.json().await.expect("catalog json");
    assert_eq!(
        catalog_body["body"]["compilerVersion"],
        "ng-nodegraph-compiler-v1"
    );
    assert_eq!(catalog_body["body"]["nodes"].as_array().unwrap().len(), 11);

    let connection_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/connections",
        envelope(
            workspace_id,
            json!({
                "connectionId": connection_id,
                "kind": "localFile",
                "name": "ng-a1-csv",
                "safeConfig": {
                    "allowedRoots": [fixture.path().to_str().expect("utf-8")],
                    "schemaInference": {"maxRows": 100, "maxBytes": 1048576}
                },
                "credentialRef": "cred://ng-a1/local",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "connection register");
    let (asset_id, projection) =
        discover_and_project(&client, &base, workspace_id, connection_id).await;
    let graph = node_graph_json(asset_id, &projection, false);

    let compiled = post_json(
        &client,
        &base,
        "/v1/node-graphs/compile",
        envelope(
            workspace_id,
            json!({
                "graph": graph,
                "connectionId": connection_id,
                "assetId": asset_id,
                "target": "execution",
                "timeoutSeconds": 30,
            }),
        ),
    )
    .await;
    let compiled_status = compiled.status();
    let compiled_text = compiled.text().await.expect("compile body");
    assert_eq!(compiled_status, 200, "node graph compile: {compiled_text}");
    let compiled_body: Value = serde_json::from_str(&compiled_text).expect("compile json");
    assert_eq!(
        compiled_body["body"]["outputSchema"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        compiled_body["body"]["nodePlanIds"]
            .as_object()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        compiled_body["body"]["canonicalPlanDigest"]
            .as_str()
            .unwrap()
            .len(),
        64
    );

    let preview = post_json(
        &client,
        &base,
        "/v1/node-graphs/preview",
        envelope(
            workspace_id,
            json!({
                "graph": graph,
                "connectionId": connection_id,
                "assetId": asset_id,
                "targetNodeId": Uuid::from_u128(0x102),
                "batchSize": 1024,
                "rowLimit": 100,
                "byteLimit": 1048576,
                "timeoutSeconds": 30,
            }),
        ),
    )
    .await;
    let decoded = arrow_stream(preview, "node graph preview").await;
    assert!(matches!(
        decoded.metadata.view,
        WireView::EnginePreview { .. }
    ));
    assert!(!decoded.batches.is_empty(), "graph preview returns rows");
    let jobs = get_json(
        &client,
        &base,
        &format!("/v1/jobs?limit=10&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(jobs.status(), 200, "preview job list");
    let jobs_body: Value = jobs.json().await.expect("jobs json");
    assert_eq!(jobs_body["body"]["jobs"].as_array().unwrap().len(), 0);

    let foreign = post_json(
        &client,
        &base,
        "/v1/node-graphs/compile",
        envelope(
            Uuid::new_v4(),
            json!({
                "graph": graph,
                "connectionId": connection_id,
                "assetId": asset_id,
                "target": "execution",
                "timeoutSeconds": 30,
            }),
        ),
    )
    .await;
    assert_eq!(foreign.status(), 404, "foreign workspace is hidden");

    let rejected = post_json(
        &client,
        &base,
        "/v1/node-graphs/compile",
        envelope(
            workspace_id,
            json!({
                "graph": node_graph_json(asset_id, &projection, true),
                "connectionId": connection_id,
                "assetId": asset_id,
                "target": "execution",
                "timeoutSeconds": 30,
            }),
        ),
    )
    .await;
    assert_eq!(rejected.status(), 400, "branch graph rejected");
    let rejected_body: Value = rejected.json().await.expect("rejection json");
    assert_eq!(rejected_body["error"]["code"], "invalidRequest");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_ng_g1_compile_preview_plan_version_restart_and_snapshot() {
    let root = tempfile::tempdir().expect("root");
    let fixture = tempfile::tempdir().expect("fixtures");
    std::fs::write(
        fixture.path().join("rows.csv"),
        b"id,label,ignored\n1, alpha ,x\n2,beta,y\n",
    )
    .expect("csv fixture");
    let config = process_config(root.path());
    let workspace_id = config.workspace_id;
    let (service, base, client) = start(config.clone()).await;

    let session_id = Uuid::new_v4();
    let session = post_json(
        &client,
        &base,
        "/v1/sessions",
        envelope(
            workspace_id,
            json!({"sessionId": session_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    assert_eq!(session.status(), 200, "session create");

    let connection_id = Uuid::new_v4();
    let connection = post_json(
        &client,
        &base,
        "/v1/connections",
        envelope(
            workspace_id,
            json!({
                "connectionId": connection_id,
                "kind": "localFile",
                "name": "ng-g1-csv",
                "safeConfig": {
                    "allowedRoots": [fixture.path().to_str().expect("utf-8")],
                    "schemaInference": {"maxRows": 100, "maxBytes": 1048576}
                },
                "credentialRef": "cred://ng-g1/local",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(connection.status(), 200, "connection register");
    let (asset_id, projection) =
        discover_and_project(&client, &base, workspace_id, connection_id).await;
    let graph = node_graph_json(asset_id, &projection, false);

    let compile_request = || {
        post_json(
            &client,
            &base,
            "/v1/node-graphs/compile",
            envelope(
                workspace_id,
                json!({
                    "graph": graph,
                    "connectionId": connection_id,
                    "assetId": asset_id,
                    "target": "execution",
                    "timeoutSeconds": 30,
                }),
            ),
        )
    };
    let first_compile = compile_request().await;
    assert_eq!(first_compile.status(), 200, "first authoritative compile");
    let first_body: Value = first_compile.json().await.expect("first compile json");
    let second_compile = compile_request().await;
    assert_eq!(
        second_compile.status(),
        200,
        "repeated authoritative compile"
    );
    let second_body: Value = second_compile.json().await.expect("second compile json");
    assert_eq!(
        first_body["body"]["canonicalPlanDigest"], second_body["body"]["canonicalPlanDigest"],
        "repeated compile keeps the canonical digest"
    );
    assert_eq!(
        first_body["body"]["planFingerprint"], second_body["body"]["planFingerprint"],
        "repeated compile keeps the plan fingerprint"
    );
    assert_eq!(
        first_body["body"]["nodePlanIds"].as_object().unwrap().len(),
        3,
        "source, trim, and output are mapped"
    );

    let preview = post_json(
        &client,
        &base,
        "/v1/node-graphs/preview",
        envelope(
            workspace_id,
            json!({
                "graph": graph,
                "connectionId": connection_id,
                "assetId": asset_id,
                "targetNodeId": Uuid::from_u128(0x102),
                "batchSize": 1024,
                "rowLimit": 100,
                "byteLimit": 1048576,
                "timeoutSeconds": 30,
            }),
        ),
    )
    .await;
    let decoded = arrow_stream(preview, "NG-G1 node graph preview").await;
    assert!(matches!(
        decoded.metadata.view,
        WireView::EnginePreview { .. }
    ));
    assert!(!decoded.batches.is_empty(), "preview has typed rows");
    let jobs = get_json(
        &client,
        &base,
        &format!("/v1/jobs?limit=10&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(jobs.status(), 200, "preview job list");
    let jobs_body: Value = jobs.json().await.expect("preview jobs json");
    assert_eq!(jobs_body["body"]["jobs"].as_array().unwrap().len(), 0);

    // The durable execution contract binds a discovered source asset to a
    // Dataset before JobRuntime materializes a Snapshot. Preview itself does
    // not create this durable object; the E2E gate creates the same binding a
    // real publish flow would persist.
    let dataset_id = Uuid::new_v4();
    let dataset = post_json(
        &client,
        &base,
        "/v1/datasets",
        envelope(
            workspace_id,
            json!({
                "datasetId": dataset_id,
                "sessionId": session_id,
                "sourceAssetId": asset_id,
                "name": "ng-g1",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(dataset.status(), 200, "dataset binding persists");

    let plan_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    let create_plan = post_json(
        &client,
        &base,
        "/v1/plans",
        envelope(
            workspace_id,
            json!({"planId": plan_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    assert_eq!(create_plan.status(), 200, "plan create");
    let save_version = post_json(
        &client,
        &base,
        &format!("/v1/plans/{plan_id}/versions"),
        envelope(
            workspace_id,
            json!({
                "planId": plan_id,
                "planVersionId": version_id,
                "versionNumber": 1,
                "parentVersionId": null,
                "logicalPlan": first_body["body"]["logicalPlan"],
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(save_version.status(), 200, "compiled plan version save");
    let saved_body: Value = save_version.json().await.expect("saved version json");
    assert_eq!(
        saved_body["body"]["canonicalPlanDigest"], first_body["body"]["canonicalPlanDigest"],
        "PlanVersion keeps compiler canonical digest"
    );
    assert_eq!(
        saved_body["body"]["planFingerprint"], first_body["body"]["planFingerprint"],
        "PlanVersion keeps compiler fingerprint"
    );
    let publish = post_json(
        &client,
        &base,
        &format!("/v1/plan-versions/{version_id}/publish"),
        envelope(
            workspace_id,
            json!({
                "planVersionId": version_id,
                "expectedCurrentVersionId": null,
                "publishedAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(publish.status(), 200, "compiled plan version publish");
    service
        .shutdown()
        .await
        .expect("shutdown after durable publish");

    // Restart before submission: JobRuntime must resolve only the durable
    // PlanVersion and source state; no NodeGraph or compiler process state is
    // carried across the restart.
    let (restarted, restarted_base, restarted_client) = start(config).await;
    let job_id = submit_materialize_job(
        &restarted_client,
        &restarted_base,
        &MaterializePlanFixture {
            workspace_id,
            session_id,
            plan_id,
            version_id,
            connection_id,
            asset_id,
        },
    )
    .await;
    let job = wait_terminal(&restarted_client, &restarted_base, workspace_id, job_id).await;
    assert_eq!(
        job["body"]["state"], "succeeded",
        "durable job succeeds: {job}"
    );
    let outputs = job["body"]["outputs"].as_array().expect("job outputs");
    assert_eq!(outputs.len(), 1, "materialize creates one output");
    assert_eq!(outputs[0]["kind"], "snapshot");
    assert_eq!(outputs[0]["committed"], true);
    assert!(outputs[0]["snapshot_id"].as_str().is_some());
    restarted.shutdown().await.expect("restart shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t2_client_loop_materializes_over_real_tcp() {
    let root = tempfile::tempdir().expect("root");
    let fixture = tempfile::tempdir().expect("fixtures");
    std::fs::write(
        fixture.path().join("rows.csv"),
        b"id,label,ignored\n1,alpha,x\n2,beta,y\n",
    )
    .expect("csv fixture");
    let (service, base, client) = start(process_config(root.path())).await;
    let workspace_id = service.workspace_id;

    // The process bootstraps its configured workspace; a second client-side
    // workspace.create still works over TCP (envelope scoped to the target).
    let second_workspace = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/workspaces",
        envelope(
            second_workspace,
            json!({"workspaceId": second_workspace, "createdAt": timestamp()}),
        ),
    )
    .await;
    let status = response.status();
    let text = response.text().await.expect("body");
    assert!(status == 200, "workspace create: {status} {text}");

    let session_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/sessions",
        envelope(
            workspace_id,
            json!({"sessionId": session_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "session create");

    let connection_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/connections",
        envelope(
            workspace_id,
            json!({
                "connectionId": connection_id,
                "kind": "localFile",
                "name": "svc-a1-csv",
                "safeConfig": {
                    "allowedRoots": [fixture.path().to_str().expect("utf-8")],
                    "schemaInference": {"maxRows": 100, "maxBytes": 1048576}
                },
                "credentialRef": "cred://svc-a1/local",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "connection register");

    let (asset_id, projection) =
        discover_and_project(&client, &base, workspace_id, connection_id).await;

    // Typed-binary connector preview over TCP (contract §6.1).
    let decoded = arrow_stream(
        post_json(
            &client,
            &base,
            "/v1/assets/preview",
            envelope(
                workspace_id,
                json!({
                    "connectionId": connection_id,
                    "assetId": asset_id,
                    "rowLimit": 100,
                    "byteLimit": 1048576,
                    "timeoutSeconds": null,
                }),
            ),
        )
        .await,
        "asset preview",
    )
    .await;
    match decoded.metadata.view {
        WireView::AssetPreview { rows_returned, .. } => {
            assert!(rows_returned > 0, "preview returns rows")
        }
        other => panic!("asset.preview returns the assetPreview view, got {other:?}"),
    }
    let envelope_meta = decoded
        .metadata
        .envelope
        .as_ref()
        .expect("preview envelope");
    assert_eq!(envelope_meta.source_asset_id, asset_id);
    assert!(
        !decoded.batches.is_empty(),
        "preview batches ride the stream"
    );

    let dataset_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/datasets",
        envelope(
            workspace_id,
            json!({
                "datasetId": dataset_id,
                "sessionId": session_id,
                "sourceAssetId": asset_id,
                "name": "svc-a1",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "dataset create");

    let plan_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/plans",
        envelope(
            workspace_id,
            json!({"planId": plan_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "plan create");

    let (plan, scan_id) = scan_materialize_plan(asset_id, projection);
    let plan_value = serde_json::to_value(&plan).expect("plan json");
    let response = post_json(
        &client,
        &base,
        &format!("/v1/plans/{plan_id}/versions"),
        envelope(
            workspace_id,
            json!({
                "planId": plan_id,
                "planVersionId": version_id,
                "versionNumber": 1,
                "parentVersionId": null,
                "logicalPlan": plan_value.clone(),
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "plan version save");

    let response = post_json(
        &client,
        &base,
        &format!("/v1/plan-versions/{version_id}/publish"),
        envelope(
            workspace_id,
            json!({"planVersionId": version_id, "expectedCurrentVersionId": null, "publishedAt": timestamp()}),
        ),
    )
    .await;
    assert_eq!(response.status(), 200, "plan version publish");

    // Typed-binary engine preview over TCP (contract §6.1): the published
    // plan targets the Scan node without creating a Job or Run.
    let decoded = arrow_stream(
        post_json(
            &client,
            &base,
            "/v1/engine/preview",
            envelope(
                workspace_id,
                json!({
                    "plan": plan_value,
                    "targetNodeId": json!(scan_id),
                    "connectionId": connection_id,
                    "assetId": asset_id,
                    "batchSize": 1024,
                    "rowLimit": 100,
                    "byteLimit": 1048576,
                    "timeoutSeconds": 30,
                }),
            ),
        )
        .await,
        "engine preview",
    )
    .await;
    match decoded.metadata.view {
        WireView::EnginePreview { .. } => {}
        other => panic!("engine.preview returns the enginePreview view, got {other:?}"),
    }

    let job_id = submit_materialize_job(
        &client,
        &base,
        &MaterializePlanFixture {
            workspace_id,
            session_id,
            plan_id,
            version_id,
            connection_id,
            asset_id,
        },
    )
    .await;
    let job = wait_terminal(&client, &base, workspace_id, job_id).await;
    assert_eq!(
        job["body"]["state"], "succeeded",
        "materialize job succeeds: {job}"
    );

    // Query paths over TCP: job list and run list.
    let jobs = get_json(
        &client,
        &base,
        &format!("/v1/jobs?limit=10&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(jobs.status(), 200, "job list");
    let runs = get_json(
        &client,
        &base,
        &format!("/v1/runs?limit=10&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(runs.status(), 200, "run list");
    let runs_body: Value = runs.json().await.expect("runs json");
    assert_eq!(
        runs_body["body"]["runs"][0]["jobId"],
        json!(job_id),
        "run belongs to the submitted job"
    );

    // The materialize product is exactly one committed Snapshot output
    // reference, read over TCP.
    let outputs = job["body"]["outputs"].as_array().expect("outputs");
    assert_eq!(outputs.len(), 1, "materialize publishes exactly one output");
    assert_eq!(outputs[0]["kind"], "snapshot", "output kind");
    assert_eq!(outputs[0]["committed"], true, "snapshot committed");
    assert!(
        outputs[0]["snapshot_id"].as_str().is_some(),
        "snapshot id present"
    );
    assert!(
        outputs[0]["version_digest"].as_str().is_some(),
        "snapshot version digest present"
    );

    // Verification over the committed snapshot, then the typed-binary
    // artifact content route over TCP (contract §6.1 + §6 T2).
    let snapshot_ref: SnapshotRef = serde_json::from_value(json!({
        "workspaceId": outputs[0]["workspace_id"],
        "sessionId": outputs[0]["session_id"],
        "datasetId": outputs[0]["dataset_id"],
        "snapshotId": outputs[0]["snapshot_id"],
        "versionDigest": outputs[0]["version_digest"],
        "schemaFingerprint": outputs[0]["schema_fingerprint"],
        "snapshotVersion": outputs[0]["snapshot_version"],
    }))
    .expect("snapshot ref from output view");
    let verification = JobOperation::try_new(
        OperationKind::Verification,
        OperationDescriptorV1::Verification {
            snapshot: snapshot_ref,
            verification_policy: VerificationPolicyV1 {
                batch_size: 1024,
                publish_rejected_rows: true,
            },
        },
    )
    .expect("verification operation validates");
    let verification_job_id = Uuid::new_v4();
    let response = post_json(
        &client,
        &base,
        "/v1/jobs",
        envelope_with_key(
            workspace_id,
            json!({
                "sessionId": session_id,
                "planVersionId": version_id,
                "planId": plan_id,
                "jobId": verification_job_id,
                "operation": serde_json::to_value(&verification).expect("operation json"),
                "inputs": [serde_json::to_value(verification.input()).expect("inputs")],
                "executionPolicy": {"deadlineSeconds": 300},
                "outputPolicy": {},
                "queuedAt": timestamp(),
                "eventId": Uuid::new_v4(),
                "correlationId": "svc-a1-verification",
                "actorRef": "actor:svc-a1",
            }),
        ),
    )
    .await;
    let status = response.status();
    let text = response.text().await.expect("body");
    assert_eq!(status, 200, "verification submit: {status} {text}");
    let verification_job = wait_terminal(&client, &base, workspace_id, verification_job_id).await;
    assert_eq!(
        verification_job["body"]["state"], "succeeded",
        "verification job succeeds: {verification_job}"
    );
    let verification_outputs = verification_job["body"]["outputs"]
        .as_array()
        .expect("verification outputs");
    let bundle_output = verification_outputs
        .iter()
        .find(|output| output["kind"] == "verificationBundle")
        .expect("verification bundle output");
    let report_member = bundle_output["members"]
        .as_array()
        .expect("bundle members")
        .iter()
        .find(|member| member["artifactKind"] == "validationReport")
        .expect("validation report member");
    let bundle_id = bundle_output["bundle_id"].as_str().expect("bundle id");
    let report_artifact_id = report_member["artifactId"].as_str().expect("artifact id");
    // SVC-A2 (issue #314): verification bundle report artifacts now carry
    // committed control-plane ArtifactRefs (contract §3/§4), so every bundle
    // member is listed and the §6.1 Arrow stream content route is reachable
    // for the only artifacts that own sections. Unknown artifact ids still
    // fail closed with the §3.2 JSON mapping (`NotFound` → 404).
    let run_id = verification_job["body"]["runId"].as_str().expect("run id");
    let listed = get_json(
        &client,
        &base,
        &format!("/v1/runs/{run_id}/artifacts?limit=50&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(listed.status(), 200, "artifact list succeeds");
    let listed: Value = listed.json().await.expect("artifact list json");
    let listed_artifacts = listed["body"]["artifacts"]
        .as_array()
        .expect("artifact page view");
    for member in bundle_output["members"].as_array().expect("bundle members") {
        let member_id = member["artifactId"].as_str().expect("member artifact id");
        let row = listed_artifacts
            .iter()
            .find(|artifact| artifact["artifactId"] == member["artifactId"])
            .unwrap_or_else(|| panic!("bundle member {member_id} is listed"));
        assert_eq!(
            row["artifactKind"], member["artifactKind"],
            "listed kind matches the bundle member"
        );
        assert_eq!(row["state"], "committed", "listed ref is committed");
    }
    let decoded = arrow_stream(
        get_json(
            &client,
            &base,
            &format!(
                "/v1/artifacts/content?bundleId={bundle_id}&artifactId={report_artifact_id}&sectionId=validation-rule-summary&maxRows=1000&maxBytes=1048576&workspaceId={workspace_id}"
            ),
        )
        .await,
        "artifact content",
    )
    .await;
    assert!(
        matches!(
            decoded.metadata.view,
            WireView::ArtifactContent {
                next_partition_sequence: None
            }
        ),
        "artifact content view shape"
    );
    let unknown_content = get_json(
        &client,
        &base,
        &format!(
            "/v1/artifacts/content?bundleId={bundle_id}&artifactId={}&sectionId=validation-rule-summary&maxRows=1000&maxBytes=1048576&workspaceId={workspace_id}",
            Uuid::new_v4()
        ),
    )
    .await;
    let status = unknown_content.status();
    let text = unknown_content.text().await.expect("body");
    assert_eq!(
        status, 404,
        "unknown artifact fails closed: {status} {text}"
    );
    let body: Value = serde_json::from_str(&text).expect("error json");
    assert_eq!(body["error"]["code"], "notFound", "§3.2 mapping holds");

    service.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_cancel_over_real_tcp() {
    let root = tempfile::tempdir().expect("root");
    let fixture = tempfile::tempdir().expect("fixtures");
    std::fs::write(
        fixture.path().join("rows.csv"),
        b"id,label\n1,alpha\n2,beta\n",
    )
    .expect("csv fixture");
    let (service, base, client) = start(process_config(root.path())).await;
    let workspace_id = service.workspace_id;
    let session_id = Uuid::new_v4();
    let connection_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/sessions",
        envelope(
            workspace_id,
            json!({"sessionId": session_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    post_json(
        &client,
        &base,
        "/v1/connections",
        envelope(
            workspace_id,
            json!({
                "connectionId": connection_id,
                "kind": "localFile",
                "name": "svc-a1-cancel",
                "safeConfig": {"allowedRoots": [fixture.path().to_str().expect("utf-8")], "schemaInference": {"maxRows": 100, "maxBytes": 1048576}},
                "credentialRef": "cred://svc-a1/local",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    let (asset_id, projection) =
        discover_and_project(&client, &base, workspace_id, connection_id).await;
    let dataset_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/datasets",
        envelope(workspace_id, json!({"datasetId": dataset_id, "sessionId": session_id, "sourceAssetId": asset_id, "name": "svc-a1-cancel", "createdAt": timestamp()})),
    )
    .await;
    let plan_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/plans",
        envelope(
            workspace_id,
            json!({"planId": plan_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    let (plan, _scan_id) = scan_materialize_plan(asset_id, projection);
    post_json(
        &client,
        &base,
        &format!("/v1/plans/{plan_id}/versions"),
        envelope(workspace_id, json!({"planId": plan_id, "planVersionId": version_id, "versionNumber": 1, "parentVersionId": null, "logicalPlan": serde_json::to_value(&plan).expect("plan json"), "createdAt": timestamp()})),
    )
    .await;
    post_json(
        &client,
        &base,
        &format!("/v1/plan-versions/{version_id}/publish"),
        envelope(workspace_id, json!({"planVersionId": version_id, "expectedCurrentVersionId": null, "publishedAt": timestamp()})),
    )
    .await;
    let job_id = submit_materialize_job(
        &client,
        &base,
        &MaterializePlanFixture {
            workspace_id,
            session_id,
            plan_id,
            version_id,
            connection_id,
            asset_id,
        },
    )
    .await;

    // The tiny fixture may finish before the cancel lands, so the cancel
    // response is either 200 (queued/running) or 409 conflict (terminal
    // already); either way the job must end terminal. The deterministic
    // cancel-race remains covered by the E5-G1 library gate.
    let cancel = post_json(
        &client,
        &base,
        &format!("/v1/jobs/{job_id}/cancel"),
        envelope(workspace_id, json!({"jobId": job_id})),
    )
    .await;
    let status = cancel.status();
    assert!(
        status == 200 || status == 409,
        "cancel responds with JobView or conflict, got {status}"
    );
    let job = wait_terminal(&client, &base, workspace_id, job_id).await;
    assert_ne!(job["body"]["state"], "running");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t4_events_list_cursor_and_sse_over_real_tcp() {
    let root = tempfile::tempdir().expect("root");
    let fixture = tempfile::tempdir().expect("fixtures");
    std::fs::write(
        fixture.path().join("rows.csv"),
        b"id,label\n1,alpha\n2,beta\n",
    )
    .expect("csv fixture");
    let (service, base, client) = start(process_config(root.path())).await;
    let workspace_id = service.workspace_id;
    let session_id = Uuid::new_v4();
    let connection_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/sessions",
        envelope(
            workspace_id,
            json!({"sessionId": session_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    post_json(
        &client,
        &base,
        "/v1/connections",
        envelope(
            workspace_id,
            json!({
                "connectionId": connection_id,
                "kind": "localFile",
                "name": "svc-a1-events",
                "safeConfig": {"allowedRoots": [fixture.path().to_str().expect("utf-8")], "schemaInference": {"maxRows": 100, "maxBytes": 1048576}},
                "credentialRef": "cred://svc-a1/local",
                "createdAt": timestamp(),
            }),
        ),
    )
    .await;
    let (asset_id, projection) =
        discover_and_project(&client, &base, workspace_id, connection_id).await;
    let dataset_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/datasets",
        envelope(workspace_id, json!({"datasetId": dataset_id, "sessionId": session_id, "sourceAssetId": asset_id, "name": "svc-a1-events", "createdAt": timestamp()})),
    )
    .await;
    let plan_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    post_json(
        &client,
        &base,
        "/v1/plans",
        envelope(
            workspace_id,
            json!({"planId": plan_id, "createdAt": timestamp()}),
        ),
    )
    .await;
    let (plan, _scan_id) = scan_materialize_plan(asset_id, projection);
    post_json(
        &client,
        &base,
        &format!("/v1/plans/{plan_id}/versions"),
        envelope(workspace_id, json!({"planId": plan_id, "planVersionId": version_id, "versionNumber": 1, "parentVersionId": null, "logicalPlan": serde_json::to_value(&plan).expect("plan json"), "createdAt": timestamp()})),
    )
    .await;
    post_json(
        &client,
        &base,
        &format!("/v1/plan-versions/{version_id}/publish"),
        envelope(workspace_id, json!({"planVersionId": version_id, "expectedCurrentVersionId": null, "publishedAt": timestamp()})),
    )
    .await;
    let job_id = submit_materialize_job(
        &client,
        &base,
        &MaterializePlanFixture {
            workspace_id,
            session_id,
            plan_id,
            version_id,
            connection_id,
            asset_id,
        },
    )
    .await;
    wait_terminal(&client, &base, workspace_id, job_id).await;

    // Durable event list over TCP.
    let events = get_json(
        &client,
        &base,
        &format!("/v1/events?streamKind=job&streamId={job_id}&limit=50&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(events.status(), 200, "event list");
    let events_body: Value = events.json().await.expect("events json");
    let first_events = events_body["body"]["events"]
        .as_array()
        .expect("events array");
    assert!(!first_events.is_empty(), "job events exist");
    let first_sequence = first_events[0]["sequence"].as_u64().expect("sequence");

    // Cursor resume: everything after the first event's sequence.
    let resumed = get_json(
        &client,
        &base,
        &format!("/v1/events?streamKind=job&streamId={job_id}&limit=50&cursor={first_sequence}&workspaceId={workspace_id}"),
    )
    .await;
    assert_eq!(resumed.status(), 200, "event list with cursor");
    let resumed_body: Value = resumed.json().await.expect("resumed json");
    for event in resumed_body["body"]["events"].as_array().expect("events") {
        let sequence = event["sequence"].as_u64().expect("sequence");
        assert!(sequence > first_sequence, "cursor strictly advances");
    }

    // SSE extension streams at least one frame.
    let mut stream = client
        .get(format!(
            "{base}/v1/events/stream?workspaceId={workspace_id}&streamKind=job&streamId={job_id}&limit=10"
        ))
        .send()
        .await
        .expect("sse connects");
    assert_eq!(stream.status(), 200);
    let content_type = stream
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .expect("content type")
        .to_owned();
    assert!(
        content_type.starts_with("text/event-stream"),
        "sse media type"
    );
    let chunk = tokio::time::timeout(Duration::from_secs(15), stream.chunk())
        .await
        .expect("sse delivers within timeout")
        .expect("chunk reads");
    let text = String::from_utf8_lossy(&chunk.expect("chunk bytes")).into_owned();
    assert!(
        text.contains("data:"),
        "sse frame payload present: {text:?}"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t7_manifest_routes_are_registered() {
    let root = tempfile::tempdir().expect("root");
    let (service, base, client) = start(process_config(root.path())).await;
    // Contract §6 T7 at 100% manifest coverage (closing PR): every manifest
    // (method, path) is registered. A bare 404 (empty body) or a 405 proves
    // the route table diverged from the authoritative manifest; malformed
    // probes still answer through the §3.2 JSON error mapping, so a 404 that
    // carries an error body still proves registration. The route set stays a
    // subset of the manifest by construction (routes.rs maps only
    // E5_A1_ROUTES entries).
    for route in E5_A1_ROUTES {
        let mut path = route.path.to_owned();
        while let Some(start) = path.find('{') {
            let Some(end) = path[start..].find('}') else {
                break;
            };
            path.replace_range(start..start + end + 1, &Uuid::nil().to_string());
        }
        let response = if route.method == "GET" {
            get_json(&client, &base, &path).await
        } else {
            client
                .post(format!("{base}{path}"))
                .body("not-json")
                .send()
                .await
                .expect("send")
        };
        let status = response.status().as_u16();
        let body = response.text().await.expect("body");
        assert!(
            status != 404 || !body.trim().is_empty(),
            "route {}/{} ({}) is not registered: bare 404",
            route.method,
            route.path,
            route.operation_id
        );
        assert_ne!(
            status, 405,
            "route {}/{} method mismatch",
            route.method, route.path
        );
    }
    service.shutdown().await.expect("shutdown");
}

fn write_config(root: &std::path::Path, workspace_id: Uuid) -> std::path::PathBuf {
    let service = ServiceConfig {
        managed_root: root.join("managed").to_string_lossy().into_owned(),
        bind_host: "127.0.0.1".to_owned(),
        bind_port: 0,
        shutdown_grace_seconds: 5,
        ..ServiceConfig::default()
    };
    let mut config = serde_json::to_value(&service).expect("service config serializes");
    config["workspaceId"] = json!(workspace_id);
    config["authorizationMode"] = json!("local-trusted");
    let path = root.join("service.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&config).expect("config json"),
    )
    .expect("config write");
    path
}

fn spawn_server(config_path: &std::path::Path, port_file: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_stillflow-server"))
        .arg("--config")
        .arg(config_path)
        .arg("--port-file")
        .arg(port_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("server spawns")
}

fn wait_ready(port_file: &std::path::Path) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(raw) = std::fs::read_to_string(port_file) {
            if let Ok(ready) = serde_json::from_str::<Value>(&raw) {
                assert_eq!(ready["event"], "ready");
                return ready;
            }
        }
        assert!(Instant::now() < deadline, "server never announced ready");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn terminate(child: &mut Child) {
    let pid = child.id() as i32;
    let sent = unsafe { libc::kill(pid, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM delivered");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().expect("wait status") {
            Some(status) => {
                assert!(
                    status.success(),
                    "server exits 0 after SIGTERM, got {status}"
                );
                return;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "server did not exit after SIGTERM"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

#[test]
fn t5_sigterm_drains_and_exits_cleanly() {
    let root = tempfile::tempdir().expect("root");
    let workspace_id = Uuid::new_v4();
    let config = write_config(root.path(), workspace_id);
    let port_file = root.path().join("port.json");
    let mut child = spawn_server(&config, &port_file);
    let ready = wait_ready(&port_file);
    assert_eq!(ready["workspaceId"], json!(workspace_id));
    assert_eq!(ready["transport"], "desktop-local");
    terminate(&mut child);
}

#[test]
fn t6_restart_reopens_durable_state() {
    let root = tempfile::tempdir().expect("root");
    let workspace_id = Uuid::new_v4();
    let config = write_config(root.path(), workspace_id);
    let port_file = root.path().join("port.json");

    let mut first = spawn_server(&config, &port_file);
    let ready = wait_ready(&port_file);
    assert_eq!(ready["workspaceId"], json!(workspace_id));
    terminate(&mut first);

    // Second process over the same managed root: the workspace row is adopted
    // (bootstrap-create conflicts, get succeeds) and handshake works again.
    std::fs::remove_file(&port_file).expect("port file removed");
    let mut second = spawn_server(&config, &port_file);
    let ready = wait_ready(&port_file);
    assert_eq!(
        ready["workspaceId"],
        json!(workspace_id),
        "same workspace served"
    );
    let base = format!(
        "http://127.0.0.1:{}",
        ready["port"].as_u64().expect("port") as u16
    );
    let client = reqwest::blocking::Client::new();
    let handshake = client
        .post(format!("{base}/v1/handshake"))
        .json(&json!({
            "meta": {"apiVersion": 1, "requestId": Uuid::new_v4(), "workspaceId": workspace_id},
            "body": {"requestedVersion": 1}
        }))
        .send()
        .expect("handshake after restart");
    assert_eq!(handshake.status(), 200, "handshake works after restart");
    terminate(&mut second);
}
