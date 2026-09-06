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
    ColumnId, JobOperation, MaterializePolicyV1, OperationDescriptorV1, OperationKind,
    SourceAssetRef,
};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind};
use stillflow_service::{start_service, AuthModeConfig, ProcessConfig, StartedService};

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

fn scan_materialize_plan(asset_id: Uuid, projection: Vec<ColumnId>) -> LogicalPlan {
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
    LogicalPlan::new(root, nodes).expect("plan validates")
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

    let plan = scan_materialize_plan(asset_id, projection);
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
                "logicalPlan": serde_json::to_value(&plan).expect("plan json"),
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
    // reference, read over TCP. Artifact-kind metadata routes (profile /
    // report / export artifacts) are exercised by the T7 registration pass;
    // the typed-binary artifact content route awaits its wire-format stage.
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
    let plan = scan_materialize_plan(asset_id, projection);
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
    let plan = scan_materialize_plan(asset_id, projection);
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
    // PR-1 client-loop subset (contract §6 staging): these manifest operations
    // must be registered; everything else must NOT be (bare 404). The typed
    // binary wire-format trio is pending its contract stage.
    const PENDING_BINARY_WIRE_FORMAT: [&str; 3] =
        ["asset.preview", "engine.preview", "artifact.content"];
    const PR1_OPS: [&str; 48] = [
        "handshake",
        "health.liveness",
        "health.readiness",
        "health.read",
        "metrics.read",
        "workspace.create",
        "workspace.archive",
        "workspace.read",
        "session.create",
        "session.list",
        "session.read",
        "session.close",
        "connection.test",
        "connection.register",
        "connection.list",
        "connection.read",
        "asset.list",
        "asset.discover",
        "asset.inspect",
        "dataset.create",
        "dataset.read",
        "dataset.archive",
        "plan.create",
        "plan.load",
        "plan.version.save",
        "plan.version.read",
        "plan.version.publish",
        "plan.clone",
        "plan.diff",
        "plan.validate",
        "job.submit",
        "drift.compare",
        "job.read",
        "job.list",
        "job.cancel",
        "export.submit",
        "export.read",
        "export.cancel",
        "export.manifest.read",
        "export.files.list",
        "export.download",
        "export.tombstone",
        "export.gc",
        "run.read",
        "run.list",
        "event.list",
        "artifact.read",
        "artifact.list",
    ];
    for route in E5_A1_ROUTES {
        let expected_registered = PR1_OPS.contains(&route.operation_id);
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
        if expected_registered {
            assert!(
                status != 404 || !body.trim().is_empty(),
                "PR-1 route {}/{} ({}) is not registered: bare 404",
                route.method,
                route.path,
                route.operation_id
            );
            assert_ne!(
                status, 405,
                "PR-1 route {}/{} method mismatch",
                route.method, route.path
            );
        } else if PENDING_BINARY_WIRE_FORMAT.contains(&route.operation_id) {
            // These static-leaf paths may fall through to a sibling param
            // route (400) while unregistered; they must simply produce no
            // successful domain response.
            assert!(
                !(200..300).contains(&status),
                "pending route {}/{} must not serve responses yet, got {status} {body}",
                route.method,
                route.path
            );
        } else {
            // A path shape shared with a registered route (param names differ
            // only) yields 405; anything else must be a bare 404. Both prove
            // no domain handler is attached.
            assert!(
                status == 404 || status == 405,
                "route {}/{} ({}) must not be registered yet, got {status} {body}",
                route.method,
                route.path,
                route.operation_id
            );
        }
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
