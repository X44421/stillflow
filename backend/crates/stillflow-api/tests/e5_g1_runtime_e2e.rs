//! E5-G1 runtime end-to-end gate (Issue #232).
//!
//! Deterministic integration coverage for the typed multi-operation JobRuntime
//! bridge delivered by E5-J2 (#235): Connector → API → Plan/Preview →
//! JobRuntime → Engine Verification/Profile/Export → durable Event Stream →
//! bounded Artifact reads → fresh restart.
//!
//! The gate exercises the real stack only: real Phase-1 connectors, the real
//! [`ApiService`], one real [`JobRuntime`], and real durable Storage. No
//! scripted connector, no second state machine, no SQL/DuckDB/SEC/AUD/AUT/OPS.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use stillflow_api::{
    ApiRequest, ApiService, CancelJobRequest, CollectExportGarbageRequest, CreateDatasetRequest,
    CreatePlanRequest, CreateSessionRequest, DiscoverAssetsRequest, ExportDownloadRequest,
    HandshakeRequest, InspectAssetRequest, ListArtifactsRequest, ListEventsRequest,
    ListExportFilesRequest, ListFindingsRequest, ListJobsRequest, ListProfileHistoryRequest,
    ListRunsRequest, ObjectIdRequest, PreviewAssetRequest, PublishPlanVersionRequest,
    RegisterSourceConnectionRequest, RequestMetadata, SavePlanVersionRequest,
    SubmitDriftComparisonRequest, SubmitExportRequest, SubmitJobRequest, TombstoneExportRequest,
};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    ArtifactKind, ArtifactRefState, ConnectorKind, ControlPlaneEventType, CredentialRef,
    DriftBaselineMode, DriftComparisonRequest, EventStreamKind, ExportDestinationV1, ExportFormat,
    ExportShape, JobOperation, JobState, MaterializePolicyV1, OperationDescriptorV1,
    ProfileColumnsV1, ProfileRequestV1, RequestContext, RunState, SourceAsset, SourceConnection,
    VerificationPolicyV1,
};
use stillflow_engine::{ExecutionEngine, JobExecutionSpec, JobResolution, JobRuntime};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind};
use stillflow_storage::{
    ControlPlaneStore, JobRecord, SnapshotStore, StorageLimits, TerminalOutputRef,
};
use uuid::Uuid;

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 3, 0, 0, 0).unwrap() + chrono::Duration::seconds(seconds)
}

fn meta(workspace_id: Uuid) -> RequestMetadata {
    RequestMetadata::new(Uuid::new_v4(), workspace_id)
}

fn write_csv(root: &std::path::Path, name: &str) {
    std::fs::write(
        root.join(name),
        b"id,label,ignored\n1,alpha,x\n2,beta,y\n3,gamma,z\n",
    )
    .expect("CSV fixture");
}

/// Resolver sealing the E5-G1 gate stack: every durable Job/Run is resolved
/// back through durable state only (PlanVersion → Scan asset → connection).
/// No process-local dispatch data crosses the boundary.
/// Optional resolve gate for the cancellation-race test: when armed, the
/// first resolution waits until the test releases it, so the job is durably
/// Running while cancellation arrives.
#[derive(Clone, Default)]
struct ResolveGate {
    armed: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct GateResolver {
    store: Arc<ControlPlaneStore>,
    gate: Option<ResolveGate>,
}

impl GateResolver {
    fn domain_connection(record: &stillflow_storage::SourceConnectionRecord) -> SourceConnection {
        serde_json::from_value(serde_json::json!({
            "id": record.id,
            "kind": record.kind,
            "name": record.name,
            "config": record.safe_config,
            "credentialRef": record.credential_ref,
            "createdAt": record.created_at,
            "updatedAt": record.updated_at,
        }))
        .expect("stored connection decodes")
    }

    fn domain_asset(record: &stillflow_storage::SourceAssetRecord) -> SourceAsset {
        let locator =
            serde_json::from_value(record.safe_locator.clone()).expect("stored locator decodes");
        SourceAsset {
            id: record.id,
            connection_id: record.connection_id,
            kind: record.kind,
            name: record.name.clone(),
            locator,
            discovered_at: record.discovered_at,
        }
    }

    fn scan_asset_id(plan: &LogicalPlan) -> Uuid {
        for node in plan.nodes.values() {
            if let PlanNodeKind::Scan {
                source_asset_id, ..
            } = node.kind
            {
                return source_asset_id;
            }
        }
        panic!("gate plans always carry a Scan node");
    }
}

impl stillflow_engine::JobRequestResolver for GateResolver {
    fn resolve(
        &self,
        job: JobRecord,
        _run: stillflow_storage::RunRecord,
        _context: RequestContext,
    ) -> JobResolution {
        let store = Arc::clone(&self.store);
        let gate = self.gate.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                if gate.armed.load(std::sync::atomic::Ordering::SeqCst) {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
            }
            let version = store
                .get_plan_version(job.plan_version_id)
                .map_err(stillflow_engine::JobRuntimeError::Storage)?;
            let plan: LogicalPlan = serde_json::from_value(version.logical_plan)
                .map_err(|_| stillflow_engine::JobRuntimeError::Invalid("gate plan decodes"))?;
            let asset_id = Self::scan_asset_id(&plan);
            let asset_record = store
                .get_source_asset(asset_id)
                .map_err(stillflow_engine::JobRuntimeError::Storage)?;
            let connection_record = store
                .get_source_connection(asset_record.connection_id)
                .map_err(stillflow_engine::JobRuntimeError::Storage)?;
            let datasets = store
                .list_datasets(job.workspace_id, 128)
                .map_err(stillflow_engine::JobRuntimeError::Storage)?;
            let dataset_id = datasets
                .iter()
                .find(|dataset| dataset.source_asset_id == asset_id)
                .map(|dataset| dataset.id)
                .ok_or(stillflow_engine::JobRuntimeError::Invalid(
                    "gate dataset is missing",
                ))?;
            let batch_size = job
                .operation
                .as_ref()
                .map(|operation| match &operation.descriptor {
                    OperationDescriptorV1::Materialize {
                        materialize_policy, ..
                    } => materialize_policy.batch_size,
                    OperationDescriptorV1::Verification {
                        verification_policy,
                        ..
                    } => verification_policy.batch_size,
                    _ => 1024,
                })
                .unwrap_or(1024);
            Ok(JobExecutionSpec {
                plan,
                connection: Self::domain_connection(&connection_record),
                asset: Self::domain_asset(&asset_record),
                schema_override: None,
                snapshot_id: Uuid::new_v4(),
                dataset_id,
                lineage: BTreeSet::new(),
                quality_score: None,
                batch_size,
                bundle_ref: None,
            })
        })
    }
}

struct GateStack {
    root: tempfile::TempDir,
    fixture_dir: tempfile::TempDir,
    store: Arc<ControlPlaneStore>,
    service: ApiService,
    runtime: Arc<JobRuntime>,
    workspace_id: Uuid,
    session_id: Uuid,
}

impl GateStack {
    async fn new(connectors: Vec<SourceConnectorRef>) -> Self {
        Self::new_gated(connectors, None).await
    }

    async fn new_gated(connectors: Vec<SourceConnectorRef>, gate: Option<ResolveGate>) -> Self {
        let root = tempfile::tempdir().expect("gate root");
        let fixture_dir = tempfile::tempdir().expect("fixture dir");
        // One managed root owns both views: ControlPlaneStore and SnapshotStore
        // are two facets of the same SQLite schema and root lock.
        let snapshot_store = Arc::new(
            SnapshotStore::open(root.path().join("store"), StorageLimits::default())
                .expect("snapshot store"),
        );
        let store = Arc::new(snapshot_store.control_plane());
        let mut engine_registry = ConnectorRegistry::new();
        let mut api_registry = ConnectorRegistry::new();
        for connector in connectors {
            engine_registry
                .register(Arc::clone(&connector))
                .expect("engine registry");
            api_registry.register(connector).expect("api registry");
        }
        let engine = Arc::new(ExecutionEngine::new(engine_registry));
        let engine_for_runtime = Arc::clone(&engine);
        let workspace_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let resolver = Arc::new(GateResolver {
            store: Arc::clone(&store),
            gate,
        });
        let runtime = Arc::new(
            JobRuntime::new_with_system_identity(
                workspace_id,
                Arc::clone(&store),
                Arc::clone(&snapshot_store),
                engine_for_runtime,
                resolver,
            )
            .expect("job runtime"),
        );
        runtime.start().await.expect("runtime starts");
        let service = ApiService::new(Arc::clone(&store))
            .with_connectors(Arc::new(api_registry))
            .with_runtime(Arc::clone(&runtime))
            .with_snapshot_store(Arc::clone(&snapshot_store));
        let stack = Self {
            root,
            fixture_dir,
            store,
            service,
            runtime,
            workspace_id,
            session_id,
        };
        stack.bootstrap().await;
        stack
    }

    async fn bootstrap(&self) {
        self.store
            .create_workspace(self.workspace_id, at(1))
            .expect("workspace");
        self.service
            .create_session(ApiRequest {
                meta: meta(self.workspace_id),
                body: CreateSessionRequest {
                    session_id: self.session_id,
                    created_at: at(2),
                },
            })
            .expect("session");
    }

    async fn local_stack() -> Self {
        Self::new(vec![Arc::new(LocalTabularConnector) as SourceConnectorRef]).await
    }

    /// Stack whose resolver blocks inside the first claim when armed, so a
    /// cancellation race hits a durably Running job deterministically.
    async fn gated_stack() -> (Self, ResolveGate) {
        let gate = ResolveGate::default();
        let stack = Self::new_gated(
            vec![Arc::new(LocalTabularConnector) as SourceConnectorRef],
            Some(gate.clone()),
        )
        .await;
        (stack, gate)
    }

    fn register_local(&self, name: &str) -> Uuid {
        let connection_id = Uuid::new_v4();
        self.service
            .register_source_connection(ApiRequest {
                meta: meta(self.workspace_id),
                body: RegisterSourceConnectionRequest {
                    connection_id,
                    kind: ConnectorKind::LocalFile,
                    name: name.to_owned(),
                    safe_config: serde_json::json!({
                        "allowedRoots": [self.fixture_dir.path().to_str().expect("UTF-8")],
                        "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
                    }),
                    credential_ref: "cred://e5-g1/local-fixtures".to_owned(),
                    created_at: at(3),
                },
            })
            .expect("register connection");
        connection_id
    }

    async fn discover_one(&self, connection_id: Uuid) -> stillflow_api::SourceAssetView {
        let assets = self
            .service
            .discover_source_assets(ApiRequest {
                meta: meta(self.workspace_id),
                body: DiscoverAssetsRequest {
                    connection_id,
                    parent_path: None,
                    timeout_seconds: None,
                },
            })
            .await
            .expect("discover")
            .body;
        assert_eq!(assets.len(), 1, "exactly one gate fixture asset");
        assets.into_iter().next().expect("asset")
    }

    fn scan_materialize_plan_for(
        asset_id: Uuid,
        projection: Vec<stillflow_core::ColumnId>,
    ) -> LogicalPlan {
        Self::scan_materialize_plan(asset_id, projection)
    }

    fn scan_materialize_plan(
        asset_id: Uuid,
        projection: Vec<stillflow_core::ColumnId>,
    ) -> LogicalPlan {
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
                    output_label: "e5-g1".to_owned(),
                },
                vec![scan],
            ),
        );
        LogicalPlan::new(root, nodes).expect("plan validates")
    }

    /// Save + publish one Scan→Materialize plan; returns (plan_id, version_id).
    fn save_plan(
        &self,
        asset_id: Uuid,
        projection: Vec<stillflow_core::ColumnId>,
        version_number: u32,
    ) -> (Uuid, Uuid) {
        let plan_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        self.service
            .create_plan(ApiRequest {
                meta: meta(self.workspace_id),
                body: CreatePlanRequest {
                    plan_id,
                    created_at: at(10),
                },
            })
            .expect("plan");
        let plan = Self::scan_materialize_plan(asset_id, projection);
        self.service
            .save_plan_version(ApiRequest {
                meta: meta(self.workspace_id),
                body: SavePlanVersionRequest {
                    plan_id,
                    plan_version_id: version_id,
                    version_number,
                    parent_version_id: None,
                    logical_plan: plan,
                    created_at: at(11),
                },
            })
            .expect("plan version");
        self.service
            .publish_plan_version(ApiRequest {
                meta: meta(self.workspace_id),
                body: PublishPlanVersionRequest {
                    plan_version_id: version_id,
                    expected_current_version_id: None,
                    published_at: at(12),
                },
            })
            .expect("publish");
        (plan_id, version_id)
    }

    fn create_dataset(&self, asset_id: Uuid) -> Uuid {
        let dataset_id = Uuid::new_v4();
        self.service
            .create_dataset(ApiRequest {
                meta: meta(self.workspace_id),
                body: CreateDatasetRequest {
                    dataset_id,
                    session_id: self.session_id,
                    source_asset_id: asset_id,
                    name: "e5-g1".to_owned(),
                    created_at: at(13),
                },
            })
            .expect("dataset");
        dataset_id
    }

    /// Submit one typed operation job through the API boundary and wait for
    /// the durable terminal state. Returns the terminal JobRecord.
    async fn submit_and_wait(
        &self,
        operation: JobOperation,
        plan_id: Uuid,
        version_id: Uuid,
        key: &str,
    ) -> JobRecord {
        let job_id = Uuid::new_v4();
        let submitted = self
            .service
            .submit_job(ApiRequest {
                meta: meta_with_key(self.workspace_id),
                body: SubmitJobRequest {
                    session_id: self.session_id,
                    plan_version_id: version_id,
                    plan_id: Some(plan_id),
                    job_id,
                    operation: Some(operation.clone()),
                    inputs: vec![operation.input()],
                    execution_policy: serde_json::json!({"deadlineSeconds": 300}),
                    output_policy: serde_json::json!({}),
                    queued_at: at(20),
                    event_id: Uuid::new_v4(),
                    correlation_id: format!("e5-g1-{key}"),
                    actor_ref: "actor:e5-g1".to_owned(),
                },
            })
            .expect("submit")
            .body;
        assert_eq!(submitted.id, job_id);
        self.wait_terminal(job_id).await
    }

    async fn wait_terminal(&self, job_id: Uuid) -> JobRecord {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let job = self
                .store
                .get_job(job_id)
                .expect("job is durable while waiting");
            if job.state.is_terminal() {
                return job;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job {job_id} did not reach terminal state"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn store_path(&self) -> std::path::PathBuf {
        self.root.path().join("store")
    }

    /// Consume the stack, close every store handle, and keep the managed
    /// root (plus fixtures, so reopened connectors resolve the same
    /// allow-listed roots) on disk so a fresh restart reopens real state.
    fn persist(mut self) -> std::path::PathBuf {
        let path = self.store_path();
        // Disable TempDir cleanup first so dropping the stack closes the
        // store handles (releasing the root lock) without deleting the
        // durable state the restart must reopen.
        self.root.disable_cleanup(true);
        self.fixture_dir.disable_cleanup(true);
        path
    }

    async fn shutdown(&self) {
        self.runtime.shutdown().await;
    }

    fn no_jobs_or_runs(&self) {
        let jobs = self
            .service
            .list_jobs(ApiRequest {
                meta: meta(self.workspace_id),
                body: ListJobsRequest {
                    limit: 10,
                    cursor: None,
                },
            })
            .expect("list jobs")
            .body;
        assert!(jobs.jobs.is_empty(), "preview publishes no Job");
        let runs = self
            .service
            .list_runs(ApiRequest {
                meta: meta(self.workspace_id),
                body: ListRunsRequest {
                    limit: 10,
                    cursor: None,
                },
            })
            .expect("list runs")
            .body;
        assert!(runs.runs.is_empty(), "preview publishes no Run");
    }
}

fn meta_with_key(workspace_id: Uuid) -> RequestMetadata {
    RequestMetadata {
        idempotency_key: Some(format!("e5-g1-{}", Uuid::new_v4())),
        ..meta(workspace_id)
    }
}

fn materialize_op(workspace_id: Uuid, connection_id: Uuid, asset_id: Uuid) -> JobOperation {
    JobOperation::try_new(
        stillflow_core::OperationKind::Materialize,
        OperationDescriptorV1::Materialize {
            source_asset: stillflow_core::SourceAssetRef {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_to_materialize_snapshot() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;

    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body;
    assert!(!inspected.schema.fields.is_empty());

    let preview = stack
        .service
        .preview_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: PreviewAssetRequest {
                connection_id,
                asset_id: asset.id,
                row_limit: 100,
                byte_limit: 1024 * 1024,
                timeout_seconds: None,
            },
        })
        .await
        .expect("preview")
        .body;
    assert_eq!(preview.rows_returned, 3);
    stack.no_jobs_or_runs();

    let projection = inspected
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);
    let job = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "csv-materialize",
        )
        .await;
    assert_eq!(job.state, JobState::Succeeded, "failure: {:?}", job.failure);
    assert_eq!(job.outputs.len(), 1);
    assert!(
        matches!(
            job.outputs[0],
            TerminalOutputRef::Snapshot {
                committed: true,
                ..
            }
        ),
        "materialize publishes exactly one committed Snapshot"
    );
    let run_id = job.run_id.expect("terminal run");
    let run = stack.store.get_run(run_id).expect("run is durable");
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.outputs, job.outputs);
}

fn snapshot_ref_of(job: &JobRecord) -> stillflow_core::SnapshotRef {
    match &job.outputs[0] {
        TerminalOutputRef::Snapshot {
            workspace_id,
            session_id,
            dataset_id,
            snapshot_id,
            version_digest,
            schema_fingerprint,
            snapshot_version,
            committed: true,
        } => stillflow_core::SnapshotRef {
            workspace_id: *workspace_id,
            session_id: *session_id,
            dataset_id: *dataset_id,
            snapshot_id: *snapshot_id,
            version_digest: *version_digest,
            schema_fingerprint: *schema_fingerprint,
            snapshot_version: *snapshot_version,
        },
        other => panic!("expected committed Snapshot output, got {other:?}"),
    }
}

fn verification_op(snapshot: stillflow_core::SnapshotRef) -> JobOperation {
    JobOperation::try_new(
        stillflow_core::OperationKind::Verification,
        OperationDescriptorV1::Verification {
            snapshot,
            verification_policy: VerificationPolicyV1 {
                batch_size: 1024,
                publish_rejected_rows: true,
            },
        },
    )
    .expect("verification operation validates")
}

fn profile_op(snapshot: stillflow_core::SnapshotRef) -> JobOperation {
    JobOperation::try_new(
        stillflow_core::OperationKind::Profile,
        OperationDescriptorV1::Profile {
            snapshot,
            profile_request: ProfileRequestV1 {
                columns: ProfileColumnsV1::All,
                top_k: 10,
                histogram_buckets: 8,
            },
        },
    )
    .expect("profile operation validates")
}

fn drift_op(
    workspace_id: Uuid,
    dataset_id: Uuid,
    candidate_history_id: Uuid,
) -> DriftComparisonRequest {
    DriftComparisonRequest {
        workspace_id,
        dataset_id,
        candidate_history_id,
        baseline: DriftBaselineMode::LatestEligible,
        threshold_policy_version: stillflow_core::DRIFT_THRESHOLD_POLICY_VERSION,
        observation_window: None,
        report_contract_version: stillflow_core::PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
    }
}

fn export_op(snapshot: stillflow_core::SnapshotRef, root: &std::path::Path) -> JobOperation {
    JobOperation::try_new(
        stillflow_core::OperationKind::Export,
        OperationDescriptorV1::Export {
            snapshot,
            export_request: stillflow_core::ExportRequestV1 {
                export_id: Uuid::new_v4(),
                format: ExportFormat::Csv,
                shape: ExportShape::SingleFile,
                destination: ExportDestinationV1::Local {
                    root: root.to_str().expect("UTF-8 export root").to_owned(),
                    components: vec!["e5-g1-export.csv".to_owned()],
                },
            },
        },
    )
    .expect("export operation validates")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_full_lifecycle_verify_profile_export() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;
    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body;
    let projection = inspected
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);

    let materialized = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "csv-materialize",
        )
        .await;
    assert_eq!(
        materialized.state,
        JobState::Succeeded,
        "failure: {:?}",
        materialized.failure
    );
    let snapshot = snapshot_ref_of(&materialized);

    // Verification publishes exactly one committed bundle; child reports stay
    // bundle members rather than duplicate direct outputs.
    let verified = stack
        .submit_and_wait(
            verification_op(snapshot.clone()),
            plan_id,
            version_id,
            "csv-verification",
        )
        .await;
    assert_eq!(
        verified.state,
        JobState::Succeeded,
        "failure: {:?}",
        verified.failure
    );
    assert_eq!(verified.outputs.len(), 1);
    let (_bundle_id, accepted_snapshot_id, member_kinds) = match &verified.outputs[0] {
        TerminalOutputRef::VerificationBundle {
            bundle_id,
            accepted_snapshot,
            members,
            ..
        } => (
            *bundle_id,
            accepted_snapshot.snapshot_id,
            members
                .iter()
                .map(|member| member.artifact_kind)
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected VerificationBundle output, got {other:?}"),
    };
    assert_eq!(accepted_snapshot_id, snapshot.snapshot_id);
    assert!(
        member_kinds.contains(&ArtifactKind::ValidationReport),
        "bundle carries validation report: {member_kinds:?}"
    );
    assert!(
        member_kinds.contains(&ArtifactKind::DeduplicationReport),
        "bundle carries dedup report: {member_kinds:?}"
    );

    // Profile publishes exactly two committed artifacts through one Job/Run.
    let profiled = stack
        .submit_and_wait(
            profile_op(snapshot.clone()),
            plan_id,
            version_id,
            "csv-profile",
        )
        .await;
    assert_eq!(
        profiled.state,
        JobState::Succeeded,
        "failure: {:?}",
        profiled.failure
    );
    assert_eq!(profiled.outputs.len(), 2);
    let mut profile_kinds = profiled.outputs.iter().map(|output| match output {
        TerminalOutputRef::Artifact {
            artifact_kind,
            state: ArtifactRefState::Committed,
            ..
        } => *artifact_kind,
        other => panic!("expected committed Artifact output, got {other:?}"),
    });
    assert_eq!(profile_kinds.next(), Some(ArtifactKind::ProfileReport));
    assert_eq!(profile_kinds.next(), Some(ArtifactKind::QualityReport));

    // Export publishes exactly one logical artifact; files stay nested.
    let export_dir = tempfile::tempdir().expect("export dir");
    let export_operation = export_op(snapshot.clone(), export_dir.path());
    let (export_snapshot, export_request) = match &export_operation.descriptor {
        OperationDescriptorV1::Export {
            snapshot,
            export_request,
        } => (snapshot.clone(), export_request.clone()),
        other => panic!("expected Export operation, got {other:?}"),
    };
    let export_id = export_request.export_id;
    let export_job_id = Uuid::new_v4();
    let submitted_export = stack
        .service
        .submit_export(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: SubmitExportRequest {
                session_id: stack.session_id,
                plan_version_id: version_id,
                plan_id: Some(plan_id),
                job_id: export_job_id,
                snapshot: export_snapshot,
                export_request,
                execution_policy: serde_json::json!({"deadlineSeconds": 300}),
                output_policy: serde_json::json!({}),
                queued_at: at(20),
                event_id: Uuid::new_v4(),
                correlation_id: "e5-g1-csv-export".to_owned(),
                actor_ref: "actor:e5-g1".to_owned(),
            },
        })
        .expect("submit Export through the X-A1 API")
        .body;
    assert_eq!(submitted_export.id, export_job_id);
    let export_status = stack
        .service
        .read_export_job(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ObjectIdRequest {
                object_id: export_job_id,
            },
        })
        .expect("read Export status")
        .body;
    assert_eq!(
        export_status.operation_kind,
        Some(stillflow_core::OperationKind::Export)
    );
    let exported = stack.wait_terminal(export_job_id).await;
    assert_eq!(
        exported.state,
        JobState::Succeeded,
        "failure: {:?}",
        exported.failure
    );
    assert_eq!(exported.outputs.len(), 1);
    assert!(
        matches!(
            exported.outputs[0],
            TerminalOutputRef::Artifact {
                artifact_kind: ArtifactKind::ExportArtifact,
                state: ArtifactRefState::Committed,
                ..
            }
        ),
        "export publishes exactly one committed ExportArtifact"
    );
    let terminal_cancel = stack
        .service
        .cancel_export_job(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: CancelJobRequest {
                job_id: export_job_id,
            },
        })
        .await
        .expect("terminal Export cancellation is idempotent")
        .body;
    assert_eq!(terminal_cancel.state, JobState::Succeeded);

    let manifest = stack
        .service
        .read_export_manifest(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ObjectIdRequest {
                object_id: export_id,
            },
        })
        .expect("read Export Manifest")
        .body;
    assert_eq!(manifest.export_id, export_id);
    assert_eq!(manifest.run_id, exported.run_id.expect("Export Run"));
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.destination.kind, "managedLocal");
    assert!(!serde_json::to_string(&manifest)
        .expect("Manifest JSON")
        .contains(export_dir.path().to_str().expect("export root UTF-8")));

    let foreign_workspace = Uuid::new_v4();
    stack
        .store
        .create_workspace(foreign_workspace, Utc::now())
        .expect("foreign workspace");
    let foreign_read = stack.service.read_export_manifest(ApiRequest {
        meta: meta(foreign_workspace),
        body: ObjectIdRequest {
            object_id: export_id,
        },
    });
    assert_eq!(
        foreign_read
            .expect_err("foreign Workspace must not read Export Manifest")
            .code,
        stillflow_api::ApiErrorCode::NotFound
    );
    let foreign_files = stack.service.list_export_files(ApiRequest {
        meta: meta(foreign_workspace),
        body: ListExportFilesRequest {
            export_id,
            limit: 1,
            cursor: None,
        },
    });
    assert_eq!(
        foreign_files
            .expect_err("foreign Workspace must not list Export files")
            .code,
        stillflow_api::ApiErrorCode::NotFound
    );

    let file_page = stack
        .service
        .list_export_files(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListExportFilesRequest {
                export_id,
                limit: 1,
                cursor: None,
            },
        })
        .expect("list Export files")
        .body;
    assert_eq!(file_page.files.len(), 1);
    assert!(file_page.next.is_none());
    let file_name = file_page.files[0].name.clone();
    assert_eq!(
        stack
            .service
            .list_export_files(ApiRequest {
                meta: meta(stack.workspace_id),
                body: ListExportFilesRequest {
                    export_id,
                    limit: 101,
                    cursor: None,
                },
            })
            .expect_err("Export file page is bounded")
            .code,
        stillflow_api::ApiErrorCode::LimitExceeded
    );
    assert_eq!(
        stack
            .service
            .list_export_files(ApiRequest {
                meta: meta(stack.workspace_id),
                body: ListExportFilesRequest {
                    export_id,
                    limit: 1,
                    cursor: Some("00".to_owned()),
                },
            })
            .expect_err("malformed Export cursor fails closed")
            .code,
        stillflow_api::ApiErrorCode::InvalidRequest
    );
    let first_chunk = stack
        .service
        .download_export(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ExportDownloadRequest {
                export_id,
                file_name: Some(file_name.clone()),
                max_bytes: 4,
                handle: None,
            },
        })
        .expect("bounded Export download")
        .body;
    assert_eq!(first_chunk.byte_count, 4);
    assert!(!first_chunk.eof);
    let second_chunk = stack
        .service
        .download_export(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ExportDownloadRequest {
                export_id,
                file_name: None,
                max_bytes: 4,
                handle: first_chunk.next.clone(),
            },
        })
        .expect("continue bounded Export download")
        .body;
    let mut downloaded = base64::engine::general_purpose::STANDARD
        .decode(first_chunk.data)
        .expect("first Export chunk base64");
    downloaded.extend(
        base64::engine::general_purpose::STANDARD
            .decode(second_chunk.data)
            .expect("second Export chunk base64"),
    );
    assert_eq!(&downloaded, b"id,label");

    let tombstoned_at = Utc::now() + chrono::Duration::seconds(1);
    let tombstoned = stack
        .service
        .tombstone_export(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: TombstoneExportRequest {
                export_id,
                tombstoned_at,
            },
        })
        .expect("tombstone Export")
        .body;
    assert_eq!(tombstoned.state, "tombstoned");
    let hidden = stack.service.read_export_manifest(ApiRequest {
        meta: meta(stack.workspace_id),
        body: ObjectIdRequest {
            object_id: export_id,
        },
    });
    assert_eq!(
        hidden.expect_err("tombstoned Export is hidden").code,
        stillflow_api::ApiErrorCode::NotFound
    );
    let collected = stack
        .service
        .collect_export_garbage(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: CollectExportGarbageRequest {
                now: tombstoned_at + chrono::Duration::seconds(1),
                retention_seconds: 0,
                max_candidates: 10,
            },
        })
        .expect("collect Export garbage")
        .body;
    assert_eq!(collected.deleted, 1);

    // Terminal Job/Run state agrees across the whole lifecycle.
    for job in [&materialized, &verified, &profiled, &exported] {
        let run = stack
            .store
            .get_run(job.run_id.expect("terminal run"))
            .expect("run is durable");
        assert_eq!(run.state, RunState::Succeeded);
        assert_eq!(run.outputs, job.outputs);
        assert_eq!(run.operation, job.operation);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn q_a1_profile_history_drift_api_uses_one_e5_lifecycle() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "q-a1-rows.csv");
    let connection_id = stack.register_local("q-a1-csv");
    let asset = stack.discover_one(connection_id).await;
    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body;
    let projection = inspected
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    let dataset_id = stack.create_dataset(asset.id);
    let materialized = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "q-a1-materialize",
        )
        .await;
    let snapshot = snapshot_ref_of(&materialized);
    let first_profile = stack
        .submit_and_wait(
            profile_op(snapshot.clone()),
            plan_id,
            version_id,
            "q-a1-profile-1",
        )
        .await;
    assert_eq!(first_profile.state, JobState::Succeeded);
    let second_profile = stack
        .submit_and_wait(profile_op(snapshot), plan_id, version_id, "q-a1-profile-2")
        .await;
    assert_eq!(second_profile.state, JobState::Succeeded);
    let histories = {
        let mut last = Vec::new();
        for _ in 0..200 {
            last = stack
                .store
                .list_profile_history(stack.workspace_id, dataset_id, None, None, 100)
                .expect("profile history")
                .entries;
            if last.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            last.len() >= 2,
            "ProfileHistory publication did not finish: {last:?}"
        );
        last
    };
    assert!(
        histories.len() >= 2,
        "two ProfileHistory entries are durable: first={first_profile:?}, second={second_profile:?}, histories={histories:?}"
    );
    let history_page = stack
        .service
        .list_profile_history(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListProfileHistoryRequest {
                dataset_id,
                state: None,
                columns: Vec::new(),
                limit: 1,
                cursor: None,
            },
        })
        .expect("list ProfileHistory page")
        .body;
    assert_eq!(history_page.entries.len(), 1);
    let history_cursor = history_page
        .next
        .clone()
        .expect("ProfileHistory continuation");
    let history_page_2 = stack
        .service
        .list_profile_history(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListProfileHistoryRequest {
                dataset_id,
                state: None,
                columns: Vec::new(),
                limit: 1,
                cursor: Some(history_cursor),
            },
        })
        .expect("continue ProfileHistory page")
        .body;
    assert_eq!(history_page_2.entries.len(), 1);
    assert_ne!(
        history_page.entries[0].history_id,
        history_page_2.entries[0].history_id
    );
    let candidate_history_id = histories[0].history_id;
    let quality_id = first_profile
        .outputs
        .iter()
        .find_map(|output| match output {
            TerminalOutputRef::Artifact {
                artifact_id,
                artifact_kind: ArtifactKind::QualityReport,
                ..
            } => Some(*artifact_id),
            _ => None,
        })
        .expect("Profile QualityReport output");
    let quality = stack
        .service
        .read_quality_report(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ObjectIdRequest {
                object_id: quality_id,
            },
        })
        .expect("read QualityReport")
        .body;
    assert_eq!(quality.body["artifact_type"], "quality_report");
    assert_eq!(quality.body["artifact_body_version"], 1);
    let quality_findings = stack
        .service
        .list_report_findings(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListFindingsRequest {
                artifact_id: quality_id,
                limit: 100,
                cursor: None,
                severity: None,
                category: None,
                origin: None,
                kind: None,
                column_name: None,
            },
        })
        .expect("read QualityReport findings")
        .body;
    assert_eq!(quality_findings.artifact_id, quality_id);
    let comparison = drift_op(stack.workspace_id, dataset_id, candidate_history_id);
    let drift_job_id = Uuid::new_v4();
    stack
        .service
        .submit_drift_comparison(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: SubmitDriftComparisonRequest {
                session_id: stack.session_id,
                plan_version_id: version_id,
                plan_id: Some(plan_id),
                job_id: drift_job_id,
                comparison,
                execution_policy: serde_json::json!({"deadlineSeconds": 300}),
                output_policy: serde_json::json!({}),
                queued_at: at(30),
                event_id: Uuid::new_v4(),
                correlation_id: "q-a1-drift".to_owned(),
                actor_ref: "actor:q-a1".to_owned(),
            },
        })
        .expect("submit Drift comparison");
    let drift_job = stack.wait_terminal(drift_job_id).await;
    assert_eq!(
        drift_job.state,
        JobState::Succeeded,
        "failure: {:?}",
        drift_job.failure
    );
    let report_id = match &drift_job.outputs[..] {
        [TerminalOutputRef::DriftComparison {
            outcome: stillflow_core::DriftOutcome::Complete | stillflow_core::DriftOutcome::Partial,
            report_artifact_id: Some(report_id),
            ..
        }, TerminalOutputRef::Artifact {
            artifact_id,
            artifact_kind: ArtifactKind::DriftReport,
            state: ArtifactRefState::Committed,
            ..
        }] if report_id == artifact_id => *report_id,
        outputs => panic!("unexpected Q-A1 Drift outputs: {outputs:?}"),
    };
    let report = stack
        .service
        .read_drift_report(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ObjectIdRequest {
                object_id: report_id,
            },
        })
        .expect("read DriftReport")
        .body;
    assert_eq!(report.body["artifact_type"], "drift_report.v1");
    let findings = stack
        .service
        .list_report_findings(ApiRequest {
            meta: meta(stack.workspace_id),
            body: stillflow_api::ListFindingsRequest {
                artifact_id: report_id,
                limit: 100,
                cursor: None,
                severity: None,
                category: None,
                origin: None,
                kind: None,
                column_name: None,
            },
        })
        .expect("read Drift findings")
        .body;
    assert_eq!(findings.artifact_id, report_id);
    let run_id = drift_job.run_id.expect("Drift run");
    let events = run_events(&stack, run_id);
    assert!(events
        .iter()
        .any(|event| event.event_type == ControlPlaneEventType::ArtifactCommitted));
    assert_eq!(
        events.last().map(|event| event.event_type),
        Some(ControlPlaneEventType::RunSucceeded)
    );

    // A fresh idempotency key and Job still reuse the canonical Q-D1 result;
    // the second terminal output has no new Artifact or comparison row.
    let replay_job_id = Uuid::new_v4();
    stack
        .service
        .submit_drift_comparison(ApiRequest {
            meta: meta_with_key(stack.workspace_id),
            body: SubmitDriftComparisonRequest {
                session_id: stack.session_id,
                plan_version_id: version_id,
                plan_id: Some(plan_id),
                job_id: replay_job_id,
                comparison,
                execution_policy: serde_json::json!({"deadlineSeconds": 300}),
                output_policy: serde_json::json!({}),
                queued_at: at(31),
                event_id: Uuid::new_v4(),
                correlation_id: "q-a1-drift-replay".to_owned(),
                actor_ref: "actor:q-a1".to_owned(),
            },
        })
        .expect("submit replay Drift comparison");
    let replay = stack.wait_terminal(replay_job_id).await;
    assert_eq!(replay.state, JobState::Succeeded);
    assert_eq!(replay.outputs.len(), 1);
    assert!(matches!(
        replay.outputs[0],
        TerminalOutputRef::DriftComparison {
            report_artifact_id: Some(id),
            ..
        } if id == report_id
    ));
}

fn job_events(stack: &GateStack, job_id: Uuid) -> Vec<stillflow_api::EventView> {
    stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Job,
                stream_id: job_id,
                cursor: None,
                limit: 1000,
            },
        })
        .expect("job events")
        .body
        .events
}

fn run_events(stack: &GateStack, run_id: Uuid) -> Vec<stillflow_api::EventView> {
    stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Run,
                stream_id: run_id,
                cursor: None,
                limit: 1000,
            },
        })
        .expect("run events")
        .body
        .events
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_events_artifacts_version_and_secret_bounds() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;
    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body;
    let projection = inspected
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);

    let materialized = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "csv-materialize",
        )
        .await;
    assert_eq!(materialized.state, JobState::Succeeded);
    let snapshot = snapshot_ref_of(&materialized);
    let run_id = materialized.run_id.expect("run");

    // Ordered durable delivery: sequences are dense from 1, terminal event is
    // last and agrees with the durable terminal state.
    let job_stream = job_events(&stack, materialized.id);
    assert!(job_stream.len() >= 3, "queued/running/succeeded events");
    let sequences = job_stream
        .iter()
        .map(|event| event.sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences[0], 1);
    for window in sequences.windows(2) {
        assert_eq!(window[1], window[0] + 1, "dense durable order");
    }
    assert_eq!(
        job_stream.last().expect("terminal").event_type,
        ControlPlaneEventType::JobSucceeded
    );
    let run_stream = run_events(&stack, run_id);
    assert_eq!(
        run_stream.last().expect("terminal").event_type,
        ControlPlaneEventType::RunSucceeded
    );

    // Resume from a cursor replays only the tail without gaps.
    let resumed = stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Job,
                stream_id: materialized.id,
                cursor: Some(1),
                limit: 1000,
            },
        })
        .expect("resume")
        .body
        .events;
    assert_eq!(resumed.len(), job_stream.len() - 1);
    assert_eq!(resumed[0].sequence, 2);

    // Event replay stays bounded: an oversized request is clamped to the
    // 1,000-event page cap rather than streaming unbounded history.
    let oversized = stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Job,
                stream_id: materialized.id,
                cursor: None,
                limit: 1001,
            },
        })
        .expect("oversized replay is clamped, not rejected")
        .body
        .events;
    assert!(
        oversized.len() <= 1000,
        "event pages stay within the durable bound"
    );
    assert_eq!(oversized.len(), job_stream.len());

    // Cross-workspace event reads stay isolated: a foreign workspace sees an
    // empty page rather than another workspace's durable history.
    let foreign = stack
        .service
        .list_events(ApiRequest {
            meta: meta(Uuid::new_v4()),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Job,
                stream_id: materialized.id,
                cursor: None,
                limit: 10,
            },
        })
        .expect("foreign read is isolated")
        .body
        .events;
    assert!(
        foreign.is_empty(),
        "foreign workspace sees no durable history"
    );
    // Cross-workspace job reads fail closed through the scope guard.
    let foreign_job = stack.service.read_job(ApiRequest {
        meta: meta(Uuid::new_v4()),
        body: ObjectIdRequest {
            object_id: materialized.id,
        },
    });
    assert!(
        foreign_job.is_err(),
        "foreign workspace job read fails closed"
    );

    // Profile artifacts are readable through bounded metadata APIs with
    // digests matching the terminal outputs.
    let profiled = stack
        .submit_and_wait(profile_op(snapshot), plan_id, version_id, "csv-profile")
        .await;
    assert_eq!(profiled.state, JobState::Succeeded);
    let profile_run = profiled.run_id.expect("profile run");
    let listed = stack
        .service
        .list_artifact_metadata(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListArtifactsRequest {
                run_id: profile_run,
                limit: 10,
                cursor: None,
            },
        })
        .expect("list artifacts")
        .body
        .artifacts;
    assert_eq!(listed.len(), 2);
    for output in &profiled.outputs {
        match output {
            TerminalOutputRef::Artifact {
                artifact_id,
                content_digest,
                ..
            } => {
                let view = stack
                    .service
                    .get_artifact_metadata(ApiRequest {
                        meta: meta(stack.workspace_id),
                        body: ObjectIdRequest {
                            object_id: *artifact_id,
                        },
                    })
                    .expect("artifact metadata")
                    .body;
                assert_eq!(view.run_id, profile_run);
                let mut expected = String::with_capacity(content_digest.len() * 2);
                for byte in content_digest {
                    use std::fmt::Write as _;
                    write!(expected, "{byte:02x}").expect("hex digest");
                }
                assert_eq!(
                    view.content_digest, expected,
                    "metadata digest matches terminal output"
                );
                // Cross-workspace artifact reads fail closed.
                let foreign_read = stack.service.get_artifact_metadata(ApiRequest {
                    meta: meta(Uuid::new_v4()),
                    body: ObjectIdRequest {
                        object_id: *artifact_id,
                    },
                });
                assert!(
                    foreign_read.is_err(),
                    "foreign workspace artifact read fails closed"
                );
            }
            other => panic!("expected Artifact output, got {other:?}"),
        }
    }

    // Version handshake: v1 succeeds, unknown versions fail closed, and the
    // generated OpenAPI representation cannot drift from the manifest.
    let handshake = stack
        .service
        .handshake(ApiRequest {
            meta: meta(stack.workspace_id),
            body: HandshakeRequest {
                requested_version: stillflow_api::ApiVersion::new(1),
            },
        })
        .expect("v1 handshake")
        .body;
    assert_eq!(handshake.selected_version.value(), 1);
    let unknown = stack.service.handshake(ApiRequest {
        meta: meta(stack.workspace_id),
        body: HandshakeRequest {
            requested_version: stillflow_api::ApiVersion::new(99),
        },
    });
    assert_eq!(
        unknown.expect_err("unknown version").code,
        stillflow_api::ApiErrorCode::UnsupportedVersion
    );
    let openapi = stillflow_api::openapi_representation();
    let manifest = serde_json::to_value(stillflow_api::E5_A1_MANIFEST).expect("manifest value");
    assert_eq!(
        openapi, manifest,
        "generated OpenAPI representation matches the route manifest"
    );

    // Secret sentinels: embedded secrets are rejected at the boundary and
    // never reach persisted records, errors, or events.
    let secret = stack.service.register_source_connection(ApiRequest {
        meta: meta(stack.workspace_id),
        body: RegisterSourceConnectionRequest {
            connection_id: Uuid::new_v4(),
            kind: ConnectorKind::LocalFile,
            name: "secret-probe".to_owned(),
            safe_config: serde_json::json!({"password": "SENTINEL-SECRET-VALUE"}),
            credential_ref: "cred://e5-g1/probe".to_owned(),
            created_at: at(4),
        },
    });
    let error = secret.expect_err("embedded secret is rejected");
    assert_eq!(error.code, stillflow_api::ApiErrorCode::InvalidRequest);
    assert!(
        !error.message.contains("SENTINEL"),
        "error carries no secret value"
    );
    for event in job_events(&stack, materialized.id) {
        let payload = serde_json::to_string(&event.payload).expect("payload value");
        assert!(
            !payload.contains("cred://"),
            "event payloads carry references only"
        );
    }
}

/// Reopen the same managed root through fresh store views: terminal state,
/// durable event order, lineage, IDs, caller timestamps, and digests are
/// unchanged, and restart reconciliation publishes no partial artifacts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_preserves_state_events_lineage_and_digests() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;
    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body;
    let projection = inspected
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    let dataset_id = stack.create_dataset(asset.id);
    let version_before = stack
        .store
        .get_plan_version(version_id)
        .expect("plan version");

    let materialized = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "csv-materialize",
        )
        .await;
    assert_eq!(materialized.state, JobState::Succeeded);
    let snapshot = snapshot_ref_of(&materialized);
    let profiled = stack
        .submit_and_wait(profile_op(snapshot), plan_id, version_id, "csv-profile")
        .await;
    assert_eq!(profiled.state, JobState::Succeeded);

    let job_events_before = job_events(&stack, materialized.id);
    let run_before = stack
        .store
        .get_run(materialized.run_id.expect("run"))
        .expect("run");
    let profile_artifacts_before = stack
        .service
        .list_artifact_metadata(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListArtifactsRequest {
                run_id: profiled.run_id.expect("profile run"),
                limit: 10,
                cursor: None,
            },
        })
        .expect("artifacts")
        .body
        .artifacts
        .len();
    stack.shutdown().await;
    let store_path = stack.persist();

    // Fresh views over the same root: nothing process-local is required.
    let reopened_snapshots = Arc::new(
        SnapshotStore::open(&store_path, StorageLimits::default()).expect("reopen snapshots"),
    );
    let reopened = reopened_snapshots.control_plane();

    let job_after = reopened.get_job(materialized.id).expect("job");
    assert_eq!(job_after.state, JobState::Succeeded);
    assert_eq!(job_after.outputs, materialized.outputs);
    assert_eq!(job_after.request_digest, materialized.request_digest);
    assert_eq!(job_after.queued_at, materialized.queued_at);
    assert_eq!(job_after.run_id, materialized.run_id);
    let run_after = reopened
        .get_run(run_before.id)
        .expect("run survives restart");
    assert_eq!(run_after.state, RunState::Succeeded);
    assert_eq!(run_after.outputs, run_before.outputs);
    assert_eq!(run_after.started_at, run_before.started_at);

    let page = reopened
        .list_events(
            job_after.workspace_id,
            EventStreamKind::Job,
            job_after.id,
            None,
            1000,
        )
        .expect("events survive restart");
    assert_eq!(page.events.len(), job_events_before.len());
    assert_eq!(
        page.events.last().expect("terminal").event_type,
        ControlPlaneEventType::JobSucceeded
    );
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        job_events_before
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>()
    );

    let version_after = reopened.get_plan_version(version_id).expect("plan version");
    assert_eq!(
        version_after.canonical_plan_digest,
        version_before.canonical_plan_digest
    );
    assert_eq!(
        version_after.plan_fingerprint,
        version_before.plan_fingerprint
    );

    let datasets = reopened
        .list_datasets(job_after.workspace_id, 128)
        .expect("datasets");
    assert!(
        datasets
            .iter()
            .any(|dataset| dataset.id == dataset_id && dataset.source_asset_id == asset.id),
        "dataset lineage is preserved"
    );

    // Restart reconciliation on a clean terminal store publishes nothing new.
    // The resolver view shares the reopened managed root instead of opening
    // a third handle against the root lock.
    let reopened_shared = Arc::new(reopened);
    let re_runtime = Arc::new(
        JobRuntime::new_with_system_identity(
            job_after.workspace_id,
            Arc::clone(&reopened_shared),
            Arc::clone(&reopened_snapshots),
            Arc::new(ExecutionEngine::new(ConnectorRegistry::new())),
            Arc::new(GateResolver {
                store: Arc::clone(&reopened_shared),
                gate: None,
            }),
        )
        .expect("restart runtime"),
    );
    re_runtime.start().await.expect("restart reconciles");
    // No partial artifacts: the profile run still exposes exactly the two
    // committed report artifacts after reconciliation.
    let artifacts_after = reopened_shared
        .list_artifact_refs(
            job_after.workspace_id,
            profiled.run_id.expect("profile run"),
            None,
            10,
        )
        .expect("artifacts after restart")
        .artifacts
        .len();
    assert_eq!(artifacts_after, profile_artifacts_before);
    re_runtime.shutdown().await;
    drop(re_runtime);
    drop(reopened_shared);
    drop(reopened_snapshots);
    let rechecked = ControlPlaneStore::open(&store_path).expect("recheck");
    assert_eq!(
        rechecked.get_job(materialized.id).expect("job").outputs,
        materialized.outputs
    );
}

fn write_ndjson(root: &std::path::Path, name: &str) {
    std::fs::write(
        root.join(name),
        b"{\"id\":1,\"label\":\"alpha\",\"ignored\":\"x\"}\n{\"id\":2,\"label\":\"beta\",\"ignored\":\"y\"}\n{\"id\":3,\"label\":\"gamma\",\"ignored\":\"z\"}\n",
    )
    .expect("NDJSON fixture");
}

fn write_parquet(root: &std::path::Path, name: &str) {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("ignored", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .expect("Parquet batch");
    let file = std::fs::File::create(root.join(name)).expect("Parquet file");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("Parquet writer");
    writer.write(&batch).expect("write Parquet batch");
    writer.close().expect("close Parquet writer");
}

/// Discover the single asset matching one file name inside a fixture dir
/// holding several gate fixtures.
async fn discover_named(
    stack: &GateStack,
    connection_id: Uuid,
    file_name: &str,
) -> stillflow_api::SourceAssetView {
    let assets = stack
        .service
        .discover_source_assets(ApiRequest {
            meta: meta(stack.workspace_id),
            body: DiscoverAssetsRequest {
                connection_id,
                parent_path: None,
                timeout_seconds: None,
            },
        })
        .await
        .expect("discover")
        .body;
    assets
        .into_iter()
        .find(|asset| asset.name.contains(file_name))
        .unwrap_or_else(|| panic!("gate fixture asset for {file_name}"))
}

async fn inspect_projection(
    stack: &GateStack,
    connection_id: Uuid,
    asset_id: Uuid,
) -> Vec<stillflow_core::ColumnId> {
    stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect")
        .body
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>()
}

async fn preview_rows(stack: &GateStack, connection_id: Uuid, asset_id: Uuid) -> usize {
    stack
        .service
        .preview_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: PreviewAssetRequest {
                connection_id,
                asset_id,
                row_limit: 100,
                byte_limit: 1024 * 1024,
                timeout_seconds: None,
            },
        })
        .await
        .expect("preview")
        .body
        .rows_returned
}

async fn materialize_named(
    stack: &GateStack,
    connection_id: Uuid,
    asset: &stillflow_api::SourceAssetView,
) -> JobRecord {
    let projection = inspect_projection(stack, connection_id, asset.id).await;
    assert_eq!(preview_rows(stack, connection_id, asset.id).await, 3);
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);
    let job = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            &format!("gate-{}", asset.name),
        )
        .await;
    assert_eq!(job.state, JobState::Succeeded, "failure: {:?}", job.failure);
    assert_eq!(job.outputs.len(), 1);
    assert!(matches!(
        job.outputs[0],
        TerminalOutputRef::Snapshot {
            committed: true,
            ..
        }
    ));
    job
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ndjson_complete_supported_lifecycle() {
    let stack = GateStack::local_stack().await;
    write_ndjson(stack.fixture_dir.path(), "rows.ndjson");
    let connection_id = stack.register_local("ndjson");
    let asset = discover_named(&stack, connection_id, "rows.ndjson").await;
    stack.no_jobs_or_runs();
    materialize_named(&stack, connection_id, &asset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_complete_supported_lifecycle() {
    let stack = GateStack::local_stack().await;
    write_parquet(stack.fixture_dir.path(), "rows.parquet");
    let connection_id = stack.register_local("parquet");
    let asset = discover_named(&stack, connection_id, "rows.parquet").await;
    materialize_named(&stack, connection_id, &asset).await;
}

const WORKBOOK_FIXTURE_B64: &str =
    include_str!("../../stillflow-connector-workbook/tests/fixtures/temperature.xlsx.b64");

async fn workbook_stack() -> GateStack {
    GateStack::new(vec![
        Arc::new(LocalTabularConnector) as SourceConnectorRef,
        Arc::new(stillflow_connector_workbook::WorkbookConnector) as SourceConnectorRef,
    ])
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workbook_applicable_lifecycle() {
    use base64::Engine as _;
    use futures::StreamExt as _;
    let stack = workbook_stack().await;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(WORKBOOK_FIXTURE_B64.split_whitespace().collect::<String>())
        .expect("workbook fixture decodes");
    std::fs::write(stack.fixture_dir.path().join("temperature.xlsx"), bytes)
        .expect("workbook fixture");
    let connection_id = Uuid::new_v4();
    stack
        .service
        .register_source_connection(ApiRequest {
            meta: meta(stack.workspace_id),
            body: RegisterSourceConnectionRequest {
                connection_id,
                kind: ConnectorKind::ExcelWorkbook,
                name: "workbook".to_owned(),
                safe_config: serde_json::json!({
                    "allowedRoots": [stack.fixture_dir.path().to_str().expect("UTF-8")],
                    "maxSheetCells": 2_000_000
                }),
                credential_ref: "cred://e5-g1/workbook-fixtures".to_owned(),
                created_at: at(3),
            },
        })
        .expect("register workbook");
    // API boundary: test + discover + inspect expose sheet assets with
    // explicit-selection findings; unselected preview fails closed.
    stack
        .service
        .test_source_connection(ApiRequest {
            meta: meta(stack.workspace_id),
            body: stillflow_api::TestSourceConnectionRequest {
                connection_id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("test connection");
    let assets = stack
        .service
        .discover_source_assets(ApiRequest {
            meta: meta(stack.workspace_id),
            body: DiscoverAssetsRequest {
                connection_id,
                parent_path: None,
                timeout_seconds: None,
            },
        })
        .await
        .expect("discover workbook")
        .body;
    assert!(!assets.is_empty(), "workbook exposes sheet assets");
    stack.no_jobs_or_runs();
    let asset_view = assets
        .iter()
        .find(|asset| asset.name.contains("Sheet1"))
        .expect("temperature sheet");
    let inspected = stack
        .service
        .inspect_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: InspectAssetRequest {
                connection_id,
                asset_id: asset_view.id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("inspect candidates")
        .body;
    let workbook_meta = inspected.workbook.expect("workbook inspection");
    assert!(
        !workbook_meta.region_candidates.is_empty(),
        "inspection proposes explicit regions"
    );
    let unselected = stack
        .service
        .preview_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: PreviewAssetRequest {
                connection_id,
                asset_id: asset_view.id,
                row_limit: 10,
                byte_limit: 1024 * 1024,
                timeout_seconds: None,
            },
        })
        .await;
    assert!(
        unselected.is_err(),
        "preview without explicit selection fails closed"
    );
    stack.no_jobs_or_runs();

    // Connector behavior with an explicit selection: region inspect yields a
    // schema, bounded preview returns rows, and the read stream is finite.
    // Selections are connector-memory only; no asset-locator mutation or Job
    // path is invented for the gate.
    let record = stack
        .store
        .get_source_asset(asset_view.id)
        .expect("persisted asset");
    let locator: stillflow_core::AssetLocator =
        serde_json::from_value(record.safe_locator.clone()).expect("locator decodes");
    let connection_record = stack
        .store
        .get_source_connection(connection_id)
        .expect("persisted connection");
    let connection = GateResolver::domain_connection(&connection_record);
    let mut asset = GateResolver::domain_asset(&record);
    let _ = locator;
    let candidate = workbook_meta
        .region_candidates
        .into_iter()
        .next()
        .expect("candidate");
    asset.locator.workbook_region = Some(stillflow_core::WorkbookRegionSelection {
        range: candidate.range,
        header: stillflow_core::WorkbookHeaderSelection::Row(0),
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(stillflow_connector_workbook::WorkbookConnector) as SourceConnectorRef)
        .expect("workbook registry");
    let selected = registry
        .inspect(
            &connection,
            stillflow_core::InspectRequest {
                context: RequestContext::new(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect selected region");
    assert!(!selected.schema.fields.is_empty());
    let preview = registry
        .preview(
            &connection,
            stillflow_core::PreviewRequest::new(asset.clone(), 10, 1024 * 1024),
        )
        .await
        .expect("selected preview");
    assert!(preview.rows_returned > 0);
    let mut stream = registry
        .read_batches(&connection, stillflow_core::ReadRequest::new(asset, 1024))
        .await
        .expect("read stream");
    let mut batches = 0;
    while let Some(item) = stream.next().await {
        item.expect("batch item");
        batches += 1;
    }
    assert!(batches > 0, "sheet stream terminates with batches");

    // Corrupt inputs fail closed at discovery through the same boundary.
    let corrupt_dir = tempfile::tempdir().expect("corrupt dir");
    std::fs::write(corrupt_dir.path().join("corrupt.xlsx"), b"not a workbook")
        .expect("corrupt fixture");
    let corrupt_id = Uuid::new_v4();
    stack
        .service
        .register_source_connection(ApiRequest {
            meta: meta(stack.workspace_id),
            body: RegisterSourceConnectionRequest {
                connection_id: corrupt_id,
                kind: ConnectorKind::ExcelWorkbook,
                name: "corrupt".to_owned(),
                safe_config: serde_json::json!({
                    "allowedRoots": [corrupt_dir.path().to_str().expect("UTF-8")],
                    "maxSheetCells": 2_000_000
                }),
                credential_ref: "cred://e5-g1/workbook-fixtures".to_owned(),
                created_at: at(5),
            },
        })
        .expect("register corrupt");
    let corrupt = stack
        .service
        .discover_source_assets(ApiRequest {
            meta: meta(stack.workspace_id),
            body: DiscoverAssetsRequest {
                connection_id: corrupt_id,
                parent_path: None,
                timeout_seconds: None,
            },
        })
        .await;
    assert!(corrupt.is_err(), "corrupt workbook discovery fails closed");
    stack.no_jobs_or_runs();
}

// ---------------------------------------------------------------------------
// Minimal S3-compatible fixture: ListObjectsV2 + HEAD + GET (+Range). The
// mock never sees credentials; the resolver only receives the opaque ref.
// ---------------------------------------------------------------------------

struct S3Counts {
    range_gets: std::sync::atomic::AtomicUsize,
    full_gets: std::sync::atomic::AtomicUsize,
}

struct S3Fixture {
    address: std::net::SocketAddr,
    counts: Arc<S3Counts>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl S3Fixture {
    fn start(objects: std::collections::BTreeMap<String, Vec<u8>>) -> Self {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind S3 fixture");
        listener.set_nonblocking(true).expect("nonblocking fixture");
        let address = listener.local_addr().expect("fixture address");
        let objects = Arc::new(objects);
        let counts = Arc::new(S3Counts {
            range_gets: std::sync::atomic::AtomicUsize::new(0),
            full_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_objects = Arc::clone(&objects);
        let worker_counts = Arc::clone(&counts);
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::spawn(move || loop {
            if worker_stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    let mut received = Vec::new();
                    let end = loop {
                        match received.windows(4).position(|item| item == b"\r\n\r\n") {
                            Some(position) => break position + 4,
                            None => {
                                let mut chunk = [0_u8; 4096];
                                match stream.read(&mut chunk) {
                                    Ok(0) | Err(_) => break usize::MAX,
                                    Ok(read) => received.extend_from_slice(&chunk[..read]),
                                }
                            }
                        }
                    };
                    if end == usize::MAX {
                        continue;
                    }
                    let header = String::from_utf8_lossy(&received[..end]).into_owned();
                    let mut lines = header.split("\r\n");
                    let request_line = lines.next().unwrap_or("").to_owned();
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_owned();
                    let target = parts.next().unwrap_or("").to_owned();
                    let mut headers = std::collections::BTreeMap::new();
                    for line in lines.filter(|line| !line.is_empty()) {
                        if let Some((name, value)) = line.split_once(':') {
                            headers
                                .insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
                        }
                    }
                    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
                    let mut respond = |status: &str,
                                       extra: &[(&str, String)],
                                       content_length: usize,
                                       body: &[u8]| {
                        let mut response = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nConnection: close\r\n",
                        );
                        for (name, value) in extra {
                            response.push_str(&format!("{name}: {value}\r\n"));
                        }
                        response.push_str("\r\n");
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.write_all(body);
                        let _ = stream.flush();
                    };
                    if method == "GET"
                        && path.trim_end_matches('/') == "/fixture-bucket"
                        && query.contains("list-type")
                    {
                        let mut body = format!(
                            "<ListBucketResult><Name>fixture-bucket</Name><IsTruncated>false</IsTruncated><KeyCount>{}</KeyCount>",
                            worker_objects.len()
                        );
                        for (key, value) in worker_objects.iter() {
                            body.push_str(&format!(
                                "<Contents><Key>{key}</Key><LastModified>2026-08-12T00:00:00Z</LastModified><ETag>\"etag-{}\"</ETag><Size>{}</Size></Contents>",
                                value.len(),
                                value.len()
                            ));
                        }
                        body.push_str("</ListBucketResult>");
                        respond(
                            "200 OK",
                            &[("Content-Type", "application/xml".to_owned())],
                            body.len(),
                            body.as_bytes(),
                        );
                        continue;
                    }
                    let Some(key) = path.strip_prefix("/fixture-bucket/") else {
                        respond("404 Not Found", &[], b"missing".len(), b"missing");
                        continue;
                    };
                    let Some(object) = worker_objects.get(key).cloned() else {
                        respond("404 Not Found", &[], b"missing".len(), b"missing");
                        continue;
                    };
                    if method == "HEAD" {
                        respond(
                            "200 OK",
                            &[("ETag", format!("\"etag-{}\"", object.len()))],
                            object.len(),
                            &[],
                        );
                        continue;
                    }
                    if method == "GET" {
                        if let Some(range_value) = headers.get("range") {
                            let range = range_value
                                .strip_prefix("bytes=")
                                .and_then(|value| value.split_once('-'))
                                .and_then(|(start, end)| {
                                    Some((
                                        start.parse::<usize>().ok()?,
                                        end.parse::<usize>().ok()? + 1,
                                    ))
                                });
                            if let Some((start, end)) = range {
                                if start >= end || end > object.len() {
                                    respond("416 Range Not Satisfiable", &[], b"".len(), b"");
                                    continue;
                                }
                                let body = &object[start..end];
                                worker_counts
                                    .range_gets
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                respond(
                                    "206 Partial Content",
                                    &[
                                        ("ETag", format!("\"etag-{}\"", object.len())),
                                        (
                                            "Content-Range",
                                            format!("bytes {}-{}/{}", start, end - 1, object.len()),
                                        ),
                                    ],
                                    body.len(),
                                    body,
                                );
                                continue;
                            }
                        }
                        worker_counts
                            .full_gets
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        respond(
                            "200 OK",
                            &[("ETag", format!("\"etag-{}\"", object.len()))],
                            object.len(),
                            &object,
                        );
                        continue;
                    }
                    respond("405 Method Not Allowed", &[], b"".len(), b"");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(_) => break,
            }
        });
        Self {
            address,
            counts,
            stop,
            worker: Some(worker),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for S3Fixture {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.address);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct S3GateResolver;

#[async_trait::async_trait]
impl stillflow_connector_object_store::ObjectStoreCredentialResolver for S3GateResolver {
    async fn resolve_s3(
        &self,
        credential_ref: &CredentialRef,
    ) -> stillflow_core::ConnectorResult<stillflow_connector_object_store::S3CredentialMaterial>
    {
        assert_eq!(credential_ref.as_str(), "cred://e5-g1/s3-fixture");
        stillflow_connector_object_store::S3CredentialMaterial::new(
            "SENTINEL_ACCESS_KEY",
            "SENTINEL_SECRET_KEY",
            None,
        )
    }
}

async fn s3_stack() -> GateStack {
    GateStack::new(vec![
        Arc::new(LocalTabularConnector) as SourceConnectorRef,
        Arc::new(stillflow_connector_object_store::ObjectStoreConnector::new(
            Arc::new(S3GateResolver),
        )) as SourceConnectorRef,
    ])
    .await
}

fn parquet_bytes() -> Vec<u8> {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
        ],
    )
    .expect("Parquet batch");
    let mut buffer = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buffer, schema, None).expect("Parquet writer");
    writer.write(&batch).expect("write Parquet batch");
    writer.close().expect("close Parquet writer");
    buffer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_compatible_bounded_range_aware_lifecycle() {
    let fixture = S3Fixture::start(std::collections::BTreeMap::from([(
        "rows.parquet".to_owned(),
        parquet_bytes(),
    )]));
    let stack = s3_stack().await;
    let connection_id = Uuid::new_v4();
    stack
        .service
        .register_source_connection(ApiRequest {
            meta: meta(stack.workspace_id),
            body: RegisterSourceConnectionRequest {
                connection_id,
                kind: ConnectorKind::ObjectStore,
                name: "s3".to_owned(),
                safe_config: serde_json::json!({
                    "provider": "s3",
                    "bucket": "fixture-bucket",
                    "region": "us-east-1",
                    "endpoint": fixture.endpoint(),
                    "pathStyle": true,
                    "anonymous": false,
                    "allowHttp": true,
                    "maxPreviewSourceBytes": 65536
                }),
                credential_ref: "cred://e5-g1/s3-fixture".to_owned(),
                created_at: at(3),
            },
        })
        .expect("register s3");
    stack
        .service
        .test_source_connection(ApiRequest {
            meta: meta(stack.workspace_id),
            body: stillflow_api::TestSourceConnectionRequest {
                connection_id,
                timeout_seconds: None,
            },
        })
        .await
        .expect("test s3");
    let asset = stack.discover_one(connection_id).await;
    // Bounded preview: parquet footer access stays range-aware; a 3-row
    // object never requires an unbounded full-object pull beyond the byte cap.
    let preview = stack
        .service
        .preview_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: PreviewAssetRequest {
                connection_id,
                asset_id: asset.id,
                row_limit: 100,
                byte_limit: 65_536,
                timeout_seconds: None,
            },
        })
        .await
        .expect("s3 preview")
        .body;
    assert_eq!(preview.rows_returned, 3);
    assert!(preview.bytes_returned <= 65_536);
    stack.no_jobs_or_runs();
    let ranges = fixture
        .counts
        .range_gets
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(ranges > 0, "parquet access stays range-aware");
    materialize_named(&stack, connection_id, &asset).await;
}

fn submit_typed(
    stack: &GateStack,
    operation: JobOperation,
    plan_id: Uuid,
    version_id: Uuid,
    key: &str,
    job_id: Uuid,
    deadline_seconds: u64,
) -> stillflow_api::JobView {
    stack
        .service
        .submit_job(ApiRequest {
            meta: RequestMetadata {
                idempotency_key: Some(key.to_owned()),
                ..meta(stack.workspace_id)
            },
            body: SubmitJobRequest {
                session_id: stack.session_id,
                plan_version_id: version_id,
                plan_id: Some(plan_id),
                job_id,
                operation: Some(operation.clone()),
                inputs: vec![operation.input()],
                execution_policy: serde_json::json!({"deadlineSeconds": deadline_seconds}),
                output_policy: serde_json::json!({}),
                queued_at: at(20),
                event_id: Uuid::new_v4(),
                correlation_id: format!("e5-g1-{key}"),
                actor_ref: "actor:e5-g1".to_owned(),
            },
        })
        .expect("submit")
        .body
}

async fn csv_stack_with_plan(stack: &GateStack) -> (Uuid, Uuid, Uuid, Uuid) {
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;
    let projection = inspect_projection(stack, connection_id, asset.id).await;
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);
    (connection_id, asset.id, plan_id, version_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_submit_replays_and_conflicts_deterministically() {
    let stack = GateStack::local_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    let operation = materialize_op(stack.workspace_id, connection_id, asset_id);

    // Same key + same digest replays the original immutable terminal result.
    let first_id = Uuid::new_v4();
    let first = submit_typed(
        &stack,
        operation.clone(),
        plan_id,
        version_id,
        "dup-key",
        first_id,
        300,
    );
    assert_eq!(first.id, first_id);
    let terminal = stack.wait_terminal(first_id).await;
    assert_eq!(terminal.state, JobState::Succeeded);
    let replayed = submit_typed(
        &stack,
        operation.clone(),
        plan_id,
        version_id,
        "dup-key",
        Uuid::new_v4(),
        300,
    );
    assert_eq!(replayed.id, first_id, "replay returns the original job");
    assert_eq!(replayed.outputs, terminal.outputs);

    // Same key + different digest (operation change) conflicts without mutation.
    let snapshot = snapshot_ref_of(&terminal);
    let profile_operation = profile_op(snapshot);
    let conflicted = stack.service.submit_job(ApiRequest {
        meta: RequestMetadata {
            idempotency_key: Some("dup-key".to_owned()),
            ..meta(stack.workspace_id)
        },
        body: SubmitJobRequest {
            session_id: stack.session_id,
            plan_version_id: version_id,
            plan_id: Some(plan_id),
            job_id: Uuid::new_v4(),
            operation: Some(profile_operation.clone()),
            inputs: vec![profile_operation.input()],
            execution_policy: serde_json::json!({"deadlineSeconds": 300}),
            output_policy: serde_json::json!({}),
            queued_at: at(20),
            event_id: Uuid::new_v4(),
            correlation_id: "e5-g1-dup-conflict".to_owned(),
            actor_ref: "actor:e5-g1".to_owned(),
        },
    });
    assert_eq!(
        conflicted.expect_err("digest change conflicts").code,
        stillflow_api::ApiErrorCode::Conflict
    );
    assert_eq!(
        stack.store.get_job(first_id).expect("original").outputs,
        terminal.outputs,
        "conflict leaves the original immutable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_cancel_is_durable_and_terminal_cancel_is_idempotent() {
    let stack = GateStack::local_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    // Stop workers so the submission stays queued and cancellation needs no race.
    stack.shutdown().await;
    let job_id = Uuid::new_v4();
    let submitted = submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset_id),
        plan_id,
        version_id,
        "cancel-key",
        job_id,
        300,
    );
    assert_eq!(submitted.state, JobState::Queued);
    let cancelled = stack
        .service
        .cancel_job(ApiRequest {
            meta: meta(stack.workspace_id),
            body: CancelJobRequest { job_id },
        })
        .await
        .expect("cancel queued")
        .body;
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(
        cancelled.outputs.is_empty(),
        "cancelled jobs expose no partial outputs"
    );
    // Terminal cancel replays the durable outcome instead of mutating it.
    let again = stack
        .service
        .cancel_job(ApiRequest {
            meta: meta(stack.workspace_id),
            body: CancelJobRequest { job_id },
        })
        .await
        .expect("terminal cancel is idempotent")
        .body;
    assert_eq!(again.state, JobState::Cancelled);
    let events = job_events(&stack, job_id);
    assert_eq!(
        events.last().expect("terminal").event_type,
        ControlPlaneEventType::JobCancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_bound_deadline_fails_closed_without_partial_outputs() {
    let stack = GateStack::local_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    let job_id = Uuid::new_v4();
    submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset_id),
        plan_id,
        version_id,
        "deadline-key",
        job_id,
        1801,
    );
    let job = stack.wait_terminal(job_id).await;
    assert_eq!(job.state, JobState::Failed);
    assert!(
        job.outputs.is_empty(),
        "deadline failure exposes no partial outputs"
    );
    assert!(job.failure.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_source_materialize_fails_closed_without_partial_artifact() {
    let stack = GateStack::local_stack().await;
    std::fs::write(
        stack.fixture_dir.path().join("corrupt.csv"),
        b"id,label\n1,\"unterminated\n2,broken\xff\xfe\n",
    )
    .expect("corrupt fixture");
    let connection_id = stack.register_local("corrupt");
    let asset = discover_named(&stack, connection_id, "corrupt.csv").await;
    // Corrupt inputs either fail closed at preview or fail the job without
    // publishing partial artifacts; both outcomes are fail-closed.
    let preview = stack
        .service
        .preview_source_asset(ApiRequest {
            meta: meta(stack.workspace_id),
            body: PreviewAssetRequest {
                connection_id,
                asset_id: asset.id,
                row_limit: 100,
                byte_limit: 1024 * 1024,
                timeout_seconds: None,
            },
        })
        .await;
    if preview.is_err() {
        stack.no_jobs_or_runs();
        return;
    }
    let projection = inspect_projection(&stack, connection_id, asset.id).await;
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);
    let job_id = Uuid::new_v4();
    submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset.id),
        plan_id,
        version_id,
        "corrupt-key",
        job_id,
        300,
    );
    let job = stack.wait_terminal(job_id).await;
    assert!(
        matches!(job.state, JobState::Failed | JobState::Cancelled),
        "corrupt source never succeeds: {:?}",
        job.state
    );
    assert!(
        job.outputs.is_empty(),
        "corrupt source leaves no visible partial artifact"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thousand_plus_event_replay_is_paginated_and_bounded() {
    let stack = GateStack::local_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    let operation = materialize_op(stack.workspace_id, connection_id, asset_id);
    let job_id = Uuid::new_v4();
    // Queue through storage directly so the worker does not race the fill.
    stack.shutdown().await;
    submit_typed(
        &stack, operation, plan_id, version_id, "page-key", job_id, 300,
    );
    let run_id = Uuid::new_v4();
    stack
        .store
        .claim_job(
            job_id,
            run_id,
            at(21),
            1,
            "engine-test",
            stillflow_storage::EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Job,
                job_id,
                job_id,
                None,
                ControlPlaneEventType::JobRunning,
                at(21),
                "request-claim",
                "correlation-claim",
                "actor:e5-g1",
                serde_json::json!({"state": "running"}),
            ),
            stillflow_storage::EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Run,
                run_id,
                job_id,
                Some(run_id),
                ControlPlaneEventType::RunRunning,
                at(21),
                "request-claim",
                "correlation-claim",
                "actor:e5-g1",
                serde_json::json!({"state": "running"}),
            ),
        )
        .expect("claim");
    for ordinal in 0..1_001 {
        stack
            .store
            .append_event(stillflow_storage::EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Run,
                run_id,
                job_id,
                Some(run_id),
                ControlPlaneEventType::RunReconciled,
                at(100 + ordinal),
                format!("request-page-{ordinal}"),
                format!("correlation-page-{ordinal}"),
                "actor:e5-g1",
                serde_json::json!({"ordinal": ordinal}),
            ))
            .expect("append");
    }
    // First page is capped at the 1,000-event durable bound with a resume cursor.
    let first = stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Run,
                stream_id: run_id,
                cursor: None,
                limit: 1000,
            },
        })
        .expect("first page")
        .body;
    assert_eq!(first.events.len(), 1000);
    assert_eq!(first.events[0].sequence, 1);
    assert_eq!(first.events[999].sequence, 1000);
    let next = first.next_sequence.expect("resume cursor");
    assert_eq!(next, 1000);
    // The tail page resumes without gaps or duplicates.
    let second = stack
        .service
        .list_events(ApiRequest {
            meta: meta(stack.workspace_id),
            body: ListEventsRequest {
                stream_kind: EventStreamKind::Run,
                stream_id: run_id,
                cursor: Some(next),
                limit: 1000,
            },
        })
        .expect("second page")
        .body;
    assert_eq!(second.events.len(), 2);
    assert_eq!(second.events[0].sequence, 1001);
    assert_eq!(second.events[1].sequence, 1002);
    assert!(second.next_sequence.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_race_reaches_running_job_without_second_state_machine() {
    let (stack, gate) = GateStack::gated_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let entered = gate.entered.notified();
    let job_id = Uuid::new_v4();
    submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset_id),
        plan_id,
        version_id,
        "race-key",
        job_id,
        300,
    );
    // The worker claims first (Running), then blocks inside resolve.
    tokio::time::timeout(Duration::from_secs(30), entered)
        .await
        .expect("worker enters resolve");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state = stack.store.get_job(job_id).expect("job").state;
        assert!(!state.is_terminal(), "job must not finish while gated");
        if state == JobState::Running {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "job never runs");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Cancellation reaches the running job through the one durable path.
    let cancelled = stack
        .service
        .cancel_job(ApiRequest {
            meta: meta(stack.workspace_id),
            body: CancelJobRequest { job_id },
        })
        .await
        .expect("cancel running")
        .body;
    assert!(
        matches!(cancelled.state, JobState::Cancelling | JobState::Cancelled),
        "cancel lands on the durable state machine"
    );
    gate.release.notify_one();
    let job = stack.wait_terminal(job_id).await;
    assert_eq!(job.state, JobState::Cancelled);
    assert!(
        job.outputs.is_empty(),
        "cancelled race exposes no partial outputs"
    );
    let run = stack.store.get_run(job.run_id.expect("run")).expect("run");
    assert_eq!(run.state, RunState::Cancelled);
    let events = job_events(&stack, job_id);
    assert_eq!(
        events.last().expect("terminal").event_type,
        ControlPlaneEventType::JobCancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_reconciles_queued_and_running_jobs_without_partial_artifacts() {
    let (stack, gate) = GateStack::gated_stack().await;
    let (connection_id, asset_id, plan_id, version_id) = csv_stack_with_plan(&stack).await;
    // One job stays queued (workers stopped), one stays running (gated).
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let entered = gate.entered.notified();
    let running_id = Uuid::new_v4();
    submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset_id),
        plan_id,
        version_id,
        "restart-running",
        running_id,
        300,
    );
    tokio::time::timeout(Duration::from_secs(30), entered)
        .await
        .expect("worker enters resolve");
    stack.shutdown().await;
    let queued_id = Uuid::new_v4();
    submit_typed(
        &stack,
        materialize_op(stack.workspace_id, connection_id, asset_id),
        plan_id,
        version_id,
        "restart-queued",
        queued_id,
        300,
    );
    assert_eq!(
        stack.store.get_job(queued_id).expect("queued").state,
        JobState::Queued
    );
    let workspace_id = stack.workspace_id;
    let store_path = stack.persist();

    // Fresh runtime restart: the abandoned Running job reconciles to Failed
    // (worker_lost) with no partial outputs; the Queued job stays queued.
    let reopened_snapshots =
        Arc::new(SnapshotStore::open(&store_path, StorageLimits::default()).expect("reopen"));
    let reopened = Arc::new(reopened_snapshots.control_plane());
    let re_runtime = Arc::new(
        JobRuntime::new_with_system_identity(
            workspace_id,
            Arc::clone(&reopened),
            Arc::clone(&reopened_snapshots),
            Arc::new(ExecutionEngine::new({
                let mut registry = ConnectorRegistry::new();
                registry
                    .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
                    .expect("registry");
                registry
            })),
            Arc::new(GateResolver {
                store: Arc::clone(&reopened),
                gate: None,
            }),
        )
        .expect("restart runtime"),
    );
    re_runtime.start().await.expect("restart reconciles");
    let lost = reopened.get_job(running_id).expect("lost job");
    assert_eq!(lost.state, JobState::Failed);
    assert!(
        lost.outputs.is_empty(),
        "worker_lost exposes no partial outputs"
    );
    let lost_run = reopened
        .get_run(lost.run_id.expect("lost run"))
        .expect("lost run");
    assert_eq!(lost_run.state, RunState::Failed);
    // The queued job is picked up by the restarted runtime and succeeds.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let queued = loop {
        let job = reopened.get_job(queued_id).expect("queued job");
        if job.state.is_terminal() {
            break job;
        }
        assert!(std::time::Instant::now() < deadline, "queued job finishes");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        queued.state,
        JobState::Succeeded,
        "failure: {:?}",
        queued.failure
    );
    assert_eq!(queued.outputs.len(), 1);
    re_runtime.shutdown().await;
}

#[test]
fn plan_save_version_cas_and_digest_are_stable_through_api() {
    let root = tempfile::tempdir().expect("root");
    let store = Arc::new(
        SnapshotStore::open(root.path().join("store"), StorageLimits::default())
            .expect("store")
            .control_plane(),
    );
    let service = ApiService::new(store);
    let workspace_id = Uuid::new_v4();
    store_create_workspace(&service, workspace_id);
    let plan_id = Uuid::new_v4();
    service
        .create_plan(ApiRequest {
            meta: meta(workspace_id),
            body: CreatePlanRequest {
                plan_id,
                created_at: at(10),
            },
        })
        .expect("plan");
    let scan_asset = Uuid::new_v4();
    let plan_v1 = GateStack::scan_materialize_plan_for(
        scan_asset,
        vec![stillflow_core::ColumnId::from_uuid(Uuid::new_v4())],
    );
    let v1 = Uuid::new_v4();
    let saved_v1 = service
        .save_plan_version(ApiRequest {
            meta: meta(workspace_id),
            body: SavePlanVersionRequest {
                plan_id,
                plan_version_id: v1,
                version_number: 1,
                parent_version_id: None,
                logical_plan: plan_v1,
                created_at: at(11),
            },
        })
        .expect("v1")
        .body;
    let digest_v1 = saved_v1.canonical_plan_digest.clone();
    // Reloads are digest-stable.
    let loaded = service
        .load_plan_version(ApiRequest {
            meta: meta(workspace_id),
            body: ObjectIdRequest { object_id: v1 },
        })
        .expect("load v1")
        .body;
    assert_eq!(loaded.canonical_plan_digest, digest_v1);
    service
        .publish_plan_version(ApiRequest {
            meta: meta(workspace_id),
            body: PublishPlanVersionRequest {
                plan_version_id: v1,
                expected_current_version_id: None,
                published_at: at(12),
            },
        })
        .expect("publish v1");
    // v2 with a different asset digest-changes; CAS publish guards the head.
    let plan_v2 = GateStack::scan_materialize_plan_for(
        Uuid::new_v4(),
        vec![stillflow_core::ColumnId::from_uuid(Uuid::new_v4())],
    );
    let v2 = Uuid::new_v4();
    let saved_v2 = service
        .save_plan_version(ApiRequest {
            meta: meta(workspace_id),
            body: SavePlanVersionRequest {
                plan_id,
                plan_version_id: v2,
                version_number: 2,
                parent_version_id: Some(v1),
                logical_plan: plan_v2,
                created_at: at(13),
            },
        })
        .expect("v2")
        .body;
    assert_ne!(saved_v2.canonical_plan_digest, digest_v1);
    let stale = service.publish_plan_version(ApiRequest {
        meta: meta(workspace_id),
        body: PublishPlanVersionRequest {
            plan_version_id: v2,
            expected_current_version_id: Some(Uuid::new_v4()),
            published_at: at(14),
        },
    });
    assert!(stale.is_err(), "stale CAS fails closed");
    service
        .publish_plan_version(ApiRequest {
            meta: meta(workspace_id),
            body: PublishPlanVersionRequest {
                plan_version_id: v2,
                expected_current_version_id: Some(v1),
                published_at: at(14),
            },
        })
        .expect("publish v2 with CAS");
    let diff = service
        .diff_plans(ApiRequest {
            meta: meta(workspace_id),
            body: stillflow_api::PlanDiffRequest {
                left_version_id: v1,
                right_version_id: v2,
            },
        })
        .expect("diff")
        .body;
    assert!(diff.changed);
    assert_eq!(diff.left_canonical_plan_digest, digest_v1);
    let same = service
        .diff_plans(ApiRequest {
            meta: meta(workspace_id),
            body: stillflow_api::PlanDiffRequest {
                left_version_id: v1,
                right_version_id: v1,
            },
        })
        .expect("self diff")
        .body;
    assert!(!same.changed);
}

fn store_create_workspace(service: &ApiService, workspace_id: Uuid) {
    service
        .create_workspace(ApiRequest {
            meta: RequestMetadata {
                idempotency_key: Some("e5-g1-workspace".to_owned()),
                ..meta(workspace_id)
            },
            body: stillflow_api::CreateWorkspaceRequest {
                workspace_id,
                created_at: at(1),
            },
        })
        .expect("workspace");
}

fn h1_export_request(
    root: &std::path::Path,
    format: ExportFormat,
    extension: &str,
) -> stillflow_core::ExportRequestV1 {
    stillflow_core::ExportRequestV1 {
        export_id: Uuid::new_v4(),
        format,
        shape: ExportShape::SingleFile,
        destination: ExportDestinationV1::Local {
            root: root.to_str().expect("UTF-8 export root").to_owned(),
            components: vec![format!("h1-output.{extension}")],
        },
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    use std::fmt::Write;
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_api_input_format_matrix_covers_tsv_and_json() {
    let cases: [(&str, &[u8]); 2] = [
        (
            "rows.tsv",
            b"id\tlabel\tignored\n1\talpha\tx\n2\tbeta\ty\n3\tgamma\tz\n",
        ),
        (
            "rows.json",
            br#"[{"id":1,"label":"alpha","ignored":"x"},{"id":2,"label":"beta","ignored":"y"},{"id":3,"label":"gamma","ignored":"z"}]"#,
        ),
    ];

    for (file_name, bytes) in cases {
        let stack = GateStack::local_stack().await;
        std::fs::write(stack.fixture_dir.path().join(file_name), bytes).expect("input fixture");
        let connection_id = stack.register_local(file_name);
        let asset = stack.discover_one(connection_id).await;
        let materialized = materialize_named(&stack, connection_id, &asset).await;
        assert!(matches!(
            materialized.outputs[0],
            TerminalOutputRef::Snapshot {
                committed: true,
                ..
            }
        ));
        stack.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_api_export_format_matrix_recomputes_file_and_set_digests() {
    let stack = GateStack::local_stack().await;
    write_csv(stack.fixture_dir.path(), "rows.csv");
    let connection_id = stack.register_local("csv");
    let asset = stack.discover_one(connection_id).await;
    let projection = inspect_projection(&stack, connection_id, asset.id).await;
    let (plan_id, version_id) = stack.save_plan(asset.id, projection, 1);
    stack.create_dataset(asset.id);
    let materialized = stack
        .submit_and_wait(
            materialize_op(stack.workspace_id, connection_id, asset.id),
            plan_id,
            version_id,
            "h1-export-formats-materialize",
        )
        .await;
    let snapshot = snapshot_ref_of(&materialized);

    for (format, extension) in [
        (ExportFormat::Csv, "csv"),
        (ExportFormat::Tsv, "tsv"),
        (ExportFormat::Jsonl, "jsonl"),
        (ExportFormat::Parquet, "parquet"),
    ] {
        let export_root = tempfile::tempdir().expect("export root");
        let export_request = h1_export_request(export_root.path(), format, extension);
        let export_id = export_request.export_id;
        let job_id = Uuid::new_v4();
        let submitted = stack
            .service
            .submit_export(ApiRequest {
                meta: RequestMetadata {
                    idempotency_key: Some(format!("h1-export-{extension}")),
                    ..meta(stack.workspace_id)
                },
                body: SubmitExportRequest {
                    session_id: stack.session_id,
                    plan_version_id: version_id,
                    plan_id: Some(plan_id),
                    job_id,
                    snapshot: snapshot.clone(),
                    export_request,
                    execution_policy: serde_json::json!({"deadlineSeconds": 300}),
                    output_policy: serde_json::json!({}),
                    queued_at: at(20),
                    event_id: Uuid::new_v4(),
                    correlation_id: format!("h1-export-{extension}"),
                    actor_ref: "actor:h1".to_owned(),
                },
            })
            .expect("submit Export format")
            .body;
        assert_eq!(submitted.id, job_id);
        let job = stack.wait_terminal(job_id).await;
        assert_eq!(job.state, JobState::Succeeded, "failure: {:?}", job.failure);

        let manifest = stack
            .service
            .read_export_manifest(ApiRequest {
                meta: meta(stack.workspace_id),
                body: ObjectIdRequest {
                    object_id: export_id,
                },
            })
            .expect("read Export Manifest")
            .body;
        assert_eq!(manifest.format, format);
        assert_eq!(manifest.input.snapshot_id, snapshot.snapshot_id);
        assert_eq!(manifest.row_count, 3);
        assert_eq!(manifest.files.len(), 1);
        let file = &manifest.files[0];
        let bytes = std::fs::read(export_root.path().join(&file.name)).expect("published bytes");
        assert_eq!(file.byte_count, bytes.len() as u64);
        assert_eq!(manifest.byte_count, file.byte_count);
        assert_eq!(file.digest, sha256_hex(&bytes));
        assert_eq!(
            manifest.set_digest,
            stillflow_storage::compute_export_set_digest([file.digest.as_str()])
        );
        assert!(!bytes.is_empty(), "{extension} export is non-empty");
    }
    stack.shutdown().await;
}
