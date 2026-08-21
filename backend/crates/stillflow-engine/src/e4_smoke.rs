//! Experimental E4 vertical-slice smoke tests.
//!
//! These tests probe CSV → Validate → Deduplicate → VerificationBundle →
//! Snapshot → CSV Export. They are not a merge gate and do not map every
//! Issue #54 acceptance criterion.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use chrono::DateTime;
use futures::stream;
use sha2::{Digest, Sha256};
use stillflow_connectors::{
    ConnectorCapabilities, ConnectorRegistry, RawBatchStream, SourceConnector, SourceConnectorRef,
};
use stillflow_core::{
    AssetKind, AssetLocator, BatchEnvelopeFactory, ColumnId, ConnectionStatus, ConnectorKind,
    ConnectorResult, CredentialRef, DiscoverRequest, Expr, InputRef, InspectRequest, LogicalField,
    LogicalInputRef, LogicalSchema, LogicalType, PreviewData, PreviewRequest, ReadRequest,
    ScalarValue, SourceAsset, SourceConnection, TestConnectionRequest,
};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule, ValidationSeverity};
use stillflow_storage::{ArtifactSectionId, SnapshotStore, StorageError, StorageLimits};
use uuid::Uuid;

use crate::error::EngineError;
use crate::{
    export_snapshot_to_csv, ExecutionEngine, ExecutionRequest, VerificationIdentities,
    VerificationRequest, ENGINE_MAX_DEADLINE,
};

const SENTINEL: &str = "STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a";

struct ScriptedConnector {
    schema: LogicalSchema,
    envelopes: Mutex<Vec<stillflow_core::BatchEnvelope>>,
}

#[async_trait]
impl SourceConnector for ScriptedConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::LocalFile
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            schema_discovery: true,
            preview: true,
            streaming: true,
            column_projection: true,
            ..ConnectorCapabilities::default()
        }
    }

    async fn test_connection(
        &self,
        _connection: &SourceConnection,
        request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus> {
        request.context.ensure_active()?;
        Ok(ConnectionStatus::Ok)
    }

    async fn discover(
        &self,
        _connection: &SourceConnection,
        request: DiscoverRequest,
    ) -> ConnectorResult<Vec<SourceAsset>> {
        request.context.ensure_active()?;
        Ok(Vec::new())
    }

    async fn inspect(
        &self,
        _connection: &SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<stillflow_core::AssetMetadata> {
        request.context.ensure_active()?;
        Ok(stillflow_core::AssetMetadata::new(
            self.schema.clone(),
            "fixture",
        ))
    }

    async fn preview(
        &self,
        _connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        request.context.ensure_active()?;
        Ok(PreviewData::empty(self.schema.clone()))
    }

    async fn read_batches(
        &self,
        _connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<RawBatchStream> {
        request.context.ensure_active()?;
        let envelopes = self.envelopes.lock().expect("fixture lock").clone();
        Ok(RawBatchStream::new(Box::pin(stream::iter(
            envelopes.into_iter().map(Ok),
        ))))
    }

    async fn checkpoint(
        &self,
        _connection: &SourceConnection,
        request: stillflow_core::CheckpointRequest,
    ) -> ConnectorResult<Option<stillflow_core::Checkpoint>> {
        request.context.ensure_active()?;
        Ok(None)
    }
}

fn column(id: u128) -> ColumnId {
    ColumnId::from_uuid(Uuid::from_u128(id))
}

fn int_schema() -> (LogicalSchema, ColumnId) {
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "value",
        LogicalType::Int64,
        false,
    )
    .expect("field")])
    .expect("schema");
    (schema, id)
}

fn utf8_schema() -> (LogicalSchema, ColumnId) {
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "text",
        LogicalType::Utf8,
        false,
    )
    .expect("field")])
    .expect("schema");
    (schema, id)
}

fn connection() -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "fixture",
        serde_json::json!({ "root": "/data/fixture" }),
        CredentialRef::new("cred://local/fixture").expect("cred"),
    )
    .expect("connection")
}

fn asset(connection_id: Uuid) -> SourceAsset {
    SourceAsset {
        id: Uuid::from_u128(42),
        connection_id,
        kind: AssetKind::File,
        name: "values.csv".to_owned(),
        locator: AssetLocator {
            path: "/values.csv".to_owned(),
            container: None,
            schema: None,
            sheet: None,
            workbook_region: None,
        },
        discovered_at: DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
    }
}

fn long_context() -> stillflow_core::RequestContext {
    stillflow_core::RequestContext::with_cancellation_and_deadline(
        stillflow_core::RequestContext::default()
            .cancellation()
            .clone(),
        tokio::time::Instant::now() + ENGINE_MAX_DEADLINE,
    )
}

fn int_values(
    schema: &LogicalSchema,
    asset_id: Uuid,
    sequence: u64,
    values: Vec<i64>,
) -> stillflow_core::BatchEnvelope {
    let array = Int64Array::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
}

fn utf8_values(
    schema: &LogicalSchema,
    asset_id: Uuid,
    sequence: u64,
    values: Vec<String>,
) -> stillflow_core::BatchEnvelope {
    let array = StringArray::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
}

fn gt_zero(id: ColumnId) -> Expr {
    Expr::Binary {
        left: Box::new(Expr::Column(id)),
        operator: stillflow_core::BinaryOperator::GreaterThan,
        right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
    }
}

fn is_present(id: ColumnId) -> Expr {
    Expr::IsNull {
        expression: Box::new(Expr::Column(id)),
        negated: true,
    }
}

fn validate_dedup_plan(
    asset_id: Uuid,
    projection: Vec<ColumnId>,
    key: ColumnId,
    predicate: Expr,
) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let rules = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(3));
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
        rules,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![
                    Rule::Validate {
                        predicate,
                        severity: ValidationSeverity::Error,
                        message: "row must pass validation".to_owned(),
                    },
                    Rule::Deduplicate { keys: vec![key] },
                ],
            },
            vec![scan],
        ),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "accepted".to_owned(),
            },
            vec![rules],
        ),
    );
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn plan_digest(plan: &LogicalPlan) -> [u8; 32] {
    let bytes = plan.canonical_bytes().expect("canonical plan bytes");
    Sha256::digest(bytes).into()
}

fn identities_for(plan: &LogicalPlan, asset_id: Uuid, base: u128) -> VerificationIdentities {
    let at = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
    VerificationIdentities {
        run_id: Uuid::from_u128(base),
        bundle_id: Uuid::from_u128(base + 1),
        bundle_artifact_id: Uuid::from_u128(base + 2),
        snapshot_id: Uuid::from_u128(base + 3),
        dataset_id: Uuid::from_u128(base + 4),
        validation_report_artifact_id: Uuid::from_u128(base + 5),
        rejected_rows_artifact_id: Some(Uuid::from_u128(base + 6)),
        deduplication_report_artifact_id: Uuid::from_u128(base + 7),
        session_id: Uuid::from_u128(base + 8),
        logical_input: LogicalInputRef {
            input: InputRef::Asset { asset_id },
            version_digest: [0x11; 32],
        },
        canonical_plan_digest: plan_digest(plan),
        created_at: at,
        started_at: at,
        committed_at: at,
        lineage: Default::default(),
        quality_score: None,
    }
}

async fn engine_with(
    schema: LogicalSchema,
    envelopes: Vec<stillflow_core::BatchEnvelope>,
) -> ExecutionEngine {
    let connector = Arc::new(ScriptedConnector {
        schema,
        envelopes: Mutex::new(envelopes),
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(connector as SourceConnectorRef)
        .expect("register");
    ExecutionEngine::new(registry)
}

fn open_store(dir: &tempfile::TempDir, limits: StorageLimits) -> SnapshotStore {
    SnapshotStore::open(dir.path(), limits).expect("store")
}

fn default_store(dir: &tempfile::TempDir) -> SnapshotStore {
    open_store(dir, StorageLimits::default())
}

fn export_csv(store: &SnapshotStore, snapshot_id: Uuid) -> String {
    let mut out = Vec::new();
    export_snapshot_to_csv(store, snapshot_id, &mut out)
        .unwrap_or_else(|error| panic!("export failed: {error}"));
    String::from_utf8(out).expect("utf8 csv")
}

fn assert_absent_bundle(store: &SnapshotStore, identities: &VerificationIdentities) {
    assert!(matches!(
        store.load_verification_bundle(identities.bundle_id),
        Err(StorageError::NotFound(_))
    ));
    assert!(matches!(
        store.load_verification_bundle_by_snapshot(identities.snapshot_id),
        Err(StorageError::NotFound(_))
    ));
    assert!(matches!(
        store.load_verification_bundle_by_run_id(identities.run_id),
        Err(StorageError::NotFound(_))
    ));
    assert!(matches!(
        store.load_manifest(identities.snapshot_id),
        Err(StorageError::NotFound(_))
    ));
}

fn assert_no_sentinel(error: &EngineError) {
    let display = error.to_string();
    let debug = format!("{error:?}");
    let summary = serde_json::to_string(&error.sanitized_summary()).expect("summary json");
    assert!(
        !display.contains(SENTINEL),
        "Display leaked sentinel: {display}"
    );
    assert!(!debug.contains(SENTINEL), "Debug leaked sentinel: {debug}");
    assert!(
        !summary.contains(SENTINEL),
        "sanitized_summary leaked sentinel: {summary}"
    );
}

fn section_rows(
    bundle: &stillflow_storage::VerificationBundle,
    artifact_id: Uuid,
    section: ArtifactSectionId,
) -> u64 {
    let manifest = if artifact_id == bundle.validation_report.manifest.artifact_id {
        &bundle.validation_report.manifest
    } else if artifact_id == bundle.deduplication_report.manifest.artifact_id {
        &bundle.deduplication_report.manifest
    } else if bundle
        .rejected_rows
        .as_ref()
        .is_some_and(|rejected| rejected.manifest.artifact_id == artifact_id)
    {
        &bundle.rejected_rows.as_ref().expect("rejected").manifest
    } else {
        panic!("artifact {artifact_id} is not in the committed bundle");
    };
    manifest
        .sections
        .iter()
        .find(|item| item.section_id == section)
        .map(|item| item.stats.row_count)
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
async fn run_verification(
    engine: &ExecutionEngine,
    plan: LogicalPlan,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
    identities: VerificationIdentities,
    store: &SnapshotStore,
    batch_size: usize,
    context: stillflow_core::RequestContext,
) -> Result<stillflow_storage::VerificationBundle, EngineError> {
    engine
        .materialize_verification(VerificationRequest {
            plan,
            connection,
            asset,
            schema_override: Some(schema),
            identities,
            context,
            batch_size,
            store,
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn e4_happy_scripted_loop_exports_accepted_csv() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![1, 2, 3])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 200);
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 3);
    assert!(bundle.rejected_rows.is_none());
    assert_eq!(
        section_rows(
            &bundle,
            bundle.validation_report.manifest.artifact_id,
            ArtifactSectionId::ValidationFinding
        ),
        0
    );
    let csv = export_csv(&store, bundle.membership.accepted_snapshot_id);
    assert_eq!(csv, "value\n1\n2\n3\n");
}

#[tokio::test(flavor = "current_thread")]
async fn e4_invalid_rows_split_accepted_and_rejected() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![1, -1, 2])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 400);
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 2);
    let rejected = bundle.rejected_rows.as_ref().expect("rejected artifact");
    assert_eq!(
        rejected
            .manifest
            .sections
            .iter()
            .find(|section| section.section_id == ArtifactSectionId::RejectedRows)
            .expect("rejected section")
            .stats
            .row_count,
        1
    );
    assert_eq!(
        section_rows(
            &bundle,
            bundle.validation_report.manifest.artifact_id,
            ArtifactSectionId::ValidationFinding
        ),
        1
    );
    let csv = export_csv(&store, bundle.membership.accepted_snapshot_id);
    assert_eq!(csv, "value\n1\n2\n");
}

#[tokio::test(flavor = "current_thread")]
async fn e4_dedup_keeps_first_across_two_batches() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![
            int_values(&schema, source.id, 0, vec![1, 2]),
            int_values(&schema, source.id, 1, vec![1, 3]),
        ],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 500);
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 3);
    let rejected = bundle.rejected_rows.as_ref().expect("duplicate rejected");
    assert_eq!(
        rejected
            .manifest
            .sections
            .iter()
            .find(|section| section.section_id == ArtifactSectionId::RejectedRows)
            .expect("rejected section")
            .stats
            .row_count,
        1
    );
    assert_eq!(
        section_rows(
            &bundle,
            bundle.deduplication_report.manifest.artifact_id,
            ArtifactSectionId::DuplicateFinding
        ),
        1
    );
    let csv = export_csv(&store, bundle.membership.accepted_snapshot_id);
    assert_eq!(csv, "value\n1\n2\n3\n");
}

#[tokio::test(flavor = "current_thread")]
async fn e4_empty_stream_is_zero_row_accepted_without_rejected_artifact() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(schema.clone(), Vec::new()).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 600);
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 0);
    assert_eq!(
        bundle
            .accepted
            .manifest
            .snapshot()
            .stats()
            .partition_count(),
        0
    );
    assert!(bundle.rejected_rows.is_none());
    assert_eq!(
        section_rows(
            &bundle,
            bundle.validation_report.manifest.artifact_id,
            ArtifactSectionId::ValidationRuleSummary
        ),
        1
    );
    let csv = export_csv(&store, bundle.membership.accepted_snapshot_id);
    assert_eq!(csv, "value\n");
}

#[tokio::test(flavor = "current_thread")]
async fn e4_all_rows_rejected_keeps_empty_accepted_snapshot() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![-1, -2])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 700);
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 0);
    assert!(bundle.rejected_rows.is_some());
    assert_eq!(
        bundle
            .rejected_rows
            .as_ref()
            .expect("rejected")
            .manifest
            .sections
            .iter()
            .find(|section| section.section_id == ArtifactSectionId::RejectedRows)
            .expect("section")
            .stats
            .row_count,
        2
    );
    let csv = export_csv(&store, bundle.membership.accepted_snapshot_id);
    assert_eq!(csv, "value\n");
}

#[tokio::test(flavor = "current_thread")]
async fn e4_zero_rejections_does_not_publish_empty_rejected_rows() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![4, 5])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 800);
    let rejected_id = identities.rejected_rows_artifact_id.expect("injected");
    let bundle = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect("verification");
    assert!(bundle.rejected_rows.is_none());
    assert!(bundle.membership.rejected_rows_artifact_id.is_none());
    assert!(matches!(
        store.open_artifact_section(
            bundle.membership.bundle_id,
            rejected_id,
            ArtifactSectionId::RejectedRows
        ),
        Err(StorageError::NotFound(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn e4_cancel_before_write_publishes_nothing() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![1, 2])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 900);
    let context = long_context();
    context.cancellation().cancel();
    let error = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities.clone(),
        &store,
        64,
        context,
    )
    .await
    .expect_err("cancelled");
    assert!(matches!(error, EngineError::Cancelled));
    assert_absent_bundle(&store, &identities);
}

#[tokio::test(flavor = "current_thread")]
async fn e4_fail_during_write_does_not_leave_a_visible_bundle() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(
        schema.clone(),
        vec![int_values(&schema, source.id, 0, vec![1, 2])],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let limits = StorageLimits::try_new(8, 1, 100, 1_000_000, 4, 4).expect("limits");
    let store = open_store(&dir, limits);
    let identities = identities_for(&plan, source.id, 1000);
    let error = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities.clone(),
        &store,
        1,
        long_context(),
    )
    .await
    .expect_err("partition limit");
    assert!(matches!(
        error,
        EngineError::Storage(StorageError::PartitionLimitExceeded { .. })
    ));
    assert_no_sentinel(&error);
    assert_absent_bundle(&store, &identities);
}

#[tokio::test(flavor = "current_thread")]
async fn e4_repeat_identities_conflict_and_same_input_is_deterministic() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let envelopes = vec![int_values(&schema, source.id, 0, vec![1, 2, 1])];
    let engine = engine_with(schema.clone(), envelopes.clone()).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let identities = identities_for(&plan, source.id, 1100);
    let first = run_verification(
        &engine,
        plan.clone(),
        connection.clone(),
        source.clone(),
        schema.clone(),
        identities.clone(),
        &store,
        64,
        long_context(),
    )
    .await
    .expect("first run");
    let csv_first = export_csv(&store, first.membership.accepted_snapshot_id);
    let repeat = run_verification(
        &engine,
        plan.clone(),
        connection.clone(),
        source.clone(),
        schema.clone(),
        identities,
        &store,
        64,
        long_context(),
    )
    .await
    .expect_err("identity conflict");
    assert!(matches!(
        repeat,
        EngineError::Storage(StorageError::AlreadyExists(_))
    ));

    let engine2 = engine_with(schema.clone(), envelopes).await;
    let dir2 = tempfile::TempDir::new().expect("temp2");
    let store2 = default_store(&dir2);
    let identities2 = identities_for(&plan, source.id, 1200);
    let second = run_verification(
        &engine2,
        plan,
        connection,
        source,
        schema,
        identities2,
        &store2,
        64,
        long_context(),
    )
    .await
    .expect("second run");
    let csv_second = export_csv(&store2, second.membership.accepted_snapshot_id);
    assert_eq!(csv_first, csv_second);
    assert_eq!(csv_first, "value\n1\n2\n");
    assert_eq!(
        first.accepted.manifest.snapshot().stats().row_count(),
        second.accepted.manifest.snapshot().stats().row_count()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn e4_sentinel_does_not_leak_through_errors_or_debug() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, is_present(id));
    let engine = engine_with(
        schema.clone(),
        vec![utf8_values(
            &schema,
            source.id,
            0,
            vec![SENTINEL.to_owned(), "ok".to_owned()],
        )],
    )
    .await;
    let dir = tempfile::TempDir::new().expect("temp");
    let limits = StorageLimits::try_new(8, 1, 100, 1_000_000, 4, 4).expect("limits");
    let store = open_store(&dir, limits);
    let identities = identities_for(&plan, source.id, 1300);
    let error = run_verification(
        &engine,
        plan,
        connection,
        source,
        schema,
        identities,
        &store,
        1,
        long_context(),
    )
    .await
    .expect_err("write failure with sentinel payload");
    assert_no_sentinel(&error);
}

#[tokio::test(flavor = "current_thread")]
async fn e4_materialize_path_still_rejects_validate_and_dedup() {
    let _guard = crate::exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let plan = validate_dedup_plan(source.id, vec![id], id, gt_zero(id));
    let engine = engine_with(schema.clone(), Vec::new()).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = default_store(&dir);
    let now = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
    let error = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: crate::ExecutionIdentities {
                snapshot_id: Uuid::from_u128(100),
                dataset_id: Uuid::from_u128(101),
                session_id: Uuid::from_u128(102),
                created_at: now,
                started_at: now,
                lineage: Default::default(),
                quality_score: None,
            },
            context: long_context(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("materialize must stay on the E2 surface");
    assert!(matches!(
        error,
        EngineError::UnsupportedRule {
            kind: "validate",
            ..
        }
    ));
}
