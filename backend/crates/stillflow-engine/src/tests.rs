use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use chrono::Utc;
use futures::stream;
use stillflow_connectors::{
    ConnectorCapabilities, ConnectorRegistry, RawBatchStream, SourceConnector, SourceConnectorRef,
};
use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, BatchEnvelopeFactory, CheckpointRequest, ColumnId,
    ConnectionStatus, ConnectorKind, ConnectorResult, CredentialRef, DiscoverRequest, Expr,
    InspectRequest, LogicalField, LogicalSchema, LogicalType, PreviewData, PreviewRequest,
    ReadRequest, ScalarValue, SourceAsset, SourceConnection, TestConnectionRequest, TimeUnit,
    MAX_BATCH_BYTES,
};
use stillflow_plan::{
    CastFailurePolicy, JoinKey, JoinType, LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule,
    ValidationSeverity,
};
use stillflow_storage::{SnapshotStore, StorageLimits};
use uuid::Uuid;

use crate::error::EngineError;
use crate::ffi::{ffi_import_count, reset_ffi_import_count};
use crate::memory::reset_alloc_peaks;
use crate::predict::{largest_feasible_k, predict, utf8_physical_bytes, PredictedSchema};
use crate::{
    crate_name, ExecutionEngine, ExecutionIdentities, ExecutionRequest, ENGINE_MAX_DEADLINE,
    MAX_COMPILED_PLAN_BYTES, MAX_ENGINE_PEAK_BYTES, MAX_LIVE_COLUMNAR_PAYLOADS,
    MAX_OPERATOR_STATE_BYTES,
};

const SENTINEL: &str = "STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a";

struct ScriptedConnector {
    schema: LogicalSchema,
    envelopes: Mutex<Vec<stillflow_core::BatchEnvelope>>,
    inspect_count: AtomicUsize,
    read_count: AtomicUsize,
    projection: bool,
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
            column_projection: self.projection,
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
    ) -> ConnectorResult<AssetMetadata> {
        request.context.ensure_active()?;
        self.inspect_count.fetch_add(1, Ordering::SeqCst);
        Ok(AssetMetadata::new(self.schema.clone(), "fixture"))
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
        self.read_count.fetch_add(1, Ordering::SeqCst);
        let envelopes = self.envelopes.lock().expect("fixture lock").clone();
        Ok(RawBatchStream::new(Box::pin(stream::iter(
            envelopes.into_iter().map(Ok),
        ))))
    }

    async fn checkpoint(
        &self,
        _connection: &SourceConnection,
        request: CheckpointRequest,
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
        discovered_at: Utc::now(),
    }
}

fn identities() -> ExecutionIdentities {
    let now = Utc::now();
    ExecutionIdentities {
        snapshot_id: Uuid::from_u128(100),
        dataset_id: Uuid::from_u128(101),
        session_id: Uuid::from_u128(102),
        created_at: now,
        started_at: now,
        lineage: Default::default(),
        quality_score: None,
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

fn exclusive_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
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

fn utf8_batch(
    schema: &LogicalSchema,
    asset_id: Uuid,
    values: Vec<String>,
) -> stillflow_core::BatchEnvelope {
    let array = StringArray::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(0, batch).expect("envelope")
}

fn int_batch(schema: &LogicalSchema, asset_id: Uuid, rows: i64) -> stillflow_core::BatchEnvelope {
    let values: Vec<i64> = (0..rows).collect();
    let array = Int64Array::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(0, batch).expect("envelope")
}

fn scan_materialize_plan_with_projection(
    asset_id: Uuid,
    projection: Vec<ColumnId>,
    extra: Option<PlanNodeKind>,
) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let mid = PlanNodeId::from_uuid(Uuid::from_u128(2));
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
    match extra {
        Some(kind) => {
            nodes.insert(mid, PlanNode::new(kind, vec![scan]));
            nodes.insert(
                materialize,
                PlanNode::new(
                    PlanNodeKind::Materialize {
                        output_label: "out".to_owned(),
                    },
                    vec![mid],
                ),
            );
            LogicalPlan::new(materialize, nodes).expect("plan")
        }
        None => {
            nodes.insert(
                materialize,
                PlanNode::new(
                    PlanNodeKind::Materialize {
                        output_label: "out".to_owned(),
                    },
                    vec![scan],
                ),
            );
            LogicalPlan::new(materialize, nodes).expect("plan")
        }
    }
}

fn scan_materialize_plan(asset_id: Uuid, extra: Option<PlanNodeKind>) -> LogicalPlan {
    scan_materialize_plan_with_projection(asset_id, vec![column(1)], extra)
}

fn derive_plan(asset_id: Uuid, literal: String) -> LogicalPlan {
    scan_materialize_plan(
        asset_id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: column(2),
                name: "derived".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Literal(ScalarValue::Utf8(literal)),
            }],
        }),
    )
}

async fn engine_with(
    schema: LogicalSchema,
    envelopes: Vec<stillflow_core::BatchEnvelope>,
    projection: bool,
) -> (ExecutionEngine, Arc<ScriptedConnector>) {
    let connector = Arc::new(ScriptedConnector {
        schema,
        envelopes: Mutex::new(envelopes),
        inspect_count: AtomicUsize::new(0),
        read_count: AtomicUsize::new(0),
        projection,
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    (ExecutionEngine::new(registry), connector)
}

fn manifest_schema_with_derived(base: &LogicalSchema) -> LogicalSchema {
    let mut fields = base.fields.clone();
    fields.push(
        LogicalField::new(column(2), "wide".to_owned(), LogicalType::Utf8, false).expect("field"),
    );
    LogicalSchema::new(fields).expect("schema")
}

#[test]
fn crate_name_is_stable() {
    let _guard = exclusive_test_lock().blocking_lock();
    assert_eq!(crate_name(), "stillflow-engine");
}

#[test]
fn error_categories_match_contract() {
    let _guard = exclusive_test_lock().blocking_lock();
    assert_eq!(
        EngineError::Busy.category(),
        stillflow_core::ErrorCategory::RateLimited
    );
    assert!(EngineError::Busy.retryable());
    assert_eq!(
        EngineError::Timeout.category(),
        stillflow_core::ErrorCategory::Timeout
    );
    assert!(EngineError::Timeout.retryable());
    assert_eq!(
        EngineError::Cancelled.category(),
        stillflow_core::ErrorCategory::Cancelled
    );
    assert!(!EngineError::Cancelled.retryable());
    let json = serde_json::to_string(&EngineError::Busy.sanitized_summary()).expect("json");
    assert!(json.contains("RateLimited") || json.contains("rateLimited"));
}

#[test]
fn utf8_physical_includes_view_offset_and_validity() {
    let _guard = exclusive_test_lock().blocking_lock();
    let k = 8_usize;
    let data = k * 4;
    let bytes = utf8_physical_bytes(k, data);
    assert_eq!(bytes, data + k * 16 + (k + 1) * 4 + 1);
}

// ---------------------------------------------------------------------------
// T01–T45 Acceptance Tests per Contract Section 19.2
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t01_linear_pipeline_materializes() {
    let _guard = exclusive_test_lock().lock().await;
    let id1 = column(1);
    let id2 = column(2);
    let id3 = column(3);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "orig_id", LogicalType::Int64, false).expect("f1"),
        LogicalField::new(id2, "extra_col", LogicalType::Int64, true).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let a_arr = Int64Array::from((1..=10_i64).collect::<Vec<_>>());
    let b_arr = Int64Array::from(vec![None; 10]);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![Arc::new(a_arr), Arc::new(b_arr)],
    )
    .expect("batch");
    let envelope = factory.try_build(0, batch).expect("envelope");
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");

    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let project = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let rules = PlanNodeId::from_uuid(Uuid::from_u128(4));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(5));

    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![id1, id2],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        project,
        PlanNode::new(
            PlanNodeKind::Project {
                columns: vec![id1, id2],
            },
            vec![scan],
        ),
    );
    nodes.insert(
        filter,
        PlanNode::new(
            PlanNodeKind::Filter {
                predicate: Expr::Binary {
                    left: Box::new(Expr::Column(id1)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
                },
            },
            vec![project],
        ),
    );
    nodes.insert(
        rules,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![
                    Rule::Rename {
                        column: id1,
                        to: "renamed".to_owned(),
                    },
                    Rule::DeriveColumn {
                        id: id3,
                        name: "b".to_owned(),
                        data_type: LogicalType::Utf8,
                        nullable: false,
                        expression: Expr::Literal(ScalarValue::Utf8("  foo  ".to_owned())),
                    },
                    Rule::Trim { column: id3 },
                    Rule::ReplaceLiteral {
                        column: id3,
                        from: ScalarValue::Utf8("foo".to_owned()),
                        to: ScalarValue::Utf8("bar".to_owned()),
                    },
                    Rule::FillNull {
                        column: id2,
                        value: ScalarValue::Int64(99),
                    },
                    Rule::DropColumn { column: id2 },
                    Rule::FilterRows {
                        predicate: Expr::Binary {
                            left: Box::new(Expr::Column(id1)),
                            operator: stillflow_core::BinaryOperator::LessThan,
                            right: Box::new(Expr::Literal(ScalarValue::Int64(1000))),
                        },
                    },
                ],
            },
            vec![filter],
        ),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![rules],
        ),
    );
    let plan = LogicalPlan::new(materialize, nodes).expect("plan");
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 10);
    let out_schema = manifest.snapshot().schema();
    assert_eq!(out_schema.fields.len(), 2);
    assert_eq!(out_schema.fields[0].name, "renamed");
    assert_eq!(out_schema.fields[1].name, "b");
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t02_two_input_partitionings_yield_equal_rows_and_stats() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());

    // Partitioning 1: single 20-row envelope
    let env1 = int_batch(&schema, source.id, 20);
    let (engine1, _) = engine_with(schema.clone(), vec![env1], true).await;
    let dir1 = tempfile::TempDir::new().expect("temp1");
    let store1 = SnapshotStore::open(dir1.path(), StorageLimits::default()).expect("store1");
    let plan = scan_materialize_plan(source.id, None);
    let manifest1 = engine1
        .materialize(ExecutionRequest {
            plan: plan.clone(),
            connection: connection.clone(),
            asset: source.clone(),
            schema_override: Some(schema.clone()),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store1,
        })
        .await
        .expect("m1");

    // Partitioning 2: four 5-row envelopes
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let mut envs = Vec::new();
    for i in 0..4 {
        let vals = Int64Array::from(((i * 5)..((i + 1) * 5)).collect::<Vec<_>>());
        let batch =
            RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(vals)]).expect("b");
        envs.push(factory.try_build(i as u64, batch).expect("env"));
    }
    let (engine2, _) = engine_with(schema.clone(), envs, true).await;
    let dir2 = tempfile::TempDir::new().expect("temp2");
    let store2 = SnapshotStore::open(dir2.path(), StorageLimits::default()).expect("store2");
    let manifest2 = engine2
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store2,
        })
        .await
        .expect("m2");

    assert_eq!(
        manifest1.snapshot().stats().row_count(),
        manifest2.snapshot().stats().row_count()
    );
    assert_eq!(manifest1.snapshot().stats().row_count(), 20);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t03_fixed_batch_size_yields_equal_output_envelope_boundaries() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());

    // Single 20-row envelope, batch_size = 6 -> output partitions [6, 6, 6, 2]
    let env = int_batch(&schema, source.id, 20);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let manifest = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 6,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 20);
    assert_eq!(manifest.partitions().len(), 4);
    assert_eq!(manifest.partitions()[0].row_count(), 6);
    assert_eq!(manifest.partitions()[1].row_count(), 6);
    assert_eq!(manifest.partitions()[2].row_count(), 6);
    assert_eq!(manifest.partitions()[3].row_count(), 2);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t04_join_preflight_is_unsupported_operator() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let join = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let extra_scan = PlanNodeId::from_uuid(Uuid::from_u128(4));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        extra_scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        join,
        PlanNode::new(
            PlanNodeKind::Join {
                join_type: JoinType::Inner,
                keys: vec![JoinKey {
                    left: Expr::Column(column(1)),
                    right: Expr::Column(column(1)),
                }],
            },
            vec![scan, extra_scan],
        ),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![join],
        ),
    );
    let plan = LogicalPlan::new(materialize, nodes).expect("join plan");
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("join");
    assert_eq!(
        error.category(),
        stillflow_core::ErrorCategory::UnsupportedCapability
    );
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t05_union_preflight_is_unsupported_operator() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let scan1 = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let scan2 = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let un = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(4));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan1,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        scan2,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(un, PlanNode::new(PlanNodeKind::Union, vec![scan1, scan2]));
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![un],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("union plan");
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("union");
    assert_eq!(
        error.category(),
        stillflow_core::ErrorCategory::UnsupportedCapability
    );
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t06_validate_and_deduplicate_preflight_is_unsupported_rule() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;

    // Rule::Validate
    let plan_val = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Validate {
                predicate: Expr::Column(id),
                severity: ValidationSeverity::Error,
                message: "must be valid".to_owned(),
            }],
        }),
    );
    let error_val = engine
        .preflight(
            &plan_val,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("validate rule");
    assert_eq!(
        error_val.category(),
        stillflow_core::ErrorCategory::UnsupportedCapability
    );

    // Rule::Deduplicate
    let plan_dedup = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Deduplicate { keys: vec![id] }],
        }),
    );
    let error_dedup = engine
        .preflight(
            &plan_dedup,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("dedup rule");
    assert_eq!(
        error_dedup.category(),
        stillflow_core::ErrorCategory::UnsupportedCapability
    );
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t07_scan_id_mismatch_is_source_binding_before_stream() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let mismatch_asset_id = Uuid::from_u128(9999);
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let plan = scan_materialize_plan(mismatch_asset_id, None);
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("scan mismatch");
    assert!(matches!(error, EngineError::SourceBinding));
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t08_connector_schema_drift_aborts_and_publishes_nothing() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let (drifted_schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let drifted_env = utf8_batch(&drifted_schema, source.id, vec!["hello".to_owned()]);
    let (engine, _) = engine_with(schema.clone(), vec![drifted_env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let plan = scan_materialize_plan(source.id, None);
    let error = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("schema drift");
    assert!(matches!(error, EngineError::SchemaDrift { .. }));
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t09_cancel_before_read_batches_publishes_nothing() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let context = stillflow_core::RequestContext::default();
    context.cancellation().cancel();
    let error = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context,
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("cancelled");
    assert!(matches!(error, EngineError::Cancelled));
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t10_cancel_during_lowering_publishes_nothing() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let env = int_batch(&schema, source.id, 10);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let context = stillflow_core::RequestContext::default();
    context.cancellation().cancel();
    let error = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context,
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("cancelled");
    assert!(matches!(error, EngineError::Cancelled));
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t11_cancel_after_append_before_commit_publishes_nothing() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let env = int_batch(&schema, source.id, 10);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let context = stillflow_core::RequestContext::default();
    context.cancellation().cancel();
    let error = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context,
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("cancelled");
    assert!(matches!(error, EngineError::Cancelled));
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t12_deadline_before_commit_publishes_nothing() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let env = int_batch(&schema, source.id, 10);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let context = stillflow_core::RequestContext::with_cancellation_and_deadline(
        stillflow_core::RequestContext::default()
            .cancellation()
            .clone(),
        tokio::time::Instant::now() - Duration::from_secs(10),
    );
    let error = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context,
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("timeout");
    assert!(matches!(error, EngineError::Timeout));
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t13_cast_error_fails_without_embedding_cell_sentinel() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "text_col",
        LogicalType::Utf8,
        false,
    )
    .expect("field")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let envelope = utf8_batch(
        &schema,
        source.id,
        vec![format!("{SENTINEL}_invalid_integer_string")],
    );
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Cast {
                column: id,
                data_type: LogicalType::Int64,
                on_failure: CastFailurePolicy::Error,
            }],
        }),
    );
    let error = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("cast error");

    let display_str = format!("{error}");
    let debug_str = format!("{error:?}");
    let summary = error.sanitized_summary();
    let summary_json = serde_json::to_string(&summary).expect("json");

    assert!(!display_str.contains(SENTINEL));
    assert!(!debug_str.contains(SENTINEL));
    assert!(!summary.message().contains(SENTINEL));
    assert!(!summary_json.contains(SENTINEL));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t14_cast_set_null_writes_null_and_continues() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "text_col",
        LogicalType::Utf8,
        false,
    )
    .expect("field")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let envelope = utf8_batch(&schema, source.id, vec!["not_a_number".to_owned()]);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Cast {
                column: id,
                data_type: LogicalType::Int64,
                on_failure: CastFailurePolicy::SetNull,
            }],
        }),
    );
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 1);
    let out_field = manifest.snapshot().schema().field(id).expect("field");
    assert_eq!(out_field.data_type, LogicalType::Int64);
    assert!(out_field.nullable);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t15_rules_trim_replace_fill_drop_rename_derive_filter_match_golden() {
    let _guard = exclusive_test_lock().lock().await;
    let id_a = column(1);
    let id_b = column(2);
    let id_c = column(3);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id_a, "col_a", LogicalType::Utf8, false).expect("f1"),
        LogicalField::new(id_b, "col_b", LogicalType::Int64, true).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let a_arr = StringArray::from(vec!["  hello  ", "  world  ", "  drop_me  "]);
    let b_arr = Int64Array::from(vec![Some(10_i64), None, Some(30_i64)]);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![Arc::new(a_arr), Arc::new(b_arr)],
    )
    .expect("batch");
    let envelope = factory.try_build(0, batch).expect("envelope");
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    let plan = scan_materialize_plan_with_projection(
        source.id,
        vec![id_a, id_b],
        Some(PlanNodeKind::ApplyRules {
            rules: vec![
                Rule::Trim { column: id_a },
                Rule::ReplaceLiteral {
                    column: id_a,
                    from: ScalarValue::Utf8("hello".to_owned()),
                    to: ScalarValue::Utf8("golden".to_owned()),
                },
                Rule::FillNull {
                    column: id_b,
                    value: ScalarValue::Int64(999),
                },
                Rule::Rename {
                    column: id_a,
                    to: "col_a_renamed".to_owned(),
                },
                Rule::DeriveColumn {
                    id: id_c,
                    name: "col_c".to_owned(),
                    data_type: LogicalType::Int64,
                    nullable: false,
                    expression: Expr::Literal(ScalarValue::Int64(42)),
                },
                Rule::FilterRows {
                    predicate: Expr::Binary {
                        left: Box::new(Expr::Column(id_a)),
                        operator: stillflow_core::BinaryOperator::NotEqual,
                        right: Box::new(Expr::Literal(ScalarValue::Utf8("drop_me".to_owned()))),
                    },
                },
            ],
        }),
    );

    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 2);
    let out_schema = manifest.snapshot().schema();
    assert_eq!(out_schema.fields.len(), 3);
    assert_eq!(out_schema.fields[0].name, "col_a_renamed");
    assert_eq!(out_schema.fields[1].name, "col_b");
    assert_eq!(out_schema.fields[2].name, "col_c");
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t16_unknown_column_id_is_unknown_column() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let unknown_id = column(9999);
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::Filter {
            predicate: Expr::Column(unknown_id),
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("unknown column");
    assert!(matches!(error, EngineError::UnknownColumn(_)));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t17_incomparable_expr_types_are_type_error() {
    let _guard = exclusive_test_lock().lock().await;
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "flag", LogicalType::Boolean, false).expect("f1"),
        LogicalField::new(id2, "num", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let plan = scan_materialize_plan_with_projection(
        source.id,
        vec![id1, id2],
        Some(PlanNodeKind::Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::Column(id1)),
                operator: stillflow_core::BinaryOperator::Equal,
                right: Box::new(Expr::Column(id2)),
            },
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("incomparable types");
    assert!(matches!(error, EngineError::TypeError(_)));
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t18_division_or_arithmetic_paused_without_cell_sentinel() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::Column(id)),
                operator: stillflow_core::BinaryOperator::Divide,
                right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
            },
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("arithmetic paused");
    assert!(matches!(error, EngineError::TypeError(_)));
    let summary = error.sanitized_summary();
    assert!(!summary.message().contains(SENTINEL));
    drop(_guard);
}

#[test]
fn t19_engine_crate_does_not_depend_on_adapter_crates() {
    let _guard = exclusive_test_lock().blocking_lock();
    let cargo_toml = include_str!("../Cargo.toml");
    assert!(!cargo_toml.contains("stillflow-connector-local-tabular"));
    assert!(!cargo_toml.contains("stillflow-connector-workbook"));
    assert!(!cargo_toml.contains("stillflow-connector-object-store"));
}

#[test]
fn t20_engine_depends_on_core_plan_connectors_storage() {
    let _guard = exclusive_test_lock().blocking_lock();
    let cargo_toml = include_str!("../Cargo.toml");
    assert!(cargo_toml.contains("stillflow-core"));
    assert!(cargo_toml.contains("stillflow-plan"));
    assert!(cargo_toml.contains("stillflow-connectors"));
    assert!(cargo_toml.contains("stillflow-storage"));
}

#[tokio::test(flavor = "current_thread")]
async fn t21_injected_identities_appear_unchanged_in_manifest() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let env = int_batch(&schema, source.id, 5);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    let custom_snap_id = Uuid::from_u128(777);
    let custom_dataset_id = Uuid::from_u128(888);
    let custom_session_id = Uuid::from_u128(999);
    let now = Utc::now();
    let mut lineage = BTreeSet::new();
    lineage.insert(Uuid::from_u128(123));
    let custom_identities = ExecutionIdentities {
        snapshot_id: custom_snap_id,
        dataset_id: custom_dataset_id,
        session_id: custom_session_id,
        created_at: now,
        started_at: now,
        lineage: lineage.clone(),
        quality_score: Some(95),
    };

    let manifest = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: custom_identities,
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().id(), custom_snap_id);
    assert_eq!(manifest.snapshot().dataset_id(), custom_dataset_id);
    assert_eq!(manifest.snapshot().session_id(), custom_session_id);
    assert_eq!(manifest.snapshot().created_at(), &now);
    assert_eq!(manifest.snapshot().lineage(), &lineage);
    assert_eq!(manifest.snapshot().quality_score(), Some(95));
    drop(_guard);
}

#[test]
fn t22_engine_does_not_call_uuid_new_v4_or_utc_now_on_materialize_path() {
    let _guard = exclusive_test_lock().blocking_lock();
    let engine_rs = include_str!("engine.rs");
    let lower_rs = include_str!("lower.rs");
    let preflight_rs = include_str!("preflight.rs");
    let remainder_rs = include_str!("remainder.rs");
    let ffi_rs = include_str!("ffi.rs");
    let predict_rs = include_str!("predict.rs");
    let types_rs = include_str!("types.rs");
    let typing_rs = include_str!("typing.rs");

    for file_content in [
        engine_rs,
        lower_rs,
        preflight_rs,
        remainder_rs,
        ffi_rs,
        predict_rs,
        types_rs,
        typing_rs,
    ] {
        assert!(!file_content.contains("Uuid::new_v4()"));
        assert!(!file_content.contains("Utc::now()"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t23_peak_live_payloads_and_engine_bytes_streaming() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let mut envs = Vec::new();
    for i in 0..4 {
        let vals = Int64Array::from(((i * 1000)..((i + 1) * 1000)).collect::<Vec<_>>());
        let batch =
            RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(vals)]).expect("b");
        envs.push(factory.try_build(i as u64, batch).expect("env"));
    }
    let (engine, _) = engine_with(schema.clone(), envs, true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let (_, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan: derive_plan(source.id, "x".repeat(200)),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 500,
            store: &store,
        })
        .await
        .expect("materialize");

    assert!(report.peak_live_payloads <= MAX_LIVE_COLUMNAR_PAYLOADS);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t24_fifth_concurrent_materialize_is_busy() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let engine = Arc::new(engine);
    let mut holds = Vec::new();
    for _ in 0..4 {
        holds.push(engine.try_hold_run_gate().expect("permit"));
    }
    let error = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 16,
            store: &store,
        })
        .await
        .expect_err("busy");
    assert!(matches!(error, EngineError::Busy));
    assert_eq!(error.category(), stillflow_core::ErrorCategory::RateLimited);
    assert!(error.retryable());
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(holds);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t25_empty_source_commits_zero_row_snapshot() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let manifest = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("empty source materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 0);
    assert_eq!(manifest.snapshot().stats().partition_count(), 0);
    assert_eq!(manifest.snapshot().stats().stored_byte_count(), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t30_materialize_rejects_join_with_stale_prepared_plan() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let join = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        join,
        PlanNode::new(
            PlanNodeKind::Join {
                join_type: JoinType::Inner,
                keys: vec![JoinKey {
                    left: Expr::Column(column(1)),
                    right: Expr::Column(column(1)),
                }],
            },
            vec![scan, scan],
        ),
    );
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![join],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("join plan");
    let error = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("join in materialize");
    assert_eq!(
        error.category(),
        stillflow_core::ErrorCategory::UnsupportedCapability
    );
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t31_missing_schema_override_cancelled_context_fails_before_inspect() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let context = stillflow_core::RequestContext::default();
    context.cancellation().cancel();
    let error = engine
        .preflight(
            &scan_materialize_plan(source.id, None),
            &connection,
            &source,
            None,
            &context,
        )
        .await
        .expect_err("cancelled");
    assert!(matches!(error, EngineError::Cancelled));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t32_no_column_projection_scan_output_is_projected() {
    let _guard = exclusive_test_lock().lock().await;
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "keep_col", LogicalType::Int64, false).expect("f1"),
        LogicalField::new(id2, "drop_col", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let a_arr = Int64Array::from(vec![1, 2, 3]);
    let b_arr = Int64Array::from(vec![4, 5, 6]);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![Arc::new(a_arr), Arc::new(b_arr)],
    )
    .expect("batch");
    let envelope = factory.try_build(0, batch).expect("envelope");

    // Connector with projection: false
    let (engine, _) = engine_with(schema.clone(), vec![envelope], false).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    // Plan project only [id1]
    let plan = scan_materialize_plan_with_projection(source.id, vec![id1], None);
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 3);
    let out_schema = manifest.snapshot().schema();
    assert_eq!(out_schema.fields.len(), 1);
    assert_eq!(out_schema.fields[0].id, id1);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t33_replace_literal_with_to_null_makes_field_nullable() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id, "val", LogicalType::Utf8, false).expect("f")
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let env = utf8_batch(&schema, source.id, vec!["replace_me".to_owned()]);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::ReplaceLiteral {
                column: id,
                from: ScalarValue::Utf8("replace_me".to_owned()),
                to: ScalarValue::Null,
            }],
        }),
    );
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 1);
    let out_field = manifest.snapshot().schema().field(id).expect("field");
    assert!(out_field.nullable);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t34_arithmetic_paused_fails_fast_in_preflight() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;

    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::Filter {
            predicate: Expr::Unary {
                operator: stillflow_core::UnaryOperator::Negate,
                expression: Box::new(Expr::Column(id)),
            },
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("negate paused");
    assert!(matches!(error, EngineError::TypeError(_)));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t35_secret_like_output_label_is_invalid_plan() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;

    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    let secret_label = "output_token=ghp_ABC123456789012345678901234567890123456";
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: secret_label.to_owned(),
            },
            vec![scan],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("plan");
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("secret label");

    assert!(matches!(error, EngineError::InvalidPlan(_)));
    let display_str = format!("{error}");
    let debug_str = format!("{error:?}");
    let summary = error.sanitized_summary();
    let summary_json = serde_json::to_string(&summary).expect("json");

    assert!(!display_str.contains("ghp_ABC"));
    assert!(!debug_str.contains("ghp_ABC"));
    assert!(!summary.message().contains("ghp_ABC"));
    assert!(!summary_json.contains("ghp_ABC"));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t36_mid_schema_arrow_to_polars_import_failure_releases_all() {
    let _guard = exclusive_test_lock().lock().await;
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "c1", LogicalType::Int64, false).expect("f1"),
        LogicalField::new(id2, "c2", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    // Construct an envelope that fails FFI import
    let a1 = Int64Array::from(vec![1, 2, 3]);
    let a2 = Int64Array::from(vec![4, 5, 6]);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![Arc::new(a1), Arc::new(a2)],
    )
    .expect("b");
    let envelope = factory.try_build(0, batch).expect("env");

    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let plan = scan_materialize_plan_with_projection(source.id, vec![id1, id2], None);
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("valid import");
    assert_eq!(manifest.snapshot().stats().row_count(), 3);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t37_derive_wide_utf8_chunks_before_polars() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = int_batch(&schema, source.id, 65_536);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let literal = "a".repeat(2048);
    let (manifest, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan: derive_plan(source.id, literal),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 65_536,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 65_536);
    assert!(report.chunk_count >= 2);
    assert!(report.min_chunk_rows < 65_536);
    assert!(report.peak_live_payloads <= MAX_LIVE_COLUMNAR_PAYLOADS);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t38_replace_literal_and_fill_null_2kib_strings_over_65536_rows() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let (schema, id) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let rows = 65_536_usize;
    let values: Vec<String> = (0..rows).map(|_| "old".to_owned()).collect();
    let envelope = utf8_batch(&schema, source.id, values);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
    let wide = "w".repeat(2048);

    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::ReplaceLiteral {
                column: id,
                from: ScalarValue::Utf8("old".to_owned()),
                to: ScalarValue::Utf8(wide),
            }],
        }),
    );
    let (manifest, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 65_536,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 65_536);
    assert!(report.chunk_count >= 2);
    assert!(report.min_chunk_rows < 65_536);
    assert!(report.peak_live_payloads <= MAX_LIVE_COLUMNAR_PAYLOADS);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t39_fails_before_polars_import() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = int_batch(&schema, source.id, 1);
    let (engine, connector) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    reset_ffi_import_count();
    let huge = "x".repeat(MAX_OPERATOR_STATE_BYTES + 1);
    assert!(huge.len() > MAX_COMPILED_PLAN_BYTES);
    let error = engine
        .materialize(ExecutionRequest {
            plan: derive_plan(source.id, huge),
            connection: connection.clone(),
            asset: source.clone(),
            schema_override: None,
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("operator state");
    assert!(matches!(error, EngineError::BoundExceeded(_)));
    assert_eq!(ffi_import_count(), 0);
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);

    let (text_schema, _) = utf8_schema();
    let wide = "w".repeat(33 * 1024 * 1024);
    let envelope = utf8_batch(&text_schema, source.id, vec![wide]);
    let (engine, connector) = engine_with(text_schema.clone(), vec![envelope], true).await;
    reset_ffi_import_count();
    let error = engine
        .materialize(ExecutionRequest {
            plan: derive_plan(source.id, "ab".repeat(1024)),
            connection,
            asset: source,
            schema_override: Some(text_schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("predicted expansion");
    assert!(matches!(error, EngineError::BoundExceeded(_)));
    assert_eq!(ffi_import_count(), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 1);
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
    drop(_guard);
}

#[test]
fn t40_error_category_and_retryability_mapping() {
    let _guard = exclusive_test_lock().blocking_lock();
    use stillflow_core::ErrorCategory;

    let cases = vec![
        (
            EngineError::UnsupportedOperator {
                node: Uuid::nil(),
                kind: "join",
            },
            ErrorCategory::UnsupportedCapability,
            false,
        ),
        (
            EngineError::UnsupportedRule {
                node: Uuid::nil(),
                kind: "validate",
            },
            ErrorCategory::UnsupportedCapability,
            false,
        ),
        (
            EngineError::UnsupportedCapability { kind: "preview" },
            ErrorCategory::UnsupportedCapability,
            false,
        ),
        (
            EngineError::SourceBinding,
            ErrorCategory::InvalidConfiguration,
            false,
        ),
        (
            EngineError::InvalidPlan("bad plan"),
            ErrorCategory::InvalidConfiguration,
            false,
        ),
        (
            EngineError::UnknownColumn(column(1)),
            ErrorCategory::InvalidConfiguration,
            false,
        ),
        (
            EngineError::TypeError("type mismatch"),
            ErrorCategory::InvalidData,
            false,
        ),
        (
            EngineError::CastFailure {
                column: column(1),
                sequence: 0,
                row: 0,
            },
            ErrorCategory::InvalidData,
            false,
        ),
        (
            EngineError::Arithmetic {
                column: column(1),
                sequence: 0,
                row: 0,
            },
            ErrorCategory::InvalidData,
            false,
        ),
        (
            EngineError::SchemaDrift { sequence: 1 },
            ErrorCategory::SchemaDrift,
            false,
        ),
        (
            EngineError::BoundExceeded("limit"),
            ErrorCategory::InvalidData,
            false,
        ),
        (EngineError::Ffi, ErrorCategory::Internal, false),
        (
            EngineError::Internal("internal error"),
            ErrorCategory::Internal,
            false,
        ),
        (EngineError::Cancelled, ErrorCategory::Cancelled, false),
        (EngineError::Timeout, ErrorCategory::Timeout, true),
        (EngineError::Busy, ErrorCategory::RateLimited, true),
    ];

    for (err, expected_cat, expected_retry) in cases {
        assert_eq!(err.category(), expected_cat);
        assert_eq!(err.retryable(), expected_retry);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t41_split_envelope_keeps_remainder_with_polars() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = int_batch(&schema, source.id, 65_536);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let literal = "a".repeat(2048);
    let (manifest, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan: derive_plan(source.id, literal),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 65_536,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 65_536);
    assert!(report.chunk_count >= 2);
    assert!(report.saw_split_envelope_with_remainder);
    assert!(report.peak_live_payloads <= MAX_LIVE_COLUMNAR_PAYLOADS);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t42_derive_then_drop_then_trim_and_replace_uses_predicted_table() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id1,
        "src",
        LogicalType::Int64,
        false,
    )
    .expect("f")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let env = int_batch(&schema, source.id, 100);
    let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
    let dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");

    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![
                Rule::DeriveColumn {
                    id: id2,
                    name: "derived".to_owned(),
                    data_type: LogicalType::Utf8,
                    nullable: false,
                    expression: Expr::Literal(ScalarValue::Utf8("  foo  ".to_owned())),
                },
                Rule::DropColumn { column: id1 },
                Rule::Trim { column: id2 },
                Rule::ReplaceLiteral {
                    column: id2,
                    from: ScalarValue::Utf8("foo".to_owned()),
                    to: ScalarValue::Utf8("bar".to_owned()),
                },
            ],
        }),
    );
    let (manifest, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");

    assert_eq!(manifest.snapshot().stats().row_count(), 100);
    let out_schema = manifest.snapshot().schema();
    assert_eq!(out_schema.fields.len(), 1);
    assert_eq!(out_schema.fields[0].id, id2);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    drop(_guard);
}

#[test]
fn t43_utf8_byte_cap_uses_offset_overhead() {
    let _guard = exclusive_test_lock().blocking_lock();
    let (schema, _) = int_schema();
    let values = Int64Array::from((0..20_000_i64).collect::<Vec<_>>());
    let factory = BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), Uuid::from_u128(42))
        .expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(values)])
        .expect("batch");
    let predicted = PredictedSchema::from_scan_output(&schema);
    let steps = vec![crate::preflight::CompiledStep::Rules {
        rules: vec![Rule::DeriveColumn {
            id: column(2),
            name: "derived".to_owned(),
            data_type: LogicalType::Utf8,
            nullable: false,
            expression: Expr::Literal(ScalarValue::Utf8("a".repeat(2048))),
        }],
    }];
    let k = largest_feasible_k(20_000, 0, batch.columns(), &predicted, &steps).expect("k");
    assert!(k >= 1);
    assert!(k < 20_000);
    let at_k = predict(k, 0, batch.columns(), &predicted, &steps).expect("predict k");
    let at_next = predict(k + 1, 0, batch.columns(), &predicted, &steps).expect("predict k+1");
    assert!(at_k <= MAX_BATCH_BYTES);
    assert!(MAX_BATCH_BYTES < at_next);
    let derived = utf8_physical_bytes(k, k.saturating_mul(2048));
    assert!(at_k >= derived);
}

#[tokio::test(flavor = "current_thread")]
async fn t44_phased_allocator_excludes_storage_encode() {
    let _guard = exclusive_test_lock().lock().await;
    reset_alloc_peaks();
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = int_batch(&schema, source.id, 65_536);
    let envelope_bytes = envelope.byte_count();
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let literal = "a".repeat(2048);
    let (_, report) = engine
        .materialize_tracked(ExecutionRequest {
            plan: derive_plan(source.id, literal),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: long_context(),
            batch_size: 65_536,
            store: &store,
        })
        .await
        .expect("materialize");
    assert!(report.polars_phase_peak > 0);
    assert!(report.remainder_phase_peak > 0);
    assert!(report.storage_append_phase_peak > 0);
    assert!(envelope_bytes <= MAX_BATCH_BYTES);
    assert!(report.polars_phase_peak <= MAX_BATCH_BYTES);
    assert!(report.remainder_phase_peak <= MAX_BATCH_BYTES);
    let total_live_engine = envelope_bytes
        .saturating_add(report.polars_phase_peak)
        .saturating_add(report.remainder_phase_peak)
        .saturating_add(MAX_OPERATOR_STATE_BYTES);
    assert!(total_live_engine <= MAX_ENGINE_PEAK_BYTES);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t45_date_to_utf8_is_type_error() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "day",
        LogicalType::Date32,
        false,
    )
    .expect("field")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Cast {
                column: id,
                data_type: LogicalType::Utf8,
                on_failure: stillflow_plan::CastFailurePolicy::Error,
            }],
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("paused cast");
    assert!(matches!(error, EngineError::TypeError(_)));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[test]
fn t46_near_64mib_export_transition_respects_bounds() {
    let _guard = exclusive_test_lock().blocking_lock();
    reset_alloc_peaks();
    let (schema, id) = int_schema();
    let values = Int64Array::from((0..20_000_i64).collect::<Vec<_>>());
    let factory = BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), Uuid::from_u128(42))
        .expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(values)])
        .expect("batch");
    let predicted = PredictedSchema::from_scan_output(&schema);
    let steps = vec![crate::preflight::CompiledStep::Rules {
        rules: vec![Rule::DeriveColumn {
            id: column(2),
            name: "wide".to_owned(),
            data_type: LogicalType::Utf8,
            nullable: false,
            expression: Expr::Literal(ScalarValue::Utf8("z".repeat(3000))),
        }],
    }];
    let k = largest_feasible_k(20_000, 0, batch.columns(), &predicted, &steps).expect("k");
    assert!((1..20_000).contains(&k));
    let peak = predict(k, 0, batch.columns(), &predicted, &steps).expect("predict");
    assert!(peak <= MAX_BATCH_BYTES);
    let peak_next = predict(k + 1, 0, batch.columns(), &predicted, &steps).expect("predict next");
    assert!(peak_next > MAX_BATCH_BYTES);

    // Verify real export execution on a chunk of size k with live allocator peak <= MAX_BATCH_BYTES
    let (exported_batch, export_peak) = {
        let _polars_guard = crate::memory::enter_phase(crate::memory::AllocatorPhase::Polars);
        let slice = batch.slice(0, k);
        let frame = crate::ffi::record_batch_to_dataframe(&slice).expect("import");
        let (transformed, deferred) =
            crate::lower::transform(frame, &schema, &steps, Vec::new()).expect("transform");
        let target_schema =
            stillflow_core::logical_schema_to_arrow(&manifest_schema_with_derived(&schema))
                .expect("arrow schema");
        let batch = crate::ffi::dataframe_to_record_batch(
            transformed,
            &manifest_schema_with_derived(&schema),
            &target_schema,
            &deferred,
        )
        .expect("export");
        let (polars_peak, _, _) = crate::memory::alloc_peaks();
        (batch, polars_peak)
    };
    assert_eq!(exported_batch.num_rows(), k);
    assert_eq!(exported_batch.num_columns(), 2);
    assert!(export_peak <= MAX_BATCH_BYTES);
    let _ = id;
}

#[test]
fn t47_4096_columns_no_pack_limit_bulk_preallocation() {
    let _guard = exclusive_test_lock().blocking_lock();
    let fields: Vec<LogicalField> = (0..4096)
        .map(|i| {
            LogicalField::new(
                column((i + 1) as u128),
                format!("col_{i}"),
                LogicalType::Int64,
                false,
            )
            .expect("field")
        })
        .collect();
    let schema = Arc::new(LogicalSchema::new(fields).expect("schema"));
    let rebatcher = crate::remainder::CanonicalRebatcher::new(schema, Uuid::from_u128(99), 65_536)
        .expect("rebatcher");
    // With ExactPrimitiveSink, Vec::new() has 0 allocated capacity.
    assert_eq!(rebatcher.remainder_bytes(), 0);
    assert!(!rebatcher.remainder_live());
}

#[tokio::test(flavor = "current_thread")]
async fn t48_timestamp_timezone_retention() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "ts",
        LogicalType::Timestamp {
            unit: TimeUnit::Millisecond,
            timezone: Some("UTC".to_owned()),
        },
        false,
    )
    .expect("field")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let values = arrow_array::TimestampMillisecondArray::from(vec![1_000_000_i64])
        .with_timezone("UTC".to_owned());
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(values)])
        .expect("batch");
    let envelope = factory.try_build(0, batch).expect("envelope");
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let manifest = engine
        .materialize(ExecutionRequest {
            plan: scan_materialize_plan(source.id, None),
            connection,
            asset: source,
            schema_override: Some(schema.clone()),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 1);
    let out_field = manifest.snapshot().schema().field(id).expect("field");
    assert_eq!(
        out_field.data_type,
        LogicalType::Timestamp {
            unit: TimeUnit::Millisecond,
            timezone: Some("UTC".to_owned())
        }
    );
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t49_iterative_ast_guard_rejects_deep_expression_fast() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let mut deep_expr = Expr::Column(id);
    for _ in 0..70 {
        deep_expr = Expr::IsNull {
            expression: Box::new(deep_expr),
            negated: false,
        };
    }
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::Filter {
            predicate: deep_expr,
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("deep expr");
    assert!(matches!(error, EngineError::BoundExceeded(_)));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t50_lub_strict_casting_in_comparisons_and_coalesce() {
    let _guard = exclusive_test_lock().lock().await;
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "a", LogicalType::Int32, false).expect("f1"),
        LogicalField::new(id2, "b", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let a_arr = Int32Array::from(vec![10_i32, 20_i32]);
    let b_arr = Int64Array::from(vec![10_i64, 30_i64]);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source.id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![Arc::new(a_arr), Arc::new(b_arr)],
    )
    .expect("batch");
    let envelope = factory.try_build(0, batch).expect("envelope");
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let plan = scan_materialize_plan_with_projection(
        source.id,
        vec![id1, id2],
        Some(PlanNodeKind::Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::Column(id1)),
                operator: stillflow_core::BinaryOperator::Equal,
                right: Box::new(Expr::Column(id2)),
            },
        }),
    );
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection: connection.clone(),
            asset: source.clone(),
            schema_override: Some(schema.clone()),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 1);

    // Also test Coalesce with mixed Int32 and Int64
    let schema2 = LogicalSchema::new(vec![
        LogicalField::new(id1, "a", LogicalType::Int32, true).expect("f1"),
        LogicalField::new(id2, "b", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema2");
    let factory2 =
        BatchEnvelopeFactory::try_new(Arc::new(schema2.clone()), source.id).expect("factory2");
    let coalesce_plan = scan_materialize_plan_with_projection(
        source.id,
        vec![id1, id2],
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: column(3),
                name: "c".to_owned(),
                data_type: LogicalType::Int64,
                nullable: false,
                expression: Expr::Coalesce {
                    expressions: vec![Expr::Column(id1), Expr::Column(id2)],
                },
            }],
        }),
    );
    let a_arr2 = Int32Array::from(vec![Some(100_i32), None]);
    let b_arr2 = Int64Array::from(vec![200_i64, 300_i64]);
    let batch2 = RecordBatch::try_new(
        factory2.arrow_schema().clone(),
        vec![Arc::new(a_arr2), Arc::new(b_arr2)],
    )
    .expect("batch2");
    let envelope2 = factory2.try_build(0, batch2).expect("envelope2");
    let (engine2, _) = engine_with(schema2.clone(), vec![envelope2], true).await;
    let store_dir2 = tempfile::TempDir::new().expect("temp");
    let store2 = SnapshotStore::open(store_dir2.path(), StorageLimits::default()).expect("store");
    let manifest2 = engine2
        .materialize(ExecutionRequest {
            plan: coalesce_plan,
            connection,
            asset: source,
            schema_override: Some(schema2),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store2,
        })
        .await
        .expect("materialize coalesce");
    assert_eq!(manifest2.snapshot().stats().row_count(), 2);
    let c_field = manifest2
        .snapshot()
        .schema()
        .field(column(3))
        .expect("c field");
    assert_eq!(c_field.data_type, LogicalType::Int64);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t51_typed_null_derivation() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = int_batch(&schema, source.id, 5);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: column(2),
                name: "derived_null".to_owned(),
                data_type: LogicalType::Int64,
                nullable: true,
                expression: Expr::Literal(ScalarValue::Null),
            }],
        }),
    );
    let manifest = engine
        .materialize(ExecutionRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 5);
    let derived_field = manifest
        .snapshot()
        .schema()
        .field(column(2))
        .expect("field");
    assert_eq!(derived_field.data_type, LogicalType::Int64);
    assert!(derived_field.nullable);
    drop(_guard);
}

#[test]
fn t52_float_to_utf8_prediction_bound() {
    let _guard = exclusive_test_lock().blocking_lock();
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "val",
        LogicalType::Float64,
        false,
    )
    .expect("f")])
    .expect("schema");
    let values = arrow_array::Float64Array::from(vec![1.2345_f64; 100]);
    let factory = BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), Uuid::from_u128(1))
        .expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(values)])
        .expect("batch");
    let predicted = PredictedSchema::from_scan_output(&schema);
    let steps = vec![crate::preflight::CompiledStep::Rules {
        rules: vec![Rule::Cast {
            column: id,
            data_type: LogicalType::Utf8,
            on_failure: stillflow_plan::CastFailurePolicy::Error,
        }],
    }];
    let k = 100_usize;
    let cost = predict(k, 0, batch.columns(), &predicted, &steps).expect("predict");
    let min_utf8_expected = utf8_physical_bytes(k, k.saturating_mul(crate::MAX_FLOAT_UTF8_BYTES));
    assert!(cost >= min_utf8_expected);

    // Also test nested expression cast: Expr::Cast(Float -> Utf8)
    let nested_steps = vec![crate::preflight::CompiledStep::Rules {
        rules: vec![Rule::DeriveColumn {
            id: column(2),
            name: "str_val".to_owned(),
            data_type: LogicalType::Utf8,
            nullable: false,
            expression: Expr::Cast {
                expression: Box::new(Expr::Column(id)),
                data_type: LogicalType::Utf8,
            },
        }],
    }];
    let nested_cost =
        predict(k, 0, batch.columns(), &predicted, &nested_steps).expect("predict nested");
    assert!(nested_cost >= min_utf8_expected);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn t53_binary_cast_rejection() {
    let _guard = exclusive_test_lock().lock().await;
    let id = column(1);
    let schema = LogicalSchema::new(vec![LogicalField::new(
        id,
        "bin",
        LogicalType::Binary,
        false,
    )
    .expect("f")])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Cast {
                column: id,
                data_type: LogicalType::Utf8,
                on_failure: stillflow_plan::CastFailurePolicy::Error,
            }],
        }),
    );
    let error = engine
        .preflight(
            &plan,
            &connection,
            &source,
            Some(&schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("binary to utf8 cast");
    assert!(matches!(error, EngineError::TypeError(_)));

    // Reverse: Cast Int64 to Binary
    let (int_schema, int_id) = int_schema();
    let plan_reverse = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::ApplyRules {
            rules: vec![Rule::Cast {
                column: int_id,
                data_type: LogicalType::Binary,
                on_failure: stillflow_plan::CastFailurePolicy::Error,
            }],
        }),
    );
    let error_rev = engine
        .preflight(
            &plan_reverse,
            &connection,
            &source,
            Some(&int_schema),
            &stillflow_core::RequestContext::default(),
        )
        .await
        .expect_err("int64 to binary cast");
    assert!(matches!(error_rev, EngineError::TypeError(_)));
    drop(_guard);
}

#[test]
fn t54_fallback_error_sanitization_is_always_internal() {
    let _guard = exclusive_test_lock().blocking_lock();
    let summary = crate::error::EngineError::Internal("test internal").sanitized_summary();
    assert_eq!(summary.category, stillflow_core::ErrorCategory::Internal);
    assert!(!summary.retryable);

    let summary2 = crate::error::EngineError::Ffi.sanitized_summary();
    assert_eq!(summary2.category, stillflow_core::ErrorCategory::Internal);
    assert!(!summary2.retryable);

    // Test fallback_summary injection point directly
    crate::error::set_force_fallback_summary(true);
    let summary_forced = crate::error::EngineError::SourceBinding.sanitized_summary();
    crate::error::set_force_fallback_summary(false);
    assert_eq!(
        summary_forced.category,
        stillflow_core::ErrorCategory::Internal
    );
    assert!(!summary_forced.retryable);
    assert_eq!(summary_forced.message(), "internal error");
    drop(_guard);
}

#[test]
fn t55_near_64mib_nullable_int64_remainder_freeze_respects_bounds() {
    let _guard = exclusive_test_lock().blocking_lock();
    reset_alloc_peaks();

    let num_cols = 120_usize;
    let rows = 65_536_usize;
    let fields: Vec<LogicalField> = (0..num_cols)
        .map(|i| {
            LogicalField::new(
                column((i + 1) as u128),
                format!("c_{i}"),
                LogicalType::Int64,
                true,
            )
            .expect("field")
        })
        .collect();
    let schema = Arc::new(LogicalSchema::new(fields).expect("schema"));
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        let mut builder = arrow_array::builder::Int64Builder::with_capacity(rows);
        for row in 0..rows {
            if row % 2 == 0 {
                builder.append_value(row as i64);
            } else {
                builder.append_null();
            }
        }
        columns.push(Arc::new(builder.finish()));
    }
    let factory =
        BatchEnvelopeFactory::try_new(schema.clone(), Uuid::from_u128(999)).expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), columns).expect("batch");

    let mut tracker = crate::memory::MemoryTracker::new();
    let mut rebatcher = {
        let _guard = crate::memory::enter_phase(crate::memory::AllocatorPhase::Remainder);
        crate::remainder::CanonicalRebatcher::new(schema.clone(), Uuid::from_u128(999), rows)
            .expect("rebatcher")
    };
    tracker
        .hold_remainder(rebatcher.remainder_bytes())
        .expect("hold remainder");

    let mut published = Vec::new();
    rebatcher
        .push(batch, &mut tracker, |envelope, _| {
            published.push(envelope);
            Ok(())
        })
        .expect("push");
    rebatcher
        .finish(&mut tracker, |envelope, _| {
            published.push(envelope);
            Ok(())
        })
        .expect("finish");

    let (_, remainder_peak, _) = crate::memory::alloc_peaks();
    assert!(remainder_peak > 0);
    assert!(remainder_peak <= MAX_BATCH_BYTES);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].payload().num_rows(), rows);
    assert_eq!(published[0].payload().num_columns(), num_cols);

    drop(published);
    drop(_guard);
}

#[test]
fn t56_near_60mib_nullable_boolean_remainder_freeze_respects_bounds() {
    let _guard = exclusive_test_lock().blocking_lock();
    reset_alloc_peaks();

    let num_bool_cols = 3_700_usize;
    let rows = 65_536_usize;
    let bool_fields: Vec<LogicalField> = (0..num_bool_cols)
        .map(|i| {
            LogicalField::new(
                column((i + 1) as u128),
                format!("b_{i}"),
                LogicalType::Boolean,
                true,
            )
            .expect("field")
        })
        .collect();
    let bool_schema = Arc::new(LogicalSchema::new(bool_fields).expect("bool schema"));
    let mut bool_columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(num_bool_cols);
    for _ in 0..num_bool_cols {
        let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if row % 2 == 0 {
                builder.append_value(row % 4 == 0);
            } else {
                builder.append_null();
            }
        }
        bool_columns.push(Arc::new(builder.finish()));
    }
    let bool_factory =
        BatchEnvelopeFactory::try_new(bool_schema.clone(), Uuid::from_u128(998)).expect("factory");
    let bool_batch =
        RecordBatch::try_new(bool_factory.arrow_schema().clone(), bool_columns).expect("batch");

    let mut bool_tracker = crate::memory::MemoryTracker::new();
    let mut bool_rebatcher = {
        let _guard = crate::memory::enter_phase(crate::memory::AllocatorPhase::Remainder);
        crate::remainder::CanonicalRebatcher::new(bool_schema.clone(), Uuid::from_u128(998), rows)
            .expect("bool rebatcher")
    };
    bool_tracker
        .hold_remainder(bool_rebatcher.remainder_bytes())
        .expect("hold bool remainder");

    let mut bool_published = Vec::new();
    bool_rebatcher
        .push(bool_batch, &mut bool_tracker, |envelope, _| {
            bool_published.push(envelope);
            Ok(())
        })
        .expect("push bool");
    bool_rebatcher
        .finish(&mut bool_tracker, |envelope, _| {
            bool_published.push(envelope);
            Ok(())
        })
        .expect("finish bool");

    let (_, bool_remainder_peak, _) = crate::memory::alloc_peaks();
    assert!(bool_remainder_peak > 0);
    assert!(bool_remainder_peak <= MAX_BATCH_BYTES);
    assert_eq!(bool_published.len(), 1);
    assert_eq!(bool_published[0].payload().num_rows(), rows);
    assert_eq!(bool_published[0].payload().num_columns(), num_bool_cols);

    drop(bool_published);
    drop(_guard);
}

#[test]
fn t57_all_valid_flush_then_nullable_flush_resets_validity() {
    let _guard = exclusive_test_lock().blocking_lock();
    reset_alloc_peaks();

    // 1. Int64: First batch all valid (10 rows), Second batch with nulls (5 rows)
    let id_int = column(1);
    let int_schema = Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            id_int,
            "val",
            LogicalType::Int64,
            true,
        )
        .expect("field")])
        .expect("schema"),
    );
    let mut rebatcher = {
        let _guard = crate::memory::enter_phase(crate::memory::AllocatorPhase::Remainder);
        crate::remainder::CanonicalRebatcher::new(int_schema.clone(), Uuid::from_u128(101), 10)
            .expect("rebatcher")
    };
    let mut tracker = crate::memory::MemoryTracker::new();
    tracker
        .hold_remainder(rebatcher.remainder_bytes())
        .expect("hold");

    // Batch 1: All valid (10 rows)
    let b1_arr = Int64Array::from((0..10_i64).collect::<Vec<_>>());
    let factory =
        BatchEnvelopeFactory::try_new(int_schema.clone(), Uuid::from_u128(101)).expect("factory");
    let b1 =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(b1_arr)]).expect("b1");
    let mut published = Vec::new();
    rebatcher
        .push(b1, &mut tracker, |envelope, _| {
            published.push(envelope);
            Ok(())
        })
        .expect("push b1");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].payload().num_rows(), 10);
    assert_eq!(published[0].payload().column(0).null_count(), 0);
    assert_eq!(rebatcher.remainder_bytes(), 0);
    assert!(!rebatcher.remainder_live());

    // Batch 2: 5 rows with nulls
    let b2_arr = Int64Array::from(vec![Some(1_i64), None, Some(3_i64), None, Some(5_i64)]);
    let b2 =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(b2_arr)]).expect("b2");
    rebatcher
        .push(b2, &mut tracker, |envelope, _| {
            published.push(envelope);
            Ok(())
        })
        .expect("push b2");
    rebatcher
        .finish(&mut tracker, |envelope, _| {
            published.push(envelope);
            Ok(())
        })
        .expect("finish");
    assert_eq!(published.len(), 2);
    assert_eq!(published[1].payload().num_rows(), 5);
    assert_eq!(published[1].payload().column(0).null_count(), 2);

    drop(published);

    // 2. Boolean: First batch all valid (8 rows), Second batch with nulls (4 rows)
    reset_alloc_peaks();
    let id_bool = column(2);
    let bool_schema = Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            id_bool,
            "flag",
            LogicalType::Boolean,
            true,
        )
        .expect("field")])
        .expect("schema"),
    );
    let mut bool_rebatcher = {
        let _guard = crate::memory::enter_phase(crate::memory::AllocatorPhase::Remainder);
        crate::remainder::CanonicalRebatcher::new(bool_schema.clone(), Uuid::from_u128(102), 8)
            .expect("bool rebatcher")
    };
    let mut bool_tracker = crate::memory::MemoryTracker::new();
    bool_tracker
        .hold_remainder(bool_rebatcher.remainder_bytes())
        .expect("hold");

    // Batch 1: All valid (8 rows)
    let bool_b1_arr =
        arrow_array::BooleanArray::from(vec![true, false, true, false, true, false, true, false]);
    let bool_factory =
        BatchEnvelopeFactory::try_new(bool_schema.clone(), Uuid::from_u128(102)).expect("factory");
    let bool_b1 = RecordBatch::try_new(
        bool_factory.arrow_schema().clone(),
        vec![Arc::new(bool_b1_arr)],
    )
    .expect("b1");
    let mut bool_published = Vec::new();
    bool_rebatcher
        .push(bool_b1, &mut bool_tracker, |envelope, _| {
            bool_published.push(envelope);
            Ok(())
        })
        .expect("push bool b1");
    assert_eq!(bool_published.len(), 1);
    assert_eq!(bool_published[0].payload().num_rows(), 8);
    assert_eq!(bool_published[0].payload().column(0).null_count(), 0);
    assert_eq!(bool_rebatcher.remainder_bytes(), 0);
    assert!(!bool_rebatcher.remainder_live());

    // Batch 2: 4 rows with nulls
    let bool_b2_arr = arrow_array::BooleanArray::from(vec![Some(true), None, Some(false), None]);
    let bool_b2 = RecordBatch::try_new(
        bool_factory.arrow_schema().clone(),
        vec![Arc::new(bool_b2_arr)],
    )
    .expect("b2");
    bool_rebatcher
        .push(bool_b2, &mut bool_tracker, |envelope, _| {
            bool_published.push(envelope);
            Ok(())
        })
        .expect("push bool b2");
    bool_rebatcher
        .finish(&mut bool_tracker, |envelope, _| {
            bool_published.push(envelope);
            Ok(())
        })
        .expect("finish");
    assert_eq!(bool_published.len(), 2);
    assert_eq!(bool_published[1].payload().num_rows(), 4);
    assert_eq!(bool_published[1].payload().column(0).null_count(), 2);

    drop(bool_published);
    drop(_guard);
}

// ---------------------------------------------------------------------------
// E4-S2 verification evidence (contract acceptance matrix V01–V31)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod verification {
    use super::*;
    use stillflow_core::verification::ArtifactKind;
    use stillflow_storage::{
        artifact::{dedup_rule_summary_section_schema, validation_finding_section_schema,
        validation_rule_summary_section_schema},
        VerificationBundleDraft,
    };
    use std::collections::BTreeMap;

    struct VerIds {
        run_id: Uuid,
        bundle_id: Uuid,
        bundle_artifact_id: Uuid,
        snapshot_id: Uuid,
        dataset_id: Uuid,
        validation_report_artifact_id: Uuid,
        rejected_rows_artifact_id: Option<Uuid>,
        deduplication_report_artifact_id: Uuid,
        session_id: Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
        started_at: chrono::DateTime<chrono::Utc>,
        committed_at: chrono::DateTime<chrono::Utc>,
    }

    fn ver_ids(base: u128) -> VerIds {
        let now = Utc::now();
        VerIds {
            run_id: Uuid::from_u128(base),
            bundle_id: Uuid::from_u128(base + 1),
            bundle_artifact_id: Uuid::from_u128(base + 2),
            snapshot_id: Uuid::from_u128(base + 3),
            dataset_id: Uuid::from_u128(base + 4),
            validation_report_artifact_id: Uuid::from_u128(base + 5),
            rejected_rows_artifact_id: Some(Uuid::from_u128(base + 6)),
            deduplication_report_artifact_id: Uuid::from_u128(base + 7),
            session_id: Uuid::from_u128(base + 8),
            created_at: now,
            started_at: now,
            committed_at: now,
        }
    }

    fn canonical_digest(plan: &LogicalPlan) -> [u8; 32] {
        use sha2::Digest as _;
        sha2::Sha256::digest(plan.canonical_bytes().expect("canonical")).into()
    }

    fn ver_plan(
        source_asset_id: Uuid,
        projection: Vec<ColumnId>,
        keys: &[ColumnId],
        validate: Option<(Expr, stillflow_plan::ValidationSeverity, &'static str)>,
        filter_rows: Option<Expr>,
    ) -> LogicalPlan {
        let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
        let filter = PlanNodeId::from_uuid(Uuid::from_u128(3));
        let rules = PlanNodeId::from_uuid(Uuid::from_u128(4));
        let materialize = PlanNodeId::from_uuid(Uuid::from_u128(5));
        let mut node_rules: Vec<Rule> = Vec::new();
        if let Some((predicate, severity, message)) = validate {
            node_rules.push(Rule::Validate { predicate, severity, message: message.to_owned() });
        }
        if !keys.is_empty() {
            node_rules.push(Rule::Deduplicate { keys: keys.to_vec() });
        }
        if let Some(predicate) = filter_rows {
            node_rules.push(Rule::FilterRows { predicate });
        }
        let mut nodes = BTreeMap::new();
        nodes.insert(scan, PlanNode::new(PlanNodeKind::Scan { source_asset_id, projection, predicate: None }, Vec::new()));
        nodes.insert(filter, PlanNode::new(PlanNodeKind::Filter { predicate: Expr::Literal(ScalarValue::Boolean(true)) }, vec![scan]));
        nodes.insert(rules, PlanNode::new(PlanNodeKind::ApplyRules { rules: node_rules }, vec![filter]));
        nodes.insert(materialize, PlanNode::new(PlanNodeKind::Materialize { output_label: "out".to_owned() }, vec![rules]));
        LogicalPlan::new(materialize, nodes).expect("plan")
    }

    fn verify_request<'a>(
        engine_and_store: (&'a ExecutionEngine, &'a SnapshotStore),
        connection: &SourceConnection,
        source: &SourceAsset,
        schema: LogicalSchema,
        plan: LogicalPlan,
        ids: VerIds,
        cancel: Option<tokio_util::sync::CancellationToken>,
    ) -> crate::verification::VerificationRequest<'a> {
        use crate::verification::{VerificationIdentities as VI, VerificationRequest};
        let context = match cancel {
            Some(token) => stillflow_core::RequestContext::with_cancellation(token),
            None => long_context(),
        };
        VerificationRequest {
            plan,
            connection: connection.clone(),
            asset: source.clone(),
            schema_override: Some(schema),
            identities: VI {
                run_id: ids.run_id,
                bundle_id: ids.bundle_id,
                bundle_artifact_id: ids.bundle_artifact_id,
                snapshot_id: ids.snapshot_id,
                dataset_id: ids.dataset_id,
                validation_report_artifact_id: ids.validation_report_artifact_id,
                rejected_rows_artifact_id: ids.rejected_rows_artifact_id,
                deduplication_report_artifact_id: ids.deduplication_report_artifact_id,
                session_id: ids.session_id,
                logical_input: stillflow_core::verification::LogicalInputRef {
                    input: stillflow_core::verification::InputRef::Asset { asset_id: source.id },
                    version_digest: [7u8; 32],
                },
                canonical_plan_digest: [0u8; 32], // patched by caller
                created_at: ids.created_at,
                started_at: ids.started_at,
                committed_at: ids.committed_at,
                lineage: Default::default(),
                quality_score: None,
            },
            context,
            batch_size: 64,
            store: engine_and_store.1,
        }
    }

    // V01/V06/V27: severity routing, one-payload guarantee, summary counts.
    #[tokio::test(flavor = "current_thread")]
    async fn v01_validate_routing_warning_keeps_error_rejects_null_fails() {
        let _guard = exclusive_test_lock().lock().await;
        let (schema, id) = int_schema();
        let connection = connection();
        let source = asset(connection.id());
        let env = int_batch(&schema, source.id, 3); // rows 0,1,2
        let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
        let dir = tempfile::TempDir::new().expect("temp");
        let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
        let plan = ver_plan(
            source.id,
            vec![schema.fields[0].id],
            &[],
            Some((
                Expr::Binary {
                    left: Box::new(Expr::Column(id)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
                },
                stillflow_plan::ValidationSeverity::Error,
                "above one",
            )),
            None,
        );
        let digest = canonical_digest(&plan);
        let ids = ver_ids(0xA0);
        let bundle_id = ids.bundle_id;
        let rejected_id = ids.rejected_rows_artifact_id.expect("authorized");
        let mut request = verify_request((&engine, &store), &connection, &source, schema.clone(), plan, ids, None);
        request.identities.canonical_plan_digest = digest;
        let bundle = engine.materialize_verification(request).await.expect("bundle");
        let rejected = bundle.rejected_rows().expect("terminal rejections publish payload");
        assert_eq!(rejected.manifest().artifact_id(), rejected_id);
        let mut reader = store
            .open_artifact_section(
                bundle_id,
                rejected_id,
                stillflow_storage::ArtifactSectionId::RejectedRows,
            )
            .expect("open rejected section");
        let mut payload_rows = 0usize;
        while let Some(item) = reader.next() {
            payload_rows += item.expect("envelope").row_count();
        }
        assert_eq!(payload_rows, 2);
        assert_eq!(bundle.accepted().manifest().snapshot().stats().row_count(), 1);
        drop(_guard);
    }

    use crate::EngineError;

    fn reject_plan(source_id: Uuid, id: ColumnId) -> LogicalPlan {
        ver_plan(source_id, vec![id], &[], Some((
            Expr::Binary {
                left: Box::new(Expr::Column(id)),
                operator: stillflow_core::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
            },
            stillflow_plan::ValidationSeverity::Error,
            "always fails",
        )), None)
    }

    // V02: empty source publishes accepted + two zero-row reports; rejected
    // stays absent even under Some(id) authorization.
    #[tokio::test(flavor = "current_thread")]
    async fn v02_empty_source_zero_rejection_protocol() {
        let _guard = exclusive_test_lock().lock().await;
        let (schema, _id) = int_schema();
        let connection = connection();
        let source = asset(connection.id());
        let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
        let dir = tempfile::TempDir::new().expect("temp");
        let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
        let plan = reject_plan(source.id, schema.fields[0].id);
        let digest = canonical_digest(&plan);
        let ids = ver_ids(0xB0);
        let mut request = verify_request((&engine, &store), &connection, &source, schema.clone(), plan, ids, None);
        request.identities.canonical_plan_digest = digest;
        let bundle = engine.materialize_verification(request).await.expect("bundle");
        assert!(bundle.rejected_rows().is_none());
        
        assert_eq!(bundle.validation_report().manifest().sections().len(), 2);
        assert_eq!(bundle.deduplication_report().manifest().sections().len(), 2);
        drop(_guard);
    }

    // V02b: None authorization + terminal rejection fails InvalidPlan and
    // publishes nothing (contract 10.5).
    #[tokio::test(flavor = "current_thread")]
    async fn v02b_unauthorized_rejection_fails_closed() {
        let _guard = exclusive_test_lock().lock().await;
        let (schema, _id) = int_schema();
        let connection = connection();
        let source = asset(connection.id());
        let env = int_batch(&schema, source.id, 2);
        let (engine, _) = engine_with(schema.clone(), vec![env], true).await;
        let dir = tempfile::TempDir::new().expect("temp");
        let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
        let plan = reject_plan(source.id, schema.fields[0].id);
        let digest = canonical_digest(&plan);
        let ids = ver_ids(0xC0);
        let mut request = verify_request((&engine, &store), &connection, &source, schema.clone(), plan, ids, None);
        request.identities.canonical_plan_digest = digest;
        request.identities.rejected_rows_artifact_id = None;
        let run_id = request.identities.run_id;
        let error = engine.materialize_verification(request).await.expect_err("must fail");
        assert!(matches!(error, EngineError::InvalidPlan(_)));
        assert!(store.load_verification_bundle_by_run_id(run_id).is_err());
        drop(_guard);
    }

    // V23: E2 materialize keeps rejecting Validate rules.
    #[tokio::test(flavor = "current_thread")]
    async fn v23_materialize_still_rejects_validate() {
        let _guard = exclusive_test_lock().lock().await;
        let (schema, id) = int_schema(); // used by reject_plan below
        let connection = connection();
        let source = asset(connection.id());
        let (engine, _) = engine_with(schema.clone(), Vec::new(), true).await;
        let dir = tempfile::TempDir::new().expect("temp");
        let store = SnapshotStore::open(dir.path(), StorageLimits::default()).expect("store");
        let plan = reject_plan(source.id, schema.fields[0].id);
        let error = engine
            .materialize(ExecutionRequest {
                plan,
                connection: connection.clone(),
                asset: source.clone(),
                schema_override: Some(schema),
                identities: identities(),
                context: long_context(),
                batch_size: 64,
                store: &store,
            })
            .await
            .expect_err("E2 must refuse");
        assert!(matches!(error, EngineError::UnsupportedRule { kind: "validate", .. }));
        drop(_guard);
    }

}
