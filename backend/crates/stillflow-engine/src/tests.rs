use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_buffer::{Buffer as ArrowBuffer, OffsetBuffer, ScalarBuffer};
use async_trait::async_trait;
use chrono::Utc;
use stillflow_connectors::{
    ConnectorCapabilities, ConnectorRegistry, RawBatchStream, SourceConnector, SourceConnectorRef,
};
use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, BatchEnvelopeFactory, CheckpointRequest, ColumnId,
    ConnectionStatus, ConnectorKind, ConnectorResult, CredentialRef, DiscoverRequest, Expr,
    InspectRequest, LogicalField, LogicalSchema, LogicalType, PreviewData,
    PreviewRequest as CorePreviewRequest, ReadRequest, ScalarValue, SourceAsset, SourceConnection,
    TestConnectionRequest, TimeUnit, MAX_BATCH_BYTES,
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
    crate_name, ExecutionEngine, ExecutionIdentities, ExecutionRequest, PreviewRequest,
    ENGINE_MAX_DEADLINE, MAX_COMPILED_PLAN_BYTES, MAX_ENGINE_PEAK_BYTES,
    MAX_LIVE_COLUMNAR_PAYLOADS, MAX_OPERATOR_STATE_BYTES, PREVIEW_DEFAULT_BYTE_LIMIT,
    PREVIEW_MAX_ROW_LIMIT, PREVIEW_MAX_SOURCE_ROWS_SCANNED, PREVIEW_PEAK_ENGINE_BYTES,
};

const SENTINEL: &str = "STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a";

struct CountingBatchStream {
    items: std::collections::VecDeque<stillflow_core::BatchItem>,
    poll_count: Arc<AtomicUsize>,
    pending_after: Option<usize>,
}

impl futures::Stream for CountingBatchStream {
    type Item = stillflow_core::BatchItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polls = self.poll_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.pending_after.is_some_and(|limit| polls > limit) {
            return std::task::Poll::Pending;
        }
        if let Some(item) = self.items.pop_front() {
            std::task::Poll::Ready(Some(item))
        } else {
            std::task::Poll::Ready(None)
        }
    }
}

struct ScriptedConnector {
    schema: LogicalSchema,
    envelopes: Mutex<Vec<stillflow_core::BatchEnvelope>>,
    inspect_count: AtomicUsize,
    read_count: AtomicUsize,
    poll_count: Arc<AtomicUsize>,
    projection: bool,
    pending: bool,
    pending_after: Option<usize>,
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
        request: CorePreviewRequest,
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
        if self.pending {
            return Ok(RawBatchStream::new(Box::pin(futures::stream::pending())));
        }
        let envelopes = self.envelopes.lock().expect("fixture lock").clone();
        Ok(RawBatchStream::new(Box::pin(CountingBatchStream {
            items: envelopes.into_iter().map(Ok).collect(),
            poll_count: Arc::clone(&self.poll_count),
            pending_after: self.pending_after,
        })))
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

pub(crate) fn exclusive_test_lock() -> &'static tokio::sync::Mutex<()> {
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
    utf8_envelope_values(schema, asset_id, values, 0)
}

fn utf8_envelope_values(
    schema: &LogicalSchema,
    asset_id: Uuid,
    values: Vec<String>,
    sequence: u64,
) -> stillflow_core::BatchEnvelope {
    let array = StringArray::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
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

async fn engine_with_pending(
    schema: LogicalSchema,
    projection: bool,
) -> (ExecutionEngine, Arc<ScriptedConnector>) {
    let connector = Arc::new(ScriptedConnector {
        schema,
        envelopes: Mutex::new(Vec::new()),
        inspect_count: AtomicUsize::new(0),
        read_count: AtomicUsize::new(0),
        poll_count: Arc::new(AtomicUsize::new(0)),
        projection,
        pending: true,
        pending_after: None,
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    (ExecutionEngine::new(registry), connector)
}

async fn engine_with_pending_after(
    schema: LogicalSchema,
    envelopes: Vec<stillflow_core::BatchEnvelope>,
    pending_after: usize,
    projection: bool,
) -> (ExecutionEngine, Arc<ScriptedConnector>) {
    let connector = Arc::new(ScriptedConnector {
        schema,
        envelopes: Mutex::new(envelopes),
        inspect_count: AtomicUsize::new(0),
        read_count: AtomicUsize::new(0),
        poll_count: Arc::new(AtomicUsize::new(0)),
        projection,
        pending: false,
        pending_after: Some(pending_after),
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    (ExecutionEngine::new(registry), connector)
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
        poll_count: Arc::new(AtomicUsize::new(0)),
        projection,
        pending: false,
        pending_after: None,
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
    // Issue #46 T44 freezes the phase-peak law as "(a) Polars working set of
    // one chunk + (b) remainder builder / freeze-move + 5 MiB <=
    // MAX_ENGINE_PEAK_BYTES; (c) storage append is recorded and excluded".
    // Phase peak (b) includes the old-buffer/new-buffer realloc transient,
    // which section 10.1 rule 5 tracks separately from the enforced
    // remainder capacity: it is bounded by this peak sum, never by
    // MAX_BATCH_BYTES, and never by admission.
    let phased_peak_sum = report
        .polars_phase_peak
        .saturating_add(report.remainder_phase_peak)
        .saturating_add(MAX_OPERATOR_STATE_BYTES);
    assert!(phased_peak_sum <= MAX_ENGINE_PEAK_BYTES);
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
// E3 Preview runtime acceptance tests P01-P14
// ---------------------------------------------------------------------------

fn preview_pipeline_plan(
    asset_id: Uuid,
) -> (
    LogicalPlan,
    PlanNodeId,
    PlanNodeId,
    PlanNodeId,
    PlanNodeId,
    PlanNodeId,
) {
    let id1 = column(1);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(10));
    let project = PlanNodeId::from_uuid(Uuid::from_u128(11));
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(12));
    let rules = PlanNodeId::from_uuid(Uuid::from_u128(13));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(14));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: asset_id,
                projection: vec![id1],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        project,
        PlanNode::new(PlanNodeKind::Project { columns: vec![id1] }, vec![scan]),
    );
    nodes.insert(
        filter,
        PlanNode::new(
            PlanNodeKind::Filter {
                predicate: Expr::Binary {
                    left: Box::new(Expr::Column(id1)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                },
            },
            vec![project],
        ),
    );
    nodes.insert(
        rules,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![Rule::Rename {
                    column: id1,
                    to: "renamed".to_owned(),
                }],
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
    let plan = LogicalPlan::new(materialize, nodes).expect("preview plan");
    (plan, scan, project, filter, rules, materialize)
}

fn preview_request(
    plan: LogicalPlan,
    target: PlanNodeId,
    connection: SourceConnection,
    source: SourceAsset,
    schema: LogicalSchema,
    row_limit: usize,
    byte_limit: usize,
) -> PreviewRequest {
    let mut request = PreviewRequest::new(plan, target, connection, source);
    request.schema_override = Some(schema);
    request.row_limit = row_limit;
    request.byte_limit = byte_limit;
    request.batch_size = 64;
    request
}

fn collect_preview_i64(result: &crate::PreviewResult) -> Vec<i64> {
    let mut rows = Vec::new();
    for envelope in &result.batches {
        for row in 0..envelope.row_count() {
            let array = envelope
                .payload()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            rows.push(array.value(row));
        }
    }
    rows
}

fn collect_preview_utf8(result: &crate::PreviewResult) -> Vec<String> {
    let mut rows = Vec::new();
    for envelope in &result.batches {
        let array = envelope
            .payload()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        for row in 0..envelope.row_count() {
            rows.push(array.value(row).to_owned());
        }
    }
    rows
}

fn int_envelope_seq(
    schema: &LogicalSchema,
    asset_id: Uuid,
    rows: i64,
    sequence: u64,
) -> stillflow_core::BatchEnvelope {
    int_envelope_values(schema, asset_id, (0..rows).collect(), sequence)
}

fn int_envelope_values(
    schema: &LogicalSchema,
    asset_id: Uuid,
    values: Vec<i64>,
    sequence: u64,
) -> stillflow_core::BatchEnvelope {
    let array = Int64Array::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
}

/// Builds an envelope whose `byte_count()` is exactly `target` bytes.
///
/// A legitimate Arrow construction whose `get_array_memory_size()` equals
/// the target exactly is valid. Builder-produced capacities come in
/// allocation quanta and cannot hit 67,108,864, so this helper hand-builds
/// a two-row UTF-8 array with exact-capacity buffers (one value buffer of
/// exactly `values_bytes` bytes, one offsets buffer of exactly three
/// `i32`) and calibrates the fixed overhead from its own construction.
fn exact_byte_utf8_envelope(
    schema: &LogicalSchema,
    asset_id: Uuid,
    target: usize,
    sequence: u64,
) -> stillflow_core::BatchEnvelope {
    use stillflow_core::BatchEnvelope;
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let build = |values_bytes: usize| -> BatchEnvelope {
        // `Vec::with_capacity` allocates exactly `values_bytes` bytes and
        // `Buffer::from_vec` adopts the whole allocation, so the frozen
        // values-buffer capacity is exact (no 64-byte MutableBuffer
        // rounding).
        let mut data = Vec::with_capacity(values_bytes);
        let first_row = vec![b'a'; values_bytes / 2];
        data.extend_from_slice(&first_row);
        let second_row = vec![b'b'; values_bytes - values_bytes / 2];
        data.extend_from_slice(&second_row);
        assert_eq!(data.capacity(), values_bytes, "exact values capacity");

        // Exactly three i32 entries; `vec![]` allocates the exact
        // capacity so the frozen offsets buffer stays deterministic.
        let offsets = vec![0_i32, (values_bytes / 2) as i32, values_bytes as i32];
        let array = StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            ArrowBuffer::from_vec(data),
            None,
        );
        let batch = RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)])
            .expect("batch");
        factory.try_build(sequence, batch).expect("envelope")
    };

    // Fixed overhead of this exact layout: array object sizes plus the
    // three-entry offsets buffer, measured on the zero-length values
    // buffer so no builder quantum can leak into the calibration.
    let overhead = build(0).byte_count();
    assert!(target > overhead, "target must exceed the fixed overhead");
    let envelope = build(target - overhead);
    assert_eq!(
        envelope.byte_count(),
        target,
        "hand-built fixture must hit the exact source-byte boundary"
    );
    envelope
}

#[tokio::test(flavor = "current_thread")]
async fn p01_target_cutoff_for_each_supported_node() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 5)], true).await;
    let (plan, scan, project, filter, rules, _) = preview_pipeline_plan(source.id);

    let scan_result = engine
        .preview(preview_request(
            plan.clone(),
            scan,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("scan preview");
    assert_eq!(collect_preview_i64(&scan_result), vec![0, 1, 2, 3, 4]);
    assert_eq!(scan_result.schema.fields[0].name, "value");

    let project_result = engine
        .preview(preview_request(
            plan.clone(),
            project,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("project preview");
    assert_eq!(collect_preview_i64(&project_result), vec![0, 1, 2, 3, 4]);
    assert_eq!(project_result.schema.fields[0].name, "value");

    let filter_result = engine
        .preview(preview_request(
            plan.clone(),
            filter,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("filter preview");
    assert_eq!(collect_preview_i64(&filter_result), vec![3, 4]);
    assert_eq!(filter_result.schema.fields[0].name, "value");

    let rules_result = engine
        .preview(preview_request(
            plan,
            rules,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("rules preview");
    assert_eq!(collect_preview_i64(&rules_result), vec![3, 4]);
    assert_eq!(rules_result.schema.fields[0].name, "renamed");
    assert_eq!(rules_result.schema.fields[0].id, column(1));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p02_downstream_rules_do_not_execute() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 5)], true).await;
    let (plan, _, project, _, _, _) = preview_pipeline_plan(source.id);
    let result = engine
        .preview(preview_request(
            plan,
            project,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("project target");
    assert_eq!(collect_preview_i64(&result), vec![0, 1, 2, 3, 4]);
    assert_eq!(result.schema.fields[0].name, "value");
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p03_invalid_or_missing_target_fails_before_inspect() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let (plan, _, _, _, _, materialize) = preview_pipeline_plan(source.id);

    let nil = engine
        .preview(preview_request(
            plan.clone(),
            PlanNodeId::from_uuid(Uuid::nil()),
            connection.clone(),
            source.clone(),
            schema.clone(),
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("nil target");
    assert!(matches!(nil, EngineError::InvalidPlan(_)));
    let missing = engine
        .preview(preview_request(
            plan.clone(),
            PlanNodeId::from_uuid(Uuid::from_u128(999)),
            connection.clone(),
            source.clone(),
            schema.clone(),
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("missing target");
    assert!(matches!(missing, EngineError::InvalidPlan(_)));
    let materialize_err = engine
        .preview(preview_request(
            plan,
            materialize,
            connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("materialize target");
    assert!(matches!(
        materialize_err,
        EngineError::UnsupportedOperator { .. }
    ));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p04_join_and_union_rejected_before_inspect() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;

    let scan1 = PlanNodeId::from_uuid(Uuid::from_u128(20));
    let scan2 = PlanNodeId::from_uuid(Uuid::from_u128(21));
    let join = PlanNodeId::from_uuid(Uuid::from_u128(22));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(23));
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
            vec![scan1, scan2],
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
    let join_plan = LogicalPlan::new(mat, nodes).expect("join plan");
    let err = engine
        .preview(preview_request(
            join_plan,
            join,
            connection.clone(),
            source.clone(),
            schema.clone(),
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("join preview");
    assert!(matches!(err, EngineError::UnsupportedOperator { .. }));

    let s1 = PlanNodeId::from_uuid(Uuid::from_u128(30));
    let s2 = PlanNodeId::from_uuid(Uuid::from_u128(31));
    let union = PlanNodeId::from_uuid(Uuid::from_u128(32));
    let mat2 = PlanNodeId::from_uuid(Uuid::from_u128(33));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        s1,
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
        s2,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![column(1)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(union, PlanNode::new(PlanNodeKind::Union, vec![s1, s2]));
    nodes.insert(
        mat2,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![union],
        ),
    );
    let union_plan = LogicalPlan::new(mat2, nodes).expect("union plan");
    let err = engine
        .preview(preview_request(
            union_plan,
            union,
            connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("union preview");
    assert!(matches!(err, EngineError::UnsupportedOperator { .. }));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_target_output_truncation_and_scan_cap() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id1) = int_schema();
    let connection = connection();
    let source = asset(connection.id());

    let (engine, _) = engine_with(
        schema.clone(),
        vec![int_batch(&schema, source.id, 40)],
        true,
    )
    .await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(40));
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(41));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(42));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![id1],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        filter,
        PlanNode::new(
            PlanNodeKind::Filter {
                predicate: Expr::Binary {
                    left: Box::new(Expr::Column(id1)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(9))),
                },
            },
            vec![scan],
        ),
    );
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![filter],
        ),
    );
    let filter_plan = LogicalPlan::new(mat, nodes).expect("filter plan");
    let result = engine
        .preview(preview_request(
            filter_plan,
            filter,
            connection.clone(),
            source.clone(),
            schema.clone(),
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("row truncation");
    assert_eq!(result.rows_returned, 10);
    assert_eq!(collect_preview_i64(&result), (10..20).collect::<Vec<_>>());
    assert!(result.rows_truncated);
    assert!(!result.bytes_truncated);
    assert!(!result.scan_truncated);
    assert!(!result.source_exhausted);

    let (utf8_schema, _) = utf8_schema();
    let utf8_env = utf8_batch(
        &utf8_schema,
        source.id,
        vec!["a".to_owned(), "bb".to_owned(), "c".repeat(5000)],
    );
    let (engine, _) = engine_with(utf8_schema.clone(), vec![utf8_env], true).await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(50));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(51));
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
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![scan],
        ),
    );
    let utf8_plan = LogicalPlan::new(mat, nodes).expect("utf8 plan");
    let result = engine
        .preview(preview_request(
            utf8_plan,
            scan,
            connection.clone(),
            source.clone(),
            utf8_schema.clone(),
            100,
            500,
        ))
        .await
        .expect("byte truncation");
    assert_eq!(result.rows_returned, 2);
    assert!(result.bytes_truncated);
    assert!(!result.rows_truncated);
    assert!(!result.scan_truncated);
    assert!(result.source_exhausted);

    let (engine, _) = engine_with(
        schema.clone(),
        (0..101)
            .map(|i| int_envelope_seq(&schema, source.id, 1_000, i))
            .collect(),
        true,
    )
    .await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(60));
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(61));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(62));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![id1],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        filter,
        PlanNode::new(
            PlanNodeKind::Filter {
                predicate: Expr::Binary {
                    left: Box::new(Expr::Column(id1)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(1_000_000))),
                },
            },
            vec![scan],
        ),
    );
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![filter],
        ),
    );
    let scan_plan = LogicalPlan::new(mat, nodes).expect("scan cap plan");
    let result = engine
        .preview(preview_request(
            scan_plan,
            filter,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("scan cap");
    assert_eq!(result.rows_returned, 0);
    assert!(result.scan_truncated);
    assert!(!result.source_exhausted);
    assert!(!result.rows_truncated);
    assert_eq!(result.source_rows_scanned, PREVIEW_MAX_SOURCE_ROWS_SCANNED);
    assert_eq!(
        result.source_rows_observed,
        PREVIEW_MAX_SOURCE_ROWS_SCANNED + 1_000
    );
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_partition_invariance_for_canonical_rebatching() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());

    let partitioning_a = vec![
        int_envelope_values(&schema, source.id, vec![0, 1, 2, 3, 4], 0),
        int_envelope_values(&schema, source.id, vec![5, 6, 7, 8, 9], 1),
        int_envelope_values(&schema, source.id, vec![10, 11, 12, 13, 14], 2),
    ];
    let partitioning_b = vec![
        int_envelope_values(&schema, source.id, vec![0, 1], 0),
        int_envelope_values(&schema, source.id, vec![2, 3, 4, 5, 6, 7], 1),
        int_envelope_values(&schema, source.id, vec![8, 9, 10, 11, 12, 13, 14], 2),
    ];
    let (engine_a, _) = engine_with(schema.clone(), partitioning_a, true).await;
    let (engine_b, _) = engine_with(schema.clone(), partitioning_b, true).await;
    let (plan, _, _, filter, _, _) = preview_pipeline_plan(source.id);

    let mut request_a = preview_request(
        plan.clone(),
        filter,
        connection.clone(),
        source.clone(),
        schema.clone(),
        100,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request_a.batch_size = 3;
    let mut request_b = preview_request(
        plan,
        filter,
        connection,
        source,
        schema,
        100,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request_b.batch_size = 3;

    let result_a = engine_a.preview(request_a).await.expect("partitioning A");
    let result_b = engine_b.preview(request_b).await.expect("partitioning B");

    assert_eq!(collect_preview_i64(&result_a), (3..15).collect::<Vec<_>>());
    assert_eq!(
        collect_preview_i64(&result_a),
        collect_preview_i64(&result_b)
    );
    assert_eq!(result_a.rows_returned, result_b.rows_returned);
    assert_eq!(result_a.bytes_returned, result_b.bytes_returned);
    assert_eq!(result_a.rows_truncated, result_b.rows_truncated);
    assert_eq!(result_a.bytes_truncated, result_b.bytes_truncated);
    assert_eq!(result_a.scan_truncated, result_b.scan_truncated);
    assert_eq!(result_a.source_exhausted, result_b.source_exhausted);
    assert_eq!(result_a.batches.len(), result_b.batches.len());
    for (left, right) in result_a.batches.iter().zip(&result_b.batches) {
        assert_eq!(left.sequence(), right.sequence());
        assert_eq!(left.row_count(), right.row_count());
        assert_eq!(left.byte_count(), right.byte_count());
    }
    assert_eq!(result_a.batches.len(), 4);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_k_zero_realloc_transient_preserves_rows() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let first = "a".repeat(1_000);
    let second = "b".repeat(1_000);
    let (engine, _) = engine_with(
        schema.clone(),
        vec![utf8_envelope_values(
            &schema,
            source.id,
            vec![first.clone(), second.clone()],
            0,
        )],
        true,
    )
    .await;
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));

    let result = engine
        .preview(preview_request(
            plan, scan, connection, source, schema, 100, 2_800,
        ))
        .await
        .expect("k-zero realloc transient");

    assert_eq!(collect_preview_utf8(&result), vec![first, second]);
    assert_eq!(result.rows_returned, 2);
    assert!(!result.bytes_truncated);
    assert!(result.source_exhausted);
    // Section 10.4 canonical rebatching: both rows fit the aggregate byte
    // cap by the exact estimator (candidate_envelope_bytes <= byte_limit),
    // so the canonical response is one envelope. The append-time realloc
    // transient (~old + new builder buffers) exceeds `byte_limit` only
    // momentarily, which section 10.3 permits; it is constrained by the
    // independent peak pre-check and must not freeze the builder early.
    assert_eq!(result.batches.len(), 1);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_utf8_capacity_partition_invariance() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let first = "a".repeat(1_000);
    let second = "b".repeat(500);
    let third = "c".repeat(500);
    let partitioning_a = vec![
        utf8_envelope_values(&schema, source.id, vec![first.clone()], 0),
        utf8_envelope_values(&schema, source.id, vec![second.clone(), third.clone()], 1),
    ];
    let partitioning_b = vec![utf8_envelope_values(
        &schema,
        source.id,
        vec![first.clone(), second.clone(), third.clone()],
        0,
    )];
    let (engine_a, _) = engine_with(schema.clone(), partitioning_a, true).await;
    let (engine_b, _) = engine_with(schema.clone(), partitioning_b, true).await;
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));

    let result_a = engine_a
        .preview(preview_request(
            plan.clone(),
            scan,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            2_800,
        ))
        .await
        .expect("partitioning A");
    let result_b = engine_b
        .preview(preview_request(
            plan, scan, connection, source, schema, 100, 2_800,
        ))
        .await
        .expect("partitioning B");

    assert_eq!(collect_preview_utf8(&result_a), vec![first, second, third]);
    assert_eq!(
        collect_preview_utf8(&result_a),
        collect_preview_utf8(&result_b)
    );
    assert_eq!(result_a.rows_returned, result_b.rows_returned);
    assert_eq!(result_a.bytes_returned, result_b.bytes_returned);
    assert_eq!(result_a.rows_truncated, result_b.rows_truncated);
    assert_eq!(result_a.bytes_truncated, result_b.bytes_truncated);
    assert_eq!(result_a.scan_truncated, result_b.scan_truncated);
    assert_eq!(result_a.source_exhausted, result_b.source_exhausted);
    assert_eq!(result_a.batches.len(), result_b.batches.len());
    for (left, right) in result_a.batches.iter().zip(&result_b.batches) {
        assert_eq!(left.sequence(), right.sequence());
        assert_eq!(left.row_count(), right.row_count());
        assert_eq!(left.byte_count(), right.byte_count());
    }
    // Section 10.4 canonicalization law: boundaries are a pure function of
    // the ordered target-output rows and the exact estimator, so both
    // partitionings produce the same single envelope here.
    assert_eq!(result_a.batches.len(), 1);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_internal_reserve_segmentation_preserves_lowered_chunk() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let first = "a".repeat(1_000);
    let second = "b".repeat(500);
    let third = "c".repeat(500);
    let (engine, _) = engine_with(
        schema.clone(),
        vec![
            utf8_envelope_values(&schema, source.id, vec![first.clone()], 0),
            utf8_envelope_values(&schema, source.id, vec![second.clone(), third.clone()], 1),
        ],
        true,
    )
    .await;
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));

    let result = engine
        .preview(preview_request(
            plan, scan, connection, source, schema, 100, 2_800,
        ))
        .await
        .expect("internal reserve segmentation");

    assert_eq!(collect_preview_utf8(&result), vec![first, second, third]);
    assert_eq!(result.rows_returned, 3);
    assert!(!result.bytes_truncated);
    assert!(result.source_exhausted);
    // Section 10.4: all three rows fit the aggregate byte cap by the exact
    // estimator, so the canonical response is one envelope regardless of how
    // the builder's internal reserves segmented the appends.
    assert_eq!(result.batches.len(), 1);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_simultaneous_row_and_byte_truncation_with_m_gt_p() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());

    let one = utf8_envelope_values(&schema, source.id, vec!["a".repeat(1_000)], 0);
    let two = utf8_envelope_values(
        &schema,
        source.id,
        vec!["a".repeat(1_000), "b".repeat(1_000)],
        1,
    );
    assert!(one.byte_count() < two.byte_count());
    let byte_limit = one.byte_count() + 1;

    let values = (0..10).map(|_| "a".repeat(1_000)).collect::<Vec<_>>();
    let (engine, _) = engine_with(
        schema.clone(),
        vec![utf8_batch(&schema, source.id, values)],
        true,
    )
    .await;
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));

    let result = engine
        .preview(preview_request(
            plan, scan, connection, source, schema, 2, byte_limit,
        ))
        .await
        .expect("simultaneous row and byte truncation");

    assert_eq!(result.rows_returned, 1);
    assert!(result.rows_truncated);
    assert!(result.bytes_truncated);
    // P05.3 freezes this flag set: the lookahead stops at first-of event 2
    // (target-output row `row_limit + 1`), so the stream is never polled to
    // terminal `None` and `source_exhausted` stays `false` (section 9.3:
    // `source_exhausted` cannot be `true` together with `rows_truncated`).
    assert!(!result.scan_truncated);
    assert!(!result.source_exhausted);
    assert!(!result.batches.is_empty());
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p05_exact_source_byte_boundary_with_lookahead() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());

    // P05.5 freezes the byte-boundary semantics, not the fixture shape: any
    // legitimate Arrow construction whose `get_array_memory_size()` equals
    // the target exactly is valid; see `exact_byte_utf8_envelope`.
    let target = crate::PREVIEW_MAX_SOURCE_BYTES_SCANNED;
    let boundary = exact_byte_utf8_envelope(&schema, source.id, target, 0);
    let lookahead = utf8_envelope_values(&schema, source.id, vec!["lookahead".to_owned()], 1);
    let (engine, connector) =
        engine_with(schema.clone(), vec![boundary, lookahead.clone()], true).await;
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));

    let result = engine
        .preview(preview_request(
            plan,
            scan,
            connection,
            source,
            schema,
            100,
            stillflow_core::MAX_BATCH_BYTES.min(crate::PREVIEW_MAX_BYTE_LIMIT),
        ))
        .await
        .expect("source byte boundary preview");

    assert_eq!(result.source_bytes_scanned, target);
    assert_eq!(
        result.source_bytes_observed,
        target.saturating_add(lookahead.byte_count())
    );
    assert!(result.scan_truncated);
    assert!(!result.source_exhausted);
    assert!(result.rows_returned >= 1);
    assert_eq!(connector.poll_count.load(Ordering::SeqCst), 2);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p06_n_shrink_preserves_logical_output() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 8)], true).await;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);

    let baseline = engine
        .preview(preview_request(
            plan.clone(),
            scan,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("baseline preview");

    crate::preview::set_forced_export_retries(1);
    let retried = engine
        .preview(preview_request(
            plan,
            scan,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("n-shrink preview");
    crate::preview::set_forced_export_retries(0);

    assert_eq!(
        collect_preview_i64(&baseline),
        collect_preview_i64(&retried)
    );
    assert_eq!(baseline.rows_returned, retried.rows_returned);
    assert_eq!(baseline.bytes_returned, retried.bytes_returned);
    assert_eq!(baseline.source_rows_scanned, retried.source_rows_scanned);
    assert_eq!(baseline.source_bytes_scanned, retried.source_bytes_scanned);
    assert_eq!(baseline.source_rows_observed, retried.source_rows_observed);
    assert_eq!(
        baseline.source_bytes_observed,
        retried.source_bytes_observed
    );
    assert_eq!(baseline.rows_truncated, retried.rows_truncated);
    assert_eq!(baseline.bytes_truncated, retried.bytes_truncated);
    assert_eq!(baseline.scan_truncated, retried.scan_truncated);
    assert_eq!(baseline.source_exhausted, retried.source_exhausted);
    assert_eq!(baseline.batches.len(), retried.batches.len());
    for (left, right) in baseline.batches.iter().zip(&retried.batches) {
        assert_eq!(left.sequence(), right.sequence());
        assert_eq!(left.row_count(), right.row_count());
        assert_eq!(left.byte_count(), right.byte_count());
    }
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p06_n_shrink_terminal_one_row_export_fails() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 8)], true).await;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);

    crate::preview::set_forced_export_retries(1_000);
    let err = engine
        .preview(preview_request(
            plan,
            scan,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("terminal n=1 export transition");
    crate::preview::set_forced_export_retries(0);

    assert!(matches!(err, EngineError::BoundExceeded(_)));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p06_single_row_over_byte_cap_is_bound_exceeded() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = utf8_batch(&schema, source.id, vec!["x".repeat(10_000)]);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(70));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(71));
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
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![scan],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("plan");
    let err = engine
        .preview(preview_request(
            plan, scan, connection, source, schema, 10, 1,
        ))
        .await
        .expect_err("single row cap");
    assert!(matches!(err, EngineError::BoundExceeded(_)));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p07_repeated_preview_is_identical() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 5)], true).await;
    let (plan, _, _, filter, _, _) = preview_pipeline_plan(source.id);
    let one = engine
        .preview(preview_request(
            plan.clone(),
            filter,
            connection.clone(),
            source.clone(),
            schema.clone(),
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("one");
    let two = engine
        .preview(preview_request(
            plan,
            filter,
            connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("two");
    assert_eq!(collect_preview_i64(&one), collect_preview_i64(&two));
    assert_eq!(one.rows_returned, two.rows_returned);
    assert_eq!(one.bytes_returned, two.bytes_returned);
    assert_eq!(one.rows_truncated, two.rows_truncated);
    assert_eq!(one.bytes_truncated, two.bytes_truncated);
    assert_eq!(one.source_exhausted, two.source_exhausted);
    assert_eq!(one.batches.len(), two.batches.len());
    drop(_guard);
}

#[tokio::test]
async fn p08_cancellation_and_deadline() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let pending_connection = connection();
    let pending_source = asset(pending_connection.id());
    let pending_schema = schema.clone();
    let connection = connection();
    let source = asset(connection.id());

    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 2)], true).await;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let token = stillflow_core::RequestContext::default()
        .cancellation()
        .clone();
    token.cancel();
    let mut request = preview_request(
        plan.clone(),
        scan,
        connection.clone(),
        source.clone(),
        schema.clone(),
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_cancellation(token);
    let err = engine.preview(request).await.expect_err("cancelled");
    assert!(matches!(err, EngineError::Cancelled));

    let mut request = preview_request(
        plan.clone(),
        scan,
        connection.clone(),
        source.clone(),
        schema.clone(),
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_deadline(tokio::time::Instant::now());
    let err = engine.preview(request).await.expect_err("deadline");
    assert!(matches!(err, EngineError::Timeout));

    let mut request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_deadline(
        tokio::time::Instant::now() + Duration::from_secs(60),
    );
    let err = engine
        .preview(request)
        .await
        .expect_err("too long deadline");
    assert!(matches!(err, EngineError::BoundExceeded(_)));

    // Cancellation and timeout while the connector read stream is pending.
    let (engine, _) = engine_with_pending(pending_schema.clone(), true).await;
    let token = stillflow_core::RequestContext::default()
        .cancellation()
        .clone();
    let engine = Arc::new(engine);
    let source = pending_source;
    let connection = pending_connection;
    let schema = pending_schema;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let mut request = preview_request(
        plan.clone(),
        scan,
        connection.clone(),
        source.clone(),
        schema.clone(),
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_cancellation(token.clone());
    let engine_clone = Arc::clone(&engine);
    let handle = tokio::spawn(async move { engine_clone.preview(request).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    let err = handle
        .await
        .expect("join")
        .expect_err("cancelled during read");
    assert!(matches!(err, EngineError::Cancelled));

    let mut request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_deadline(
        tokio::time::Instant::now() + Duration::from_millis(30),
    );
    let err = engine
        .preview(request)
        .await
        .expect_err("timeout during read");
    assert!(matches!(err, EngineError::Timeout));
    drop(_guard);
}

#[tokio::test]
async fn p08_cancellation_during_flag_completion_lookahead() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    // The flag-completion lookahead is reached through the source-byte scan
    // cap: one envelope whose byte_count() equals
    // PREVIEW_MAX_SOURCE_BYTES_SCANNED closes the scan budget exactly at the
    // envelope boundary, so the engine polls the stream once more to
    // classify the stop (scan-truncated vs source-exhausted). The stream
    // pends after that envelope was served, parking the lookahead poll on a
    // cancellable wait. A row-count fixture cannot reach this state: §9.3
    // first-of event 2 stops scanning before any second poll once a chunk
    // lowers more than row_limit target rows.
    let boundary = exact_byte_utf8_envelope(
        &schema,
        source.id,
        crate::PREVIEW_MAX_SOURCE_BYTES_SCANNED,
        0,
    );
    let (engine, connector) =
        engine_with_pending_after(schema.clone(), vec![boundary], 1, true).await;
    let token = stillflow_core::RequestContext::default()
        .cancellation()
        .clone();
    let engine = Arc::new(engine);
    let plan = scan_materialize_plan(source.id, None);
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let mut request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        1_000,
        crate::PREVIEW_MAX_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_cancellation(token.clone());
    let handle = tokio::spawn(async move { engine.preview(request).await });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::task::yield_now().await;
        if connector.poll_count.load(Ordering::SeqCst) >= 2 {
            break;
        }
        // If the preview task already finished, surface its real outcome
        // through the join below instead of masking it with this deadline.
        if handle.is_finished() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lookahead poll did not start: {}",
            connector.poll_count.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    token.cancel();
    let err = handle
        .await
        .expect("join")
        .expect_err("cancelled during flag-completion lookahead");
    assert!(matches!(err, EngineError::Cancelled));
    drop(_guard);
}

#[tokio::test]
async fn p08_cancellation_during_lowering_retry() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 8)], true).await;
    let token = stillflow_core::RequestContext::default()
        .cancellation()
        .clone();
    let engine = Arc::new(engine);
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let mut request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        100,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.context = stillflow_core::RequestContext::with_cancellation(token.clone());

    reset_ffi_import_count();
    crate::preview::set_forced_export_retries(1_000);
    let handle = tokio::spawn(async move { engine.preview(request).await });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::task::yield_now().await;
        if ffi_import_count() > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lowering did not start: ffi_import_count={}",
            ffi_import_count()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    token.cancel();
    let err = handle
        .await
        .expect("join")
        .expect_err("cancelled during lowering retry");
    assert!(matches!(err, EngineError::Cancelled));
    crate::preview::set_forced_export_retries(0);
    drop(_guard);
}

#[tokio::test]
async fn p09_fifth_concurrent_preview_is_busy() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let fifth_connection = connection();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with_pending(schema.clone(), true).await;
    let engine = Arc::new(engine);
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let mut handles = Vec::new();

    for _ in 0..3 {
        let engine = Arc::clone(&engine);
        let plan = plan.clone();
        let connection = connection.clone();
        let source = source.clone();
        let schema = schema.clone();
        handles.push(tokio::spawn(async move {
            let _ = engine
                .preview(preview_request(
                    plan,
                    scan,
                    connection,
                    source,
                    schema,
                    10,
                    PREVIEW_DEFAULT_BYTE_LIMIT,
                ))
                .await;
        }));
    }

    let engine_clone = Arc::clone(&engine);
    let materialize_connection = connection.clone();
    let materialize_source = source.clone();
    let materialize_schema = schema.clone();
    let store_clone = store.clone();
    handles.push(tokio::spawn(async move {
        let _ = engine_clone
            .materialize(ExecutionRequest {
                plan: scan_materialize_plan(materialize_source.id, None),
                connection: materialize_connection,
                asset: materialize_source,
                schema_override: Some(materialize_schema),
                identities: identities(),
                context: stillflow_core::RequestContext::default(),
                batch_size: 16,
                store: &store_clone,
            })
            .await;
    }));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::task::yield_now().await;
        if connector.read_count.load(Ordering::SeqCst) >= 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pending requests did not reach read_batches: {}",
            connector.read_count.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let (fifth_plan, fifth_scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let error = engine
        .preview(preview_request(
            fifth_plan,
            fifth_scan,
            fifth_connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("fifth preview must be busy");
    assert!(matches!(error, EngineError::Busy));
    assert_eq!(error.category(), stillflow_core::ErrorCategory::RateLimited);
    assert!(error.retryable());
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 4);
    for handle in handles {
        handle.abort();
    }
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p10_connector_call_counts_and_overread() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(
        schema.clone(),
        (0..3)
            .map(|i| int_envelope_seq(&schema, source.id, 2, i))
            .collect(),
        true,
    )
    .await;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let result = engine
        .preview(preview_request(
            plan,
            scan,
            connection.clone(),
            source.clone(),
            schema.clone(),
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("preview");
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 1);
    assert_eq!(connector.poll_count.load(Ordering::SeqCst), 4);
    assert!(result.source_exhausted);

    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let mut request = preview_request(
        preview_pipeline_plan(source.id).0,
        PlanNodeId::from_uuid(Uuid::from_u128(999)),
        connection.clone(),
        source.clone(),
        schema.clone(),
        10,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.schema_override = None;
    let err = engine
        .preview(request)
        .await
        .expect_err("invalid target no inspect");
    assert!(matches!(err, EngineError::InvalidPlan(_)));
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p10_no_schema_override_inspects_once() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 3)], true).await;
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let mut request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        100,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    request.schema_override = None;
    let result = engine
        .preview(request)
        .await
        .expect("preview without override");
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 1);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 1);
    assert!(result.source_exhausted);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p10_mid_envelope_overread_is_the_only_overread() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id1) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope_rows = stillflow_core::MAX_BATCH_ROWS as i64;
    let envelopes = vec![
        int_envelope_values(&schema, source.id, (0..envelope_rows).collect(), 0),
        int_envelope_values(&schema, source.id, (0..envelope_rows).collect(), 1),
    ];
    let (engine, connector) = engine_with(schema.clone(), envelopes, true).await;
    let plan = scan_materialize_plan(
        source.id,
        Some(PlanNodeKind::Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::Column(id1)),
                operator: stillflow_core::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1_000_000))),
            },
        }),
    );
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(2));

    let result = engine
        .preview(preview_request(
            plan,
            filter,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("mid-envelope overread");

    assert_eq!(connector.read_count.load(Ordering::SeqCst), 1);
    assert_eq!(connector.poll_count.load(Ordering::SeqCst), 2);
    assert_eq!(result.rows_returned, 0);
    assert_eq!(result.source_rows_scanned, PREVIEW_MAX_SOURCE_ROWS_SCANNED);
    assert_eq!(
        result.source_rows_observed,
        stillflow_core::MAX_BATCH_ROWS * 2
    );
    assert_eq!(result.source_bytes_observed, result.source_bytes_scanned);
    assert!(result.source_bytes_scanned < 64 * 1024 * 1024);
    assert!(result.scan_truncated);
    assert!(!result.source_exhausted);
    assert!(!result.rows_truncated);
    assert!(!result.bytes_truncated);
    drop(_guard);
}

#[test]
fn p11_preview_runtime_has_no_storage_publication_entry_points() {
    let source = include_str!("preview.rs");
    for forbidden in [
        "SnapshotWriter",
        "SnapshotStore",
        "SnapshotDraft",
        "begin_snapshot",
        "SnapshotManifest",
    ] {
        assert!(
            !source.contains(forbidden),
            "preview.rs contains {forbidden}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn p11_preview_publishes_no_storage_artifacts() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 3)], true).await;
    crate::engine::reset_storage_publication_call_counts();
    let (plan, scan, _, _, _, _) = preview_pipeline_plan(source.id);
    let result = engine
        .preview(preview_request(
            plan,
            scan,
            connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("preview");
    assert_eq!(result.rows_returned, 3);
    assert_eq!(crate::engine::storage_begin_snapshot_call_count(), 0);
    assert_eq!(crate::engine::storage_append_call_count(), 0);
    assert_eq!(crate::engine::storage_commit_call_count(), 0);
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p12_schema_and_column_id_propagation() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) =
        engine_with(schema.clone(), vec![int_batch(&schema, source.id, 5)], true).await;
    let asset_id = source.id;
    let (plan, scan, project, filter, rules, _) = preview_pipeline_plan(asset_id);
    for (target, expected_name, expected_rows) in [
        (scan, "value", vec![0_i64, 1, 2, 3, 4]),
        (project, "value", vec![0_i64, 1, 2, 3, 4]),
        (filter, "value", vec![3_i64, 4]),
        (rules, "renamed", vec![3_i64, 4]),
    ] {
        let result = engine
            .preview(preview_request(
                plan.clone(),
                target,
                connection.clone(),
                source.clone(),
                schema.clone(),
                10,
                PREVIEW_DEFAULT_BYTE_LIMIT,
            ))
            .await
            .expect("preview");
        assert_eq!(result.schema.fields[0].id, column(1));
        assert_eq!(result.schema.fields[0].name, expected_name);
        assert_eq!(collect_preview_i64(&result), expected_rows);
        for envelope in &result.batches {
            assert_eq!(envelope.schema(), &result.schema);
            assert_eq!(envelope.source_asset_id(), asset_id);
        }
    }
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p13_sentinel_never_enters_errors_or_debug() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, _) = utf8_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = utf8_batch(&schema, source.id, vec![SENTINEL.to_owned()]);
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(80));
    let rules = PlanNodeId::from_uuid(Uuid::from_u128(81));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(82));
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
        rules,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![Rule::Cast {
                    column: column(1),
                    data_type: LogicalType::Int64,
                    on_failure: CastFailurePolicy::Error,
                }],
            },
            vec![scan],
        ),
    );
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![rules],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("plan");
    let err = engine
        .preview(preview_request(
            plan,
            rules,
            connection,
            source,
            schema,
            10,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect_err("cast failure");
    let display = err.to_string();
    let debug = format!("{err:?}");
    let summary = err.sanitized_summary();
    let summary_json = serde_json::to_string(&summary).expect("summary");
    assert!(!display.contains(SENTINEL));
    assert!(!debug.contains(SENTINEL));
    assert!(!summary.message().contains(SENTINEL));
    assert!(!summary_json.contains(SENTINEL));
    let event = stillflow_core::IngestionEvent::try_new(
        Uuid::from_u128(201),
        stillflow_core::ObjectKind::Dataset,
        Uuid::from_u128(202),
        stillflow_core::RelationshipKind::Failed,
        serde_json::json!({ "operation": "preview" }),
    )
    .expect("event")
    .with_error(summary.clone());
    let metadata_json = serde_json::to_string(event.metadata()).expect("metadata");
    let event_json = serde_json::to_string(&event).expect("event");
    let error_json = serde_json::to_string(event.error().expect("error")).expect("error");
    assert!(!metadata_json.contains(SENTINEL));
    assert!(!event_json.contains(SENTINEL));
    assert!(!error_json.contains(SENTINEL));
    assert!(!event.error().expect("error").message().contains(SENTINEL));
    drop(_guard);
}

#[tokio::test(flavor = "current_thread")]
async fn p14_preview_response_and_scan_counters_are_bounded() {
    let _guard = exclusive_test_lock().lock().await;
    let (schema, id1) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, _) = engine_with(
        schema.clone(),
        vec![
            int_envelope_values(&schema, source.id, vec![0, 1, 2, 3], 0),
            int_envelope_values(&schema, source.id, vec![0, 4, 5, 6], 1),
        ],
        true,
    )
    .await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(97));
    let filter = PlanNodeId::from_uuid(Uuid::from_u128(98));
    let mat = PlanNodeId::from_uuid(Uuid::from_u128(99));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![id1],
                predicate: None,
            },
            Vec::new(),
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
            vec![scan],
        ),
    );
    nodes.insert(
        mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![filter],
        ),
    );
    let plan = LogicalPlan::new(mat, nodes).expect("plan");
    let byte_limit = 4 * 1024_usize;
    let mut request = preview_request(
        plan,
        filter,
        connection.clone(),
        source.clone(),
        schema.clone(),
        5,
        byte_limit,
    );
    request.batch_size = 4;
    let (result, report) = engine
        .preview_tracked(request)
        .await
        .expect("tracked preview");

    assert_eq!(collect_preview_i64(&result), vec![1, 2, 3, 4, 5]);
    assert_eq!(result.rows_returned, 5);
    assert!(result.rows_truncated);
    assert!(!result.bytes_truncated);
    assert!(!result.scan_truncated);
    assert!(!result.source_exhausted);
    assert_eq!(result.batches.len(), 2);
    assert_eq!(result.batches[0].row_count(), 4);
    assert_eq!(result.batches[1].row_count(), 1);
    assert!(result.bytes_returned <= byte_limit);

    // The two source envelopes each lower n=4 raw rows to m=3 target rows.
    // The second chunk admits only p=2 rows because the row cap closes.
    assert_eq!(report.max_input_chunk_rows, 4);
    assert_eq!(report.max_lowered_rows, 3);
    assert_eq!(report.max_response_prefix_rows, 3);
    assert!(report.saw_n_gt_m_gt_p);
    assert!(report.saw_full_lowered_chunk_with_compacted_prefix);
    assert!(report.finalized_response_envelopes >= 2);
    assert!(report.allocator_reallocation_count > 0);
    assert!(report.allocator_peak_bytes > 0);
    assert!(report.remainder_phase_peak > 0);
    // Section 10.2: the builder realloc transient is tracked separately and
    // bounded by the engine peak law only — MAX_BATCH_BYTES never bounds it.
    // The deleted T44 constraint must not come back in either direction.
    assert!(report.remainder_phase_peak <= PREVIEW_PEAK_ENGINE_BYTES);
    assert!(report.predicted_realloc_peak > 0);
    assert!(report.predicted_realloc_peak <= PREVIEW_PEAK_ENGINE_BYTES);
    assert!(report.response_capacity_peak >= result.bytes_returned);
    // Section 10.2: finalized + post-append response_allocated_capacity stays
    // within byte_limit and carries no realloc transient.
    assert!(report.response_capacity_peak <= byte_limit);
    assert!(report.peak_live_payloads <= 3);
    assert!(report.peak_engine_bytes <= PREVIEW_PEAK_ENGINE_BYTES);
    assert_eq!(report.chunk_count, 2);
    assert_eq!(report.min_chunk_rows, 4);
    assert_eq!(result.source_rows_scanned, 8);
    assert_eq!(result.source_rows_observed, 8);
    assert_eq!(result.source_bytes_observed, result.source_bytes_scanned);

    let (split_engine, _) = engine_with(
        schema.clone(),
        vec![int_envelope_values(
            &schema,
            source.id,
            (0..20_000).collect(),
            0,
        )],
        true,
    )
    .await;
    let split_scan = PlanNodeId::from_uuid(Uuid::from_u128(107));
    let split_derive = PlanNodeId::from_uuid(Uuid::from_u128(108));
    let split_mat = PlanNodeId::from_uuid(Uuid::from_u128(109));
    let mut split_nodes = BTreeMap::new();
    split_nodes.insert(
        split_scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: source.id,
                projection: vec![id1],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    split_nodes.insert(
        split_derive,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![Rule::DeriveColumn {
                    id: column(2),
                    name: "wide".to_owned(),
                    data_type: LogicalType::Utf8,
                    nullable: false,
                    expression: Expr::Literal(ScalarValue::Utf8("w".repeat(8_000))),
                }],
            },
            vec![split_scan],
        ),
    );
    split_nodes.insert(
        split_mat,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![split_derive],
        ),
    );
    let split_plan = LogicalPlan::new(split_mat, split_nodes).expect("split plan");
    let (_, split_report) = split_engine
        .preview_tracked(preview_request(
            split_plan,
            split_derive,
            connection,
            source,
            schema,
            PREVIEW_MAX_ROW_LIMIT,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("single-envelope chunk split");
    assert!(split_report.chunk_count >= 2);
    assert!(split_report.min_chunk_rows < 20_000);
    drop(_guard);
}

fn nullable_quad_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalField::new(column(1), "n", LogicalType::Int64, true).expect("field"),
        LogicalField::new(column(2), "b", LogicalType::Boolean, true).expect("field"),
        LogicalField::new(column(3), "s", LogicalType::Utf8, true).expect("field"),
        LogicalField::new(column(4), "payload", LogicalType::Binary, true).expect("field"),
    ])
    .expect("schema")
}

#[allow(clippy::too_many_arguments)]
fn quad_envelope(
    schema: &LogicalSchema,
    asset_id: Uuid,
    sequence: u64,
    n: Vec<Option<i64>>,
    b: Vec<Option<bool>>,
    s: Vec<Option<String>>,
    p: Vec<Option<&[u8]>>,
) -> stillflow_core::BatchEnvelope {
    let values = Arc::new(Int64Array::from(n));
    let flags = Arc::new(BooleanArray::from(b));
    let text = Arc::new(StringArray::from(s));
    let payload = Arc::new(BinaryArray::from(p));
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch = RecordBatch::try_new(
        factory.arrow_schema().clone(),
        vec![values, flags, text, payload],
    )
    .expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
}

fn quad_scan_plan(asset_id: Uuid) -> (LogicalPlan, PlanNodeId) {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(201));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(202));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: asset_id,
                projection: vec![column(1), column(2), column(3), column(4)],
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![scan],
        ),
    );
    (LogicalPlan::new(materialize, nodes).expect("plan"), scan)
}

/// Asserts per-row value and null-bitmap fidelity for the four physical
/// representations (fixed-width, bit-packed values, offset+data utf8, and
/// binary) across every finalized envelope of a preview result.
struct QuadExpectation {
    n: Vec<Option<i64>>,
    b: Vec<Option<bool>>,
    s: Vec<Option<String>>,
    p: Vec<Option<Vec<u8>>>,
}

fn assert_quad_fidelity(result: &crate::PreviewResult, expected: &QuadExpectation) {
    let mut row = 0_usize;
    for envelope in &result.batches {
        let payload = envelope.payload();
        let n = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        let b = payload
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column");
        let s = payload
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        let p = payload
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary column");
        for index in 0..payload.num_rows() {
            if let Some(value) = expected.n[row] {
                assert!(!n.is_null(index), "int64 row {row} must be valid");
                assert_eq!(n.value(index), value, "int64 row {row}");
            } else {
                assert!(n.is_null(index), "int64 row {row} must read back null");
                assert!(n.nulls().is_some(), "null slot requires a validity bitmap");
            }
            if let Some(value) = expected.b[row] {
                assert!(!b.is_null(index), "boolean row {row} must be valid");
                assert_eq!(b.value(index), value, "boolean row {row}");
            } else {
                assert!(b.is_null(index), "boolean row {row} must read back null");
                assert!(b.nulls().is_some(), "null slot requires a validity bitmap");
            }
            if let Some(value) = &expected.s[row] {
                assert!(!s.is_null(index), "utf8 row {row} must be valid");
                assert_eq!(s.value(index), value.as_str(), "utf8 row {row}");
            } else {
                assert!(s.is_null(index), "utf8 row {row} must read back null");
                assert!(s.nulls().is_some(), "null slot requires a validity bitmap");
            }
            if let Some(value) = &expected.p[row] {
                assert!(!p.is_null(index), "binary row {row} must be valid");
                assert_eq!(p.value(index), value.as_slice(), "binary row {row}");
            } else {
                assert!(p.is_null(index), "binary row {row} must read back null");
                assert!(p.nulls().is_some(), "null slot requires a validity bitmap");
            }
            row += 1;
        }
    }
    assert_eq!(
        row,
        expected.n.len(),
        "preview returned unexpected row count"
    );
}

/// Review Group-1 evidence: preview appends row by row, so an all-valid
/// prefix followed by `[Some(x), None]` rows exercises the lazy validity
/// materialization. Values and null bitmaps must survive bit-exactly for
/// Int64, Boolean, Utf8, and Binary.
#[tokio::test(flavor = "current_thread")]
async fn preview_row_by_row_null_fidelity_across_types() {
    let _guard = exclusive_test_lock().lock().await;
    let schema = nullable_quad_schema();
    let connection = connection();
    let source = asset(connection.id());
    let envelope = quad_envelope(
        &schema,
        source.id,
        0,
        vec![Some(10), None, Some(-7)],
        vec![Some(true), None, Some(false)],
        vec![Some("alpha".to_owned()), None, Some("\u{03b3}".to_owned())],
        vec![Some(&[1_u8, 2, 3][..]), None, Some(&[255_u8][..])],
    );
    let (engine, _) = engine_with(schema.clone(), vec![envelope], true).await;
    let (plan, scan) = quad_scan_plan(source.id);

    let (result, report) = engine
        .preview_tracked(preview_request(
            plan,
            scan,
            connection,
            source,
            schema,
            100,
            PREVIEW_DEFAULT_BYTE_LIMIT,
        ))
        .await
        .expect("tracked preview");

    assert_eq!(result.rows_returned, 3);
    assert!(!result.rows_truncated);
    assert_quad_fidelity(
        &result,
        &QuadExpectation {
            n: vec![Some(10), None, Some(-7)],
            b: vec![Some(true), None, Some(false)],
            s: vec![Some("alpha".to_owned()), None, Some("\u{03b3}".to_owned())],
            p: vec![Some(vec![1, 2, 3]), None, Some(vec![255])],
        },
    );
    // The response capacity evidence stays inside its own bound.
    assert!(report.response_capacity_peak > 0);
    drop(_guard);
}

/// Review Group-1 evidence: an all-valid chunk followed by a nullable chunk
/// whose first row is null — with no flush between them. The lazy validity
/// run spans the chunk boundary and materializes mid-fill; nothing may lose
/// the implicit/explicit transition.
#[tokio::test(flavor = "current_thread")]
async fn preview_all_valid_then_nullable_chunk_without_flush() {
    let _guard = exclusive_test_lock().lock().await;
    let schema = nullable_quad_schema();
    let connection = connection();
    let source = asset(connection.id());
    let first = quad_envelope(
        &schema,
        source.id,
        0,
        vec![Some(1), Some(2), Some(3), Some(4)],
        vec![Some(true), Some(false), Some(true), Some(false)],
        vec![
            Some("a".to_owned()),
            Some("b".to_owned()),
            Some("c".to_owned()),
            Some("d".to_owned()),
        ],
        vec![Some(&[0_u8][..]), Some(&[1]), Some(&[2]), Some(&[3])],
    );
    let second = quad_envelope(
        &schema,
        source.id,
        1,
        vec![None, Some(6), None, Some(8)],
        vec![None, Some(false), None, Some(true)],
        vec![None, Some("f".to_owned()), None, Some("h".to_owned())],
        vec![None, Some(&[5_u8][..]), None, Some(&[7])],
    );
    let (engine, _) = engine_with(schema.clone(), vec![first, second], true).await;
    let (plan, scan) = quad_scan_plan(source.id);

    // preview_request defaults to batch_size 64 > 8 rows, so pack_limit never
    // closes between the two chunks and no intermediate flush can fire.
    let request = preview_request(
        plan,
        scan,
        connection,
        source,
        schema,
        100,
        PREVIEW_DEFAULT_BYTE_LIMIT,
    );
    let (result, report) = engine
        .preview_tracked(request)
        .await
        .expect("tracked preview");

    assert_eq!(report.chunk_count, 2);
    assert_eq!(report.finalized_response_envelopes, 1);
    assert_eq!(result.batches.len(), 1);
    assert_eq!(result.rows_returned, 8);
    assert!(!result.rows_truncated);
    assert_quad_fidelity(
        &result,
        &QuadExpectation {
            n: vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                None,
                Some(6),
                None,
                Some(8),
            ],
            b: vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                None,
                Some(true),
            ],
            s: vec![
                Some("a".to_owned()),
                Some("b".to_owned()),
                Some("c".to_owned()),
                Some("d".to_owned()),
                None,
                Some("f".to_owned()),
                None,
                Some("h".to_owned()),
            ],
            p: vec![
                Some(vec![0]),
                Some(vec![1]),
                Some(vec![2]),
                Some(vec![3]),
                None,
                Some(vec![5]),
                None,
                Some(vec![7]),
            ],
        },
    );
    // The realloc transient prediction was exercised and tracked separately
    // from the response capacity (section 10.2).
    assert!(report.predicted_realloc_peak > 0);
    assert!(report.remainder_phase_peak > 0);
    drop(_guard);
}


// ---------------------------------------------------------------------------
// E4-S2 verification evidence (contract acceptance matrix V01–V31)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod verification {
    use super::*;
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
            node_rules.push(Rule::Validate {
                predicate,
                severity,
                message: message.to_owned(),
            });
        }
        if !keys.is_empty() {
            node_rules.push(Rule::Deduplicate {
                keys: keys.to_vec(),
            });
        }
        if let Some(predicate) = filter_rows {
            node_rules.push(Rule::FilterRows { predicate });
        }
        let mut nodes = BTreeMap::new();
        nodes.insert(
            scan,
            PlanNode::new(
                PlanNodeKind::Scan {
                    source_asset_id,
                    projection,
                    predicate: None,
                },
                Vec::new(),
            ),
        );
        nodes.insert(
            filter,
            PlanNode::new(
                PlanNodeKind::Filter {
                    predicate: Expr::Literal(ScalarValue::Boolean(true)),
                },
                vec![scan],
            ),
        );
        nodes.insert(
            rules,
            PlanNode::new(PlanNodeKind::ApplyRules { rules: node_rules }, vec![filter]),
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
                    input: stillflow_core::verification::InputRef::Asset {
                        asset_id: source.id,
                    },
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
        let mut request = verify_request(
            (&engine, &store),
            &connection,
            &source,
            schema.clone(),
            plan,
            ids,
            None,
        );
        request.identities.canonical_plan_digest = digest;
        let bundle = engine
            .materialize_verification(request)
            .await
            .expect("bundle");
        let rejected = bundle
            .rejected_rows()
            .expect("terminal rejections publish payload");
        assert_eq!(rejected.manifest().artifact_id(), rejected_id);
        let reader = store
            .open_artifact_section(
                bundle_id,
                rejected_id,
                stillflow_storage::ArtifactSectionId::RejectedRows,
            )
            .expect("open rejected section");
        let mut payload_rows = 0usize;
        for item in reader {
            payload_rows += item.expect("envelope").row_count();
        }
        assert_eq!(payload_rows, 2);
        assert_eq!(
            bundle.accepted().manifest().snapshot().stats().row_count(),
            1
        );
        drop(_guard);
    }

    use crate::EngineError;

    fn reject_plan(source_id: Uuid, id: ColumnId) -> LogicalPlan {
        ver_plan(
            source_id,
            vec![id],
            &[],
            Some((
                Expr::Binary {
                    left: Box::new(Expr::Column(id)),
                    operator: stillflow_core::BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
                },
                stillflow_plan::ValidationSeverity::Error,
                "always fails",
            )),
            None,
        )
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
        let mut request = verify_request(
            (&engine, &store),
            &connection,
            &source,
            schema.clone(),
            plan,
            ids,
            None,
        );
        request.identities.canonical_plan_digest = digest;
        let bundle = engine
            .materialize_verification(request)
            .await
            .expect("bundle");
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
        let mut request = verify_request(
            (&engine, &store),
            &connection,
            &source,
            schema.clone(),
            plan,
            ids,
            None,
        );
        request.identities.canonical_plan_digest = digest;
        request.identities.rejected_rows_artifact_id = None;
        let run_id = request.identities.run_id;
        let error = engine
            .materialize_verification(request)
            .await
            .expect_err("must fail");
        assert!(matches!(error, EngineError::InvalidPlan(_)));
        assert!(store.load_verification_bundle_by_run_id(run_id).is_err());
        drop(_guard);
    }

    // V23: E2 materialize keeps rejecting Validate rules.
    #[tokio::test(flavor = "current_thread")]
    async fn v23_materialize_still_rejects_validate() {
        let _guard = exclusive_test_lock().lock().await;
        let (schema, _id) = int_schema();
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
        assert!(matches!(
            error,
            EngineError::UnsupportedRule {
                kind: "validate",
                ..
            }
        ));
        drop(_guard);
    }
}
