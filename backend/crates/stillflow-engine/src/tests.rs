use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
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
use stillflow_plan::{JoinKey, JoinType, LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule};
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

fn exclusive_materialize() -> &'static tokio::sync::Mutex<()> {
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
    let mut nodes = std::collections::BTreeMap::new();
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

#[test]
fn crate_name_is_stable() {
    assert_eq!(crate_name(), "stillflow-engine");
}

#[test]
fn error_categories_match_contract() {
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
    let k = 8_usize;
    let data = k * 4;
    let bytes = utf8_physical_bytes(k, data);
    assert_eq!(bytes, data + k * 16 + (k + 1) * 4 + 1);
}

#[tokio::test(flavor = "current_thread")]
async fn join_is_unsupported_before_inspect() {
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let (engine, connector) = engine_with(schema.clone(), Vec::new(), true).await;
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let join = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let extra_scan = PlanNodeId::from_uuid(Uuid::from_u128(4));
    let mut nodes = std::collections::BTreeMap::new();
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
}

#[tokio::test(flavor = "current_thread")]
async fn t39_fails_before_polars_import() {
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
}

#[tokio::test(flavor = "current_thread")]
async fn t45_date_to_utf8_is_type_error() {
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
}

#[tokio::test(flavor = "current_thread")]
async fn t37_derive_wide_utf8_chunks_before_polars() {
    let _guard = exclusive_materialize().lock().await;
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
}

#[tokio::test(flavor = "current_thread")]
async fn t41_split_envelope_keeps_remainder_with_polars() {
    let _guard = exclusive_materialize().lock().await;
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
}

#[tokio::test(flavor = "current_thread")]
async fn t44_phased_allocator_excludes_storage_encode() {
    let _guard = exclusive_materialize().lock().await;
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
    let engine_phases = report
        .polars_phase_peak
        .saturating_add(report.remainder_phase_peak)
        .saturating_add(MAX_OPERATOR_STATE_BYTES);
    assert!(engine_phases <= MAX_ENGINE_PEAK_BYTES);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
}

#[test]
fn t43_utf8_byte_cap_uses_offset_overhead() {
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
async fn fifth_materialize_is_busy_without_inspect() {
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
    assert_eq!(connector.inspect_count.load(Ordering::SeqCst), 0);
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 0);
    drop(holds);
}

#[test]
fn t46_near_64mib_export_transition_respects_bounds() {
    let (schema, id) = int_schema();
    let values = Int64Array::from((0..10_000_i64).collect::<Vec<_>>());
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
    let k = largest_feasible_k(10_000, 0, batch.columns(), &predicted, &steps).expect("k");
    let peak = predict(k, 0, batch.columns(), &predicted, &steps).expect("predict");
    assert!(peak <= MAX_BATCH_BYTES);
    let _ = id;
}

#[test]
fn t47_4096_columns_no_pack_limit_bulk_preallocation() {
    let fields: Vec<LogicalField> = (0..4096)
        .map(|i| {
            LogicalField::new(column(i + 1), format!("col_{i}"), LogicalType::Int64, false)
                .expect("field")
        })
        .collect();
    let schema = Arc::new(LogicalSchema::new(fields).expect("schema"));
    let rebatcher = crate::remainder::CanonicalRebatcher::new(schema, Uuid::from_u128(99), 65_536)
        .expect("rebatcher");
    assert_eq!(rebatcher.remainder_bytes(), 0);
    assert!(!rebatcher.remainder_live());
}

#[tokio::test(flavor = "current_thread")]
async fn t48_timestamp_timezone_retention() {
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
}

#[tokio::test(flavor = "current_thread")]
async fn t49_iterative_ast_guard_rejects_deep_expression_fast() {
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
}

#[tokio::test(flavor = "current_thread")]
async fn t50_lub_strict_casting_in_comparisons_and_coalesce() {
    let id1 = column(1);
    let id2 = column(2);
    let schema = LogicalSchema::new(vec![
        LogicalField::new(id1, "a", LogicalType::Int32, false).expect("f1"),
        LogicalField::new(id2, "b", LogicalType::Int64, false).expect("f2"),
    ])
    .expect("schema");
    let connection = connection();
    let source = asset(connection.id());
    let a_arr = arrow_array::Int32Array::from(vec![10_i32, 20_i32]);
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
}

#[tokio::test(flavor = "current_thread")]
async fn t51_typed_null_derivation() {
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
}

#[test]
fn t52_float_to_utf8_prediction_bound() {
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
}

#[tokio::test(flavor = "current_thread")]
async fn t53_binary_cast_rejection() {
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
        .expect_err("binary cast");
    assert!(matches!(error, EngineError::TypeError(_)));
}

#[test]
fn t54_fallback_error_sanitization_is_always_internal() {
    let summary = crate::error::EngineError::Internal("test internal").sanitized_summary();
    assert_eq!(summary.category, stillflow_core::ErrorCategory::Internal);
    assert!(!summary.retryable);
}
