use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
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
    ReadRequest, ScalarValue, SourceAsset, SourceConnection, TestConnectionRequest,
};
use stillflow_plan::{JoinKey, JoinType, LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule};
use stillflow_storage::{SnapshotStore, StorageLimits};
use uuid::Uuid;

use crate::error::EngineError;
use crate::predict::{largest_feasible_k, utf8_physical_bytes, PredictedSchema};
use crate::{
    crate_name, ExecutionEngine, ExecutionIdentities, ExecutionRequest, MAX_ENGINE_PEAK_BYTES,
    MAX_LIVE_COLUMNAR_PAYLOADS,
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

fn int_batch(schema: &LogicalSchema, asset_id: Uuid, rows: i64) -> stillflow_core::BatchEnvelope {
    let values: Vec<i64> = (0..rows).collect();
    let array = Int64Array::from(values);
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch =
        RecordBatch::try_new(factory.arrow_schema().clone(), vec![Arc::new(array)]).expect("batch");
    factory.try_build(0, batch).expect("envelope")
}

fn scan_materialize_plan(asset_id: Uuid, extra: Option<PlanNodeKind>) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let mid = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: asset_id,
                projection: vec![column(1)],
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

#[tokio::test]
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

#[tokio::test]
async fn t39_single_row_predicted_expansion_fails_before_import() {
    let (schema, _) = int_schema();
    let connection = connection();
    let source = asset(connection.id());
    let huge = "x".repeat(stillflow_core::MAX_BATCH_BYTES + 1);
    let envelope = int_batch(&schema, source.id, 1);
    let (engine, connector) = engine_with(schema.clone(), vec![envelope], true).await;
    let store_dir = tempfile::TempDir::new().expect("temp");
    let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
    let error = engine
        .materialize(ExecutionRequest {
            plan: derive_plan(source.id, huge),
            connection,
            asset: source,
            schema_override: Some(schema),
            identities: identities(),
            context: stillflow_core::RequestContext::default(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect_err("bound");
    assert!(matches!(error, EngineError::BoundExceeded(_)));
    assert_eq!(connector.read_count.load(Ordering::SeqCst), 1);
    assert!(store.load_manifest(Uuid::from_u128(100)).is_err());
}

#[tokio::test]
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

#[tokio::test]
async fn t37_t41_t44_split_envelope_keeps_remainder() {
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
            context: stillflow_core::RequestContext::default(),
            batch_size: 65_536,
            store: &store,
        })
        .await
        .expect("materialize");
    assert_eq!(manifest.snapshot().stats().row_count(), 65_536);
    assert!(report.chunk_count >= 2);
    assert!(report.min_chunk_rows < 65_536);
    assert!(report.saw_split_envelope_with_remainder);
    assert!(report.peak_live_payloads <= MAX_LIVE_COLUMNAR_PAYLOADS);
    assert!(report.peak_engine_bytes <= MAX_ENGINE_PEAK_BYTES);
    assert!(report.polars_phase_peak > 0);
    assert!(report.remainder_phase_peak > 0);
    let engine_phases = report
        .polars_phase_peak
        .saturating_add(report.remainder_phase_peak)
        .saturating_add(crate::MAX_OPERATOR_STATE_BYTES);
    assert!(engine_phases <= MAX_ENGINE_PEAK_BYTES);
}

#[test]
fn t43_utf8_byte_cap_uses_offset_overhead() {
    let (schema, _) = int_schema();
    let values = Int64Array::from(vec![1_i64, 2, 3]);
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
            expression: Expr::Literal(ScalarValue::Utf8("abcd".repeat(16))),
        }],
    }];
    let k = largest_feasible_k(3, 0, batch.columns(), &predicted, &steps).expect("k");
    assert!(k >= 1);
    assert!(k <= 3);
}

#[tokio::test]
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
