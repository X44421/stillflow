//! Measurement fixtures and neutrality tests for the predictor instrumentation
//! (issue #284 [O0-P1]).
//!
//! Layers:
//! 1. Output-neutrality tests that run in BOTH modes (`predict-metrics`
//!    enabled and disabled): predicted values and `largest_feasible_k`
//!    outputs must be identical across instrumentation resets, and the
//!    disabled snapshot must stay all-zero while the predictor runs.
//! 2. Exact per-call attribution tests (enabled mode only): counters around a
//!    single `largest_feasible_k`/`predict` call must equal a replay-derived
//!    expectation, proving the counters observe real work instead of being
//!    inferred from source.
//! 3. End-to-end fixture tests (both modes compile): the issue's fixture
//!    matrix driven through `ExecutionEngine::materialize_tracked`, printing
//!    one JSON line per timed run (`PM_RUN`) and a summary (`PM_SUMMARY`).
//!    Run these under `flock` with `--test-threads=1 --nocapture`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use stillflow_connectors::{
    ConnectorCapabilities, ConnectorRegistry, RawBatchStream, SourceConnector, SourceConnectorRef,
};
use stillflow_core::{
    AssetKind, AssetLocator, BatchEnvelopeFactory, ColumnId, ConnectionStatus, ConnectorKind,
    ConnectorResult, CredentialRef, DiscoverRequest, Expr, InspectRequest, LogicalField,
    LogicalSchema, LogicalType, ReadRequest, ScalarValue, SourceAsset, SourceConnection,
    TestConnectionRequest,
};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule};
use stillflow_storage::{SnapshotStore, StorageLimits};
use uuid::Uuid;

use crate::predict::{largest_feasible_k, predict, PredictedSchema};
use crate::predict_metrics::{self, PredictMetricsSnapshot};
use crate::{
    ExecutionEngine, ExecutionIdentities, ExecutionRequest, PreviewRequest, ENGINE_MAX_DEADLINE,
    PREVIEW_DEFAULT_BYTE_LIMIT,
};

/// True when the `predict-metrics` feature is compiled in.
const INSTRUMENTED: bool = cfg!(feature = "predict-metrics");

/// Timed repetitions per fixture/row-count (>= 5 per the measurement policy).
const MEASUREMENT_RUNS: usize = 7;

// The counters are process-global, so concurrent tests (including unrelated
// engine tests that run the predictor) must not overlap a measurement window;
// every metrics test takes the engine test lock.

// ---------------------------------------------------------------------------
// Shared helpers (self-contained; does not reach into tests.rs fixtures)
// ---------------------------------------------------------------------------

fn col(id: u128) -> ColumnId {
    ColumnId::from_uuid(Uuid::from_u128(id))
}

fn schema_of(fields: Vec<LogicalField>) -> LogicalSchema {
    LogicalSchema::new(fields).expect("schema")
}

fn int_field(id: u128, name: &str) -> LogicalField {
    LogicalField::new(col(id), name.to_owned(), LogicalType::Int64, false).expect("field")
}

fn utf8_field(id: u128, name: &str) -> LogicalField {
    LogicalField::new(col(id), name.to_owned(), LogicalType::Utf8, false).expect("field")
}

fn int_values(rows: usize) -> ArrayRef {
    Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>()))
}

fn utf8_values(rows: usize, width: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec!["x".repeat(width); rows]))
}

fn envelope(
    schema: &LogicalSchema,
    asset_id: Uuid,
    sequence: u64,
    columns: Vec<ArrayRef>,
) -> stillflow_core::BatchEnvelope {
    let factory =
        BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), asset_id).expect("factory");
    let batch = RecordBatch::try_new(factory.arrow_schema().clone(), columns).expect("batch");
    factory.try_build(sequence, batch).expect("envelope")
}

fn connection() -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "predict-metrics-fixture",
        serde_json_fixture_root(),
        CredentialRef::new("cred://local/fixture").expect("cred"),
    )
    .expect("connection")
}

fn serde_json_fixture_root() -> serde_json::Value {
    serde_json::json!({ "root": "/data/predict-metrics-fixture" })
}

fn asset(connection_id: Uuid) -> SourceAsset {
    SourceAsset {
        id: Uuid::from_u128(42),
        connection_id,
        kind: AssetKind::File,
        name: "fixture.csv".to_owned(),
        locator: AssetLocator {
            path: "/fixture.csv".to_owned(),
            container: None,
            schema: None,
            sheet: None,
            workbook_region: None,
        },
        discovered_at: chrono::Utc::now(),
    }
}

fn identities() -> ExecutionIdentities {
    let now = chrono::Utc::now();
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

/// Minimal scripted source connector yielding the same envelopes on every read.
struct FixtureConnector {
    schema: LogicalSchema,
    envelopes: Mutex<Vec<stillflow_core::BatchEnvelope>>,
    read_count: AtomicUsize,
}

#[async_trait]
impl SourceConnector for FixtureConnector {
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
        request: stillflow_core::PreviewRequest,
    ) -> ConnectorResult<stillflow_core::PreviewData> {
        request.context.ensure_active()?;
        Ok(stillflow_core::PreviewData::empty(self.schema.clone()))
    }

    async fn read_batches(
        &self,
        _connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<RawBatchStream> {
        request.context.ensure_active()?;
        self.read_count.fetch_add(1, Ordering::SeqCst);
        let envelopes = self.envelopes.lock().expect("fixture lock").clone();
        Ok(RawBatchStream::new(Box::pin(futures::stream::iter(
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

fn fixture_engine(
    schema: LogicalSchema,
    envelopes: Vec<stillflow_core::BatchEnvelope>,
) -> ExecutionEngine {
    let connector = Arc::new(FixtureConnector {
        schema,
        envelopes: Mutex::new(envelopes),
        read_count: AtomicUsize::new(0),
    });
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::clone(&connector) as SourceConnectorRef)
        .expect("register");
    ExecutionEngine::new(registry)
}

/// Scan -> `chain` -> Materialize logical plan.
fn chain_plan(asset_id: Uuid, projection: Vec<ColumnId>, chain: Vec<PlanNodeKind>) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
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
    let mut parent = scan;
    for (index, kind) in chain.into_iter().enumerate() {
        let id = PlanNodeId::from_uuid(Uuid::from_u128(100 + index as u128));
        nodes.insert(id, PlanNode::new(kind, vec![parent]));
        parent = id;
    }
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(999));
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![parent],
        ),
    );
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Layer 1: output neutrality (both modes)
// ---------------------------------------------------------------------------

/// Int64 source + a 2048-byte literal derive: same shape as tests t43/t46, so
/// `largest_feasible_k` lands strictly inside (0, remaining) and the binary
/// search exercises both feasible and infeasible probes.
fn near_limit_fixture() -> (
    LogicalSchema,
    Vec<ArrayRef>,
    Vec<crate::preflight::CompiledStep>,
) {
    let schema = schema_of(vec![int_field(1, "value")]);
    let rows = 200_000_usize;
    let arrays: Vec<ArrayRef> = vec![int_values(rows)];
    let steps = vec![crate::preflight::CompiledStep::Rules {
        rules: vec![Rule::DeriveColumn {
            id: col(2),
            name: "wide".to_owned(),
            data_type: LogicalType::Utf8,
            nullable: false,
            expression: Expr::Literal(ScalarValue::Utf8("a".repeat(2048))),
        }],
    }];
    (schema, arrays, steps)
}

#[test]
fn metrics_predict_outputs_stable_across_resets() {
    let _guard = crate::tests::exclusive_test_lock().blocking_lock();
    let (schema, arrays, steps) = near_limit_fixture();
    let predicted = PredictedSchema::from_scan_output(&schema);
    let remaining = 200_000_usize;

    predict_metrics::reset();
    let k1 = largest_feasible_k(remaining, 0, &arrays, &predicted, &steps).expect("k1");
    let bytes1 = predict(k1, 0, &arrays, &predicted, &steps).expect("bytes1");
    let single_row1 = predict(1, 0, &arrays, &predicted, &steps).expect("single-row");

    // A reset (a no-op when disabled) must not change any predicted output.
    predict_metrics::reset();
    let k2 = largest_feasible_k(remaining, 0, &arrays, &predicted, &steps).expect("k2");
    let bytes2 = predict(k2, 0, &arrays, &predicted, &steps).expect("bytes2");
    let single_row2 = predict(1, 0, &arrays, &predicted, &steps).expect("single-row2");

    assert_eq!(k1, k2, "largest_feasible_k changed across metric resets");
    assert_eq!(bytes1, bytes2, "predict bytes changed across metric resets");
    assert_eq!(single_row1, single_row2);
    assert!(single_row1 <= stillflow_core::MAX_BATCH_BYTES);
    assert!(bytes1 <= stillflow_core::MAX_BATCH_BYTES);
    assert!(
        predict(k1 + 1, 0, &arrays, &predicted, &steps).expect("k1+1")
            > stillflow_core::MAX_BATCH_BYTES,
        "k is not the boundary value"
    );
    assert!(k1 > 1 && k1 < remaining);
}

#[test]
fn metrics_disabled_snapshot_stays_zero() {
    let _guard = crate::tests::exclusive_test_lock().blocking_lock();
    let (schema, arrays, steps) = near_limit_fixture();
    let predicted = PredictedSchema::from_scan_output(&schema);
    let before = predict_metrics::snapshot();
    let _ = largest_feasible_k(200_000, 0, &arrays, &predicted, &steps).expect("k");
    let _ = predict(17, 0, &arrays, &predicted, &steps).expect("predict");
    let after = predict_metrics::snapshot();
    if INSTRUMENTED {
        assert!(after.predict_probes > before.predict_probes);
    } else {
        // Disabled build: the snapshot is the all-zero default regardless of
        // predictor activity (behavior-neutral by construction).
        assert_eq!(after, PredictMetricsSnapshot::default());
        assert_eq!(after.clone_total(), 0);
        assert_eq!(after.lfk_calls, 0);
    }
}

// ---------------------------------------------------------------------------
// Layer 2: exact per-call attribution (enabled mode only)
// ---------------------------------------------------------------------------

/// Replays the `largest_feasible_k` binary search with the same probe
/// sequence, returning the answer and the exact probe `k` values (including
/// the mandatory single-row probe).
#[cfg(feature = "predict-metrics")]
fn replay_lfk(
    row_count: usize,
    offset: usize,
    arrays: &[ArrayRef],
    schema: &PredictedSchema,
    steps: &[crate::preflight::CompiledStep],
) -> (usize, Vec<usize>) {
    let remaining = row_count.saturating_sub(offset);
    if remaining == 0 {
        return (0, Vec::new());
    }
    let mut probes = vec![1_usize];
    assert!(
        predict(1, offset, arrays, schema, steps).expect("single-row")
            <= stillflow_core::MAX_BATCH_BYTES
    );
    let mut low = 1_usize;
    let mut high = remaining;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        probes.push(mid);
        if predict(mid, offset, arrays, schema, steps).expect("probe")
            <= stillflow_core::MAX_BATCH_BYTES
        {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    (low, probes)
}

/// Copy of a snapshot with the non-deterministic timer fields zeroed, so
/// counter determinism can be compared across runs.
#[cfg(feature = "predict-metrics")]
fn counter_view(snap: PredictMetricsSnapshot) -> PredictMetricsSnapshot {
    let mut view = snap;
    view.lfk_wall_ns = 0;
    view.predict_wall_ns = 0;
    view.refresh_source_widths_wall_ns = 0;
    view.column_physical_sum_wall_ns = 0;
    view.export_transition_wall_ns = 0;
    view
}

#[cfg(feature = "predict-metrics")]
#[test]
fn metrics_enabled_fixed_width_probe_attribution_exact() {
    let _guard = crate::tests::exclusive_test_lock().blocking_lock();
    let (schema, arrays, steps) = near_limit_fixture();
    let predicted = PredictedSchema::from_scan_output(&schema);
    let remaining = 200_000_usize;

    predict_metrics::reset();
    let k = largest_feasible_k(remaining, 0, &arrays, &predicted, &steps).expect("k");
    let observed = predict_metrics::snapshot();

    // Replay in a separate counter window to derive the expected probe shape.
    predict_metrics::reset();
    let (replayed_k, probes) = replay_lfk(remaining, 0, &arrays, &predicted, &steps);
    assert_eq!(replayed_k, k);

    // Determinism: a fresh measured run reproduces the counter snapshot
    // exactly (timers are excluded; they are wall-clock measurements).
    predict_metrics::reset();
    let k2 = largest_feasible_k(remaining, 0, &arrays, &predicted, &steps).expect("k2");
    let repeated = predict_metrics::snapshot();
    assert_eq!(k2, k);
    assert_eq!(counter_view(repeated), counter_view(observed));

    let probe_count = probes.len() as u64;
    assert_eq!(observed.lfk_calls, 1);
    assert_eq!(observed.predict_probes, probe_count);
    assert!(observed.predict_wall_ns > 0);
    assert!(observed.lfk_wall_ns > 0);

    // Schema clones: one working-init clone per probe plus one clone for the
    // DeriveColumn rule per probe; no Project or Filter clones.
    assert_eq!(observed.clone_working_init, probe_count);
    assert_eq!(observed.clone_project, 0);
    assert_eq!(observed.clone_filter, 0);
    assert_eq!(observed.clone_rule, probe_count);
    assert_eq!(observed.clone_total(), 2 * probe_count);

    // Source width refresh: one call per probe; the single Int64 source column
    // is refreshed but never row-scanned (fixed slot width).
    assert_eq!(observed.refresh_source_widths_calls, probe_count);
    assert_eq!(observed.source_columns_refreshed, probe_count);
    assert_eq!(observed.max_variable_width_calls, 0);
    assert_eq!(observed.width_scan_rows, 0);
    assert_eq!(observed.width_scan_value_bytes, 0);
    assert_eq!(observed.variable_data_bytes_calls, 0);

    // Per probe: 1 initial sum over 1 column, 1 derived-column temporary byte
    // computation, and the export transition visiting 2 columns twice.
    assert_eq!(observed.column_physical_sum_calls, probe_count);
    assert_eq!(observed.column_physical_sum_columns, probe_count);
    assert_eq!(observed.derive_temp_byte_calls, probe_count);
    assert_eq!(observed.to_logical_schema_calls, 0);
    assert_eq!(observed.project_full_recomputes, 0);
    assert_eq!(observed.rule_full_recomputes, 0);
    assert_eq!(
        observed.column_physical_bytes_calls,
        6 * probe_count,
        "1 initial sum + 1 derive temp + 4 export per probe"
    );
    assert_eq!(observed.export_transition_calls, probe_count);
    assert_eq!(observed.export_transition_columns, 2 * probe_count);
    assert_eq!(
        observed.export_transition_column_byte_calls,
        4 * probe_count
    );
}

#[cfg(feature = "predict-metrics")]
#[test]
fn metrics_enabled_variable_width_scan_attribution_exact() {
    let _guard = crate::tests::exclusive_test_lock().blocking_lock();
    let width = 32_usize;
    let rows = 5_000_usize;
    let schema = schema_of(vec![utf8_field(1, "text")]);
    let arrays: Vec<ArrayRef> = vec![utf8_values(rows, width)];
    let predicted = PredictedSchema::from_scan_output(&schema);
    let steps: Vec<crate::preflight::CompiledStep> = Vec::new();

    predict_metrics::reset();
    let k = largest_feasible_k(rows, 0, &arrays, &predicted, &steps).expect("k");
    let observed = predict_metrics::snapshot();

    predict_metrics::reset();
    let (replayed_k, probes) = replay_lfk(rows, 0, &arrays, &predicted, &steps);
    assert_eq!(replayed_k, k);
    assert_eq!(k, rows, "expected the fixture to fit one chunk");

    let probe_count = probes.len() as u64;
    let rows_total: u64 = probes.iter().map(|&probe_k| probe_k as u64).sum();
    assert_eq!(observed.lfk_calls, 1);
    assert_eq!(observed.predict_probes, probe_count);
    assert_eq!(observed.clone_working_init, probe_count);
    assert_eq!(observed.clone_total(), probe_count);

    // Every probe rescans every row of the probe window: rows examined is the
    // sum of probe k values, and bytes examined is width x rows.
    assert_eq!(observed.refresh_source_widths_calls, probe_count);
    assert_eq!(observed.source_columns_refreshed, probe_count);
    assert_eq!(observed.max_variable_width_calls, probe_count);
    assert_eq!(observed.width_scan_rows, rows_total);
    assert_eq!(observed.width_scan_value_bytes, width as u64 * rows_total);

    // column_physical_bytes for a variable-width source goes through
    // variable_data_bytes: 1x in the initial sum + 2x in the export pass.
    assert_eq!(observed.variable_data_bytes_calls, 3 * probe_count);
    assert_eq!(observed.variable_data_rows, 3 * rows_total);
    assert_eq!(
        observed.variable_data_span_bytes,
        width as u64 * 3 * rows_total
    );
    assert_eq!(observed.column_physical_bytes_calls, 3 * probe_count);
    assert_eq!(observed.column_physical_sum_calls, probe_count);
    assert_eq!(observed.column_physical_sum_columns, probe_count);
    assert_eq!(observed.export_transition_calls, probe_count);
    assert_eq!(observed.export_transition_columns, probe_count);
    assert_eq!(
        observed.export_transition_column_byte_calls,
        2 * probe_count
    );
}

#[cfg(feature = "predict-metrics")]
#[test]
fn metrics_enabled_step_and_rule_categories_exact() {
    let _guard = crate::tests::exclusive_test_lock().blocking_lock();
    let width = 32_usize;
    let rows = 10_usize;
    let schema = schema_of(vec![
        utf8_field(1, "a"),
        int_field(2, "b"),
        utf8_field(3, "c"),
    ]);
    let arrays: Vec<ArrayRef> = vec![
        utf8_values(rows, width),
        int_values(rows),
        utf8_values(rows, width),
    ];
    let predicted = PredictedSchema::from_scan_output(&schema);
    let steps = vec![
        crate::preflight::CompiledStep::Project {
            columns: vec![col(1), col(2)],
        },
        crate::preflight::CompiledStep::Filter {
            predicate: Expr::IsNull {
                expression: Box::new(Expr::Column(col(2))),
                negated: true,
            },
        },
        crate::preflight::CompiledStep::Rules {
            rules: vec![
                Rule::Trim { column: col(1) },
                Rule::ReplaceLiteral {
                    column: col(1),
                    from: ScalarValue::Utf8("x".repeat(width)),
                    to: ScalarValue::Utf8("yyy".to_owned()),
                },
                Rule::FillNull {
                    column: col(1),
                    value: ScalarValue::Utf8("fill".to_owned()),
                },
                Rule::Cast {
                    column: col(1),
                    data_type: LogicalType::Int64,
                    on_failure: stillflow_plan::CastFailurePolicy::SetNull,
                },
                Rule::DropColumn { column: col(2) },
                Rule::DeriveColumn {
                    id: col(4),
                    name: "d".to_owned(),
                    data_type: LogicalType::Utf8,
                    nullable: false,
                    expression: Expr::Cast {
                        expression: Box::new(Expr::Column(col(1))),
                        data_type: LogicalType::Utf8,
                    },
                },
            ],
        },
    ];

    predict_metrics::reset();
    let peak = predict(rows, 0, &arrays, &predicted, &steps).expect("predict");
    let observed = predict_metrics::snapshot();
    assert!(peak > 0);

    assert_eq!(observed.predict_probes, 1);
    assert_eq!(
        observed.lfk_calls, 0,
        "direct predict does not count lfk calls"
    );

    // One clone per site: working-init, Project, Filter, and one per rule (6).
    assert_eq!(observed.clone_working_init, 1);
    assert_eq!(observed.clone_project, 1);
    assert_eq!(observed.clone_filter, 1);
    assert_eq!(observed.clone_rule, 6);
    assert_eq!(observed.clone_total(), 9);

    assert_eq!(observed.project_full_recomputes, 1);
    assert_eq!(observed.rule_full_recomputes, 5);
    assert_eq!(observed.rule_recompute_trim, 1);
    assert_eq!(observed.rule_recompute_replace_literal, 1);
    assert_eq!(observed.rule_recompute_fill_null, 1);
    assert_eq!(observed.rule_recompute_cast, 1);
    assert_eq!(observed.rule_recompute_drop_column, 1);
    assert_eq!(observed.derive_temp_byte_calls, 1);
    assert_eq!(observed.to_logical_schema_calls, 1);

    // Refresh runs once against the full input schema (3 source columns, two
    // of them utf8 and therefore row-scanned across the 10-row window).
    assert_eq!(observed.refresh_source_widths_calls, 1);
    assert_eq!(observed.source_columns_refreshed, 3);
    assert_eq!(observed.max_variable_width_calls, 2);
    assert_eq!(observed.width_scan_rows, 2 * rows as u64);
    assert_eq!(
        observed.width_scan_value_bytes,
        2 * rows as u64 * width as u64
    );
    assert_eq!(observed.export_transition_calls, 1);
}

// ---------------------------------------------------------------------------
// Layer 3: end-to-end fixture matrix
// ---------------------------------------------------------------------------

fn print_run_line(
    fixture: &str,
    rows: usize,
    run: usize,
    e2e_ns: u64,
    snap: &PredictMetricsSnapshot,
) {
    let predictor_ns = snap.site_ingest_lfk_wall_ns;
    let share = if e2e_ns > 0 {
        100.0 * predictor_ns as f64 / e2e_ns as f64
    } else {
        0.0
    };
    let line = serde_json::json!({
        "mode": if INSTRUMENTED { "enabled" } else { "disabled" },
        "fixture": fixture,
        "rows": rows,
        "run": run,
        "e2e_ms": e2e_ns as f64 / 1_000_000.0,
        "predictor_ms": predictor_ns as f64 / 1_000_000.0,
        "predictor_share_pct": share,
        "lfk_calls": snap.lfk_calls,
        "predict_probes": snap.predict_probes,
        "counters": snap.to_json(),
    });
    println!("PM_RUN {line}");
}

fn print_summary_line(
    fixture: &str,
    rows: usize,
    e2e: &mut [u64],
    pred: &mut [u64],
    share: &mut [f64],
) {
    e2e.sort_unstable();
    pred.sort_unstable();
    let mut share_sorted = share.to_vec();
    share_sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite shares"));
    let e2e_p50 = percentile(e2e, 50.0);
    let e2e_p95 = percentile(e2e, 95.0);
    let pred_p50 = percentile(pred, 50.0);
    let pred_p95 = percentile(pred, 95.0);
    let spread = if e2e_p50 > 0 {
        100.0 * (e2e_p95 as f64 - e2e_p50 as f64) / e2e_p50 as f64
    } else {
        0.0
    };
    let line = serde_json::json!({
        "mode": if INSTRUMENTED { "enabled" } else { "disabled" },
        "fixture": fixture,
        "rows": rows,
        "runs": e2e.len(),
        "e2e_ms_p50": e2e_p50 as f64 / 1_000_000.0,
        "e2e_ms_p95": e2e_p95 as f64 / 1_000_000.0,
        "e2e_spread_p95_vs_p50_pct": spread,
        "predictor_ms_p50": pred_p50 as f64 / 1_000_000.0,
        "predictor_ms_p95": pred_p95 as f64 / 1_000_000.0,
        "predictor_share_p50_pct": share_sorted[share_sorted.len() / 2],
    });
    println!("PM_SUMMARY {line}");
}

/// Drives one fixture/row-count through `materialize_tracked`: one untimed
/// warmup, then `MEASUREMENT_RUNS` timed repetitions with per-run counter
/// windows (reset -> run -> snapshot).
async fn measure_materialize(
    fixture: &str,
    rows: usize,
    schema: LogicalSchema,
    envelopes: Vec<stillflow_core::BatchEnvelope>,
    plan: &LogicalPlan,
    connection: &SourceConnection,
    source: &SourceAsset,
) {
    let _guard = crate::tests::exclusive_test_lock().lock().await;
    let expected_rows = envelopes
        .iter()
        .map(|envelope| envelope.row_count())
        .sum::<usize>();
    assert_eq!(expected_rows, rows, "fixture envelope rows must add up");
    let engine = fixture_engine(schema.clone(), envelopes);

    let mut e2e: Vec<u64> = Vec::new();
    let mut pred: Vec<u64> = Vec::new();
    let mut share: Vec<f64> = Vec::new();

    for run in 0..=MEASUREMENT_RUNS {
        let store_dir = tempfile::TempDir::new().expect("temp");
        let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
        predict_metrics::reset();
        let started = Instant::now();
        let (manifest, _report) = engine
            .materialize_tracked(ExecutionRequest {
                plan: plan.clone(),
                connection: connection.clone(),
                asset: source.clone(),
                schema_override: Some(schema.clone()),
                identities: identities(),
                context: long_context(),
                batch_size: 65_536,
                store: &store,
            })
            .await
            .expect("materialize");
        let e2e_ns = started.elapsed().as_nanos() as u64;
        assert_eq!(
            manifest.snapshot().stats().row_count() as usize,
            rows,
            "fixture must produce every row"
        );
        let snap = predict_metrics::snapshot();
        if run == 0 {
            println!(
                "PM_WARMUP fixture={fixture} rows={rows} e2e_ms={:.3}",
                e2e_ns as f64 / 1_000_000.0
            );
            continue;
        }
        print_run_line(fixture, rows, run, e2e_ns, &snap);
        e2e.push(e2e_ns);
        pred.push(snap.site_ingest_lfk_wall_ns);
        share.push(if e2e_ns > 0 {
            100.0 * snap.site_ingest_lfk_wall_ns as f64 / e2e_ns as f64
        } else {
            0.0
        });
    }
    print_summary_line(fixture, rows, &mut e2e, &mut pred, &mut share);
    if INSTRUMENTED {
        assert!(
            pred.iter().all(|value| *value > 0),
            "predictor time must be observed"
        );
    }
}

/// Splits `rows` rows across envelopes of `rows_per_envelope` so no single
/// envelope approaches the engine peak budget.
fn split_utf8_envelopes(
    schema: &LogicalSchema,
    asset_id: Uuid,
    rows: usize,
    rows_per_envelope: usize,
    width: usize,
    columns: usize,
) -> Vec<stillflow_core::BatchEnvelope> {
    let mut envelopes = Vec::new();
    let mut emitted = 0_usize;
    let mut sequence = 0_u64;
    while emitted < rows {
        let take = (rows - emitted).min(rows_per_envelope);
        let cols: Vec<ArrayRef> = (0..columns).map(|_| utf8_values(take, width)).collect();
        envelopes.push(envelope(schema, asset_id, sequence, cols));
        emitted += take;
        sequence += 1;
    }
    envelopes
}

fn split_int_envelopes(
    schema: &LogicalSchema,
    asset_id: Uuid,
    rows: usize,
    rows_per_envelope: usize,
) -> Vec<stillflow_core::BatchEnvelope> {
    let mut envelopes = Vec::new();
    let mut emitted = 0_usize;
    let mut sequence = 0_u64;
    while emitted < rows {
        let take = (rows - emitted).min(rows_per_envelope);
        envelopes.push(envelope(schema, asset_id, sequence, vec![int_values(take)]));
        emitted += take;
        sequence += 1;
    }
    envelopes
}

/// Fixture 1: narrow fixed-width (1 x Int64), one derive rule.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f1_narrow_fixed_width() {
    let schema = schema_of(vec![int_field(1, "value")]);
    let plan = chain_plan(
        Uuid::from_u128(42),
        vec![col(1)],
        vec![PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: col(2),
                name: "derived".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Literal(ScalarValue::Utf8("y".repeat(64))),
            }],
        }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000, 1_000_000] {
        let envelopes = split_int_envelopes(&schema, Uuid::from_u128(42), rows, 65_536);
        measure_materialize(
            "f1_narrow_fixed",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 2: wide fixed-width (64 x Int64), no rules.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f2_wide_fixed_width() {
    let schema = schema_of(
        (1..=64)
            .map(|index| int_field(index, &format!("c{index}")))
            .collect(),
    );
    let plan = chain_plan(Uuid::from_u128(42), (1..=64).map(col).collect(), Vec::new());
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let mut envelopes = Vec::new();
        let mut emitted = 0_usize;
        let mut sequence = 0_u64;
        while emitted < rows {
            let take = (rows - emitted).min(50_000);
            let columns: Vec<ArrayRef> = (0..64).map(|_| int_values(take)).collect();
            envelopes.push(envelope(&schema, Uuid::from_u128(42), sequence, columns));
            emitted += take;
            sequence += 1;
        }
        measure_materialize(
            "f2_wide_fixed",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 3: wide mixed variable-width (16 x Int64 + 16 x Utf8[32]) plus one
/// derive rule.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f3_wide_mixed_variable() {
    let mut fields = Vec::new();
    for index in 1..=16 {
        fields.push(int_field(index, &format!("i{index}")));
    }
    for index in 17..=32 {
        fields.push(utf8_field(index, &format!("s{index}")));
    }
    let schema = schema_of(fields);
    let plan = chain_plan(
        Uuid::from_u128(42),
        (1..=32).map(col).collect(),
        vec![PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: col(200),
                name: "derived".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Literal(ScalarValue::Utf8("y".repeat(32))),
            }],
        }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let mut envelopes = Vec::new();
        let per_envelope = 50_000_usize;
        let mut emitted = 0_usize;
        let mut sequence = 0_u64;
        while emitted < rows {
            let take = (rows - emitted).min(per_envelope);
            let mut columns: Vec<ArrayRef> = (1..=16).map(|_| int_values(take)).collect();
            columns.extend((0..16).map(|_| utf8_values(take, 32)));
            envelopes.push(envelope(&schema, Uuid::from_u128(42), sequence, columns));
            emitted += take;
            sequence += 1;
        }
        measure_materialize(
            "f3_wide_mixed",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 4: long UTF-8 (2 x Utf8[2048]).
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f4_long_utf8() {
    let schema = schema_of(vec![utf8_field(1, "a"), utf8_field(2, "b")]);
    let plan = chain_plan(Uuid::from_u128(42), vec![col(1), col(2)], Vec::new());
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 50_000] {
        let mut envelopes = Vec::new();
        let per_envelope = 6_000_usize;
        let mut emitted = 0_usize;
        let mut sequence = 0_u64;
        while emitted < rows {
            let take = (rows - emitted).min(per_envelope);
            envelopes.push(envelope(
                &schema,
                Uuid::from_u128(42),
                sequence,
                vec![utf8_values(take, 2048), utf8_values(take, 2048)],
            ));
            emitted += take;
            sequence += 1;
        }
        measure_materialize(
            "f4_long_utf8",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 5: rule-heavy (8 x Utf8[32] + 8 x Int64; 24 mutating rules).
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f5_rule_heavy() {
    let mut fields = Vec::new();
    for index in 1..=8 {
        fields.push(utf8_field(index, &format!("s{index}")));
    }
    for index in 9..=16 {
        fields.push(int_field(index, &format!("i{index}")));
    }
    let schema = schema_of(fields);
    let mut rules = Vec::new();
    for index in 1..=8 {
        rules.push(Rule::Trim { column: col(index) });
        rules.push(Rule::ReplaceLiteral {
            column: col(index),
            from: ScalarValue::Utf8("x".repeat(32)),
            to: ScalarValue::Utf8("yyy".to_owned()),
        });
        rules.push(Rule::FillNull {
            column: col(index),
            value: ScalarValue::Utf8("fill".to_owned()),
        });
    }
    let plan = chain_plan(
        Uuid::from_u128(42),
        (1..=16).map(col).collect(),
        vec![PlanNodeKind::ApplyRules { rules }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let mut envelopes = Vec::new();
        let per_envelope = 50_000_usize;
        let mut emitted = 0_usize;
        let mut sequence = 0_u64;
        while emitted < rows {
            let take = (rows - emitted).min(per_envelope);
            let mut columns: Vec<ArrayRef> = (0..8).map(|_| utf8_values(take, 32)).collect();
            columns.extend((0..8).map(|_| int_values(take)));
            envelopes.push(envelope(&schema, Uuid::from_u128(42), sequence, columns));
            emitted += take;
            sequence += 1;
        }
        measure_materialize(
            "f5_rule_heavy",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 6: Project-heavy (64 mixed columns projected down to 8).
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f6_project_heavy() {
    let mut fields = Vec::new();
    for index in 1..=32 {
        fields.push(int_field(index, &format!("i{index}")));
    }
    for index in 33..=64 {
        fields.push(utf8_field(index, &format!("s{index}")));
    }
    let schema = schema_of(fields);
    let plan = chain_plan(
        Uuid::from_u128(42),
        (1..=64).map(col).collect(),
        vec![PlanNodeKind::Project {
            columns: (1..=8).map(col).collect(),
        }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let mut envelopes = Vec::new();
        let per_envelope = 25_000_usize;
        let mut emitted = 0_usize;
        let mut sequence = 0_u64;
        while emitted < rows {
            let take = (rows - emitted).min(per_envelope);
            let mut columns: Vec<ArrayRef> = (0..32).map(|_| int_values(take)).collect();
            columns.extend((0..32).map(|_| utf8_values(take, 16)));
            envelopes.push(envelope(&schema, Uuid::from_u128(42), sequence, columns));
            emitted += take;
            sequence += 1;
        }
        measure_materialize(
            "f6_project_heavy",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 7: Filter-heavy (8 x Utf8[32]; 32 chained Filter nodes).
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f7_filter_heavy() {
    let schema = schema_of(
        (1..=8)
            .map(|index| utf8_field(index, &format!("s{index}")))
            .collect(),
    );
    let filters: Vec<PlanNodeKind> = (0..32)
        .map(|_| PlanNodeKind::Filter {
            predicate: Expr::IsNull {
                expression: Box::new(Expr::Column(col(1))),
                negated: true,
            },
        })
        .collect();
    let plan = chain_plan(Uuid::from_u128(42), (1..=8).map(col).collect(), filters);
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let envelopes = split_utf8_envelopes(&schema, Uuid::from_u128(42), rows, 50_000, 32, 8);
        measure_materialize(
            "f7_filter_heavy",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 8: expression/derive-heavy (8 x Utf8[32]; 16 Cast-to-UTF8 derives,
/// each forcing `to_logical_schema` + expression typing per probe).
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f8_derive_heavy() {
    let schema = schema_of(
        (1..=8)
            .map(|index| utf8_field(index, &format!("s{index}")))
            .collect(),
    );
    let rules: Vec<Rule> = (0..16)
        .map(|index| Rule::DeriveColumn {
            id: col(200 + index as u128),
            name: format!("d{index}"),
            data_type: LogicalType::Utf8,
            nullable: false,
            expression: Expr::Cast {
                expression: Box::new(Expr::Column(col(1 + (index % 8) as u128))),
                data_type: LogicalType::Utf8,
            },
        })
        .collect();
    let plan = chain_plan(
        Uuid::from_u128(42),
        (1..=8).map(col).collect(),
        vec![PlanNodeKind::ApplyRules { rules }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [10_000_usize, 100_000] {
        let envelopes = split_utf8_envelopes(&schema, Uuid::from_u128(42), rows, 50_000, 32, 8);
        measure_materialize(
            "f8_derive_heavy",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 9: near-limit (1 x Int64 + 2048-byte literal derive). Admission
/// caps k near 32k rows, so 100k-row envelopes force many chunks per envelope
/// and binary searches with both feasible and infeasible probes.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f9_near_limit_many_probes() {
    let schema = schema_of(vec![int_field(1, "value")]);
    let plan = chain_plan(
        Uuid::from_u128(42),
        vec![col(1)],
        vec![PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: col(2),
                name: "wide".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Literal(ScalarValue::Utf8("a".repeat(2048))),
            }],
        }],
    );
    let connection = connection();
    let source = asset(connection.id());
    for rows in [100_000_usize, 500_000, 1_000_000] {
        let envelopes = split_int_envelopes(&schema, Uuid::from_u128(42), rows, 65_536);
        measure_materialize(
            "f9_near_limit",
            rows,
            schema.clone(),
            envelopes,
            &plan,
            &connection,
            &source,
        )
        .await;
    }
}

/// Fixture 10: small input where instrumentation overhead could dominate.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_f10_small_input() {
    let schema = schema_of(vec![utf8_field(1, "text")]);
    let plan = chain_plan(
        Uuid::from_u128(42),
        vec![col(1)],
        vec![PlanNodeKind::ApplyRules {
            rules: vec![Rule::DeriveColumn {
                id: col(2),
                name: "derived".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Literal(ScalarValue::Utf8("y".repeat(8))),
            }],
        }],
    );
    let rows = 100_usize;
    let connection = connection();
    let source = asset(connection.id());
    let envelopes = split_utf8_envelopes(&schema, Uuid::from_u128(42), rows, rows, 32, 1);
    measure_materialize(
        "f10_small",
        rows,
        schema.clone(),
        envelopes,
        &plan,
        &connection,
        &source,
    )
    .await;
}

/// Lights up the preview site (`Site::Preview`) counters through the public
/// preview API.
#[tokio::test(flavor = "current_thread")]
async fn metrics_e2e_preview_site_smoke() {
    let _guard = crate::tests::exclusive_test_lock().lock().await;
    let rows = 2_000_usize;
    let schema = schema_of(vec![utf8_field(1, "text")]);
    let envelopes = split_utf8_envelopes(&schema, Uuid::from_u128(42), rows, rows, 32, 1);
    let plan = chain_plan(Uuid::from_u128(42), vec![col(1)], Vec::new());
    let engine = fixture_engine(schema.clone(), envelopes);
    let connection = connection();
    let source = asset(connection.id());

    predict_metrics::reset();
    let mut request = PreviewRequest::new(
        plan,
        PlanNodeId::from_uuid(Uuid::from_u128(1)),
        connection,
        source,
    );
    request.schema_override = Some(schema);
    request.row_limit = 10_000;
    request.byte_limit = PREVIEW_DEFAULT_BYTE_LIMIT;
    request.batch_size = 512;
    let result = engine.preview(request).await.expect("preview");
    let preview_rows: usize = result.batches.iter().map(|batch| batch.row_count()).sum();
    assert_eq!(preview_rows, rows);

    let snap = predict_metrics::snapshot();
    if INSTRUMENTED {
        assert!(
            snap.site_preview_lfk_calls >= 1,
            "preview site must be observed"
        );
        assert!(snap.site_preview_lfk_wall_ns > 0);
        assert_eq!(snap.site_ingest_lfk_calls, 0, "no ingest chunks in preview");
        println!(
            "PM_PREVIEW preview_lfk_calls={} predict_probes={} wall_ns={}",
            snap.site_preview_lfk_calls, snap.predict_probes, snap.site_preview_lfk_wall_ns
        );
    } else {
        assert_eq!(snap.site_preview_lfk_calls, 0);
    }
}
