//! NX-N3 (#367) acceptance: explicit temporal parsing and numeric failure
//! behaviour on **real data**.
//!
//! The fixture is NDJSON so a NULL, an invalid value and a valid value are all
//! representable. Every assertion runs through the production NodeGraph compile
//! path and the engine over values produced by a real read.
//!
//! Contract: `docs/contracts/issue-362-nx-c0-twelve-category-capability-matrix.md`
//! §2 category 3 and §3.2; the paused ledger (§4) keeps second-unit timestamps
//! and date→utf8 casts paused, and this suite proves they stay paused.

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, Date32Array, Int32Array, StringArray};
use chrono::{NaiveDate, TimeZone};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, LogicalSchema, LogicalType, NodeConfig, NodeEdge,
    NodeGraph, NodeGraphErrorCode, NodeId, NodePort, NodeRegistry, PortId, RequestContext,
    SourceAsset, SourceConnection, TimeUnit,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler, Rule};
use tempfile::TempDir;
use uuid::Uuid;

/// `clean` is non-nullable and always parseable; `dirty` carries an invalid
/// value and a NULL; `ambiguous` holds a DST-ambiguous local wall time.
const NDJSON: &str = concat!(
    r#"{"id":1,"clean":"2024-03-01","dirty":"2024-03-02","ambiguous":"2024-11-03T01:30:00"}"#,
    "\n",
    r#"{"id":2,"clean":"2024-03-03","dirty":"nope","ambiguous":"2024-03-03T01:30:00"}"#,
    "\n",
    r#"{"id":3,"clean":"2024-03-04","dirty":null,"ambiguous":"2024-03-04T01:30:00"}"#,
    "\n",
);

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn port(name: &str) -> PortId {
    PortId::new(name).expect("port id")
}

fn config(id: u128, type_id: &str, value: serde_json::Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("node config")
}

fn edge(from: u128, to: u128) -> NodeEdge {
    NodeEdge {
        from: NodePort {
            node_id: node(from),
            port: port("out"),
        },
        to: NodePort {
            node_id: node(to),
            port: port("in"),
        },
    }
}

fn graph(transforms: Vec<(u128, &str, serde_json::Value)>, asset_id: Uuid) -> NodeGraph {
    let output_id = 900 + transforms.len() as u128;
    let mut nodes = vec![config(
        1,
        "stillflow.node.source",
        json!({ "sourceAssetId": asset_id }),
    )];
    for (id, type_id, value) in &transforms {
        nodes.push(config(*id, type_id, value.clone()));
    }
    nodes.push(config(
        output_id,
        "stillflow.node.output",
        json!({ "outputLabel": "out" }),
    ));

    let mut edges = Vec::new();
    let mut previous = 1_u128;
    for (id, _, _) in &transforms {
        edges.push(edge(previous, *id));
        previous = *id;
    }
    edges.push(edge(previous, output_id));

    NodeGraph::new(
        uuid(1),
        node(1),
        node(output_id),
        nodes,
        edges,
        BTreeMap::new(),
    )
    .expect("valid graph")
}

/// A `cast` node with an explicit temporal format.
fn parse(
    id: u128,
    column: ColumnId,
    data_type: serde_json::Value,
    on_failure: &str,
    format: &str,
    timezone: Option<&str>,
) -> (u128, &'static str, serde_json::Value) {
    let mut value = json!({
        "column": column,
        "dataType": data_type,
        "onFailure": on_failure,
        "format": format,
    });
    if let Some(timezone) = timezone {
        value["timezone"] = json!(timezone);
    }
    (id, "stillflow.node.cast", value)
}

fn date32() -> serde_json::Value {
    json!({ "kind": "date32" })
}

fn timestamp(unit: &str, timezone: Option<&str>) -> serde_json::Value {
    let mut value = json!({ "unit": unit });
    if let Some(timezone) = timezone {
        value["timezone"] = json!(timezone);
    }
    json!({ "kind": "timestamp", "value": value })
}

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("stamps.ndjson"), NDJSON).expect("write fixture");

    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-n3 fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-n3").expect("credential ref"),
    )
    .expect("connection");

    let registry = registry();
    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let asset = assets
        .into_iter()
        .find(|asset| asset.name == "stamps.ndjson")
        .expect("fixture asset");

    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");

    Fixture {
        _root: root,
        connection,
        asset,
        schema: metadata.schema,
    }
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(std::sync::Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

impl Fixture {
    fn column(&self, name: &str) -> ColumnId {
        self.schema
            .fields
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("column {name} exists"))
            .id
    }

    fn field(&self, name: &str) -> &stillflow_core::LogicalField {
        self.schema
            .fields
            .iter()
            .find(|field| field.name == name)
            .expect("field exists")
    }

    fn compile(
        &self,
        graph: &NodeGraph,
    ) -> Result<stillflow_plan::CompiledNodeGraph, stillflow_plan::NodeGraphCompileError> {
        NodeGraphCompiler::new(NodeRegistry::deployed()).compile(
            graph,
            &AuthorizedSourceContext::new(self.asset.id, self.schema.clone())
                .expect("authorized source"),
            CompileTarget::Execution,
        )
    }

    async fn run(
        &self,
        graph: &NodeGraph,
        target_node: u128,
    ) -> Result<PreviewResult, stillflow_engine::EngineError> {
        let compiled = self.compile(graph).expect("graph compiles");
        let target = *compiled
            .node_plan_ids
            .get(&node(target_node))
            .expect("target plan node");
        let mut request = PreviewRequest::new(
            compiled.plan.clone(),
            target,
            self.connection.clone(),
            self.asset.clone(),
        );
        request.row_limit = 100;
        request.byte_limit = 1_048_576;
        ExecutionEngine::new(registry()).preview(request).await
    }

    async fn preview(&self, graph: &NodeGraph, target_node: u128) -> PreviewResult {
        self.run(graph, target_node)
            .await
            .expect("preview executes")
    }
}

fn column_index(result: &PreviewResult, column: &ColumnId) -> usize {
    result
        .schema
        .fields
        .iter()
        .position(|field| field.id == *column)
        .expect("column present")
}

fn dates(result: &PreviewResult, column: &ColumnId) -> Vec<Option<i32>> {
    let index = column_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            });
        }
    }
    values
}

fn datetimes(result: &PreviewResult, column: &ColumnId) -> Vec<Option<i64>> {
    let index = column_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .expect("millisecond timestamp column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            });
        }
    }
    values
}

fn ints(result: &PreviewResult, column: &ColumnId) -> Vec<Option<i32>> {
    let index = column_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            });
        }
    }
    values
}

fn strings(result: &PreviewResult, column: &ColumnId) -> Vec<Option<String>> {
    let index = column_index(result, column);
    let mut values = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        for row in 0..array.len() {
            values.push(if array.is_null(row) {
                None
            } else {
                Some(array.value(row).to_owned())
            });
        }
    }
    values
}

fn days(date: &str) -> i32 {
    let parsed = NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("fixture date");
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    parsed.signed_duration_since(epoch).num_days() as i32
}

/// Collects the rules of every `ApplyRules` node, in plan order.
fn rules_of(plan: &stillflow_plan::LogicalPlan) -> Vec<Rule> {
    plan.nodes
        .values()
        .filter_map(|node| match &node.kind {
            stillflow_plan::PlanNodeKind::ApplyRules { rules } => Some(rules.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

// ---------------------------------------------------------------------------
// Fixture guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_shapes_the_three_columns_as_expected() {
    let fixture = fixture().await;
    assert_eq!(fixture.field("clean").data_type, LogicalType::Utf8);
    assert!(!fixture.field("clean").nullable, "clean has no NULL row");
    assert!(fixture.field("dirty").nullable, "dirty has a NULL row");

    let graph = graph(Vec::new(), fixture.asset.id);
    let result = fixture.preview(&graph, 1).await;
    assert_eq!(
        strings(&result, &fixture.column("dirty")),
        vec![Some("2024-03-02".to_owned()), Some("nope".to_owned()), None]
    );
}

// ---------------------------------------------------------------------------
// Valid and invalid dates, failure policy, nullability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn valid_dates_parse_with_an_explicit_format() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![parse(2, clean, date32(), "error", "%Y-%m-%d", None)],
        fixture.asset.id,
    );
    let result = fixture.preview(&graph, 2).await;

    assert_eq!(
        dates(&result, &clean),
        vec![
            Some(days("2024-03-01")),
            Some(days("2024-03-03")),
            Some(days("2024-03-04")),
        ]
    );
    let field = result
        .schema
        .fields
        .iter()
        .find(|field| field.id == clean)
        .expect("clean field");
    assert_eq!(field.data_type, LogicalType::Date32);
    assert!(
        !field.nullable,
        "error mode preserves the source nullability when every value parses"
    );
}

#[tokio::test]
async fn an_unparseable_value_fails_under_error_policy() {
    let fixture = fixture().await;
    let dirty = fixture.column("dirty");
    let graph = graph(
        vec![parse(2, dirty, date32(), "error", "%Y-%m-%d", None)],
        fixture.asset.id,
    );

    let error = fixture
        .run(&graph, 2)
        .await
        .expect_err("an invalid date must fail the run under error policy");
    assert!(
        matches!(error, stillflow_engine::EngineError::CastFailure { .. }),
        "expected a cast failure, got {error:?}"
    );
}

#[tokio::test]
async fn set_null_policy_maps_invalid_values_to_null_and_widens_nullability() {
    let fixture = fixture().await;
    let dirty = fixture.column("dirty");
    let graph = graph(
        vec![parse(2, dirty, date32(), "setNull", "%Y-%m-%d", None)],
        fixture.asset.id,
    );
    let result = fixture.preview(&graph, 2).await;

    assert_eq!(
        dates(&result, &dirty),
        vec![Some(days("2024-03-02")), None, None],
        "the invalid value and the existing NULL both become NULL"
    );
    let field = result
        .schema
        .fields
        .iter()
        .find(|field| field.id == dirty)
        .expect("dirty field");
    assert!(field.nullable, "setNull widens the output nullability");
}

#[tokio::test]
async fn a_mismatched_format_rejects_every_row() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    // The fixture holds `%Y-%m-%d`; asking for a different layout must not
    // silently succeed.
    let graph = graph(
        vec![parse(2, clean, date32(), "error", "%d/%m/%Y", None)],
        fixture.asset.id,
    );
    assert!(fixture.run(&graph, 2).await.is_err());
}

// ---------------------------------------------------------------------------
// Timestamps: explicit timezone, no implicit inference, ambiguity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_timestamp_without_a_timezone_stays_naive() {
    let fixture = fixture().await;
    let ambiguous = fixture.column("ambiguous");
    let graph = graph(
        vec![parse(
            2,
            ambiguous,
            timestamp("millisecond", None),
            "error",
            "%Y-%m-%dT%H:%M:%S",
            None,
        )],
        fixture.asset.id,
    );
    let result = fixture.preview(&graph, 2).await;

    assert_eq!(
        result
            .schema
            .fields
            .iter()
            .find(|field| field.id == ambiguous)
            .expect("field")
            .data_type,
        LogicalType::Timestamp {
            unit: TimeUnit::Millisecond,
            timezone: None,
        },
        "no timezone is inferred when none is declared"
    );
    // 2024-03-03T01:30:00 UTC as epoch milliseconds.
    let expected = chrono::Utc
        .with_ymd_and_hms(2024, 3, 3, 1, 30, 0)
        .single()
        .expect("unambiguous UTC instant")
        .timestamp_millis();
    assert_eq!(datetimes(&result, &ambiguous)[1], Some(expected));
}

#[tokio::test]
async fn a_declared_timezone_is_rejected_for_every_temporal_target() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");

    // A timezone-aware timestamp target is refused: applying a timezone
    // correctly needs a Polars feature that changes an unrelated frozen
    // ingestion behaviour, so it needs its own decision. Fail closed rather
    // than silently ignoring the declaration.
    let timestamp_graph = graph(
        vec![parse(
            2,
            clean,
            timestamp("millisecond", None),
            "error",
            "%Y-%m-%d",
            Some("America/New_York"),
        )],
        fixture.asset.id,
    );
    let error = fixture
        .compile(&timestamp_graph)
        .expect_err("a declared timezone must be refused");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn an_ambiguous_wall_time_parses_naively_and_deterministically() {
    let fixture = fixture().await;
    let ambiguous = fixture.column("ambiguous");
    // Without a timezone there is no ambiguity to resolve: the wall time is
    // taken as written and the output type carries no timezone. The two runs
    // prove the result is deterministic rather than environment-dependent.
    let graph = graph(
        vec![parse(
            2,
            ambiguous,
            timestamp("millisecond", None),
            "error",
            "%Y-%m-%dT%H:%M:%S",
            None,
        )],
        fixture.asset.id,
    );
    let first = fixture.preview(&graph, 2).await;
    let second = fixture.preview(&graph, 2).await;

    let expected = chrono::Utc
        .with_ymd_and_hms(2024, 11, 3, 1, 30, 0)
        .single()
        .expect("instant")
        .timestamp_millis();
    assert_eq!(datetimes(&first, &ambiguous)[0], Some(expected));
    assert_eq!(
        datetimes(&first, &ambiguous),
        datetimes(&second, &ambiguous),
        "the naive parse is deterministic"
    );
}

// ---------------------------------------------------------------------------
// Configuration laws and paused capabilities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_format_on_a_non_temporal_target_is_rejected() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![parse(
            2,
            clean,
            json!({ "kind": "int64" }),
            "error",
            "%Y-%m-%d",
            None,
        )],
        fixture.asset.id,
    );
    let error = fixture.compile(&graph).expect_err("format on int64");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn a_timezone_on_a_date_target_is_rejected() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![parse(2, clean, date32(), "error", "%Y-%m-%d", Some("UTC"))],
        fixture.asset.id,
    );
    let error = fixture.compile(&graph).expect_err("timezone on date32");
    assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
}

#[tokio::test]
async fn a_second_unit_timestamp_target_stays_paused() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![parse(
            2,
            clean,
            timestamp("second", None),
            "error",
            "%Y-%m-%d",
            None,
        )],
        fixture.asset.id,
    );
    let error = fixture
        .compile(&graph)
        .expect_err("second-unit timestamps are paused");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn a_non_text_source_is_rejected() {
    let fixture = fixture().await;
    let id = fixture.column("id");
    let graph = graph(
        vec![parse(2, id, date32(), "error", "%Y-%m-%d", None)],
        fixture.asset.id,
    );
    let error = fixture.compile(&graph).expect_err("int64 source");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

// ---------------------------------------------------------------------------
// Existing cast behaviour: numeric overflow, precision, and plan compatibility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn numeric_overflow_follows_the_declared_failure_policy() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");

    // A date string is not an integer: error policy fails.
    let error_graph = graph(
        vec![(
            2,
            "stillflow.node.cast",
            json!({
                "column": clean,
                "dataType": {"kind": "int32"},
                "onFailure": "error"
            }),
        )],
        fixture.asset.id,
    );
    assert!(fixture.run(&error_graph, 2).await.is_err());

    // setNull maps every unparseable value to NULL and widens nullability.
    let null_graph = graph(
        vec![(
            2,
            "stillflow.node.cast",
            json!({
                "column": clean,
                "dataType": {"kind": "int32"},
                "onFailure": "setNull"
            }),
        )],
        fixture.asset.id,
    );
    let result = fixture.preview(&null_graph, 2).await;
    assert_eq!(ints(&result, &clean), vec![None, None, None]);
    assert!(
        result
            .schema
            .fields
            .iter()
            .find(|field| field.id == clean)
            .expect("field")
            .nullable
    );
}

#[tokio::test]
async fn an_existing_cast_configuration_keeps_lowering_to_the_cast_rule() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![(
            2,
            "stillflow.node.cast",
            json!({
                "column": clean,
                "dataType": {"kind": "utf8"},
                "onFailure": "setNull"
            }),
        )],
        fixture.asset.id,
    );
    let compiled = fixture.compile(&graph).expect("compiles");

    let rules = rules_of(&compiled.plan);
    assert!(
        rules.iter().any(|rule| matches!(rule, Rule::Cast { .. })),
        "a configuration without a format must keep using Rule::Cast"
    );
    assert!(
        !rules
            .iter()
            .any(|rule| matches!(rule, Rule::ParseTemporal { .. })),
        "no ParseTemporal rule may appear without a declared format"
    );
}

#[tokio::test]
async fn a_declared_format_lowers_to_a_temporal_parse_rule() {
    let fixture = fixture().await;
    let clean = fixture.column("clean");
    let graph = graph(
        vec![parse(2, clean, date32(), "error", "%Y-%m-%d", None)],
        fixture.asset.id,
    );
    let compiled = fixture.compile(&graph).expect("compiles");

    let rules = rules_of(&compiled.plan);
    assert!(
        rules.iter().any(|rule| matches!(
            rule,
            Rule::ParseTemporal { format, .. } if format == "%Y-%m-%d"
        )),
        "the declared format must be carried into the plan"
    );
}
