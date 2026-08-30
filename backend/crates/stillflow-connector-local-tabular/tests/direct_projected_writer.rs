//! E24-JSON-A2 production differential oracle for issue #158
//! (`json-direct-projected-writer`).
//!
//! Every test asserts absolute observable behavior (final tabular values as
//! extracted from the public envelopes, error categories, stable messages,
//! earliest failing row) on identical fixture bytes. The file compiles and must
//! pass IDENTICALLY with the private feature off (exact current production
//! path) and on (raw-slice direct projected assembly). Running the connector
//! suite in both modes is the mechanical OFF/ON comparison required by the
//! issue contract; any mismatch is a semantic reject.
//!
//! Fixture convention: inspection samples only the FIRST row (`maxRows: 1`),
//! which acts as the canonical schema-establishing sentinel; test content
//! follows from row 2 onward, mirroring post-inspection drift in production.
//!
//! Temporal disclosure: the retained Polars JsonReader decodes ISO-string
//! timestamps with the pre-existing #151 upstream scale quirk (`TIMESTAMP_ROOT_CAUSE_POLARS_UPSTREAM`)
//! in BOTH modes. These tests assert controlled accept/reject parity and row
//! order only — never temporal correctness — and add no numeric compensation.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampMillisecondArray, UInt64Array,
};
use std::pin::Pin;

use futures::{Stream, StreamExt};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    BatchEnvelope, ColumnId, ConnectorError, CredentialRef, DiscoverRequest, ErrorCategory,
    InspectRequest, LogicalField, LogicalSchema, LogicalType, ReadRequest, RequestContext,
    SourceConnection, TimeUnit,
};

/// The public read-stream surface returned by `read_batches`.
type EnvelopeStream = Pin<Box<dyn Stream<Item = Result<BatchEnvelope, ConnectorError>> + Send>>;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const SCHEMA_MSG: &str = "JSON row does not match the established schema";

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 8388608 }
        }),
        CredentialRef::new("cred://local/fixtures").expect("credential reference"),
    )
    .expect("connection")
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

/// One streamed outcome: either an envelope payload batch or the terminal
/// stream error (category + stable message).
type Outcome = Result<RecordBatch, (ErrorCategory, String)>;

/// Writes `lines` as `case.ndjson`, discovers/inspects it, optionally applies a
/// schema override that keeps source field names/order but pins logical types,
/// selects projected fields by override index, and drains the read stream.
async fn run_case(
    lines: &[&str],
    override_types: Option<&[(LogicalType, bool)]>,
    projection_indices: Option<&[usize]>,
    batch_size: usize,
) -> Vec<Outcome> {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(
        temp.path().join("case.ndjson"),
        format!("{}\n", lines.join("\n")),
    )
    .expect("write case.ndjson");
    let connection = connection(temp.path());
    drain_asset_with_context(
        &connection,
        "case.ndjson",
        override_types,
        projection_indices,
        batch_size,
        RequestContext::default(),
    )
    .await
}

/// Discovers `name`, inspects it, applies the optional pinned
/// override/projection, and drains the stream into outcomes.
async fn drain_asset(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    projection_indices: Option<&[usize]>,
    batch_size: usize,
) -> Vec<Outcome> {
    drain_asset_with_context(
        connection,
        name,
        override_types,
        projection_indices,
        batch_size,
        RequestContext::default(),
    )
    .await
}

/// Same as `drain_asset` with an explicit request context (cancellation /
/// deadline differential coverage).
async fn drain_asset_with_context(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    projection_indices: Option<&[usize]>,
    batch_size: usize,
    context: RequestContext,
) -> Vec<Outcome> {
    let mut stream = open_stream_with_context(
        connection,
        name,
        override_types,
        projection_indices,
        batch_size,
        context,
    )
    .await;
    let mut outcomes = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(envelope) => outcomes.push(Ok(envelope.payload().clone())),
            Err(error) => {
                outcomes.push(Err((error.category(), error.to_string())));
                break;
            }
        }
    }
    outcomes
}

/// Opens the read stream with an explicit request context and returns it
/// without draining, for tests that interleave with the stream.
/// Same as `open_stream_with_context` but surfaces the open failure instead of
/// panicking, for tests that pin the fail-closed open surface.
async fn try_open_stream_with_context(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    projection_indices: Option<&[usize]>,
    batch_size: usize,
    context: RequestContext,
) -> Result<EnvelopeStream, ConnectorError> {
    let registry = registry();
    let assets = registry
        .discover(
            connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let asset = assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("{name} discovered"))
        .clone();
    let metadata = registry
        .inspect(
            connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("inspect {name}: {error}"));

    let override_schema = override_types.map(|types| {
        assert_eq!(
            types.len(),
            metadata.schema.fields.len(),
            "override pins every source field"
        );
        let fields = metadata
            .schema
            .fields
            .iter()
            .zip(types.iter())
            .map(|(source, (data_type, nullable))| {
                LogicalField::new(source.id, source.name.clone(), data_type.clone(), *nullable)
                    .expect("override field")
            })
            .collect();
        LogicalSchema::new(fields).expect("override schema")
    });
    let projection: Option<Vec<ColumnId>> = projection_indices.map(|indices| {
        let schema = override_schema.as_ref().unwrap_or(&metadata.schema);
        indices.iter().map(|&i| schema.fields[i].id).collect()
    });

    let mut request = ReadRequest::new(asset, batch_size);
    request.context = context;
    request.schema_override = override_schema;
    request.projection = projection;
    registry.read_batches(connection, request).await
}

async fn open_stream_with_context(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    projection_indices: Option<&[usize]>,
    batch_size: usize,
    context: RequestContext,
) -> EnvelopeStream {
    try_open_stream_with_context(
        connection,
        name,
        override_types,
        projection_indices,
        batch_size,
        context,
    )
    .await
    .expect("open read stream")
}

fn outcome_ok(outcome: &Outcome, label: &str) -> RecordBatch {
    outcome
        .as_ref()
        .unwrap_or_else(|error| panic!("{label}: unexpected error {error:?}"))
        .clone()
}

#[allow(dead_code)]
fn outcome_err(outcome: &Outcome, label: &str) -> (ErrorCategory, String) {
    outcome
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("{label}: expected terminal error, got a batch"))
        .clone()
}

fn expect_single_ok(outcomes: &[Outcome], label: &str) -> RecordBatch {
    assert_eq!(outcomes.len(), 1, "{label}: exactly one outcome expected");
    outcome_ok(&outcomes[0], label)
}

fn expect_schema_drift_at_row(outcomes: &[Outcome], label: &str, row: usize) {
    assert_eq!(outcomes.len(), 1, "{label}: terminal-only outcome expected");
    let (category, message) = outcome_err(&outcomes[0], label);
    assert_eq!(category, ErrorCategory::SchemaDrift, "{label}: {message}");
    assert!(
        message.contains(SCHEMA_MSG),
        "{label}: unexpected message: {message}"
    );
    assert!(
        message.contains(&format!("row {row}")),
        "{label}: earliest failing row expected {row}: {message}"
    );
}

fn expect_invalid_data_message(outcomes: &[Outcome], label: &str, needle: &str) {
    assert_eq!(outcomes.len(), 1, "{label}: terminal-only outcome expected");
    let (category, message) = outcome_err(&outcomes[0], label);
    assert_eq!(category, ErrorCategory::InvalidData, "{label}: {message}");
    assert!(message.contains(needle), "{label}: {message}");
}

fn column<'a, A: arrow_array::Array + 'static>(batch: &'a RecordBatch, name: &str) -> &'a A {
    let index = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column {name} present"));
    batch
        .column(index)
        .as_any()
        .downcast_ref::<A>()
        .unwrap_or_else(|| panic!("column {name} has the expected arrow type"))
}

#[allow(dead_code)]
macro_rules! scalar_extractor {
    ($name:ident, $ty:ty, $native:ty) => {
        // Some extractors intentionally cover types no fixture in this suite
        // exercises yet (e.g. Float32 columns arrive only via inference).
        #[allow(dead_code)]
        fn $name(batch: &RecordBatch, column_name: &str) -> Vec<Option<$native>> {
            let array = column::<$ty>(batch, column_name);
            (0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i)))
                .collect()
        }
    };
}

scalar_extractor!(i64_values, Int64Array, i64);
scalar_extractor!(u64_values, UInt64Array, u64);
scalar_extractor!(f32_values, Float32Array, f32);
scalar_extractor!(f64_values, Float64Array, f64);
scalar_extractor!(bool_values, BooleanArray, bool);

fn str_values(batch: &RecordBatch, column_name: &str) -> Vec<Option<String>> {
    scalar_string_values(batch, column_name)
}

fn scalar_string_values(batch: &RecordBatch, column_name: &str) -> Vec<Option<String>> {
    let array = column::<StringArray>(batch, column_name);
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array.value(i).to_owned()))
        .collect()
}

#[allow(dead_code)]
fn date_values(batch: &RecordBatch, column_name: &str) -> Vec<Option<String>> {
    let array = column::<Date32Array>(batch, column_name);
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array.value_as_date(i).expect("date").to_string()))
        .collect()
}

fn list_i64_values(batch: &RecordBatch, column_name: &str) -> Vec<Option<Vec<Option<i64>>>> {
    let array = column::<ListArray>(batch, column_name);
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                return None;
            }
            let values = array.value(i);
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("list of int64");
            Some(
                (0..values.len())
                    .map(|j| (!values.is_null(j)).then(|| values.value(j)))
                    .collect(),
            )
        })
        .collect()
}

/// Extracts `(x, y)` pairs from a struct column whose fields are named x/y.
fn struct_xy_values(
    batch: &RecordBatch,
    column_name: &str,
) -> Vec<Option<(Option<i64>, Option<String>)>> {
    let array = column::<StructArray>(batch, column_name);
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                return None;
            }
            let x = array.column_by_name("x").expect("struct field x");
            let x = x.as_any().downcast_ref::<Int64Array>().expect("x int64");
            let y = array.column_by_name("y").expect("struct field y");
            let y = y.as_any().downcast_ref::<StringArray>().expect("y utf8");
            Some((
                (!x.is_null(i)).then(|| x.value(i)),
                (!y.is_null(i)).then(|| y.value(i).to_owned()),
            ))
        })
        .collect()
}

/// Four-column scalar schema: a Int64 required, b Utf8 required,
/// c Float64 required, d Boolean nullable. The sentinel establishes names.
fn abcd_types() -> Vec<(LogicalType, bool)> {
    vec![
        (LogicalType::Int64, false),
        (LogicalType::Utf8, false),
        (LogicalType::Float64, false),
        (LogicalType::Boolean, true),
    ]
}

const ABCD_SENTINEL: &str = r#"{"a":0,"b":"h","c":0.0,"d":true}"#;

#[tokio::test]
async fn key_order_independence_and_projection_matrix() {
    let lines = [
        r#"{"a":1,"b":"x","c":1.5,"d":true}"#,
        r#"{"d":false,"c":2.5,"b":"y","a":2}"#,
        r#"{"c":3.5,"a":3,"d":null,"b":"z"}"#,
    ];
    let types = abcd_types();

    // Full dense projection in schema order.
    let full = run_case(&lines, Some(&types), Some(&[0, 1, 2, 3]), 4).await;
    let batch = expect_single_ok(&full, "full projection");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect();
    assert_eq!(names, ["a", "b", "c", "d"], "projection.schema order wins");
    assert_eq!(i64_values(&batch, "a"), [Some(1), Some(2), Some(3)]);
    assert_eq!(
        str_values(&batch, "b"),
        [Some("x".into()), Some("y".into()), Some("z".into())]
    );
    assert_eq!(f64_values(&batch, "c"), [Some(1.5), Some(2.5), Some(3.5)]);
    assert_eq!(bool_values(&batch, "d"), [Some(true), Some(false), None]);

    // Sparse projection selecting c then a only.
    let sparse = run_case(&lines, Some(&types), Some(&[2, 0]), 4).await;
    let batch = expect_single_ok(&sparse, "sparse projection");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect();
    assert_eq!(names, ["c", "a"], "sparse selection keeps schema order");
    assert_eq!(f64_values(&batch, "c"), [Some(1.5), Some(2.5), Some(3.5)]);
    assert_eq!(i64_values(&batch, "a"), [Some(1), Some(2), Some(3)]);

    // Every single-field position stays readable.
    for index in 0..4_usize {
        let outcomes = run_case(&lines, Some(&types), Some(&[index]), 4).await;
        expect_single_ok(&outcomes, "single-field projection");
    }
}

#[tokio::test]
async fn selected_field_positions_first_middle_last() {
    let lines = [
        ABCD_SENTINEL,
        r#"{"a":10,"b":"first","c":1.0}"#,
        r#"{"b":"mid","a":20,"c":2.0}"#,
        r#"{"c":3.0,"a":30,"b":"last"}"#,
    ];
    let types = abcd_types();
    let outcomes = run_case(&lines, Some(&types), Some(&[0, 1, 2]), 4).await;
    let batch = expect_single_ok(&outcomes, "selected positions");
    assert_eq!(
        i64_values(&batch, "a"),
        [Some(0), Some(10), Some(20), Some(30)]
    );
    assert_eq!(
        str_values(&batch, "b"),
        [
            Some("h".into()),
            Some("first".into()),
            Some("mid".into()),
            Some("last".into())
        ]
    );
}

#[tokio::test]
async fn unknown_field_position_parity_selected_and_not() {
    let types = abcd_types();
    for (name, line) in [
        (
            "unknown-before-selected",
            r#"{"z":true,"a":1,"b":"x","c":1.0}"#,
        ),
        (
            "unknown-after-selected",
            r#"{"a":1,"b":"x","c":1.0,"z":true}"#,
        ),
    ] {
        for projection in [Some(&[0usize, 1, 2][..]), Some(&[1usize][..])] {
            let outcomes = run_case(&[ABCD_SENTINEL, line], Some(&types), projection, 4).await;
            expect_schema_drift_at_row(&outcomes, name, 2);
        }
    }
}

#[tokio::test]
async fn duplicate_late_position_detected_before_value_acceptance() {
    let types = abcd_types();

    // The duplicate is the LAST key of the object and its second value would
    // be perfectly valid on its own; detection must still happen at the
    // duplicate key, before any value acceptance into the row.
    let dup_selected = r#"{"a":1,"b":"x","c":1.0,"d":true,"a":9}"#;
    let outcomes = run_case(
        &[ABCD_SENTINEL, dup_selected],
        Some(&types),
        Some(&[0, 1, 2]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "duplicate selected key in last position", 2);

    // Duplicate non-selected key in last position too.
    let dup_non_selected = r#"{"k":7,"b":"first","b":"second"}"#;
    let outcomes = run_case(
        &[ABCD_SENTINEL, dup_non_selected],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "duplicate non-selected key in last position", 2);

    // An unknown key appearing EARLIER than the duplicate wins ordering.
    let unknown_then_dup = r#"{"a":1,"zzz":1,"a":2}"#;
    let outcomes = run_case(
        &[ABCD_SENTINEL, unknown_then_dup],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "unknown key before duplicate", 2);

    // A duplicate whose FIRST occurrence is invalid reports the first-value
    // validation failure (validation blocks before the duplicate is seen).
    let invalid_first_occurrence = r#"{"a":"bad","b":"x","c":1.0,"a":2}"#;
    let outcomes = run_case(
        &[ABCD_SENTINEL, invalid_first_occurrence],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "first occurrence validated before duplicate", 2);
}

#[tokio::test]
async fn escaped_top_level_keys_match_generic_acceptance() {
    // Escaped top-level keys must be accepted, resolved to their unescaped
    // names, and deduplicated exactly like the generic path's `String` keys.
    let types = abcd_types();

    // Escaped selected key resolves to field `a`.
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"\u0061":7,"b":"x","c":1.0}"#],
        Some(&types),
        Some(&[0, 1]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "escaped selected key");
    assert_eq!(i64_values(&batch, "a"), [Some(0), Some(7)]);

    // Escaped non-selected key resolves to field `b`.
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"a":7,"\u0062":"y","c":1.0}"#],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_single_ok(&outcomes, "escaped non-selected key");

    // An escaped key that resolves to an unknown field is still unknown.
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"a":7,"\u0071":1}"#],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "escaped unknown key", 2);

    // An escaped duplicate (`\u0061` == `a`) is detected as a duplicate.
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"a":1,"\u0061":2,"b":"x","c":1.0}"#],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "escaped duplicate key", 2);
}

#[tokio::test]
async fn valid_prefix_rows_stream_before_terminal_row_error() {
    let types = abcd_types();
    let lines = [
        ABCD_SENTINEL,
        r#"{"a":1,"b":"x","c":1.5,"d":true}"#,
        r#"{"a":2,"b":"dup","b":"oops","c":2.5}"#,
        r#"{"a":3,"b":"never","c":3.5,"d":null}"#,
    ];
    let outcomes = run_case(&lines, Some(&types), Some(&[0, 1]), 8).await;
    assert_eq!(
        outcomes.len(),
        1,
        "failing row shares the batch window so no partial frame may be emitted"
    );
    let (_, message) = outcome_err(&outcomes[0], "terminal row 3");
    assert!(message.contains(SCHEMA_MSG), "{message}");
    assert!(message.contains("row 3"), "earliest failing row: {message}");
}

#[tokio::test]
async fn required_null_missing_and_all_nullable_empty_object() {
    let types = abcd_types();

    // Explicit null for a required field (row 2).
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"a":null,"b":"x","c":1.0}"#],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "required field null", 2);

    // Missing required field (row 2).
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"b":"x","c":1.0}"#],
        Some(&types),
        Some(&[0]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "missing required field", 2);

    // Missing nullable field fills with null.
    let outcomes = run_case(
        &[ABCD_SENTINEL, r#"{"a":5,"b":"x","c":1.0}"#],
        Some(&types),
        Some(&[0, 1, 2, 3]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "missing nullable fills null");
    assert_eq!(i64_values(&batch, "a"), [Some(0), Some(5)]);
    assert_eq!(bool_values(&batch, "d"), [Some(true), None]);

    // Empty object against an all-nullable schema yields an all-null row.
    let nullable_types: Vec<(LogicalType, bool)> =
        abcd_types().into_iter().map(|(t, _)| (t, true)).collect();
    let outcomes = run_case(
        &[ABCD_SENTINEL, "{}"],
        Some(&nullable_types),
        Some(&[0, 1, 2, 3]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "all-nullable empty object");
    assert_eq!(i64_values(&batch, "a"), [Some(0), None]);
    assert_eq!(str_values(&batch, "b"), [Some("h".into()), None]);
    assert_eq!(f64_values(&batch, "c"), [Some(0.0), None]);
    assert_eq!(bool_values(&batch, "d"), [Some(true), None]);
}

#[tokio::test]
async fn wrong_scalar_types_rejected_identically_when_selected_and_not() {
    // Fields: i8v, u64v, f32v, s, d, ts — one wrong value per case (row 2),
    // probed once while selected by the projection and once excluded from it.
    let base: Vec<(LogicalType, bool)> = vec![
        (LogicalType::Int8, false),
        (LogicalType::UInt64, false),
        (LogicalType::Float32, false),
        (LogicalType::Utf8, false),
        (LogicalType::Date32, false),
        (
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: None,
            },
            false,
        ),
    ];
    const SIX_SENTINEL: &str =
        r#"{"i8v":0,"u64v":0,"f32v":0.0,"s":"h","d":"2024-01-01","ts":"2024-01-01T00:00:00"}"#;
    #[rustfmt::skip]
    let bad_values: &[(&str, &str)] = &[
        ("i8v", "128"),
        ("i8v", "-129"),
        ("i8v", "true"),
        ("i8v", "1.5"),
        ("i8v", "1e2"),
        ("u64v", "-1"),
        ("u64v", "18446744073709551616"),
        ("f32v", "\"text\""),
        ("f32v", "true"),
        ("s", "5"),
        ("s", "[1]"),
        ("s", "{\"x\":1}"),
        ("d", "\"not-a-date\""),
        ("d", "20240229"),
        ("ts", "\"2024-02-29T01:02:03+05:00\""),
        ("ts", "1709166123123"),
    ];
    let names = ["i8v", "u64v", "f32v", "s", "d", "ts"];

    for (field, raw) in bad_values {
        let line = format!(
            "{{\"i8v\":1,\"u64v\":2,\"f32v\":0.5,\"s\":\"ok\",\"d\":\"2024-01-31\",\"ts\":\"2024-02-29T01:02:03.123\",\"{field}\":{raw}}}"
        );
        let index = names
            .iter()
            .position(|name| name == field)
            .expect("known field");
        for role in [
            ("selected", Some(vec![index])),
            ("non-selected", Some(vec![(index + 1) % 6])),
        ] {
            let outcomes = run_case(
                &[SIX_SENTINEL, line.as_str()],
                Some(&base),
                role.1.as_deref(),
                4,
            )
            .await;
            expect_schema_drift_at_row(&outcomes, &format!("{field}={raw} ({})", role.0), 2);
        }
    }
}

#[tokio::test]
async fn numeric_boundary_values_and_exponent_notation_round_trip() {
    // NOTE: narrow ints (Int8/16/32) cannot round-trip through the RETAINED
    // Polars JSON decode in the CURRENT baseline either (polars-core panics
    // constructing those series from AnyValues); that pre-existing limitation
    // is identical in both modes and out of scope here. Positive round-trips
    // below use the int widths the current pipeline supports.
    let base: Vec<(LogicalType, bool)> = vec![
        (LogicalType::Int64, false),
        (LogicalType::UInt64, false),
        (LogicalType::Float64, false),
        (LogicalType::Utf8, false),
    ];
    let lines = [
        r#"{"iv":-9223372036854775808,"u64v":18446744073709551615,"fv":1e2,"s":"edge"}"#,
        r#"{"iv":9223372036854775807,"u64v":0,"fv":-0.25,"s":""}"#,
        r#"{"iv":0,"u64v":1709166123123,"fv":-1.5e-3,"s":"exp"}"#,
        // An integer beyond u64::MAX parses as a float in both modes: accepted
        // on Float64 (same f64 value), rejected on Int64 (SchemaDrift).
        r#"{"iv":1000000000000000000000000000000,"u64v":0,"fv":1000000000000000000000000000000,"s":"big"}"#,
    ];
    let outcomes = run_case(&lines, Some(&base), Some(&[0, 1, 2, 3]), 4).await;
    assert_eq!(
        outcomes.len(),
        1,
        "Int64 rejects the beyond-u64 integer on row 4; no partial frames"
    );
    let (category, message) = outcome_err(&outcomes[0], "big int on Int64");
    assert_eq!(category, ErrorCategory::SchemaDrift, "{message}");
    assert!(message.contains("row 4"), "{message}");

    // Float64 accepts it with the identical f64 value in both modes. Own
    // fixture: the big literal sits only on the Float64 field, because a
    // NON-selected Int64 field rejects it in BOTH modes (the full-projection
    // case above pins that).
    let float_lines = [
        r#"{"iv":0,"u64v":0,"fv":1e2,"s":"edge"}"#,
        r#"{"iv":0,"u64v":0,"fv":-0.25,"s":""}"#,
        r#"{"iv":0,"u64v":0,"fv":-1.5e-3,"s":"exp"}"#,
        r#"{"iv":0,"u64v":0,"fv":1000000000000000000000000000000,"s":"big"}"#,
    ];
    let float_only = run_case(&float_lines, Some(&base), Some(&[2]), 4).await;
    let batch = expect_single_ok(&float_only, "big int on Float64");
    assert_eq!(
        f64_values(&batch, "fv"),
        [
            Some(100.0),
            Some(-0.25),
            Some(-0.0015),
            Some(1_000_000_000_000_000_000_000_000_000_000.0)
        ]
    );
}

#[tokio::test]
async fn selected_parse_class_failures_map_to_invalid_data() {
    // Parse-class failures the DOM parse of the generic path surfaces as
    // syntax-classified errors: out-of-range exponent floats and lone
    // surrogate escapes. The direct path must reproduce the exact category
    // and message, not degrade them to the semantic SchemaDrift surface.
    let base: Vec<(LogicalType, bool)> =
        vec![(LogicalType::Float64, false), (LogicalType::Utf8, false)];
    const SENTINEL: &str = r#"{"fv":1.5,"s":"ok"}"#;
    #[rustfmt::skip]
    let cases: &[(&str, &str)] = &[
        ("float-range", r#"{"fv":1e999,"s":"ok"}"#),
        ("float-range-negative", r#"{"fv":-1e999,"s":"ok"}"#),
        ("lone-surrogate", r#"{"fv":1.5,"s":"\ud800"}"#),
        ("lone-surrogate-trailing", r#"{"fv":1.5,"s":"ok\ud800"}"#),
    ];
    for (name, line) in cases {
        for (role, projection) in [
            ("selected", Some(&[0usize, 1][..])),
            ("fv-selected", Some(&[0usize][..])),
            ("s-selected", Some(&[1usize][..])),
        ] {
            let outcomes = run_case(&[SENTINEL, line], Some(&base), projection, 4).await;
            expect_invalid_data_message(&outcomes[0..1], &format!("{name} ({role})"), SCHEMA_MSG);
            let (_, message) = outcome_err(&outcomes[0], name);
            assert!(message.contains("row 2"), "{name} ({role}): {message}");
        }
    }
}

#[tokio::test]
async fn string_escapes_unicode_surrogates_and_control_escapes() {
    let base: Vec<(LogicalType, bool)> =
        vec![(LogicalType::Int64, false), (LogicalType::Utf8, false)];
    let escaped = "quote\\\" backslash\\\\ newline\\n tab\\t unicode\\u00e9 \\ud83d\\ude00 end";
    let line = format!("{{\"id\":1,\"s\":\"{escaped}\"}}");
    let outcomes = run_case(&[line.as_str()], Some(&base), Some(&[1]), 4).await;
    let batch = expect_single_ok(&outcomes, "escaped string accepted");
    let decoded = &str_values(&batch, "s")[0];
    assert_eq!(
        decoded.as_deref(),
        Some("quote\" backslash\\ newline\n tab\t unicodeé 😀 end"),
        "all escape forms decode identically"
    );
}

#[tokio::test]
async fn temporal_forms_accepted_consistently() {
    let base: Vec<(LogicalType, bool)> = vec![
        (LogicalType::Date32, false),
        (
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: None,
            },
            false,
        ),
        (LogicalType::Int64, false),
    ];
    // Note: both modes feed VALUE-identical timestamp strings to the retained
    // Polars parse, so any form accepted/rejected downstream is accepted or
    // rejected identically. Fixture timestamps use whole-second ISO forms:
    // fractional-second forms decode through the retained Polars JsonReader
    // with the pre-existing #151 upstream scale quirk in BOTH modes (baseline
    // behavior), so here they would only add an out-of-scope baseline constant
    // to pin. This is a controlled known-upstream-bug surface, NOT a
    // correctness proof, and no numeric compensation exists anywhere.
    let lines = [
        r#"{"d":"2024-02-29","ts":"2024-02-29T01:02:03","k":1}"#,
        r#"{"d":"2023-12-31","ts":"2023-12-31T23:59:59","k":2}"#,
    ];
    let outcomes = run_case(&lines, Some(&base), Some(&[0, 1]), 4).await;
    let batch = expect_single_ok(&outcomes, "temporal forms accepted");
    assert_eq!(
        date_values(&batch, "d"),
        [Some("2024-02-29".into()), Some("2023-12-31".into())],
    );
    let ts = column::<TimestampMillisecondArray>(&batch, "ts");
    // DISCLOSED BASELINE QUIRK (verified identical under feature OFF before
    // this suite was authored): the retained Polars JsonReader decodes
    // ISO-string timestamps into the TimestampMillisecond column with a
    // constant x1000 scale shift (e.g. "2024-02-29T01:02:03" lands in year
    // ~56131). This path retains that parser verbatim, so the in-scope
    // guarantees here are: both rows decode non-null and row ORDER survives
    // the monotonic shift; cross-mode parity of outcomes is proven by running
    // this identical suite under feature OFF and feature ON.
    let first = ts.value_as_datetime(0).expect("timestamp decodes");
    let second = ts.value_as_datetime(1).expect("timestamp decodes");
    assert!(
        first > second,
        "input row order must survive the retained parse: {first} !> {second}"
    );
}

/// Nested schema used across the nested tests:
/// m Int64, li List(Int64) nullable, st Struct{x Int64 required, y Utf8 nullable}.
fn nested_types() -> Vec<(LogicalType, bool)> {
    let st_fields = vec![
        LogicalField::new(ColumnId::random(), "x", LogicalType::Int64, false).expect("x"),
        LogicalField::new(ColumnId::random(), "y", LogicalType::Utf8, true).expect("y"),
    ];
    vec![
        (LogicalType::Int64, false),
        (LogicalType::List(Box::new(LogicalType::Int64)), true),
        (LogicalType::Struct(st_fields), false),
    ]
}

const NESTED_SENTINEL: &str = r#"{"m":0,"li":[0],"st":{"x":0,"y":null}}"#;

#[tokio::test]
async fn nested_list_and_struct_values_with_shuffled_keys() {
    let types = nested_types();
    let lines = [
        NESTED_SENTINEL,
        r#"{"m":1,"li":[1,2,null,3],"st":{"x":10,"y":"s"}}"#,
        r#"{"st":{"y":null,"x":20},"li":[],"m":2}"#,
        r#"{"m":3,"st":{"x":30}}"#,
    ];
    let outcomes = run_case(&lines, Some(&types), Some(&[0, 1, 2]), 4).await;
    let batch = expect_single_ok(&outcomes, "nested values accepted");
    assert_eq!(
        i64_values(&batch, "m"),
        [Some(0), Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        list_i64_values(&batch, "li"),
        [
            Some(vec![Some(0)]),
            Some(vec![Some(1), Some(2), None, Some(3)]),
            Some(vec![]),
            None,
        ],
        "lists preserved incl. inner nulls, empty list, missing->null"
    );
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [
            Some((Some(0), None)),
            Some((Some(10), Some("s".into()))),
            Some((Some(20), None)),
            Some((Some(30), None)),
        ],
        "structs preserved incl. missing nullable member"
    );
}

#[tokio::test]
async fn pretty_printed_array_rows_with_newlines_inside_selected_values() {
    // A raw newline inside a SELECTED List/Struct value is only possible for
    // array documents framed across lines. The generic path re-serializes
    // selected values compactly through serde_json::Value; the direct path
    // must canonicalize exactly those captures so no raw newline can split a
    // row inside the downstream line-oriented JSON reader.
    let types = nested_types();
    let temp = TempDir::new().expect("temporary fixture root");
    let document = concat!(
        "[\n",
        "  {\"m\":0,\"li\":[0],\"st\":{\"x\":0,\"y\":null}},\n",
        "  {\"m\":1,\"li\":[\n    1,\n    2\n  ],\"st\":{\n    \"x\":10,\n    \"y\":\"s\"\n  }},\n",
        "  {\"m\":2,\"li\":[3],\"st\":{\"x\":20,\"y\":null}}\n",
        "]"
    );
    std::fs::write(temp.path().join("case.json"), document).expect("fixture");
    let connection = connection(temp.path());
    let outcomes = drain_asset(&connection, "case.json", Some(&types), Some(&[0, 1, 2]), 4).await;
    let batch = expect_single_ok(&outcomes, "pretty-printed array document");
    assert_eq!(i64_values(&batch, "m"), [Some(0), Some(1), Some(2)]);
    assert_eq!(
        list_i64_values(&batch, "li"),
        [
            Some(vec![Some(0)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]
    );
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [
            Some((Some(0), None)),
            Some((Some(10), Some("s".into()))),
            Some((Some(20), None)),
        ]
    );
}

#[tokio::test]
async fn nested_duplicate_key_last_wins_when_selected_error_when_not() {
    let types = nested_types();
    let line = r#"{"m":1,"st":{"x":1,"y":"a","x":2}}"#;

    // Selected struct: accepted, last duplicated value wins (the generic path
    // parses selected values through serde_json::Value whose map collapses
    // duplicates silently).
    let outcomes = run_case(&[NESTED_SENTINEL, line], Some(&types), Some(&[0, 2]), 4).await;
    let batch = outcome_ok(&outcomes[0], "duplicate inside SELECTED struct collapses");
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [Some((Some(0), None)), Some((Some(2), Some("a".into())))],
        "last duplicated nested value wins on the selected path"
    );

    // Non-selected struct: rejected outright (typed seed path).
    let outcomes = run_case(&[NESTED_SENTINEL, line], Some(&types), Some(&[0]), 4).await;
    expect_schema_drift_at_row(&outcomes, "duplicate inside NON-selected struct", 2);
}

#[tokio::test]
async fn nested_negatives_match_across_selection_roles() {
    let types = nested_types();
    #[rustfmt::skip]
    let cases: &[(&str, &str)] = &[
        ("wrong-nested-type", r#"{"m":1,"li":[1],"st":{"x":"bad","y":null}}"#),
        ("unknown-nested-field", r#"{"m":1,"li":[1],"st":{"x":1,"z":2,"y":null}}"#),
        ("missing-nested-required", r#"{"m":1,"li":[1],"st":{"y":"q"}}"#),
        ("list-element-wrong-type", r#"{"m":1,"li":[1,"a"],"st":{"x":1}}"#),
        ("list-not-array", r#"{"m":1,"li":{"x":1},"st":{"x":1}}"#),
        ("struct-not-object", r#"{"m":1,"li":[1],"st":[1]}"#),
    ];
    for (name, line) in cases {
        for role in [
            ("selected", Some(&[0usize, 1, 2][..])),
            ("non-selected", Some(&[0usize][..])),
        ] {
            let outcomes = run_case(&[NESTED_SENTINEL, line], Some(&types), role.1, 4).await;
            expect_schema_drift_at_row(&outcomes, &format!("{name} ({})", role.0), 2);
        }
    }
}

#[tokio::test]
async fn framing_failures_preserve_category_order() {
    let base: Vec<(LogicalType, bool)> =
        vec![(LogicalType::Int64, false), (LogicalType::Utf8, false)];
    let sentinel = r#"{"id":0,"s":"h"}"#;

    // Malformed JSON inside the object (row 2).
    let outcomes = run_case(
        &[sentinel, "{\"id\":1,\"s\":o?ps}"],
        Some(&base),
        Some(&[0]),
        4,
    )
    .await;
    expect_invalid_data_message(&outcomes[0..1], "malformed value", SCHEMA_MSG);
    assert_eq!(
        outcomes[0].as_ref().err().unwrap().0,
        ErrorCategory::InvalidData
    );

    // The same malformed value on a SELECTED field: capture and validation
    // surface the identical InvalidData category and message.
    let outcomes = run_case(
        &[sentinel, "{\"id\":1,\"s\":o?ps}"],
        Some(&base),
        Some(&[0, 1]),
        4,
    )
    .await;
    expect_invalid_data_message(&outcomes[0..1], "malformed selected value", SCHEMA_MSG);

    // Row is not an object at all.
    let outcomes = run_case(&[sentinel, "[1,2]"], Some(&base), Some(&[0]), 4).await;
    expect_invalid_data_message(
        &outcomes[0..1],
        "non-object row",
        "JSON row is not an object",
    );

    // Trailing garbage after a complete object.
    let outcomes = run_case(
        &[sentinel, "{\"id\":1,\"s\":\"x\"} trailing"],
        Some(&base),
        Some(&[0]),
        4,
    )
    .await;
    expect_invalid_data_message(&outcomes[0..1], "trailing garbage", "JSON row is malformed");

    // Multi-row framing: valid prefix rows cannot mask the earliest failing
    // row. An UNTERMINATED string is a syntax error surfaced through the
    // shared schema-mismatch message (same category path as the generic path).
    let outcomes = run_case(
        &[
            sentinel,
            "{\"id\":1,\"s\":\"ok\"}",
            "{\"id\":2,\"s\":\"boom}extra",
        ],
        Some(&base),
        Some(&[0]),
        8,
    )
    .await;
    expect_invalid_data_message(&outcomes[0..1], "late malformed row", SCHEMA_MSG);
    let (_, message) = outcome_err(&outcomes[0], "late malformed row");
    assert!(message.contains("row 3"), "{message}");
}

#[tokio::test]
async fn long_string_row_stays_under_the_batch_byte_bound() {
    let base: Vec<(LogicalType, bool)> =
        vec![(LogicalType::Int64, false), (LogicalType::Utf8, false)];
    let long = "x".repeat(60_000);
    let line = format!("{{\"id\":1,\"s\":\"{long}\"}}");
    let outcomes = run_case(&[line.as_str()], Some(&base), Some(&[1]), 4).await;
    let batch = outcome_ok(&outcomes[0], "long row accepted");
    let value = &str_values(&batch, "s")[0];
    assert_eq!(value.as_deref().map(str::len), Some(60_000));

    // A multi-megabyte selected value near the framing bounds keeps its exact
    // bytes through capture, span assembly, and the retained decode.
    let huge = "y".repeat(4 * 1024 * 1024);
    let line = format!("{{\"id\":1,\"s\":\"{huge}\"}}");
    let outcomes = run_case(&[line.as_str()], Some(&base), Some(&[1]), 4).await;
    let batch = outcome_ok(&outcomes[0], "multi-megabyte row accepted");
    let value = &str_values(&batch, "s")[0];
    assert_eq!(
        value.as_deref().map(str::len),
        Some(4 * 1024 * 1024),
        "long selected value survives byte-exactly"
    );
}

#[tokio::test]
async fn multi_row_batch_slicing_stays_stable() {
    let types = abcd_types();
    let lines = [
        ABCD_SENTINEL,
        r#"{"a":1,"b":"r1","c":1.0,"d":true}"#,
        r#"{"a":2,"b":"r2","c":2.0}"#,
        r#"{"a":3,"b":"r3","c":3.0,"d":false}"#,
        r#"{"a":4,"b":"r4","c":4.0}"#,
        r#"{"a":5,"b":"r5","c":5.0,"d":true}"#,
    ];
    let outcomes = run_case(&lines, Some(&types), Some(&[0]), 2).await;
    assert_eq!(outcomes.len(), 3, "envelopes split 2+2+1");
    let mut seen = Vec::new();
    for outcome in &outcomes {
        seen.extend(i64_values(&outcome_ok(outcome, "slice batch"), "a"));
    }
    assert_eq!(seen, [Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]);
}

#[tokio::test]
async fn cancellation_mid_stream_preserves_the_public_surface() {
    let types = abcd_types();
    let temp = TempDir::new().expect("temporary fixture root");
    let mut body = String::from(ABCD_SENTINEL);
    for index in 1..=20_usize {
        body.push_str(&format!(
            "\n{{\"a\":{index},\"b\":\"r{index}\",\"c\":{index}.0}}"
        ));
    }
    std::fs::write(temp.path().join("case.ndjson"), format!("{body}\n")).expect("fixture");
    let cancellation = CancellationToken::new();
    let context = RequestContext::with_cancellation(cancellation.clone());
    let connection = connection(temp.path());
    let mut stream = open_stream_with_context(
        &connection,
        "case.ndjson",
        Some(&types),
        Some(&[0, 1]),
        4,
        context,
    )
    .await;

    // The first envelope streams in full before cancellation.
    let first = stream
        .next()
        .await
        .expect("first stream item")
        .expect("first envelope before cancellation");
    assert_eq!(first.row_count(), 4, "full first envelope streams");

    // Cancelling mid-stream surfaces the same terminal category in both
    // modes: every row/batch checkpoint runs the shared ensure_active gate.
    cancellation.cancel();
    let second = stream
        .next()
        .await
        .expect("stream yields a terminal outcome after cancellation");
    let error = second.expect_err("cancellation must terminate the stream");
    assert_eq!(error.category(), ErrorCategory::Cancelled);
}

#[tokio::test]
async fn expired_deadline_fails_closed_identically() {
    let types = abcd_types();
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(
        temp.path().join("case.ndjson"),
        format!("{ABCD_SENTINEL}\n"),
    )
    .expect("fixture");
    let context = RequestContext::with_deadline(
        tokio::time::Instant::now() - std::time::Duration::from_secs(1),
    );
    let connection = connection(temp.path());
    // An already-expired deadline fails closed at the FIRST admission
    // checkpoint (opening the read stream), before any row is touched.
    let error = try_open_stream_with_context(
        &connection,
        "case.ndjson",
        Some(&types),
        Some(&[0]),
        4,
        context,
    )
    .await
    .err()
    .unwrap_or_else(|| panic!("expired deadline must fail the open"));
    assert_eq!(
        error.category(),
        ErrorCategory::Timeout,
        "expired deadline surfaces as Timeout: {error}"
    );
}

#[tokio::test]
async fn json_array_shape_document_parity() {
    let base: Vec<(LogicalType, bool)> =
        vec![(LogicalType::Int64, false), (LogicalType::Utf8, false)];
    let temp = TempDir::new().expect("temp root");
    std::fs::write(
        temp.path().join("case.json"),
        r#"[{"id":1,"s":"a"},{"s":"b","id":2}]"#,
    )
    .expect("fixture");
    let outcomes = drain_asset(
        &connection(temp.path()),
        "case.json",
        Some(&base),
        Some(&[0, 1]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "array document");
    assert_eq!(i64_values(&batch, "id"), [Some(1), Some(2)]);
    assert_eq!(
        str_values(&batch, "s"),
        [Some("a".into()), Some("b".into())]
    );
}

/// Third fixture schema: m Int64, ls List(Struct{p Int64 required,
/// q Utf8 nullable}) nullable, st Struct{x Int64 required, y Utf8 nullable}.
fn list_struct_types() -> Vec<(LogicalType, bool)> {
    let element_fields = vec![
        LogicalField::new(ColumnId::random(), "p", LogicalType::Int64, false).expect("p"),
        LogicalField::new(ColumnId::random(), "q", LogicalType::Utf8, true).expect("q"),
    ];
    let st_fields = vec![
        LogicalField::new(ColumnId::random(), "x", LogicalType::Int64, false).expect("x"),
        LogicalField::new(ColumnId::random(), "y", LogicalType::Utf8, true).expect("y"),
    ];
    vec![
        (LogicalType::Int64, false),
        (
            LogicalType::List(Box::new(LogicalType::Struct(element_fields))),
            true,
        ),
        (LogicalType::Struct(st_fields), false),
    ]
}

/// Decoded list-of-struct shape: None = row-null; inner vec of (p, q) pairs.
type ListStructValues = Vec<Option<Vec<Option<(Option<i64>, Option<String>)>>>>;

fn list_struct_values(batch: &RecordBatch, column_name: &str) -> ListStructValues {
    let array = column::<ListArray>(batch, column_name);
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                return None;
            }
            let elements = array.value(i);
            let elements = elements
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("list of struct");
            let p = elements
                .column_by_name("p")
                .expect("struct field p")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("p int64");
            let q = elements
                .column_by_name("q")
                .expect("struct field q")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("q utf8");
            Some(
                (0..elements.len())
                    .map(|j| {
                        Some((
                            (!p.is_null(j)).then(|| p.value(j)),
                            (!q.is_null(j)).then(|| q.value(j).to_owned()),
                        ))
                    })
                    .collect(),
            )
        })
        .collect()
}

const LIST_STRUCT_SENTINEL: &str = r#"{"m":0,"ls":[{"p":0,"q":null}],"st":{"x":0,"y":null}}"#;

/// The generic path validates every SELECTED value in its DOM-collapsed
/// (last-value-wins) form, so a nested object that repeats a key is accepted
/// whenever the LAST occurrence is valid — even when an earlier occurrence is
/// invalid. The direct path's streaming second parse validates every
/// occurrence, so it must recover to the collapsed-form verdict to keep the
/// accept/reject set identical. These fixtures pin that recovery from both
/// directions, plus the unchanged rejection of non-selected duplicates.
#[tokio::test]
async fn selected_nested_duplicate_collapsed_form_decides_acceptance() {
    let types = list_struct_types();

    // Non-last occurrence invalid (list element struct): accepted, last wins.
    let outcomes = run_case(
        &[
            LIST_STRUCT_SENTINEL,
            r#"{"m":1,"ls":[{"p":"bad","p":2,"q":"z"}],"st":{"x":1}}"#,
        ],
        Some(&types),
        Some(&[0, 1, 2]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "invalid non-last list-element occurrence");
    assert_eq!(
        list_struct_values(&batch, "ls"),
        [
            Some(vec![Some((Some(0), None))]),
            Some(vec![Some((Some(2), Some("z".into())))]),
        ],
        "collapsed element p=2 q=z wins"
    );

    // Mirror image: the LAST occurrence is invalid — rejected in both modes.
    let outcomes = run_case(
        &[
            LIST_STRUCT_SENTINEL,
            r#"{"m":1,"ls":[{"p":2,"p":"bad","q":"z"}],"st":{"x":1}}"#,
        ],
        Some(&types),
        Some(&[0, 1, 2]),
        4,
    )
    .await;
    expect_schema_drift_at_row(&outcomes, "invalid last list-element occurrence", 2);

    // Same recovery inside a plain selected struct.
    let outcomes = run_case(
        &[LIST_STRUCT_SENTINEL, r#"{"m":1,"st":{"x":"bad","x":2}}"#],
        Some(&types),
        Some(&[0, 2]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "invalid non-last struct occurrence");
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [Some((Some(0), None)), Some((Some(2), None))],
        "collapsed x=2 wins"
    );

    // A null non-last occurrence collapses away exactly the same way.
    let outcomes = run_case(
        &[LIST_STRUCT_SENTINEL, r#"{"m":1,"st":{"x":null,"x":3}}"#],
        Some(&types),
        Some(&[0, 2]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "null non-last struct occurrence");
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [Some((Some(0), None)), Some((Some(3), None))],
        "collapsed x=3 wins"
    );

    // All-valid repeated keys keep flowing through the canonical fallback
    // (last value wins at the original position) inside list elements too.
    let outcomes = run_case(
        &[
            LIST_STRUCT_SENTINEL,
            r#"{"m":1,"ls":[{"p":1,"p":2,"q":"z"}],"st":{"x":1}}"#,
        ],
        Some(&types),
        Some(&[0, 1]),
        4,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "all-valid repeated element keys");
    assert_eq!(
        list_struct_values(&batch, "ls"),
        [
            Some(vec![Some((Some(0), None))]),
            Some(vec![Some((Some(2), Some("z".into())))]),
        ],
        "last repeated element value wins"
    );

    // The SAME duplicate-bearing rows stay rejected when the nested object is
    // NOT selected: the typed streaming path rejects duplicates outright in
    // both modes.
    for (name, line) in [
        (
            "non-selected invalid non-last",
            r#"{"m":1,"ls":[{"p":"bad","p":2,"q":"z"}],"st":{"x":1}}"#,
        ),
        (
            "non-selected all-valid",
            r#"{"m":1,"ls":[{"p":1,"p":2,"q":"z"}],"st":{"x":1}}"#,
        ),
    ] {
        let outcomes = run_case(&[LIST_STRUCT_SENTINEL, line], Some(&types), Some(&[0]), 4).await;
        expect_schema_drift_at_row(&outcomes, name, 2);
    }
}

/// Raw JSON control bytes (newlines, carriage returns, tabs) inside a selected
/// List/Struct value are structural whitespace only. The generic path
/// re-serializes selected values compactly, so its assembled rows never carry
/// a raw control byte; the direct path must canonicalize exactly those
/// captures (including \r, which a mid-line fixture can produce) so the
/// downstream line-oriented JsonReader never sees a split row.
#[tokio::test]
async fn raw_control_bytes_inside_selected_subtree_are_canonicalized() {
    let types = list_struct_types();
    let temp = TempDir::new().expect("temporary fixture root");
    let document = "[\n".to_owned()
        + r#"  {"m":0,"ls":[{"p":0,"q":null}],"st":{"x":0,"y":null}},"#
        + "\n"
        + r#"  {"m":1,"ls":[
		{"p":1,
		 "q":"a"}
  ],"st":{
		"x":10,
		"y":"s"
  }},"# + "\n"
        + r#"  {"m":2,"ls":[
    {"p":3}
  ],"st":{"x":20,"y":null}}
]"#;
    std::fs::write(temp.path().join("case.json"), document).expect("fixture");
    let connection = connection(temp.path());
    let outcomes = drain_asset(&connection, "case.json", Some(&types), Some(&[0, 1, 2]), 4).await;
    let batch = expect_single_ok(&outcomes, "control-byte document");
    assert_eq!(i64_values(&batch, "m"), [Some(0), Some(1), Some(2)]);
    assert_eq!(
        list_struct_values(&batch, "ls"),
        [
            Some(vec![Some((Some(0), None))]),
            Some(vec![Some((Some(1), Some("a".into())))]),
            Some(vec![Some((Some(3), None))]),
        ],
        "tab/CR/LF-padded selected subtrees decode identically"
    );
    assert_eq!(
        struct_xy_values(&batch, "st"),
        [
            Some((Some(0), None)),
            Some((Some(10), Some("s".into()))),
            Some((Some(20), None)),
        ]
    );
}

/// Integer literals wider than serde_json's own integer parse are promoted to
/// f64 by the generic DOM parse and re-encoded in that normalized form; the
/// direct path's raw spans must canonicalize exactly those captures or the
/// retained Polars JsonReader rejects the bare integer spelling on float
/// columns. Pins the scalar, list-element, negative-overflow, and
/// u64-boundary surfaces.
#[tokio::test]
async fn wide_integer_literals_match_generic_encoding() {
    let base: Vec<(LogicalType, bool)> = vec![
        (LogicalType::Float64, false),
        (LogicalType::List(Box::new(LogicalType::Float64)), true),
    ];
    fn list_f64_first(batch: &RecordBatch, column_name: &str) -> Vec<Option<Option<f64>>> {
        let array = column::<ListArray>(batch, column_name);
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    return None;
                }
                let values = array.value(i);
                let values = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("list of float64");
                Some((!values.is_null(0)).then(|| values.value(0)))
            })
            .collect()
    }
    const SENTINEL: &str = r#"{"w":0.0,"li":[0.0]}"#;
    #[rustfmt::skip]
    let cases: &[(&str, &str, f64, Option<f64>)] = &[
        // 31-digit positive integer on a Float64 field: the generic path
        // re-encodes the DOM f64; the raw integer spelling is unusable.
        ("scalar-overflow", r#"{"w":1000000000000000000000000000000,"li":null}"#, 1e30, None),
        // Same literal inside a selected list.
        ("list-element-overflow", r#"{"w":0.0,"li":[1000000000000000000000000000000]}"#, 0.0, Some(1e30)),
        // 19-digit negative overflows i64::MIN and is promoted to f64.
        ("negative-overflow", r#"{"w":-9999999999999999999,"li":null}"#, -1e19, None),
        // u64::MAX keeps its unsigned form end to end.
        ("u64-boundary", r#"{"w":18446744073709551615,"li":null}"#, 18446744073709551616.0, None),
    ];
    for (name, line, expected_w, expected_li) in cases {
        let outcomes = run_case(&[SENTINEL, line], Some(&base), Some(&[0, 1]), 4).await;
        let batch = expect_single_ok(&outcomes, name);
        assert_eq!(
            f64_values(&batch, "w"),
            [Some(0.0), Some(*expected_w)],
            "{name}: float field decodes identically"
        );
        match expected_li {
            Some(value) => assert_eq!(
                list_f64_first(&batch, "li"),
                [Some(Some(0.0)), Some(Some(*value))],
                "{name}: wide literal decodes as f64 inside the list"
            ),
            None => assert_eq!(
                list_f64_first(&batch, "li"),
                [Some(Some(0.0)), None],
                "{name}: null list survives"
            ),
        }
    }
}
