//! E24-JSON-A2 differential oracle for issue #148 (`json-direct-projected-writer`).
//!
//! Every test asserts absolute observable behavior (final tabular values as
//! extracted from the public envelopes, error categories, stable messages,
//! earliest failing row) on identical fixture bytes. The file compiles and must
//! pass IDENTICALLY with the private feature off (exact current production
//! path) and on (raw-slice direct projected assembly). Running the connector
//! suite in both modes is the mechanical OFF/ON comparison required by the
//! experiment contract; any mismatch is a semantic reject.
//!
//! Fixture convention: inspection samples only the FIRST row (`maxRows: 1`),
//! which acts as the canonical schema-establishing sentinel; test content
//! follows from row 2 onward, mirroring post-inspection drift in production.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampMillisecondArray, UInt64Array,
};
use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    ColumnId, CredentialRef, DiscoverRequest, ErrorCategory, InspectRequest, LogicalField,
    LogicalSchema, LogicalType, ReadRequest, RequestContext, SourceConnection, TimeUnit,
};
use tempfile::TempDir;

const SCHEMA_MSG: &str = "JSON row does not match the established schema";

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
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
    drain_asset(
        &connection(temp.path()),
        "case.ndjson",
        override_types,
        projection_indices,
        batch_size,
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
    request.schema_override = override_schema;
    request.projection = projection;
    let mut stream = registry
        .read_batches(connection, request)
        .await
        .expect("open read stream");

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
    ];
    let outcomes = run_case(&lines, Some(&base), Some(&[0, 1, 2, 3]), 4).await;
    let batch = expect_single_ok(&outcomes, "boundary values accepted");
    assert_eq!(
        i64_values(&batch, "iv"),
        [Some(i64::MIN), Some(i64::MAX), Some(0)]
    );
    assert_eq!(
        u64_values(&batch, "u64v"),
        [Some(u64::MAX), Some(0), Some(1709166123123)]
    );
    assert_eq!(
        f64_values(&batch, "fv"),
        [Some(100.0), Some(-0.25), Some(-0.0015)]
    );
    assert_eq!(
        str_values(&batch, "s"),
        [Some("edge".into()), Some("".into()), Some("exp".into())]
    );
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
    // with a pre-existing x1000 scale quirk in BOTH modes (baseline behavior,
    // covered separately by the temporal negative test above), so here they
    // would only add an out-of-scope baseline constant to pin.
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
    // ~56131). The experiment retains that parser verbatim, so the in-scope
    // guarantees here are: both rows decode non-null and row ORDER survives
    // the monotonic shift; cross-mode byte parity of outcomes is proven by
    // running this identical suite under feature OFF and feature ON.
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
