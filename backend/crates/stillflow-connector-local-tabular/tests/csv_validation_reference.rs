//! O1-C1 (#298) — CSV validation reference suite.
//!
//! Pins the CURRENT end-to-end behavior of the local-tabular CSV/TSV read
//! path — the exact acceptance ranges per logical type, the illegal-input
//! surfaces, the error categories/messages, the decode/validator lockstep
//! ordering, and the bounded-read / early-release behavior — as the admission
//! gate for any future validator change (the O0-C1 evidence, issue #285 /
//! PR #295, counted the lockstep `csv`-crate validator at 26.5–36% of ingest
//! time and 100% logical re-read; trimming it requires this suite to pass
//! unchanged, per the O1-C2 task #299).
//!
//! # The two-layer error surface (measured reality, pinned here)
//!
//! The path is `Polars CSV decode` (strict dtypes, `missing_is_null`,
//! `try_parse_dates`) FOLLOWED BY the lockstep `csv`-crate validator
//! (`flexible(false)`), batch by batch. Their overlap determines which error
//! a given input produces:
//!
//! 1. **Decoder-first**: most malformed values fail the strict Polars dtype
//!    parse before the validator sees the row. These surface as the
//!    normalized message "source data is malformed or incompatible with the
//!    established schema" (category per the message text: parse/schema-class
//!    → `SchemaDrift`, else `InvalidData`) WITHOUT row information.
//! 2. **Validator-only surfaces** (the decoder is lenient where the validator
//!    is strict): non-finite float spellings (`inf`/`NaN` — Rust `parse`
//!    accepts them, Polars re-encodes them, the finite check rejects), an
//!    empty field on a non-nullable column (`missing_is_null` fills it, the
//!    empty rule rejects), whitespace-padded numerics (the decoder trims,
//!    `str::parse` does not), and text-shape mismatches the decoder tolerates.
//!    These surface as the granular `SchemaDrift` "delimited value does not
//!    match the established schema at row N" with ONE-BASED data-row numbers
//!    (header excluded).
//! 3. **Ragged rows are asymmetric**: a row with TOO FEW fields is padded by
//!    the decoder (`missing_is_null`), so the lockstep `csv`-crate reader
//!    (`flexible(false)`) hits its own UnequalLengths error and surfaces
//!    "delimited source contains a malformed row" (`InvalidData`); a row with
//!    TOO MANY fields fails the decoder first (normalized message). The
//!    in-loop width check in `read.rs::validate_rows` is defense-in-depth
//!    behind both — a fact O1-C2 must preserve or consciously change.
//! 4. **Blank lines anywhere** are counted by the `csv`-crate reader but
//!    skipped by the Polars decoder, so the lockstep count gate fails closed:
//!    "delimited decoder row counts are inconsistent" (`InvalidData`). A CSV
//!    file containing a blank line is not readable, at any position.
//! 5. **`Binary`, `List`, `Struct` columns** fail closed in the decoder over
//!    CSV text (normalized message) — the validator's per-type rejection for
//!    them is behind the decoder surface. A timezone-bearing `Timestamp`
//!    column over CSV fails closed at the Arrow bridge
//!    ("decoded values cannot be represented by the established schema").
//!
//! Traceability: the small inline fixtures mirror the O0-C1 probe structure
//! (decoder + lockstep validator over the same bytes, bounded inference
//! sentinel as row 1); the large #295 digest-identified fixtures stay in the
//! O0 evidence harnesses, not here.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, Int8Array, StringArray, UInt8Array,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    BatchEnvelope, ColumnId, ConnectorError, ConnectorKind, CredentialRef, DiscoverRequest,
    ErrorCategory, InspectRequest, LogicalField, LogicalSchema, LogicalType, ReadRequest,
    RequestContext, SourceConnection, TimeUnit,
};
use tempfile::TempDir;

/// The public read-stream surface returned by `read_batches`.
type EnvelopeStream = Pin<Box<dyn Stream<Item = Result<BatchEnvelope, ConnectorError>> + Send>>;

/// One drained outcome: a payload batch, or the terminal stream error
/// (category + full stable message).
type Outcome = Result<arrow_array::RecordBatch, (ErrorCategory, String)>;

const DECODER_MSG: &str = "source data is malformed or incompatible with the established schema";
const VALUE_MSG: &str = "delimited value does not match the established schema";
const MALFORMED_MSG: &str = "delimited source contains a malformed row";
const COUNT_MSG: &str = "delimited decoder row counts are inconsistent";
const BRIDGE_MSG: &str = "decoded values cannot be represented by the established schema";

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 8388608 }
        }),
        CredentialRef::new("cred://local/fixtures").expect("credential reference"),
    )
    .expect("connection")
}

/// Same root with the O1-C2-relevant config knobs pinned explicitly.
fn connection_with(root: &std::path::Path, delimiter: &str, quote: &str) -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 8388608 },
            "csv": { "delimiter": delimiter, "quote": quote, "hasHeader": true }
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

fn override_schema(source: &LogicalSchema, types: &[(LogicalType, bool)]) -> LogicalSchema {
    assert_eq!(types.len(), source.fields.len());
    let fields = source
        .fields
        .iter()
        .zip(types.iter())
        .map(|(source, (data_type, nullable))| {
            LogicalField::new(source.id, source.name.clone(), data_type.clone(), *nullable)
                .expect("override field")
        })
        .collect();
    LogicalSchema::new(fields).expect("override schema")
}

/// Writes `body` as `case.csv`, discovers/inspects it, pins the logical types
/// via the schema override (keeping source names/order), and drains the read
/// stream.
async fn run_csv(
    body: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    batch_size: usize,
) -> Vec<Outcome> {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.csv"), body).expect("write case.csv");
    let connection = connection(temp.path());
    drain(&connection, "case.csv", override_types, batch_size).await
}

/// TSV variant of `run_csv` (`.tsv` extension → tab delimiter).
async fn run_tsv(
    body: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    batch_size: usize,
) -> Vec<Outcome> {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.tsv"), body).expect("write case.tsv");
    let connection = connection(temp.path());
    drain(&connection, "case.tsv", override_types, batch_size).await
}

async fn drain(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    batch_size: usize,
) -> Vec<Outcome> {
    let mut stream = try_open_stream(connection, name, override_types, batch_size)
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

async fn try_open_stream(
    connection: &SourceConnection,
    name: &str,
    override_types: Option<&[(LogicalType, bool)]>,
    batch_size: usize,
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
        .await?;

    let override_schema = override_types.map(|types| override_schema(&metadata.schema, types));
    let mut request = ReadRequest::new(asset, batch_size);
    request.schema_override = override_schema;
    registry.read_batches(connection, request).await
}

fn expect_single_ok<'a>(outcomes: &'a [Outcome], case: &str) -> &'a arrow_array::RecordBatch {
    assert_eq!(outcomes.len(), 1, "{case}: expected exactly one envelope");
    match &outcomes[0] {
        Ok(batch) => batch,
        Err((category, message)) => panic!("{case}: unexpected error {category:?}: {message}"),
    }
}

fn expect_err<'a>(outcomes: &'a [Outcome], case: &str) -> &'a (ErrorCategory, String) {
    match outcomes.last().expect("terminal outcome") {
        Err(error) => error,
        Ok(_) => panic!("{case}: expected a terminal stream error"),
    }
}

macro_rules! typed_values {
    ($batch:expr, $name:expr, $array:ty) => {{
        let column = $batch
            .column_by_name($name)
            .unwrap_or_else(|| panic!("column {}", $name))
            .as_any()
            .downcast_ref::<$array>()
            .unwrap_or_else(|| panic!("{} is not an {}", $name, stringify!($array)));
        (0..column.len())
            .map(|i| {
                if column.is_null(i) {
                    None
                } else {
                    Some(column.value(i))
                }
            })
            .collect::<Vec<_>>()
    }};
}

fn i64_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<i64>> {
    typed_values!(batch, name, Int64Array)
}

fn i8_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<i8>> {
    typed_values!(batch, name, Int8Array)
}

fn u8_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<u8>> {
    typed_values!(batch, name, UInt8Array)
}

fn f64_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<f64>> {
    typed_values!(batch, name, Float64Array)
}

fn bool_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<bool>> {
    typed_values!(batch, name, BooleanArray)
}

fn str_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<String>> {
    typed_values!(batch, name, StringArray)
        .into_iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}

fn date_values(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<i32>> {
    typed_values!(batch, name, Date32Array)
}

// ---------------------------------------------------------------------------
// 1. Integer acceptance: exactly Rust `str::parse` for the type; everything
//    Rust rejects, the strict decoder rejects first (normalized message, no
//    row number).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn integer_accepts_plus_sign_and_boundaries() {
    let body = "n\n+1\n-1\n127\n-128\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Int8, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "int8 boundaries");
    assert_eq!(
        i8_values(batch, "n"),
        [Some(1), Some(-1), Some(127), Some(-128)]
    );
}

#[tokio::test]
async fn integer_overflow_and_padding_fail_in_the_decoder() {
    // "128" overflows Int8: the decoder errors first — normalized message,
    // no row number, SchemaDrift (parse-class text).
    let outcomes = run_csv("n\n127\n128\n", Some(&[(LogicalType::Int8, false)]), 4096).await;
    let error = expect_err(&outcomes, "int8 overflow");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);

    // Unsigned negatives/overflow: same decoder-first surface.
    let outcomes = run_csv(
        "n\n0\n255\n256\n",
        Some(&[(LogicalType::UInt8, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "uint8 overflow");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);

    // Float text on an integer column: decoder-first.
    let outcomes = run_csv("n\n1.0\n", Some(&[(LogicalType::Int64, false)]), 4096).await;
    let error = expect_err(&outcomes, "float text");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);

    // Boundary values decode exactly.
    let outcomes = run_csv("n\n0\n255\n", Some(&[(LogicalType::UInt8, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "uint8 boundary");
    assert_eq!(u8_values(batch, "n"), [Some(0), Some(255)]);
}

#[tokio::test]
async fn whitespace_padded_numerics_fail_in_the_validator_with_the_row() {
    // The decoder trims surrounding whitespace; `str::parse` does not. The
    // value decodes, the lockstep validator rejects it — granular, with the
    // row.
    let outcomes = run_csv("n\n 1\n", Some(&[(LogicalType::Int64, false)]), 4096).await;
    let error = expect_err(&outcomes, "leading space");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 1"));
}

#[tokio::test]
async fn integer_empty_field_is_null_iff_nullable() {
    // An empty FIELD inside a row (an empty LINE is a different, structural
    // case — see `blank_lines_anywhere_break_the_row_count_lockstep`).
    let body = "n,m\n1,\n2,3\n";
    let outcomes = run_csv(
        body,
        Some(&[(LogicalType::Int64, true), (LogicalType::Int64, true)]),
        4096,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "nullable empty");
    assert_eq!(i64_values(batch, "n"), [Some(1), Some(2)]);
    assert_eq!(i64_values(batch, "m"), [None, Some(3)]);

    // Empty on a REQUIRED column: the decoder fills null (missing_is_null),
    // the lockstep validator rejects — the granular surface, with the row.
    // (Measured quirk: in a SINGLE-column schema the all-empty row fails the
    // decoder instead — normalized message. Two columns keep the granular
    // surface; both pinned here via the two-column form.)
    let body = "n,m\n,3\n2,3\n";
    let outcomes = run_csv(
        body,
        Some(&[(LogicalType::Int64, false), (LogicalType::Int64, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "required empty");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 1"));
}

// ---------------------------------------------------------------------------
// 2. Float acceptance: Rust parse + finite. `inf`/`NaN` are the canonical
//    validator-only surface: the decoder accepts and re-encodes them, the
//    finite check rejects, and the granular row number is preserved.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn float64_accepts_finite_spellings_and_decodes_exactly() {
    let body = "x\n1.5\n-0.0\n1e3\n-2.5e-3\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Float64, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "float64 decode");
    assert_eq!(
        f64_values(batch, "x"),
        [Some(1.5), Some(-0.0), Some(1000.0), Some(-0.0025)]
    );
}

#[tokio::test]
async fn non_finite_floats_fail_in_the_validator_with_the_exact_row() {
    let body = "x\n1.5\ninf\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Float64, false)]), 4096).await;
    let error = expect_err(&outcomes, "inf");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 2"));

    let body = "x\n1.0\n2.0\nnan\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Float64, false)]), 4096).await;
    let error = expect_err(&outcomes, "NaN");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 3"));

    // A value that overflows f32 to infinity: same validator surface.
    let body = "x\n1e30\n1e40\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Float32, false)]), 4096).await;
    let error = expect_err(&outcomes, "f32 overflow");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 2"));
}

// ---------------------------------------------------------------------------
// 3. Boolean: exactly "true" / "false", case-sensitive; every other spelling
//    fails in the decoder.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn boolean_accepts_only_exact_lowercase_spellings() {
    let body = "b\ntrue\nfalse\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Boolean, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "boolean decode");
    assert_eq!(bool_values(batch, "b"), [Some(true), Some(false)]);

    // "True" is the validator-only boolean surface: the decoder is
    // case-lenient (accepts and decodes it), the exact-match rule rejects it.
    let outcomes = run_csv(
        "b\ntrue\nTrue\n",
        Some(&[(LogicalType::Boolean, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "True");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 2"));

    let outcomes = run_csv("b\nyes\n", Some(&[(LogicalType::Boolean, false)]), 4096).await;
    let error = expect_err(&outcomes, "yes");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);
}

// ---------------------------------------------------------------------------
// 4. Utf8 accepts any non-empty text, including delimiter, quote and newline
//    bytes inside quotes, unicode, and control bytes; empty is null iff
//    nullable.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn utf8_accepts_delimiters_quotes_and_newlines_inside_quotes() {
    let body = "s\n\"a,b\"\n\"say \"\"hi\"\"\"\n\"line1\nline2\"\nplain\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Utf8, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "utf8 matrix");
    assert_eq!(
        str_values(batch, "s"),
        [
            Some("a,b".into()),
            Some("say \"hi\"".into()),
            Some("line1\nline2".into()),
            Some("plain".into()),
        ]
    );
}

#[tokio::test]
async fn utf8_accepts_unicode_and_control_bytes_inside_quotes() {
    let body = "s\n\"héllo → 世界\"\n\"tab\there\"\n\"bell:\u{7}\"";
    let outcomes = run_csv(body, Some(&[(LogicalType::Utf8, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "utf8 unicode/control");
    assert_eq!(str_values(batch, "s")[0], Some("héllo → 世界".into()));
    assert_eq!(str_values(batch, "s")[1], Some("tab\there".into()));
    assert_eq!(str_values(batch, "s")[2], Some("bell:\u{7}".into()));
}

#[tokio::test]
async fn utf8_empty_field_is_null_iff_nullable() {
    // An empty FIELD inside a row (an empty LINE is a different, structural
    // case — see `blank_lines_anywhere_break_the_row_count_lockstep`).
    let body = "s,t\nx,\ny,z\n";
    let outcomes = run_csv(
        body,
        Some(&[(LogicalType::Utf8, true), (LogicalType::Utf8, true)]),
        4096,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "utf8 nullable empty");
    assert_eq!(str_values(batch, "s"), [Some("x".into()), Some("y".into())]);
    assert_eq!(str_values(batch, "t"), [None, Some("z".into())]);

    // Empty on a REQUIRED Utf8 column: the same granular validator surface
    // as the integer case (the single-column decoder quirk does not depend
    // on the type — see the integer test's comment).
    let body = "s,t\n,z\ny,z\n";
    let outcomes = run_csv(
        body,
        Some(&[(LogicalType::Utf8, false), (LogicalType::Utf8, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "utf8 required empty");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 1"));
}

// ---------------------------------------------------------------------------
// 5. Temporal: Date32 accepts %Y-%m-%d INCLUDING non-padded months/days
//    (chrono and the decoder both accept "2024-1-3"); impossible dates and
//    datetime text fail in the decoder. Naive timestamps fail CLOSED over
//    CSV under every unit; timezone-bearing text on a naive column fails in
//    the validator with the row; a timezone-bearing column fails closed at
//    the Arrow bridge.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn date32_accepts_iso_dates_including_non_padded_forms() {
    let body = "d\n2024-01-31\n1970-01-01\n0001-01-01\n2024-1-3\n";
    let outcomes = run_csv(body, Some(&[(LogicalType::Date32, false)]), 4096).await;
    let batch = expect_single_ok(&outcomes, "date32 decode");
    assert_eq!(date_values(batch, "d").len(), 4, "all four forms decode");
}

#[tokio::test]
async fn date32_rejects_impossible_dates_and_datetime_text_in_the_decoder() {
    let outcomes = run_csv(
        "d\n2024-02-30\n",
        Some(&[(LogicalType::Date32, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "impossible date");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);

    let outcomes = run_csv(
        "d\n2024-01-31T00:00:00\n",
        Some(&[(LogicalType::Date32, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "datetime text");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);
}

#[tokio::test]
async fn naive_timestamps_fail_closed_over_csv_under_every_unit() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        let body = "t\n2024-01-31T12:34:56\n2024-01-31 12:34:56.5\n";
        let outcomes = run_csv(
            body,
            Some(&[(
                LogicalType::Timestamp {
                    unit,
                    timezone: None,
                },
                false,
            )]),
            4096,
        )
        .await;
        let error = expect_err(&outcomes, "naive timestamp");
        assert_eq!(error.0, ErrorCategory::SchemaDrift, "unit {unit:?}");
        assert_eq!(error.1, DECODER_MSG, "unit {unit:?}");
    }
}

#[tokio::test]
async fn zoned_text_on_a_naive_column_fails_in_the_validator_with_the_row() {
    let body = "t\n2024-01-31T12:34:56Z\n";
    let outcomes = run_csv(
        body,
        Some(&[(
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
            false,
        )]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "zoned text on naive column");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, format!("{VALUE_MSG} at row 1"));
}

#[tokio::test]
async fn zoned_timestamp_columns_fail_closed_at_the_bridge_over_csv() {
    let body = "t\n2024-01-31T12:34:56Z\n2024-01-31T12:34:56-05:00\n";
    let outcomes = run_csv(
        body,
        Some(&[(
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            false,
        )]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "zoned column");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, BRIDGE_MSG);
}

// ---------------------------------------------------------------------------
// 6. Binary, List and Struct columns fail closed in the decoder over CSV
//    text (validator rejections for them sit behind the decoder surface).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn binary_columns_fail_closed_in_the_decoder() {
    let outcomes = run_csv(
        "b\n\"bytes\"\n",
        Some(&[(LogicalType::Binary, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "binary column");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, DECODER_MSG);
}

#[tokio::test]
async fn list_and_struct_columns_fail_closed_in_the_decoder() {
    let outcomes = run_csv(
        "l\n\"[1,2]\"\n1\n",
        Some(&[(LogicalType::List(Box::new(LogicalType::Int64)), false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "list column");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, DECODER_MSG);

    let fields =
        vec![LogicalField::new(ColumnId::random(), "x", LogicalType::Int64, false).expect("x")];
    let outcomes = run_csv(
        "s\n\"{\"\"x\"\":1}\"\n1\n",
        Some(&[(LogicalType::Struct(fields), false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "struct column");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, DECODER_MSG);
}

// ---------------------------------------------------------------------------
// 7. Structural errors: ragged rows surface from the csv-crate reader itself
//    (flexible(false)) as "malformed row"; blank lines ANYWHERE break the
//    decoder/validator row-count lockstep and fail closed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ragged_rows_are_asymmetric_short_and_long() {
    let types = [(LogicalType::Int64, false), (LogicalType::Int64, false)];

    // TOO FEW fields: the decoder pads with null, the csv-crate reader
    // (flexible(false)) hits its own UnequalLengths error → malformed row.
    let outcomes = run_csv("a,b\n1,2\n3\n", Some(&types), 4096).await;
    let error = expect_err(&outcomes, "too few fields");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, MALFORMED_MSG);

    // TOO MANY fields: the decoder fails first → normalized message.
    let outcomes = run_csv("a,b\n1,2\n3,4,5\n", Some(&types), 4096).await;
    let error = expect_err(&outcomes, "too many fields");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);
}

#[tokio::test]
async fn unterminated_quotes_fail_in_the_decoder() {
    let outcomes = run_csv(
        "s\n\"unterminated\n",
        Some(&[(LogicalType::Utf8, false)]),
        4096,
    )
    .await;
    let error = expect_err(&outcomes, "unterminated quote");
    assert_eq!(error.0, ErrorCategory::SchemaDrift);
    assert_eq!(error.1, DECODER_MSG);
}

#[tokio::test]
async fn blank_lines_anywhere_break_the_row_count_lockstep() {
    let types = [(LogicalType::Int64, true)];

    // Trailing blank line.
    let outcomes = run_csv("n\n1\n\n", Some(&types), 4096).await;
    let error = expect_err(&outcomes, "trailing blank line");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, COUNT_MSG);

    // Blank line in the middle.
    let outcomes = run_csv("n\n1\n\n2\n", Some(&types), 4096).await;
    let error = expect_err(&outcomes, "middle blank line");
    assert_eq!(error.0, ErrorCategory::InvalidData);
    assert_eq!(error.1, COUNT_MSG);
}

// ---------------------------------------------------------------------------
// 8. TSV and configured delimiters/quotes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tsv_uses_tab_delimiter_with_the_same_value_surface() {
    let body = "a\tb\n1\tx\n2\ty\n";
    let outcomes = run_tsv(
        body,
        Some(&[(LogicalType::Int64, false), (LogicalType::Utf8, false)]),
        4096,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "tsv decode");
    assert_eq!(i64_values(batch, "a"), [Some(1), Some(2)]);
    assert_eq!(str_values(batch, "b"), [Some("x".into()), Some("y".into())]);
}

#[tokio::test]
async fn configured_delimiter_and_quote_are_honored() {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.csv"), "a;b\n1;'x;y'\n2;'z'\n").expect("fixture");
    let connection = connection_with(temp.path(), ";", "'");
    let outcomes = drain(
        &connection,
        "case.csv",
        Some(&[(LogicalType::Int64, false), (LogicalType::Utf8, false)]),
        4096,
    )
    .await;
    let batch = expect_single_ok(&outcomes, "custom delimiter");
    assert_eq!(i64_values(batch, "a"), [Some(1), Some(2)]);
    assert_eq!(
        str_values(batch, "b"),
        [Some("x;y".into()), Some("z".into())]
    );
}

// ---------------------------------------------------------------------------
// 9. Batching and bounded reads: per-row values are invariant to the batch
//    size, and the envelope slicing is deterministic.
// ---------------------------------------------------------------------------

fn five_row_fixture() -> String {
    "a,b\n1,x\n2,y\n3,z\n4,w\n5,v\n".to_owned()
}

fn flattened_columns(outcomes: &[Outcome]) -> (Vec<Option<i64>>, Vec<Option<String>>) {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for outcome in outcomes {
        let batch = outcome.as_ref().expect("ok batch");
        a.extend(i64_values(batch, "a"));
        b.extend(str_values(batch, "b"));
    }
    (a, b)
}

#[tokio::test]
async fn batch_slicing_is_deterministic_and_values_are_batch_size_invariant() {
    let body = five_row_fixture();
    let types = [(LogicalType::Int64, false), (LogicalType::Utf8, false)];

    let single = run_csv(&body, Some(&types), 1).await;
    assert_eq!(single.len(), 5, "batch_size 1 → one envelope per row");
    let bulk = run_csv(&body, Some(&types), 4096).await;
    assert_eq!(bulk.len(), 1, "batch_size 4096 → one envelope for 5 rows");
    let sliced = run_csv(&body, Some(&types), 2).await;
    assert_eq!(sliced.len(), 3, "batch_size 2 → 2/2/1 envelopes");

    // Per-row values are identical across all three slicings.
    let (single_a, single_b) = flattened_columns(&single);
    assert_eq!(single_a, [Some(1), Some(2), Some(3), Some(4), Some(5)]);
    assert_eq!(
        single_b,
        ["x", "y", "z", "w", "v"].map(|s| Some(s.to_owned()))
    );
    assert_eq!(
        flattened_columns(&bulk),
        (single_a.clone(), single_b.clone())
    );
    assert_eq!(flattened_columns(&sliced), (single_a, single_b));
}

// ---------------------------------------------------------------------------
// 10. Early release: dropping the stream mid-read is clean, releases the
//     source, and does not poison the asset for a subsequent read.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_the_stream_mid_read_is_clean_and_releases_the_source() {
    // 10k rows so the stream is still mid-read when dropped after one batch.
    let mut body = String::from("a\n");
    for index in 0..10_000_usize {
        body.push_str(&index.to_string());
        body.push('\n');
    }
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.csv"), &body).expect("fixture");
    let connection = connection(temp.path());

    {
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
            .iter()
            .find(|asset| asset.name == "case.csv")
            .expect("asset")
            .clone();
        let mut stream: EnvelopeStream = registry
            .read_batches(&connection, ReadRequest::new(asset, 256))
            .await
            .expect("open stream");
        let first = stream.next().await.expect("first batch").expect("ok");
        assert_eq!(first.row_count(), 256);
        // Early release boundary (O0-C1 early-drop semantics): the consumer
        // stops reading here and drops the stream without draining.
        drop(stream);
    }

    // The same asset reads fully and correctly afterwards: no poisoned state,
    // no retained file handle observable through the connector surface.
    let outcomes = drain(&connection, "case.csv", None, 4096).await;
    let mut values = Vec::new();
    for outcome in &outcomes {
        values.extend(i64_values(outcome.as_ref().expect("ok batch"), "a"));
    }
    assert_eq!(
        values,
        (0..10_000_usize)
            .map(|v| Some(v as i64))
            .collect::<Vec<_>>()
    );
}
