//! O1-C2 (#299) — per-cell validator accept-set differential corpus.
//!
//! The O0-C1 evidence (`o0-c1-csv-duplicate-work.md` §11.4) gates any removal
//! of a duplicated validation substep on a per-`LogicalType` accept-set
//! equivalence proof between the lockstep `csv`-crate validator
//! (`csv_value_matches`) and the strict Polars decode under the exact
//! production options. This corpus is that proof, executed end-to-end through
//! the real connector path: every spelling's terminating surface — decoder
//! (normalized message), lockstep validator (granular `at row N`), bridge, or
//! acceptance — is pinned here and must not move.
//!
//! The O1-C1 suite (`csv_validation_reference.rs`) remains the primary
//! admission gate and passes unchanged; this file extends the pinned surface
//! with the spelling classes the numeric re-verification fast paths (O1-C2)
//! rely on: digit-only integers, bounded digit/dot floats, every Rust
//! non-finite spelling variant, sign/boundary/overflow integers, Unicode
//! digits, whitespace paddings, and the invalid-UTF-8 surfaces that keep the
//! validator's byte-record conversion lazy.
//!
//! Fixture convention: one quoted data cell per file (`maxRows: 1` inference
//! sentinel as the header, schema override pins the type), so every case is
//! observed independently of batch abort ordering.

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    BatchEnvelope, ConnectorError, ConnectorKind, CredentialRef, DiscoverRequest, ErrorCategory,
    InspectRequest, LogicalField, LogicalSchema, LogicalType, ReadRequest, RequestContext,
    SourceConnection, TimeUnit,
};
use tempfile::TempDir;

/// One drained outcome: unit on full acceptance, or the terminal stream error
/// (category + full stable message).
type Outcome = Result<(), (ErrorCategory, String)>;

const DECODER_MSG: &str = "source data is malformed or incompatible with the established schema";
const VALUE_AT_ROW_1: &str = "delimited value does not match the established schema at row 1";
const BRIDGE_MSG: &str = "decoded values cannot be represented by the established schema";
const INSPECT_UTF8_MSG: &str = "text source is not valid UTF-8";

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

fn single_type_schema(
    source: &LogicalSchema,
    data_type: &LogicalType,
    nullable: bool,
) -> LogicalSchema {
    let fields = source
        .fields
        .iter()
        .map(|source| {
            LogicalField::new(source.id, source.name.clone(), data_type.clone(), nullable)
                .expect("override field")
        })
        .collect();
    LogicalSchema::new(fields).expect("override schema")
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

/// Writes `name` with `body`, pins every column to `data_type`, and drains the
/// read stream to its terminal outcome.
async fn probe(name: &str, body: &[u8], data_type: &LogicalType, nullable: bool) -> Outcome {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join(name), body).expect("write fixture");
    let connection = connection(temp.path());
    let assets = registry()
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
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("{name} discovered"))
        .clone();
    let metadata = registry()
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");
    let mut request = ReadRequest::new(asset, 4096);
    request.schema_override = Some(single_type_schema(&metadata.schema, data_type, nullable));
    let mut stream: Pin<
        Box<dyn futures::Stream<Item = Result<BatchEnvelope, ConnectorError>> + Send>,
    > = registry()
        .read_batches(&connection, request)
        .await
        .expect("open read stream");
    let mut outcome = Outcome::Ok(());
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => {}
            Err(error) => {
                outcome = Outcome::Err((error.category(), error.to_string()));
                break;
            }
        }
    }
    outcome
}

async fn quoted_cell(spelling: &str, data_type: &LogicalType) -> Outcome {
    let body = format!("c\n\"{spelling}\"\n");
    probe("case.csv", body.as_bytes(), data_type, false).await
}

/// Asserts one spelling's terminating surface. `Ok(())` = the cell is accepted
/// end-to-end; `(category, message)` = the terminal error must match exactly.
#[track_caller]
fn expect_surface(outcome: &Outcome, expected: Result<(), &str>, label: &str, spelling: &str) {
    match (outcome, expected) {
        (Ok(()), Ok(())) => {}
        (Err((category, message)), Err(expected_message)) => {
            assert_eq!(
                message, expected_message,
                "{label}: wrong message for {spelling:?}"
            );
            if expected_message != BRIDGE_MSG && expected_message != INSPECT_UTF8_MSG {
                assert_eq!(
                    *category,
                    ErrorCategory::SchemaDrift,
                    "{label}: wrong category for {spelling:?}"
                );
            }
        }
        (Ok(()), Err(expected_message)) => {
            panic!("{label}: {spelling:?} was accepted, expected error {expected_message:?}")
        }
        (Err((category, message)), Ok(())) => {
            panic!("{label}: {spelling:?} was rejected with {category:?}: {message}")
        }
    }
}

async fn sweep(
    label: &str,
    spellings: &[String],
    expected: Result<(), &str>,
    data_type: &LogicalType,
) {
    for spelling in spellings {
        let outcome = quoted_cell(spelling, data_type).await;
        expect_surface(&outcome, expected, label, spelling);
    }
}

fn s(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[tokio::test]
async fn int64_accept_set_is_pinned() {
    let mut decoder_face = s(&[
        "9223372036854775808",
        "-9223372036854775809",
        "1 ",
        " 1 ",
        "1\t",
        "1\r",
        "abc",
        "1.5",
        "1e3",
        "1_000",
        "١٢٣",
        "＋1",
    ]);
    decoder_face.push("9".repeat(39));
    sweep(
        "int64/decoder",
        &decoder_face,
        Err(DECODER_MSG),
        &LogicalType::Int64,
    )
    .await;
    sweep(
        "int64/validator",
        &s(&[" 1"]),
        Err(VALUE_AT_ROW_1),
        &LogicalType::Int64,
    )
    .await;
    sweep(
        "int64/accepted",
        &s(&[
            "0",
            "7",
            "-7",
            "+7",
            "007",
            "9223372036854775807",
            "-9223372036854775808",
        ]),
        Ok(()),
        &LogicalType::Int64,
    )
    .await;
}

#[tokio::test]
async fn float64_accept_set_is_pinned() {
    let mut validator_face = s(&[
        "1e309",
        "-1e309",
        "inf",
        "+inf",
        "-inf",
        "Inf",
        "INFINITY",
        "iNf",
        "infinity",
        "-infinity",
        "NaN",
        "nan",
        "+NaN",
        "NAN",
        " 1.5",
    ]);
    validator_face.push("1".repeat(400));
    validator_face.push("9".repeat(309));
    sweep(
        "float64/validator",
        &validator_face,
        Err(VALUE_AT_ROW_1),
        &LogicalType::Float64,
    )
    .await;
    let decoder_face = s(&["1.5 ", "1_000.5", "1.2.3", "0x1p3", "abc", ".", "1e", "e5"]);
    sweep(
        "float64/decoder",
        &decoder_face,
        Err(DECODER_MSG),
        &LogicalType::Float64,
    )
    .await;
    let mut accepted = s(&["0", "1.5", "-1.5", ".5", "5.", "1e3", "1E3", "1e-3", "+.5"]);
    accepted.push("9".repeat(308));
    sweep("float64/accepted", &accepted, Ok(()), &LogicalType::Float64).await;
}

#[tokio::test]
async fn float32_accept_set_is_pinned() {
    let mut accepted = s(&["1.5", "1e38", "1e-46", "0.1"]);
    accepted.push("1".repeat(38));
    accepted.push("1".repeat(39));
    sweep("float32/accepted", &accepted, Ok(()), &LogicalType::Float32).await;
    sweep(
        "float32/validator",
        &s(&["1e39", "3.5e38", "inf"]),
        Err(VALUE_AT_ROW_1),
        &LogicalType::Float32,
    )
    .await;
}

#[tokio::test]
async fn boolean_accept_set_is_pinned() {
    sweep(
        "bool/validator",
        &s(&["TRUE", "True", "FALSE"]),
        Err(VALUE_AT_ROW_1),
        &LogicalType::Boolean,
    )
    .await;
    sweep(
        "bool/decoder",
        &s(&["1", "0", "yes", " true"]),
        Err(DECODER_MSG),
        &LogicalType::Boolean,
    )
    .await;
    sweep(
        "bool/accepted",
        &s(&["true", "false"]),
        Ok(()),
        &LogicalType::Boolean,
    )
    .await;
}

#[tokio::test]
async fn date32_accept_set_is_pinned() {
    sweep(
        "date32/validator",
        &s(&["2024/01/03"]),
        Err(VALUE_AT_ROW_1),
        &LogicalType::Date32,
    )
    .await;
    sweep(
        "date32/decoder",
        &s(&[
            "2024-01-3 ",
            "20240103",
            "2023-02-29",
            "2024-13-01",
            "2024-01",
        ]),
        Err(DECODER_MSG),
        &LogicalType::Date32,
    )
    .await;
    sweep(
        "date32/accepted",
        &s(&[
            "2024-01-03",
            "2024-1-3",
            " 2024-01-03",
            "2024-02-29",
            "24-01-03",
        ]),
        Ok(()),
        &LogicalType::Date32,
    )
    .await;
}

#[tokio::test]
async fn naive_timestamp_accept_set_is_pinned() {
    let data_type = &LogicalType::Timestamp {
        unit: TimeUnit::Millisecond,
        timezone: None,
    };
    sweep(
        "ts-naive/validator",
        &s(&["2024-01-03T10:00:00Z", "2024-01-03T10:00"]),
        Err(VALUE_AT_ROW_1),
        data_type,
    )
    .await;
    sweep(
        "ts-naive/decoder",
        &s(&["2024-01-03T10:00:00 "]),
        Err(DECODER_MSG),
        data_type,
    )
    .await;
    sweep(
        "ts-naive/accepted",
        &s(&[
            "2024-01-03T10:00:00",
            "2024-01-03 10:00:00",
            "2024-01-03T10:00:00.123",
            "2024-1-3T10:00:00",
        ]),
        Ok(()),
        data_type,
    )
    .await;
}

#[tokio::test]
async fn zoned_timestamp_fails_closed_at_the_bridge() {
    let data_type = LogicalType::Timestamp {
        unit: TimeUnit::Millisecond,
        timezone: Some("UTC".into()),
    };
    for spelling in ["2024-01-03T10:00:00+00:00", "2024-01-03T10:00:00Z"] {
        let outcome = quoted_cell(spelling, &data_type).await;
        expect_surface(&outcome, Err(BRIDGE_MSG), "ts-tz", spelling);
    }
}

/// Invalid UTF-8 inside the bounded inference prefix fails inspection itself;
/// it can never reach the lockstep validator.
#[tokio::test]
async fn invalid_utf8_inside_prefix_fails_inspection() {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.csv"), b"c\n\"\xFF\"\n").expect("write fixture");
    let connection = connection(temp.path());
    let assets = registry()
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
        .expect("discovered")
        .clone();
    let error = registry()
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset,
            },
        )
        .await
        .expect_err("inspect must reject invalid UTF-8");
    assert_eq!(error.category(), ErrorCategory::InvalidData);
    assert_eq!(error.to_string(), INSPECT_UTF8_MSG);
}

/// Invalid UTF-8 beyond the bounded inference prefix keeps the decoder as the
/// terminating stage (normalized message) — the lockstep validator never
/// observes a non-UTF-8 record, which is what makes its lazy per-cell
/// `str::from_utf8` conversion behavior-preserving.
#[tokio::test]
async fn invalid_utf8_beyond_prefix_fails_in_the_decoder() {
    let mut body = Vec::new();
    body.extend_from_slice(b"c\n");
    for index in 0..1_500_000_u64 {
        body.extend_from_slice(format!("{index}\n").as_bytes());
    }
    body.extend_from_slice(b"\"\xFF\"\n");
    let outcome = probe("case.csv", &body, &LogicalType::Int64, false).await;
    expect_surface(
        &outcome,
        Err(DECODER_MSG),
        "utf8-invalid-beyond-prefix",
        "\\xFF past the 8 MiB inspect prefix",
    );
}

/// Empty fields keep the two-column pinned form (a single-column all-empty row
/// fails in the decoder instead — see the O1-C1 suite for that quirk).
#[tokio::test]
async fn empty_field_nullability_surface_is_pinned() {
    let temp = TempDir::new().expect("temporary fixture root");
    std::fs::write(temp.path().join("case.csv"), "n,m\n1,\n2,3\n").expect("write fixture");
    let connection = connection(temp.path());
    let assets = registry()
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
        .expect("discovered")
        .clone();
    let metadata = registry()
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");

    let mut request = ReadRequest::new(asset, 4096);
    request.schema_override = Some(single_type_schema(
        &metadata.schema,
        &LogicalType::Int64,
        false,
    ));
    let mut stream: Pin<
        Box<dyn futures::Stream<Item = Result<BatchEnvelope, ConnectorError>> + Send>,
    > = registry()
        .read_batches(&connection, request)
        .await
        .expect("open read stream");
    let mut outcome = Outcome::Ok(());
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => {}
            Err(error) => {
                outcome = Outcome::Err((error.category(), error.to_string()));
                break;
            }
        }
    }
    expect_surface(
        &outcome,
        Err(VALUE_AT_ROW_1),
        "int64",
        "required empty field",
    );
}
