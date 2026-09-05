//! O0-B1 — measurement-only post-H3 performance baseline (Issue #282).
//!
//! Feature-gated (`io-metrics`) and ignored so default CI never runs it.
//! Measurement only: no optimization, no threshold claim, no production
//! behavior change. One case runs per process (`O0_B1_CASE`) so that
//! `/usr/bin/time -v` max RSS and CPU cover exactly that case.
//!
//! Fixture generators for the `anchor-*` fixtures reuse the accepted
//! E24-B2BASE generator in `tests/read_baseline.rs` verbatim so the
//! cross-baseline comparison keeps the same fixture identity. All fixtures
//! are deterministic; the harness records their SHA-256 with every record.
//!
//! Reuses the `io-metrics` counter side channel (`E24_IO_METRICS_OUT`, the
//! historical variable name). Counters are cumulative for the process; the
//! harness computes per-run deltas itself.

#![cfg(feature = "io-metrics")]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    BatchEnvelope, CredentialRef, DiscoverRequest, Expr, InspectRequest, LogicalField,
    LogicalSchema, LogicalType, ReadRequest, RequestContext, ScalarValue, SourceAsset,
    SourceConnection, TimeUnit,
};
use stillflow_engine::{ExecutionEngine, ExecutionIdentities, ExecutionRequest};
use stillflow_plan::{CastFailurePolicy, LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule};
use stillflow_storage::{SnapshotStore, StorageLimits};
use tempfile::TempDir;

const CASE_ENV: &str = "O0_B1_CASE";
const MODE_ENV: &str = "O0_B1_MODE";
const FIXTURE_ROOT_ENV: &str = "O0_B1_FIXTURE_ROOT";
const HEAD_ENV: &str = "O0_B1_HEAD";
const METRICS_OUT_ENV: &str = "E24_IO_METRICS_OUT";
const BATCH_SIZE: usize = 4_096;
const PARQUET_CHUNK_ROWS: usize = 8_192;
const DEFAULT_HEAD: &str = "f61e0853b67ff5ca7bedb0bddb707befb922baff";
const COUNTER_LABELS: &[&str] = &[
    "validator_read_bytes",
    "decoder_os_bytes",
    "json_handle_bytes",
    "json_framed_bytes",
    "json_reencode_bytes",
    "inference_phase_bytes",
    "csv_decoder_invocations",
    "csv_rows_validated",
    "json_framed_rows",
    "json_polars_decode_invocations",
    "parquet_reader_constructions",
    "parquet_batch_finishes",
];

// ---------------------------------------------------------------------------
// Case table
// ---------------------------------------------------------------------------

struct CaseSpec {
    id: &'static str,
    family: &'static str,
    fixture: &'static str,
    rows: usize,
    cols: usize,
    reps: usize,
}

const CASES: &[CaseSpec] = &[
    // Cross-baseline anchor cases: fixture identity and rep count match the
    // accepted E24-B2BASE ingestion baseline (Issue #99, head 3493f224).
    CaseSpec {
        id: "ingest-csv-anchor-10c-100k",
        family: "ingest-micro",
        fixture: "anchor-csv-10c-100k",
        rows: 100_000,
        cols: 10,
        reps: 30,
    },
    CaseSpec {
        id: "ingest-csv-anchor-100c-100k",
        family: "ingest-micro",
        fixture: "anchor-csv-100c-100k",
        rows: 100_000,
        cols: 100,
        reps: 30,
    },
    CaseSpec {
        id: "ingest-csv-anchor-10c-1m",
        family: "ingest-micro",
        fixture: "anchor-csv-10c-1m",
        rows: 1_000_000,
        cols: 10,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-ndjson-anchor-10c-100k",
        family: "ingest-micro",
        fixture: "anchor-ndjson-10c-100k",
        rows: 100_000,
        cols: 10,
        reps: 30,
    },
    CaseSpec {
        id: "ingest-ndjson-anchor-100c-100k",
        family: "ingest-micro",
        fixture: "anchor-ndjson-100c-100k",
        rows: 100_000,
        cols: 100,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-json-array-anchor-10c-100k",
        family: "ingest-micro",
        fixture: "anchor-array-10c-100k",
        rows: 100_000,
        cols: 10,
        reps: 30,
    },
    CaseSpec {
        id: "ingest-parquet-anchor-10c-100k",
        family: "ingest-micro",
        fixture: "anchor-parquet-10c-100k",
        rows: 100_000,
        cols: 10,
        reps: 30,
    },
    CaseSpec {
        id: "ingest-parquet-anchor-100c-100k",
        family: "ingest-micro",
        fixture: "anchor-parquet-100c-100k",
        rows: 100_000,
        cols: 100,
        reps: 7,
    },
    // Required O0-B1 fixture coverage.
    CaseSpec {
        id: "ingest-csv-narrow-fixed-8c-100k",
        family: "ingest-micro",
        fixture: "narrow-fixed-8c-100k",
        rows: 100_000,
        cols: 8,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-csv-wide-mixed-128c-100k",
        family: "ingest-micro",
        fixture: "wide-mixed-128c-100k",
        rows: 100_000,
        cols: 128,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-csv-longutf8-8c-100k",
        family: "ingest-micro",
        fixture: "longutf8-8c-100k",
        rows: 100_000,
        cols: 8,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-ndjson-timestamps-10c-100k",
        family: "ingest-micro",
        fixture: "timestamps-10c-100k",
        rows: 100_000,
        cols: 6,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-csv-malformed-10c-60k",
        family: "ingest-micro",
        fixture: "malformed-csv-10c-60k",
        rows: 60_000,
        cols: 10,
        reps: 7,
    },
    CaseSpec {
        id: "ingest-ndjson-malformed-10c-30k",
        family: "ingest-micro",
        fixture: "malformed-ndjson-10c-30k",
        rows: 30_000,
        cols: 10,
        reps: 7,
    },
    // Engine E2E: preflight + connector read + rules + snapshot write.
    CaseSpec {
        id: "engine-narrow-simple-8c-100k",
        family: "engine-e2e",
        fixture: "narrow-fixed-8c-100k",
        rows: 100_000,
        cols: 8,
        reps: 7,
    },
    CaseSpec {
        id: "engine-wide-mixed-128c-100k",
        family: "engine-e2e",
        fixture: "wide-mixed-128c-100k",
        rows: 100_000,
        cols: 128,
        reps: 5,
    },
    CaseSpec {
        id: "engine-rule-heavy-8c-100k",
        family: "engine-e2e",
        fixture: "narrow-fixed-8c-100k",
        rows: 100_000,
        cols: 8,
        reps: 7,
    },
    CaseSpec {
        id: "engine-expression-heavy-8c-100k",
        family: "engine-e2e",
        fixture: "narrow-fixed-8c-100k",
        rows: 100_000,
        cols: 8,
        reps: 7,
    },
    CaseSpec {
        id: "engine-parquet-100c-100k",
        family: "engine-e2e",
        fixture: "anchor-parquet-100c-100k",
        rows: 100_000,
        cols: 100,
        reps: 5,
    },
    CaseSpec {
        id: "engine-ndjson-timestamps-override-10c-100k",
        family: "engine-e2e",
        fixture: "timestamps-10c-100k",
        rows: 100_000,
        cols: 6,
        reps: 7,
    },
    CaseSpec {
        id: "engine-narrow-write-8c-1m",
        family: "engine-e2e",
        fixture: "narrow-fixed-8c-1m",
        rows: 1_000_000,
        cols: 8,
        reps: 5,
    },
];

// ---------------------------------------------------------------------------
// Fixture generation (deterministic; sha256 recorded per run)
// ---------------------------------------------------------------------------

struct FixtureFile {
    name: &'static str,
    bytes: u64,
    sha256: String,
}

/// Verbatim from tests/read_baseline.rs (E24-B2BASE) for fixture identity.
fn cell_payload(row: usize, col: usize) -> String {
    let len = 32 + (row.wrapping_mul(31).wrapping_add(col.wrapping_mul(7))) % 65;
    let prefix = format!("v{row:08}_{col:02}_");
    let mut payload = prefix;
    while payload.len() < len {
        payload.push((b'a' + ((row + col + payload.len()) % 26) as u8) as char);
    }
    payload
}

/// Verbatim from tests/read_baseline.rs (E24-B2BASE) for fixture identity.
fn field_names(cols: usize) -> Vec<String> {
    (0..cols).map(|col| format!("c{col}")).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_file(path: &Path) -> String {
    sha256_hex(&fs::read(path).expect("read fixture for digest"))
}

fn write_buf(path: &Path, emit: impl Fn(&mut dyn Write)) -> u64 {
    let file = File::create(path).expect("create fixture");
    let mut out = BufWriter::with_capacity(1 << 20, file);
    emit(&mut out);
    out.flush().expect("flush fixture");
    out.get_ref().metadata().expect("fixture metadata").len()
}

/// Verbatim from tests/read_baseline.rs (E24-B2BASE) for fixture identity.
fn write_anchor(path: &Path, format: &str, cols: usize, rows: usize) -> u64 {
    let mut out = BufWriter::with_capacity(1 << 20, File::create(path).expect("fixture"));
    let names = field_names(cols);
    let mut bytes = 0_u64;
    if format == "csv" {
        let header = names.join(",");
        out.write_all(header.as_bytes()).unwrap();
        out.write_all(b"\n").unwrap();
        bytes += header.len() as u64 + 1;
    } else if format == "array" {
        out.write_all(b"[").unwrap();
        bytes += 1;
    }
    for row in 0..rows {
        if format == "array" && row > 0 {
            out.write_all(b",").unwrap();
            bytes += 1;
        }
        if format != "csv" {
            out.write_all(b"{").unwrap();
            bytes += 1;
        }
        for (col, name) in names.iter().enumerate() {
            if col > 0 {
                out.write_all(b",").unwrap();
                bytes += 1;
            }
            let payload = cell_payload(row, col);
            if format == "csv" {
                out.write_all(payload.as_bytes()).unwrap();
                bytes += payload.len() as u64;
            } else {
                let cell = format!("\"{name}\":\"{payload}\"");
                out.write_all(cell.as_bytes()).unwrap();
                bytes += cell.len() as u64;
            }
        }
        if format == "csv" {
            out.write_all(b"\n").unwrap();
            bytes += 1;
        } else {
            out.write_all(b"}").unwrap();
            bytes += 1;
            if format == "ndjson" {
                out.write_all(b"\n").unwrap();
                bytes += 1;
            }
        }
    }
    if format == "array" {
        out.write_all(b"]\n").unwrap();
        bytes += 2;
    }
    out.flush().unwrap();
    drop(out);
    let actual = fs::metadata(path).unwrap().len();
    assert_eq!(actual, bytes, "anchor fixture byte accounting");
    actual
}

/// Verbatim from tests/read_baseline.rs (E24-B2BASE) for fixture identity.
fn write_anchor_parquet(path: &Path, cols: usize, rows: usize) -> u64 {
    let names = field_names(cols);
    let fields: Vec<Field> = names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = File::create(path).expect("parquet fixture");
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
    let mut start = 0_usize;
    while start < rows {
        let end = (start + PARQUET_CHUNK_ROWS).min(rows);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(cols);
        for col in 0..cols {
            let mut values = Vec::with_capacity(end - start);
            for row in start..end {
                values.push(cell_payload(row, col));
            }
            columns.push(Arc::new(StringArray::from(values)));
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("parquet batch");
        writer.write(&batch).expect("parquet write");
        start = end;
    }
    writer.close().expect("parquet close");
    fs::metadata(path).expect("parquet metadata").len()
}

fn fixed_string(row: usize, col: usize, width: usize) -> String {
    let mut value = format!("{row:08}{col:02}");
    while value.len() < width {
        let byte = b'A' + ((row + col + value.len()) % 26) as u8;
        value.push(byte as char);
    }
    value.truncate(width);
    value
}

fn write_narrow_fixed(path: &Path, rows: usize) -> u64 {
    write_buf(path, |out| {
        out.write_all(b"c0,c1,c2,c3,c4,c5,c6,c7\n").unwrap();
        for row in 0..rows {
            writeln!(
                out,
                "{:010},{:010},{:014.4},{},{},{},{},{}",
                row,
                row.wrapping_mul(7) % 100_000,
                (row % 10_000) as f64 / 8.0,
                fixed_string(row, 3, 12),
                fixed_string(row, 4, 12),
                fixed_string(row, 5, 12),
                fixed_string(row, 6, 12),
                fixed_string(row, 7, 12),
            )
            .unwrap();
        }
    })
}

fn write_wide_mixed(path: &Path, rows: usize) -> u64 {
    write_buf(path, |out| {
        let names: Vec<String> = (0..128).map(|col| format!("m{col:03}")).collect();
        out.write_all(names.join(",").as_bytes()).unwrap();
        out.write_all(b"\n").unwrap();
        for row in 0..rows {
            for (col, _) in names.iter().enumerate() {
                if col > 0 {
                    out.write_all(b",").unwrap();
                }
                match col % 4 {
                    0 => write!(out, "{}", row.wrapping_mul(13) % 1_000_000).unwrap(),
                    1 => write!(out, "{:.3}", (row % 97_000) as f64 / 7.0).unwrap(),
                    2 => write!(out, "{}", fixed_string(row, col, 12)).unwrap(),
                    _ => write!(out, "{}", row.wrapping_mul(3) % 10_000).unwrap(),
                }
            }
            out.write_all(b"\n").unwrap();
        }
    })
}

fn write_long_utf8(path: &Path, rows: usize) -> u64 {
    write_buf(path, |out| {
        out.write_all(b"c0,c1,c2,payload,c4,c5,c6,c7\n").unwrap();
        for row in 0..rows {
            let len = 32 + (row * 37) % 2_017;
            let mut payload = format!("u{row:08}_");
            let mut seed = row.wrapping_mul(9_731);
            while payload.len() < len {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let byte = b'a' + ((seed >> 33) % 26) as u8;
                payload.push(byte as char);
            }
            payload.truncate(len);
            writeln!(
                out,
                "{:010},{:08},\"{}\",\"{}\",{},{},{},{}",
                row,
                row % 100_000_000,
                fixed_string(row, 2, 8),
                payload,
                fixed_string(row, 4, 10),
                fixed_string(row, 5, 10),
                row % 2,
                row % 7,
            )
            .unwrap();
        }
    })
}

fn timestamp_row(row: usize) -> String {
    let seconds = 1_768_476_600_u64 + (row % 86_400) as u64;
    let millis = row % 1_000;
    let (hour, minute, second) = (seconds / 3_600 % 24, seconds / 60 % 60, seconds % 60);
    format!("2026-01-15T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn write_timestamps(path: &Path, rows: usize) -> u64 {
    write_buf(path, |out| {
        for row in 0..rows {
            let seconds = 1_768_476_600_u64 + (row % 86_400) as u64;
            let (hour, minute, second) = (seconds / 3_600 % 24, seconds / 60 % 60, seconds % 60);
            writeln!(
                out,
                "{{\"id\":{row},\"event_utc\":\"{}\",\"event_local\":\"2026-01-15T{hour:02}:{minute:02}:{second:02}.{milli:03}+09:00\",\"event_date\":\"2026-01-15\",\"tz\":\"Asia/Tokyo\",\"value\":{}}}",
                timestamp_row(row),
                row.wrapping_mul(11) % 500_000,
                hour = hour,
                minute = minute,
                second = second,
                milli = row % 1_000,
            )
            .unwrap();
        }
    })
}

fn malformed_row(row: usize) -> String {
    format!(
        "{:010},{:010},{:014.4},{},{},{},{},{}",
        row,
        row % 100_000,
        (row % 10_000) as f64 / 8.0,
        fixed_string(row, 3, 12),
        fixed_string(row, 4, 12),
        fixed_string(row, 5, 12),
        fixed_string(row, 6, 12),
        fixed_string(row, 7, 12),
    )
}

fn write_malformed_csv(path: &Path, rows: usize) -> u64 {
    let bad_row = rows * 2 / 3;
    write_buf(path, |out| {
        out.write_all(b"c0,c1,c2,c3,c4,c5,c6,c7,c8,c9\n").unwrap();
        for row in 0..rows {
            if row == bad_row {
                // Typed-drift failure mid-stream: three fields instead of ten.
                // No quote characters, so inference (bounded prefix) stays
                // clean and the failure surfaces in the lockstep validator.
                out.write_all(b"404,short,row\n").unwrap();
            } else {
                let line = malformed_row(row);
                out.write_all(line.as_bytes()).unwrap();
                out.write_all(b"\n").unwrap();
            }
        }
    })
}

fn write_malformed_ndjson(path: &Path, rows: usize) -> u64 {
    let bad_row = rows / 2;
    write_buf(path, |out| {
        for row in 0..rows {
            if row == bad_row {
                out.write_all(b"{\"c0\":broken,\"c1\":\"x\"}\n").unwrap();
            } else {
                write!(
                    out,
                    "{{\"c0\":{},\"c1\":\"{}\",\"c2\":\"{}\",\"c3\":\"{}\",\"c4\":\"{}\",\"c5\":\"{}\",\"c6\":\"{}\",\"c7\":\"{}\",\"c8\":{},\"c9\":{}}}\n",
                    row,
                    fixed_string(row, 1, 10),
                    fixed_string(row, 2, 10),
                    fixed_string(row, 3, 10),
                    fixed_string(row, 4, 10),
                    fixed_string(row, 5, 10),
                    fixed_string(row, 6, 10),
                    fixed_string(row, 7, 10),
                    row % 2,
                    row % 3,
                )
                .unwrap();
            }
        }
    })
}

fn generate_fixture(root: &Path, spec: &CaseSpec) -> FixtureFile {
    let dir = root.join(spec.fixture);
    fs::create_dir_all(&dir).expect("fixture dir");
    let (name, bytes): (&str, u64) = match spec.fixture {
        "anchor-csv-10c-100k" => (
            "f.csv",
            write_anchor(&dir.join("f.csv"), "csv", 10, 100_000),
        ),
        "anchor-csv-100c-100k" => (
            "f.csv",
            write_anchor(&dir.join("f.csv"), "csv", 100, 100_000),
        ),
        "anchor-csv-10c-1m" => (
            "f.csv",
            write_anchor(&dir.join("f.csv"), "csv", 10, 1_000_000),
        ),
        "anchor-ndjson-10c-100k" => (
            "f.ndjson",
            write_anchor(&dir.join("f.ndjson"), "ndjson", 10, 100_000),
        ),
        "anchor-ndjson-100c-100k" => (
            "f.ndjson",
            write_anchor(&dir.join("f.ndjson"), "ndjson", 100, 100_000),
        ),
        "anchor-array-10c-100k" => (
            "f.json",
            write_anchor(&dir.join("f.json"), "array", 10, 100_000),
        ),
        "anchor-parquet-10c-100k" => (
            "f.parquet",
            write_anchor_parquet(&dir.join("f.parquet"), 10, 100_000),
        ),
        "anchor-parquet-100c-100k" => (
            "f.parquet",
            write_anchor_parquet(&dir.join("f.parquet"), 100, 100_000),
        ),
        "narrow-fixed-8c-100k" => ("f.csv", write_narrow_fixed(&dir.join("f.csv"), 100_000)),
        "narrow-fixed-8c-1m" => ("f.csv", write_narrow_fixed(&dir.join("f.csv"), 1_000_000)),
        "wide-mixed-128c-100k" => ("f.csv", write_wide_mixed(&dir.join("f.csv"), 100_000)),
        "longutf8-8c-100k" => ("f.csv", write_long_utf8(&dir.join("f.csv"), 100_000)),
        "timestamps-10c-100k" => ("f.ndjson", write_timestamps(&dir.join("f.ndjson"), 100_000)),
        "malformed-csv-10c-60k" => ("f.csv", write_malformed_csv(&dir.join("f.csv"), 60_000)),
        "malformed-ndjson-10c-30k" => (
            "f.ndjson",
            write_malformed_ndjson(&dir.join("f.ndjson"), 30_000),
        ),
        other => panic!("unknown fixture {other}"),
    };
    let sha256 = sha256_file(&dir.join(name));
    FixtureFile {
        name,
        bytes,
        sha256,
    }
}

// ---------------------------------------------------------------------------
// Measurement helpers (adapted from tests/read_baseline.rs)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn cpu_ticks_total() -> Option<u64> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(not(target_os = "linux"))]
fn cpu_ticks_total() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn peak_resident_kib() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|value| value.parse().ok())
            })
        })
}

#[cfg(not(target_os = "linux"))]
fn peak_resident_kib() -> Option<u64> {
    None
}

fn percentile(mut values: Vec<u128>, quantile: f64) -> u128 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() as f64 - 1.0) * quantile).round() as usize;
    values[index.min(values.len() - 1)]
}

fn read_counter_snapshot(path: &Path) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    let Ok(text) = fs::read_to_string(path) else {
        // Preflight-level failures never construct a reader, so no dump side
        // channel file exists; treat as zero counters for that run.
        for label in COUNTER_LABELS {
            map.insert((*label).to_string(), 0);
        }
        return map;
    };
    for line in text.lines() {
        if let Some((label, value)) = line.split_once('=') {
            if let Ok(value) = value.parse::<u64>() {
                map.insert(label.to_string(), value);
            }
        }
    }
    for label in COUNTER_LABELS {
        map.entry((*label).to_string()).or_insert(0);
    }
    map
}

fn counter_delta(
    current: &BTreeMap<String, u64>,
    previous: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    current
        .iter()
        .map(|(label, value)| {
            (
                label.clone(),
                value.saturating_sub(previous.get(label).copied().unwrap_or(0)),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Witness: deterministic digest over emitted batches
// ---------------------------------------------------------------------------

fn digest_batch(batch: &RecordBatch, hasher: &mut Sha256) {
    hasher.update(batch.num_rows().to_le_bytes());
    for (index, column) in batch.columns().iter().enumerate() {
        let data = column.to_data();
        hasher.update(index.to_le_bytes());
        hasher.update(format!("{:?}", data.data_type()).as_bytes());
        hasher.update(data.len().to_le_bytes());
        for buffer in data.buffers() {
            hasher.update(buffer.as_slice());
        }
        if let Some(nulls) = data.nulls() {
            hasher.update(nulls.null_count().to_le_bytes());
        }
    }
}

struct Witness {
    rows: u64,
    digest: String,
    schema: String,
}

fn witness_envelopes(envelopes: &[BatchEnvelope]) -> Witness {
    let mut hasher = Sha256::new();
    let mut rows = 0_u64;
    let mut schema = String::new();
    for (sequence, envelope) in envelopes.iter().enumerate() {
        hasher.update(sequence.to_le_bytes());
        digest_batch(envelope.payload(), &mut hasher);
        rows += envelope.payload().num_rows() as u64;
        if schema.is_empty() {
            for field in envelope.schema().fields.iter() {
                schema.push_str(&format!(
                    "{}:{:?}:{},",
                    field.name, field.data_type, field.nullable
                ));
            }
        }
    }
    Witness {
        rows,
        digest: sha256_hex(hasher.finalize().as_slice()),
        schema,
    }
}

// ---------------------------------------------------------------------------
// Case runners
// ---------------------------------------------------------------------------

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "o0-b1 fixture root",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/o0-b1").expect("credential reference"),
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

fn asset_named(assets: &[SourceAsset], name: &str) -> SourceAsset {
    assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("asset {name} not found"))
        .clone()
}

struct RunSample {
    wall_ms: u128,
    cpu_ticks: Option<u64>,
    rows: u64,
    error: Option<String>,
}

async fn drain_ingest(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) -> Result<(u64, usize), stillflow_core::ConnectorError> {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await?;
    let mut rows = 0_u64;
    let mut batches = 0_usize;
    while let Some(item) = stream.next().await {
        let envelope = item?;
        rows += envelope.payload().num_rows() as u64;
        batches += 1;
    }
    Ok((rows, batches))
}

fn error_witness(error: &stillflow_core::ConnectorError) -> String {
    format!(
        "category={:?} retryable={} message={}",
        error.category(),
        error.retryable(),
        error.user_message()
    )
}

async fn run_ingest_case(spec: &CaseSpec, root: &Path, head: &str) -> serde_json::Value {
    let fixture = generate_fixture(root, spec);
    let dir = root.join(spec.fixture);
    let connection = connection(&dir);
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
    let asset = asset_named(&assets, fixture.name);
    let metrics_out = root.join(spec.fixture).join("counters.out");

    // Warm-up: one untimed run (Polars pool init, page cache for the fixture).
    let warm = drain_ingest(&connection, &registry, &asset).await;
    let warm_error = warm.as_ref().err().map(error_witness);
    if warm.is_err() {
        eprintln!(
            "[o0-b1] case {} fails as expected: {:?}",
            spec.id, warm_error
        );
    }

    let mut samples: Vec<RunSample> = Vec::with_capacity(spec.reps);
    let mut previous_counters = read_counter_snapshot(&metrics_out);
    let mut last_counters: BTreeMap<String, u64> = BTreeMap::new();
    for _ in 0..spec.reps {
        let _ = fs::remove_file(&metrics_out);
        let cpu_before = cpu_ticks_total();
        let start = Instant::now();
        let outcome = drain_ingest(&connection, &registry, &asset).await;
        let wall_ms = start.elapsed().as_millis();
        let cpu_ticks = match (cpu_before, cpu_ticks_total()) {
            (Some(before), Some(after)) => Some(after.saturating_sub(before)),
            _ => None,
        };
        let (rows, error) = match outcome {
            Ok((rows, _batches)) => (rows, None),
            Err(error) => (0, Some(error_witness(&error))),
        };
        samples.push(RunSample {
            wall_ms,
            cpu_ticks,
            rows,
            error,
        });
        let current = read_counter_snapshot(&metrics_out);
        last_counters = counter_delta(&current, &previous_counters);
        previous_counters = current;
    }

    // Witness run (untimed): full digest of emitted batches, or error witness.
    let witness = match drain_ingest(&connection, &registry, &asset).await {
        Ok((_rows, _batches)) => {
            let mut stream = registry
                .read_batches(&connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
                .await
                .expect("witness stream");
            let mut envelopes = Vec::new();
            while let Some(item) = stream.next().await {
                envelopes.push(item.expect("witness batch"));
            }
            let witness = witness_envelopes(&envelopes);
            serde_json::json!({
                "kind": "ingest_batches",
                "rows": witness.rows,
                "digest": witness.digest,
                "schema": witness.schema,
            })
        }
        Err(error) => serde_json::json!({
            "kind": "error",
            "rows": 0,
            "error": error_witness(&error),
        }),
    };

    let walls: Vec<u128> = samples.iter().map(|sample| sample.wall_ms).collect();
    let p50 = percentile(walls.clone(), 0.5);
    let p95 = percentile(walls.clone(), 0.95);
    let min = walls.iter().copied().min().unwrap_or(0);
    let max = walls.iter().copied().max().unwrap_or(0);
    let mut cpus: Vec<u128> = samples
        .iter()
        .map(|sample| {
            sample
                .cpu_ticks
                .map(|ticks| ticks as u128 * 10)
                .unwrap_or(0)
        })
        .collect();
    let cpu_reliable = samples.iter().all(|sample| sample.cpu_ticks.is_some());
    let cpu_p50 = percentile(cpus.clone(), 0.5);
    cpus.clear();
    let rows_seen: Vec<u64> = samples.iter().map(|sample| sample.rows).collect();
    let stable_rows = rows_seen.iter().all(|rows| *rows == rows_seen[0]);
    let errors_seen: Vec<Option<String>> =
        samples.iter().map(|sample| sample.error.clone()).collect();
    let stable_error = errors_seen.iter().all(|error| *error == errors_seen[0]);

    serde_json::json!({
        "case": spec.id,
        "family": spec.family,
        "head": head,
        "feature": "io-metrics",
        "fixture": {
            "name": fixture.name,
            "rows_expected": spec.rows,
            "cols": spec.cols,
            "bytes": fixture.bytes,
            "sha256": fixture.sha256,
        },
        "warmup_runs": 1,
        "reps": samples.len(),
        "wall_ms": { "p50": p50, "p95": p95, "min": min, "max": max },
        "cpu_ms": { "p50": if cpu_reliable { serde_json::json!(cpu_p50) } else { serde_json::Value::Null }, "reliable": cpu_reliable },
        "peak_rss_kib_process_lifetime": peak_resident_kib(),
        "peak_rss_attribution": "process-lifetime VmHWM including warm-up; not per-run",
        "rows_stable_across_reps": stable_rows,
        "error_stable_across_reps": stable_error,
        "error_witness": warm_error,
        "witness": witness,
        "counters_last_run": last_counters,
        "counter_semantics": {
            "validator_read_bytes": "exact logical CSV validator-pass bytes (CountingReader)",
            "decoder_os_bytes": "handle/OS-level observation; NOT exact logical read bytes",
            "json_handle_bytes": "exact logical JSON framing-pass bytes (CountingReader)",
        },
    })
}

fn scan_project_all(asset_id: uuid::Uuid, schema: &LogicalSchema) -> PlanNodeKind {
    PlanNodeKind::Scan {
        source_asset_id: asset_id,
        projection: schema.fields.iter().map(|field| field.id).collect(),
        predicate: None,
    }
}

fn plan_scan_materialize(asset_id: uuid::Uuid, schema: &LogicalSchema) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(uuid::Uuid::from_u128(1));
    let materialize = PlanNodeId::from_uuid(uuid::Uuid::from_u128(3));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(scan_project_all(asset_id, schema), Vec::new()),
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
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn plan_scan_rules_materialize(
    asset_id: uuid::Uuid,
    schema: &LogicalSchema,
    rules: Vec<Rule>,
) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(uuid::Uuid::from_u128(1));
    let apply = PlanNodeId::from_uuid(uuid::Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(uuid::Uuid::from_u128(3));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(scan_project_all(asset_id, schema), Vec::new()),
    );
    nodes.insert(
        apply,
        PlanNode::new(PlanNodeKind::ApplyRules { rules }, vec![scan]),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![apply],
        ),
    );
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn plan_scan_filter_rules_materialize(
    asset_id: uuid::Uuid,
    schema: &LogicalSchema,
    predicate: Expr,
    rules: Vec<Rule>,
) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(uuid::Uuid::from_u128(1));
    let filter = PlanNodeId::from_uuid(uuid::Uuid::from_u128(2));
    let apply = PlanNodeId::from_uuid(uuid::Uuid::from_u128(4));
    let materialize = PlanNodeId::from_uuid(uuid::Uuid::from_u128(3));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(scan_project_all(asset_id, schema), Vec::new()),
    );
    nodes.insert(
        filter,
        PlanNode::new(PlanNodeKind::Filter { predicate }, vec![scan]),
    );
    nodes.insert(
        apply,
        PlanNode::new(PlanNodeKind::ApplyRules { rules }, vec![filter]),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "out".to_owned(),
            },
            vec![apply],
        ),
    );
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn column_by_name(schema: &LogicalSchema, name: &str) -> stillflow_core::ColumnId {
    schema
        .fields
        .iter()
        .find(|field| field.name == name)
        .unwrap_or_else(|| panic!("column {name} missing"))
        .id
}

fn rule_heavy_rules(schema: &LogicalSchema) -> Vec<Rule> {
    let mut rules = Vec::new();
    for field in schema.fields.iter() {
        match field.data_type {
            LogicalType::Utf8 => {
                rules.push(Rule::Trim { column: field.id });
                rules.push(Rule::ReplaceLiteral {
                    column: field.id,
                    from: ScalarValue::Utf8("ZZZ".to_owned()),
                    to: ScalarValue::Utf8("YY".to_owned()),
                });
                rules.push(Rule::FillNull {
                    column: field.id,
                    value: ScalarValue::Utf8("filled".to_owned()),
                });
                rules.push(Rule::Cast {
                    column: field.id,
                    data_type: LogicalType::Utf8,
                    on_failure: CastFailurePolicy::Error,
                });
                rules.push(Rule::Rename {
                    column: field.id,
                    to: format!("r_{}", field.name),
                });
            }
            LogicalType::Int64 => {
                rules.push(Rule::FillNull {
                    column: field.id,
                    value: ScalarValue::Int64(0),
                });
                rules.push(Rule::Cast {
                    column: field.id,
                    data_type: LogicalType::Int64,
                    on_failure: CastFailurePolicy::Error,
                });
                rules.push(Rule::Rename {
                    column: field.id,
                    to: format!("r_{}", field.name),
                });
            }
            LogicalType::Float64 => {
                rules.push(Rule::FillNull {
                    column: field.id,
                    value: ScalarValue::Float64(
                        stillflow_core::FiniteF64::new(0.0).expect("finite float"),
                    ),
                });
                rules.push(Rule::Cast {
                    column: field.id,
                    data_type: LogicalType::Float64,
                    on_failure: CastFailurePolicy::Error,
                });
                rules.push(Rule::Rename {
                    column: field.id,
                    to: format!("r_{}", field.name),
                });
            }
            _ => {}
        }
    }
    for index in 0..14_usize {
        let source = if index % 2 == 0 { "c0" } else { "c2" };
        rules.push(Rule::DeriveColumn {
            id: stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(500 + index as u128)),
            name: format!("d{index:02}"),
            data_type: if index % 2 == 0 {
                LogicalType::Int64
            } else {
                LogicalType::Float64
            },
            nullable: false,
            expression: Expr::Column(column_by_name(schema, source)),
        });
    }
    rules
}

/// Deep boolean/comparison chain. Arithmetic operators (Add, Multiply, ...)
/// are paused on this head ("checked arithmetic is paused until overflow
/// semantics are implemented"), so the expression-heavy workload is built
/// from the supported surface: comparisons, And/Or/Not, IsNull, Coalesce.
fn bool_chain(schema: &LogicalSchema, depth: usize, seed: i64) -> Expr {
    let c0 = column_by_name(schema, "c0");
    let c1 = column_by_name(schema, "c1");
    let mut expression = Expr::Binary {
        left: Box::new(Expr::Column(c0)),
        operator: stillflow_core::BinaryOperator::GreaterThanOrEqual,
        right: Box::new(Expr::Literal(ScalarValue::Int64(seed))),
    };
    for step in 0..depth {
        let leaf = match step % 4 {
            0 => Expr::Binary {
                left: Box::new(Expr::Column(c1)),
                operator: stillflow_core::BinaryOperator::LessThan,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1_000_000))),
            },
            1 => Expr::Binary {
                left: Box::new(Expr::Column(column_by_name(schema, "c3"))),
                operator: stillflow_core::BinaryOperator::Equal,
                right: Box::new(Expr::Literal(ScalarValue::Utf8(fixed_string(
                    seed.unsigned_abs() as usize,
                    3,
                    12,
                )))),
            },
            2 => Expr::Binary {
                left: Box::new(Expr::Column(c0)),
                operator: stillflow_core::BinaryOperator::NotEqual,
                right: Box::new(Expr::Literal(ScalarValue::Int64(-1))),
            },
            _ => Expr::IsNull {
                expression: Box::new(Expr::Column(column_by_name(schema, "c4"))),
                negated: true,
            },
        };
        let operator = if step % 2 == 0 {
            stillflow_core::BinaryOperator::And
        } else {
            stillflow_core::BinaryOperator::Or
        };
        expression = Expr::Binary {
            left: Box::new(expression),
            operator,
            right: Box::new(leaf),
        };
    }
    expression
}

fn expression_heavy_rules(schema: &LogicalSchema) -> Vec<Rule> {
    let mut rules = Vec::new();
    for index in 0..16_usize {
        rules.push(Rule::DeriveColumn {
            id: stillflow_core::ColumnId::from_uuid(uuid::Uuid::from_u128(900 + index as u128)),
            name: format!("e{index:02}"),
            data_type: LogicalType::Boolean,
            nullable: false,
            expression: bool_chain(schema, 24, 1 + index as i64),
        });
    }
    rules.push(Rule::FilterRows {
        predicate: Expr::Binary {
            left: Box::new(Expr::Column(column_by_name(schema, "c1"))),
            operator: stillflow_core::BinaryOperator::LessThan,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1_000_000_000))),
        },
    });
    rules
}

fn timestamp_override_schema(schema: &LogicalSchema) -> LogicalSchema {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let data_type = match field.name.as_str() {
                "event_utc" => LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: Some("UTC".to_owned()),
                },
                "event_date" => LogicalType::Date32,
                _ => field.data_type.clone(),
            };
            LogicalField {
                id: field.id,
                name: field.name.clone(),
                data_type,
                nullable: field.nullable,
                metadata: field.metadata.clone(),
            }
        })
        .collect();
    LogicalSchema {
        version: schema.version,
        fields,
        metadata: schema.metadata.clone(),
    }
}

fn identities() -> ExecutionIdentities {
    let now = chrono::Utc::now();
    ExecutionIdentities {
        snapshot_id: uuid::Uuid::from_u128(100),
        dataset_id: uuid::Uuid::from_u128(101),
        session_id: uuid::Uuid::from_u128(102),
        created_at: now,
        started_at: now,
        lineage: Default::default(),
        quality_score: None,
    }
}

async fn run_engine_case(spec: &CaseSpec, root: &Path, head: &str) -> serde_json::Value {
    let fixture = generate_fixture(root, spec);
    let dir = root.join(spec.fixture);
    let connection = connection(&dir);
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
    let asset = asset_named(&assets, fixture.name);
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
    let schema = metadata.schema;
    let engine = ExecutionEngine::new(registry);
    let metrics_out = dir.join("counters.out");

    let (plan, schema_override) = match spec.id {
        "engine-narrow-simple-8c-100k"
        | "engine-wide-mixed-128c-100k"
        | "engine-parquet-100c-100k"
        | "engine-narrow-write-8c-1m" => (plan_scan_materialize(asset.id, &schema), None),
        "engine-rule-heavy-8c-100k" => {
            let rules = rule_heavy_rules(&schema);
            assert_eq!(rules.len(), 48, "rule-heavy plan size");
            (plan_scan_rules_materialize(asset.id, &schema, rules), None)
        }
        "engine-expression-heavy-8c-100k" => {
            let rules = expression_heavy_rules(&schema);
            let predicate = Expr::Binary {
                left: Box::new(Expr::Column(column_by_name(&schema, "c0"))),
                operator: stillflow_core::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
            };
            (
                plan_scan_filter_rules_materialize(asset.id, &schema, predicate, rules),
                None,
            )
        }
        "engine-ndjson-timestamps-override-10c-100k" => {
            let override_schema = timestamp_override_schema(&schema);
            (
                plan_scan_materialize(asset.id, &override_schema),
                Some(override_schema),
            )
        }
        other => panic!("unknown engine case {other}"),
    };

    async fn materialize_once<'a>(
        engine: &'a ExecutionEngine,
        connection: &'a SourceConnection,
        asset: &'a SourceAsset,
        plan: &'a LogicalPlan,
        schema_override: Option<&'a LogicalSchema>,
        store: &'a SnapshotStore,
    ) -> Result<(u64, u64), String> {
        let request = ExecutionRequest {
            plan: plan.clone(),
            connection: connection.clone(),
            asset: asset.clone(),
            schema_override: schema_override.cloned(),
            identities: identities(),
            context: RequestContext::default(),
            batch_size: BATCH_SIZE,
            store,
        };
        match engine.materialize(request).await {
            Ok(manifest) => {
                let stats = manifest.snapshot().stats();
                Ok((stats.row_count(), stats.stored_byte_count()))
            }
            Err(error) => Err(format!(
                "category={:?} retryable={} message={error}",
                error.category(),
                error.retryable(),
            )),
        }
    }

    // Warm-up: one untimed run.
    let warm_store_dir = TempDir::new().expect("warm-up store dir");
    let warm_store = SnapshotStore::open(warm_store_dir.path(), StorageLimits::default())
        .expect("warm-up store");
    let warm = materialize_once(
        &engine,
        &connection,
        &asset,
        &plan,
        schema_override.as_ref(),
        &warm_store,
    )
    .await;
    let warm_error = warm.as_ref().err().cloned();
    if warm.is_err() {
        eprintln!(
            "[o0-b1] case {} fails as expected: {:?}",
            spec.id, warm_error
        );
    }
    drop(warm_store);

    let mut samples: Vec<RunSample> = Vec::with_capacity(spec.reps);
    let mut previous_counters = read_counter_snapshot(&metrics_out);
    let mut last_counters: BTreeMap<String, u64> = BTreeMap::new();
    for _ in 0..spec.reps {
        let _ = fs::remove_file(&metrics_out);
        let store_dir = TempDir::new().expect("store dir");
        let store = SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("store");
        let cpu_before = cpu_ticks_total();
        let start = Instant::now();
        let outcome = materialize_once(
            &engine,
            &connection,
            &asset,
            &plan,
            schema_override.as_ref(),
            &store,
        )
        .await;
        let wall_ms = start.elapsed().as_millis();
        let cpu_ticks = match (cpu_before, cpu_ticks_total()) {
            (Some(before), Some(after)) => Some(after.saturating_sub(before)),
            _ => None,
        };
        let (rows, error) = match outcome {
            Ok((rows, _stored)) => (rows, None),
            Err(error) => (0, Some(error)),
        };
        samples.push(RunSample {
            wall_ms,
            cpu_ticks,
            rows,
            error,
        });
        let current = read_counter_snapshot(&metrics_out);
        last_counters = counter_delta(&current, &previous_counters);
        previous_counters = current;
        drop(store);
        drop(store_dir);
    }

    // Witness run (untimed): materialize once more, read the snapshot back and
    // digest every emitted batch.
    let witness = {
        let store_dir = TempDir::new().expect("witness store dir");
        let store =
            SnapshotStore::open(store_dir.path(), StorageLimits::default()).expect("witness store");
        match materialize_once(
            &engine,
            &connection,
            &asset,
            &plan,
            schema_override.as_ref(),
            &store,
        )
        .await
        {
            Ok((rows, stored)) => {
                let mut reader = store
                    .read_batches(identities().snapshot_id)
                    .expect("read back");
                let manifest = reader.manifest().clone();
                let mut envelopes = Vec::new();
                for item in reader.by_ref() {
                    envelopes.push(item.expect("witness batch"));
                }
                let witness = witness_envelopes(&envelopes);
                serde_json::json!({
                    "kind": "snapshot_read_back",
                    "rows": witness.rows,
                    "digest": witness.digest,
                    "schema": witness.schema,
                    "manifest_rows": rows,
                    "stored_byte_count": stored,
                    "partitions": manifest.partitions().len(),
                })
            }
            Err(error) => serde_json::json!({
                "kind": "error",
                "rows": 0,
                "error": error,
            }),
        }
    };

    let walls: Vec<u128> = samples.iter().map(|sample| sample.wall_ms).collect();
    let p50 = percentile(walls.clone(), 0.5);
    let p95 = percentile(walls.clone(), 0.95);
    let min = walls.iter().copied().min().unwrap_or(0);
    let max = walls.iter().copied().max().unwrap_or(0);
    let cpus: Vec<u128> = samples
        .iter()
        .map(|sample| {
            sample
                .cpu_ticks
                .map(|ticks| ticks as u128 * 10)
                .unwrap_or(0)
        })
        .collect();
    let cpu_reliable = samples.iter().all(|sample| sample.cpu_ticks.is_some());
    let cpu_p50 = percentile(cpus, 0.5);
    let rows_seen: Vec<u64> = samples.iter().map(|sample| sample.rows).collect();
    let stable_rows = rows_seen.iter().all(|rows| *rows == rows_seen[0]);
    let errors_seen: Vec<Option<String>> =
        samples.iter().map(|sample| sample.error.clone()).collect();
    let stable_error = errors_seen.iter().all(|error| *error == errors_seen[0]);

    serde_json::json!({
        "case": spec.id,
        "family": spec.family,
        "head": head,
        "feature": "io-metrics",
        "fixture": {
            "name": fixture.name,
            "rows_expected": spec.rows,
            "cols": spec.cols,
            "bytes": fixture.bytes,
            "sha256": fixture.sha256,
        },
        "warmup_runs": 1,
        "reps": samples.len(),
        "wall_ms": { "p50": p50, "p95": p95, "min": min, "max": max },
        "cpu_ms": { "p50": if cpu_reliable { serde_json::json!(cpu_p50) } else { serde_json::Value::Null }, "reliable": cpu_reliable },
        "peak_rss_kib_process_lifetime": peak_resident_kib(),
        "peak_rss_attribution": "process-lifetime VmHWM including warm-up; not per-run",
        "rows_stable_across_reps": stable_rows,
        "error_stable_across_reps": stable_error,
        "error_witness": warm_error,
        "witness": witness,
        "counters_last_run": last_counters,
        "counter_semantics": {
            "validator_read_bytes": "exact logical CSV validator-pass bytes (CountingReader)",
            "decoder_os_bytes": "handle/OS-level observation; NOT exact logical read bytes",
            "json_handle_bytes": "exact logical JSON framing-pass bytes (CountingReader)",
        },
    })
}

#[tokio::test]
#[ignore]
async fn o0_b1_baseline() {
    let case_id = std::env::var(CASE_ENV).expect("O0_B1_CASE selects one case");
    let mode = std::env::var(MODE_ENV).unwrap_or_else(|_| "measure".to_owned());
    let root = PathBuf::from(
        std::env::var(FIXTURE_ROOT_ENV).unwrap_or_else(|_| "/tmp/o0-b1-fixtures".to_owned()),
    );
    let head = std::env::var(HEAD_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_owned());
    let spec = CASES
        .iter()
        .find(|spec| spec.id == case_id)
        .unwrap_or_else(|| panic!("unknown case {case_id}"));

    fs::create_dir_all(&root).expect("fixture root");
    std::env::set_var(
        METRICS_OUT_ENV,
        root.join(spec.fixture).join("counters.out"),
    );

    if mode == "generate" {
        let fixture = generate_fixture(&root, spec);
        println!(
            "{}",
            serde_json::json!({
                "case": spec.id,
                "mode": "generate",
                "fixture": {
                    "name": fixture.name,
                    "rows_expected": spec.rows,
                    "cols": spec.cols,
                    "bytes": fixture.bytes,
                    "sha256": fixture.sha256,
                },
            })
        );
        return;
    }

    let record = if spec.family == "ingest-micro" {
        run_ingest_case(spec, &root, &head).await
    } else {
        run_engine_case(spec, &root, &head).await
    };
    println!("{record}");
}
