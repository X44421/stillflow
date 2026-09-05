//! O0-C1 — measurement-only CSV/TSV decode/validation duplicate-work
//! attribution (Issue #285).
//!
//! Measurement only: no optimization, no production change, no behavior
//! change. The harness compiles with and without the private `io-metrics`
//! feature; with the feature off it still runs every case so the emitted
//! digests and error witnesses can be compared against the instrumented runs
//! (behavioral parity witness). With the feature on, the connector dumps its
//! cumulative counter snapshot through the E24 side channel
//! (`E24_IO_METRICS_OUT`, historical variable name) once per reader drop, and
//! the harness computes per-run deltas itself.
//!
//! One case runs per process (`O0_C1_CASE`) so process-lifetime peak RSS
//! (VmHWM) and the cumulative counters cover exactly that case.
//!
//! Modes:
//! - `full`           — production `read_batches` drain to the end.
//! - `bounded-preview`— production bounded read (`PreviewRequest` row limit),
//!                      so only a prefix of the file is consumed.
//! - `bounded-earlydrop` — consumer-driven prefix consumption: take N
//!                      envelopes then drop the stream.
//! - `validate-probe` — harness-side reference probe: one plain `csv`-crate
//!                      pass over the fixture (same delimiter/quote/header
//!                      settings as the production validator). This isolates
//!                      the marginal cost of one validation pass; it is not
//!                      the production path.
//!
//! Fixture generators reuse the accepted E24-B2BASE anchor generator
//! (`tests/read_baseline.rs`) cell payloads for the anchor shapes and the
//! O0-B1 fixture generators (sibling baseline, PR #293) for the
//! narrow/wide/long-UTF-8/malformed shapes, plus TSV variants of both and a
//! typed schema-drift variant. All fixtures are deterministic; the harness
//! records their SHA-256 with every record.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use sha2::{Digest, Sha256};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, PreviewRequest, ReadRequest, RequestContext, SourceAsset,
    SourceConnection,
};

const CASE_ENV: &str = "O0_C1_CASE";
const MODE_ENV: &str = "O0_C1_MODE";
const FIXTURE_ROOT_ENV: &str = "O0_C1_FIXTURE_ROOT";
const HEAD_ENV: &str = "O0_C1_HEAD";
const METRICS_OUT_ENV: &str = "E24_IO_METRICS_OUT";
const DEFAULT_HEAD: &str = "f61e0853b67ff5ca7bedb0bddb707befb922baff";
const BATCH_SIZE: usize = 4_096;
const PREVIEW_ROW_LIMIT: usize = 10_000;
const PREVIEW_BYTE_LIMIT: usize = 50 * 1024 * 1024;
const EARLY_DROP_BATCHES: usize = 3;
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
    "csv_rows_decoded",
    "csv_fail_decode",
    "csv_fail_validate",
    "ingest_inspect_nanos",
    "ingest_prepare_nanos",
    "ingest_decode_nanos",
    "ingest_validate_nanos",
];

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn alloc_snapshot() -> (u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

// ---------------------------------------------------------------------------
// Case table
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Full,
    Preview,
    EarlyDrop,
    ValidateProbe,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Full => "full",
            Mode::Preview => "bounded-preview",
            Mode::EarlyDrop => "bounded-earlydrop",
            Mode::ValidateProbe => "validate-probe",
        }
    }
}

struct CaseSpec {
    id: &'static str,
    fixture: &'static str,
    mode: Mode,
    rows_expected: usize,
    cols: usize,
    delimiter: u8,
    reps: usize,
}

const CASES: &[CaseSpec] = &[
    // Full-read cases: the lockstep duplicate-work question.
    CaseSpec {
        id: "full-csv-anchor-10c-100k",
        fixture: "anchor-csv-10c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 10,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "full-csv-anchor-10c-1m",
        fixture: "anchor-csv-10c-1m",
        mode: Mode::Full,
        rows_expected: 1_000_000,
        cols: 10,
        delimiter: b',',
        reps: 5,
    },
    CaseSpec {
        id: "full-csv-narrow-fixed-8c-100k",
        fixture: "narrow-fixed-8c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "full-csv-wide-mixed-128c-100k",
        fixture: "wide-mixed-128c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 128,
        delimiter: b',',
        reps: 5,
    },
    CaseSpec {
        id: "full-csv-longutf8-8c-100k",
        fixture: "longutf8-8c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "full-tsv-anchor-10c-100k",
        fixture: "anchor-tsv-10c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 10,
        delimiter: b'\t',
        reps: 7,
    },
    CaseSpec {
        id: "full-tsv-narrow-fixed-8c-100k",
        fixture: "narrow-fixed-tsv-8c-100k",
        mode: Mode::Full,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b'\t',
        reps: 7,
    },
    // Malformed cases: which stage raises, and with which category.
    CaseSpec {
        id: "full-csv-malformed-width-10c-60k",
        fixture: "malformed-width-10c-60k",
        mode: Mode::Full,
        rows_expected: 60_000,
        cols: 10,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "full-csv-malformed-typed-8c-60k",
        fixture: "malformed-typed-8c-60k",
        mode: Mode::Full,
        rows_expected: 60_000,
        cols: 8,
        delimiter: b',',
        reps: 7,
    },
    // Bounded/prefix cases: the consumed range is a strict prefix, so the
    // duplicate work must be attributed to that prefix only.
    CaseSpec {
        id: "bounded-preview-csv-narrow-fixed-8c-limit10k",
        fixture: "narrow-fixed-8c-100k",
        mode: Mode::Preview,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "bounded-preview-csv-longutf8-8c-limit10k",
        fixture: "longutf8-8c-100k",
        mode: Mode::Preview,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "bounded-preview-tsv-narrow-fixed-8c-limit10k",
        fixture: "narrow-fixed-tsv-8c-100k",
        mode: Mode::Preview,
        rows_expected: 100_000,
        cols: 8,
        delimiter: b'\t',
        reps: 7,
    },
    CaseSpec {
        id: "bounded-earlydrop-csv-anchor-10c-3batches",
        fixture: "anchor-csv-10c-100k",
        mode: Mode::EarlyDrop,
        rows_expected: 100_000,
        cols: 10,
        delimiter: b',',
        reps: 7,
    },
    // Harness-side reference probes: one plain csv-crate pass over the whole
    // fixture (the marginal cost of one validation pass alone).
    CaseSpec {
        id: "probe-csv-validate-anchor-10c-100k",
        fixture: "anchor-csv-10c-100k",
        mode: Mode::ValidateProbe,
        rows_expected: 100_000,
        cols: 10,
        delimiter: b',',
        reps: 7,
    },
    CaseSpec {
        id: "probe-csv-validate-wide-mixed-128c-100k",
        fixture: "wide-mixed-128c-100k",
        mode: Mode::ValidateProbe,
        rows_expected: 100_000,
        cols: 128,
        delimiter: b',',
        reps: 5,
    },
];

// ---------------------------------------------------------------------------
// Fixture generation (deterministic; sha256 recorded per record)
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
    let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
    emit(&mut out);
    out.flush().expect("flush fixture");
    out.get_ref().metadata().expect("fixture metadata").len()
}

/// E24-B2BASE anchor cell payloads (verbatim) with a configurable delimiter so
/// the TSV anchors keep the same cells as the CSV anchors.
fn write_anchor_delimited(path: &Path, delimiter: u8, cols: usize, rows: usize) -> u64 {
    let sep = delimiter as char;
    write_buf(path, |out| {
        let names = field_names(cols);
        let header = names.join(&sep.to_string());
        out.write_all(header.as_bytes()).unwrap();
        out.write_all(b"\n").unwrap();
        for row in 0..rows {
            for (col, _) in names.iter().enumerate() {
                if col > 0 {
                    write!(out, "{sep}").unwrap();
                }
                out.write_all(cell_payload(row, col).as_bytes()).unwrap();
            }
            out.write_all(b"\n").unwrap();
        }
    })
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

/// O0-B1 narrow fixed-width shape (sibling baseline #293) with a configurable
/// delimiter.
fn write_narrow_fixed_delimited(path: &Path, delimiter: u8, rows: usize) -> u64 {
    let sep = delimiter as char;
    write_buf(path, |out| {
        let names = field_names(8);
        out.write_all(names.join(&sep.to_string()).as_bytes())
            .unwrap();
        out.write_all(b"\n").unwrap();
        for row in 0..rows {
            writeln!(
                out,
                "{:010}{sep}{:010}{sep}{:014.4}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}",
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

/// O0-B1 wide mixed-schema shape (sibling baseline #293).
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

/// O0-B1 long-UTF-8 variable-width shape (sibling baseline #293).
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

/// O0-B1 malformed width-drift shape (sibling baseline #293): one
/// three-field row at two thirds of the stream, no quote characters, so the
/// bounded inference prefix stays clean and the failure must surface in the
/// streaming stages.
fn write_malformed_width(path: &Path, rows: usize) -> u64 {
    let bad_row = rows * 2 / 3;
    write_buf(path, |out| {
        out.write_all(b"c0,c1,c2,c3,c4,c5,c6,c7,c8,c9\n").unwrap();
        for row in 0..rows {
            if row == bad_row {
                out.write_all(b"404,short,row\n").unwrap();
            } else {
                writeln!(
                    out,
                    "{:010},{:010},{:014.4},{},{},{},{},{},{},{}",
                    row,
                    row % 100_000,
                    (row % 10_000) as f64 / 8.0,
                    fixed_string(row, 3, 12),
                    fixed_string(row, 4, 12),
                    fixed_string(row, 5, 12),
                    fixed_string(row, 6, 12),
                    fixed_string(row, 7, 12),
                    row % 2,
                    row % 3,
                )
                .unwrap();
            }
        }
    })
}

/// O0-C1 typed schema-drift variant: every sampled cell is an integer so
/// inference establishes Int64 columns, then one row at two thirds of the
/// stream replaces one cell with a ten-character non-integer. Field width is
/// preserved so only the type drifts, never the row width.
fn write_malformed_typed(path: &Path, rows: usize) -> u64 {
    let bad_row = rows * 2 / 3;
    write_buf(path, |out| {
        out.write_all(b"c0,c1,c2,c3,c4,c5,c6,c7\n").unwrap();
        for row in 0..rows {
            if row == bad_row {
                writeln!(
                    out,
                    "{:010},X{:09},{:010},{:010},{:010},{:010},{:010},{:010}",
                    row,
                    row,
                    row % 100_000,
                    row % 999_990,
                    row % 900_001,
                    row % 123_456,
                    row % 777_777,
                    row % 314_159,
                )
                .unwrap();
            } else {
                writeln!(
                    out,
                    "{:010},{:010},{:010},{:010},{:010},{:010},{:010},{:010}",
                    row,
                    row % 100_000,
                    row % 999_990,
                    row % 900_001,
                    row % 123_456,
                    row % 777_777,
                    row % 314_159,
                    row % 271_828,
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
            write_anchor_delimited(&dir.join("f.csv"), b',', 10, 100_000),
        ),
        "anchor-csv-10c-1m" => (
            "f.csv",
            write_anchor_delimited(&dir.join("f.csv"), b',', 10, 1_000_000),
        ),
        "anchor-tsv-10c-100k" => (
            "f.tsv",
            write_anchor_delimited(&dir.join("f.tsv"), b'\t', 10, 100_000),
        ),
        "narrow-fixed-8c-100k" => (
            "f.csv",
            write_narrow_fixed_delimited(&dir.join("f.csv"), b',', 100_000),
        ),
        "narrow-fixed-tsv-8c-100k" => (
            "f.tsv",
            write_narrow_fixed_delimited(&dir.join("f.tsv"), b'\t', 100_000),
        ),
        "wide-mixed-128c-100k" => ("f.csv", write_wide_mixed(&dir.join("f.csv"), 100_000)),
        "longutf8-8c-100k" => ("f.csv", write_long_utf8(&dir.join("f.csv"), 100_000)),
        "malformed-width-10c-60k" => ("f.csv", write_malformed_width(&dir.join("f.csv"), 60_000)),
        "malformed-typed-8c-60k" => ("f.csv", write_malformed_typed(&dir.join("f.csv"), 60_000)),
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
// Measurement helpers (adapted from tests/read_baseline.rs and the sibling
// O0-B1 harness)
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

fn read_counter_snapshot(path: &Path) -> (BTreeMap<String, u64>, bool) {
    let mut map = BTreeMap::new();
    let Ok(text) = fs::read_to_string(path) else {
        // Without the io-metrics feature the connector never constructs a dump
        // side channel; with the feature a run that fails before reader
        // construction leaves no dump either. Report absence explicitly.
        for label in COUNTER_LABELS {
            map.insert((*label).to_string(), 0);
        }
        return (map, false);
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
    (map, true)
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
// Witness: deterministic digest over produced batches
// ---------------------------------------------------------------------------

fn digest_batch(batch: &arrow_array::RecordBatch, hasher: &mut Sha256) {
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

// ---------------------------------------------------------------------------
// Case runners
// ---------------------------------------------------------------------------

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "o0-c1 fixture root",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/o0-c1").expect("credential reference"),
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

fn error_witness(error: &stillflow_core::ConnectorError) -> String {
    format!(
        "category={:?} retryable={} message={}",
        error.category(),
        error.retryable(),
        error.user_message()
    )
}

struct RunSample {
    wall_ms: u128,
    cpu_ticks: Option<u64>,
    rows: u64,
    error: Option<String>,
    counters: BTreeMap<String, u64>,
}

/// Full production drain (`read_batches` to the end).
async fn drain_full(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) -> Result<u64, stillflow_core::ConnectorError> {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await?;
    let mut rows = 0_u64;
    while let Some(item) = stream.next().await {
        rows += item?.payload().num_rows() as u64;
    }
    Ok(rows)
}

/// Consumer-driven prefix consumption: take N envelopes then drop the stream.
async fn drain_early_drop(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) -> Result<u64, stillflow_core::ConnectorError> {
    let stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await?;
    let mut rows = 0_u64;
    let mut taken = 0_usize;
    let mut stream = pin!(stream);
    while taken < EARLY_DROP_BATCHES {
        let Some(item) = stream.next().await else {
            break;
        };
        rows += item?.payload().num_rows() as u64;
        taken += 1;
    }
    drop(stream);
    Ok(rows)
}

/// Production bounded read: `PreviewRequest` with a row limit.
async fn run_preview(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) -> Result<u64, stillflow_core::ConnectorError> {
    let request = PreviewRequest::new(asset.clone(), PREVIEW_ROW_LIMIT, PREVIEW_BYTE_LIMIT);
    let preview = registry.preview(connection, request).await?;
    Ok(preview.rows_returned as u64)
}

/// Harness-side reference probe: one plain csv-crate pass over the fixture
/// with the same delimiter/quote/header settings as the production validator.
fn run_validate_probe(path: &Path, delimiter: u8) -> Result<u64, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .quote(b'"')
        .has_headers(true)
        .flexible(false)
        .from_reader(file);
    let mut record = csv::StringRecord::new();
    let mut rows = 0_u64;
    loop {
        match reader.read_record(&mut record) {
            Ok(true) => rows += 1,
            Ok(false) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(rows)
}

async fn witness_batches(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
    early_drop: bool,
) -> Result<serde_json::Value, String> {
    let stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await
        .map_err(|error| error_witness(&error))?;
    let mut hasher = Sha256::new();
    let mut rows = 0_u64;
    let mut taken = 0_usize;
    let mut schema = String::new();
    let mut stream = pin!(stream);
    while let Some(item) = stream.next().await {
        match item {
            Ok(envelope) => {
                hasher.update(taken.to_le_bytes());
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
                taken += 1;
                if early_drop && taken >= EARLY_DROP_BATCHES {
                    break;
                }
            }
            Err(error) => {
                return Ok(serde_json::json!({
                    "kind": "error",
                    "rows": rows,
                    "error": error_witness(&error),
                }));
            }
        }
    }
    drop(stream);
    Ok(serde_json::json!({
        "kind": if early_drop { "early_drop_batches" } else { "ingest_batches" },
        "rows": rows,
        "batches": taken,
        "digest": sha256_hex(hasher.finalize().as_slice()),
        "schema": schema,
    }))
}

async fn witness_preview(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) -> Result<serde_json::Value, String> {
    let request = PreviewRequest::new(asset.clone(), PREVIEW_ROW_LIMIT, PREVIEW_BYTE_LIMIT);
    let preview = registry
        .preview(connection, request)
        .await
        .map_err(|error| error_witness(&error))?;
    let mut hasher = Sha256::new();
    let mut rows = 0_u64;
    for (sequence, envelope) in preview.batches.iter().enumerate() {
        hasher.update(sequence.to_le_bytes());
        digest_batch(envelope.payload(), &mut hasher);
        rows += envelope.payload().num_rows() as u64;
    }
    Ok(serde_json::json!({
        "kind": "preview_batches",
        "rows": rows,
        "batches": preview.batches.len(),
        "digest": sha256_hex(hasher.finalize().as_slice()),
        "rows_returned": preview.rows_returned,
        "rows_truncated": preview.rows_truncated,
        "bytes_returned": preview.bytes_returned,
        "bytes_truncated": preview.bytes_truncated,
    }))
}

async fn run_case(spec: &CaseSpec, root: &Path, head: &str) -> serde_json::Value {
    let fixture = generate_fixture(root, spec);
    let dir = root.join(spec.fixture);
    let fixture_path = dir.join(fixture.name);
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
    let metrics_out = dir.join("counters.out");
    std::env::set_var(METRICS_OUT_ENV, &metrics_out);

    let alloc_before = alloc_snapshot();

    if spec.mode == Mode::ValidateProbe {
        // Warm-up (untimed): one probe pass to warm the page cache.
        run_validate_probe(&fixture_path, spec.delimiter).expect("probe warm-up");
        let mut samples: Vec<RunSample> = Vec::with_capacity(spec.reps);
        for _ in 0..spec.reps {
            let cpu_before = cpu_ticks_total();
            let start = Instant::now();
            let outcome = run_validate_probe(&fixture_path, spec.delimiter);
            let wall_ms = start.elapsed().as_millis();
            let cpu_ticks = match (cpu_before, cpu_ticks_total()) {
                (Some(before), Some(after)) => Some(after.saturating_sub(before)),
                _ => None,
            };
            let (rows, error) = match outcome {
                Ok(rows) => (rows, None),
                Err(error) => (0, Some(error)),
            };
            samples.push(RunSample {
                wall_ms,
                cpu_ticks,
                rows,
                error,
                counters: BTreeMap::new(),
            });
        }
        let witness_rows =
            run_validate_probe(&fixture_path, spec.delimiter).expect("probe witness");
        let alloc_after = alloc_snapshot();
        return finish_record(
            spec,
            head,
            &fixture,
            samples,
            witness_rows,
            None,
            &BTreeMap::new(),
            false,
            alloc_before,
            alloc_after,
            serde_json::json!({
                "kind": "validate_probe",
                "rows": witness_rows,
            }),
        );
    }

    // Warm-up: one untimed run (Polars pool init, page cache for the fixture).
    let warm = match spec.mode {
        Mode::Full => drain_full(&connection, &registry, &asset).await,
        Mode::EarlyDrop => drain_early_drop(&connection, &registry, &asset).await,
        Mode::Preview => run_preview(&connection, &registry, &asset).await,
        Mode::ValidateProbe => unreachable!("probe handled above"),
    };
    let warm_error = warm.as_ref().err().map(error_witness);
    if warm.is_err() {
        eprintln!(
            "[o0-c1] case {} fails as expected: {:?}",
            spec.id, warm_error
        );
    }

    let mut samples: Vec<RunSample> = Vec::with_capacity(spec.reps);
    let (mut previous_counters, _) = read_counter_snapshot(&metrics_out);
    let mut last_counters: BTreeMap<String, u64> = BTreeMap::new();
    let mut metrics_present = false;
    for _ in 0..spec.reps {
        let _ = fs::remove_file(&metrics_out);
        let cpu_before = cpu_ticks_total();
        let start = Instant::now();
        let outcome = match spec.mode {
            Mode::Full => drain_full(&connection, &registry, &asset)
                .await
                .map_err(|error| error_witness(&error)),
            Mode::EarlyDrop => drain_early_drop(&connection, &registry, &asset)
                .await
                .map_err(|error| error_witness(&error)),
            Mode::Preview => run_preview(&connection, &registry, &asset)
                .await
                .map_err(|error| error_witness(&error)),
            Mode::ValidateProbe => unreachable!("probe handled above"),
        };
        let wall_ms = start.elapsed().as_millis();
        let cpu_ticks = match (cpu_before, cpu_ticks_total()) {
            (Some(before), Some(after)) => Some(after.saturating_sub(before)),
            _ => None,
        };
        let (rows, error) = match outcome {
            Ok(rows) => (rows, None),
            Err(error) => (0, Some(error)),
        };
        let (current, present) = read_counter_snapshot(&metrics_out);
        metrics_present |= present;
        let rep_counters = counter_delta(&current, &previous_counters);
        previous_counters = current;
        samples.push(RunSample {
            wall_ms,
            cpu_ticks,
            rows,
            error,
            counters: rep_counters,
        });
        last_counters = samples.last().expect("sample").counters.clone();
    }

    // Witness run (untimed): full digest of the produced output, or the error
    // witness for malformed fixtures.
    let witness = match spec.mode {
        Mode::Full | Mode::EarlyDrop => {
            witness_batches(&connection, &registry, &asset, spec.mode == Mode::EarlyDrop)
                .await
                .expect("witness drain")
        }
        Mode::Preview => witness_preview(&connection, &registry, &asset)
            .await
            .expect("witness preview"),
        Mode::ValidateProbe => unreachable!("probe handled above"),
    };
    let alloc_after = alloc_snapshot();

    finish_record(
        spec,
        head,
        &fixture,
        samples,
        warm.unwrap_or(0),
        warm_error.as_deref(),
        &last_counters,
        metrics_present,
        alloc_before,
        alloc_after,
        witness,
    )
}

#[allow(clippy::too_many_arguments)]
fn counter_percentiles(samples: &[RunSample], quantile: f64) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    for label in COUNTER_LABELS {
        let mut values: Vec<u64> = samples
            .iter()
            .map(|sample| sample.counters.get(*label).copied().unwrap_or(0))
            .collect();
        values.sort_unstable();
        let index = ((values.len() as f64 - 1.0) * quantile).round() as usize;
        let index = index.min(values.len().saturating_sub(1));
        map.insert(
            (*label).to_string(),
            values.get(index).copied().unwrap_or(0),
        );
    }
    map
}

fn finish_record(
    spec: &CaseSpec,
    head: &str,
    fixture: &FixtureFile,
    samples: Vec<RunSample>,
    rows_witness: u64,
    error_witness: Option<&str>,
    last_counters: &BTreeMap<String, u64>,
    metrics_present: bool,
    alloc_before: (u64, u64),
    alloc_after: (u64, u64),
    witness: serde_json::Value,
) -> serde_json::Value {
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

    let mut counters_json = serde_json::Map::new();
    for (label, value) in last_counters {
        counters_json.insert(label.clone(), serde_json::json!(value));
    }

    serde_json::json!({
        "case": spec.id,
        "mode": spec.mode.name(),
        "head": head,
        "feature_io_metrics": metrics_present,
        "fixture": {
            "name": fixture.name,
            "rows_expected": spec.rows_expected,
            "cols": spec.cols,
            "bytes": fixture.bytes,
            "sha256": fixture.sha256,
        },
        "warmup_runs": 1,
        "reps": samples.len(),
        "wall_ms": { "p50": p50, "p95": p95, "min": min, "max": max },
        "cpu_ms": { "p50": if cpu_reliable { serde_json::json!(cpu_p50) } else { serde_json::Value::Null }, "reliable": cpu_reliable },
        "rows_per_run_stable": stable_rows,
        "rows_last_run": rows_witness,
        "error_stable_across_reps": stable_error,
        "error_witness": error_witness,
        "peak_rss_kib_process_lifetime": peak_resident_kib(),
        "peak_rss_attribution": "process-lifetime VmHWM including warm-up; not per-run",
        "alloc_count_delta_case": alloc_after.0.saturating_sub(alloc_before.0),
        "alloc_bytes_delta_case": alloc_after.1.saturating_sub(alloc_before.1),
        "alloc_attribution": "case-level counting allocator delta including warm-up, reps and witness",
        "counters_last_run": counters_json,
        "counters_p50_across_reps": counter_percentiles(&samples, 0.5),
        "counters_p95_across_reps": counter_percentiles(&samples, 0.95),
        "witness": witness,
        "counter_semantics": {
            "validator_read_bytes": "exact logical bytes pulled through the csv-crate validator handle (CountingReader); includes the header re-check read",
            "decoder_os_bytes": "recorded full-file size for the mmap-backed Polars decoder; handle/OS-level, NOT exact logical read bytes",
            "inference_phase_bytes": "exact logical bytes read by the bounded schema-inference pass",
            "csv_rows_decoded": "row heights of the Polars-decoded frames",
            "csv_rows_validated": "records read by the csv-crate lockstep validator",
            "csv_fail_decode": "1 if the Polars decode stage raised the terminating error",
            "csv_fail_validate": "1 if the validation stage raised the terminating error",
            "ingest_inspect_nanos": "wall time of the bounded inspect/inference stage",
            "ingest_prepare_nanos": "wall time of the delimited prepare segment (validator build, header re-check, Polars batched-reader construction)",
            "ingest_decode_nanos": "consumer-side wall time blocked in Polars next_batches; Polars may decode ahead on its own threads, so this is a blocking-time observation, not exclusive CPU time",
            "ingest_validate_nanos": "wall time of the csv-crate lockstep validation for the same frames",
        },
    })
}

#[tokio::test]
#[ignore]
async fn o0_c1_csv_dup_work() {
    let case_id = std::env::var(CASE_ENV).expect("O0_C1_CASE selects one case");
    let mode = std::env::var(MODE_ENV).unwrap_or_else(|_| "measure".to_owned());
    let root = PathBuf::from(
        std::env::var(FIXTURE_ROOT_ENV).unwrap_or_else(|_| "/tmp/o0-c1-fixtures".to_owned()),
    );
    let head = std::env::var(HEAD_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_owned());
    let spec = CASES
        .iter()
        .find(|spec| spec.id == case_id)
        .unwrap_or_else(|| panic!("unknown case {case_id}"));

    fs::create_dir_all(&root).expect("fixture root");

    if mode == "generate" {
        let fixture = generate_fixture(&root, spec);
        println!(
            "{}",
            serde_json::json!({
                "case": spec.id,
                "mode": "generate",
                "fixture": {
                    "name": fixture.name,
                    "rows_expected": spec.rows_expected,
                    "cols": spec.cols,
                    "bytes": fixture.bytes,
                    "sha256": fixture.sha256,
                },
            })
        );
        return;
    }

    let record = run_case(spec, &root, &head).await;
    println!("{record}");
}
