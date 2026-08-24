//! E24-B2BASE — measurement-only ingestion baseline for the local-tabular
//! connector (CSV / NDJSON / top-level-array JSON / Parquet).
//!
//! Contract: Issue #99. Feature-gated: this test only exists and runs with
//! `--features io-metrics` and is `#[ignore]`d so ordinary test runs never
//! execute it. It records a baseline only: no optimization, no threshold, no
//! improvement claim.
//!
//! Output: one machine-readable JSON object per fixture case on stdout:
//! `{"head":...,"feature":"io-metrics","fixture":{...},"metrics":{...}}`
//!
//! Side channel: the library writes cumulative counter lines to the path in
//! `E24_IO_METRICS_OUT` when a `PreparedReader` drops; this test deletes that
//! file before each case and derives per-case deltas from the cumulative
//! snapshots (library counters are never reset).

#![cfg(feature = "io-metrics")]
#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, ReadRequest, RequestContext, SourceConnection,
};
use tempfile::TempDir;

const HEAD_SHA_ENV: &str = "E24_HEAD_SHA";
const METRICS_OUT_ENV: &str = "E24_IO_METRICS_OUT";
const DEFAULT_HEAD: &str = "636cd7db443bed45e7adcf1596785670cfc3ff1c";
const REPS: usize = 30;
const BATCH_SIZE: usize = 4096;
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
// Allocator hook (test binary only; safe wrapper around System).
// ---------------------------------------------------------------------------

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
// Platform observables (safe /proc reads; USER_HZ assumed 100 → 10 ms/tick).
// ---------------------------------------------------------------------------

fn cpu_ticks_total() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").expect("Linux process stat");
    let rest = stat.rsplit_once(')').map(|(_, r)| r).unwrap_or(&stat);
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
    let stime: u64 = fields.get(13).and_then(|v| v.parse().ok()).unwrap_or(0);
    utime + stime
}

/// VmHWM is the process-LIFETIME high-water mark; it includes fixture
/// generation and cannot be attributed per case. Reported honestly as such.
fn peak_resident_kib() -> u64 {
    fs::read_to_string("/proc/self/status")
        .expect("Linux process status")
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .expect("VmHWM in Linux process status")
}

fn percentile(mut values: Vec<u128>, quantile: f64) -> u128 {
    values.sort_unstable();
    let index = ((values.len() as f64) * quantile).ceil() as usize;
    values[index.min(values.len() - 1)]
}

// ---------------------------------------------------------------------------
// Deterministic fixtures.
// ---------------------------------------------------------------------------

fn cell_payload(row: usize, col: usize) -> String {
    let len = 32 + (row.wrapping_mul(31).wrapping_add(col.wrapping_mul(7))) % 65;
    let prefix = format!("v{row:08}_{col:02}_");
    let mut payload = prefix;
    while payload.len() < len {
        payload.push((b'a' + ((row + col + payload.len()) % 26) as u8) as char);
    }
    payload
}

fn field_names(cols: usize) -> Vec<String> {
    (0..cols).map(|col| format!("c{col}")).collect()
}

fn write_delimited_or_json(
    path: &Path,
    format: &str,
    cols: usize,
    rows: usize,
) -> std::io::Result<u64> {
    let mut out = std::io::BufWriter::new(File::create(path)?);
    let names = field_names(cols);
    let mut bytes = 0_u64;
    if format == "csv" {
        out.write_all(names.join(",").as_bytes())?;
        out.write_all(b"\n")?;
        bytes += names.join(",").len() as u64 + 1;
    } else if format == "array" {
        out.write_all(b"[")?;
        bytes += 1;
    }
    for row in 0..rows {
        if format == "array" && row > 0 {
            out.write_all(b",")?;
            bytes += 1;
        }
        if format != "csv" {
            out.write_all(b"{")?;
            bytes += 1;
        }
        for (col, name) in names.iter().enumerate() {
            if col > 0 {
                out.write_all(b",")?;
                bytes += 1;
            }
            let payload = cell_payload(row, col);
            if format == "csv" {
                out.write_all(payload.as_bytes())?;
                bytes += payload.len() as u64;
            } else {
                let cell = format!("\"{name}\":\"{payload}\"");
                out.write_all(cell.as_bytes())?;
                bytes += cell.len() as u64;
            }
        }
        if format == "csv" {
            out.write_all(b"\n")?;
            bytes += 1;
        } else {
            out.write_all(b"}")?;
            bytes += 1;
            if format == "ndjson" {
                out.write_all(b"\n")?;
                bytes += 1;
            }
        }
    }
    if format == "array" {
        out.write_all(b"]\n")?;
        bytes += 2;
    }
    out.flush()?;
    Ok(bytes)
}

fn write_parquet(path: &Path, cols: usize, rows: usize) -> std::io::Result<u64> {
    let mut schema = String::from("message schema {");
    for col in 0..cols {
        schema.push_str(&format!(" REQUIRED BYTE_ARRAY c{col};"));
    }
    schema.push_str(" }");
    let message_type = parse_message_type(&schema).expect("fixture parquet schema");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, Arc::new(message_type), Arc::new(props))
        .expect("parquet writer");
    let mut row_group = writer.next_row_group().expect("row group");
    let mut bytes = 0_u64;
    for col in 0..cols {
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            let payload = cell_payload(row, col);
            bytes += payload.len() as u64 + 4;
            values.push(ByteArray::from(payload.into_bytes()));
        }
        let mut column = row_group
            .next_column()
            .expect("column in row group")
            .expect("column writer");
        {
            let typed = column.typed::<ByteArrayType>();
            typed.write_batch(&values, None, None).expect("write batch");
        }
        column.close().expect("close column");
    }
    row_group.close().expect("close row group");
    writer.close().expect("close writer");
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

async fn setup_and_discover(
    root: &Path,
) -> (
    stillflow_core::SourceConnection,
    ConnectorRegistry,
    Vec<stillflow_core::SourceAsset>,
) {
    let connection = SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "e24-b2base fixture root",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 64, "maxBytes": 65536 }
        }),
        CredentialRef::new("cred://local/e24-b2base").expect("credential reference"),
    )
    .expect("connection");
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
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
    (connection, registry, assets)
}

fn asset_named(assets: &[stillflow_core::SourceAsset], name: &str) -> stillflow_core::SourceAsset {
    assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("asset {name} not found"))
        .clone()
}

async fn ingest_once(
    connection: &stillflow_core::SourceConnection,
    registry: &ConnectorRegistry,
    asset: &stillflow_core::SourceAsset,
) {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await
        .expect("open bounded stream");
    while let Some(item) = stream.next().await {
        item.expect("ingestion batch");
    }
    drop(stream); // triggers the library counter dump on PreparedReader drop
}

fn read_counter_snapshot(path: &str) -> BTreeMap<String, u64> {
    let text = fs::read_to_string(path).expect("metrics dump file");
    let mut map: BTreeMap<String, u64> = BTreeMap::new();
    for line in text.lines() {
        if let Some((label, value)) = line.split_once('=') {
            if let Ok(value) = value.parse::<u64>() {
                map.insert(label.to_string(), value);
            }
        }
    }
    for label in COUNTER_LABELS {
        map.entry(label.to_string()).or_insert(0);
    }
    map
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[allow(clippy::too_many_arguments)]
fn print_case(
    format: &str,
    cols: usize,
    rows: usize,
    fixture_bytes: u64,
    walls: &[u128],
    cpu_tick_deltas: &[u64],
    alloc_before: (u64, u64),
    alloc_after: (u64, u64),
    peak_kib: u64,
    counter_deltas: &BTreeMap<String, u64>,
) {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let p50 = percentile(walls.to_vec(), 0.5);
    let p95 = percentile(walls.to_vec(), 0.95);
    let cpu_ticks: u64 = cpu_tick_deltas.iter().sum();

    let mut metrics = String::new();
    for (label, delta) in counter_deltas {
        metrics.push_str(&format!("\"{}\":{},", json_escape(label), delta));
    }
    metrics.push_str(&format!("\"wall_p50_ms\":{p50},"));
    metrics.push_str(&format!("\"wall_p95_ms\":{p95},"));
    metrics.push_str(&format!("\"wall_reps\":{}", walls.len()));
    metrics.push_str(&format!(",\"cpu_ticks_sum\":{cpu_ticks}"));
    metrics.push_str(&format!(",\"cpu_ms_est\":{}", cpu_ticks * 10));
    metrics.push_str(&format!(
        ",\"alloc_count_delta\":{}",
        alloc_after.0.saturating_sub(alloc_before.0)
    ));
    metrics.push_str(&format!(
        ",\"alloc_bytes_delta\":{}",
        alloc_after.1.saturating_sub(alloc_before.1)
    ));
    metrics.push_str(&format!(",\"peak_rss_lifetime_kib\":{peak_kib}"));

    println!(
        "{{\"head\":\"{}\",\"feature\":\"io-metrics\",\"fixture\":{{\"format\":\"{}\",\"cols\":{},\"rows\":{},\"bytes\":{}}},\"metrics\":{{{}}}}}",
        json_escape(&head),
        json_escape(format),
        cols,
        rows,
        fixture_bytes,
        metrics
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn e24_b2_ingestion_baseline() {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let metrics_out =
        std::env::var(METRICS_OUT_ENV).unwrap_or_else(|_| "/tmp/e24_io_metrics.out".to_string());
    eprintln!("[e24-b2base] head={head} reps/case={REPS} metrics_out={metrics_out}");

    let temp = TempDir::new().expect("fixture root");
    let root = temp.path();

    let cases: Vec<(&str, usize, usize)> = vec![
        ("csv", 10, 100_000),
        ("csv", 100, 100_000),
        ("csv", 10, 1_000_000),
        ("csv", 100, 1_000_000),
        ("ndjson", 10, 100_000),
        ("ndjson", 100, 100_000),
        ("ndjson", 10, 1_000_000),
        ("ndjson", 100, 1_000_000),
        ("array", 10, 100_000),
        ("array", 100, 100_000),
        ("array", 10, 1_000_000),
        ("array", 100, 1_000_000),
        ("parquet", 10, 100_000),
        ("parquet", 100, 100_000),
        ("parquet", 10, 1_000_000),
        ("parquet", 100, 1_000_000),
    ];

    let mut generated: BTreeMap<(String, usize, usize), (String, u64)> = BTreeMap::new();
    for (format, cols, rows) in &cases {
        let ext = match *format {
            "csv" => "csv",
            "ndjson" => "ndjson",
            "array" => "json",
            "parquet" => "parquet",
            _ => unreachable!(),
        };
        let name = format!("f_{format}_{cols}c_{rows}r.{ext}");
        let path = root.join(&name);
        let bytes = if *format == "parquet" {
            write_parquet(&path, *cols, *rows).expect("parquet fixture")
        } else {
            write_delimited_or_json(&path, format, *cols, *rows).expect("text fixture")
        };
        generated.insert((format.to_string(), *cols, *rows), (name, bytes));
    }

    let (connection, registry, assets) = setup_and_discover(root).await;

    // Cumulative counter values captured just before a case's first rep.
    let mut case_start: BTreeMap<String, u64> = COUNTER_LABELS
        .iter()
        .map(|label| (label.to_string(), 0_u64))
        .collect();

    for (format, cols, rows) in &cases {
        let (name, fixture_bytes) = generated
            .get(&(format.to_string(), *cols, *rows))
            .expect("generated fixture");
        let asset = asset_named(&assets, name);

        let alloc_before = alloc_snapshot();
        let mut walls = Vec::with_capacity(REPS);
        let mut cpu_deltas = Vec::with_capacity(REPS);
        let mut cumulative_after_case = case_start.clone();

        for _rep in 0..REPS {
            let _ = fs::remove_file(&metrics_out);
            let cpu_before = cpu_ticks_total();
            let start = Instant::now();
            ingest_once(&connection, &registry, &asset).await;
            walls.push(start.elapsed().as_millis());
            cpu_deltas.push(cpu_ticks_total().saturating_sub(cpu_before));
            cumulative_after_case = read_counter_snapshot(&metrics_out);
        }

        // Per-case deltas = cumulative after last rep minus cumulative before
        // the first rep of this case.
        let mut counter_deltas = BTreeMap::new();
        for label in COUNTER_LABELS {
            let current = cumulative_after_case.get(*label).copied().unwrap_or(0);
            let previous = case_start.get(*label).copied().unwrap_or(0);
            counter_deltas.insert(label.to_string(), current.saturating_sub(previous));
        }

        let alloc_after = alloc_snapshot();
        let peak_kib = peak_resident_kib();
        print_case(
            format,
            *cols,
            *rows,
            *fixture_bytes,
            &walls,
            &cpu_deltas,
            alloc_before,
            alloc_after,
            peak_kib,
            &counter_deltas,
        );

        case_start = cumulative_after_case;
    }
}
