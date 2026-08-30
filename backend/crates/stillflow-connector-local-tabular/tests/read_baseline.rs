//! E24-B2BASE — measurement-only ingestion baseline (Issue #99).
//!
//! Feature-gated and ignored so default CI never runs it. Records a baseline
//! only: no optimization, no threshold, no improvement claim.
//!
//! One JSON object per fixture case is printed to stdout. CSV `decoder_os_bytes`
//! is a handle/OS-level observation (Polars may mmap); it is not exact logical
//! read bytes and must not be used to prove or disprove C1.

#![cfg(feature = "io-metrics")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, ReadRequest, RequestContext, SourceAsset, SourceConnection,
};
use tempfile::TempDir;

const HEAD_SHA_ENV: &str = "E24_HEAD_SHA";
const METRICS_OUT_ENV: &str = "E24_IO_METRICS_OUT";
const DEFAULT_HEAD: &str = "04966586192f8750a02790da988db71a28d82074";
const REPS: usize = 30;
const FOCUSED_JSON_REPS: usize = 3;
const BATCH_SIZE: usize = 4_096;
const PARQUET_CHUNK_ROWS: usize = 8_192;
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
    "json_arrow_flushes",
    "parquet_reader_constructions",
    "parquet_batch_finishes",
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
    let mut out = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let names = field_names(cols);
    let mut bytes = 0_u64;
    if format == "csv" {
        let header = names.join(",");
        out.write_all(header.as_bytes())?;
        out.write_all(b"\n")?;
        bytes += header.len() as u64 + 1;
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
    let names = field_names(cols);
    let fields: Vec<Field> = names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut logical_bytes = 0_u64;
    let mut start = 0_usize;
    while start < rows {
        let end = (start + PARQUET_CHUNK_ROWS).min(rows);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(cols);
        for col in 0..cols {
            let mut values = Vec::with_capacity(end - start);
            for row in start..end {
                let payload = cell_payload(row, col);
                logical_bytes += payload.len() as u64;
                values.push(payload);
            }
            columns.push(Arc::new(StringArray::from(values)));
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        start = end;
    }
    writer
        .close()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(logical_bytes)
}

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "e24-b2base fixture root",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/e24-b2base").expect("credential reference"),
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

async fn discover_assets(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
) -> Vec<SourceAsset> {
    registry
        .discover(
            connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover")
}

fn asset_named(assets: &[SourceAsset], name: &str) -> SourceAsset {
    assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("asset {name} not found"))
        .clone()
}

async fn ingest_once(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), BATCH_SIZE))
        .await
        .expect("open bounded stream");
    while let Some(item) = stream.next().await {
        item.expect("ingestion batch");
    }
}

fn read_counter_snapshot(path: &Path) -> BTreeMap<String, u64> {
    let text = fs::read_to_string(path).expect("metrics dump file");
    let mut map = BTreeMap::new();
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

fn print_case(
    format: &str,
    cols: usize,
    rows: usize,
    fixture_bytes: u64,
    walls: &[u128],
    cpu_tick_deltas: &[Option<u64>],
    alloc_before: (u64, u64),
    alloc_after: (u64, u64),
    peak_kib: Option<u64>,
    counter_deltas: &BTreeMap<String, u64>,
) {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let p50 = percentile(walls.to_vec(), 0.5);
    let p95 = percentile(walls.to_vec(), 0.95);
    let cpu_available = cpu_tick_deltas.iter().all(Option::is_some);
    let cpu_ticks_sum = if cpu_available {
        Some(
            cpu_tick_deltas
                .iter()
                .filter_map(|value| *value)
                .sum::<u64>(),
        )
    } else {
        None
    };

    let mut metrics = serde_json::Map::new();
    for (label, delta) in counter_deltas {
        metrics.insert(label.clone(), serde_json::json!(delta));
    }
    metrics.insert("wall_p50_ms".to_string(), serde_json::json!(p50));
    metrics.insert("wall_p95_ms".to_string(), serde_json::json!(p95));
    metrics.insert("wall_reps".to_string(), serde_json::json!(walls.len()));
    metrics.insert(
        "alloc_count_delta".to_string(),
        serde_json::json!(alloc_after.0.saturating_sub(alloc_before.0)),
    );
    metrics.insert(
        "alloc_bytes_delta".to_string(),
        serde_json::json!(alloc_after.1.saturating_sub(alloc_before.1)),
    );
    match cpu_ticks_sum {
        Some(ticks) => {
            metrics.insert("cpu_ticks_sum".to_string(), serde_json::json!(ticks));
            metrics.insert("cpu_ms_est".to_string(), serde_json::json!(ticks * 10));
        }
        None => {
            metrics.insert("cpu_ticks_sum".to_string(), serde_json::Value::Null);
            metrics.insert("cpu_ms_est".to_string(), serde_json::Value::Null);
            metrics.insert(
                "cpu_observation_limitation".to_string(),
                serde_json::json!("process CPU ticks require Linux /proc/self/stat"),
            );
        }
    }
    match peak_kib {
        Some(kib) => {
            metrics.insert("peak_rss_lifetime_kib".to_string(), serde_json::json!(kib));
            metrics.insert(
                "peak_rss_attribution".to_string(),
                serde_json::json!("process-lifetime VmHWM; not per-case"),
            );
        }
        None => {
            metrics.insert("peak_rss_lifetime_kib".to_string(), serde_json::Value::Null);
            metrics.insert(
                "peak_rss_observation_limitation".to_string(),
                serde_json::json!("peak RSS requires Linux /proc/self/status VmHWM"),
            );
        }
    }
    metrics.insert(
        "decoder_os_bytes_kind".to_string(),
        serde_json::json!("handle_or_os_level_observation"),
    );
    metrics.insert(
        "decoder_os_bytes_not_logical".to_string(),
        serde_json::json!(true),
    );
    metrics.insert(
        "c1_note".to_string(),
        serde_json::json!(
            "CSV decoder_os_bytes is handle/OS-level (Polars may mmap). Do not use it to prove or disprove C1. Use validator_read_bytes as the exact logical validator-pass count."
        ),
    );

    let record = serde_json::json!({
        "head": head,
        "feature": "io-metrics",
        "fixture": {
            "format": format,
            "cols": cols,
            "rows": rows,
            "bytes": fixture_bytes
        },
        "metrics": metrics
    });
    println!("{record}");
}

#[tokio::test]
#[ignore]
async fn read_baseline() {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let temp = TempDir::new().expect("fixture root");
    let root = temp.path();
    let metrics_out: PathBuf = root.join("e24_io_metrics.out");
    std::env::set_var(METRICS_OUT_ENV, &metrics_out);
    eprintln!(
        "[e24-b2base] head={head} reps/case={REPS} metrics_out={}",
        metrics_out.display()
    );

    let cases: [(&str, usize, usize); 16] = [
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
    for (format, cols, rows) in cases {
        let ext = match format {
            "csv" => "csv",
            "ndjson" => "ndjson",
            "array" => "json",
            "parquet" => "parquet",
            _ => unreachable!(),
        };
        let name = format!("f_{format}_{cols}c_{rows}r.{ext}");
        let path = root.join(&name);
        eprintln!("[e24-b2base] generating {name}");
        let bytes = if format == "parquet" {
            write_parquet(&path, cols, rows).expect("parquet fixture")
        } else {
            write_delimited_or_json(&path, format, cols, rows).expect("text fixture")
        };
        generated.insert((format.to_string(), cols, rows), (name, bytes));
    }

    let connection = connection(root);
    let registry = registry();
    let assets = discover_assets(&registry, &connection).await;

    let mut case_start: BTreeMap<String, u64> = COUNTER_LABELS
        .iter()
        .map(|label| ((*label).to_string(), 0_u64))
        .collect();

    for (format, cols, rows) in cases {
        let (name, fixture_bytes) = generated
            .get(&(format.to_string(), cols, rows))
            .expect("generated fixture");
        let asset = asset_named(&assets, name);
        eprintln!("[e24-b2base] measuring {name}");

        let alloc_before = alloc_snapshot();
        let mut walls = Vec::with_capacity(REPS);
        let mut cpu_deltas = Vec::with_capacity(REPS);
        let mut cumulative_after_case = case_start.clone();

        for _rep in 0..REPS {
            let _ = fs::remove_file(&metrics_out);
            let cpu_before = cpu_ticks_total();
            let start = Instant::now();
            ingest_once(&connection, &registry, &asset).await;
            walls.push(u128::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            ));
            cpu_deltas.push(match (cpu_before, cpu_ticks_total()) {
                (Some(before), Some(after)) => Some(after.saturating_sub(before)),
                _ => None,
            });
            cumulative_after_case = read_counter_snapshot(&metrics_out);
        }

        let mut counter_deltas = BTreeMap::new();
        for label in COUNTER_LABELS {
            let current = cumulative_after_case.get(*label).copied().unwrap_or(0);
            let previous = case_start.get(*label).copied().unwrap_or(0);
            counter_deltas.insert((*label).to_string(), current.saturating_sub(previous));
        }

        let alloc_after = alloc_snapshot();
        print_case(
            format,
            cols,
            rows,
            *fixture_bytes,
            &walls,
            &cpu_deltas,
            alloc_before,
            alloc_after,
            peak_resident_kib(),
            &counter_deltas,
        );
        case_start = cumulative_after_case;
    }
}

fn set_json_direct(on: bool) {
    if on {
        std::env::set_var("STILLFLOW_JSON_ARROW_DIRECT", "1");
    } else {
        std::env::remove_var("STILLFLOW_JSON_ARROW_DIRECT");
    }
}

fn print_json_ab_case(
    strategy: &str,
    format: &str,
    cols: usize,
    rows: usize,
    fixture_bytes: u64,
    walls: &[u128],
    counter_deltas: &BTreeMap<String, u64>,
) {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let p50 = percentile(walls.to_vec(), 0.5);
    let p95 = percentile(walls.to_vec(), 0.95);
    let mut metrics = serde_json::Map::new();
    for (label, delta) in counter_deltas {
        metrics.insert(label.clone(), serde_json::json!(delta));
    }
    metrics.insert("strategy".to_string(), serde_json::json!(strategy));
    metrics.insert("wall_p50_ms".to_string(), serde_json::json!(p50));
    metrics.insert("wall_p95_ms".to_string(), serde_json::json!(p95));
    metrics.insert("wall_reps".to_string(), serde_json::json!(walls.len()));
    metrics.insert(
        "wall_samples_ms".to_string(),
        serde_json::json!(walls.iter().copied().collect::<Vec<_>>()),
    );
    let record = serde_json::json!({
        "head": head,
        "feature": "io-metrics,json-arrow-direct",
        "fixture": {
            "format": format,
            "cols": cols,
            "rows": rows,
            "bytes": fixture_bytes
        },
        "metrics": metrics
    });
    println!("{record}");
}

#[cfg(feature = "json-arrow-direct")]
#[tokio::test]
#[ignore]
async fn read_json_arrow_ab() {
    let head = std::env::var(HEAD_SHA_ENV).unwrap_or_else(|_| DEFAULT_HEAD.to_string());
    let temp = TempDir::new().expect("fixture root");
    let root = temp.path();
    let metrics_out: PathBuf = root.join("e24_io_metrics.out");
    std::env::set_var(METRICS_OUT_ENV, &metrics_out);
    set_json_direct(false);
    eprintln!(
        "[e24-b2json-a0] focused M3 head={head} reps/strategy/cell={FOCUSED_JSON_REPS} cells=ndjson 10x100k,100x100k"
    );

    let cases: [(&str, usize, usize); 2] = [("ndjson", 10, 100_000), ("ndjson", 100, 100_000)];

    let mut generated: BTreeMap<(String, usize, usize), (String, u64)> = BTreeMap::new();
    for (format, cols, rows) in cases {
        let ext = if format == "ndjson" { "ndjson" } else { "json" };
        let name = format!("f_{format}_{cols}c_{rows}r.{ext}");
        let path = root.join(&name);
        eprintln!("[e24-b2json-a0] generating {name}");
        let bytes = write_delimited_or_json(&path, format, cols, rows).expect("fixture");
        generated.insert((format.to_string(), cols, rows), (name, bytes));
    }

    let connection = connection(root);
    let registry = registry();
    let assets = discover_assets(&registry, &connection).await;

    for (format, cols, rows) in cases {
        let (name, fixture_bytes) = generated
            .get(&(format.to_string(), cols, rows))
            .expect("generated fixture");
        let asset = asset_named(&assets, name);
        eprintln!("[e24-b2json-a0] measuring {name}");

        set_json_direct(false);
        ingest_once(&connection, &registry, &asset).await;
        set_json_direct(true);
        ingest_once(&connection, &registry, &asset).await;
        let mut prev_snap = read_counter_snapshot(&metrics_out);

        let mut legacy_walls = Vec::with_capacity(FOCUSED_JSON_REPS);
        let mut direct_walls = Vec::with_capacity(FOCUSED_JSON_REPS);
        let zeros: BTreeMap<String, u64> = COUNTER_LABELS
            .iter()
            .map(|label| ((*label).to_string(), 0_u64))
            .collect();
        let mut legacy_deltas = zeros.clone();
        let mut direct_deltas = zeros.clone();

        for rep in 0..FOCUSED_JSON_REPS {
            let direct_first = (rep + cols + rows) % 2 == 0;
            for strategy in if direct_first {
                ["direct", "legacy"]
            } else {
                ["legacy", "direct"]
            } {
                let direct = strategy == "direct";
                set_json_direct(direct);
                let start = Instant::now();
                ingest_once(&connection, &registry, &asset).await;
                let wall =
                    u128::from(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX));
                let snap = read_counter_snapshot(&metrics_out);
                let mut ingest_delta = BTreeMap::new();
                for label in COUNTER_LABELS {
                    let current = snap.get(*label).copied().unwrap_or(0);
                    let previous = prev_snap.get(*label).copied().unwrap_or(0);
                    ingest_delta.insert((*label).to_string(), current.saturating_sub(previous));
                }
                prev_snap = snap;
                let target = if direct {
                    direct_walls.push(wall);
                    &mut direct_deltas
                } else {
                    legacy_walls.push(wall);
                    &mut legacy_deltas
                };
                for label in COUNTER_LABELS {
                    let add = ingest_delta.get(*label).copied().unwrap_or(0);
                    let slot = target.entry((*label).to_string()).or_insert(0);
                    *slot = slot.saturating_add(add);
                }
            }
        }

        print_json_ab_case(
            "legacy",
            format,
            cols,
            rows,
            *fixture_bytes,
            &legacy_walls,
            &legacy_deltas,
        );
        print_json_ab_case(
            "direct",
            format,
            cols,
            rows,
            *fixture_bytes,
            &direct_walls,
            &direct_deltas,
        );
        set_json_direct(false);
    }
}
