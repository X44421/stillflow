//! E24-JSON-A2-PROD evidence harness for issue #158.
//!
//! Ignored by default (never runs under a plain `cargo test`); driven by the
//! dispatch evidence scripts under `.dispatch-158-e24/`. Each invocation
//! generates-or-reuses one deterministic fixture, drains it exactly once
//! through the public connector surface (`ConnectorRegistry::read_batches`),
//! and prints one machine-readable sample line:
//!
//! ```text
//! E24SAMPLE cell=<cell> mode=<off|on> batch=<n> rows=<n> envelopes=<n> elapsed_ns=<n> rss_start_kb=<n> rss_end_kb=<n> vmhwm_kb=<n>
//! ```
//!
//! plus one `E24FIXTURE <name> <bytes>` line per fixture it created.
//!
//! Determinism: every fixture byte is a pure function of (row, column) via an
//! integer LCG — no RNG crate, no timestamps, no filesystem order. The driver
//! records the fixture SHA-256 separately. Memory sampling reads
//! `/proc/self/status` (`VmRSS`/`VmHWM`); the fresh-process-per-sample design
//! makes each sample's `VmHWM` that sample's true peak.

use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::Array;
use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, DiscoverRequest, InspectRequest, ReadRequest,
    RequestContext, SourceConnection,
};

/// O1-J1 (#296): the projected-row routing is a runtime connection-config
/// key. The harness selects its arm via `E24_JSON_A2_MODE=on|off`
/// (default `off`, today's production default) and records the arm in every
/// emitted line, so both arms of the ≥30% ingest comparison run from one
/// binary at one head.
fn mode() -> &'static str {
    match std::env::var("E24_JSON_A2_MODE").as_deref() {
        Ok("on") => "on",
        _ => "off",
    }
}

const PRIMARY_ROWS: usize = 100_000;
const PRIMARY_COLS: usize = 100;
const NARROW_ROWS: usize = 100_000;
const NARROW_COLS: usize = 10;
const NESTED_ROWS: usize = 20_000;
const ESCAPE_ROWS: usize = 50_000;

fn fixtures_dir() -> std::path::PathBuf {
    std::env::var("E24_EVIDENCE_FIXTURES")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/e24-158-fixtures"))
}

/// Column type cycle shared by the wide/narrow fixtures: Int64, Utf8, Float64,
/// Boolean, repeating. Names are `c00..cNN`.
fn column_kind(index: usize) -> &'static str {
    match index % 4 {
        0 => "int",
        1 => "utf8",
        2 => "float",
        _ => "bool",
    }
}

fn column_name(index: usize) -> String {
    format!("c{index:0>3}")
}

fn write_scalar(row: usize, index: usize) -> String {
    match column_kind(index) {
        "int" => ((row * 31 + index * 7) % 90_000 + 1).to_string(),
        "float" => format!(
            "{:.3}",
            ((row * 13 + index * 5) % 100_000) as f64 / 8.0 + 0.125
        ),
        "bool" => {
            if (row + index) % 2 == 0 {
                "true"
            } else {
                "false"
            }
        }
        .to_string(),
        _ => format!("\"v{}x{}\"", row % 9_973, index),
    }
}

/// Deterministic LCG for the escape fixtures (values never leave the file).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

fn escape_unit(seed: &mut Lcg, sink: &mut String) {
    // High escape density: quotes, backslashes, \n and \t spellings, and
    // non-ASCII text, with a few plain digits for realism. These are literal
    // file bytes; the JSON escapes are written explicitly.
    match seed.next() % 6 {
        0 => sink.push_str("\\\"q"),
        1 => sink.push_str("\\\\p"),
        2 => sink.push_str("\\n"),
        3 => sink.push_str("\\t"),
        4 => sink.push_str("é\\u00e9"),
        _ => sink.push_str(&format!("{}", seed.next() % 100)),
    }
}

fn generate_wide(path: &std::path::Path, rows: usize, cols: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        out.write_all(b"{")?;
        for index in 0..cols {
            if index > 0 {
                out.write_all(b",")?;
            }
            out.write_all(format!("\"{}\":", column_name(index)).as_bytes())?;
            out.write_all(write_scalar(row, index).as_bytes())?;
        }
        out.write_all(b"}\n")?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

fn generate_nested(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        out.write_all(
            format!(
                "{{\"m\":{},\"li\":[{},{},{}],\"st\":{{\"x\":{},\"y\":\"t{}\"}}}}\n",
                row,
                row % 7,
                (row * 3) % 11,
                row % 5,
                row % 13,
                row % 101,
            )
            .as_bytes(),
        )?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

fn generate_escape(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        let mut short = String::with_capacity(160);
        let mut seed =
            Lcg(0x9E37_79B9_7F4A_7C15 ^ (row as u64).wrapping_mul(0xFF51_AFD7_ED55_8CCD));
        for _ in 0..20 {
            escape_unit(&mut seed, &mut short);
        }
        let mut long = String::with_capacity(720);
        for _ in 0..90 {
            escape_unit(&mut seed, &mut long);
        }
        out.write_all(format!("{{\"s\":\"{short}\",\"long\":\"{long}\"}}\n").as_bytes())?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

fn ensure_fixture(name: &str) -> std::path::PathBuf {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(name);
    let bytes = match name {
        "e24_narrow_10x100k.ndjson" => {
            generate_wide(&path, NARROW_ROWS, NARROW_COLS).expect("generate narrow fixture")
        }
        "e24_primary_100x100k.ndjson" => {
            generate_wide(&path, PRIMARY_ROWS, PRIMARY_COLS).expect("generate primary fixture")
        }
        "e24_nested_20k.ndjson" => generate_nested(&path, NESTED_ROWS).expect("generate nested"),
        "e24_escape_50k.ndjson" => generate_escape(&path, ESCAPE_ROWS).expect("generate escape"),
        other => panic!("unknown fixture {other}"),
    };
    println!("E24FIXTURE {name} {bytes}");
    path
}

fn kb_of(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("proc status field")
        .parse()
        .expect("proc status number")
}

fn memory_snapshot() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    (kb_of(&status, "VmRSS:"), kb_of(&status, "VmHWM:"))
}

/// Drains `fixture` once through the public read surface with `projection`
/// selecting field indices (None = full projection) and prints one sample.
#[allow(clippy::too_many_arguments)]
fn drain_and_sample(
    cell: &str,
    fixture: &std::path::Path,
    projection: Option<&[usize]>,
    batch_size: usize,
) {
    let (rss_start, _) = memory_snapshot();
    let started = Instant::now();
    let mode = mode();

    let mut config = serde_json::json!({
        "allowedRoots": [fixture.parent().and_then(|p| p.to_str()).expect("fixture root")],
        "schemaInference": { "maxRows": 1, "maxBytes": 8388608 }
    });
    if mode == "on" {
        config["jsonDirectProjectedWriter"] = serde_json::Value::Bool(true);
    }
    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "fixtures",
        config,
        CredentialRef::new("cred://local/e24-evidence").expect("credential reference"),
    )
    .expect("connection");
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    let assets = futures::executor::block_on(registry.discover(
        &connection,
        DiscoverRequest {
            context: RequestContext::default(),
            parent_path: None,
        },
    ))
    .expect("discover");
    let name = fixture
        .file_name()
        .and_then(|name| name.to_str())
        .expect("fixture name");
    let asset = assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("{name} discovered"))
        .clone();
    let metadata = futures::executor::block_on(registry.inspect(
        &connection,
        InspectRequest {
            context: RequestContext::default(),
            asset: asset.clone(),
        },
    ))
    .expect("inspect");
    let projection_ids: Option<Vec<ColumnId>> = projection.map(|indices| {
        indices
            .iter()
            .map(|&i| metadata.schema.fields[i].id)
            .collect()
    });
    let mut request = ReadRequest::new(asset, batch_size);
    request.projection = projection_ids;
    let mut stream = futures::executor::block_on(registry.read_batches(&connection, request))
        .expect("open read stream");

    let mut rows = 0_usize;
    let mut envelopes = 0_usize;
    while let Some(item) = futures::executor::block_on(stream.next()) {
        match item {
            Ok(envelope) => {
                rows += envelope.row_count();
                envelopes += 1;
                // Touch every payload column so the decode cost is honest.
                let _ = envelope.payload().columns().len();
                let _ = envelope.payload().column(0).len();
            }
            Err(error) => panic!("{cell}: stream error {error}"),
        }
    }
    let elapsed = started.elapsed();
    let (rss_end, vmhwm) = memory_snapshot();
    println!(
        "E24SAMPLE cell={cell} mode={mode} batch={batch_size} rows={rows} envelopes={envelopes} elapsed_ns={} rss_start_kb={rss_start} rss_end_kb={rss_end} vmhwm_kb={vmhwm}",
        elapsed.as_nanos()
    );
}

const SPARSE_PROJECTION: [usize; 5] = [0, 17, 42, 73, 99];

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_cell_narrow() {
    let fixture = ensure_fixture("e24_narrow_10x100k.ndjson");
    drain_and_sample("narrow", &fixture, None, 4_096);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_cell_primary() {
    let fixture = ensure_fixture("e24_primary_100x100k.ndjson");
    drain_and_sample("primary", &fixture, None, 4_096);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_cell_sparse() {
    let fixture = ensure_fixture("e24_primary_100x100k.ndjson");
    drain_and_sample("sparse", &fixture, Some(&SPARSE_PROJECTION), 4_096);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_cell_nested() {
    let fixture = ensure_fixture("e24_nested_20k.ndjson");
    drain_and_sample("nested", &fixture, None, 4_096);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_cell_escape() {
    let fixture = ensure_fixture("e24_escape_50k.ndjson");
    drain_and_sample("escape", &fixture, None, 4_096);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_mem_primary_256() {
    let fixture = ensure_fixture("e24_primary_100x100k.ndjson");
    drain_and_sample("mem256", &fixture, None, 256);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_mem_primary_1024() {
    let fixture = ensure_fixture("e24_primary_100x100k.ndjson");
    drain_and_sample("mem1024", &fixture, None, 1_024);
}

#[ignore = "E24 evidence harness: driven by .dispatch-158-e24 scripts"]
#[test]
fn e24_mem_primary_4096() {
    let fixture = ensure_fixture("e24_primary_100x100k.ndjson");
    drain_and_sample("mem4096", &fixture, None, 4_096);
}
