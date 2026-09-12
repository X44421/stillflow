//! Isolated E24-JSON-M1: `serde_json::Value` DOM vs schema-driven Visitor sink.
//! Input is already-framed in-memory JSON object bytes. No Arrow/Polars.

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const WARMUP: usize = 1;
const REPS: usize = 5;
const ROWS_NARROW: usize = 8_000;
const ROWS_WIDE: usize = 2_000;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        std::alloc::System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        std::alloc::System.dealloc(ptr, layout)
    }
}

fn snapshot_alloc() -> (u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn utf8_payload(col: usize, row: usize) -> String {
    format!("字段{col:03}-{row:05}-αβγ-データ-éè")
}

fn object_bytes(width: usize, row: usize) -> Vec<u8> {
    let mut object = Map::new();
    for col in 0..width {
        if col % 4 == 0 {
            object.insert(
                format!("col_{col:03}"),
                Value::from(row as i64 + col as i64),
            );
        } else {
            object.insert(format!("col_{col:03}"), Value::from(utf8_payload(col, row)));
        }
    }
    serde_json::to_vec(&Value::Object(object)).expect("fixture encode")
}

fn fixtures(width: usize, rows: usize) -> Vec<Vec<u8>> {
    (0..rows).map(|row| object_bytes(width, row)).collect()
}

#[derive(Clone, Copy)]
enum FieldKind {
    Int,
    Utf8,
}

fn schema(width: usize) -> Vec<(String, FieldKind)> {
    (0..width)
        .map(|col| {
            let kind = if col % 4 == 0 {
                FieldKind::Int
            } else {
                FieldKind::Utf8
            };
            (format!("col_{col:03}"), kind)
        })
        .collect()
}

fn checksum_value(value: &Value) -> u64 {
    match value {
        Value::Number(number) => number.as_i64().unwrap_or(0) as u64,
        Value::String(text) => text.len() as u64,
        _ => 0,
    }
}

fn parse_dom(bytes: &[u8], schema: &[(String, FieldKind)]) -> Result<(u64, usize), String> {
    let map: Map<String, Value> =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let mut checksum = 0_u64;
    let mut fields = 0_usize;
    for (name, kind) in schema {
        let value = map.get(name).ok_or_else(|| format!("missing {name}"))?;
        match kind {
            FieldKind::Int if value.is_i64() => {}
            FieldKind::Utf8 if value.is_string() => {}
            _ => return Err(format!("type mismatch {name}")),
        }
        checksum = checksum.wrapping_add(checksum_value(value));
        fields += 1;
    }
    Ok((checksum, fields))
}

struct Sink {
    checksum: u64,
    fields: usize,
}

struct RowSeed<'a> {
    lookup: &'a HashMap<&'a str, FieldKind>,
    sink: &'a mut Sink,
}

impl<'de> DeserializeSeed<'de> for RowSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RowVisitor {
            lookup: self.lookup,
            sink: self.sink,
        })
    }
}

struct RowVisitor<'a> {
    lookup: &'a HashMap<&'a str, FieldKind>,
    sink: &'a mut Sink,
}

impl<'de> Visitor<'de> for RowVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("json object")
    }

    fn visit_map<A>(self, mut access: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = access.next_key::<&str>()? {
            let kind = *self
                .lookup
                .get(key)
                .ok_or_else(|| de::Error::custom("unknown field"))?;
            match kind {
                FieldKind::Int => {
                    let value: i64 = access.next_value()?;
                    self.sink.checksum = self.sink.checksum.wrapping_add(value as u64);
                }
                FieldKind::Utf8 => {
                    let value: String = access.next_value()?;
                    self.sink.checksum = self.sink.checksum.wrapping_add(value.len() as u64);
                }
            }
            self.sink.fields += 1;
        }
        Ok(())
    }
}

fn parse_visitor(bytes: &[u8], lookup: &HashMap<&str, FieldKind>) -> Result<(u64, usize), String> {
    let mut sink = Sink {
        checksum: 0,
        fields: 0,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    RowSeed {
        lookup,
        sink: &mut sink,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok((sink.checksum, sink.fields))
}

fn median(samples: &[Duration]) -> Duration {
    let mut ordered = samples.to_vec();
    ordered.sort();
    ordered[ordered.len() / 2]
}

fn ms_list(samples: &[Duration]) -> Vec<f64> {
    samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect()
}

fn improvement(baseline: Duration, candidate: Duration) -> f64 {
    let base = baseline.as_secs_f64();
    if base == 0.0 {
        0.0
    } else {
        (base - candidate.as_secs_f64()) / base * 100.0
    }
}

fn run_shape(width: usize, rows: usize) {
    let schema = schema(width);
    let lookup: HashMap<&str, FieldKind> = schema
        .iter()
        .map(|(name, kind)| (name.as_str(), *kind))
        .collect();
    let rows_bytes = fixtures(width, rows);
    let mut expected = (0_u64, 0_usize);
    for row in &rows_bytes {
        let got = parse_dom(row, &schema).expect("dom oracle");
        let visitor = parse_visitor(row, &lookup).expect("visitor oracle");
        assert_eq!(got, visitor);
        expected.0 = expected.0.wrapping_add(got.0);
        expected.1 += got.1;
    }

    let measure_dom = || {
        let started = Instant::now();
        let before = snapshot_alloc();
        let mut checksum = 0_u64;
        let mut fields = 0_usize;
        for row in &rows_bytes {
            let (row_checksum, row_fields) = parse_dom(row, &schema).expect("parse");
            checksum = checksum.wrapping_add(row_checksum);
            fields += row_fields;
        }
        let after = snapshot_alloc();
        let result = (checksum, fields);
        black_box(result);
        (
            started.elapsed(),
            after.0 - before.0,
            after.1 - before.1,
            result,
        )
    };
    let measure_visitor = || {
        let started = Instant::now();
        let before = snapshot_alloc();
        let mut checksum = 0_u64;
        let mut fields = 0_usize;
        for row in &rows_bytes {
            let (row_checksum, row_fields) = parse_visitor(row, &lookup).expect("parse");
            checksum = checksum.wrapping_add(row_checksum);
            fields += row_fields;
        }
        let after = snapshot_alloc();
        let result = (checksum, fields);
        black_box(result);
        (
            started.elapsed(),
            after.0 - before.0,
            after.1 - before.1,
            result,
        )
    };

    let _ = measure_dom();
    let mut dom_samples = Vec::new();
    let mut dom_allocs = Vec::new();
    let mut dom_bytes = Vec::new();
    let mut dom_result = (0_u64, 0_usize);
    for _ in 0..REPS {
        let (elapsed, allocs, bytes, result) = measure_dom();
        dom_samples.push(elapsed);
        dom_allocs.push(allocs);
        dom_bytes.push(bytes);
        dom_result = result;
    }

    let _ = measure_visitor();
    let mut visitor_samples = Vec::new();
    let mut visitor_allocs = Vec::new();
    let mut visitor_bytes = Vec::new();
    let mut visitor_result = (0_u64, 0_usize);
    for _ in 0..REPS {
        let (elapsed, allocs, bytes, result) = measure_visitor();
        visitor_samples.push(elapsed);
        visitor_allocs.push(allocs);
        visitor_bytes.push(bytes);
        visitor_result = result;
    }

    assert_eq!(dom_result, expected);
    assert_eq!(visitor_result, expected);

    let wall_gain = improvement(median(&dom_samples), median(&visitor_samples));
    let mut ordered_dom_bytes = dom_bytes.clone();
    ordered_dom_bytes.sort_unstable();
    let mut ordered_vis_bytes = visitor_bytes.clone();
    ordered_vis_bytes.sort_unstable();
    let alloc_gain = if ordered_dom_bytes[2] == 0 {
        0.0
    } else {
        (ordered_dom_bytes[2] as f64 - ordered_vis_bytes[2] as f64) / ordered_dom_bytes[2] as f64
            * 100.0
    };
    let label = if wall_gain < 10.0 && alloc_gain < 30.0 {
        "STOP_VISITOR"
    } else if wall_gain <= 25.0 && alloc_gain <= 30.0 {
        "EVIDENCE_ONLY"
    } else {
        "PROMOTE_VISITOR_CANDIDATE"
    };

    eprintln!(
        "M1 width={width} rows={rows} checksum={} fields={} wall_gain={:.1}% alloc_byte_gain={:.1}% verdict={label}",
        expected.0, expected.1, wall_gain, alloc_gain
    );
    eprintln!("  dom_ms={:?}", ms_list(&dom_samples));
    eprintln!("  visitor_ms={:?}", ms_list(&visitor_samples));
    eprintln!("  dom_allocs={dom_allocs:?} bytes={dom_bytes:?}");
    eprintln!("  visitor_allocs={visitor_allocs:?} bytes={visitor_bytes:?}");
    eprintln!(
        "  median_ms dom={:.3} visitor={:.3}",
        median(&dom_samples).as_secs_f64() * 1000.0,
        median(&visitor_samples).as_secs_f64() * 1000.0
    );
}

#[test]
fn e24_json_m1_value_vs_visitor_microbench() {
    let _ = WARMUP;
    run_shape(10, ROWS_NARROW);
    run_shape(100, ROWS_WIDE);
}
