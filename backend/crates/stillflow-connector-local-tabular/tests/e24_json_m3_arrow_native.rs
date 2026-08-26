//! Isolated E24-JSON-M3: architecture ceiling of `arrow-json` 59 NDJSON → RecordBatch.
//! Does not touch PreparedReader, production read/json_stream, or default features.

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Map, Value};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const REPS: usize = 5;
const ROWS_NARROW: usize = 6_000;
const ROWS_WIDE: usize = 1_200;

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

fn snapshot() -> (u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn utf8_payload(col: usize, row: usize) -> String {
    format!("字段{col:03}-{row:05}-αβγ")
}

fn ndjson_and_schema(width: usize, rows: usize) -> (Vec<u8>, Arc<Schema>) {
    let mut bytes = Vec::new();
    for row in 0..rows {
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
        serde_json::to_writer(&mut bytes, &Value::Object(object)).expect("ndjson row");
        bytes.push(b'\n');
    }
    let fields = (0..width)
        .map(|col| {
            let name = format!("col_{col:03}");
            if col % 4 == 0 {
                Field::new(name, DataType::Int64, true)
            } else {
                Field::new(name, DataType::Utf8, true)
            }
        })
        .collect::<Vec<_>>();
    (bytes, Arc::new(Schema::new(fields)))
}

fn checksum_value_map(map: &Map<String, Value>) -> u64 {
    let mut checksum = 0_u64;
    for value in map.values() {
        checksum = checksum.wrapping_add(match value {
            Value::Number(number) => number.as_i64().unwrap_or(0) as u64,
            Value::String(text) => text.len() as u64,
            _ => 0,
        });
    }
    checksum
}

fn checksum_batch(batch: &RecordBatch) -> u64 {
    let mut checksum = 0_u64;
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(index);
        match field.data_type() {
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().expect("i64");
                for row in 0..array.len() {
                    if array.is_valid(row) {
                        checksum = checksum.wrapping_add(array.value(row) as u64);
                    }
                }
            }
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().expect("utf8");
                for row in 0..array.len() {
                    if array.is_valid(row) {
                        checksum = checksum.wrapping_add(array.value(row).len() as u64);
                    }
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    checksum
}

fn legacy_double_parse(ndjson: &[u8]) -> (u64, usize) {
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    for line in ndjson.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let first: Map<String, Value> = serde_json::from_slice(line).expect("first parse");
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &Value::Object(first.clone())).expect("re-encode");
        let second: Map<String, Value> = serde_json::from_slice(&encoded).expect("second parse");
        checksum = checksum.wrapping_add(checksum_value_map(&second));
        rows += 1;
    }
    (checksum, rows)
}

fn arrow_native(ndjson: &[u8], schema: Arc<Schema>) -> (u64, usize, usize, bool) {
    let mut reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(1024)
        .build(Cursor::new(ndjson))
        .expect("arrow-json reader");
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    let mut batches = 0_usize;
    let mut schema_ok = true;
    while let Some(batch) = reader.next() {
        let batch = batch.expect("record batch");
        schema_ok &= batch.schema().as_ref() == schema.as_ref();
        rows += batch.num_rows();
        batches += 1;
        checksum = checksum.wrapping_add(checksum_batch(&batch));
    }
    (checksum, rows, batches, schema_ok)
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

fn verdict(gain: f64) -> &'static str {
    if gain > 50.0 {
        "PROMOTE_ARROW_NATIVE_CANDIDATE ARCHITECTURE_LEVEL_SIGNAL"
    } else if gain > 25.0 {
        "PROMOTE_ARROW_NATIVE_CANDIDATE"
    } else if gain >= 10.0 {
        "EVIDENCE_ONLY"
    } else {
        "STOP_ARROW_NATIVE"
    }
}

fn run_shape(width: usize, rows: usize) {
    let (ndjson, schema) = ndjson_and_schema(width, rows);
    let (legacy_checksum, legacy_rows) = legacy_double_parse(&ndjson);
    let (arrow_checksum, arrow_rows, arrow_batches, schema_ok) =
        arrow_native(&ndjson, schema.clone());
    assert_eq!(legacy_rows, rows);
    assert_eq!(arrow_rows, rows);
    assert!(schema_ok, "schema equality");
    assert_eq!(legacy_checksum, arrow_checksum, "checksum parity");

    let measure_legacy = || {
        let before = snapshot();
        let started = Instant::now();
        let got = legacy_double_parse(&ndjson);
        let elapsed = started.elapsed();
        let after = snapshot();
        (elapsed, after.0 - before.0, after.1 - before.1, got)
    };
    let measure_arrow = || {
        let before = snapshot();
        let started = Instant::now();
        let got = arrow_native(&ndjson, schema.clone());
        let elapsed = started.elapsed();
        let after = snapshot();
        (elapsed, after.0 - before.0, after.1 - before.1, got)
    };

    let _ = measure_legacy();
    let mut legacy_s = Vec::new();
    let mut legacy_a = Vec::new();
    let mut legacy_b = Vec::new();
    for _ in 0..REPS {
        let (e, a, b, got) = measure_legacy();
        assert_eq!(got.0, legacy_checksum);
        legacy_s.push(e);
        legacy_a.push(a);
        legacy_b.push(b);
    }

    let _ = measure_arrow();
    let mut arrow_s = Vec::new();
    let mut arrow_a = Vec::new();
    let mut arrow_b = Vec::new();
    for _ in 0..REPS {
        let (e, a, b, got) = measure_arrow();
        assert_eq!(got.0, arrow_checksum);
        assert_eq!(got.1, rows);
        arrow_s.push(e);
        arrow_a.push(a);
        arrow_b.push(b);
    }

    let gain = improvement(median(&legacy_s), median(&arrow_s));
    eprintln!(
        "M3 width={width} rows={rows} batches={arrow_batches} checksum={legacy_checksum} schema_ok={schema_ok} gain={:.1}% verdict={} dep=arrow-json:59",
        gain,
        verdict(gain)
    );
    eprintln!("  legacy_ms={:?}", ms_list(&legacy_s));
    eprintln!("  arrow_ms={:?}", ms_list(&arrow_s));
    eprintln!("  legacy_allocs={legacy_a:?} bytes={legacy_b:?}");
    eprintln!("  arrow_allocs={arrow_a:?} bytes={arrow_b:?}");
    eprintln!(
        "  median_ms legacy={:.3} arrow={:.3}",
        median(&legacy_s).as_secs_f64() * 1000.0,
        median(&arrow_s).as_secs_f64() * 1000.0
    );
}

#[test]
fn e24_json_m3_arrow_native_microbench() {
    run_shape(10, ROWS_NARROW);
    run_shape(100, ROWS_WIDE);
}
