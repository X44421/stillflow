//! Isolated E24-JSON-M2: projected-object re-encode + second parse tax.

use serde_json::{Map, Value};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const REPS: usize = 5;
const ROWS_NARROW: usize = 6_000;
const ROWS_WIDE: usize = 1_500;

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
    format!("字段{col:03}-{row:05}-αβγ-データ")
}

fn framed_rows(width: usize, rows: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|row| {
            let mut object = Map::new();
            for col in 0..width {
                if col % 4 == 0 {
                    object.insert(format!("col_{col:03}"), Value::from(row as i64 + col as i64));
                } else {
                    object.insert(format!("col_{col:03}"), Value::from(utf8_payload(col, row)));
                }
            }
            serde_json::to_vec(&Value::Object(object)).expect("fixture")
        })
        .collect()
}

fn selected_names(width: usize) -> Vec<String> {
    (0..width)
        .filter(|col| *col % 3 != 2)
        .map(|col| format!("col_{col:03}"))
        .collect()
}

fn project(map: &Map<String, Value>, selected: &[String]) -> Map<String, Value> {
    let mut out = Map::new();
    for name in selected {
        if let Some(value) = map.get(name) {
            out.insert(name.clone(), value.clone());
        }
    }
    out
}

fn checksum_map(map: &Map<String, Value>) -> u64 {
    let mut checksum = 0_u64;
    for (name, value) in map {
        checksum = checksum.wrapping_add(name.len() as u64);
        checksum = checksum.wrapping_add(match value {
            Value::Number(number) => number.as_i64().unwrap_or(0) as u64,
            Value::String(text) => text.len() as u64,
            _ => 0,
        });
    }
    checksum
}

fn path_double(rows: &[Vec<u8>], selected: &[String]) -> u64 {
    let mut checksum = 0_u64;
    for row in rows {
        let parsed: Map<String, Value> = serde_json::from_slice(row).expect("first parse");
        let projected = project(&parsed, selected);
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &Value::Object(projected)).expect("re-encode");
        let second: Map<String, Value> = serde_json::from_slice(&encoded).expect("second parse");
        checksum = checksum.wrapping_add(checksum_map(&second));
    }
    checksum
}

fn path_single(rows: &[Vec<u8>], selected: &[String]) -> u64 {
    let mut checksum = 0_u64;
    for row in rows {
        let parsed: Map<String, Value> = serde_json::from_slice(row).expect("parse");
        let projected = project(&parsed, selected);
        checksum = checksum.wrapping_add(checksum_map(&projected));
    }
    checksum
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
        "PROMOTE_SINGLE_PARSE_ARCHITECTURE ARCHITECTURE_LEVEL_SIGNAL"
    } else if gain > 25.0 {
        "PROMOTE_SINGLE_PARSE_ARCHITECTURE"
    } else if gain >= 10.0 {
        "EVIDENCE_ONLY"
    } else {
        "STOP_SECOND_PARSE_HYPOTHESIS"
    }
}

fn run_shape(width: usize, rows: usize) {
    let framed = framed_rows(width, rows);
    let selected = selected_names(width);
    let expected = path_single(&framed, &selected);
    assert_eq!(expected, path_double(&framed, &selected));

    let measure = |work: fn(&[Vec<u8>], &[String]) -> u64| {
        let before = snapshot();
        let started = Instant::now();
        let checksum = work(&framed, &selected);
        let elapsed = started.elapsed();
        let after = snapshot();
        black_box(checksum);
        (elapsed, after.0 - before.0, after.1 - before.1, checksum)
    };

    let _ = measure(path_double);
    let mut double_s = Vec::new();
    let mut double_a = Vec::new();
    let mut double_b = Vec::new();
    let mut double_c = 0;
    for _ in 0..REPS {
        let (e, a, b, c) = measure(path_double);
        double_s.push(e);
        double_a.push(a);
        double_b.push(b);
        double_c = c;
    }

    let _ = measure(path_single);
    let mut single_s = Vec::new();
    let mut single_a = Vec::new();
    let mut single_b = Vec::new();
    let mut single_c = 0;
    for _ in 0..REPS {
        let (e, a, b, c) = measure(path_single);
        single_s.push(e);
        single_a.push(a);
        single_b.push(b);
        single_c = c;
    }

    assert_eq!(double_c, expected);
    assert_eq!(single_c, expected);
    let gain = improvement(median(&double_s), median(&single_s));
    eprintln!(
        "M2 width={width} rows={rows} selected={} checksum={expected} gain={:.1}% verdict={}",
        selected.len(),
        gain,
        verdict(gain)
    );
    eprintln!("  double_ms={:?}", ms_list(&double_s));
    eprintln!("  single_ms={:?}", ms_list(&single_s));
    eprintln!("  double_allocs={double_a:?} bytes={double_b:?}");
    eprintln!("  single_allocs={single_a:?} bytes={single_b:?}");
    eprintln!(
        "  median_ms double={:.3} single={:.3}",
        median(&double_s).as_secs_f64() * 1000.0,
        median(&single_s).as_secs_f64() * 1000.0
    );
}

#[test]
fn e24_json_m2_second_parse_tax_microbench() {
    run_shape(10, ROWS_NARROW);
    run_shape(100, ROWS_WIDE);
}
