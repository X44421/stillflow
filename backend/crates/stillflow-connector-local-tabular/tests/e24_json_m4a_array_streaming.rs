//! Isolated E24-JSON-M4A: JSON-array balanced framing vs `SeqAccess` traversal.

use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const REPS: usize = 5;
const ROWS_NARROW: usize = 4_000;
const ROWS_WIDE: usize = 800;

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

fn utf8_payload(col: usize, row: usize) -> Value {
    if col % 5 == 0 {
        Value::Array(vec![
            Value::from(format!("nested-{row}-{col}")),
            Value::from("quote\\\"slash\\\\"),
        ])
    } else if col % 4 == 0 {
        Value::from(row as i64 + col as i64)
    } else {
        Value::from(format!("字段{col:03}-{row:05}-αβγ-\"esc\""))
    }
}

fn array_bytes(width: usize, rows: usize) -> Vec<u8> {
    let objects: Vec<Value> = (0..rows)
        .map(|row| {
            let mut object = Map::new();
            for col in 0..width {
                object.insert(format!("col_{col:03}"), utf8_payload(col, row));
            }
            Value::Object(object)
        })
        .collect();
    serde_json::to_vec(&Value::Array(objects)).expect("array fixture")
}

fn checksum_map(map: &Map<String, Value>) -> u64 {
    let encoded = serde_json::to_vec(&Value::Object(map.clone())).expect("stable");
    encoded.len() as u64
        ^ encoded
            .iter()
            .enumerate()
            .map(|(index, byte)| (*byte as u64).wrapping_mul(index as u64 + 3))
            .fold(0_u64, u64::wrapping_add)
}

fn read_balanced_object(bytes: &[u8], mut pos: usize) -> Result<(Vec<u8>, usize), &'static str> {
    if bytes.get(pos) != Some(&b'{') {
        return Err("every JSON array element must be an object");
    }
    let mut raw = vec![b'{'];
    pos += 1;
    let mut depth = 1_usize;
    let mut in_string = false;
    let mut escaped = false;
    while depth > 0 {
        let Some(&byte) = bytes.get(pos) else {
            return Err("JSON object ended before its closing brace");
        };
        pos += 1;
        raw.push(byte);
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth -= 1,
            _ => {}
        }
    }
    Ok((raw, pos))
}

fn skip_ws(bytes: &[u8], mut pos: usize) -> usize {
    while matches!(bytes.get(pos), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        pos += 1;
    }
    pos
}

fn balanced_frame(bytes: &[u8]) -> Result<(u64, usize), &'static str> {
    let mut pos = skip_ws(bytes, 0);
    if bytes.get(pos) != Some(&b'[') {
        return Err("JSON source must be one top-level array");
    }
    pos += 1;
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    let mut after_comma = false;
    loop {
        pos = skip_ws(bytes, pos);
        if bytes.get(pos) == Some(&b']') {
            if after_comma {
                return Err("JSON array must not contain a trailing comma");
            }
            pos += 1;
            pos = skip_ws(bytes, pos);
            if pos != bytes.len() {
                return Err("JSON contains data after the top-level array");
            }
            return Ok((checksum, rows));
        }
        if pos >= bytes.len() {
            return Err("JSON array ended before its closing bracket");
        }
        let (raw, next) = read_balanced_object(bytes, pos)?;
        pos = next;
        let parsed: Map<String, Value> =
            serde_json::from_slice(&raw).map_err(|_| "malformed object")?;
        checksum = checksum.wrapping_add(checksum_map(&parsed));
        rows += 1;
        pos = skip_ws(bytes, pos);
        match bytes.get(pos) {
            Some(b',') => {
                after_comma = true;
                pos += 1;
            }
            Some(b']') => after_comma = false,
            None => return Err("JSON array ended before its separator or closing bracket"),
            Some(_) => return Err("JSON array elements must be separated by commas"),
        }
    }
}

struct ArraySink {
    checksum: u64,
    rows: usize,
}

struct ArraySeed<'a> {
    sink: &'a mut ArraySink,
}

impl<'de> DeserializeSeed<'de> for ArraySeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayVisitor { sink: self.sink })
    }
}

struct ArrayVisitor<'a> {
    sink: &'a mut ArraySink,
}

impl<'de> Visitor<'de> for ArrayVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("json array")
    }

    fn visit_seq<A>(self, mut access: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(map) = access.next_element::<Map<String, Value>>()? {
            self.sink.checksum = self.sink.checksum.wrapping_add(checksum_map(&map));
            self.sink.rows += 1;
        }
        Ok(())
    }
}

fn seq_access(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut sink = ArraySink {
        checksum: 0,
        rows: 0,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    ArraySeed { sink: &mut sink }
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok((sink.checksum, sink.rows))
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
        "PROMOTE_ARRAY_STREAMING_CANDIDATE ARCHITECTURE_LEVEL_SIGNAL"
    } else if gain > 25.0 {
        "PROMOTE_ARRAY_STREAMING_CANDIDATE"
    } else if gain >= 10.0 {
        "EVIDENCE_ONLY"
    } else {
        "STOP_ARRAY_STREAMING"
    }
}

fn correctness_set() {
    let ok = br#"[{"a":1},{"a":"x\"y"},{"a":[1,{"k":"v"}]}]"#;
    assert_eq!(
        balanced_frame(ok).expect("ok balanced"),
        seq_access(ok).expect("ok seq")
    );
    assert!(balanced_frame(br#"[{"a":1},]"#).is_err());
    assert!(seq_access(br#"[{"a":1},]"#).is_err());
    assert!(balanced_frame(br#"[{"a":1}"#).is_err());
    assert!(seq_access(br#"[{"a":1}"#).is_err());
    assert!(balanced_frame(br#"[{"a":1}]{"extra":true}"#).is_err());
    assert!(seq_access(br#"[{"a":1}]{"extra":true}"#).is_err());
}

fn run_shape(width: usize, rows: usize) {
    let bytes = array_bytes(width, rows);
    let expected = balanced_frame(&bytes).expect("balanced oracle");
    assert_eq!(expected, seq_access(&bytes).expect("seq oracle"));

    let measure_b = || {
        let before = snapshot();
        let started = Instant::now();
        let got = balanced_frame(&bytes).expect("balanced");
        let elapsed = started.elapsed();
        let after = snapshot();
        (elapsed, after.0 - before.0, after.1 - before.1, got)
    };
    let measure_s = || {
        let before = snapshot();
        let started = Instant::now();
        let got = seq_access(&bytes).expect("seq");
        let elapsed = started.elapsed();
        let after = snapshot();
        (elapsed, after.0 - before.0, after.1 - before.1, got)
    };

    let _ = measure_b();
    let mut b_s = Vec::new();
    let mut b_a = Vec::new();
    let mut b_b = Vec::new();
    for _ in 0..REPS {
        let (e, a, b, got) = measure_b();
        assert_eq!(got, expected);
        b_s.push(e);
        b_a.push(a);
        b_b.push(b);
    }
    let _ = measure_s();
    let mut s_s = Vec::new();
    let mut s_a = Vec::new();
    let mut s_b = Vec::new();
    for _ in 0..REPS {
        let (e, a, b, got) = measure_s();
        assert_eq!(got, expected);
        s_s.push(e);
        s_a.push(a);
        s_b.push(b);
    }

    let gain = improvement(median(&b_s), median(&s_s));
    eprintln!(
        "M4A width={width} rows={rows} bytes={} checksum={} gain={:.1}% verdict={}",
        bytes.len(),
        expected.0,
        gain,
        verdict(gain)
    );
    eprintln!("  balanced_ms={:?}", ms_list(&b_s));
    eprintln!("  seq_ms={:?}", ms_list(&s_s));
    eprintln!("  balanced_allocs={b_a:?} bytes={b_b:?}");
    eprintln!("  seq_allocs={s_a:?} bytes={s_b:?}");
    eprintln!(
        "  median_ms balanced={:.3} seq={:.3}",
        median(&b_s).as_secs_f64() * 1000.0,
        median(&s_s).as_secs_f64() * 1000.0
    );
}

#[test]
fn e24_json_m4a_array_streaming_microbench() {
    correctness_set();
    run_shape(10, ROWS_NARROW);
    run_shape(100, ROWS_WIDE);
}
