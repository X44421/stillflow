//! Isolated E24-JSON-M4N: NDJSON newline framing and per-row copy cost.
//! Does not parse JSON, validate schema, or touch production `json_stream.rs`.

use std::io::{BufRead, Cursor};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const REPS: usize = 5;

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

fn ndjson_payload(short_rows: usize, wide_rows: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for row in 0..short_rows {
        out.extend_from_slice(format!("{{\"id\":{row}}}\n").as_bytes());
    }
    let wide = "x".repeat(256);
    for row in 0..wide_rows {
        out.extend_from_slice(format!("{{\"id\":{row},\"blob\":\"{wide}\"}}\n").as_bytes());
    }
    out
}

fn strip_line(mut line: Vec<u8>) -> Option<Vec<u8>> {
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    if line.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        Some(line)
    }
}

fn checksum_owned(line: &[u8]) -> u64 {
    line.len() as u64
        ^ line
            .iter()
            .enumerate()
            .map(|(index, byte)| (*byte as u64).wrapping_mul(index as u64 + 1))
            .fold(0_u64, u64::wrapping_add)
}

/// Current-style buffered scan + per-row owned `Vec<u8>` copy (mirrors `next_line_object`).
fn copy_frame(bytes: &[u8]) -> (u64, usize, usize) {
    let mut reader = Cursor::new(bytes);
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    let mut copied = 0_usize;
    loop {
        let mut line = Vec::new();
        loop {
            let available = reader.fill_buf().expect("fill");
            if available.is_empty() {
                break;
            }
            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            let ended = available.get(consumed - 1) == Some(&b'\n');
            let decoded = &available[..consumed];
            line.extend_from_slice(decoded);
            copied += decoded.len();
            reader.consume(consumed);
            if ended {
                break;
            }
        }
        if line.is_empty() {
            break;
        }
        if let Some(line) = strip_line(line) {
            checksum = checksum.wrapping_add(checksum_owned(&line));
            rows += 1;
        }
    }
    (checksum, rows, copied)
}

fn slice_frame(bytes: &[u8]) -> (u64, usize, usize) {
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    let copied = 0_usize;
    let mut start = 0_usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            let mut line = &bytes[start..index];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len().saturating_sub(1)];
            }
            if !line.iter().all(u8::is_ascii_whitespace) {
                checksum = checksum.wrapping_add(checksum_owned(line));
                rows += 1;
            }
            start = index + 1;
        }
    }
    if start < bytes.len() {
        let mut line = &bytes[start..];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            checksum = checksum.wrapping_add(checksum_owned(line));
            rows += 1;
        }
    }
    (checksum, rows, copied)
}

fn memchr_frame(bytes: &[u8]) -> (u64, usize, usize) {
    let mut checksum = 0_u64;
    let mut rows = 0_usize;
    let mut start = 0_usize;
    for index in memchr::memchr_iter(b'\n', bytes) {
        let mut line = &bytes[start..index];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len().saturating_sub(1)];
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            checksum = checksum.wrapping_add(checksum_owned(line));
            rows += 1;
        }
        start = index + 1;
    }
    if start < bytes.len() {
        let mut line = &bytes[start..];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            checksum = checksum.wrapping_add(checksum_owned(line));
            rows += 1;
        }
    }
    (checksum, rows, 0)
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

fn throughput_mbs(bytes: usize, elapsed: Duration) -> f64 {
    if elapsed.as_secs_f64() == 0.0 {
        0.0
    } else {
        (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    }
}

fn verdict(gain: f64) -> &'static str {
    if gain > 25.0 {
        "PROMOTE_NDJSON_FRAMING_CANDIDATE"
    } else if gain >= 10.0 {
        "EVIDENCE_ONLY"
    } else {
        "STOP_NDJSON_FRAMING"
    }
}

fn bench<F>(
    bytes: &[u8],
    expected: (u64, usize),
    mut work: F,
) -> (Vec<Duration>, Vec<u64>, Vec<u64>)
where
    F: FnMut(&[u8]) -> (u64, usize, usize),
{
    let _ = work(bytes);
    let mut samples = Vec::new();
    let mut allocs = Vec::new();
    let mut abytes = Vec::new();
    for _ in 0..REPS {
        let before = snapshot();
        let started = Instant::now();
        let got = work(bytes);
        samples.push(started.elapsed());
        let after = snapshot();
        assert_eq!((got.0, got.1), expected);
        allocs.push(after.0 - before.0);
        abytes.push(after.1 - before.1);
    }
    (samples, allocs, abytes)
}

#[test]
fn e24_json_m4n_ndjson_framing_microbench() {
    let payload = ndjson_payload(80_000, 4_000);
    let expected = {
        let copy = copy_frame(&payload);
        let slice = slice_frame(&payload);
        let mem = memchr_frame(&payload);
        assert_eq!((copy.0, copy.1), (slice.0, slice.1));
        assert_eq!((copy.0, copy.1), (mem.0, mem.1));
        (copy.0, copy.1)
    };

    let (copy_s, copy_a, copy_b) = bench(&payload, expected, copy_frame);
    let (slice_s, slice_a, slice_b) = bench(&payload, expected, slice_frame);
    let (mem_s, mem_a, mem_b) = bench(&payload, expected, memchr_frame);

    let copy_med = median(&copy_s);
    let slice_med = median(&slice_s);
    let mem_med = median(&mem_s);
    let slice_gain = improvement(copy_med, slice_med);
    let mem_gain = improvement(copy_med, mem_med);
    let best_gain = slice_gain.max(mem_gain);

    eprintln!(
        "M4N bytes={} rows={} checksum={} slice_gain={:.1}% memchr_gain={:.1}% verdict={} dep=memchr:2 (experiment-only)",
        payload.len(),
        expected.1,
        expected.0,
        slice_gain,
        mem_gain,
        verdict(best_gain)
    );
    eprintln!("  copy_ms={:?}", ms_list(&copy_s));
    eprintln!("  slice_ms={:?}", ms_list(&slice_s));
    eprintln!("  memchr_ms={:?}", ms_list(&mem_s));
    eprintln!("  copy_allocs={copy_a:?} bytes={copy_b:?}");
    eprintln!("  slice_allocs={slice_a:?} bytes={slice_b:?}");
    eprintln!("  memchr_allocs={mem_a:?} bytes={mem_b:?}");
    eprintln!(
        "  median_ms copy={:.3} slice={:.3} memchr={:.3} copy_MBps={:.1} slice_MBps={:.1} memchr_MBps={:.1} rows_per_s_copy={:.0}",
        copy_med.as_secs_f64() * 1000.0,
        slice_med.as_secs_f64() * 1000.0,
        mem_med.as_secs_f64() * 1000.0,
        throughput_mbs(payload.len(), copy_med),
        throughput_mbs(payload.len(), slice_med),
        throughput_mbs(payload.len(), mem_med),
        expected.1 as f64 / copy_med.as_secs_f64()
    );
}
