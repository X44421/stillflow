//! Isolated E24-JSON-M0 microbench: linear schema lookup vs indexed lookup.
//! Does not parse JSON and does not touch production ingestion.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const WARMUP: usize = 1;
const REPS: usize = 5;
const ROWS_NARROW: usize = 40_000;
const ROWS_WIDE: usize = 8_000;

struct Field {
    name: String,
}

fn schema(width: usize) -> Vec<Field> {
    (0..width)
        .map(|index| Field {
            name: format!("col_{index:03}"),
        })
        .collect()
}

fn probes<'a>(fields: &'a [Field], rows: usize) -> Vec<&'a str> {
    let mut names = Vec::with_capacity(rows * fields.len());
    for row in 0..rows {
        let rotate = row % fields.len();
        for offset in 0..fields.len() {
            names.push(fields[(rotate + offset) % fields.len()].name.as_str());
        }
    }
    names
}

fn linear_find(fields: &[Field], name: &str) -> Option<usize> {
    fields.iter().position(|field| field.name == name)
}

fn indexed_lookup(map: &HashMap<&str, usize>, name: &str) -> Option<usize> {
    map.get(name).copied()
}

fn known_key(index: usize) -> usize {
    index
}

fn checksum_linear(fields: &[Field], probes: &[&str]) -> u64 {
    let mut checksum = 0_u64;
    for name in probes {
        checksum = checksum.wrapping_add(linear_find(fields, name).expect("known field") as u64);
    }
    checksum
}

fn checksum_indexed(map: &HashMap<&str, usize>, probes: &[&str]) -> u64 {
    let mut checksum = 0_u64;
    for name in probes {
        checksum = checksum.wrapping_add(indexed_lookup(map, name).expect("known field") as u64);
    }
    checksum
}

fn checksum_known(indices: &[usize]) -> u64 {
    let mut checksum = 0_u64;
    for index in indices {
        checksum = checksum.wrapping_add(known_key(*index) as u64);
    }
    checksum
}

fn median(samples: &[Duration]) -> Duration {
    let mut ordered = samples.to_vec();
    ordered.sort();
    ordered[ordered.len() / 2]
}

fn time_reps<T>(warmup: usize, reps: usize, mut work: impl FnMut() -> T) -> (Vec<Duration>, T) {
    let mut last = work();
    for _ in 1..warmup {
        last = work();
    }
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        last = work();
        samples.push(started.elapsed());
        black_box(&last);
    }
    (samples, last)
}

fn improvement(baseline: Duration, candidate: Duration) -> f64 {
    let base = baseline.as_secs_f64();
    if base == 0.0 {
        return 0.0;
    }
    (base - candidate.as_secs_f64()) / base * 100.0
}

fn verdict(gain: f64) -> &'static str {
    if gain < 10.0 {
        "STOP_LOOKUP"
    } else if gain <= 25.0 {
        "EVIDENCE_ONLY"
    } else {
        "PROMOTE_LOOKUP_CANDIDATE"
    }
}

fn run_shape(width: usize, rows: usize) {
    let fields = schema(width);
    let names = probes(&fields, rows);
    let map: HashMap<&str, usize> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name.as_str(), index))
        .collect();
    let known_indices: Vec<usize> = names
        .iter()
        .map(|name| *map.get(name).expect("probe in schema"))
        .collect();

    let linear_oracle = checksum_linear(&fields, &names);
    let indexed_oracle = checksum_indexed(&map, &names);
    let known_oracle = checksum_known(&known_indices);
    assert_eq!(linear_oracle, indexed_oracle);
    assert_eq!(linear_oracle, known_oracle);

    let (linear_samples, linear_checksum) = time_reps(WARMUP, REPS, || checksum_linear(&fields, &names));
    let (indexed_samples, indexed_checksum) = time_reps(WARMUP, REPS, || checksum_indexed(&map, &names));
    let (known_samples, known_checksum) = time_reps(WARMUP, REPS, || checksum_known(&known_indices));
    assert_eq!(linear_checksum, linear_oracle);
    assert_eq!(indexed_checksum, linear_oracle);
    assert_eq!(known_checksum, linear_oracle);

    let linear_median = median(&linear_samples);
    let indexed_median = median(&indexed_samples);
    let known_median = median(&known_samples);
    let indexed_gain = improvement(linear_median, indexed_median);

    eprintln!(
        "M0 width={width} rows={rows} probes={} checksum={linear_oracle}",
        names.len()
    );
    eprintln!("  linear_ms={:?}", linear_samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect::<Vec<_>>());
    eprintln!("  indexed_ms={:?}", indexed_samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect::<Vec<_>>());
    eprintln!("  known_key_ms={:?}", known_samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect::<Vec<_>>());
    eprintln!(
        "  median_ms linear={:.3} indexed={:.3} known_key={:.3} indexed_gain={:.1}% verdict={}",
        linear_median.as_secs_f64() * 1000.0,
        indexed_median.as_secs_f64() * 1000.0,
        known_median.as_secs_f64() * 1000.0,
        indexed_gain,
        verdict(indexed_gain)
    );
}

#[test]
fn e24_json_m0_field_lookup_microbench() {
    eprintln!(
        "env rustc={} os={} arch={}",
        option_env!("RUSTC_VERSION").unwrap_or("unknown-at-compile"),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    run_shape(10, ROWS_NARROW);
    run_shape(100, ROWS_WIDE);
}
