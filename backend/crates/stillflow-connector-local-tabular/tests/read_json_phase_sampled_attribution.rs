//! E24-JSON-P2: interleaved validity rerun of sampled NDJSON attribution.
//! Instrumentation in `src/read.rs` must stay identical to PR #130.
//! Requires `--features io-metrics`.

#![cfg(feature = "io-metrics")]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, ReadRequest, RequestContext, SourceAsset, SourceConnection,
};
use tempfile::TempDir;

const SAMPLE_ENV: &str = "STILLFLOW_JSON_PHASE_SAMPLE";
const METRICS_OUT_ENV: &str = "E24_IO_METRICS_OUT";
const MEASURE_ORDER: [bool; 6] = [false, true, true, false, false, true];
const OVERHEAD_LIMIT_PCT: f64 = 3.0;
const COVERAGE_FLOOR: f64 = 0.80;
const COVERAGE_CEILING: f64 = 1.15;
const MIN_SAMPLED_ROWS: u64 = 1_000;

const COUNTER_LABELS: &[&str] = &[
    "json_framed_rows",
    "json_phase_sampled_rows",
    "json_phase_sampled_frame_ns",
    "json_phase_sampled_project_validate_ns",
    "json_phase_sampled_reencode_ns",
    "json_phase_polars_decode_ns",
    "json_phase_reorder_ns",
];

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "e24-json-p1",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/e24-json-p1").expect("credential reference"),
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

fn set_sampling(enabled: bool) {
    if enabled {
        std::env::set_var(SAMPLE_ENV, "1");
    } else {
        std::env::remove_var(SAMPLE_ENV);
    }
}

async fn discover(registry: &ConnectorRegistry, connection: &SourceConnection) -> Vec<SourceAsset> {
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
        .unwrap_or_else(|| panic!("missing {name}"))
        .clone()
}

async fn ingest_once(
    connection: &SourceConnection,
    registry: &ConnectorRegistry,
    asset: &SourceAsset,
) {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), 4_096))
        .await
        .expect("open stream");
    while let Some(item) = stream.next().await {
        item.expect("ingest batch");
    }
}

async fn collect_fingerprint(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: &SourceAsset,
) -> (usize, String) {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset.clone(), 4_096))
        .await
        .expect("open stream");
    let mut rows = 0_usize;
    let mut fingerprint = String::new();
    while let Some(item) = stream.next().await {
        let envelope = item.expect("batch");
        let batch = envelope.payload();
        rows += batch.num_rows();
        fingerprint.push_str(&format!("{}x{};", batch.num_rows(), batch.num_columns()));
    }
    (rows, fingerprint)
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

fn write_wide_ndjson(path: &Path, cols: usize, rows: usize) {
    let mut out = BufWriter::with_capacity(1 << 20, File::create(path).expect("create"));
    let names: Vec<String> = (0..cols).map(|col| format!("c{col}")).collect();
    for row in 0..rows {
        write!(out, "{{").expect("object start");
        for (col, name) in names.iter().enumerate() {
            if col > 0 {
                write!(out, ",").expect("comma");
            }
            write!(out, "\"{name}\":\"{}\"", cell_payload(row, col)).expect("cell");
        }
        writeln!(out, "}}").expect("object end");
    }
    out.flush().expect("flush");
}

fn median_u128(samples: &[u128]) -> u128 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    ordered[ordered.len() / 2]
}

fn median_u64(samples: &[u64]) -> u64 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    ordered[ordered.len() / 2]
}

fn read_counter_snapshot(path: &Path) -> BTreeMap<String, u64> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let Some((label, value)) = line.split_once('=') else {
            continue;
        };
        if let Ok(parsed) = value.parse::<u64>() {
            values.insert(label.to_string(), parsed);
        }
    }
    values
}

fn counter_delta(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    let mut delta = BTreeMap::new();
    for label in COUNTER_LABELS {
        let current = after.get(*label).copied().unwrap_or(0);
        let previous = before.get(*label).copied().unwrap_or(0);
        delta.insert((*label).to_string(), current.saturating_sub(previous));
    }
    delta
}

fn estimate_stage(sampled_ns: u64, sampled_rows: u64, total_rows: u64) -> u64 {
    if sampled_rows == 0 {
        0
    } else {
        u64::try_from(u128::from(sampled_ns) * u128::from(total_rows) / u128::from(sampled_rows))
            .unwrap_or(u64::MAX)
    }
}

fn estimate_from_delta(delta: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    let sampled_rows = delta.get("json_phase_sampled_rows").copied().unwrap_or(0);
    let total_rows = delta.get("json_framed_rows").copied().unwrap_or(0);
    let mut estimated = BTreeMap::new();
    estimated.insert(
        "estimated_frame_ns".to_string(),
        estimate_stage(
            delta
                .get("json_phase_sampled_frame_ns")
                .copied()
                .unwrap_or(0),
            sampled_rows,
            total_rows,
        ),
    );
    estimated.insert(
        "estimated_project_validate_ns".to_string(),
        estimate_stage(
            delta
                .get("json_phase_sampled_project_validate_ns")
                .copied()
                .unwrap_or(0),
            sampled_rows,
            total_rows,
        ),
    );
    estimated.insert(
        "estimated_reencode_ns".to_string(),
        estimate_stage(
            delta
                .get("json_phase_sampled_reencode_ns")
                .copied()
                .unwrap_or(0),
            sampled_rows,
            total_rows,
        ),
    );
    estimated.insert(
        "exact_polars_decode_ns".to_string(),
        delta
            .get("json_phase_polars_decode_ns")
            .copied()
            .unwrap_or(0),
    );
    estimated.insert(
        "exact_reorder_ns".to_string(),
        delta.get("json_phase_reorder_ns").copied().unwrap_or(0),
    );
    estimated
}

fn overhead_pct(off: u128, on: u128) -> f64 {
    if off == 0 {
        0.0
    } else {
        (on as f64 - off as f64) / off as f64 * 100.0
    }
}

fn classify_verdict(
    overhead_10: f64,
    overhead_100: f64,
    sampled_10: u64,
    sampled_100: u64,
    coverage_100: f64,
    largest_stage: &str,
    largest_share: f64,
) -> String {
    if overhead_10.abs() > OVERHEAD_LIMIT_PCT || overhead_100.abs() > OVERHEAD_LIMIT_PCT {
        return "INTERLEAVED_ATTRIBUTION_INVALID_OVERHEAD".to_string();
    }
    if sampled_10 < MIN_SAMPLED_ROWS || sampled_100 < MIN_SAMPLED_ROWS {
        return "INTERLEAVED_ATTRIBUTION_INSUFFICIENT".to_string();
    }
    if coverage_100 < COVERAGE_FLOOR || coverage_100 > COVERAGE_CEILING {
        return "INTERLEAVED_ATTRIBUTION_INCOMPLETE".to_string();
    }
    if largest_share >= 0.40 {
        let stage = largest_stage
            .strip_prefix("estimated_")
            .or_else(|| largest_stage.strip_prefix("exact_"))
            .unwrap_or(largest_stage)
            .to_ascii_uppercase();
        return format!("INTERLEAVED_ATTRIBUTION_DOMINANT_{stage}");
    }
    "INTERLEAVED_ATTRIBUTION_MIXED".to_string()
}

#[tokio::test]
#[ignore]
async fn json_phase_sampled_attribution_focused() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path();
    let metrics_out: PathBuf = root.join("e24_json_phase_sample.out");
    std::env::set_var(METRICS_OUT_ENV, &metrics_out);
    let cases = [(10_usize, 100_000_usize), (100, 100_000)];
    for (cols, rows) in cases {
        let name = format!("p1_ndjson_{cols}c_{rows}r.ndjson");
        eprintln!("[e24-json-p2] generating {name}");
        write_wide_ndjson(&root.join(&name), cols, rows);
    }
    let connection = connection(root);
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    let mut cell_results = Vec::new();

    for (cols, rows) in cases {
        let name = format!("p1_ndjson_{cols}c_{rows}r.ndjson");
        let asset = asset_named(&assets, &name);
        eprintln!("[e24-json-p2] measuring {name}");

        set_sampling(false);
        ingest_once(&connection, &registry, &asset).await;
        set_sampling(true);
        ingest_once(&connection, &registry, &asset).await;

        set_sampling(false);
        let off_fp = collect_fingerprint(&registry, &connection, &asset).await;
        set_sampling(true);
        let on_fp = collect_fingerprint(&registry, &connection, &asset).await;
        assert_eq!(off_fp, on_fp, "sampling on/off must not change output");
        assert_eq!(off_fp.0, rows);

        let mut snapshot = read_counter_snapshot(&metrics_out);
        let mut off_walls = Vec::new();
        let mut on_walls = Vec::new();
        let mut on_raw: Vec<BTreeMap<String, u64>> = Vec::new();
        let mut on_estimated: Vec<BTreeMap<String, u64>> = Vec::new();
        let mut chronological = Vec::new();

        for (index, sampling_on) in MEASURE_ORDER.into_iter().enumerate() {
            set_sampling(sampling_on);
            let started = Instant::now();
            ingest_once(&connection, &registry, &asset).await;
            let wall = u128::from(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            let after = read_counter_snapshot(&metrics_out);
            let delta = counter_delta(&snapshot, &after);
            snapshot = after;
            let estimated = estimate_from_delta(&delta);
            chronological.push(serde_json::json!({
                "index": index,
                "sampling": if sampling_on { "on" } else { "off" },
                "wall_ms": wall,
                "raw_counters": delta,
                "estimated_ns": estimated,
            }));
            if sampling_on {
                on_walls.push(wall);
                on_raw.push(delta);
                on_estimated.push(estimated);
            } else {
                off_walls.push(wall);
            }
        }
        set_sampling(false);

        let off_med = median_u128(&off_walls);
        let on_med = median_u128(&on_walls);
        let overhead = overhead_pct(off_med, on_med);
        let sampled_counts: Vec<u64> = on_raw
            .iter()
            .map(|row| row.get("json_phase_sampled_rows").copied().unwrap_or(0))
            .collect();
        let sampled_median = median_u64(&sampled_counts);

        let estimate_keys = [
            "estimated_frame_ns",
            "estimated_project_validate_ns",
            "estimated_reencode_ns",
            "exact_polars_decode_ns",
            "exact_reorder_ns",
        ];
        let mut estimate_medians = BTreeMap::new();
        for key in estimate_keys {
            let samples: Vec<u64> = on_estimated
                .iter()
                .map(|row| row.get(key).copied().unwrap_or(0))
                .collect();
            estimate_medians.insert(key.to_string(), median_u64(&samples));
        }
        let stage_sum_ns: u64 = estimate_medians.values().copied().sum();
        let wall_ns = on_med.saturating_mul(1_000_000);
        let coverage = if wall_ns == 0 {
            0.0
        } else {
            stage_sum_ns as f64 / wall_ns as f64
        };
        let (largest_label, largest_ns) = estimate_medians
            .iter()
            .max_by_key(|(_, value)| *value)
            .map(|(label, value)| (label.clone(), *value))
            .expect("stages");
        let largest_share = if wall_ns == 0 {
            0.0
        } else {
            largest_ns as f64 / wall_ns as f64
        };

        cell_results.push(serde_json::json!({
            "fixture": { "format": "ndjson", "cols": cols, "rows": rows },
            "schedule": ["off", "on", "on", "off", "off", "on"],
            "chronological": chronological,
            "fingerprint_rows": off_fp.0,
            "off_wall_ms": off_walls,
            "on_wall_ms": on_walls,
            "off_median_ms": off_med,
            "on_median_ms": on_med,
            "overhead_pct": overhead,
            "on_raw_counters": on_raw,
            "on_estimated_ns": on_estimated,
            "on_estimate_median_ns": estimate_medians,
            "sampled_rows": sampled_counts,
            "sampled_rows_median": sampled_median,
            "coverage": coverage,
            "largest_stage": largest_label,
            "largest_share": largest_share,
        }));
    }

    let overhead_10 = cell_results[0]["overhead_pct"].as_f64().unwrap_or(0.0);
    let overhead_100 = cell_results[1]["overhead_pct"].as_f64().unwrap_or(0.0);
    let sampled_10 = cell_results[0]["sampled_rows_median"].as_u64().unwrap_or(0);
    let sampled_100 = cell_results[1]["sampled_rows_median"].as_u64().unwrap_or(0);
    let coverage_100 = cell_results[1]["coverage"].as_f64().unwrap_or(0.0);
    let largest_stage = cell_results[1]["largest_stage"]
        .as_str()
        .unwrap_or("unknown");
    let largest_share = cell_results[1]["largest_share"].as_f64().unwrap_or(0.0);
    let verdict = classify_verdict(
        overhead_10,
        overhead_100,
        sampled_10,
        sampled_100,
        coverage_100,
        largest_stage,
        largest_share,
    );

    println!(
        "{}",
        serde_json::json!({
            "cells": cell_results,
            "verdict": verdict,
        })
    );
}
