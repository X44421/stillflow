//! E24-JSON-P5: fused selected Utf8 deserialize+validate vs legacy Value path.
//! Requires `--features json-selected-utf8-fused`. Default crates stay legacy.

#![cfg(feature = "json-selected-utf8-fused")]

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, ErrorCategory, InspectRequest, ReadRequest, RequestContext,
    SourceAsset, SourceConnection,
};
use tempfile::TempDir;

const FUSED_ENV: &str = "STILLFLOW_JSON_SELECTED_UTF8_FUSED";
const MEASURE_ORDER: [bool; 10] = [
    false, true, true, false, false, true, true, false, false, true,
];

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "e24-json-p5",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/e24-json-p5").expect("credential reference"),
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

fn set_fused(enabled: bool) {
    if enabled {
        std::env::set_var(FUSED_ENV, "1");
    } else {
        std::env::remove_var(FUSED_ENV);
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

async fn inspect(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: &SourceAsset,
) -> stillflow_core::AssetMetadata {
    registry
        .inspect(
            connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect")
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
    request: ReadRequest,
) -> (usize, String) {
    let mut stream = registry
        .read_batches(connection, request)
        .await
        .expect("open stream");
    let mut rows = 0_usize;
    let mut fingerprint = String::new();
    while let Some(item) = stream.next().await {
        let envelope = item.expect("batch");
        let batch = envelope.payload();
        rows += batch.num_rows();
        fingerprint.push_str(&format!("{}x{};", batch.num_rows(), batch.num_columns()));
        for column in 0..batch.num_columns() {
            fingerprint.push_str(&format!("{:?}", batch.column(column)));
        }
    }
    (rows, fingerprint)
}

async fn drain_error_category(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: SourceAsset,
) -> ErrorCategory {
    let mut stream = registry
        .read_batches(connection, ReadRequest::new(asset, 16))
        .await
        .expect("open stream");
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            return error.category();
        }
    }
    panic!("expected a terminal error");
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

fn gain_pct(off: u128, on: u128) -> f64 {
    if off == 0 {
        0.0
    } else {
        (off as f64 - on as f64) / off as f64 * 100.0
    }
}

fn classify_verdict(gain_10: f64, gain_100: f64) -> String {
    if gain_100 < 10.0 {
        "FOCUSED_FUSED_UTF8_WEAK".to_string()
    } else if gain_10 < -5.0 {
        "FOCUSED_FUSED_UTF8_WIDE_ONLY".to_string()
    } else {
        "PROMOTE_FUSED_UTF8_CANDIDATE".to_string()
    }
}

#[tokio::test]
async fn json_selected_utf8_fused_semantic_suite() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path();
    fs::write(
        root.join("ok.ndjson"),
        "{\"id\":\"1\",\"label\":\"alpha\"}\n",
    )
    .expect("ok");
    fs::write(
        root.join("unknown.ndjson"),
        "{\"id\":\"1\",\"label\":\"alpha\"}\n{\"id\":\"2\",\"label\":\"beta\",\"extra\":\"nope\"}\n",
    )
    .expect("unknown");
    fs::write(
        root.join("duplicate.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"1\",\"label\":\"a\",\"id\":\"2\"}\n",
    )
    .expect("duplicate");
    fs::write(
        root.join("missing.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\"}\n",
    )
    .expect("missing");
    fs::write(
        root.join("nullable-null.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":null}\n",
    )
    .expect("nullable");
    fs::write(
        root.join("required-null.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":null}\n",
    )
    .expect("required null");
    fs::write(
        root.join("number.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":1}\n",
    )
    .expect("number");
    fs::write(
        root.join("bool.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":true}\n",
    )
    .expect("bool");
    fs::write(
        root.join("object.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":{}}\n",
    )
    .expect("object");
    fs::write(
        root.join("array.ndjson"),
        "{\"id\":\"1\",\"label\":\"a\"}\n{\"id\":\"2\",\"label\":[]}\n",
    )
    .expect("array");
    fs::write(
        root.join("order.ndjson"),
        "{\"label\":\"b\",\"id\":\"7\"}\n",
    )
    .expect("order");

    let connection = connection(root);
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    let ok_asset = asset_named(&assets, "ok.ndjson");
    let ok_meta = inspect(&registry, &connection, &ok_asset).await;
    let mut ok_req = ReadRequest::new(ok_asset, 16);
    ok_req.projection = Some(vec![
        ok_meta.schema.fields[0].id,
        ok_meta.schema.fields[1].id,
    ]);

    let order_asset = asset_named(&assets, "order.ndjson");
    let order_meta = inspect(&registry, &connection, &order_asset).await;
    let mut order_req = ReadRequest::new(order_asset, 16);
    order_req.projection = Some(vec![
        order_meta.schema.fields[0].id,
        order_meta.schema.fields[1].id,
    ]);

    let nullable_asset = asset_named(&assets, "nullable-null.ndjson");
    let nullable_meta = inspect(&registry, &connection, &nullable_asset).await;
    let mut nullable_fields = nullable_meta.schema.fields.clone();
    nullable_fields[1].nullable = true;
    let mut nullable_req = ReadRequest::new(nullable_asset, 16);
    nullable_req.schema_override =
        Some(stillflow_core::LogicalSchema::new(nullable_fields).expect("nullable override"));

    let error_names = [
        "unknown.ndjson",
        "duplicate.ndjson",
        "missing.ndjson",
        "required-null.ndjson",
        "number.ndjson",
        "bool.ndjson",
        "object.ndjson",
        "array.ndjson",
    ];

    set_fused(false);
    let mut legacy_errors = Vec::new();
    for name in error_names {
        legacy_errors
            .push(drain_error_category(&registry, &connection, asset_named(&assets, name)).await);
    }
    let legacy_ok = collect_fingerprint(&registry, &connection, ok_req.clone()).await;
    let legacy_order = collect_fingerprint(&registry, &connection, order_req.clone()).await;
    let legacy_nullable = collect_fingerprint(&registry, &connection, nullable_req.clone()).await;

    set_fused(true);
    for (name, expected) in error_names.iter().zip(legacy_errors.iter()) {
        assert_eq!(
            drain_error_category(&registry, &connection, asset_named(&assets, name)).await,
            *expected,
            "{name}"
        );
    }
    assert_eq!(
        collect_fingerprint(&registry, &connection, ok_req).await,
        legacy_ok
    );
    assert_eq!(
        collect_fingerprint(&registry, &connection, order_req).await,
        legacy_order
    );
    assert_eq!(
        collect_fingerprint(&registry, &connection, nullable_req).await,
        legacy_nullable
    );
    assert_eq!(legacy_ok.0, 1);
    assert_eq!(legacy_order.0, 1);
    assert_eq!(legacy_nullable.0, 2);
    set_fused(false);
}

#[tokio::test]
#[ignore]
async fn json_selected_utf8_fused_focused_ab() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path();
    let cases = [(10_usize, 100_000_usize), (100, 100_000)];
    for (cols, rows) in cases {
        let name = format!("p5_ndjson_{cols}c_{rows}r.ndjson");
        eprintln!("[e24-json-p5] generating {name}");
        write_wide_ndjson(&root.join(&name), cols, rows);
    }
    let connection = connection(root);
    let registry = registry();
    let assets = discover(&registry, &connection).await;
    let mut cell_results = Vec::new();

    for (cols, rows) in cases {
        let name = format!("p5_ndjson_{cols}c_{rows}r.ndjson");
        let asset = asset_named(&assets, &name);
        eprintln!("[e24-json-p5] measuring {name}");

        set_fused(false);
        ingest_once(&connection, &registry, &asset).await;
        set_fused(true);
        ingest_once(&connection, &registry, &asset).await;

        set_fused(false);
        let off_fp = collect_fingerprint(
            &registry,
            &connection,
            ReadRequest::new(asset.clone(), 4_096),
        )
        .await;
        set_fused(true);
        let on_fp = collect_fingerprint(
            &registry,
            &connection,
            ReadRequest::new(asset.clone(), 4_096),
        )
        .await;
        assert_eq!(off_fp, on_fp, "fused on/off must not change output");
        assert_eq!(off_fp.0, rows);

        let mut off_walls = Vec::new();
        let mut on_walls = Vec::new();
        let mut chronological = Vec::new();
        for (index, fused) in MEASURE_ORDER.into_iter().enumerate() {
            set_fused(fused);
            let started = Instant::now();
            ingest_once(&connection, &registry, &asset).await;
            let wall = u128::from(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            chronological.push(serde_json::json!({
                "index": index,
                "mode": if fused { "on" } else { "off" },
                "wall_ms": wall,
            }));
            if fused {
                on_walls.push(wall);
            } else {
                off_walls.push(wall);
            }
        }
        set_fused(false);
        let off_med = median_u128(&off_walls);
        let on_med = median_u128(&on_walls);
        cell_results.push(serde_json::json!({
            "fixture": { "format": "ndjson", "cols": cols, "rows": rows },
            "schedule": ["off","on","on","off","off","on","on","off","off","on"],
            "chronological": chronological,
            "off_wall_ms": off_walls,
            "on_wall_ms": on_walls,
            "off_median_ms": off_med,
            "on_median_ms": on_med,
            "gain_pct": gain_pct(off_med, on_med),
        }));
    }

    let gain_10 = cell_results[0]["gain_pct"].as_f64().unwrap_or(0.0);
    let gain_100 = cell_results[1]["gain_pct"].as_f64().unwrap_or(0.0);
    println!(
        "{}",
        serde_json::json!({
            "cells": cell_results,
            "verdict": classify_verdict(gain_10, gain_100),
        })
    );
}
