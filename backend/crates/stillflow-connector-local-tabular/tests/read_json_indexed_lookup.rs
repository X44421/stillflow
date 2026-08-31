//! E24-JSON-L1: linear vs reader-level indexed top-level JSON field lookup.
//! Requires `--features json-indexed-lookup`. Default crates stay on linear scan.

#![cfg(feature = "json-indexed-lookup")]

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

const INDEXED_ENV: &str = "STILLFLOW_JSON_INDEXED_LOOKUP";
const FOCUSED_REPS: usize = 3;

fn connection(root: &Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "e24-json-l1",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/e24-json-l1").expect("credential reference"),
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

fn set_indexed(enabled: bool) {
    if enabled {
        std::env::set_var(INDEXED_ENV, "1");
    } else {
        std::env::remove_var(INDEXED_ENV);
    }
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

fn median(samples: &[u128]) -> u128 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    ordered[ordered.len() / 2]
}

#[tokio::test]
async fn json_indexed_lookup_semantic_suite() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path();
    fs::write(root.join("ok.ndjson"), "{\"id\":1,\"label\":\"alpha\"}\n").expect("ok");
    fs::write(
        root.join("unknown.ndjson"),
        "{\"id\":1,\"label\":\"alpha\"}\n{\"id\":2,\"label\":\"beta\",\"extra\":\"nope\"}\n",
    )
    .expect("unknown");
    fs::write(
        root.join("duplicate.ndjson"),
        "{\"id\":1,\"label\":\"a\"}\n{\"id\":1,\"label\":\"a\",\"id\":2}\n",
    )
    .expect("duplicate");
    fs::write(
        root.join("missing.ndjson"),
        "{\"id\":1,\"label\":\"a\"}\n{\"id\":2}\n",
    )
    .expect("missing");
    fs::write(
        root.join("nullable-missing.ndjson"),
        "{\"id\":1,\"label\":\"a\"}\n{\"id\":2}\n",
    )
    .expect("nullable");
    fs::write(root.join("order.ndjson"), "{\"label\":\"b\",\"id\":7}\n").expect("order");

    let connection = connection(root);
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    let ok_asset = asset_named(&assets, "ok.ndjson");
    let ok_meta = inspect(&registry, &connection, &ok_asset).await;
    let mut ok_req = ReadRequest::new(ok_asset, 16);
    ok_req.projection = Some(vec![ok_meta.schema.fields[0].id, ok_meta.schema.fields[1].id]);

    let order_asset = asset_named(&assets, "order.ndjson");
    let order_meta = inspect(&registry, &connection, &order_asset).await;
    let mut order_req = ReadRequest::new(order_asset, 16);
    order_req.projection = Some(vec![
        order_meta.schema.fields[0].id,
        order_meta.schema.fields[1].id,
    ]);

    let nullable_asset = asset_named(&assets, "nullable-missing.ndjson");
    let nullable_meta = inspect(&registry, &connection, &nullable_asset).await;
    let mut nullable_fields = nullable_meta.schema.fields.clone();
    nullable_fields[1].nullable = true;
    let mut nullable_req = ReadRequest::new(nullable_asset, 16);
    nullable_req.schema_override =
        Some(stillflow_core::LogicalSchema::new(nullable_fields).expect("nullable override"));

    set_indexed(false);
    let linear_unknown = drain_error_category(
        &registry,
        &connection,
        asset_named(&assets, "unknown.ndjson"),
    )
    .await;
    let linear_duplicate = drain_error_category(
        &registry,
        &connection,
        asset_named(&assets, "duplicate.ndjson"),
    )
    .await;
    let linear_missing = drain_error_category(
        &registry,
        &connection,
        asset_named(&assets, "missing.ndjson"),
    )
    .await;
    let linear_ok = collect_fingerprint(&registry, &connection, ok_req.clone()).await;
    let linear_order = collect_fingerprint(&registry, &connection, order_req.clone()).await;
    let linear_nullable = collect_fingerprint(&registry, &connection, nullable_req.clone()).await;

    set_indexed(true);
    assert_eq!(linear_unknown, ErrorCategory::SchemaDrift);
    assert_eq!(
        drain_error_category(
            &registry,
            &connection,
            asset_named(&assets, "unknown.ndjson"),
        )
        .await,
        linear_unknown
    );
    assert_eq!(linear_duplicate, ErrorCategory::SchemaDrift);
    assert_eq!(
        drain_error_category(
            &registry,
            &connection,
            asset_named(&assets, "duplicate.ndjson"),
        )
        .await,
        linear_duplicate
    );
    assert_eq!(linear_missing, ErrorCategory::SchemaDrift);
    assert_eq!(
        drain_error_category(
            &registry,
            &connection,
            asset_named(&assets, "missing.ndjson"),
        )
        .await,
        linear_missing
    );
    assert_eq!(
        collect_fingerprint(&registry, &connection, ok_req).await,
        linear_ok
    );
    assert_eq!(
        collect_fingerprint(&registry, &connection, order_req).await,
        linear_order
    );
    assert_eq!(
        collect_fingerprint(&registry, &connection, nullable_req).await,
        linear_nullable
    );
    assert_eq!(linear_ok.0, 1);
    assert_eq!(linear_order.0, 1);
    assert_eq!(linear_nullable.0, 2);
    set_indexed(false);
}

#[tokio::test]
#[ignore]
async fn json_indexed_lookup_focused_ab() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path();
    let cases = [(10_usize, 100_000_usize), (100, 100_000)];
    for (cols, rows) in cases {
        let name = format!("f_ndjson_{cols}c_{rows}r.ndjson");
        eprintln!("[e24-json-l1] generating {name}");
        write_wide_ndjson(&root.join(&name), cols, rows);
    }
    let connection = connection(root);
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    for (cols, rows) in cases {
        let name = format!("f_ndjson_{cols}c_{rows}r.ndjson");
        let asset = asset_named(&assets, &name);
        eprintln!("[e24-json-l1] measuring {name}");

        set_indexed(false);
        ingest_once(&connection, &registry, &asset).await;
        set_indexed(true);
        ingest_once(&connection, &registry, &asset).await;

        let mut linear = Vec::new();
        let mut indexed = Vec::new();
        for rep in 0..FOCUSED_REPS {
            let indexed_first = rep % 2 == 0;
            for enabled in if indexed_first {
                [true, false]
            } else {
                [false, true]
            } {
                set_indexed(enabled);
                let started = Instant::now();
                ingest_once(&connection, &registry, &asset).await;
                let wall =
                    u128::from(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
                if enabled {
                    indexed.push(wall);
                } else {
                    linear.push(wall);
                }
            }
        }
        set_indexed(false);
        let linear_med = median(&linear);
        let indexed_med = median(&indexed);
        let gain = if linear_med == 0 {
            0.0
        } else {
            (linear_med as f64 - indexed_med as f64) / linear_med as f64 * 100.0
        };
        println!(
            "{}",
            serde_json::json!({
                "fixture": { "format": "ndjson", "cols": cols, "rows": rows },
                "linear_ms": linear,
                "indexed_ms": indexed,
                "linear_median_ms": linear_med,
                "indexed_median_ms": indexed_med,
                "gain_pct": gain
            })
        );
    }
}
