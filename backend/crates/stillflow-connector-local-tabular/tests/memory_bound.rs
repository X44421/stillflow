#![cfg(target_os = "linux")]

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::sync::Arc;

use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, PreviewRequest, ReadRequest, RequestContext, SourceConnection,
};
use tempfile::TempDir;

const SOURCE_BYTES: usize = 64 * 1024 * 1024;
const PEAK_TOLERANCE_KIB: usize = 32 * 1024;

fn peak_resident_kib() -> usize {
    fs::read_to_string("/proc/self/status")
        .expect("Linux process status")
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .expect("VmHWM in Linux process status")
}

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "memory-bound fixture",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 64, "maxBytes": 65536 }
        }),
        CredentialRef::new("cred://local/memory-bound").expect("credential reference"),
    )
    .expect("connection")
}

#[tokio::test(flavor = "current_thread")]
async fn first_batch_peak_memory_does_not_scale_with_a_large_source() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(
        temp.path().join("warmup.ndjson"),
        b"{\"id\":1,\"payload\":\"warmup\"}\n",
    )
    .expect("warmup fixture");

    let row = format!("{{\"id\":1,\"payload\":\"{}\"}}\n", "x".repeat(512));
    let mut writer =
        BufWriter::new(File::create(temp.path().join("large.ndjson")).expect("large fixture file"));
    let mut written = 0_usize;
    while written < SOURCE_BYTES {
        writer.write_all(row.as_bytes()).expect("large fixture row");
        written += row.len();
    }
    writer.flush().expect("flush large fixture");
    drop(writer);

    let connection = connection(temp.path());
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover fixtures");
    let warmup = assets
        .iter()
        .find(|asset| asset.name == "warmup.ndjson")
        .expect("warmup asset")
        .clone();
    let large = assets
        .into_iter()
        .find(|asset| asset.name == "large.ndjson")
        .expect("large asset");

    registry
        .preview(&connection, PreviewRequest::new(warmup, 1, 1024 * 1024))
        .await
        .expect("warm decoder");
    let baseline = peak_resident_kib();

    let mut stream = registry
        .read_batches(&connection, ReadRequest::new(large, 16))
        .await
        .expect("open bounded stream");
    let first = stream
        .next()
        .await
        .expect("first stream item")
        .expect("first bounded batch");
    assert_eq!(first.row_count(), 16);
    let growth = peak_resident_kib().saturating_sub(baseline);
    assert!(
        growth <= PEAK_TOLERANCE_KIB,
        "first-batch peak grew by {growth} KiB for a {SOURCE_BYTES}-byte source; tolerance is {PEAK_TOLERANCE_KIB} KiB"
    );
}
