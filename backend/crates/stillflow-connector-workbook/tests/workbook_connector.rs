use std::fs::{self, File};
use std::io::Write as _;
use std::sync::Arc;

use base64::Engine as _;
use futures::StreamExt;
use stillflow_connector_workbook::WorkbookConnector;
use stillflow_connectors::ConnectorRegistry;
use stillflow_core::{
    CellCoordinate, CellRange, ConnectionStatus, ConnectorKind, CredentialRef, DiscoverRequest,
    ErrorCategory, InspectRequest, PreviewRequest, ReadRequest, RequestContext, SourceConnection,
    TestConnectionRequest, WorkbookHeaderSelection, WorkbookRegionSelection,
    WorkbookSheetVisibility,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const FIXTURES: &[(&str, &str)] = &[
    (
        "any_sheets.xlsx",
        include_str!("fixtures/any_sheets.xlsx.b64"),
    ),
    (
        "any_sheets.xlsb",
        include_str!("fixtures/any_sheets.xlsb.b64"),
    ),
    (
        "any_sheets.xls",
        include_str!("fixtures/any_sheets.xls.b64"),
    ),
    (
        "any_sheets.ods",
        include_str!("fixtures/any_sheets.ods.b64"),
    ),
    ("issue3.xlsm", include_str!("fixtures/issue3.xlsm.b64")),
    (
        "temperature.xlsx",
        include_str!("fixtures/temperature.xlsx.b64"),
    ),
];

fn fixture_root() -> TempDir {
    let root = TempDir::new().expect("fixture root");
    for (name, encoded) in FIXTURES {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.split_whitespace().collect::<String>())
            .expect("base64 fixture");
        fs::write(root.path().join(name), bytes).expect("write fixture");
    }
    root
}

fn connection(root: &TempDir) -> SourceConnection {
    connection_with_config(serde_json::json!({
        "allowedRoots": [root.path().to_string_lossy()],
        "maxSheetCells": 2_000_000
    }))
}

fn connection_with_config(config: serde_json::Value) -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::ExcelWorkbook,
        "fixtures",
        config,
        CredentialRef::new("cred://local/workbook-fixtures").expect("credential reference"),
    )
    .expect("connection")
}

fn write_zip(path: &std::path::Path, entries: &[(&str, &[u8])]) {
    let file = File::create(path).expect("create package");
    let mut archive = ZipWriter::new(file);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for (name, bytes) in entries {
        archive
            .start_file(*name, options)
            .expect("start package entry");
        archive.write_all(bytes).expect("write package entry");
    }
    archive.finish().expect("finish package");
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(WorkbookConnector))
        .expect("register connector");
    registry
}

#[tokio::test]
async fn discovers_all_supported_formats_and_sheet_visibility() {
    let root = fixture_root();
    let connection = connection(&root);
    let registry = registry();
    let status = registry
        .test_connection(
            &connection,
            TestConnectionRequest {
                context: RequestContext::default(),
            },
        )
        .await
        .expect("test connection");
    assert_eq!(status, ConnectionStatus::Ok);

    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    for extension in [".xls", ".xlsx", ".xlsm", ".xlsb", ".ods"] {
        assert!(
            assets
                .iter()
                .any(|asset| asset.locator.path.ends_with(extension)),
            "missing {extension}"
        );
    }
    assert!(assets.iter().all(|asset| asset.locator.sheet.is_some()));
    assert!(assets
        .iter()
        .all(|asset| asset.locator.workbook_region.is_none()));
    assert!(assets
        .iter()
        .all(|asset| asset.locator.sheet.as_deref() != Some("Chart")));
    let rediscovered = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("rediscover");
    assert_eq!(
        assets.iter().map(|asset| asset.id).collect::<Vec<_>>(),
        rediscovered
            .iter()
            .map(|asset| asset.id)
            .collect::<Vec<_>>()
    );

    let hidden = assets
        .iter()
        .find(|asset| {
            asset.locator.path == "any_sheets.xlsx"
                && asset.locator.sheet.as_deref() == Some("Hidden")
        })
        .expect("hidden sheet")
        .clone();
    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: hidden,
            },
        )
        .await
        .expect("inspect hidden sheet");
    assert_eq!(
        metadata
            .workbook
            .expect("workbook metadata")
            .sheet_visibility,
        WorkbookSheetVisibility::Hidden
    );
}

#[tokio::test]
async fn inspects_selects_previews_and_streams_a_region() {
    let root = fixture_root();
    let connection = connection(&root);
    let registry = registry();
    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let mut asset = assets
        .into_iter()
        .find(|asset| {
            asset.locator.path == "temperature.xlsx"
                && asset.locator.sheet.as_deref() == Some("Sheet1")
        })
        .expect("temperature sheet");
    let unselected = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect candidates");
    let candidate = unselected
        .workbook
        .expect("workbook metadata")
        .region_candidates
        .into_iter()
        .next()
        .expect("region candidate");
    assert_eq!(
        candidate.range,
        CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(2, 1)).expect("range")
    );
    assert!(candidate
        .header_candidates
        .iter()
        .any(|header| header.row == 0 && header.score >= 80));
    asset.locator.workbook_region = Some(WorkbookRegionSelection {
        range: candidate.range,
        header: WorkbookHeaderSelection::Row(0),
    });

    let selected = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect selected region");
    assert_eq!(selected.row_count, Some(2));
    assert_eq!(selected.schema.fields.len(), 2);
    assert_eq!(selected.schema.fields[0].name, "label");
    assert_eq!(selected.schema.fields[1].name, "value");

    let preview = registry
        .preview(
            &connection,
            PreviewRequest::new(asset.clone(), 1, 1024 * 1024),
        )
        .await
        .expect("preview");
    assert_eq!(preview.rows_returned, 1);
    assert!(preview.rows_truncated);
    assert_eq!(preview.batches.len(), 1);

    let mut stream = registry
        .read_batches(&connection, ReadRequest::new(asset, 1))
        .await
        .expect("read stream");
    let first = stream
        .next()
        .await
        .expect("first item")
        .expect("first batch");
    let second = stream
        .next()
        .await
        .expect("second item")
        .expect("second batch");
    assert_eq!(first.sequence(), 0);
    assert_eq!(second.sequence(), 1);
    assert!(Arc::ptr_eq(first.shared_schema(), second.shared_schema()));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn rejects_preview_without_explicit_selection_and_corrupt_workbooks() {
    let root = fixture_root();
    fs::write(root.path().join("corrupt.xlsx"), b"not a workbook").expect("corrupt fixture");
    let connection = connection(&root);
    let registry = registry();
    assert!(registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .is_err());

    fs::remove_file(root.path().join("corrupt.xlsx")).expect("remove corrupt fixture");
    let asset = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover")
        .into_iter()
        .find(|asset| asset.locator.path == "temperature.xlsx")
        .expect("temperature sheet");
    assert!(registry
        .preview(&connection, PreviewRequest::new(asset, 10, 1024 * 1024))
        .await
        .is_err());
}

#[tokio::test]
async fn enforces_package_and_ods_expansion_bounds_before_decode() {
    let root = TempDir::new().expect("fixture root");
    write_zip(
        &root.path().join("expanded.xlsx"),
        &[("[Content_Types].xml", b"01234567890")],
    );
    let bounded_connection = connection_with_config(serde_json::json!({
        "allowedRoots": [root.path().to_string_lossy()],
        "maxExpandedArchiveBytes": 10
    }));
    let error = registry()
        .discover(
            &bounded_connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect_err("expanded package must be rejected");
    assert_eq!(error.category(), ErrorCategory::InvalidData);

    fs::remove_file(root.path().join("expanded.xlsx")).expect("remove expanded package");
    write_zip(
        &root.path().join("repeated.ods"),
        &[(
            "content.xml",
            br#"<table:table-row table:number-rows-repeated="2000001"><table:table-cell/></table:table-row>"#,
        )],
    );
    let error = registry()
        .discover(
            &connection(&root),
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect_err("ODS repeat expansion must be rejected");
    assert_eq!(error.category(), ErrorCategory::InvalidData);
    let root_text = root.path().to_string_lossy();
    assert!(!error.user_message().contains(root_text.as_ref()));

    fs::remove_file(root.path().join("repeated.ods")).expect("remove repeated package");
    write_zip(
        &root.path().join("unsafe.xlsx"),
        &[("../outside.xml", b"unsafe")],
    );
    let error = registry()
        .discover(
            &connection(&root),
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect_err("unsafe package entry must be rejected");
    assert_eq!(error.category(), ErrorCategory::InvalidData);
}

#[tokio::test]
async fn rejects_traversal_and_honours_pre_cancelled_requests() {
    let root = fixture_root();
    let connection = connection(&root);
    let registry = registry();
    let mut asset = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover")
        .into_iter()
        .next()
        .expect("sheet");
    asset.locator.path = "../outside.xlsx".to_owned();
    let error = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset,
            },
        )
        .await
        .expect_err("traversal must be rejected");
    assert_eq!(error.category(), ErrorCategory::InvalidConfiguration);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::with_cancellation(cancellation),
                parent_path: None,
            },
        )
        .await
        .expect_err("cancelled discovery");
    assert_eq!(error.category(), ErrorCategory::Cancelled);
}

#[cfg(unix)]
#[tokio::test]
async fn discovery_does_not_follow_file_links() {
    use std::os::unix::fs::symlink;

    let root = fixture_root();
    let outside = TempDir::new().expect("outside root");
    fs::write(outside.path().join("outside.xlsx"), b"outside").expect("outside workbook");
    symlink(
        outside.path().join("outside.xlsx"),
        root.path().join("linked.xlsx"),
    )
    .expect("workbook symlink");
    let assets = registry()
        .discover(
            &connection(&root),
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    assert!(assets
        .iter()
        .all(|asset| asset.locator.path != "linked.xlsx"));
}

#[tokio::test]
async fn header_only_regions_projection_and_stream_release_are_bounded() {
    let root = fixture_root();
    let connection = connection(&root);
    let registry = registry();
    let mut asset = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover")
        .into_iter()
        .find(|asset| {
            asset.locator.path == "temperature.xlsx"
                && asset.locator.sheet.as_deref() == Some("Sheet1")
        })
        .expect("temperature sheet");
    asset.locator.workbook_region = Some(WorkbookRegionSelection {
        range: CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(2, 1))
            .expect("range"),
        header: WorkbookHeaderSelection::Row(0),
    });
    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");
    let mut request = ReadRequest::new(asset.clone(), 1);
    request.projection = Some(vec![
        metadata.schema.fields[1].id,
        metadata.schema.fields[0].id,
    ]);
    let mut stream = registry
        .read_batches(&connection, request)
        .await
        .expect("projected stream");
    let batch = stream
        .next()
        .await
        .expect("batch")
        .expect("projected batch");
    assert_eq!(batch.schema().fields[0].name, "value");
    assert_eq!(batch.schema().fields[1].name, "label");
    drop(stream);

    asset.locator.workbook_region = Some(WorkbookRegionSelection {
        range: CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(0, 1))
            .expect("header range"),
        header: WorkbookHeaderSelection::Row(0),
    });
    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("header-only inspection");
    assert_eq!(metadata.row_count, Some(0));
    let preview = registry
        .preview(&connection, PreviewRequest::new(asset, 10, 1024 * 1024))
        .await
        .expect("header-only preview");
    assert_eq!(preview.rows_returned, 0);
    assert!(preview.batches.is_empty());

    fs::remove_file(root.path().join("temperature.xlsx"))
        .expect("prepared reader released workbook handle");
}
