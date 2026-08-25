//! Semantic differential: legacy Polars JSON path vs `json-arrow-direct`.
//!
//! Compiled only with `--features json-arrow-direct`. Default (unset switch)
//! remains the accepted legacy path; `STILLFLOW_JSON_ARROW_DIRECT=1` selects
//! the experimental decoder. Cases run in one test so the env switch stays
//! exclusive without holding a mutex across `.await`.

#![cfg(feature = "json-arrow-direct")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array, ListArray};
use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    BatchEnvelope, ColumnId, CredentialRef, DiscoverRequest, ErrorCategory, InspectRequest,
    LogicalField, LogicalSchema, LogicalType, ReadRequest, RequestContext, SourceAsset,
    SourceConnection,
};
use tempfile::TempDir;

const DIRECT_SWITCH_ENV: &str = "STILLFLOW_JSON_ARROW_DIRECT";

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/fixtures").expect("credential reference"),
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

fn set_direct(on: bool) {
    if on {
        std::env::set_var(DIRECT_SWITCH_ENV, "1");
    } else {
        std::env::remove_var(DIRECT_SWITCH_ENV);
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

async fn collect(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: SourceAsset,
    batch_size: usize,
    projection: Option<Vec<ColumnId>>,
) -> Result<Vec<BatchEnvelope>, stillflow_core::ConnectorError> {
    let mut request = ReadRequest::new(asset, batch_size);
    request.projection = projection;
    let mut stream = registry.read_batches(connection, request).await?;
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

fn assert_equivalent(legacy: &[BatchEnvelope], direct: &[BatchEnvelope], label: &str) {
    assert_eq!(legacy.len(), direct.len(), "{label} envelope count");
    for (index, (left, right)) in legacy.iter().zip(direct).enumerate() {
        assert_eq!(left.sequence(), right.sequence(), "{label} seq {index}");
        assert_eq!(left.row_count(), right.row_count(), "{label} rows {index}");
        assert_eq!(left.schema(), right.schema(), "{label} schema {index}");
        assert_eq!(
            left.payload().schema(),
            right.payload().schema(),
            "{label} arrow schema {index}"
        );
        assert_eq!(left.payload(), right.payload(), "{label} payload {index}");
    }
}

async fn compare_ok(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: SourceAsset,
    batch_size: usize,
    projection: Option<Vec<ColumnId>>,
    label: &str,
) {
    set_direct(false);
    let legacy = collect(
        registry,
        connection,
        asset.clone(),
        batch_size,
        projection.clone(),
    )
    .await
    .unwrap_or_else(|error| panic!("legacy {label}: {error}"));
    set_direct(true);
    let direct = collect(registry, connection, asset, batch_size, projection)
        .await
        .unwrap_or_else(|error| panic!("direct {label}: {error}"));
    set_direct(false);
    assert_equivalent(&legacy, &direct, label);
}

async fn compare_err(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: SourceAsset,
    batch_size: usize,
    label: &str,
) -> ErrorCategory {
    set_direct(false);
    let legacy = collect(registry, connection, asset.clone(), batch_size, None)
        .await
        .expect_err("legacy should fail");
    set_direct(true);
    let direct = collect(registry, connection, asset, batch_size, None)
        .await
        .expect_err("direct should fail");
    set_direct(false);
    assert_eq!(
        legacy.category(),
        direct.category(),
        "{label} error category"
    );
    legacy.category()
}

async fn inspect_category(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    name: &str,
) -> ErrorCategory {
    let assets = discover(registry, connection).await;
    let asset = assets
        .into_iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("missing {name}"));
    registry
        .inspect(
            connection,
            InspectRequest {
                context: RequestContext::default(),
                asset,
            },
        )
        .await
        .expect_err("inspect should fail")
        .category()
}

async fn legacy_and_direct_json_paths_match() {
    set_direct(false);
    let temp = TempDir::new().expect("tmp");
    std::fs::write(
        temp.path().join("rows.json"),
        br#"[{"id":1,"label":"alpha","ignored":"x"},{"id":2,"label":"beta","ignored":"y"},{"id":3,"label":"gamma","ignored":"z"}]"#,
    )
    .expect("json");
    std::fs::write(
        temp.path().join("rows.ndjson"),
        b"{\"id\":1,\"label\":\"alpha\",\"ignored\":\"x\"}\n{\"id\":2,\"label\":\"beta\",\"ignored\":\"y\"}\n{\"id\":3,\"label\":\"gamma\",\"ignored\":\"z\"}\n",
    )
    .expect("ndjson");
    std::fs::write(
        temp.path().join("nested.json"),
        br#"[{"id":1,"items":[1,2],"meta":{"ok":true}},{"id":2,"items":[],"meta":{"ok":false}}]"#,
    )
    .expect("nested");
    std::fs::write(temp.path().join("objects.json"), b"[{},{}]").expect("objects");
    std::fs::write(
        temp.path().join("all-null.ndjson"),
        b"{\"value\":null}\n{\"value\":null}\n",
    )
    .expect("nulls");
    std::fs::write(
        temp.path().join("numbers.ndjson"),
        b"{\"i\":-128,\"u\":255,\"f\":-0.0,\"ok\":true,\"s\":\"hi\"}\n{\"i\":127,\"u\":0,\"f\":1.5,\"ok\":false,\"s\":\"\"}\n",
    )
    .expect("numbers");
    std::fs::write(
        temp.path().join("dates.ndjson"),
        b"{\"day\":\"2026-08-08\",\"instant\":\"2026-08-08T00:00:00.000Z\"}\n{\"day\":null,\"instant\":null}\n",
    )
    .expect("dates");
    std::fs::write(
        temp.path().join("missing-null.ndjson"),
        b"{\"id\":1}\n{\"id\":2,\"label\":null}\n",
    )
    .expect("missing");

    let connection = connection(temp.path());
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    for name in ["rows.json", "rows.ndjson"] {
        let asset = assets
            .iter()
            .find(|asset| asset.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .clone();
        compare_ok(&registry, &connection, asset.clone(), 2, None, name).await;
        compare_ok(
            &registry,
            &connection,
            asset.clone(),
            ReadRequest::MAX_BATCH_SIZE,
            None,
            &format!("{name} max batch"),
        )
        .await;
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
        let label = metadata.schema.fields[1].id;
        let id = metadata.schema.fields[0].id;
        compare_ok(
            &registry,
            &connection,
            asset,
            2,
            Some(vec![label, id]),
            &format!("{name} projection"),
        )
        .await;
    }

    let nested = assets
        .iter()
        .find(|asset| asset.name == "nested.json")
        .expect("nested")
        .clone();
    compare_ok(&registry, &connection, nested.clone(), 1, None, "nested").await;
    set_direct(true);
    let direct_nested = collect(&registry, &connection, nested, 10, None)
        .await
        .expect("direct nested");
    set_direct(false);
    assert_eq!(direct_nested.len(), 1);
    assert_eq!(direct_nested[0].row_count(), 2);
    let items = direct_nested[0]
        .payload()
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list");
    assert_eq!(items.len(), 2);
    assert_eq!(items.value(0).len(), 2);
    assert_eq!(items.value(1).len(), 0);

    let objects = assets
        .iter()
        .find(|asset| asset.name == "objects.json")
        .expect("objects")
        .clone();
    compare_ok(&registry, &connection, objects, 1, None, "empty objects").await;

    let all_null = assets
        .iter()
        .find(|asset| asset.name == "all-null.ndjson")
        .expect("all-null")
        .clone();
    compare_ok(&registry, &connection, all_null, 10, None, "all-null").await;

    let numbers = assets
        .iter()
        .find(|asset| asset.name == "numbers.ndjson")
        .expect("numbers")
        .clone();
    compare_ok(&registry, &connection, numbers.clone(), 10, None, "numbers").await;
    set_direct(true);
    let direct_numbers = collect(&registry, &connection, numbers, 10, None)
        .await
        .expect("direct numbers");
    set_direct(false);
    let floats = direct_numbers[0]
        .payload()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64");
    assert!(floats.value(0).is_sign_negative() || floats.value(0) == 0.0);

    let dates = assets
        .iter()
        .find(|asset| asset.name == "dates.ndjson")
        .expect("dates")
        .clone();
    compare_ok(&registry, &connection, dates, 10, None, "dates").await;

    let missing = assets
        .iter()
        .find(|asset| asset.name == "missing-null.ndjson")
        .expect("missing")
        .clone();
    compare_ok(
        &registry,
        &connection,
        missing,
        10,
        None,
        "nullable missing",
    )
    .await;
}

async fn legacy_and_direct_json_errors_match_category() {
    set_direct(false);
    let temp = TempDir::new().expect("tmp");
    std::fs::write(temp.path().join("not-array.json"), b"{\"id\":1}").expect("shape");
    std::fs::write(temp.path().join("scalar.ndjson"), b"1\n").expect("scalar");
    std::fs::write(
        temp.path().join("unknown.ndjson"),
        b"{\"id\":1}\n{\"id\":2,\"extra\":true}\n",
    )
    .expect("unknown");
    std::fs::write(temp.path().join("dup.ndjson"), b"{\"id\":1,\"id\":2}\n").expect("dup");
    std::fs::write(
        temp.path().join("required.ndjson"),
        b"{\"id\":1,\"label\":\"a\"}\n{\"label\":\"b\"}\n",
    )
    .expect("required");
    std::fs::write(
        temp.path().join("bad-int.ndjson"),
        b"{\"id\":1}\n{\"id\":true}\n",
    )
    .expect("bad int");
    std::fs::write(
        temp.path().join("truncated.ndjson"),
        b"{\"id\":1}\n{\"id\":",
    )
    .expect("trunc");

    let connection = connection(temp.path());
    let registry = registry();
    assert_eq!(
        inspect_category(&registry, &connection, "not-array.json").await,
        ErrorCategory::InvalidData
    );
    assert_eq!(
        inspect_category(&registry, &connection, "scalar.ndjson").await,
        ErrorCategory::InvalidData
    );

    let drift_connection = SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "bounded inference",
        serde_json::json!({
            "allowedRoots": [temp.path().to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/bounded").expect("credential reference"),
    )
    .expect("drift connection");

    std::fs::write(temp.path().join("ok.ndjson"), b"{\"id\":1}\n{\"id\":2}\n").expect("ok seed");
    let assets = discover(&registry, &drift_connection).await;
    for (name, expected) in [
        ("unknown.ndjson", ErrorCategory::SchemaDrift),
        ("dup.ndjson", ErrorCategory::SchemaDrift),
        ("required.ndjson", ErrorCategory::SchemaDrift),
        ("bad-int.ndjson", ErrorCategory::SchemaDrift),
        ("truncated.ndjson", ErrorCategory::InvalidData),
    ] {
        let asset = assets
            .iter()
            .find(|asset| asset.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .clone();
        let category = compare_err(&registry, &drift_connection, asset, 10, name).await;
        assert_eq!(category, expected, "{name}");
    }
}

async fn direct_json_honours_cancellation_and_batch_bounds() {
    set_direct(true);
    let temp = TempDir::new().expect("tmp");
    let mut body = String::new();
    for value in 0..10_000 {
        body.push_str(&format!("{{\"id\":{value}}}\n"));
    }
    std::fs::write(temp.path().join("many.ndjson"), body).expect("many");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection).await.pop().expect("asset");

    let token = tokio_util::sync::CancellationToken::new();
    let mut request = ReadRequest::new(asset.clone(), 128);
    request.context = RequestContext::with_cancellation(token.clone());
    let mut stream = registry
        .read_batches(&connection, request)
        .await
        .expect("open");
    assert!(stream.next().await.expect("first").is_ok());
    token.cancel();
    let error = stream
        .next()
        .await
        .expect("cancelled item")
        .expect_err("cancelled");
    assert_eq!(error.category(), ErrorCategory::Cancelled);
    assert!(stream.next().await.is_none());

    set_direct(false);
    let _ = collect(&registry, &connection, asset, 128, None)
        .await
        .expect("legacy still works after switch off");
}

async fn override_nullability_matches_legacy() {
    set_direct(false);
    let temp = TempDir::new().expect("tmp");
    std::fs::write(
        temp.path().join("rows.ndjson"),
        b"{\"id\":1,\"label\":null}\n",
    )
    .expect("rows");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection).await.pop().expect("asset");
    let required = LogicalSchema::new(vec![
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(101)),
            "id",
            LogicalType::Int64,
            false,
        )
        .expect("id"),
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(102)),
            "label",
            LogicalType::Utf8,
            false,
        )
        .expect("label"),
    ])
    .expect("schema");
    let mut request = ReadRequest::new(asset, 10);
    request.schema_override = Some(required);

    async fn one_err(
        registry: &ConnectorRegistry,
        connection: &SourceConnection,
        request: ReadRequest,
    ) -> ErrorCategory {
        let mut stream = registry
            .read_batches(connection, request)
            .await
            .expect("open");
        stream
            .next()
            .await
            .expect("item")
            .expect_err("must fail")
            .category()
    }

    set_direct(false);
    let legacy = one_err(&registry, &connection, request.clone()).await;
    set_direct(true);
    let direct = one_err(&registry, &connection, request).await;
    set_direct(false);
    assert_eq!(legacy, ErrorCategory::SchemaDrift);
    assert_eq!(direct, ErrorCategory::SchemaDrift);
}

#[tokio::test]
async fn json_arrow_direct_semantic_suite() {
    set_direct(false);
    legacy_and_direct_json_paths_match().await;
    legacy_and_direct_json_errors_match_category().await;
    direct_json_honours_cancellation_and_batch_bounds().await;
    override_nullability_matches_legacy().await;
    set_direct(false);
}
