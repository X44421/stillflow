use std::fs::{self, File};
use std::sync::Arc;

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnector, SourceConnectorRef};
use stillflow_core::{
    CheckpointRequest, ColumnId, CredentialRef, DiscoverRequest, ErrorCategory, Expr,
    InspectRequest, LogicalField, LogicalSchema, LogicalType, PreviewRequest, ReadRequest,
    RequestContext, ScalarValue, SourceConnection, SourceFilter,
};
use tempfile::TempDir;

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

async fn discover(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
) -> Vec<stillflow_core::SourceAsset> {
    registry
        .discover(
            connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover fixtures")
}

fn write_fixtures(root: &std::path::Path) {
    fs::write(
        root.join("rows.csv"),
        b"id,label,ignored\n1,alpha,x\n2,beta,y\n3,gamma,z\n",
    )
    .expect("CSV fixture");
    fs::write(
        root.join("rows.tsv"),
        b"id\tlabel\tignored\n1\talpha\tx\n2\tbeta\ty\n3\tgamma\tz\n",
    )
    .expect("TSV fixture");
    fs::write(
        root.join("rows.json"),
        br#"[{"id":1,"label":"alpha","ignored":"x"},{"id":2,"label":"beta","ignored":"y"},{"id":3,"label":"gamma","ignored":"z"}]"#,
    )
    .expect("JSON fixture");
    fs::write(
        root.join("rows.ndjson"),
        b"{\"id\":1,\"label\":\"alpha\",\"ignored\":\"x\"}\n{\"id\":2,\"label\":\"beta\",\"ignored\":\"y\"}\n{\"id\":3,\"label\":\"gamma\",\"ignored\":\"z\"}\n",
    )
    .expect("NDJSON fixture");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("ignored", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .expect("Parquet batch");
    let mut writer = ArrowWriter::try_new(
        File::create(root.join("rows.parquet")).expect("Parquet file"),
        schema,
        None,
    )
    .expect("Parquet writer");
    writer.write(&batch).expect("write Parquet batch");
    writer.close().expect("close Parquet writer");
}

#[tokio::test]
async fn all_formats_inspect_project_preview_and_stream_in_stable_batches() {
    let temp = TempDir::new().expect("temporary fixture root");
    write_fixtures(temp.path());
    let connection = connection(temp.path());
    let registry = registry();
    let assets = discover(&registry, &connection).await;
    assert_eq!(assets.len(), 5);

    for asset in assets {
        let metadata = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: asset.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("inspect {}: {error}", asset.name));
        let repeated = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: asset.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("repeat inspect {}: {error}", asset.name));
        assert_eq!(metadata.schema, repeated.schema, "{}", asset.name);
        if asset.name.ends_with(".parquet") {
            assert_eq!(metadata.row_count, Some(3));
        } else {
            assert_eq!(metadata.row_count, None);
        }
        assert_eq!(
            metadata
                .schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "label", "ignored"],
            "{}",
            asset.name
        );
        let id = metadata.schema.fields[0].id;
        let label = metadata.schema.fields[1].id;

        let mut preview_request = PreviewRequest::new(asset.clone(), 2, 1024 * 1024);
        preview_request.projection = Some(vec![label, id]);
        let preview = registry
            .preview(&connection, preview_request)
            .await
            .unwrap_or_else(|error| panic!("preview {}: {error}", asset.name));
        assert_eq!(preview.rows_returned, 2, "{}", asset.name);
        assert!(preview.rows_truncated, "{}", asset.name);
        assert_eq!(preview.schema.fields[0].name, "label");
        assert_eq!(preview.schema.fields[1].name, "id");

        let mut read_request = ReadRequest::new(asset.clone(), 2);
        read_request.projection = Some(vec![label, id]);
        let mut stream = registry
            .read_batches(&connection, read_request)
            .await
            .unwrap_or_else(|error| panic!("open stream {}: {error}", asset.name));
        let mut sizes = Vec::new();
        let mut labels = Vec::new();
        while let Some(item) = stream.next().await {
            let envelope = item.unwrap_or_else(|error| panic!("stream {}: {error}", asset.name));
            sizes.push(envelope.row_count());
            let values = envelope.payload().column(0);
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("canonical UTF-8 projection");
            labels.extend(values.iter().map(|value| value.expect("label").to_owned()));
        }
        assert_eq!(sizes, [2, 1], "{}", asset.name);
        assert_eq!(labels, ["alpha", "beta", "gamma"], "{}", asset.name);

        let mut large_stream = registry
            .read_batches(
                &connection,
                ReadRequest::new(asset.clone(), ReadRequest::MAX_BATCH_SIZE),
            )
            .await
            .unwrap_or_else(|error| panic!("open max batch {}: {error}", asset.name));
        let first = large_stream
            .next()
            .await
            .unwrap_or_else(|| panic!("max batch missing for {}", asset.name))
            .unwrap_or_else(|error| panic!("max batch {}: {error}", asset.name));
        assert_eq!(first.row_count(), 3, "{}", asset.name);
        assert!(large_stream.next().await.is_none(), "{}", asset.name);
    }
}

#[tokio::test]
async fn preview_reports_row_and_byte_truncation_independently() {
    let temp = TempDir::new().expect("temporary fixture root");
    let first_value = "a".repeat(128);
    let second_value = "x".repeat(128);
    fs::write(
        temp.path().join("wide.ndjson"),
        format!("{{\"value\":\"{first_value}\"}}\n{{\"value\":\"{second_value}\"}}\n"),
    )
    .expect("wide NDJSON fixture");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection)
        .await
        .pop()
        .expect("wide asset");

    let first = registry
        .preview(
            &connection,
            PreviewRequest::new(asset.clone(), 1, 1024 * 1024),
        )
        .await
        .expect("one row preview");
    assert!(first.rows_truncated);
    let byte_limit = first.bytes_returned;

    let bounded = registry
        .preview(
            &connection,
            PreviewRequest::new(asset.clone(), 10, byte_limit),
        )
        .await
        .expect("byte-bounded preview");
    assert_eq!(bounded.rows_returned, 1);
    assert!(!bounded.rows_truncated);
    assert!(bounded.bytes_truncated);

    let error = registry
        .preview(&connection, PreviewRequest::new(asset, 10, 1))
        .await
        .expect_err("one row cannot fit");
    assert_eq!(error.category(), ErrorCategory::InvalidData);
}

#[tokio::test]
async fn post_inference_drift_is_typed_and_batch_partition_is_invariant() {
    let temp = TempDir::new().expect("temporary fixture root");
    write_fixtures(temp.path());
    fs::write(temp.path().join("drift.csv"), b"id\n1\n2\nnot-an-integer\n").expect("drift fixture");
    fs::write(temp.path().join("stable.csv"), b"id\n1\n2\n3\n4\n").expect("stable fixture");
    fs::write(
        temp.path().join("projected-drift.ndjson"),
        b"{\"id\":1,\"ignored\":\"text\"}\n{\"id\":2,\"ignored\":99}\n",
    )
    .expect("projected drift fixture");
    fs::write(
        temp.path().join("projected-drift.csv"),
        b"id,ignored\n1,10\n2,not-an-integer\n",
    )
    .expect("projected CSV drift fixture");
    fs::write(
        temp.path().join("projected-drift.tsv"),
        b"id\tignored\n1\t10\n2\tnot-an-integer\n",
    )
    .expect("projected TSV drift fixture");
    fs::write(
        temp.path().join("projected-temporal.ndjson"),
        b"{\"id\":1,\"ignored\":\"2026-08-08\"}\n{\"id\":2,\"ignored\":\"not-a-date\"}\n",
    )
    .expect("projected temporal drift fixture");
    let connection = SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "bounded inference",
        serde_json::json!({
            "allowedRoots": [temp.path().to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/bounded").expect("credential reference"),
    )
    .expect("connection");
    let registry = registry();
    let assets = discover(&registry, &connection).await;
    let drift = assets
        .iter()
        .find(|asset| asset.name == "drift.csv")
        .expect("drift asset")
        .clone();
    let stable = assets
        .iter()
        .find(|asset| asset.name == "stable.csv")
        .expect("stable asset")
        .clone();
    let parquet = assets
        .iter()
        .find(|asset| asset.name == "rows.parquet")
        .expect("Parquet drift asset")
        .clone();

    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: drift.clone(),
            },
        )
        .await
        .expect("bounded inspection");
    assert_eq!(
        metadata.findings[0].code,
        "inspect.schema_inference_truncated"
    );
    let mut stream = registry
        .read_batches(&connection, ReadRequest::new(drift, 1))
        .await
        .expect("open drift stream");
    let mut terminal = None;
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            terminal = Some(error.category());
            break;
        }
    }
    assert_eq!(terminal, Some(ErrorCategory::SchemaDrift));

    let parquet_metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: parquet.clone(),
            },
        )
        .await
        .expect("Parquet drift inspection");
    let mut parquet_fields = parquet_metadata.schema.fields.clone();
    let ignored = parquet_fields.get_mut(2).expect("Parquet ignored field");
    ignored.data_type = LogicalType::Int64;
    let parquet_override = LogicalSchema::new(parquet_fields).expect("Parquet override");
    let mut parquet_request = ReadRequest::new(parquet, 1);
    parquet_request.projection = Some(vec![parquet_override.fields[0].id]);
    parquet_request.schema_override = Some(parquet_override);
    let error = match registry.read_batches(&connection, parquet_request).await {
        Ok(_) => panic!("incompatible unselected Parquet override must fail"),
        Err(error) => error,
    };
    assert_eq!(error.category(), ErrorCategory::SchemaDrift);

    for name in [
        "projected-drift.csv",
        "projected-drift.tsv",
        "projected-drift.ndjson",
    ] {
        let projected_drift = assets
            .iter()
            .find(|asset| asset.name == name)
            .unwrap_or_else(|| panic!("missing projected drift asset {name}"))
            .clone();
        let projected_metadata = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: projected_drift.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("inspect projected drift {name}: {error}"));
        let mut projected_request = ReadRequest::new(projected_drift, 1);
        projected_request.projection = Some(vec![projected_metadata.schema.fields[0].id]);
        let mut projected_stream = registry
            .read_batches(&connection, projected_request)
            .await
            .unwrap_or_else(|error| panic!("open projected drift {name}: {error}"));
        let mut terminal = None;
        while let Some(item) = projected_stream.next().await {
            if let Err(error) = item {
                terminal = Some(error);
                break;
            }
        }
        let error = terminal.unwrap_or_else(|| panic!("missing projected drift error for {name}"));
        assert_eq!(
            error.category(),
            ErrorCategory::SchemaDrift,
            "unselected drift remains strict for {name}"
        );
    }

    let temporal = assets
        .iter()
        .find(|asset| asset.name == "projected-temporal.ndjson")
        .expect("projected temporal asset")
        .clone();
    let temporal_metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: temporal.clone(),
            },
        )
        .await
        .expect("projected temporal inspection");
    let temporal_override = LogicalSchema::new(vec![
        temporal_metadata.schema.fields[0].clone(),
        LogicalField::new(
            temporal_metadata.schema.fields[1].id,
            "ignored",
            LogicalType::Date32,
            false,
        )
        .expect("date override field"),
    ])
    .expect("temporal override");
    let mut temporal_request = ReadRequest::new(temporal, 1);
    temporal_request.schema_override = Some(temporal_override.clone());
    temporal_request.projection = Some(vec![temporal_override.fields[0].id]);
    let mut temporal_stream = registry
        .read_batches(&connection, temporal_request)
        .await
        .expect("open projected temporal drift stream");
    assert!(temporal_stream
        .next()
        .await
        .expect("first temporal batch")
        .is_ok());
    let error = temporal_stream
        .next()
        .await
        .expect("temporal drift terminal item")
        .expect_err("unselected invalid date remains strict");
    assert_eq!(error.category(), ErrorCategory::SchemaDrift);

    async fn values(
        registry: &ConnectorRegistry,
        connection: &SourceConnection,
        asset: stillflow_core::SourceAsset,
        batch_size: usize,
    ) -> Vec<i64> {
        let mut stream = registry
            .read_batches(connection, ReadRequest::new(asset, batch_size))
            .await
            .expect("open stable stream");
        let mut output = Vec::new();
        while let Some(item) = stream.next().await {
            let envelope = item.expect("stable batch");
            let values = envelope
                .payload()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 values");
            output.extend(values.values());
        }
        output
    }
    assert_eq!(
        values(&registry, &connection, stable.clone(), 1).await,
        values(&registry, &connection, stable, 3).await
    );
}

#[tokio::test]
async fn rejects_malformed_inputs_unknown_projection_and_unsupported_operations() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(temp.path().join("duplicate.csv"), b"id,id\n1,2\n").expect("duplicate header");
    fs::write(temp.path().join("invalid.json"), b"{\"id\":1}").expect("invalid JSON shape");
    fs::write(temp.path().join("invalid.ndjson"), b"1\n").expect("invalid NDJSON shape");
    fs::write(temp.path().join("invalid.parquet"), b"not parquet").expect("invalid Parquet");
    fs::write(temp.path().join("invalid.csv"), [b'i', b'd', b'\n', 0xff]).expect("invalid UTF-8");
    fs::write(temp.path().join("invalid.tsv"), [b'i', b'd', b'\n', 0xff])
        .expect("invalid TSV UTF-8");
    fs::write(
        temp.path().join("malformed-quoted.csv"),
        b"id,label\n1,\"quoted\",extra\n",
    )
    .expect("malformed quoted CSV row");
    fs::write(
        temp.path().join("bad-footer.parquet"),
        [b'P', b'A', b'R', b'1', 0, 0, 0, 0, b'N', b'O', b'P', b'E'],
    )
    .expect("invalid Parquet footer");
    fs::write(
        temp.path().join("bad-metadata.parquet"),
        [b'P', b'A', b'R', b'1', 0, 0, 0, 0, b'P', b'A', b'R', b'1'],
    )
    .expect("invalid Parquet metadata");
    let connection = connection(temp.path());
    let registry = registry();
    let assets = discover(&registry, &connection).await;
    for asset in &assets {
        let error = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: asset.clone(),
                },
            )
            .await
            .expect_err("malformed fixture");
        assert_eq!(
            error.category(),
            ErrorCategory::InvalidData,
            "{}",
            asset.name
        );
        assert!(!error
            .user_message()
            .contains(temp.path().to_str().expect("path")));
    }

    fs::write(temp.path().join("valid.csv"), b"id\n1\n").expect("valid CSV");
    let valid = discover(&registry, &connection)
        .await
        .into_iter()
        .find(|asset| asset.name == "valid.csv")
        .expect("valid asset");
    let mut unknown_projection = PreviewRequest::new(valid.clone(), 10, 1024);
    unknown_projection.projection = Some(vec![ColumnId::from_uuid(uuid::Uuid::from_u128(999))]);
    let error = registry
        .preview(&connection, unknown_projection)
        .await
        .expect_err("unknown projection");
    assert_eq!(error.category(), ErrorCategory::InvalidConfiguration);

    let mut filtered = PreviewRequest::new(valid.clone(), 10, 1024);
    filtered.filter =
        Some(SourceFilter::new(Expr::Literal(ScalarValue::Boolean(true))).expect("filter"));
    let error = registry
        .preview(&connection, filtered)
        .await
        .expect_err("filter is unsupported");
    assert_eq!(error.category(), ErrorCategory::UnsupportedCapability);

    let mut sampled = PreviewRequest::new(valid, 10, 1024);
    sampled.sampling = stillflow_core::SamplingStrategy::Reservoir;
    let error = registry
        .preview(&connection, sampled)
        .await
        .expect_err("sampling is unsupported");
    assert_eq!(error.category(), ErrorCategory::UnsupportedCapability);
}

#[tokio::test]
async fn cancellation_terminates_an_open_stream_and_empty_source_accepts_override() {
    let temp = TempDir::new().expect("temporary fixture root");
    let mut rows = String::from("id\n");
    for value in 0..10_000 {
        rows.push_str(&format!("{value}\n"));
    }
    fs::write(temp.path().join("many.csv"), rows).expect("many rows");
    fs::write(temp.path().join("empty.csv"), b"").expect("empty CSV");
    let connection = connection(temp.path());
    let registry = registry();
    let assets = discover(&registry, &connection).await;
    let many = assets
        .iter()
        .find(|asset| asset.name == "many.csv")
        .expect("many asset")
        .clone();
    let empty = assets
        .iter()
        .find(|asset| asset.name == "empty.csv")
        .expect("empty asset")
        .clone();

    let token = tokio_util::sync::CancellationToken::new();
    let mut request = ReadRequest::new(many, 128);
    request.context = RequestContext::with_cancellation(token.clone());
    let mut stream = registry
        .read_batches(&connection, request)
        .await
        .expect("open cancellable stream");
    assert!(stream.next().await.expect("first item").is_ok());
    token.cancel();
    let error = stream
        .next()
        .await
        .expect("cancelled item")
        .expect_err("cancelled stream");
    assert_eq!(error.category(), ErrorCategory::Cancelled);
    assert!(stream.next().await.is_none());

    let override_schema = LogicalSchema::new(vec![LogicalField::new(
        ColumnId::from_uuid(uuid::Uuid::from_u128(44)),
        "id",
        LogicalType::Int64,
        true,
    )
    .expect("override field")])
    .expect("override schema");
    let mut preview = PreviewRequest::new(empty, 10, 1024);
    preview.schema_override = Some(override_schema.clone());
    let result = registry
        .preview(&connection, preview)
        .await
        .expect("empty preview with override");
    assert_eq!(result.schema, override_schema);
    assert_eq!(result.rows_returned, 0);
}

#[tokio::test]
async fn nested_json_and_parquet_temporal_types_cross_the_arrow_bridge() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(
        temp.path().join("nested.json"),
        br#"[{"id":1,"items":[1,2],"meta":{"ok":true}},{"id":2,"items":[3],"meta":{"ok":false}}]"#,
    )
    .expect("nested JSON");

    let temporal_schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Date32, true),
        Field::new(
            "instant",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]));
    let temporal_batch = RecordBatch::try_new(
        Arc::clone(&temporal_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), None])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None]).with_timezone("UTC")),
        ],
    )
    .expect("temporal batch");
    let mut writer = ArrowWriter::try_new(
        File::create(temp.path().join("temporal.parquet")).expect("temporal file"),
        temporal_schema,
        None,
    )
    .expect("temporal writer");
    writer.write(&temporal_batch).expect("write temporal data");
    writer.close().expect("close temporal writer");

    let connection = connection(temp.path());
    let registry = registry();
    for asset in discover(&registry, &connection).await {
        let metadata = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: asset.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("inspect {}: {error}", asset.name));
        if asset.name == "nested.json" {
            assert!(matches!(
                metadata.schema.fields[1].data_type,
                LogicalType::List(_)
            ));
            assert!(matches!(
                metadata.schema.fields[2].data_type,
                LogicalType::Struct(_)
            ));
        } else {
            assert_eq!(metadata.schema.fields[0].data_type, LogicalType::Date32);
            assert!(matches!(
                metadata.schema.fields[1].data_type,
                LogicalType::Timestamp { .. }
            ));
        }
        let preview = registry
            .preview(
                &connection,
                PreviewRequest::new(asset.clone(), 10, 1024 * 1024),
            )
            .await
            .unwrap_or_else(|error| panic!("preview {}: {error}", asset.name));
        assert_eq!(preview.rows_returned, 2, "{}", asset.name);
        assert_eq!(
            preview.batches[0].payload().num_columns(),
            metadata.schema.fields.len()
        );
    }
}

#[tokio::test]
async fn configured_csv_dialect_tsv_tab_and_utf8_bom_are_honoured() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(
        temp.path().join("custom.csv"),
        b"id;label\n1;'semi;colon'\n",
    )
    .expect("custom CSV");
    fs::write(
        temp.path().join("fixed.tsv"),
        b"id\tlabel\n1\t\"tab\tinside\"\n",
    )
    .expect("fixed TSV");
    fs::write(
        temp.path().join("bom.csv"),
        b"\xEF\xBB\xBFid;label\n1;bom-csv\n",
    )
    .expect("BOM CSV");
    fs::write(
        temp.path().join("bom.tsv"),
        b"\xEF\xBB\xBFid\tlabel\n1\tbom-tsv\n",
    )
    .expect("BOM TSV");
    fs::write(
        temp.path().join("bom.json"),
        b"\xEF\xBB\xBF[{\"id\":1,\"label\":\"bom-json\"}]",
    )
    .expect("BOM JSON");
    fs::write(
        temp.path().join("bom.ndjson"),
        b"\xEF\xBB\xBF{\"id\":1,\"label\":\"bom\"}\n",
    )
    .expect("BOM NDJSON");
    let connection = SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "dialects",
        serde_json::json!({
            "allowedRoots": [temp.path().to_str().expect("UTF-8 fixture path")],
            "csv": { "delimiter": ";", "quote": "'", "hasHeader": true },
            "tsv": { "hasHeader": true }
        }),
        CredentialRef::new("cred://local/dialects").expect("credential reference"),
    )
    .expect("connection");
    let registry = registry();
    for asset in discover(&registry, &connection).await {
        let preview = registry
            .preview(
                &connection,
                PreviewRequest::new(asset.clone(), 10, 1024 * 1024),
            )
            .await
            .unwrap_or_else(|error| panic!("preview {}: {error}", asset.name));
        assert_eq!(preview.rows_returned, 1, "{}", asset.name);
        assert_eq!(preview.schema.fields[0].name, "id", "{}", asset.name);
        assert_eq!(preview.schema.fields[1].name, "label", "{}", asset.name);
    }
}

#[tokio::test]
async fn empty_sources_zero_field_rows_and_nullability_are_preserved() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(temp.path().join("empty.csv"), b"").expect("empty CSV");
    fs::write(temp.path().join("empty.json"), b"[]").expect("empty JSON");
    fs::write(temp.path().join("empty.ndjson"), b"").expect("empty NDJSON");
    fs::write(temp.path().join("objects.json"), b"[{},{}]").expect("empty objects");
    fs::write(
        temp.path().join("all-null.ndjson"),
        b"{\"value\":null}\n{\"value\":null}\n",
    )
    .expect("all-null rows");
    fs::write(temp.path().join("nullable.csv"), b"id,label\n1,alpha\n2,\n").expect("nullable CSV");

    let connection = connection(temp.path());
    let registry = registry();
    let assets = discover(&registry, &connection).await;

    for name in ["empty.csv", "empty.json", "empty.ndjson"] {
        let asset = assets
            .iter()
            .find(|asset| asset.name == name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .clone();
        let metadata = registry
            .inspect(
                &connection,
                InspectRequest {
                    context: RequestContext::default(),
                    asset: asset.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("inspect {name}: {error}"));
        assert!(metadata.schema.fields.is_empty(), "{name}");
        let preview = registry
            .preview(&connection, PreviewRequest::new(asset, 10, 1024))
            .await
            .unwrap_or_else(|error| panic!("preview {name}: {error}"));
        assert_eq!(preview.rows_returned, 0, "{name}");
    }

    let objects = assets
        .iter()
        .find(|asset| asset.name == "objects.json")
        .expect("empty-object asset")
        .clone();
    let preview = registry
        .preview(&connection, PreviewRequest::new(objects.clone(), 10, 1))
        .await
        .expect("empty-object preview");
    assert!(preview.schema.fields.is_empty());
    assert_eq!(preview.rows_returned, 2);
    assert_eq!(preview.bytes_returned, 0);
    assert_eq!(preview.batches[0].payload().num_columns(), 0);
    let mut stream = registry
        .read_batches(&connection, ReadRequest::new(objects, 1))
        .await
        .expect("empty-object stream");
    let mut sizes = Vec::new();
    while let Some(item) = stream.next().await {
        sizes.push(item.expect("empty-object batch").row_count());
    }
    assert_eq!(sizes, [1, 1]);

    let all_null = assets
        .iter()
        .find(|asset| asset.name == "all-null.ndjson")
        .expect("all-null asset")
        .clone();
    let null_metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: all_null.clone(),
            },
        )
        .await
        .expect("all-null inspection");
    assert_eq!(null_metadata.schema.fields[0].data_type, LogicalType::Null);
    assert!(null_metadata.schema.fields[0].nullable);
    assert_eq!(
        registry
            .preview(&connection, PreviewRequest::new(all_null, 10, 1024 * 1024),)
            .await
            .expect("all-null preview")
            .rows_returned,
        2
    );

    let nullable = assets
        .iter()
        .find(|asset| asset.name == "nullable.csv")
        .expect("nullable CSV asset")
        .clone();
    let nullable_metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: nullable,
            },
        )
        .await
        .expect("nullable CSV inspection");
    assert!(!nullable_metadata.schema.fields[0].nullable);
    assert!(nullable_metadata.schema.fields[1].nullable);
}

#[tokio::test]
async fn schema_override_enforces_shape_type_and_required_values() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(temp.path().join("rows.csv"), b"id,label\n1,\n").expect("CSV fixture");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection)
        .await
        .pop()
        .expect("CSV asset");

    let required = LogicalSchema::new(vec![
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(101)),
            "id",
            LogicalType::Int64,
            false,
        )
        .expect("id override"),
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(102)),
            "label",
            LogicalType::Utf8,
            false,
        )
        .expect("label override"),
    ])
    .expect("required override");
    let mut request = ReadRequest::new(asset.clone(), 10);
    request.schema_override = Some(required);
    let mut stream = registry
        .read_batches(&connection, request)
        .await
        .expect("open override stream");
    let error = stream
        .next()
        .await
        .expect("override terminal item")
        .expect_err("required null must fail");
    assert_eq!(error.category(), ErrorCategory::SchemaDrift);

    let wrong_shape = LogicalSchema::new(vec![LogicalField::new(
        ColumnId::from_uuid(uuid::Uuid::from_u128(103)),
        "unknown",
        LogicalType::Utf8,
        true,
    )
    .expect("unknown field")])
    .expect("wrong shape");
    let mut preview = PreviewRequest::new(asset.clone(), 10, 1024);
    preview.schema_override = Some(wrong_shape);
    let error = registry
        .preview(&connection, preview)
        .await
        .expect_err("unknown override field");
    assert_eq!(error.category(), ErrorCategory::SchemaDrift);

    let wrong_type = LogicalSchema::new(vec![
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(104)),
            "id",
            LogicalType::Boolean,
            false,
        )
        .expect("boolean id"),
        LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(105)),
            "label",
            LogicalType::Utf8,
            true,
        )
        .expect("nullable label"),
    ])
    .expect("wrong type");
    let mut request = ReadRequest::new(asset, 10);
    request.schema_override = Some(wrong_type);
    let mut stream = registry
        .read_batches(&connection, request)
        .await
        .expect("open type override stream");
    let error = stream
        .next()
        .await
        .expect("type override terminal item")
        .expect_err("incompatible type must fail");
    assert_eq!(error.category(), ErrorCategory::SchemaDrift);
}

#[tokio::test]
async fn capabilities_checkpoint_deadline_and_pre_cancel_are_exact() {
    let temp = TempDir::new().expect("temporary fixture root");
    fs::write(temp.path().join("rows.csv"), b"id\n1\n").expect("CSV fixture");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection)
        .await
        .pop()
        .expect("CSV asset");
    let connector = LocalTabularConnector;
    let capabilities = connector.capabilities();
    assert!(capabilities.schema_discovery);
    assert!(capabilities.preview);
    assert!(capabilities.streaming);
    assert!(capabilities.column_projection);
    assert!(!capabilities.incremental_read);
    assert!(!capabilities.predicate_pushdown);
    assert!(!capabilities.range_read);
    assert!(!capabilities.change_tracking);
    assert!(matches!(
        connector
            .test_connection(
                &connection,
                stillflow_core::TestConnectionRequest {
                    context: RequestContext::default(),
                },
            )
            .await
            .expect("healthy connection"),
        stillflow_core::ConnectionStatus::Ok
    ));
    assert!(connector
        .checkpoint(
            &connection,
            CheckpointRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("checkpoint")
        .is_none());

    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    let error = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::with_cancellation(cancelled),
                asset: asset.clone(),
            },
        )
        .await
        .expect_err("cancel before open");
    assert_eq!(error.category(), ErrorCategory::Cancelled);

    let mut timed_out = ReadRequest::new(asset.clone(), 1);
    timed_out.context = RequestContext::with_deadline(tokio::time::Instant::now());
    let error = match registry.read_batches(&connection, timed_out).await {
        Ok(_) => panic!("deadline before open must fail"),
        Err(error) => error,
    };
    assert_eq!(error.category(), ErrorCategory::Timeout);
    assert!(error.retryable());

    let mut random = PreviewRequest::new(asset, 10, 1024);
    random.sampling = stillflow_core::SamplingStrategy::Random;
    let error = registry
        .preview(&connection, random)
        .await
        .expect_err("random sampling unsupported");
    assert_eq!(error.category(), ErrorCategory::UnsupportedCapability);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dropping_a_stream_releases_its_file_handle() {
    fn matching_descriptors(path: &std::path::Path) -> usize {
        fs::read_dir("/proc/self/fd")
            .expect("process descriptors")
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_link(entry.path()).ok())
            .filter(|target| target == path)
            .count()
    }

    let temp = TempDir::new().expect("temporary fixture root");
    let path = temp.path().join("rows.csv");
    fs::write(&path, b"id\n1\n2\n3\n").expect("CSV fixture");
    let connection = connection(temp.path());
    let registry = registry();
    let asset = discover(&registry, &connection)
        .await
        .pop()
        .expect("CSV asset");
    let stream = registry
        .read_batches(&connection, ReadRequest::new(asset, 1))
        .await
        .expect("open stream");
    assert!(matching_descriptors(&path) >= 1);
    drop(stream);
    assert_eq!(matching_descriptors(&path), 0);
}
