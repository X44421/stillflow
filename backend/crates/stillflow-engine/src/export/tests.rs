//! Export contract and golden tests.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    ArrayRef, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use chrono::{DateTime, Utc};
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::TempDir;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use stillflow_core::{
    logical_schema_to_arrow, BatchEnvelope, ColumnId, ExportDestination, ExportFormat,
    ExportPolicy, ExportShape, LogicalField, LogicalSchema, LogicalType, RequestContext, TimeUnit,
};
use stillflow_storage::{SnapshotDraft, SnapshotStore, StorageLimits};

use crate::export::{run_export, ExportRequest};
use crate::EngineError;

fn at(second: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(second, 0).expect("valid timestamp")
}
fn store(temp: &TempDir) -> SnapshotStore {
    SnapshotStore::open(temp.path(), StorageLimits::default()).expect("open store")
}
fn destination_root(temp: &TempDir) -> PathBuf {
    let root = temp.path().join("published");
    std::fs::create_dir_all(&root).expect("destination root");
    root
}
fn simple_schema() -> Arc<LogicalSchema> {
    Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(11)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("valid field")])
        .expect("valid schema"),
    )
}
fn draft(snapshot_id: Uuid, source_asset_id: Uuid, schema: &LogicalSchema) -> SnapshotDraft {
    SnapshotDraft::try_new(
        snapshot_id,
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        source_asset_id,
        schema.clone(),
        BTreeSet::from([Uuid::from_u128(9)]),
        Some(97),
        at(1_700_000_000),
    )
    .expect("valid draft")
}
fn envelope(
    schema: Arc<LogicalSchema>,
    source_asset_id: Uuid,
    sequence: u64,
    values: Vec<i64>,
) -> BatchEnvelope {
    let arrow_schema = logical_schema_to_arrow(&schema).expect("Arrow schema");
    let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))])
        .expect("record batch");
    BatchEnvelope::try_new(schema, source_asset_id, sequence, batch).expect("envelope")
}
fn publish(
    temp: &TempDir,
    snapshot_id: Uuid,
    source_asset_id: Uuid,
    schema: Arc<LogicalSchema>,
    partitions: Vec<Vec<i64>>,
) -> stillflow_storage::SnapshotManifest {
    let s = store(temp);
    let mut writer = s
        .begin_snapshot(
            draft(snapshot_id, source_asset_id, &schema),
            at(1_700_000_001),
        )
        .expect("begin snapshot");
    for (seq, values) in partitions.into_iter().enumerate() {
        writer
            .append(&envelope(
                Arc::clone(&schema),
                source_asset_id,
                seq as u64,
                values,
            ))
            .expect("append");
    }
    writer.commit().expect("commit")
}
/// Publishes a snapshot of exactly `total_rows` rows as full
/// `MAX_BATCH_ROWS` envelopes (one partition per envelope), which is the
/// densest legal snapshot shape for boundary tests.
fn publish_rows(
    temp: &TempDir,
    snapshot_id: Uuid,
    source_asset_id: Uuid,
    schema: Arc<LogicalSchema>,
    total_rows: u64,
) -> stillflow_storage::SnapshotManifest {
    let per_batch = stillflow_core::MAX_BATCH_ROWS as u64;
    let mut partitions = Vec::new();
    let mut remaining = total_rows;
    let mut value = 0_i64;
    while remaining > 0 {
        let take = usize::try_from(remaining.min(per_batch)).expect("rows fit usize");
        partitions.push(vec![value; take]);
        remaining -= take as u64;
        value += 1;
    }
    publish(temp, snapshot_id, source_asset_id, schema, partitions)
}
#[allow(clippy::too_many_arguments)]
fn export_request(
    export_id: Uuid,
    snapshot_id: Uuid,
    root: &Path,
    relative: &[&str],
    format: ExportFormat,
    shape: ExportShape,
    context: RequestContext,
    created_at: DateTime<Utc>,
) -> ExportRequest {
    ExportRequest {
        export_id,
        snapshot_id,
        format,
        policy: ExportPolicy { shape },
        destination: ExportDestination::local(
            root,
            relative.iter().map(|s| (*s).to_owned()).collect(),
            format,
            shape,
        )
        .expect("local destination"),
        created_at,
        context,
    }
}
fn artifact_path(root: &Path, relative: &[&str]) -> PathBuf {
    let mut p = root.to_path_buf();
    for c in relative {
        p.push(c);
    }
    p
}

#[test]
fn only_committed_visible_snapshot_is_accepted() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let s = store(&temp);
    let mut w = s
        .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
        .expect("begin");
    w.append(&envelope(Arc::clone(&schema), source, 0, vec![1, 2]))
        .expect("append");
    let manifest = w.commit().expect("commit");
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let nil_req = export_request(
        Uuid::nil(),
        snap,
        &root,
        &["reports", "nil.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let err = run_export(&s, nil_req).expect_err("nil export id must fail");
    assert!(matches!(err, EngineError::Connector(_)));
    let missing_req = export_request(
        Uuid::from_u128(100),
        Uuid::from_u128(999),
        &root,
        &["reports", "missing.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let err = run_export(&s, missing_req).expect_err("missing snapshot must fail");
    assert!(matches!(err, EngineError::Storage(_)));
    let draft_id = Uuid::from_u128(555);
    let _draft_writer = s
        .begin_snapshot(draft(draft_id, source, &schema), at(1_700_000_002))
        .expect("begin draft");
    let live_req = export_request(
        Uuid::from_u128(101),
        draft_id,
        &root,
        &["reports", "live.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let err = run_export(&s, live_req).expect_err("live draft must fail");
    assert!(matches!(err, EngineError::Storage(_)));
    let ok_req = export_request(
        Uuid::from_u128(102),
        snap,
        &root,
        &["reports", "ok.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let result = run_export(&s, ok_req).expect("visible snapshot must succeed");
    assert_eq!(result.row_count(), 2);
    assert!(artifact_path(&root, &["reports", "ok.csv"]).exists());
    let _ = manifest;
}

#[test]
fn input_verification_before_first_byte_fails_closed() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let manifest = publish(
        &temp,
        snap,
        source,
        Arc::clone(&schema),
        vec![vec![1, 2], vec![3]],
    );
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let partitions_dir = temp.path().join("partitions").join(snap.to_string());
    let _deleted = (|| {
        let entries = std::fs::read_dir(&partitions_dir).ok()?;
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_file() {
                let _ = std::fs::remove_file(&path);
                return Some(true);
            }
        }
        None
    })()
    .unwrap_or(false);
    let req = export_request(
        Uuid::from_u128(200),
        snap,
        &root,
        &["reports", "corrupt.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let err = run_export(&store(&temp), req).expect_err("corrupt input must fail");
    assert!(!artifact_path(&root, &["reports", "corrupt.csv"]).exists());
    assert!(matches!(
        err,
        EngineError::Storage(_) | EngineError::Internal(_) | EngineError::Connector(_)
    ));
    let _ = manifest;
}

fn csv_tsv_schema() -> Arc<LogicalSchema> {
    Arc::new(
        LogicalSchema::new(vec![
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(101)),
                "id",
                LogicalType::Int64,
                false,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(102)),
                "name",
                LogicalType::Utf8,
                true,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(103)),
                "note",
                LogicalType::Utf8,
                true,
            )
            .expect("field"),
        ])
        .expect("schema"),
    )
}
fn csv_tsv_envelope(
    schema: Arc<LogicalSchema>,
    source: Uuid,
    rows: Vec<(i64, Option<String>, Option<String>)>,
) -> BatchEnvelope {
    let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow schema");
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut notes = Vec::new();
    for (id, name, note) in rows {
        ids.push(id);
        names.push(name);
        notes.push(note);
    }
    let id_arr = Arc::new(Int64Array::from(ids)) as ArrayRef;
    let name_arr = Arc::new(StringArray::from(names)) as ArrayRef;
    let note_arr = Arc::new(StringArray::from(notes)) as ArrayRef;
    let batch =
        RecordBatch::try_new(arrow_schema, vec![id_arr, name_arr, note_arr]).expect("batch");
    BatchEnvelope::try_new(schema, source, 0, batch).expect("envelope")
}

#[test]
fn csv_golden_header_null_empty_quote_delimiter_lf_cr() {
    let temp = TempDir::new().expect("temp dir");
    let schema = csv_tsv_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let s = store(&temp);
    let mut writer = s
        .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
        .expect("begin");
    writer
        .append(&csv_tsv_envelope(
            Arc::clone(&schema),
            source,
            vec![
                (1, Some("a".to_owned()), None),
                (2, Some("".to_owned()), Some("".to_owned())),
                (3, Some("a,b".to_owned()), Some("x".to_owned())),
                (4, Some("a\"b".to_owned()), Some("y".to_owned())),
                (5, Some("a\nb".to_owned()), Some("z".to_owned())),
                (6, Some("a\rb".to_owned()), Some("w".to_owned())),
            ],
        ))
        .expect("append");
    let manifest = writer.commit().expect("commit");
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let req = export_request(
        Uuid::from_u128(300),
        snap,
        &root,
        &["reports", "golden.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let result = run_export(&s, req).expect("csv export");
    let bytes = std::fs::read(artifact_path(&root, &["reports", "golden.csv"])).expect("read csv");
    let text = String::from_utf8(bytes).expect("utf8");
    let expected = concat!(
        "id,name,note\n",
        "1,a,\n",
        "2,\"\",\"\"\n",
        "3,\"a,b\",x\n",
        "4,\"a\"\"b\",y\n",
        "5,\"a\nb\",z\n",
        "6,\"a\rb\",w\n"
    );
    assert_eq!(text, expected);
    assert_eq!(result.row_count(), 6);
    let _ = manifest;
}

#[test]
fn tsv_golden_same_edges_with_tab_delimiter() {
    let temp = TempDir::new().expect("temp dir");
    let schema = csv_tsv_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let s = store(&temp);
    let mut writer = s
        .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
        .expect("begin");
    writer
        .append(&csv_tsv_envelope(
            Arc::clone(&schema),
            source,
            vec![
                (1, Some("a".to_owned()), None),
                (2, Some("".to_owned()), Some("".to_owned())),
                (3, Some("a\tb".to_owned()), Some("x".to_owned())),
                (4, Some("a\"b".to_owned()), Some("y".to_owned())),
            ],
        ))
        .expect("append");
    let manifest = writer.commit().expect("commit");
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let req = export_request(
        Uuid::from_u128(301),
        snap,
        &root,
        &["reports", "golden.tsv"],
        ExportFormat::Tsv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    run_export(&s, req).expect("tsv export");
    let bytes = std::fs::read(artifact_path(&root, &["reports", "golden.tsv"])).expect("read tsv");
    let text = String::from_utf8(bytes).expect("utf8");
    let expected = "id\tname\tnote\n1\ta\t\n2\t\"\"\t\"\"\n3\t\"a\tb\"\tx\n4\t\"a\"\"b\"\ty\n";
    assert_eq!(text, expected);
    let _ = manifest;
}

fn jsonl_schema() -> Arc<LogicalSchema> {
    Arc::new(
        LogicalSchema::new(vec![
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(201)),
                "id",
                LogicalType::Int64,
                false,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(202)),
                "name",
                LogicalType::Utf8,
                true,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(203)),
                "score",
                LogicalType::Float64,
                true,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(204)),
                "ts",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: None,
                },
                true,
            )
            .expect("field"),
        ])
        .expect("schema"),
    )
}

#[test]
fn jsonl_golden_field_order_escaping_numeric_timestamp() {
    let temp = TempDir::new().expect("temp dir");
    let schema = jsonl_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow");
    let id_arr = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let name_arr = Arc::new(StringArray::from(vec![Some("a\"b\n"), Some("c")])) as ArrayRef;
    let score_arr = Arc::new(Float64Array::from(vec![Some(1.5), None])) as ArrayRef;
    let ts_arr = Arc::new(TimestampMillisecondArray::from(vec![
        Some(1609459200123),
        None,
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(arrow_schema, vec![id_arr, name_arr, score_arr, ts_arr])
        .expect("batch");
    let envelope = BatchEnvelope::try_new(Arc::clone(&schema), source, 0, batch).expect("envelope");
    let s = store(&temp);
    let mut writer = s
        .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
        .expect("begin");
    writer.append(&envelope).expect("append");
    let manifest = writer.commit().expect("commit");
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let req = export_request(
        Uuid::from_u128(302),
        snap,
        &root,
        &["reports", "golden.jsonl"],
        ExportFormat::Jsonl,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    run_export(&s, req).expect("jsonl export");
    let bytes = std::fs::read(artifact_path(&root, &["reports", "golden.jsonl"])).expect("read");
    let text = String::from_utf8(bytes).expect("utf8");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(text.ends_with('\n'));
    let v1: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
    assert_eq!(v1["id"], 1);
    assert_eq!(v1["name"], "a\"b\n");
    assert_eq!(v1["score"], 1.5);
    assert_eq!(v1["ts"], "2021-01-01T00:00:00.123Z");
    let _ = manifest;
}

fn parquet_schema_simple() -> Arc<LogicalSchema> {
    Arc::new(
        LogicalSchema::new(vec![
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(301)),
                "id",
                LogicalType::Int64,
                false,
            )
            .expect("field"),
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(302)),
                "data",
                LogicalType::Binary,
                true,
            )
            .expect("field"),
        ])
        .expect("schema"),
    )
}

#[test]
fn parquet_golden_canonical_schema_snappy_metadata() {
    let temp = TempDir::new().expect("temp dir");
    let schema = parquet_schema_simple();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let arrow_schema = logical_schema_to_arrow(&schema).expect("arrow");
    let id_arr = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let data_arr = Arc::new(BinaryArray::from(vec![Some(b"hello" as &[u8]), None])) as ArrayRef;
    let batch = RecordBatch::try_new(arrow_schema, vec![id_arr, data_arr]).expect("batch");
    let envelope = BatchEnvelope::try_new(Arc::clone(&schema), source, 0, batch).expect("envelope");
    let s = store(&temp);
    let mut writer = s
        .begin_snapshot(draft(snap, source, &schema), at(1_700_000_001))
        .expect("begin");
    writer.append(&envelope).expect("append");
    let manifest = writer.commit().expect("commit");
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let req = export_request(
        Uuid::from_u128(303),
        snap,
        &root,
        &["reports", "golden.parquet"],
        ExportFormat::Parquet,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    run_export(&s, req).expect("parquet export");
    let path = artifact_path(&root, &["reports", "golden.parquet"]);
    assert!(path.exists());
    let file = std::fs::File::open(&path).expect("open parquet");
    let reader = SerializedFileReader::new(file).expect("reader");
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    for col in row_group.columns() {
        assert_eq!(col.compression(), parquet::basic::Compression::SNAPPY);
    }
    let kvs = metadata.file_metadata().key_value_metadata().expect("kvs");
    let keys: Vec<_> = kvs.iter().map(|kv| kv.key.as_str()).collect();
    assert!(keys.contains(&"stillflow:schema_fingerprint"));
    assert!(keys.contains(&"stillflow:export_manifest_version"));
    assert!(keys.contains(&"stillflow:export_format_contract_version"));
    assert!(keys.contains(&"stillflow:export_encoder_version"));
    assert!(keys.contains(&"stillflow:engine_contract_version"));
    assert_eq!(metadata.num_row_groups(), 1);
    assert_eq!(metadata.file_metadata().num_rows(), 2);
    let _ = manifest;
}

#[test]
fn format_matrix_binary_non_finite() {
    let temp = TempDir::new().expect("temp dir");
    let binary_schema = Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(401)),
            "bin",
            LogicalType::Binary,
            false,
        )
        .expect("field")])
        .expect("schema"),
    );
    let float_schema = Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(403)),
            "f",
            LogicalType::Float64,
            false,
        )
        .expect("field")])
        .expect("schema"),
    );
    let source = Uuid::from_u128(4);
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let snap_bin = Uuid::from_u128(10);
    let arrow_schema = logical_schema_to_arrow(&binary_schema).expect("arrow");
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(BinaryArray::from(vec![b"hi" as &[u8]])) as ArrayRef],
    )
    .expect("batch");
    let env = BatchEnvelope::try_new(Arc::clone(&binary_schema), source, 0, batch).expect("env");
    let mut w = store(&temp)
        .begin_snapshot(draft(snap_bin, source, &binary_schema), at(1_700_000_001))
        .expect("begin");
    w.append(&env).expect("append");
    w.commit().expect("commit");
    let req = export_request(
        Uuid::from_u128(400),
        snap_bin,
        &root,
        &["reports", "bin.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let err = run_export(&store(&temp), req).expect_err("binary csv must fail");
    assert!(matches!(err, EngineError::TypeError(_)));
    let req = export_request(
        Uuid::from_u128(401),
        snap_bin,
        &root,
        &["reports", "bin.parquet"],
        ExportFormat::Parquet,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    run_export(&store(&temp), req).expect("binary parquet must succeed");
    let snap_float = Uuid::from_u128(12);
    let arrow_schema = logical_schema_to_arrow(&float_schema).expect("arrow");
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef],
    )
    .expect("batch");
    let env = BatchEnvelope::try_new(Arc::clone(&float_schema), source, 0, batch).expect("env");
    let mut w = store(&temp)
        .begin_snapshot(draft(snap_float, source, &float_schema), at(1_700_000_003))
        .expect("begin");
    w.append(&env).expect("append");
    w.commit().expect("commit");
    for (fid, fmt) in [
        (404, ExportFormat::Csv),
        (405, ExportFormat::Jsonl),
        (406, ExportFormat::Parquet),
    ] {
        let ext = fmt.extension();
        let req = export_request(
            Uuid::from_u128(fid),
            snap_float,
            &root,
            &["reports", &format!("nan.{ext}")],
            fmt,
            ExportShape::SingleFile,
            RequestContext::new(),
            created_at,
        );
        let err = run_export(&store(&temp), req).expect_err("non-finite must fail");
        assert!(matches!(err, EngineError::TypeError(_)));
    }
}

#[test]
fn deterministic_repeated_export_byte_equality() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let manifest = publish(
        &temp,
        snap,
        source,
        Arc::clone(&schema),
        vec![vec![3, 1], vec![2]],
    );
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let s = store(&temp);
    let req1 = export_request(
        Uuid::from_u128(500),
        snap,
        &root,
        &["reports", "det.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let r1 = run_export(&s, req1).expect("first export");
    let bytes1 = std::fs::read(artifact_path(&root, &["reports", "det.csv"])).expect("read1");
    let req2 = export_request(
        Uuid::from_u128(501),
        snap,
        &root,
        &["reports", "det2.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let r2 = run_export(&s, req2).expect("second export");
    let bytes2 = std::fs::read(artifact_path(&root, &["reports", "det2.csv"])).expect("read2");
    assert_eq!(bytes1, bytes2);
    assert_eq!(r1.files()[0].digest(), r2.files()[0].digest());
    let text = String::from_utf8(bytes1).expect("utf8");
    assert_eq!(text, "value\n3\n1\n2\n");
    let _ = manifest;
}

#[test]
fn single_file_and_partitioned_naming() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let manifest = publish(
        &temp,
        snap,
        source,
        Arc::clone(&schema),
        vec![vec![1], vec![2], vec![3]],
    );
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let snapshot_store = store(&temp);
    let req = export_request(
        Uuid::from_u128(600),
        snap,
        &root,
        &["reports", "single.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let r = run_export(&snapshot_store, req).expect("single");
    assert_eq!(r.files()[0].name(), "single.csv");
    assert_eq!(r.files().len(), 1);
    assert!(artifact_path(&root, &["reports", "single.csv"]).exists());
    let temp2 = TempDir::new().expect("temp2");
    let schema2 = simple_schema();
    let snap2 = Uuid::from_u128(2);
    let _m2 = publish(
        &temp2,
        snap2,
        source,
        Arc::clone(&schema2),
        vec![vec![10], vec![20]],
    );
    let root2 = destination_root(&temp2);
    let snapshot_store2 = store(&temp2);
    let req = export_request(
        Uuid::from_u128(601),
        snap2,
        &root2,
        &["reports", "parts"],
        ExportFormat::Csv,
        ExportShape::PartitionedSet,
        RequestContext::new(),
        created_at,
    );
    let r = run_export(&snapshot_store2, req).expect("partitioned");
    assert_eq!(r.files().len(), 2);
    assert_eq!(r.files()[0].name(), "part-0000000000.csv");
    assert_eq!(r.files()[1].name(), "part-0000000001.csv");
    let _ = manifest;
}

#[test]
fn cancellation_at_checkpoints_leaves_no_artifact() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let _m = publish(
        &temp,
        snap,
        source,
        Arc::clone(&schema),
        vec![vec![1, 2, 3]],
    );
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let s = store(&temp);
    let token = CancellationToken::new();
    token.cancel();
    let ctx = RequestContext::with_cancellation(token);
    let req = export_request(
        Uuid::from_u128(800),
        snap,
        &root,
        &["reports", "cancel_before.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        ctx,
        created_at,
    );
    let err = run_export(&s, req).expect_err("cancelled before verification");
    assert!(!format!("{err:?}").is_empty());
    assert!(!artifact_path(&root, &["reports", "cancel_before.csv"]).exists());
    let ctx = RequestContext::with_deadline(Instant::now() - Duration::from_millis(1));
    let req = export_request(
        Uuid::from_u128(801),
        snap,
        &root,
        &["reports", "deadline_before.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        ctx,
        created_at,
    );
    let err = run_export(&s, req).expect_err("deadline before verification");
    assert!(!format!("{err:?}").is_empty());
}

#[test]
fn deterministic_retry_after_cancellation_or_failure() {
    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let snap = Uuid::from_u128(1);
    let _m = publish(&temp, snap, source, Arc::clone(&schema), vec![vec![42]]);
    let root = destination_root(&temp);
    let created_at = at(1_700_100_000);
    let s = store(&temp);
    let token = CancellationToken::new();
    token.cancel();
    let ctx = RequestContext::with_cancellation(token);
    let req = export_request(
        Uuid::from_u128(900),
        snap,
        &root,
        &["reports", "retry.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        ctx,
        created_at,
    );
    assert!(run_export(&s, req).is_err());
    assert!(!artifact_path(&root, &["reports", "retry.csv"]).exists());
    let _ = s.recover(at(1_700_100_500), Duration::from_secs(60), 16);
    let req = export_request(
        Uuid::from_u128(900),
        snap,
        &root,
        &["reports", "retry.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let r1 = run_export(&s, req).expect("retry after cancel must succeed");
    let bytes1 = std::fs::read(artifact_path(&root, &["reports", "retry.csv"])).expect("read");
    let req = export_request(
        Uuid::from_u128(901),
        snap,
        &root,
        &["reports", "retry2.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let r2 = run_export(&s, req).expect("second retry");
    let bytes2 = std::fs::read(artifact_path(&root, &["reports", "retry2.csv"])).expect("read2");
    assert_eq!(bytes1, bytes2);
    assert_eq!(r1.files()[0].digest(), r2.files()[0].digest());
}

#[test]
fn no_api_job_surface_introduced() {
    // Compile-time guard: the api crate must keep existing, and its root
    // must never reference an export/job surface symbol. Routes are not
    // implemented yet, so lib.rs is the whole api source today; extend
    // this guard when more api files appear.
    let api_lib = include_str!("../../../stillflow-api/src/lib.rs");
    for surface in [
        "run_export",
        "ExportRequest",
        "ExportArtifact",
        "ExportFormat",
        "ExportPolicy",
    ] {
        assert!(
            !api_lib.contains(surface),
            "stillflow-api must not reference export surface symbol {surface}"
        );
    }
}

#[test]
fn bounds_rows_partitions_deadline() {
    use stillflow_core::export::{MAX_EXPORT_PARTITIONS, MAX_EXPORT_ROWS};

    assert_eq!(MAX_EXPORT_ROWS, 10_000_000);
    assert_eq!(MAX_EXPORT_PARTITIONS, 1_024);
    assert_eq!(
        crate::ENGINE_MAX_DEADLINE,
        Duration::from_secs(30 * 60),
        "engine deadline cap is ADR-004 §8"
    );

    let temp = TempDir::new().expect("temp dir");
    let schema = simple_schema();
    let source = Uuid::from_u128(4);
    let created_at = at(1_700_100_000);
    let root = destination_root(&temp);

    // An already-expired deadline fails typed before any store access:
    // the snapshot id does not exist, so only the deadline can fire.
    let expired = RequestContext::with_deadline(Instant::now() - Duration::from_secs(1));
    let req = export_request(
        Uuid::from_u128(700),
        Uuid::from_u128(999),
        &root,
        &["reports", "deadline.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        expired,
        created_at,
    );
    match run_export(&store(&temp), req) {
        Err(EngineError::Timeout) => {}
        other => panic!("expected Timeout before any I/O, got {other:?}"),
    }

    // Row bound: 10_000_001 valid rows fail before the first output byte.
    let snap = Uuid::from_u128(701);
    publish_rows(
        &temp,
        snap,
        source,
        Arc::clone(&schema),
        MAX_EXPORT_ROWS + 1,
    );
    let req = export_request(
        Uuid::from_u128(702),
        snap,
        &root,
        &["reports", "rows.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    match run_export(&store(&temp), req) {
        Err(EngineError::BoundExceeded(message)) => {
            assert!(message.contains("row bound"), "unexpected bound: {message}");
        }
        other => panic!("expected row BoundExceeded, got {other:?}"),
    }
    assert!(!artifact_path(&root, &["reports", "rows.csv"]).exists());

    // The exact cap is accepted: 10_000_000 rows export cleanly.
    let snap = Uuid::from_u128(703);
    publish_rows(&temp, snap, source, Arc::clone(&schema), MAX_EXPORT_ROWS);
    let req = export_request(
        Uuid::from_u128(704),
        snap,
        &root,
        &["reports", "rows_exact.csv"],
        ExportFormat::Csv,
        ExportShape::SingleFile,
        RequestContext::new(),
        created_at,
    );
    let result = run_export(&store(&temp), req).expect("exact row cap exports");
    assert_eq!(result.files().len(), 1);
    assert!(artifact_path(&root, &["reports", "rows_exact.csv"]).exists());

    // Partition bound: a 1_025-partition partitioned set fails in the
    // phase-1 verification battery, before encoding any byte.
    let snap = Uuid::from_u128(705);
    let partitions: Vec<Vec<i64>> = (0..=i64::from(MAX_EXPORT_PARTITIONS))
        .map(|i| vec![i])
        .collect();
    publish(&temp, snap, source, Arc::clone(&schema), partitions);
    let req = export_request(
        Uuid::from_u128(706),
        snap,
        &root,
        &["reports", "parts_over"],
        ExportFormat::Csv,
        ExportShape::PartitionedSet,
        RequestContext::new(),
        created_at,
    );
    match run_export(&store(&temp), req) {
        Err(EngineError::BoundExceeded(message)) => {
            assert!(message.contains("partition"), "unexpected bound: {message}");
        }
        other => panic!("expected partition BoundExceeded, got {other:?}"),
    }
    assert!(!artifact_path(&root, &["reports", "parts_over"]).exists());
}
