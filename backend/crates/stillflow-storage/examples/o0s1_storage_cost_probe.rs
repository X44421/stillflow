//! O0-S1 storage cost probe (issue #286) — measurement harness only.
//!
//! Runs representative snapshot writes, failure/recovery cases, and SQLite
//! control-plane operations against throwaway store roots and emits one JSON
//! object per line on stdout:
//!
//! - `{"kind":"info",...}` machine/mode/calibration facts;
//! - `{"kind":"sample",...}` per-iteration metrics (event aggregates from the
//!   opt-in `storage-metrics` instrumentation plus a harness-measured wall
//!   clock);
//! - `{"kind":"witness",...}` correctness witnesses (external SHA-256, verify
//!   results, publication-abort and corruption-detection checks);
//! - `{"kind":"conc_sample",...}` per-operation latency samples under the
//!   concurrency scenario.
//!
//! The harness changes no storage behavior: it only calls the public storage
//! API, reads files it owns, and drains the opt-in metrics log. Run it with
//! `cargo run -p stillflow-storage --features storage-metrics --example
//! o0s1_storage_cost_probe` so events are recorded; without the feature it
//! still runs and witnesses correctness but reports no metrics.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use chrono::Utc;
use serde_json::{json, Map, Value};
use sha2::{Digest as _, Sha256};
use stillflow_core::{
    logical_schema_to_arrow, AssetKind, BatchEnvelope, ColumnId, ConnectorKind, CredentialRef,
    LogicalField, LogicalSchema, LogicalType, TimeUnit,
};
use stillflow_storage::{
    metrics, ControlPlaneStore, IntegrityFailure, SnapshotDraft, SnapshotManifest, SnapshotStore,
    StorageError, StorageLimits,
};
use uuid::Uuid;

const DIGEST_BUFFER_BYTES: usize = 64 * 1024; // matches storage::DIGEST_BUFFER_BYTES

fn main() {
    let mut iterations = 7_usize;
    let mut b_op_iterations = 30_usize;
    let mut seq_iterations = 15_usize;
    let mut conc_ms = 1_500_u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" => {
                iterations = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(iterations);
            }
            "--b-op-iterations" => {
                b_op_iterations = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(b_op_iterations);
            }
            "--seq-iterations" => {
                seq_iterations = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(seq_iterations);
            }
            "--conc-ms" => {
                conc_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(conc_ms);
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }

    emit(
        "info",
        json!({
            "probe": "o0s1_storage_cost_probe",
            "issue": 286,
            "instrumentation_enabled": metrics::enabled(),
            "iterations": iterations,
            "b_op_iterations": b_op_iterations,
            "seq_iterations": seq_iterations,
            "conc_ms": conc_ms,
        }),
    );
    emit("info", machine_info());

    let sha_ns_per_byte = sha256_calibration();
    emit(
        "info",
        json!({
            "calibration": "sha256",
            "buffer_bytes": DIGEST_BUFFER_BYTES,
            "ns_per_byte": sha_ns_per_byte,
            "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file",
        }),
    );

    let base = std::env::temp_dir().join(format!(
        "o0s1-probe-{}-{}",
        std::process::id(),
        Utc::now().timestamp_millis()
    ));
    fs::create_dir_all(&base).expect("create probe base directory");

    let small = fixture(&FixtureKind::Small);
    let medium = fixture(&FixtureKind::Medium);
    let wide = fixture(&FixtureKind::Wide);
    let longvar = fixture(&FixtureKind::LongVar);
    for (name, fixture) in [
        ("small", &small),
        ("medium", &medium),
        ("wide", &wide),
        ("longvar", &longvar),
    ] {
        emit(
            "info",
            json!({
                "fixture": name,
                "rows": fixture.envelope.row_count(),
                "columns": fixture.schema.fields.len(),
                "logical_bytes": fixture.envelope.byte_count(),
            }),
        );
    }

    // Part A — representative snapshot writes (small / medium / wide / long).
    for (name, fixture) in [
        ("small", &small),
        ("medium", &medium),
        ("wide", &wide),
        ("longvar", &longvar),
    ] {
        scenario_a_write(&base, name, fixture, iterations);
    }

    // Part A — failure/recovery: dropped writer must not publish anything.
    scenario_a_fail_drop(&base, &medium);

    // Part A — failure/recovery: corruption detection still gates reads.
    scenario_a_fail_corrupt(&base, &small);

    // Part B — single-operation latency attribution.
    let b_setup = setup_part_b(&base, &small);
    scenario_b_op_load_manifest(&b_setup, b_op_iterations);
    scenario_b_op_create_dataset(&b_setup, b_op_iterations);

    // Part B — realistic short operation sequence.
    scenario_b_sequence(&b_setup, &small, seq_iterations);

    // Part B — lock/busy behavior under existing supported concurrency.
    scenario_b_concurrency(&b_setup, &small, conc_ms);

    emit(
        "info",
        json!({
            "phase": "final",
            "vm_hwm_kib": vm_hwm_kib(),
        }),
    );
    let _ = fs::remove_dir_all(&base);
}

struct Fixture {
    schema: Arc<LogicalSchema>,
    envelope: BatchEnvelope,
}

impl Clone for Fixture {
    fn clone(&self) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            envelope: self.envelope.clone(),
        }
    }
}

enum FixtureKind {
    Small,
    Medium,
    Wide,
    LongVar,
}

struct XorShift(u32);

impl XorShift {
    fn next(&mut self) -> u32 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.0 = x;
        x
    }

    fn text(&mut self, max_len: u32) -> String {
        let len = (self.next() % max_len.max(1)) as usize;
        let byte = b'a' + (self.next() % 26) as u8;
        std::iter::repeat(byte as char).take(len).collect()
    }
}

fn fixture(kind: &FixtureKind) -> Fixture {
    let mut rng = XorShift(0x9E37_79B9);
    let rows = match kind {
        FixtureKind::Small => 1_000,
        FixtureKind::Medium => 65_536,
        FixtureKind::Wide => 2_000,
        FixtureKind::LongVar => 65_536,
    };
    let (schema_fields, arrays): (Vec<LogicalField>, Vec<ArrayRef>) = match kind {
        FixtureKind::Small => {
            let mut ids = Vec::new();
            let mut int_values = Vec::with_capacity(rows);
            let mut float_values = Vec::with_capacity(rows);
            let mut text_values = Vec::with_capacity(rows);
            let mut bool_values = Vec::with_capacity(rows);
            for i in 0..rows {
                int_values.push(i as i64);
                float_values.push((i as f64) * 0.5);
                text_values.push(format!("label-{i}"));
                bool_values.push(i % 2 == 0);
            }
            for _ in 0..4 {
                ids.push(ColumnId::from_uuid(Uuid::new_v4()));
            }
            let fields = vec![
                field(ids[0], "id", LogicalType::Int64),
                field(ids[1], "value", LogicalType::Float64),
                field(ids[2], "label", LogicalType::Utf8),
                field(ids[3], "flag", LogicalType::Boolean),
            ];
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(int_values)),
                Arc::new(Float64Array::from(float_values)),
                Arc::new(StringArray::from(text_values)),
                Arc::new(BooleanArray::from(bool_values)),
            ];
            (fields, arrays)
        }
        FixtureKind::Medium => {
            let mut int_values = Vec::with_capacity(rows);
            let mut int32_values = Vec::with_capacity(rows);
            let mut float_values = Vec::with_capacity(rows);
            let mut text_values = Vec::with_capacity(rows);
            let mut bool_values = Vec::with_capacity(rows);
            let mut date_values = Vec::with_capacity(rows);
            let mut ts_values = Vec::with_capacity(rows);
            let mut u32_values = Vec::with_capacity(rows);
            for i in 0..rows {
                int_values.push(i as i64);
                int32_values.push(i as i32);
                float_values.push((i as f64) * 1.25);
                text_values.push(format!("row-{i}-{}", rng.text(16)));
                bool_values.push(i % 3 != 0);
                date_values.push((i % 20_000) as i32);
                ts_values.push(1_700_000_000_000 + i as i64);
                u32_values.push(i as u32);
            }
            let fields = vec![
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "id",
                    LogicalType::Int64,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "num",
                    LogicalType::Int32,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "ratio",
                    LogicalType::Float64,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "note",
                    LogicalType::Utf8,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "flag",
                    LogicalType::Boolean,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "day",
                    LogicalType::Date32,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "seen_at",
                    LogicalType::Timestamp {
                        unit: TimeUnit::Microsecond,
                        timezone: None,
                    },
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "code",
                    LogicalType::UInt32,
                ),
            ];
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(int_values)),
                Arc::new(Int32Array::from(int32_values)),
                Arc::new(Float64Array::from(float_values)),
                Arc::new(StringArray::from(text_values)),
                Arc::new(BooleanArray::from(bool_values)),
                Arc::new(Date32Array::from(date_values)),
                Arc::new(TimestampMicrosecondArray::from(ts_values)),
                Arc::new(UInt32Array::from(u32_values)),
            ];
            (fields, arrays)
        }
        FixtureKind::Wide => {
            // 50 groups of (Int64, Float64, Utf8, Boolean) = 200 columns.
            let mut fields = Vec::with_capacity(200);
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(200);
            for group in 0..50 {
                let mut int_values = Vec::with_capacity(rows);
                let mut float_values = Vec::with_capacity(rows);
                let mut text_values = Vec::with_capacity(rows);
                let mut bool_values = Vec::with_capacity(rows);
                for i in 0..rows {
                    int_values.push((group * rows + i) as i64);
                    float_values.push(((group * rows + i) as f64) * 0.25);
                    text_values.push(format!("g{group}r{i}"));
                    bool_values.push(i % 4 == 0);
                }
                fields.push(field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    format!("g{group}_id"),
                    LogicalType::Int64,
                ));
                fields.push(field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    format!("g{group}_amount"),
                    LogicalType::Float64,
                ));
                fields.push(field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    format!("g{group}_tag"),
                    LogicalType::Utf8,
                ));
                fields.push(field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    format!("g{group}_flag"),
                    LogicalType::Boolean,
                ));
                columns.push(Arc::new(Int64Array::from(int_values)));
                columns.push(Arc::new(Float64Array::from(float_values)));
                columns.push(Arc::new(StringArray::from(text_values)));
                columns.push(Arc::new(BooleanArray::from(bool_values)));
            }
            (fields, columns)
        }
        FixtureKind::LongVar => {
            // Variable-length dominated: mostly Utf8/Binary with row-varying
            // lengths plus a nullable text column with ~10% nulls.
            let mut int_values = Vec::with_capacity(rows);
            let mut text_a: Vec<String> = Vec::with_capacity(rows);
            let mut text_b: Vec<String> = Vec::with_capacity(rows);
            let mut nullable: Vec<Option<String>> = Vec::with_capacity(rows);
            let mut blob_values: Vec<Vec<u8>> = Vec::with_capacity(rows);
            let mut fixed_values = Vec::with_capacity(rows);
            for i in 0..rows {
                int_values.push(i as i64);
                text_a.push(rng.text(256));
                text_b.push(rng.text(256));
                nullable.push(if i % 10 == 0 {
                    None
                } else {
                    Some(rng.text(128))
                });
                let blob_len = (rng.next() % 256) as usize;
                let byte = (rng.next() % 256) as u8;
                blob_values.push(vec![byte; blob_len]);
                fixed_values.push(format!("k-{i}"));
            }
            let fields = vec![
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "id",
                    LogicalType::Int64,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "note_a",
                    LogicalType::Utf8,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "note_b",
                    LogicalType::Utf8,
                ),
                nullable_field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "comment",
                    LogicalType::Utf8,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "payload",
                    LogicalType::Binary,
                ),
                field(
                    ColumnId::from_uuid(Uuid::new_v4()),
                    "key",
                    LogicalType::Utf8,
                ),
            ];
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(int_values)),
                Arc::new(StringArray::from(text_a)),
                Arc::new(StringArray::from(text_b)),
                Arc::new(StringArray::from(nullable)),
                Arc::new(BinaryArray::from_iter_values(blob_values)),
                Arc::new(StringArray::from(fixed_values)),
            ];
            (fields, arrays)
        }
    };
    let schema =
        Arc::new(LogicalSchema::new(schema_fields).expect("probe fixture schema must be valid"));
    let arrow_schema = logical_schema_to_arrow(&schema).expect("probe fixture arrow schema");
    let batch = RecordBatch::try_new(arrow_schema, arrays).expect("probe fixture batch");
    let envelope = BatchEnvelope::try_new(Arc::clone(&schema), Uuid::new_v4(), 0, batch)
        .expect("probe fixture envelope");
    Fixture { schema, envelope }
}

fn field(id: ColumnId, name: impl Into<String>, data_type: LogicalType) -> LogicalField {
    LogicalField::new(id, name, data_type, false).expect("probe fixture field")
}

fn nullable_field(id: ColumnId, name: impl Into<String>, data_type: LogicalType) -> LogicalField {
    LogicalField::new(id, name, data_type, true).expect("probe fixture field")
}

// ---------------------------------------------------------------------------
// Part A scenarios
// ---------------------------------------------------------------------------

fn scenario_a_write(base: &Path, name: &str, fixture: &Fixture, iterations: usize) {
    let scenario = format!("a.write.{name}");
    let root = base.join(&scenario);
    let open_started = Instant::now();
    let store = SnapshotStore::open(&root, StorageLimits::default()).expect("open store");
    emit(
        "info",
        json!({
            "scenario": scenario,
            "phase": "store_open_with_migrate",
            "wall_ns": open_started.elapsed().as_nanos() as u64,
        }),
    );

    let dataset_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let source_asset_id = fixture.envelope.source_asset_id();
    for iteration in 0..iterations {
        let _ = metrics::drain();
        let started = Utc::now();
        let t0 = Instant::now();
        let draft = SnapshotDraft::try_new(
            Uuid::new_v4(),
            dataset_id,
            session_id,
            source_asset_id,
            (*fixture.schema).clone(),
            BTreeSet::new(),
            None,
            started,
        )
        .expect("draft");
        let mut writer = store
            .begin_snapshot(draft, started)
            .expect("begin snapshot");
        writer.append(&fixture.envelope).expect("append");
        let manifest = writer.commit().expect("commit");
        let wall_ns = t0.elapsed().as_nanos() as u64;

        let events = metrics::drain();
        let mut sample = aggregate_events(&events);
        sample.insert("wall_ns".into(), json!(wall_ns));
        sample.insert("vm_hwm_kib".into(), json!(vm_hwm_kib()));
        emit_sample(&scenario, iteration, sample);
        if iteration + 1 == iterations {
            emit("witness", write_witness(&root, &store, &manifest));
        }
    }
}

/// External correctness witness: the committed manifest digest must equal an
/// independent SHA-256 over the published file bytes, the published file
/// length must equal the manifest stored byte count, full verification must
/// pass, and the committed logical version digest must be present.
fn write_witness(root: &Path, store: &SnapshotStore, manifest: &SnapshotManifest) -> Value {
    let snapshot = manifest.snapshot();
    let snapshot_id = snapshot.id();
    let partition = &manifest.partitions()[0];
    let path = root
        .join("partitions")
        .join(snapshot_id.to_string())
        .join(format!(
            "{:010}-{}.parquet",
            partition.sequence(),
            partition.digest()
        ));
    let file_bytes = fs::read(&path).expect("read published partition for witness");
    let mut hasher = Sha256::new();
    hasher.update(&file_bytes);
    let file_sha256 = hex(&hasher.finalize());
    let digest_match = file_sha256 == partition.digest().to_string();
    let size_match = file_bytes.len() as u64 == partition.stored_byte_count();
    let verify_ok = store.verify_snapshot(snapshot_id).is_ok();
    let version_digest = store
        .version_digest(snapshot_id)
        .map(|digest| hex(&digest))
        .unwrap_or_else(|_| "unavailable".to_string());
    json!({
        "scenario": "write_witness",
        "snapshot_id": snapshot_id.to_string(),
        "partition_count": manifest.partitions().len(),
        "file_size": file_bytes.len(),
        "file_sha256": file_sha256,
        "manifest_digest": partition.digest().to_string(),
        "digest_match": digest_match,
        "stored_byte_count": partition.stored_byte_count(),
        "size_match": size_match,
        "verify_ok": verify_ok,
        "version_digest": version_digest,
    })
}

fn scenario_a_fail_drop(base: &Path, fixture: &Fixture) {
    let scenario = "a.fail.drop";
    let root = base.join(scenario);
    let store = SnapshotStore::open(&root, StorageLimits::default()).expect("open store");
    let _ = metrics::drain();
    let started = Utc::now();
    let t0 = Instant::now();
    let draft = SnapshotDraft::try_new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        fixture.envelope.source_asset_id(),
        (*fixture.schema).clone(),
        BTreeSet::new(),
        None,
        started,
    )
    .expect("draft");
    let snapshot_id = draft.id();
    let mut writer = store
        .begin_snapshot(draft, started)
        .expect("begin snapshot");
    writer.append(&fixture.envelope).expect("append");
    let staging_dir = root.join("staging").join(snapshot_id.to_string());
    let staged_present_after_append = staging_dir.is_dir();
    drop(writer); // abort path: staging cleanup + publication journal delete
    let wall_ns = t0.elapsed().as_nanos() as u64;

    let manifest_result = store.load_manifest(snapshot_id);
    let staging_removed = !staging_dir.exists();
    let recovery = store
        .recover(Utc::now(), Duration::from_secs(1), 16)
        .expect("recover");
    let events = metrics::drain();
    let mut sample = aggregate_events(&events);
    sample.insert("wall_ns".into(), json!(wall_ns));
    sample.insert("vm_hwm_kib".into(), json!(vm_hwm_kib()));
    emit_sample(scenario, 0, sample);
    emit(
        "witness",
        json!({
            "scenario": scenario,
            "snapshot_id": snapshot_id.to_string(),
            "staged_present_after_append": staged_present_after_append,
            "staging_removed_after_drop": staging_removed,
            "manifest_not_found": manifest_result.is_err(),
            "recover_examined": recovery.examined(),
            "recover_recovered": recovery.recovered(),
            "recorded_parquet_write": count_events(&events, "parquet_write_count") > 0,
            "recorded_partition_install": count_events(&events, "partition_install_count") > 0,
            "recorded_manifest_commit": count_events(&events, "db_op_manifest_commit_count") > 0,        }),
    );
}

fn scenario_a_fail_corrupt(base: &Path, fixture: &Fixture) {
    let scenario = "a.fail.corrupt";
    let root = base.join(scenario);
    let store = SnapshotStore::open(&root, StorageLimits::default()).expect("open store");
    let _ = metrics::drain();
    let started = Utc::now();
    let draft = SnapshotDraft::try_new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        fixture.envelope.source_asset_id(),
        (*fixture.schema).clone(),
        BTreeSet::new(),
        None,
        started,
    )
    .expect("draft");
    let snapshot_id = draft.id();
    let mut writer = store
        .begin_snapshot(draft, started)
        .expect("begin snapshot");
    writer.append(&fixture.envelope).expect("append");
    let manifest = writer.commit().expect("commit");

    // Deliberately corrupt one published byte from outside the storage API.
    let partition = &manifest.partitions()[0];
    let path = root
        .join("partitions")
        .join(snapshot_id.to_string())
        .join(format!(
            "{:010}-{}.parquet",
            partition.sequence(),
            partition.digest()
        ));
    let mut bytes = fs::read(&path).expect("read published partition");
    let offset = bytes.len() / 2;
    bytes[offset] ^= 0xFF;
    fs::write(&path, &bytes).expect("write corrupted partition");

    let verify_result = store.verify_snapshot(snapshot_id);
    let corruption_detected = matches!(
        &verify_result,
        Err(StorageError::Integrity {
            kind: IntegrityFailure::DigestMismatch,
            ..
        })
    );
    let verify_error = verify_result.err().map(|error| error.to_string());
    let read_result = store.read_batches(snapshot_id).and_then(|reader| {
        for batch in reader {
            batch?;
        }
        Ok(())
    });
    let read_error = read_result.err().map(|error| error.to_string());
    let events = metrics::drain();
    emit(
        "witness",
        json!({
            "scenario": scenario,
            "snapshot_id": snapshot_id.to_string(),
            "corrupted_offset": offset,
            "corruption_detected": corruption_detected,
            "verify_error": verify_error,
            "read_path_error": read_error,
            "verify_reread_events_recorded": count_events(&events, "verify_reread_count") > 0,
        }),
    );
}

// ---------------------------------------------------------------------------
// Part B scenarios
// ---------------------------------------------------------------------------

struct PartB {
    store: SnapshotStore,
    control_plane: ControlPlaneStore,
    workspace_id: Uuid,
    session_id: Uuid,
    source_asset_id: Uuid,
    snapshot_ids: Vec<Uuid>,
}

fn setup_part_b(base: &Path, fixture: &Fixture) -> PartB {
    let root = base.join("part-b");
    let store = SnapshotStore::open(&root, StorageLimits::default()).expect("open store");
    let control_plane = store.control_plane();
    let workspace_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let connection_id = Uuid::new_v4();
    // Snapshots appended in Part B must carry the fixture envelope's lineage,
    // so the control-plane asset reuses the envelope's source identity.
    let source_asset_id = fixture.envelope.source_asset_id();
    let now = Utc::now();
    control_plane
        .create_workspace(workspace_id, now)
        .expect("create workspace");
    control_plane
        .create_session(workspace_id, session_id, now)
        .expect("create session");
    control_plane
        .create_source_connection(
            workspace_id,
            connection_id,
            ConnectorKind::LocalFile,
            "probe connection",
            json!({"path": "probe.csv"}),
            CredentialRef::new("cred://probe/local").expect("credential ref"),
            now,
        )
        .expect("create source connection");
    control_plane
        .create_source_asset(
            workspace_id,
            connection_id,
            source_asset_id,
            AssetKind::File,
            "probe asset",
            json!({"path": "probe.csv"}),
            now,
        )
        .expect("create source asset");

    // Seed eight visible snapshots for the reader/concurrency scenarios.
    let mut snapshot_ids = Vec::new();
    for _ in 0..8 {
        let started = Utc::now();
        let draft = SnapshotDraft::try_new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            source_asset_id,
            (*fixture.schema).clone(),
            BTreeSet::new(),
            None,
            started,
        )
        .expect("draft");
        let mut writer = store
            .begin_snapshot(draft, started)
            .expect("begin snapshot");
        writer.append(&fixture.envelope).expect("append");
        let manifest = writer.commit().expect("commit");
        snapshot_ids.push(manifest.snapshot().id());
    }
    PartB {
        store,
        control_plane,
        workspace_id,
        session_id,
        source_asset_id,
        snapshot_ids,
    }
}

fn scenario_b_op_load_manifest(setup: &PartB, iterations: usize) {
    let scenario = "b.op.load_manifest";
    let target = setup.snapshot_ids[0];
    for iteration in 0..iterations {
        let _ = metrics::drain();
        let t0 = Instant::now();
        let manifest = setup.store.load_manifest(target).expect("load manifest");
        let wall_ns = t0.elapsed().as_nanos() as u64;
        let events = metrics::drain();
        let mut sample = aggregate_events(&events);
        sample.insert("wall_ns".into(), json!(wall_ns));
        sample.insert("partitions".into(), json!(manifest.partitions().len()));
        emit_sample(scenario, iteration, sample);
    }
}

fn scenario_b_op_create_dataset(setup: &PartB, iterations: usize) {
    let scenario = "b.op.create_dataset";
    for iteration in 0..iterations {
        let _ = metrics::drain();
        let t0 = Instant::now();
        let record = setup
            .control_plane
            .create_dataset(
                setup.workspace_id,
                setup.session_id,
                setup.source_asset_id,
                Uuid::new_v4(),
                format!("probe-dataset-{iteration}"),
                Utc::now(),
            )
            .expect("create dataset");
        let wall_ns = t0.elapsed().as_nanos() as u64;
        let events = metrics::drain();
        let mut sample = aggregate_events(&events);
        sample.insert("wall_ns".into(), json!(wall_ns));
        sample.insert("dataset_id".into(), json!(record.id.to_string()));
        emit_sample(scenario, iteration, sample);
    }
}

/// One realistic short sequence: publish one small snapshot, read its
/// manifest back, stream its partition, and compute its logical version
/// digest. Counts every SQLite connection opened along the way.
fn scenario_b_sequence(setup: &PartB, fixture: &Fixture, iterations: usize) {
    let scenario = "b.seq.publish_read";
    let dataset_id = Uuid::new_v4();
    for iteration in 0..iterations {
        let _ = metrics::drain();
        let started = Utc::now();
        let t0 = Instant::now();
        let draft = SnapshotDraft::try_new(
            Uuid::new_v4(),
            dataset_id,
            Uuid::new_v4(),
            setup.source_asset_id,
            (*fixture.schema).clone(),
            BTreeSet::new(),
            None,
            started,
        )
        .expect("draft");
        let mut writer = setup
            .store
            .begin_snapshot(draft, started)
            .expect("begin snapshot");
        writer.append(&fixture.envelope).expect("append");
        let manifest = writer.commit().expect("commit");
        let snapshot_id = manifest.snapshot().id();
        let reloaded = setup.store.load_manifest(snapshot_id).expect("reload");
        let batches = setup.store.read_batches(snapshot_id).expect("read batches");
        let mut batch_count = 0_usize;
        for batch in batches {
            batch.expect("batch");
            batch_count += 1;
        }
        let version_digest = setup
            .store
            .version_digest(snapshot_id)
            .expect("version digest");
        let wall_ns = t0.elapsed().as_nanos() as u64;
        let events = metrics::drain();
        let mut sample = aggregate_events(&events);
        sample.insert("wall_ns".into(), json!(wall_ns));
        sample.insert("batch_count".into(), json!(batch_count));
        sample.insert(
            "reloaded_partitions".into(),
            json!(reloaded.partitions().len()),
        );
        sample.insert("version_digest".into(), json!(hex(&version_digest)));
        emit_sample(scenario, iteration, sample);
    }
}

/// Mixed reader/publisher load within the existing supported concurrency
/// limits (default reader/publisher ceilings; SQLite WAL plus the frozen
/// 5-second busy timeout). Emits per-operation latency samples, busy/error
/// counts, and the sampled peak of SQLite-related open file descriptors.
fn scenario_b_concurrency(setup: &PartB, fixture: &Fixture, duration_ms: u64) {
    let scenario = "b.conc.mixed";
    let stop = Arc::new(AtomicBool::new(false));
    let samples: Arc<Mutex<Vec<(String, usize, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let reader_busy = Arc::new(AtomicU64::new(0));
    let reader_errors = Arc::new(AtomicU64::new(0));
    let publisher_busy = Arc::new(AtomicU64::new(0));
    let publisher_errors = Arc::new(AtomicU64::new(0));
    let fd_peak = Arc::new(AtomicU64::new(0));
    let total_fd_peak = Arc::new(AtomicU64::new(0));

    let sampler = {
        let stop = Arc::clone(&stop);
        let fd_peak = Arc::clone(&fd_peak);
        let total_fd_peak = Arc::clone(&total_fd_peak);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (sqlite_fds, total_fds) = count_sqlite_fds();
                fd_peak.fetch_max(sqlite_fds, Ordering::Relaxed);
                total_fd_peak.fetch_max(total_fds, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };

    let mut handles = Vec::new();
    for reader_index in 0..4 {
        let store = setup.store.clone();
        let snapshot_ids = setup.snapshot_ids.clone();
        let stop = Arc::clone(&stop);
        let samples = Arc::clone(&samples);
        let busy = Arc::clone(&reader_busy);
        let errors = Arc::clone(&reader_errors);
        handles.push(std::thread::spawn(move || {
            let mut sequence = 0_usize;
            let mut local = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let target = snapshot_ids[sequence % snapshot_ids.len()];
                sequence += 1;
                let t0 = Instant::now();
                match store.load_manifest(target) {
                    Ok(manifest) => {
                        let _ = manifest.partitions().len();
                        local.push(t0.elapsed().as_nanos() as u64);
                    }
                    Err(StorageError::Busy(_)) => {
                        busy.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            let mut all = samples.lock().expect("samples lock");
            for op_ns in local.iter().take(10_000) {
                all.push(("reader".to_string(), reader_index, *op_ns));
            }
            local.len() as u64
        }))
    }
    for publisher_index in 0..2 {
        let store = setup.store.clone();
        let fixture = fixture.clone();
        let source_asset_id = setup.source_asset_id;
        let stop = Arc::clone(&stop);
        let samples = Arc::clone(&samples);
        let busy = Arc::clone(&publisher_busy);
        let errors = Arc::clone(&publisher_errors);
        handles.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let started = Utc::now();
                let t0 = Instant::now();
                let draft = SnapshotDraft::try_new(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    source_asset_id,
                    (*fixture.schema).clone(),
                    BTreeSet::new(),
                    None,
                    started,
                )
                .expect("draft");
                match store.begin_snapshot(draft, started).and_then(|mut writer| {
                    writer.append(&fixture.envelope)?;
                    writer.commit()
                }) {
                    Ok(_) => local.push(t0.elapsed().as_nanos() as u64),
                    Err(StorageError::Busy(_)) => {
                        busy.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            let mut all = samples.lock().expect("samples lock");
            for op_ns in local.iter().take(10_000) {
                all.push(("publisher".to_string(), publisher_index, *op_ns));
            }
            local.len() as u64
        }));
    }

    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        let op_count = handle.join().expect("join worker");
        let _ = op_count;
    }
    sampler.join().expect("join sampler");

    let all = samples.lock().expect("samples lock");
    let mut reader_ops = Vec::new();
    let mut publisher_ops = Vec::new();
    for (role, _, op_ns) in all.iter() {
        if role == "reader" {
            reader_ops.push(*op_ns);
        } else {
            publisher_ops.push(*op_ns);
        }
    }
    emit(
        "info",
        json!({
            "scenario": scenario,
            "reader_threads": 4,
            "publisher_threads": 2,
            "reader_op_count": reader_ops.len(),
            "publisher_op_count": publisher_ops.len(),
            "reader_busy": reader_busy.load(Ordering::Relaxed),
            "reader_errors": reader_errors.load(Ordering::Relaxed),
            "publisher_busy": publisher_busy.load(Ordering::Relaxed),
            "publisher_errors": publisher_errors.load(Ordering::Relaxed),
            "sqlite_fd_peak_sampled": fd_peak.load(Ordering::Relaxed),
            "total_fd_peak_sampled": total_fd_peak.load(Ordering::Relaxed),
            "reader_p50_ns": percentile(&reader_ops, 0.50),
            "reader_p95_ns": percentile(&reader_ops, 0.95),
            "publisher_p50_ns": percentile(&publisher_ops, 0.50),
            "publisher_p95_ns": percentile(&publisher_ops, 0.95),
            "sample_note": "per-thread samples capped at 10,000",
        }),
    );
    for (role, thread_index, op_ns) in all.iter() {
        emit(
            "conc_sample",
            json!({
                "scenario": scenario,
                "role": role,
                "thread": thread_index,
                "op_ns": op_ns,
            }),
        );
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn aggregate_events(events: &[metrics::Event]) -> Map<String, Value> {
    let mut out = Map::new();
    let mut insert = |key: String, value: u64| {
        out.entry(key)
            .and_modify(|existing| {
                *existing = json!(existing.as_u64().unwrap_or(0) + value);
            })
            .or_insert_with(|| json!(value));
    };
    for event in events {
        match *event {
            metrics::Event::ConnectionOpen {
                open_ns,
                configure_ns,
                pragma_count,
            } => {
                insert("conn_open_count".into(), 1);
                insert("conn_open_open_ns".into(), open_ns);
                insert("conn_open_configure_ns".into(), configure_ns);
                insert("conn_open_pragma_count".into(), u64::from(pragma_count));
            }
            metrics::Event::DbOp {
                op,
                open_ns,
                txn_begin_ns,
                stmt_ns,
                commit_ns,
                opens,
                wall_ns,
            } => {
                let prefix = format!("db_op_{}", op.name());
                insert(format!("{prefix}_count"), 1);
                insert(format!("{prefix}_open_ns"), open_ns);
                insert(format!("{prefix}_txn_begin_ns"), txn_begin_ns);
                insert(format!("{prefix}_stmt_ns"), stmt_ns);
                insert(format!("{prefix}_commit_ns"), commit_ns);
                insert(format!("{prefix}_opens"), u64::from(opens));
                insert(format!("{prefix}_wall_ns"), wall_ns);
            }
            metrics::Event::ParquetWrite {
                bytes_written,
                create_ns,
                encode_ns,
                fsync_ns,
                stat_ns,
                rewind_ns,
                digest_ns,
                digest_reread_bytes,
                digest_reread_passes,
                total_ns,
            } => {
                insert("parquet_write_count".into(), 1);
                insert("parquet_bytes_written".into(), bytes_written);
                insert("parquet_create_ns".into(), create_ns);
                insert("parquet_encode_ns".into(), encode_ns);
                insert("parquet_fsync_ns".into(), fsync_ns);
                insert("parquet_stat_ns".into(), stat_ns);
                insert("parquet_rewind_ns".into(), rewind_ns);
                insert("parquet_digest_ns".into(), digest_ns);
                insert("parquet_reread_bytes".into(), digest_reread_bytes);
                insert(
                    "parquet_reread_passes".into(),
                    u64::from(digest_reread_passes),
                );
                insert("parquet_total_ns".into(), total_ns);
            }
            metrics::Event::PartitionInstall {
                rename_count,
                rename_ns,
                dir_fsync_count,
                dir_fsync_ns,
                wall_ns,
            } => {
                insert("partition_install_count".into(), 1);
                insert("install_rename_count".into(), u64::from(rename_count));
                insert("install_rename_ns".into(), rename_ns);
                insert("install_dir_fsync_count".into(), u64::from(dir_fsync_count));
                insert("install_dir_fsync_ns".into(), dir_fsync_ns);
                insert("install_wall_ns".into(), wall_ns);
            }
            metrics::Event::VerifyDigestReread { bytes, passes, ns } => {
                insert("verify_reread_count".into(), 1);
                insert("verify_reread_bytes".into(), bytes);
                insert("verify_reread_passes".into(), u64::from(passes));
                insert("verify_reread_ns".into(), ns);
            }
            metrics::Event::CanonicalBatch {
                input_bytes,
                canonical_bytes,
                ns,
            } => {
                insert("canonical_batch_count".into(), 1);
                insert("canonical_input_bytes".into(), input_bytes);
                insert("canonical_bytes".into(), canonical_bytes);
                insert("canonical_ns".into(), ns);
            }
        }
    }
    out
}

fn count_events(events: &[metrics::Event], key: &str) -> usize {
    let aggregated = aggregate_events(events);
    aggregated.get(key).and_then(Value::as_u64).unwrap_or(0) as usize
}

/// Pure-CPU SHA-256 throughput over the same 64 KiB chunk pattern used by
/// `digest_file`; used to attribute digest CPU separately from read wall time.
fn sha256_calibration() -> f64 {
    let mut rng = XorShift(0x1234_5678);
    let buffer: Vec<u8> = (0..8 * 1024 * 1024)
        .map(|_| (rng.next() & 0xFF) as u8)
        .collect();
    const ROUNDS: usize = 8;
    let started = Instant::now();
    for _ in 0..ROUNDS {
        let mut hasher = Sha256::new();
        for chunk in buffer.chunks_exact(DIGEST_BUFFER_BYTES) {
            hasher.update(chunk);
        }
        let _ = hasher.finalize();
    }
    let ns = started.elapsed().as_nanos() as f64;
    ns / ((ROUNDS * buffer.len()) as f64)
}

fn count_sqlite_fds() -> (u64, u64) {
    let mut sqlite_fds = 0_u64;
    let mut total_fds = 0_u64;
    let entries = match fs::read_dir("/proc/self/fd") {
        Ok(entries) => entries,
        Err(_) => return (0, 0),
    };
    for entry in entries.flatten() {
        total_fds += 1;
        if let Ok(target) = fs::read_link(entry.path()) {
            if target.to_string_lossy().contains("metadata.sqlite3") {
                sqlite_fds += 1;
            }
        }
    }
    (sqlite_fds, total_fds)
}

fn vm_hwm_kib() -> u64 {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
        }
    }
    0
}

fn percentile(samples: &[u64], fraction: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = (((sorted.len() as f64) * fraction).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn emit_sample(scenario: &str, iteration: usize, sample: Map<String, Value>) {
    let mut ordered = Map::new();
    ordered.insert("kind".into(), json!("sample"));
    ordered.insert("scenario".into(), json!(scenario));
    ordered.insert("iter".into(), json!(iteration));
    for (key, value) in sample {
        ordered.insert(key, value);
    }
    println!("{}", Value::Object(ordered));
}

fn machine_info() -> Value {
    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("model name"))
        .unwrap_or("unknown")
        .trim_start_matches(|c: char| c == ':' || c.is_whitespace())
        .to_string();
    let cpu_count = fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("processor"))
        .count();
    let mem_total_kib = fs::read_to_string("/proc/meminfo")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or(0);
    let kernel = fs::read_to_string("/proc/version")
        .unwrap_or_default()
        .lines()
        .next()
        .unwrap_or("unknown")
        .to_string();
    json!({
        "machine": {
            "os": std::env::consts::OS,
            "kernel": kernel,
            "cpu_model": cpu_model,
            "cpu_count": cpu_count,
            "mem_total_kib": mem_total_kib,
        }
    })
}

fn emit(kind: &str, mut value: Value) {
    if let Value::Object(object) = &mut value {
        let mut ordered = Map::new();
        ordered.insert("kind".into(), json!(kind));
        for (key, val) in object.iter() {
            ordered.insert(key.clone(), val.clone());
        }
        println!("{}", Value::Object(ordered));
    } else {
        println!("{value}");
    }
}
