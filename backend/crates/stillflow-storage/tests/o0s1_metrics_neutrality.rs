//! O0-S1 (issue #286) neutrality test for the opt-in storage cost
//! instrumentation.
//!
//! The instrumentation must observe storage operations without changing
//! them. This integration test runs as its own process with a single test
//! function so the process-global metrics log is deterministic: other tests
//! cannot interleave events. It asserts, for one full publish/verify/abort
//! lifecycle:
//!
//! - with the `storage-metrics` feature disabled, nothing is recorded and
//!   the storage behavior is unchanged;
//! - with the feature enabled, one staged Parquet write produces exactly one
//!   logical digest reread pass over exactly the stored byte count;
//! - publication performs the frozen rename + directory-fsync phases;
//! - `begin_snapshot` and commit each open exactly one connection and every
//!   connection applies the three frozen PRAGMAs;
//! - a dropped writer aborts without any install/manifest-commit events;
//! - a deliberately corrupted published byte still fails closed with
//!   `DigestMismatch`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use chrono::DateTime;
use tempfile::TempDir;
use uuid::Uuid;

use stillflow_core::{
    logical_schema_to_arrow, BatchEnvelope, ColumnId, LogicalField, LogicalSchema, LogicalType,
};
use stillflow_storage::metrics::{self, DbOpKind, Event};
use stillflow_storage::{
    IntegrityFailure, SnapshotDraft, SnapshotStore, StorageError, StorageLimits,
};

#[test]
fn cost_metrics_observe_storage_without_changing_behavior() {
    let temp = TempDir::new().expect("temp directory");
    let store = SnapshotStore::open(temp.path(), StorageLimits::default()).expect("open store");
    let schema = Arc::new(
        LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(0xA0)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("valid field")])
        .expect("valid schema"),
    );
    let source_asset_id = Uuid::from_u128(0xA1);
    let _ = metrics::drain();

    let snapshot_id = Uuid::from_u128(0xA2);
    let started = at(1_700_000_001);
    let draft = SnapshotDraft::try_new(
        snapshot_id,
        Uuid::from_u128(0xA3),
        Uuid::from_u128(0xA4),
        source_asset_id,
        (*schema).clone(),
        BTreeSet::new(),
        None,
        started,
    )
    .expect("valid draft");
    let mut writer = store
        .begin_snapshot(draft, started)
        .expect("begin snapshot");
    writer
        .append(&envelope(&schema, source_asset_id, 0, vec![7, 8, 9]))
        .expect("append envelope");
    let manifest = writer.commit().expect("commit snapshot");
    let events = metrics::drain();

    if !metrics::enabled() {
        assert!(
            events.is_empty(),
            "disabled instrumentation must not record events"
        );
        return;
    }

    // Exactly one staged Parquet write: the digest reread is exactly one
    // logical pass over exactly the stored byte count.
    let writes: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::ParquetWrite {
                bytes_written,
                digest_reread_bytes,
                digest_reread_passes,
                ..
            } => Some((*bytes_written, *digest_reread_bytes, *digest_reread_passes)),
            _ => None,
        })
        .collect();
    assert_eq!(writes.len(), 1, "one appended envelope -> one staged write");
    let (bytes_written, reread_bytes, reread_passes) = writes[0];
    assert_eq!(
        bytes_written,
        manifest.partitions()[0].stored_byte_count(),
        "recorded write bytes must equal the manifest stored byte count"
    );
    assert_eq!(reread_bytes, bytes_written);
    assert_eq!(reread_passes, 1);

    // Publication install: one rename plus the two frozen directory fsyncs.
    let installs: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::PartitionInstall {
                rename_count,
                dir_fsync_count,
                ..
            } => Some((*rename_count, *dir_fsync_count)),
            _ => None,
        })
        .collect();
    assert_eq!(installs, vec![(1, 2)]);

    // Part B: begin_snapshot (journal) and commit (manifest) each open
    // exactly one connection; every connection applies the three PRAGMAs.
    let journal_ops = db_ops(&events, DbOpKind::PublicationJournal);
    let commit_ops = db_ops(&events, DbOpKind::ManifestCommit);
    assert_eq!(journal_ops, vec![(1,)]);
    assert_eq!(commit_ops, vec![(1,)]);
    let connection_opens: Vec<u32> = events
        .iter()
        .filter_map(|event| match event {
            Event::ConnectionOpen { pragma_count, .. } => Some(*pragma_count),
            _ => None,
        })
        .collect();
    assert_eq!(
        connection_opens,
        vec![3, 3],
        "every opened connection applies the frozen three-PRAGMA batch"
    );

    // Read path: verification rereads the stored bytes exactly once and the
    // snapshot still verifies cleanly under instrumentation.
    store.verify_snapshot(snapshot_id).expect("verify snapshot");
    let read_events = metrics::drain();
    let verify_reads: Vec<_> = read_events
        .iter()
        .filter_map(|event| match event {
            Event::VerifyDigestReread { bytes, passes, .. } => Some((*bytes, *passes)),
            _ => None,
        })
        .collect();
    assert_eq!(verify_reads, vec![(bytes_written, 1)]);

    // Failure path: a dropped writer aborts — the staged bytes were measured
    // but no publication events exist and nothing became visible.
    let _ = metrics::drain();
    let abort_started = at(1_700_000_100);
    let abort_draft = SnapshotDraft::try_new(
        Uuid::from_u128(0xA5),
        Uuid::from_u128(0xA6),
        Uuid::from_u128(0xA7),
        source_asset_id,
        (*schema).clone(),
        BTreeSet::new(),
        None,
        abort_started,
    )
    .expect("valid abort draft");
    let mut abort_writer = store
        .begin_snapshot(abort_draft, abort_started)
        .expect("begin abort snapshot");
    abort_writer
        .append(&envelope(&schema, source_asset_id, 0, vec![10]))
        .expect("append abort envelope");
    drop(abort_writer);
    let abort_events = metrics::drain();
    assert!(abort_events
        .iter()
        .any(|event| matches!(event, Event::ParquetWrite { .. })));
    assert!(abort_events.iter().any(|event| matches!(
        event,
        Event::DbOp {
            op: DbOpKind::PublicationJournal,
            ..
        }
    )));
    assert!(abort_events.iter().any(|event| matches!(
        event,
        Event::DbOp {
            op: DbOpKind::PublicationAbort,
            ..
        }
    )));
    assert!(abort_events
        .iter()
        .all(|event| !matches!(event, Event::PartitionInstall { .. })));
    assert!(abort_events.iter().all(|event| !matches!(
        event,
        Event::DbOp {
            op: DbOpKind::ManifestCommit,
            ..
        }
    )));
    assert!(matches!(
        store.load_manifest(Uuid::from_u128(0xA5)),
        Err(StorageError::NotFound(_))
    ));

    // Corruption detection: flipping one published byte must still fail
    // closed while the instrumentation keeps measuring the reread.
    let _ = metrics::drain();
    let partition = &manifest.partitions()[0];
    let path = temp
        .path()
        .join("partitions")
        .join(snapshot_id.to_string())
        .join(format!(
            "{:010}-{}.parquet",
            partition.sequence(),
            partition.digest()
        ));
    let mut bytes = std::fs::read(&path).expect("read published partition");
    let offset = bytes.len() / 2;
    bytes[offset] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("corrupt published partition");
    let verify_result = store.verify_snapshot(snapshot_id);
    assert!(matches!(
        verify_result,
        Err(StorageError::Integrity {
            kind: IntegrityFailure::DigestMismatch,
            ..
        })
    ));
    let corrupt_events = metrics::drain();
    assert!(corrupt_events
        .iter()
        .any(|event| matches!(event, Event::VerifyDigestReread { .. })));

    // Recovery over a quiesced store remains available and clean.
    let report = store
        .recover(at(1_700_100_000), Duration::from_secs(1), 16)
        .expect("recover");
    assert_eq!(report.recovered(), 0);
}

fn at(second: i64) -> DateTime<chrono::Utc> {
    DateTime::from_timestamp(second, 0).expect("valid timestamp")
}

fn db_ops(events: &[Event], kind: DbOpKind) -> Vec<(u32,)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::DbOp { op, opens, .. } if *op == kind => Some((*opens,)),
            _ => None,
        })
        .collect()
}

fn envelope(
    schema: &Arc<LogicalSchema>,
    source_asset_id: Uuid,
    sequence: u64,
    values: Vec<i64>,
) -> BatchEnvelope {
    let arrow_schema = logical_schema_to_arrow(schema).expect("Arrow schema");
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values))];
    let batch = RecordBatch::try_new(arrow_schema, columns).expect("record batch");
    BatchEnvelope::try_new(Arc::clone(schema), source_asset_id, sequence, batch).expect("envelope")
}
