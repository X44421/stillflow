use std::io;

use thiserror::Error;
use uuid::Uuid;

use stillflow_core::SnapshotError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityFailure {
    Missing,
    Symlink,
    NotRegularFile,
    LengthMismatch,
    DigestMismatch,
    InvalidParquet,
    RowCountMismatch,
    SchemaMismatch,
    UnexpectedBatchCount,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid storage configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("invalid snapshot draft: {0}")]
    InvalidDraft(&'static str),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("unsupported storage schema version {0}")]
    UnsupportedStorageVersion(i64),
    #[error("snapshot {0} was not found")]
    NotFound(Uuid),
    #[error("snapshot {0} already exists")]
    AlreadyExists(Uuid),
    #[error("storage is busy: {0}")]
    Busy(&'static str),
    #[error("input sequence {actual} does not match expected sequence {expected}")]
    Sequence { expected: u64, actual: u64 },
    #[error("input source identity changed at sequence {sequence}")]
    LineageMismatch { sequence: u64 },
    #[error("input logical schema changed at sequence {sequence}")]
    SchemaDrift { sequence: u64 },
    #[error("input envelope count {actual} exceeds limit {maximum}")]
    EnvelopeLimitExceeded { actual: u32, maximum: u32 },
    #[error("partition count {actual} exceeds limit {maximum}")]
    PartitionLimitExceeded { actual: u32, maximum: u32 },
    #[error("snapshot row count {actual} exceeds limit {maximum}")]
    RowLimitExceeded { actual: u64, maximum: u64 },
    #[error("snapshot stored byte count {actual} exceeds limit {maximum}")]
    StoredByteLimitExceeded { actual: u64, maximum: u64 },
    #[error("artifact row count {actual} exceeds limit {maximum}")]
    ArtifactRowLimitExceeded { actual: u64, maximum: u64 },
    #[error("artifact stored byte count {actual} exceeds limit {maximum}")]
    ArtifactByteLimitExceeded { actual: u64, maximum: u64 },
    #[error("artifact partition count {actual} exceeds limit {maximum}")]
    ArtifactPartitionLimitExceeded { actual: u32, maximum: u32 },
    #[error("dedup key uses {actual} bytes; maximum is {maximum}")]
    DedupKeyLimitExceeded { actual: usize, maximum: usize },
    #[error("dedup index exceeded its {resource} limit {maximum}")]
    DedupIndexLimitExceeded {
        resource: &'static str,
        maximum: u64,
    },
    #[error("timestamp ordering is invalid for {0}")]
    InvalidTimestampOrder(&'static str),
    #[error("checked arithmetic failed for {0}")]
    ArithmeticOverflow(&'static str),
    #[error("manifest is invalid: {0}")]
    InvalidManifest(&'static str),
    #[error("snapshot {snapshot_id} partition {sequence} failed integrity verification: {kind:?}")]
    Integrity {
        snapshot_id: Uuid,
        sequence: u32,
        kind: IntegrityFailure,
    },
    #[error("filesystem operation failed during {operation}: {kind:?}")]
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
    #[error("SQLite operation failed during {0}")]
    Database(&'static str),
    #[error("Parquet operation failed during {0}")]
    Parquet(&'static str),
    #[error("serialization operation failed during {0}")]
    Serialization(&'static str),
    #[error("batch-envelope validation failed during {0}")]
    Batch(&'static str),
    #[error("storage activity state is unavailable")]
    ActivityState,
}

impl StorageError {
    pub(crate) fn io(operation: &'static str, error: &io::Error) -> Self {
        Self::Io {
            operation,
            kind: error.kind(),
        }
    }

    pub(crate) const fn database(operation: &'static str) -> Self {
        Self::Database(operation)
    }

    pub(crate) const fn parquet(operation: &'static str) -> Self {
        Self::Parquet(operation)
    }
}
