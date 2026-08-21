//! Versioned SQLite metadata and immutable Parquet snapshot storage.
//!
//! The crate owns local persistence adapters only. Stable logical identities and
//! schemas remain in `stillflow-core`.

mod bundle;
mod dedup;
mod digest;
mod error;
mod manifest;
mod store;

pub use bundle::{
    AcceptedSnapshotArtifact, ArtifactBatchReader, ArtifactManifest, ArtifactPartition,
    ArtifactSection, ArtifactSectionId, ArtifactSectionStats, DeduplicationReportArtifact,
    RejectedRowsArtifact, ValidationReportArtifact, VerificationBundle, VerificationBundleDraft,
    VerificationBundleMembership, VerificationBundleWriter, ARTIFACT_MANIFEST_VERSION,
};
pub use dedup::{DedupIndex, DedupInsert, MAX_DEDUP_INDEX_CACHE_BYTES, MAX_DEDUP_INDEX_PAGES};
pub use digest::{ContentDigest, DIGEST_BUFFER_BYTES};
pub use error::{IntegrityFailure, StorageError};
pub use manifest::{
    GarbageCollectionReport, RecoveryReport, SnapshotDraft, SnapshotManifest, SnapshotPartition,
    StorageLimits, MAX_ACTIVE_PUBLISHERS, MAX_ACTIVE_READERS, MAX_INPUT_ENVELOPES,
    MAX_MAINTENANCE_CANDIDATES, MAX_SNAPSHOT_PARTITIONS, MAX_SNAPSHOT_ROWS,
    MAX_SNAPSHOT_STORED_BYTES, SQLITE_BUSY_TIMEOUT_MILLIS, STORAGE_SCHEMA_VERSION,
};
pub use store::{SnapshotBatchReader, SnapshotStore, SnapshotWriter};
