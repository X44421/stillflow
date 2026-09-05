//! Opt-in storage-cost instrumentation for the O0-S1 measurement issue.
//!
//! This module exists purely to attribute storage costs (issue #286): Parquet
//! write/digest-reread attribution and SQLite connection-lifecycle
//! attribution. It records **observations only**. Every call site keeps the
//! exact production code path — staging, fsync, publication, transaction, and
//! connection behavior are untouched, and no recorded value ever feeds a
//! decision.
//!
//! Instrumentation is compiled in only when the `storage-metrics` cargo
//! feature is enabled. With the feature disabled every helper in this module
//! collapses to a constant `false` / `None` / no-op, so production builds pay
//! nothing and cannot observe any difference.

#![allow(dead_code)] // the no-op surface is intentionally unused when disabled

use std::sync::Mutex;
use std::time::Instant;

/// Returns `true` when cost instrumentation is compiled in.
pub const fn enabled() -> bool {
    cfg!(feature = "storage-metrics")
}

/// Starts a phase timer when instrumentation is enabled (`None` otherwise).
#[inline]
pub fn start() -> Option<Instant> {
    if enabled() {
        Some(Instant::now())
    } else {
        None
    }
}

/// Elapsed nanoseconds since [`start`] (always `0` when disabled).
#[inline]
pub fn elapsed_ns(start: Option<Instant>) -> u64 {
    start
        .map(|stamp| stamp.elapsed().as_nanos() as u64)
        .unwrap_or(0)
}

/// One attributed observation. Field semantics:
///
/// - `*_ns` values are wall-clock nanoseconds measured around the exact
///   production code the issue attributes (no code is moved or duplicated).
/// - `ConnectionOpen` separates `Connection::open` from the busy-timeout and
///   PRAGMA configuration applied to every new connection.
/// - `ParquetWrite` separates file creation, Arrow/Parquet encoding, the
///   staged-file `sync_all`, the stored-byte stat, the rewind, and the
///   SHA-256 digest reread that production performs on every staged
///   partition. `digest_reread_bytes` counts the logical bytes read back by
///   `digest_file` (one full pass over the staged file), not device I/O.
/// - `DbOp` attributes one representative logical SQLite operation to its
///   connection open, transaction begin, statement, and commit phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// One SQLite connection was opened and configured (per connection).
    ConnectionOpen {
        open_ns: u64,
        configure_ns: u64,
        pragma_count: u32,
    },
    /// One representative logical SQLite operation completed.
    DbOp {
        op: DbOpKind,
        open_ns: u64,
        txn_begin_ns: u64,
        stmt_ns: u64,
        commit_ns: u64,
        opens: u32,
        wall_ns: u64,
    },
    /// One staged Parquet partition write (snapshot or artifact path).
    ParquetWrite {
        bytes_written: u64,
        create_ns: u64,
        encode_ns: u64,
        fsync_ns: u64,
        stat_ns: u64,
        rewind_ns: u64,
        digest_ns: u64,
        digest_reread_bytes: u64,
        digest_reread_passes: u32,
        total_ns: u64,
    },
    /// One publication install: partition renames plus directory fsyncs.
    PartitionInstall {
        rename_count: u32,
        rename_ns: u64,
        dir_fsync_count: u32,
        dir_fsync_ns: u64,
        wall_ns: u64,
    },
    /// One digest-verification reread on the read path (`read_partition`).
    VerifyDigestReread { bytes: u64, passes: u32, ns: u64 },
    /// One canonical-batch encoding (digest preimage / summary input).
    CanonicalBatch {
        input_bytes: u64,
        canonical_bytes: u64,
        ns: u64,
    },
}

/// The representative logical SQLite operations attributed by
/// [`Event::DbOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbOpKind {
    /// `insert_publication`: the `begin_snapshot` journal write (explicit
    /// IMMEDIATE transaction).
    PublicationJournal,
    /// `commit_manifest`: the visible-snapshot manifest transaction.
    ManifestCommit,
    /// `load_manifest_inner`: snapshot + partition manifest read.
    LoadManifest,
    /// `ControlPlaneStore::create_dataset`: representative autocommit write
    /// (no explicit transaction; every statement commits independently).
    CreateDataset,
    /// `abort_publication`: best-effort journal cleanup on the abort path.
    PublicationAbort,
}

impl DbOpKind {
    /// Stable name used by the measurement harness output.
    pub const fn name(self) -> &'static str {
        match self {
            Self::PublicationJournal => "publication_journal",
            Self::ManifestCommit => "manifest_commit",
            Self::LoadManifest => "load_manifest",
            Self::CreateDataset => "create_dataset",
            Self::PublicationAbort => "publication_abort",
        }
    }
}

static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

/// Recording cap so a runaway caller cannot grow the log without bound.
const EVENT_LOG_CAP: usize = 1_000_000;

/// Records one observation. No-op unless the `storage-metrics` feature is
/// enabled; never affects control flow or stored state.
pub fn record(event: Event) {
    if !enabled() {
        return;
    }
    if let Ok(mut events) = EVENTS.lock() {
        if events.len() < EVENT_LOG_CAP {
            events.push(event);
        }
    }
}

/// Drains every recorded observation (empty unless the feature is enabled).
pub fn drain() -> Vec<Event> {
    if !enabled() {
        return Vec::new();
    }
    EVENTS
        .lock()
        .map(|mut events| std::mem::take(&mut *events))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_drain_roundtrip_only_when_enabled() {
        if enabled() {
            // Other tests may run concurrently in this process and record
            // their own events, so scan for this call's distinct event
            // instead of asserting an exact count.
            record(Event::ConnectionOpen {
                open_ns: 1,
                configure_ns: 2,
                pragma_count: 3,
            });
            let after = drain();
            assert!(after.iter().any(|event| matches!(
                event,
                Event::ConnectionOpen {
                    open_ns: 1,
                    configure_ns: 2,
                    pragma_count: 3,
                }
            )));
        } else {
            assert!(
                drain().is_empty(),
                "disabled instrumentation must not record events"
            );
            record(Event::ConnectionOpen {
                open_ns: 1,
                configure_ns: 2,
                pragma_count: 3,
            });
            assert!(drain().is_empty());
        }
    }

    #[test]
    fn elapsed_helpers_are_zero_when_disabled() {
        let stamp = start();
        if !enabled() {
            assert_eq!(
                elapsed_ns(stamp),
                0,
                "disabled instrumentation must not report time"
            );
        } else {
            assert!(stamp.is_some());
        }
    }
}
