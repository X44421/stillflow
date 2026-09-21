//! Measurement-only instrumentation for per-chunk Polars gathering.
//!
//! Scope: how many times one chunk's lowering crosses into Polars execution.
//! Before #370's lazy-plan consolidation every step called
//! `.lazy().<one op>().collect()`, so a chunk with `n` steps forced `n` gathers.
//! After consolidation a chunk forces exactly one, and that is what these
//! counters make observable.
//!
//! Contract (mirrors `predict_metrics`):
//! - Every counter is inert unless the `predict-metrics` cargo feature is
//!   enabled. With the feature disabled the hooks compile to zero-sized
//!   `#[inline(always)]` no-ops, so production behavior is unchanged.
//! - With the feature enabled the hooks only touch process-global `AtomicU64`
//!   counters with `Relaxed` ordering. They never influence control flow,
//!   chunk boundaries, or results.
//! - Counters are cumulative process-global aggregates; deltas around a single
//!   run give exact attribution because engine runs are serialized by the
//!   engine semaphore.

// The read-side API (`snapshot`/`reset`) is consumed only by measurement tests,
// and the whole surface is inert when the feature is off, so dead-code analysis
// flags it in non-instrumented builds. Expected for measurement-only code, and
// the same allowance `predict_metrics` takes.
#![allow(dead_code)]

#[cfg(feature = "predict-metrics")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "predict-metrics")]
static CHUNK_GATHERS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ChunkMetricsSnapshot {
    /// Number of times one chunk's step lowering crossed into Polars
    /// execution (one per `transform` call after consolidation).
    pub chunk_gathers: u64,
}

/// Records one chunk gather. Inert without the `predict-metrics` feature.
#[inline(always)]
pub(crate) fn record_chunk_gather() {
    #[cfg(feature = "predict-metrics")]
    {
        CHUNK_GATHERS.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "predict-metrics")]
pub(crate) fn snapshot() -> ChunkMetricsSnapshot {
    ChunkMetricsSnapshot {
        chunk_gathers: CHUNK_GATHERS.load(Ordering::Relaxed),
    }
}

#[cfg(not(feature = "predict-metrics"))]
pub(crate) fn snapshot() -> ChunkMetricsSnapshot {
    ChunkMetricsSnapshot::default()
}

#[cfg(feature = "predict-metrics")]
pub(crate) fn reset() {
    CHUNK_GATHERS.store(0, Ordering::Relaxed);
}

#[cfg(not(feature = "predict-metrics"))]
pub(crate) fn reset() {}
