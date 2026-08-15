use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use crate::error::{live_payload_guard, peak_guard, EngineError};
use crate::MAX_OPERATOR_STATE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllocatorPhase {
    Idle = 0,
    Polars = 1,
    Remainder = 2,
    StorageAppend = 3,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryReport {
    pub peak_engine_bytes: usize,
    pub peak_live_payloads: u8,
    pub polars_phase_peak: usize,
    pub remainder_phase_peak: usize,
    pub storage_append_phase_peak: usize,
    pub chunk_count: usize,
    pub min_chunk_rows: usize,
    pub saw_split_envelope_with_remainder: bool,
}

static GLOBAL_PHASE: AtomicU8 = AtomicU8::new(0);
static POLARS_LIVE: AtomicUsize = AtomicUsize::new(0);
static POLARS_PEAK: AtomicUsize = AtomicUsize::new(0);
static REMAINDER_LIVE: AtomicUsize = AtomicUsize::new(0);
static REMAINDER_PEAK: AtomicUsize = AtomicUsize::new(0);
static STORAGE_LIVE: AtomicUsize = AtomicUsize::new(0);
static STORAGE_PEAK: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn set_alloc_phase(phase: AllocatorPhase) {
    GLOBAL_PHASE.store(phase as u8, Ordering::SeqCst);
}

pub(crate) fn current_alloc_phase() -> AllocatorPhase {
    match GLOBAL_PHASE.load(Ordering::SeqCst) {
        1 => AllocatorPhase::Polars,
        2 => AllocatorPhase::Remainder,
        3 => AllocatorPhase::StorageAppend,
        _ => AllocatorPhase::Idle,
    }
}

pub(crate) struct PhaseGuard {
    previous: AllocatorPhase,
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        set_alloc_phase(self.previous);
    }
}

pub(crate) fn enter_phase(phase: AllocatorPhase) -> PhaseGuard {
    let previous = current_alloc_phase();
    set_alloc_phase(phase);
    PhaseGuard { previous }
}

pub(crate) fn reset_alloc_peaks() {
    set_alloc_phase(AllocatorPhase::Idle);
    POLARS_LIVE.store(0, Ordering::SeqCst);
    POLARS_PEAK.store(0, Ordering::SeqCst);
    REMAINDER_LIVE.store(0, Ordering::SeqCst);
    REMAINDER_PEAK.store(0, Ordering::SeqCst);
    STORAGE_LIVE.store(0, Ordering::SeqCst);
    STORAGE_PEAK.store(0, Ordering::SeqCst);
}

pub(crate) fn alloc_peaks() -> (usize, usize, usize) {
    (
        POLARS_PEAK.load(Ordering::SeqCst),
        REMAINDER_PEAK.load(Ordering::SeqCst),
        STORAGE_PEAK.load(Ordering::SeqCst),
    )
}

#[cfg(test)]
pub(crate) fn record_alloc(size: usize) {
    match current_alloc_phase() {
        AllocatorPhase::Idle => {}
        AllocatorPhase::Polars => add_live_atomic(&POLARS_LIVE, &POLARS_PEAK, size),
        AllocatorPhase::Remainder => add_live_atomic(&REMAINDER_LIVE, &REMAINDER_PEAK, size),
        AllocatorPhase::StorageAppend => add_live_atomic(&STORAGE_LIVE, &STORAGE_PEAK, size),
    }
}

#[cfg(test)]
pub(crate) fn record_dealloc(size: usize) {
    match current_alloc_phase() {
        AllocatorPhase::Idle => {}
        AllocatorPhase::Polars => sub_live_atomic(&POLARS_LIVE, size),
        AllocatorPhase::Remainder => {
            sub_live_with_fallback(&REMAINDER_LIVE, &POLARS_LIVE, size);
        }
        AllocatorPhase::StorageAppend => {
            sub_live_with_fallback(&STORAGE_LIVE, &REMAINDER_LIVE, size);
        }
    }
}

#[cfg(test)]
pub(crate) fn record_realloc(old_size: usize, new_size: usize) {
    match current_alloc_phase() {
        AllocatorPhase::Idle => {}
        AllocatorPhase::Polars => {
            realloc_live_atomic(&POLARS_LIVE, &POLARS_PEAK, old_size, new_size)
        }
        AllocatorPhase::Remainder => {
            realloc_live_atomic(&REMAINDER_LIVE, &REMAINDER_PEAK, old_size, new_size)
        }
        AllocatorPhase::StorageAppend => {
            realloc_live_atomic(&STORAGE_LIVE, &STORAGE_PEAK, old_size, new_size)
        }
    }
}

#[cfg(test)]
fn add_live_atomic(live: &AtomicUsize, peak: &AtomicUsize, size: usize) {
    let next = live.fetch_add(size, Ordering::SeqCst).saturating_add(size);
    peak.fetch_max(next, Ordering::SeqCst);
}

#[cfg(test)]
fn realloc_live_atomic(live: &AtomicUsize, peak: &AtomicUsize, old_size: usize, new_size: usize) {
    let current = live.load(Ordering::SeqCst);
    let transient = current.saturating_add(new_size);
    peak.fetch_max(transient, Ordering::SeqCst);
    if new_size >= old_size {
        live.fetch_add(new_size - old_size, Ordering::SeqCst);
    } else {
        sub_live_atomic(live, old_size - new_size);
    }
}

#[cfg(test)]
fn sub_live_atomic(live: &AtomicUsize, size: usize) {
    let _ = live.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
        Some(val.saturating_sub(size))
    });
}

#[cfg(test)]
fn sub_live_with_fallback(primary: &AtomicUsize, fallback: &AtomicUsize, size: usize) {
    let prev = primary.load(Ordering::SeqCst);
    if prev >= size {
        primary.fetch_sub(size, Ordering::SeqCst);
    } else {
        primary.store(0, Ordering::SeqCst);
        let remaining = size - prev;
        sub_live_atomic(fallback, remaining);
    }
}

#[derive(Debug)]
pub(crate) struct MemoryTracker {
    envelope_bytes: usize,
    working_bytes: usize,
    remainder_bytes: usize,
    operator_state_bytes: usize,
    envelope_live: bool,
    polars_live: bool,
    incoming_live: bool,
    remainder_live: bool,
    report: MemoryReport,
}

impl MemoryTracker {
    pub(crate) fn new() -> Self {
        reset_alloc_peaks();
        Self {
            envelope_bytes: 0,
            working_bytes: 0,
            remainder_bytes: 0,
            operator_state_bytes: MAX_OPERATOR_STATE_BYTES,
            envelope_live: false,
            polars_live: false,
            incoming_live: false,
            remainder_live: false,
            report: MemoryReport::default(),
        }
    }

    pub(crate) fn report(&self) -> MemoryReport {
        let mut report = self.report.clone();
        let (polars, remainder, storage) = alloc_peaks();
        report.polars_phase_peak = report.polars_phase_peak.max(polars);
        report.remainder_phase_peak = report.remainder_phase_peak.max(remainder);
        report.storage_append_phase_peak = report.storage_append_phase_peak.max(storage);
        report
    }

    pub(crate) fn record_chunk(&mut self, rows: usize, remainder_live: bool) {
        self.report.chunk_count = self.report.chunk_count.saturating_add(1);
        if self.report.min_chunk_rows == 0 || rows < self.report.min_chunk_rows {
            self.report.min_chunk_rows = rows;
        }
        if remainder_live && self.envelope_live && self.polars_live {
            self.report.saw_split_envelope_with_remainder = true;
        }
    }

    pub(crate) fn hold_envelope(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.envelope_live = true;
        self.envelope_bytes = bytes;
        self.refresh()
    }

    pub(crate) fn drop_envelope(&mut self) -> Result<(), EngineError> {
        self.envelope_live = false;
        self.envelope_bytes = 0;
        self.refresh()
    }

    pub(crate) fn hold_polars(&mut self, bytes: usize) -> Result<(), EngineError> {
        if bytes > stillflow_core::MAX_BATCH_BYTES {
            return Err(EngineError::Internal(
                "polars working set exceeded MAX_BATCH_BYTES",
            ));
        }
        set_alloc_phase(AllocatorPhase::Polars);
        self.polars_live = true;
        self.incoming_live = false;
        self.working_bytes = bytes;
        self.refresh()
    }

    pub(crate) fn drop_polars(&mut self) -> Result<(), EngineError> {
        self.polars_live = false;
        if !self.incoming_live {
            self.working_bytes = 0;
        }
        self.refresh()
    }

    pub(crate) fn hold_incoming(&mut self, bytes: usize) -> Result<(), EngineError> {
        if bytes > stillflow_core::MAX_BATCH_BYTES {
            return Err(EngineError::BoundExceeded(
                "incoming canonical chunk exceeds MAX_BATCH_BYTES",
            ));
        }
        self.incoming_live = bytes > 0;
        self.polars_live = false;
        self.working_bytes = bytes;
        self.refresh()
    }

    pub(crate) fn drop_incoming(&mut self) -> Result<(), EngineError> {
        self.incoming_live = false;
        self.working_bytes = 0;
        self.refresh()
    }

    pub(crate) fn hold_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        if bytes > stillflow_core::MAX_BATCH_BYTES {
            return Err(EngineError::BoundExceeded(
                "canonical remainder exceeded MAX_BATCH_BYTES",
            ));
        }
        set_alloc_phase(AllocatorPhase::Remainder);
        self.remainder_live = bytes > 0;
        self.remainder_bytes = bytes;
        self.refresh()
    }

    pub(crate) fn record_storage_append(&mut self, bytes: usize) {
        self.report.storage_append_phase_peak = self.report.storage_append_phase_peak.max(bytes);
    }

    fn live_payloads(&self) -> u8 {
        u8::from(self.envelope_live)
            + u8::from(self.polars_live || self.incoming_live)
            + u8::from(self.remainder_live)
    }

    fn engine_bytes(&self) -> usize {
        self.envelope_bytes
            .saturating_add(self.working_bytes)
            .saturating_add(self.remainder_bytes)
            .saturating_add(self.operator_state_bytes)
    }

    fn refresh(&mut self) -> Result<(), EngineError> {
        if self.polars_live && self.incoming_live {
            return Err(EngineError::peak_exceeded());
        }
        let live = self.live_payloads();
        live_payload_guard(live)?;
        let bytes = self.engine_bytes();
        peak_guard(bytes)?;
        self.report.peak_live_payloads = self.report.peak_live_payloads.max(live);
        self.report.peak_engine_bytes = self.report.peak_engine_bytes.max(bytes);
        let (polars, remainder, storage) = alloc_peaks();
        self.report.polars_phase_peak = self.report.polars_phase_peak.max(polars);
        self.report.remainder_phase_peak = self.report.remainder_phase_peak.max(remainder);
        self.report.storage_append_phase_peak = self.report.storage_append_phase_peak.max(storage);
        Ok(())
    }
}
