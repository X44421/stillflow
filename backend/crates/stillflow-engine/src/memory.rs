use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{live_payload_guard, peak_guard, EngineError};
use crate::MAX_OPERATOR_STATE_BYTES;

static ACTIVE_PHASES: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllocatorPhase {
    Idle,
    Polars,
    Remainder,
    StorageAppend,
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

thread_local! {
    static PHASE: Cell<AllocatorPhase> = const { Cell::new(AllocatorPhase::Idle) };
    static POLARS_LIVE: Cell<usize> = const { Cell::new(0) };
    static POLARS_PEAK: Cell<usize> = const { Cell::new(0) };
    static REMAINDER_LIVE: Cell<usize> = const { Cell::new(0) };
    static REMAINDER_PEAK: Cell<usize> = const { Cell::new(0) };
    static STORAGE_LIVE: Cell<usize> = const { Cell::new(0) };
    static STORAGE_PEAK: Cell<usize> = const { Cell::new(0) };
}

pub(crate) fn set_alloc_phase(phase: AllocatorPhase) {
    PHASE.with(|cell| {
        let previous = cell.get();
        if previous == phase {
            return;
        }
        cell.set(phase);
        match (
            previous == AllocatorPhase::Idle,
            phase == AllocatorPhase::Idle,
        ) {
            (true, false) => {
                ACTIVE_PHASES.fetch_add(1, Ordering::Relaxed);
            }
            (false, true) => {
                ACTIVE_PHASES.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    });
}

pub(crate) fn reset_alloc_peaks() {
    set_alloc_phase(AllocatorPhase::Idle);
    POLARS_LIVE.with(|cell| cell.set(0));
    POLARS_PEAK.with(|cell| cell.set(0));
    REMAINDER_LIVE.with(|cell| cell.set(0));
    REMAINDER_PEAK.with(|cell| cell.set(0));
    STORAGE_LIVE.with(|cell| cell.set(0));
    STORAGE_PEAK.with(|cell| cell.set(0));
}

pub(crate) fn alloc_peaks() -> (usize, usize, usize) {
    (
        POLARS_PEAK.with(Cell::get),
        REMAINDER_PEAK.with(Cell::get),
        STORAGE_PEAK.with(Cell::get),
    )
}

#[inline]
fn tracking_active() -> bool {
    ACTIVE_PHASES.load(Ordering::Relaxed) != 0
}

pub(crate) fn record_alloc(size: usize) {
    if !tracking_active() {
        return;
    }
    match PHASE.with(Cell::get) {
        AllocatorPhase::Idle => {}
        AllocatorPhase::Polars => add_live(&POLARS_LIVE, &POLARS_PEAK, size),
        AllocatorPhase::Remainder => add_live(&REMAINDER_LIVE, &REMAINDER_PEAK, size),
        AllocatorPhase::StorageAppend => add_live(&STORAGE_LIVE, &STORAGE_PEAK, size),
    }
}

pub(crate) fn record_dealloc(size: usize) {
    if !tracking_active() {
        return;
    }
    match PHASE.with(Cell::get) {
        AllocatorPhase::Idle => {}
        AllocatorPhase::Polars => sub_live(&POLARS_LIVE, size),
        AllocatorPhase::Remainder => sub_live(&REMAINDER_LIVE, size),
        AllocatorPhase::StorageAppend => sub_live(&STORAGE_LIVE, size),
    }
}

fn add_live(
    live: &'static std::thread::LocalKey<Cell<usize>>,
    peak: &'static std::thread::LocalKey<Cell<usize>>,
    size: usize,
) {
    live.with(|cell| {
        let next = cell.get().saturating_add(size);
        cell.set(next);
        peak.with(|peak_cell| peak_cell.set(peak_cell.get().max(next)));
    });
}

fn sub_live(live: &'static std::thread::LocalKey<Cell<usize>>, size: usize) {
    live.with(|cell| cell.set(cell.get().saturating_sub(size)));
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
        set_alloc_phase(AllocatorPhase::StorageAppend);
        record_alloc(bytes);
        self.report.storage_append_phase_peak = self.report.storage_append_phase_peak.max(bytes);
        record_dealloc(bytes);
        set_alloc_phase(AllocatorPhase::Idle);
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
