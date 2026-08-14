use crate::error::{live_payload_guard, peak_guard, EngineError};
use crate::{MAX_ENGINE_PEAK_BYTES, MAX_OPERATOR_STATE_BYTES};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadKind {
    ConnectorEnvelope,
    PolarsWorkingSet,
    CanonicalRemainder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllocatorPhase {
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

#[derive(Debug)]
pub(crate) struct MemoryTracker {
    envelope_bytes: usize,
    polars_bytes: usize,
    remainder_bytes: usize,
    operator_state_bytes: usize,
    envelope_live: bool,
    polars_live: bool,
    remainder_live: bool,
    phase: AllocatorPhase,
    report: MemoryReport,
}

impl MemoryTracker {
    pub(crate) fn new() -> Self {
        Self {
            envelope_bytes: 0,
            polars_bytes: 0,
            remainder_bytes: 0,
            operator_state_bytes: MAX_OPERATOR_STATE_BYTES,
            envelope_live: false,
            polars_live: false,
            remainder_live: false,
            phase: AllocatorPhase::Polars,
            report: MemoryReport::default(),
        }
    }

    pub(crate) fn report(&self) -> MemoryReport {
        self.report.clone()
    }

    pub(crate) fn set_phase(&mut self, phase: AllocatorPhase) {
        self.phase = phase;
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
        self.polars_live = true;
        self.polars_bytes = bytes;
        self.set_phase(AllocatorPhase::Polars);
        self.refresh()
    }

    pub(crate) fn drop_polars(&mut self) -> Result<(), EngineError> {
        self.polars_live = false;
        self.polars_bytes = 0;
        self.refresh()
    }

    pub(crate) fn hold_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        if bytes > stillflow_core::MAX_BATCH_BYTES {
            return Err(EngineError::BoundExceeded(
                "canonical remainder exceeded MAX_BATCH_BYTES",
            ));
        }
        self.remainder_live = bytes > 0;
        self.remainder_bytes = bytes;
        self.set_phase(AllocatorPhase::Remainder);
        self.refresh()
    }

    pub(crate) fn record_storage_append(&mut self, bytes: usize) {
        self.set_phase(AllocatorPhase::StorageAppend);
        self.report.storage_append_phase_peak = self.report.storage_append_phase_peak.max(bytes);
    }

    fn live_payloads(&self) -> u8 {
        u8::from(self.envelope_live) + u8::from(self.polars_live) + u8::from(self.remainder_live)
    }

    fn engine_bytes(&self) -> usize {
        self.envelope_bytes
            .saturating_add(self.polars_bytes)
            .saturating_add(self.remainder_bytes)
            .saturating_add(self.operator_state_bytes)
    }

    fn refresh(&mut self) -> Result<(), EngineError> {
        let live = self.live_payloads();
        debug_assert_eq!(live as usize, self.live_kinds().count());
        live_payload_guard(live)?;
        let bytes = self.engine_bytes();
        peak_guard(bytes)?;
        if bytes > MAX_ENGINE_PEAK_BYTES {
            return Err(EngineError::peak_exceeded());
        }
        self.report.peak_live_payloads = self.report.peak_live_payloads.max(live);
        self.report.peak_engine_bytes = self.report.peak_engine_bytes.max(bytes);
        match self.phase {
            AllocatorPhase::Polars => {
                self.report.polars_phase_peak =
                    self.report.polars_phase_peak.max(self.polars_bytes);
            }
            AllocatorPhase::Remainder => {
                self.report.remainder_phase_peak =
                    self.report.remainder_phase_peak.max(self.remainder_bytes);
            }
            AllocatorPhase::StorageAppend => {}
        }
        Ok(())
    }

    fn live_kinds(&self) -> impl Iterator<Item = PayloadKind> {
        [
            self.envelope_live.then_some(PayloadKind::ConnectorEnvelope),
            self.polars_live.then_some(PayloadKind::PolarsWorkingSet),
            self.remainder_live
                .then_some(PayloadKind::CanonicalRemainder),
        ]
        .into_iter()
        .flatten()
    }
}
