//! Ingestion execution and orchestration for Stillflow sessions.

#![deny(unsafe_code)]

mod engine;
mod error;
#[allow(unsafe_code)]
mod ffi;
mod lower;
mod lookup;
mod memory;
mod predict;
mod preflight;
pub(crate) mod preview;
mod remainder;
mod types;
mod typing;

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use stillflow_core::{
    BatchEnvelope, LogicalSchema, RequestContext, SourceAsset, SourceConnection, MAX_BATCH_BYTES,
    MAX_BATCH_ROWS,
};
use stillflow_plan::{LogicalPlan, PlanFingerprint, PlanNodeId};
use stillflow_storage::SnapshotStore;
use uuid::Uuid;

pub use engine::ExecutionEngine;
pub use error::EngineError;
pub use preflight::PreparedPlan;

pub const ENGINE_CONTRACT_VERSION: u16 = 1;
pub const MAX_PLAN_NODES: usize = 64;
pub const MAX_RULES_PER_NODE: usize = 256;
pub const MAX_EXPR_NODES: usize = 1_024;
pub const MAX_EXPR_DEPTH: usize = 64;
pub const MAX_LIVE_COLUMNAR_PAYLOADS: u8 = 3;
pub const MAX_COMPILED_PLAN_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_FFI_SCRATCH_BYTES: usize = 1024 * 1024;
pub const MAX_OPERATOR_STATE_BYTES: usize = MAX_COMPILED_PLAN_BYTES + MAX_FFI_SCRATCH_BYTES;
pub const MAX_ENGINE_PEAK_BYTES: usize =
    (MAX_LIVE_COLUMNAR_PAYLOADS as usize) * MAX_BATCH_BYTES + MAX_OPERATOR_STATE_BYTES;
pub const MAX_ENGINE_CONCURRENT_RUNS: u16 = 4;
pub const ENGINE_DEFAULT_DEADLINE: Duration = Duration::from_secs(15 * 60);
pub const ENGINE_MAX_DEADLINE: Duration = Duration::from_secs(30 * 60);
pub const MAX_BOOL_UTF8_BYTES: usize = 5;
pub const MAX_INT_UTF8_BYTES: usize = 20;
pub const MAX_FLOAT_UTF8_BYTES: usize = 32;
pub const UTF8_VIEW_SLOT_BYTES: usize = 16;
pub const UTF8_OFFSET_SLOT_BYTES: usize = 4;

pub const PREVIEW_DEFAULT_ROW_LIMIT: usize = 1_000;
pub const PREVIEW_MAX_ROW_LIMIT: usize = 10_000;
pub const PREVIEW_DEFAULT_BYTE_LIMIT: usize = 8 * 1024 * 1024;
pub const PREVIEW_MAX_BYTE_LIMIT: usize = 50 * 1024 * 1024;
pub const PREVIEW_DEFAULT_BATCH_SIZE: usize = 1_024;
pub const PREVIEW_MAX_SOURCE_ROWS_SCANNED: usize = 100_000;
pub const PREVIEW_MAX_SOURCE_BYTES_SCANNED: usize = MAX_BATCH_BYTES;
pub const PREVIEW_MAX_SOURCE_ROWS_OBSERVED: usize =
    PREVIEW_MAX_SOURCE_ROWS_SCANNED + MAX_BATCH_ROWS;
pub const PREVIEW_MAX_SOURCE_BYTES_OBSERVED: usize =
    PREVIEW_MAX_SOURCE_BYTES_SCANNED + MAX_BATCH_BYTES;
pub const PREVIEW_DEFAULT_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_CONCURRENT_REQUESTS: u16 = MAX_ENGINE_CONCURRENT_RUNS;
pub const PREVIEW_RESPONSE_MAX_BYTES: usize = PREVIEW_MAX_BYTE_LIMIT;
pub const PREVIEW_PEAK_ENGINE_BYTES: usize =
    MAX_BATCH_BYTES + MAX_BATCH_BYTES + PREVIEW_RESPONSE_MAX_BYTES + MAX_OPERATOR_STATE_BYTES;

pub struct PreviewRequest {
    pub plan: LogicalPlan,
    pub target_node_id: PlanNodeId,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub context: RequestContext,
    pub batch_size: usize,
    pub row_limit: usize,
    pub byte_limit: usize,
}

impl PreviewRequest {
    pub fn new(
        plan: LogicalPlan,
        target_node_id: PlanNodeId,
        connection: SourceConnection,
        asset: SourceAsset,
    ) -> Self {
        Self {
            plan,
            target_node_id,
            connection,
            asset,
            schema_override: None,
            context: RequestContext::default(),
            batch_size: PREVIEW_DEFAULT_BATCH_SIZE,
            row_limit: PREVIEW_DEFAULT_ROW_LIMIT,
            byte_limit: PREVIEW_DEFAULT_BYTE_LIMIT,
        }
    }
}

pub struct PreviewResult {
    pub plan_fingerprint: PlanFingerprint,
    pub target_node_id: PlanNodeId,
    pub schema: LogicalSchema,
    pub batches: Vec<BatchEnvelope>,
    pub rows_returned: usize,
    pub bytes_returned: usize,
    pub source_rows_scanned: usize,
    pub source_bytes_scanned: usize,
    pub source_rows_observed: usize,
    pub source_bytes_observed: usize,
    pub rows_truncated: bool,
    pub bytes_truncated: bool,
    pub scan_truncated: bool,
    pub source_exhausted: bool,
}

impl fmt::Debug for PreviewResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreviewResult")
            .field("plan_fingerprint", &self.plan_fingerprint)
            .field("target_node_id", &self.target_node_id)
            .field("schema", &self.schema)
            .field("batch_count", &self.batches.len())
            .field("rows_returned", &self.rows_returned)
            .field("bytes_returned", &self.bytes_returned)
            .field("source_rows_scanned", &self.source_rows_scanned)
            .field("source_bytes_scanned", &self.source_bytes_scanned)
            .field("source_rows_observed", &self.source_rows_observed)
            .field("source_bytes_observed", &self.source_bytes_observed)
            .field("rows_truncated", &self.rows_truncated)
            .field("bytes_truncated", &self.bytes_truncated)
            .field("scan_truncated", &self.scan_truncated)
            .field("source_exhausted", &self.source_exhausted)
            .finish_non_exhaustive()
    }
}

pub struct ExecutionIdentities {
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
}

pub struct ExecutionRequest<'a> {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub identities: ExecutionIdentities,
    pub context: RequestContext,
    pub batch_size: usize,
    pub store: &'a SnapshotStore,
}

/// Returns the name of this crate, as a smoke test for workspace wiring.
pub fn crate_name() -> &'static str {
    "stillflow-engine"
}

#[cfg(test)]
#[allow(unsafe_code)]
mod test_alloc {
    use std::alloc::{GlobalAlloc, Layout, System};

    #[repr(C)]
    struct AllocHeader {
        magic: u32,
        phase: u8,
        _pad: [u8; 3],
        user_size: usize,
    }

    const MAGIC: u32 = 0x5711_F100;
    const HEADER_ALIGN: usize = std::mem::align_of::<AllocHeader>();
    const HEADER_BASE: usize = std::mem::size_of::<AllocHeader>();

    fn compute_header_size(align: usize) -> usize {
        let required_align = align.max(HEADER_ALIGN);
        let rem = HEADER_BASE % required_align;
        if rem == 0 {
            HEADER_BASE
        } else {
            HEADER_BASE + (required_align - rem)
        }
    }

    pub struct PhasedAlloc;

    unsafe impl GlobalAlloc for PhasedAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let phase = crate::memory::current_alloc_phase() as u8;
            let header_size = compute_header_size(layout.align());
            let raw_size = layout.size().saturating_add(header_size);
            let raw_align = layout.align().max(HEADER_ALIGN);
            let Ok(raw_layout) = Layout::from_size_align(raw_size, raw_align) else {
                return std::ptr::null_mut();
            };
            let raw_ptr = unsafe { System.alloc(raw_layout) };
            if raw_ptr.is_null() {
                return raw_ptr;
            }
            let header = AllocHeader {
                magic: MAGIC,
                phase,
                _pad: [0; 3],
                user_size: layout.size(),
            };
            unsafe {
                std::ptr::write(raw_ptr.cast::<AllocHeader>(), header);
                crate::memory::record_alloc_phase(phase, layout.size());
                raw_ptr.add(header_size)
            }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let header_size = compute_header_size(layout.align());
            let raw_ptr = unsafe { ptr.sub(header_size) };
            let header = unsafe { std::ptr::read(raw_ptr.cast::<AllocHeader>()) };
            if header.magic == MAGIC {
                crate::memory::record_dealloc_phase(header.phase, header.user_size);
                let raw_size = layout.size().saturating_add(header_size);
                let raw_align = layout.align().max(HEADER_ALIGN);
                if let Ok(raw_layout) = Layout::from_size_align(raw_size, raw_align) {
                    unsafe { System.dealloc(raw_ptr, raw_layout) }
                }
            } else {
                unsafe { System.dealloc(ptr, layout) }
            }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let phase = crate::memory::current_alloc_phase() as u8;
            let header_size = compute_header_size(layout.align());
            let raw_size = layout.size().saturating_add(header_size);
            let raw_align = layout.align().max(HEADER_ALIGN);
            let Ok(raw_layout) = Layout::from_size_align(raw_size, raw_align) else {
                return std::ptr::null_mut();
            };
            let raw_ptr = unsafe { System.alloc_zeroed(raw_layout) };
            if raw_ptr.is_null() {
                return raw_ptr;
            }
            let header = AllocHeader {
                magic: MAGIC,
                phase,
                _pad: [0; 3],
                user_size: layout.size(),
            };
            unsafe {
                std::ptr::write(raw_ptr.cast::<AllocHeader>(), header);
                crate::memory::record_alloc_phase(phase, layout.size());
                raw_ptr.add(header_size)
            }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let header_size = compute_header_size(layout.align());
            let raw_ptr = unsafe { ptr.sub(header_size) };
            let header = unsafe { std::ptr::read(raw_ptr.cast::<AllocHeader>()) };
            if header.magic == MAGIC {
                let old_raw_size = layout.size().saturating_add(header_size);
                let raw_align = layout.align().max(HEADER_ALIGN);
                let Ok(old_raw_layout) = Layout::from_size_align(old_raw_size, raw_align) else {
                    return std::ptr::null_mut();
                };
                let new_raw_size = new_size.saturating_add(header_size);
                let new_raw_ptr = unsafe { System.realloc(raw_ptr, old_raw_layout, new_raw_size) };
                if new_raw_ptr.is_null() {
                    return new_raw_ptr;
                }
                crate::memory::record_realloc_phase(header.phase, header.user_size, new_size);
                let new_header = AllocHeader {
                    magic: MAGIC,
                    phase: header.phase,
                    _pad: [0; 3],
                    user_size: new_size,
                };
                unsafe {
                    std::ptr::write(new_raw_ptr.cast::<AllocHeader>(), new_header);
                    new_raw_ptr.add(header_size)
                }
            } else {
                unsafe { System.realloc(ptr, layout, new_size) }
            }
        }
    }
}

#[cfg(test)]
#[global_allocator]
static TEST_ALLOC: test_alloc::PhasedAlloc = test_alloc::PhasedAlloc;

#[cfg(test)]
mod tests;
