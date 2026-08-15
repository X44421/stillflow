//! Ingestion execution and orchestration for Stillflow sessions.

#![deny(unsafe_code)]

mod engine;
mod error;
#[allow(unsafe_code)]
mod ffi;
mod lower;
mod memory;
mod predict;
mod preflight;
mod remainder;
mod types;
mod typing;

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use stillflow_core::{
    LogicalSchema, RequestContext, SourceAsset, SourceConnection, MAX_BATCH_BYTES,
};
use stillflow_plan::LogicalPlan;
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

    pub struct PhasedAlloc;

    unsafe impl GlobalAlloc for PhasedAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            crate::memory::record_alloc(layout.size());
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            crate::memory::record_dealloc(layout.size());
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            crate::memory::record_alloc(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            crate::memory::record_dealloc(layout.size());
            crate::memory::record_alloc(new_size);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }
}

#[cfg(test)]
#[global_allocator]
static TEST_ALLOC: test_alloc::PhasedAlloc = test_alloc::PhasedAlloc;

#[cfg(test)]
mod tests;
