//! API boundary limits. Domain authorities may impose stricter limits.

/// Shared transport/application bounds. These values only cap API work; they
/// never widen the stricter limits owned by Engine or Storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiLimits {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_rows_per_page: usize,
    pub max_artifact_page_bytes: usize,
    pub max_event_payload_bytes: usize,
    pub max_timeout_seconds: u64,
    pub max_concurrent_requests: usize,
}

impl ApiLimits {
    pub const DEFAULT: Self = Self {
        max_request_bytes: 2 * 1024 * 1024,
        max_response_bytes: 2 * 1024 * 1024,
        max_rows_per_page: 1_024,
        max_artifact_page_bytes: 2 * 1024 * 1024,
        max_event_payload_bytes: stillflow_core::MAX_EVENT_PAYLOAD_BYTES,
        max_timeout_seconds: 300,
        max_concurrent_requests: 64,
    };
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
