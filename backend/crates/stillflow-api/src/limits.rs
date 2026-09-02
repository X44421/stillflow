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

    /// Caps caller-provided limits at the frozen v1/application ceilings and
    /// therefore cannot widen a stricter Engine, Storage, or Core bound.
    pub fn bounded(self) -> Self {
        Self {
            max_request_bytes: self.max_request_bytes.min(Self::DEFAULT.max_request_bytes),
            max_response_bytes: self
                .max_response_bytes
                .min(Self::DEFAULT.max_response_bytes),
            max_rows_per_page: self
                .max_rows_per_page
                .min(stillflow_core::MAX_EVENT_PAGE_SIZE),
            max_artifact_page_bytes: self
                .max_artifact_page_bytes
                .min(Self::DEFAULT.max_artifact_page_bytes),
            max_event_payload_bytes: self
                .max_event_payload_bytes
                .min(stillflow_core::MAX_EVENT_PAYLOAD_BYTES),
            max_timeout_seconds: self.max_timeout_seconds.min(30 * 60),
            max_concurrent_requests: self
                .max_concurrent_requests
                .min(Self::DEFAULT.max_concurrent_requests),
        }
    }

    pub const fn request_size_allowed(self, bytes: usize) -> bool {
        bytes <= self.max_request_bytes
    }

    pub const fn response_size_allowed(self, bytes: usize) -> bool {
        bytes <= self.max_response_bytes
    }
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
