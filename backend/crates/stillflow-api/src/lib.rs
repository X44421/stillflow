//! Transport-neutral, versioned API boundary for the Stillflow backend.
//!
//! The API crate translates requests and results at the application boundary;
//! domain state, canonical plan semantics, durable events, and execution remain
//! owned by the existing core, plan, storage, and engine authorities.
//!
//! The `event-stream` feature is intentionally off by default. E5-E1 owns the
//! implementation file and its tests after stacking on the API bootstrap.

pub mod authorization;
pub mod envelope;
pub mod error;
pub mod limits;
pub mod manifest;
pub mod service;
pub mod version;

#[cfg(feature = "event-stream")]
pub mod event_stream {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/event_stream.rs"));
}

pub use authorization::{AuthorizationMode, Capability};
pub use envelope::{
    ApiErrorBody, ApiErrorResponse, ApiRequest, ApiResponse, RequestMetadata, RequestPrincipal,
    RequestPrincipalKind, ResponseMetadata,
};
pub use error::{ApiError, ApiErrorCode, ApiResult};
pub use limits::ApiLimits;
pub use manifest::{
    openapi_representation, RouteManifest, RouteSpec, SchemaSpec, BOOTSTRAP_MANIFEST,
    E5_A1_MANIFEST, E5_A1_ROUTES, E5_A1_SCHEMAS,
};
pub use service::*;
pub use version::{ApiVersion, API_V1, SUPPORTED_API_VERSIONS};

pub fn crate_name() -> &'static str {
    "stillflow-api"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate_name(), "stillflow-api");
    }

    #[test]
    fn bootstrap_exposes_only_the_supported_version() {
        assert!(ApiVersion::new(1).is_supported());
        assert!(!ApiVersion::new(2).is_supported());
    }
}
