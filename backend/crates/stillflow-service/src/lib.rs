//! SVC-A1 HTTP service entry (#303).
//!
//! Composition root that wires the transport-neutral API surface onto a
//! startable HTTP process. This crate is a pure adapter: every manifest route
//! delegates to exactly one [`stillflow_api::ApiService`] method, the durable
//! event log stays the only replay authority, and no domain semantics are
//! decided here. The frozen contract is
//! `docs/issues/issue-303-svc-a1-http-service-entry-contract.md`.

pub mod adapter;
pub mod config;
pub mod process;
pub mod resolver;
pub mod routes;
pub mod sse;

pub use config::{AuthModeConfig, ProcessConfig};
pub use process::{start_service, ProcessError, StartedService};
