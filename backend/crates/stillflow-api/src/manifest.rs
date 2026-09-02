//! Authoritative route/schema manifest seed.
//!
//! E5-A1 adds operation entries here as typed handlers land. OpenAPI output is
//! derived from this manifest; it is not an independent source of truth.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteSpec {
    pub operation_id: &'static str,
    pub method: &'static str,
    pub path: &'static str,
    pub request_schema: &'static str,
    pub response_schema: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaSpec {
    pub name: &'static str,
    pub version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteManifest {
    pub api_version: u16,
    pub routes: &'static [RouteSpec],
    pub schemas: &'static [SchemaSpec],
}

pub const BOOTSTRAP_MANIFEST: RouteManifest = RouteManifest {
    api_version: 1,
    routes: &[],
    schemas: &[],
};
