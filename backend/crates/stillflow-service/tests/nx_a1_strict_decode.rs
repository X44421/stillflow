//! NX-A1 (#338) strict-decoding and error-envelope tests at the transport
//! boundary (NX-C0 contract §7.5 / §3.2).

use stillflow_api::{ApiError, ApiErrorResponse, NodeGraphDiagnosticView};
use stillflow_core::NodeId;

/// The strict decoding gate (§7.5, R-1): duplicate object keys inside the
/// request envelope are a typed rejection. serde_json alone tolerates them
/// with last-wins semantics; the gate runs before the typed decode.
#[test]
fn duplicate_object_keys_are_rejected() {
    let body = r#"{
        "meta": {"apiVersion": 1, "requestId": "00000000-0000-0000-0000-000000000001", "workspaceId": "00000000-0000-0000-0000-000000000002"},
        "body": {"graph": {"version": 1, "version": 2}}
    }"#;
    let tolerated: Result<stillflow_api::ApiRequest<serde_json::Value>, _> =
        serde_json::from_slice(body.as_bytes());
    assert!(
        tolerated.is_ok(),
        "precondition: serde_json alone accepts duplicates"
    );
    let strict = stillflow_service::adapter::reject_duplicate_keys(body.as_bytes());
    assert!(strict.is_err(), "duplicate keys must fail closed");
}

#[test]
fn duplicate_keys_inside_node_configs_are_rejected() {
    let body = r#"{
        "meta": {"apiVersion": 1, "requestId": "00000000-0000-0000-0000-000000000001", "workspaceId": "00000000-0000-0000-0000-000000000002"},
        "body": {"nodes": [{"id": "00000000-0000-0000-0000-000000000003", "id": "00000000-0000-0000-0000-000000000004"}]}
    }"#;
    let strict = stillflow_service::adapter::reject_duplicate_keys(body.as_bytes());
    assert!(
        strict.is_err(),
        "duplicate node-level keys must fail closed"
    );
}

#[test]
fn envelopes_without_duplicates_pass_the_gate() {
    let body = r#"{
        "meta": {"apiVersion": 1, "requestId": "00000000-0000-0000-0000-000000000001", "workspaceId": "00000000-0000-0000-0000-000000000002"},
        "body": {"graph": {"version": 1, "nodes": []}}
    }"#;
    assert!(stillflow_service::adapter::reject_duplicate_keys(body.as_bytes()).is_ok());
}

/// The request id is echoed on typed-route failures (§3.2 rule 5); the
/// diagnostics ride on the error body (§7.1).
#[test]
fn error_envelope_carries_request_id_and_diagnostics() {
    let request_id = uuid::Uuid::from_u128(0xD1);
    let error = ApiError::invalid("node graph compilation rejected (NG_UNKNOWN_COLUMN)")
        .with_diagnostics(vec![NodeGraphDiagnosticView {
            code: "NG_UNKNOWN_COLUMN".to_owned(),
            node_id: Some(NodeId::from_uuid(uuid::Uuid::from_u128(0xD2))),
            column_id: None,
            field_path: Some("columns".to_owned()),
            expected: None,
            actual: None,
            message: "column is absent from the working schema".to_owned(),
        }]);
    let response = ApiErrorResponse::new(request_id, error);
    assert_eq!(response.meta.request_id, request_id);
    assert_eq!(response.error.diagnostics.len(), 1);
    assert_eq!(
        response.error.diagnostics[0].field_path.as_deref(),
        Some("columns")
    );
    let encoded = serde_json::to_value(&response).expect("envelope");
    assert_eq!(encoded["meta"]["requestId"], serde_json::json!(request_id));
    assert_eq!(
        encoded["error"]["diagnostics"][0]["code"],
        serde_json::json!("NG_UNKNOWN_COLUMN")
    );
}
