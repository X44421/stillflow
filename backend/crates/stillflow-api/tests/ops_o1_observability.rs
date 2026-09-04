use std::sync::Arc;

use chrono::TimeZone;
use stillflow_api::{
    ApiRequest, ApiService, EmptyRequest, HealthStatus, ObjectIdRequest, RequestMetadata,
};
use stillflow_core::{MetricName, Telemetry, TelemetryEvent};
use stillflow_storage::ControlPlaneStore;
use tempfile::tempdir;
use uuid::Uuid;

fn request(request_id: u128, workspace_id: Uuid) -> RequestMetadata {
    RequestMetadata::new(Uuid::from_u128(request_id), workspace_id)
}

#[test]
fn health_reports_liveness_and_dependency_readiness() {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
    let service = ApiService::new(store);
    let workspace_id = Uuid::from_u128(1);

    let liveness = service
        .liveness(ApiRequest {
            meta: request(2, workspace_id),
            body: EmptyRequest {},
        })
        .expect("liveness")
        .body;
    assert_eq!(liveness.status, HealthStatus::Healthy);
    assert_eq!(liveness.checks[0].name, "process");

    let readiness = service
        .readiness(ApiRequest {
            meta: request(3, workspace_id),
            body: EmptyRequest {},
        })
        .expect("readiness")
        .body;
    assert_eq!(readiness.status, HealthStatus::Degraded);
    assert!(readiness
        .checks
        .iter()
        .any(|check| check.name == "controlPlane" && check.status == HealthStatus::Healthy));
    assert!(readiness
        .checks
        .iter()
        .any(|check| check.name == "engine" && check.status == HealthStatus::Unavailable));
}

#[test]
fn metrics_are_aggregated_and_never_expose_identity_labels() {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
    let (telemetry, sink) = Telemetry::in_memory();
    let service = ApiService::new(Arc::clone(&store)).with_telemetry(telemetry);
    let workspace_id = Uuid::from_u128(10);
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("timestamp");
    service
        .create_workspace(ApiRequest {
            meta: request(11, workspace_id),
            body: stillflow_api::CreateWorkspaceRequest {
                workspace_id,
                created_at,
            },
        })
        .expect("workspace create");

    let metrics = service
        .metrics(ApiRequest {
            meta: request(12, workspace_id),
            body: ObjectIdRequest {
                object_id: workspace_id,
            },
        })
        .expect("metrics")
        .body;
    assert_eq!(metrics.queue_depth, 0);
    assert!(metrics
        .metrics
        .iter()
        .any(|point| point.name == MetricName::QueueDepth && point.value == 0));
    assert!(metrics.metrics.iter().all(|point| {
        point.labels.values().all(|value| {
            !value.contains(&workspace_id.to_string()) && !value.contains("credential")
        })
    }));
    assert!(metrics
        .metrics
        .iter()
        .any(|point| { point.name == MetricName::ApiRequestsTotal && point.value >= 1 }));
    assert!(sink.events().iter().all(|event| match event {
        TelemetryEvent::StructuredLog(log) => !log
            .fields
            .values()
            .any(|value| value == &workspace_id.to_string()),
        TelemetryEvent::Metric(point) => point.labels.len() <= 4,
        TelemetryEvent::SpanStarted(span) | TelemetryEvent::SpanFinished(span) => {
            !span.correlation_id.is_empty()
        }
    }));
}
