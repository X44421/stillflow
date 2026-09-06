//! Manifest → axum route table (100% manifest coverage; contract §6 T7).
//! Every route delegates to exactly one `ApiService` method; the registered
//! set always equals the authoritative manifest. The three typed-binary
//! response views (`asset.preview`, `engine.preview`, `artifact.content`)
//! serve Arrow IPC streams per the frozen wire format (contract §6.1).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;

use stillflow_api::event_stream::EventStreamService;
use stillflow_api::{
    ApiService, ArchiveWorkspaceRequest, ArtifactContentRequest, AssignRoleRequest,
    AuditLineageRequest, AutomationIdRequest, AutomationTransitionRequest,
    BeginCredentialRotationRequest, CancelJobRequest, ClonePlanRequest,
    CollectExportGarbageRequest, CompleteCredentialRotationRequest, CreateAutomationRequest,
    CreateDatasetRequest, CreateMemberRequest, CreatePlanRequest, CreateRoleRequest,
    CreateServiceAccountRequest, CreateSessionRequest, CreateWorkspaceRequest,
    DiscoverAssetsRequest, EmptyRequest, EnginePreviewRequest, ExportDownloadRequest,
    InspectAssetRequest, ListArtifactsRequest, ListAuditEventsRequest,
    ListAutomationHistoryRequest, ListAutomationsRequest, ListEventsRequest,
    ListExportFilesRequest, ListFindingsRequest, ListJobsRequest, ListProfileHistoryRequest,
    ListRequest, ListRunsRequest, ObjectIdRequest, PlanDiffRequest, PreviewAssetRequest,
    PublishPlanVersionRequest, RecoverCredentialRequest, RegisterCredentialReferenceRequest,
    RegisterSourceConnectionRequest, RetireSourceConnectionRequest, RevokeCredentialRequest,
    RevokeMemberRequest, RevokeServiceAccountRequest, SavePlanVersionRequest,
    SetRoleCapabilitiesRequest, SubmitDriftComparisonRequest, SubmitExportRequest,
    SubmitJobRequest, TestSourceConnectionRequest, TombstoneExportRequest,
    TransitionSourceConnectionRequest, TriggerAutomationRequest, UpdateAutomationRequest,
    UpdateSourceConnectionRequest, ValidatePlanRequest,
};

use stillflow_api::ListPlanVersionsRequest;

use crate::adapter;
use crate::wire;

#[derive(Clone)]
pub struct ServiceState {
    pub api: Arc<ApiService>,
    pub events: Arc<EventStreamService>,
}

pub fn router(state: ServiceState) -> Router {
    Router::new()
        .route("/v1/handshake", post(handshake))
        .route("/v1/health/live", get(health_liveness))
        .route("/v1/health/ready", get(health_readiness))
        .route("/v1/health", get(health_read))
        .route("/v1/metrics", get(metrics_read))
        .route("/v1/workspaces", post(workspace_create))
        .route(
            "/v1/workspaces/{workspaceId}/archive",
            post(workspace_archive),
        )
        .route("/v1/workspaces/{objectId}", get(workspace_read))
        .route("/v1/sessions", post(session_create).get(session_list))
        .route("/v1/sessions/{objectId}", get(session_read))
        .route("/v1/sessions/{sessionId}/close", post(session_close))
        .route("/v1/members", post(member_create))
        .route("/v1/members/{objectId}", get(member_read))
        .route("/v1/members/{objectId}/revoke", post(member_revoke))
        .route("/v1/members/{objectId}/roles", post(member_role_assign))
        .route("/v1/roles", post(role_create))
        .route("/v1/roles/{objectId}", get(role_read))
        .route(
            "/v1/roles/{roleId}/capabilities",
            post(role_capabilities_set),
        )
        .route("/v1/service-accounts", post(service_account_create))
        .route("/v1/service-accounts/{objectId}", get(service_account_read))
        .route(
            "/v1/service-accounts/{objectId}/revoke",
            post(service_account_revoke),
        )
        .route("/v1/credentials", post(credential_register))
        .route("/v1/credentials/{objectId}", get(credential_read))
        .route(
            "/v1/credentials/{objectId}/rotation",
            post(credential_rotation_begin),
        )
        .route(
            "/v1/credentials/{objectId}/rotation/complete",
            post(credential_rotation_complete),
        )
        .route("/v1/credentials/{objectId}/revoke", post(credential_revoke))
        .route(
            "/v1/credentials/{objectId}/recover",
            post(credential_recover),
        )
        .route("/v1/connections/test", post(connection_test))
        .route(
            "/v1/connections",
            post(connection_register).get(connection_list),
        )
        .route(
            "/v1/connections/{objectId}",
            get(connection_read).post(connection_update),
        )
        .route(
            "/v1/connections/{objectId}/transition",
            post(connection_transition),
        )
        .route("/v1/connections/{objectId}/retire", post(connection_retire))
        .route("/v1/assets", get(asset_list))
        .route("/v1/assets/discover", post(asset_discover))
        .route("/v1/assets/inspect", post(asset_inspect))
        .route("/v1/assets/preview", post(asset_preview))
        .route("/v1/datasets", post(dataset_create).get(dataset_list))
        .route("/v1/datasets/{objectId}", get(dataset_read))
        .route("/v1/datasets/{objectId}/archive", post(dataset_archive))
        .route(
            "/v1/datasets/{objectId}/profile-history",
            get(dataset_profile_history),
        )
        .route("/v1/engine/preview", post(engine_preview))
        .route("/v1/audit/events", get(audit_events_list))
        .route("/v1/audit/lineage", get(audit_lineage_read))
        .route("/v1/audit/export", get(audit_export))
        .route("/v1/plans", post(plan_create).get(plan_list))
        .route("/v1/plans/clone", post(plan_clone))
        .route("/v1/plans/diff", post(plan_diff))
        .route("/v1/plans/validate", post(plan_validate))
        .route("/v1/plans/{objectId}", get(plan_load))
        .route(
            "/v1/plans/{planId}/versions",
            post(plan_version_save).get(plan_version_list),
        )
        .route("/v1/plan-versions/{objectId}", get(plan_version_read))
        .route(
            "/v1/plan-versions/{planVersionId}/publish",
            post(plan_version_publish),
        )
        .route("/v1/jobs", post(job_submit).get(job_list))
        .route("/v1/jobs/{objectId}", get(job_read))
        .route("/v1/jobs/{jobId}/cancel", post(job_cancel))
        .route("/v1/drift/comparisons", post(drift_compare))
        .route("/v1/exports", post(export_submit))
        .route("/v1/exports/gc", post(export_gc))
        .route("/v1/exports/{jobId}", get(export_read))
        .route("/v1/exports/{jobId}/cancel", post(export_cancel))
        .route("/v1/exports/{exportId}/manifest", get(export_manifest_read))
        .route("/v1/exports/{exportId}/files", get(export_files_list))
        .route("/v1/exports/{exportId}/download", get(export_download))
        .route("/v1/exports/{exportId}/tombstone", post(export_tombstone))
        .route("/v1/runs", get(run_list))
        .route("/v1/runs/{objectId}", get(run_read))
        .route("/v1/runs/{runId}/artifacts", get(artifact_list))
        .route("/v1/artifacts/content", get(artifact_content))
        .route("/v1/drift-reports/{artifactId}", get(drift_report_read))
        .route("/v1/quality-reports/{artifactId}", get(quality_report_read))
        .route(
            "/v1/reports/{artifactId}/findings",
            get(report_findings_list),
        )
        .route("/v1/events", get(event_list))
        .route("/v1/events/stream", get(crate::sse::events_stream))
        .route("/v1/artifacts/{objectId}", get(artifact_read))
        .route(
            "/v1/automations",
            post(automation_create).get(automation_list),
        )
        .route(
            "/v1/automations/{automationId}",
            get(automation_read).post(automation_update),
        )
        .route(
            "/v1/automations/{automationId}/pause",
            post(automation_pause),
        )
        .route(
            "/v1/automations/{automationId}/resume",
            post(automation_resume),
        )
        .route(
            "/v1/automations/{automationId}/delete",
            post(automation_delete),
        )
        .route(
            "/v1/automations/{automationId}/next-run",
            get(automation_next_run),
        )
        .route(
            "/v1/automations/{automationId}/history",
            get(automation_history),
        )
        .route(
            "/v1/automations/{automationId}/trigger",
            post(automation_trigger),
        )
        .with_state(state)
}

async fn handshake(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<stillflow_api::HandshakeRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.handshake(request)),
        Err(response) => response,
    }
}

async fn health_liveness(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<EmptyRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.liveness(request)),
        Err(response) => response,
    }
}

async fn health_readiness(
    State(state): State<ServiceState>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<EmptyRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.readiness(request)),
        Err(response) => response,
    }
}

async fn health_read(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<EmptyRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.health(request)),
        Err(response) => response,
    }
}

async fn metrics_read(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.metrics(request)),
        Err(response) => response,
    }
}

async fn workspace_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateWorkspaceRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_workspace(request)),
        Err(response) => response,
    }
}

async fn workspace_archive(
    State(state): State<ServiceState>,
    Path(workspace_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<ArchiveWorkspaceRequest>(
        &bytes,
        vec![("workspaceId".to_owned(), workspace_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.archive_workspace(request)),
        Err(response) => response,
    }
}

async fn workspace_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_workspace(request)),
        Err(response) => response,
    }
}

async fn session_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateSessionRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_session(request)),
        Err(response) => response,
    }
}

async fn session_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_sessions(request)),
        Err(response) => response,
    }
}

async fn session_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_session(request)),
        Err(response) => response,
    }
}

async fn session_close(
    State(state): State<ServiceState>,
    Path(session_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<stillflow_api::CloseSessionRequest>(
        &bytes,
        vec![("sessionId".to_owned(), session_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.close_session(request)),
        Err(response) => response,
    }
}

async fn connection_test(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<TestSourceConnectionRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.test_source_connection(request).await),
        Err(response) => response,
    }
}

async fn connection_register(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<RegisterSourceConnectionRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.register_source_connection(request)),
        Err(response) => response,
    }
}

async fn connection_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_source_connection(request)),
        Err(response) => response,
    }
}

async fn connection_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_source_connections(request)),
        Err(response) => response,
    }
}

async fn asset_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_source_assets(request)),
        Err(response) => response,
    }
}

async fn asset_discover(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<DiscoverAssetsRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.discover_source_assets(request).await),
        Err(response) => response,
    }
}

async fn asset_inspect(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<InspectAssetRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.inspect_source_asset(request).await),
        Err(response) => response,
    }
}

async fn dataset_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateDatasetRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_dataset(request)),
        Err(response) => response,
    }
}

async fn dataset_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_dataset(request)),
        Err(response) => response,
    }
}

async fn dataset_archive(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<ObjectIdRequest>(&bytes, vec![("objectId".to_owned(), object_id)]) {
        Ok(request) => adapter::ok_response(state.api.archive_dataset(request)),
        Err(response) => response,
    }
}

async fn plan_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreatePlanRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_plan(request)),
        Err(response) => response,
    }
}

async fn plan_clone(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<ClonePlanRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.clone_plan(request)),
        Err(response) => response,
    }
}

async fn plan_diff(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<PlanDiffRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.diff_plans(request)),
        Err(response) => response,
    }
}

async fn plan_validate(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<ValidatePlanRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.validate_plan(request)),
        Err(response) => response,
    }
}

async fn plan_load(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.load_plan(request)),
        Err(response) => response,
    }
}

async fn plan_version_save(
    State(state): State<ServiceState>,
    Path(plan_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<SavePlanVersionRequest>(
        &bytes,
        vec![("planId".to_owned(), plan_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.save_plan_version(request)),
        Err(response) => response,
    }
}

async fn plan_version_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.load_plan_version(request)),
        Err(response) => response,
    }
}

async fn plan_version_publish(
    State(state): State<ServiceState>,
    Path(plan_version_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<PublishPlanVersionRequest>(
        &bytes,
        vec![("planVersionId".to_owned(), plan_version_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.publish_plan_version(request)),
        Err(response) => response,
    }
}

async fn job_submit(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<SubmitJobRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.submit_job(request)),
        Err(response) => response,
    }
}

async fn job_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListJobsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_jobs(request)),
        Err(response) => response,
    }
}

async fn job_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_job(request)),
        Err(response) => response,
    }
}

async fn job_cancel(
    State(state): State<ServiceState>,
    Path(job_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<CancelJobRequest>(&bytes, vec![("jobId".to_owned(), job_id)]) {
        Ok(request) => adapter::ok_response(state.api.cancel_job(request).await),
        Err(response) => response,
    }
}

async fn drift_compare(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<SubmitDriftComparisonRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.submit_drift_comparison(request)),
        Err(response) => response,
    }
}

async fn export_submit(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<SubmitExportRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.submit_export(request)),
        Err(response) => response,
    }
}

async fn export_gc(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CollectExportGarbageRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.collect_export_garbage(request)),
        Err(response) => response,
    }
}

async fn export_read(
    State(state): State<ServiceState>,
    Path(job_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("jobId".to_owned(), job_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_export_job(request)),
        Err(response) => response,
    }
}

async fn export_cancel(
    State(state): State<ServiceState>,
    Path(job_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<CancelJobRequest>(&bytes, vec![("jobId".to_owned(), job_id)]) {
        Ok(request) => adapter::ok_response(state.api.cancel_export_job(request).await),
        Err(response) => response,
    }
}

async fn export_manifest_read(
    State(state): State<ServiceState>,
    Path(export_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("exportId".to_owned(), export_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_export_manifest(request)),
        Err(response) => response,
    }
}

async fn export_files_list(
    State(state): State<ServiceState>,
    Path(export_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListExportFilesRequest>(
        query,
        vec![("exportId".to_owned(), export_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_export_files(request)),
        Err(response) => response,
    }
}

async fn export_download(
    State(state): State<ServiceState>,
    Path(export_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ExportDownloadRequest>(
        query,
        vec![("exportId".to_owned(), export_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.download_export(request)),
        Err(response) => response,
    }
}

async fn export_tombstone(
    State(state): State<ServiceState>,
    Path(export_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<TombstoneExportRequest>(
        &bytes,
        vec![("exportId".to_owned(), export_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.tombstone_export(request)),
        Err(response) => response,
    }
}

async fn run_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRunsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_runs(request)),
        Err(response) => response,
    }
}

async fn run_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_run(request)),
        Err(response) => response,
    }
}

async fn artifact_list(
    State(state): State<ServiceState>,
    Path(run_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListArtifactsRequest>(
        query,
        vec![("runId".to_owned(), run_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_artifact_metadata(request)),
        Err(response) => response,
    }
}

async fn event_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListEventsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_events(request)),
        Err(response) => response,
    }
}

async fn artifact_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.get_artifact_metadata(request)),
        Err(response) => response,
    }
}

// Typed-binary response views (contract §6.1): success bodies are Arrow IPC
// streams; failures keep the §3.2 JSON mapping.

async fn asset_preview(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<PreviewAssetRequest>(&bytes, vec![]) {
        Ok(request) => match state
            .api
            .preview_source_asset(request)
            .await
            .and_then(wire::encode_preview_view)
        {
            Ok(body) => adapter::binary_response(body),
            Err(error) => adapter::service_error(error),
        },
        Err(response) => response,
    }
}

async fn engine_preview(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<EnginePreviewRequest>(&bytes, vec![]) {
        Ok(request) => match state
            .api
            .preview_plan(request)
            .await
            .and_then(wire::encode_engine_preview_view)
        {
            Ok(body) => adapter::binary_response(body),
            Err(error) => adapter::service_error(error),
        },
        Err(response) => response,
    }
}

async fn artifact_content(
    State(state): State<ServiceState>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ArtifactContentRequest>(query, vec![]) {
        Ok(request) => match state
            .api
            .read_artifact_content(request)
            .and_then(wire::encode_artifact_content)
        {
            Ok(body) => adapter::binary_response(body),
            Err(error) => adapter::service_error(error),
        },
        Err(response) => response,
    }
}

// Workspace members, roles, and service accounts.

async fn member_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateMemberRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_member(request)),
        Err(response) => response,
    }
}

async fn member_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_member(request)),
        Err(response) => response,
    }
}

async fn member_revoke(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<RevokeMemberRequest>(
        &bytes,
        vec![("memberId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.revoke_member(request)),
        Err(response) => response,
    }
}

async fn member_role_assign(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<AssignRoleRequest>(&bytes, vec![("memberId".to_owned(), object_id)])
    {
        Ok(request) => adapter::ok_response(state.api.assign_role(request)),
        Err(response) => response,
    }
}

async fn role_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateRoleRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_role(request)),
        Err(response) => response,
    }
}

async fn role_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_role(request)),
        Err(response) => response,
    }
}

async fn role_capabilities_set(
    State(state): State<ServiceState>,
    Path(role_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<SetRoleCapabilitiesRequest>(
        &bytes,
        vec![("roleId".to_owned(), role_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.set_role_capabilities(request)),
        Err(response) => response,
    }
}

async fn service_account_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateServiceAccountRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_service_account(request)),
        Err(response) => response,
    }
}

async fn service_account_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_service_account(request)),
        Err(response) => response,
    }
}

async fn service_account_revoke(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<RevokeServiceAccountRequest>(
        &bytes,
        vec![("serviceAccountId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.revoke_service_account(request)),
        Err(response) => response,
    }
}

// Credential references (rotation lifecycle, revocation, recovery).

async fn credential_register(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<RegisterCredentialReferenceRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.register_credential_reference(request)),
        Err(response) => response,
    }
}

async fn credential_read(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("objectId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_credential_reference(request)),
        Err(response) => response,
    }
}

async fn credential_rotation_begin(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<BeginCredentialRotationRequest>(
        &bytes,
        vec![("credentialId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.begin_credential_rotation(request)),
        Err(response) => response,
    }
}

async fn credential_rotation_complete(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<CompleteCredentialRotationRequest>(
        &bytes,
        vec![("credentialId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.complete_credential_rotation(request)),
        Err(response) => response,
    }
}

async fn credential_revoke(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<RevokeCredentialRequest>(
        &bytes,
        vec![("credentialId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.revoke_credential(request)),
        Err(response) => response,
    }
}

async fn credential_recover(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<RecoverCredentialRequest>(
        &bytes,
        vec![("credentialId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.recover_credential(request)),
        Err(response) => response,
    }
}

// Source connection lifecycle updates.

async fn connection_update(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<UpdateSourceConnectionRequest>(
        &bytes,
        vec![("connectionId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.update_source_connection(request)),
        Err(response) => response,
    }
}

async fn connection_transition(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<TransitionSourceConnectionRequest>(
        &bytes,
        vec![("connectionId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.transition_source_connection(request)),
        Err(response) => response,
    }
}

async fn connection_retire(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<RetireSourceConnectionRequest>(
        &bytes,
        vec![("connectionId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.retire_source_connection(request)),
        Err(response) => response,
    }
}

// Dataset profile history, audit surfaces, and report reads.

async fn dataset_profile_history(
    State(state): State<ServiceState>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListProfileHistoryRequest>(
        query,
        vec![("datasetId".to_owned(), object_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_profile_history(request)),
        Err(response) => response,
    }
}

async fn audit_events_list(
    State(state): State<ServiceState>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListAuditEventsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_audit_events(request)),
        Err(response) => response,
    }
}

async fn audit_lineage_read(
    State(state): State<ServiceState>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<AuditLineageRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.get_audit_lineage(request)),
        Err(response) => response,
    }
}

async fn audit_export(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListAuditEventsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.export_audit_events(request)),
        Err(response) => response,
    }
}

async fn drift_report_read(
    State(state): State<ServiceState>,
    Path(artifact_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("artifactId".to_owned(), artifact_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_drift_report(request)),
        Err(response) => response,
    }
}

async fn quality_report_read(
    State(state): State<ServiceState>,
    Path(artifact_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ObjectIdRequest>(
        query,
        vec![("artifactId".to_owned(), artifact_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_quality_report(request)),
        Err(response) => response,
    }
}

async fn report_findings_list(
    State(state): State<ServiceState>,
    Path(artifact_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListFindingsRequest>(
        query,
        vec![("artifactId".to_owned(), artifact_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_report_findings(request)),
        Err(response) => response,
    }
}

// Automations.

async fn automation_create(State(state): State<ServiceState>, bytes: Bytes) -> Response {
    match adapter::parse_body::<CreateAutomationRequest>(&bytes, vec![]) {
        Ok(request) => adapter::ok_response(state.api.create_automation(request)),
        Err(response) => response,
    }
}

async fn automation_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListAutomationsRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_automations(request)),
        Err(response) => response,
    }
}

async fn automation_read(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<AutomationIdRequest>(
        query,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.read_automation(request)),
        Err(response) => response,
    }
}

async fn automation_update(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<UpdateAutomationRequest>(
        &bytes,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.update_automation(request)),
        Err(response) => response,
    }
}

async fn automation_pause(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<AutomationTransitionRequest>(
        &bytes,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.pause_automation(request)),
        Err(response) => response,
    }
}

async fn automation_resume(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<AutomationTransitionRequest>(
        &bytes,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.resume_automation(request)),
        Err(response) => response,
    }
}

async fn automation_delete(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<AutomationTransitionRequest>(
        &bytes,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.delete_automation(request)),
        Err(response) => response,
    }
}

async fn automation_next_run(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<AutomationIdRequest>(
        query,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.next_automation_run(request)),
        Err(response) => response,
    }
}

async fn automation_history(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListAutomationHistoryRequest>(
        query,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_automation_history(request)),
        Err(response) => response,
    }
}

async fn automation_trigger(
    State(state): State<ServiceState>,
    Path(automation_id): Path<String>,
    bytes: Bytes,
) -> Response {
    match adapter::parse_body::<TriggerAutomationRequest>(
        &bytes,
        vec![("automationId".to_owned(), automation_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.trigger_automation(request)),
        Err(response) => response,
    }
}

// Manifest list routes sharing a path with a mutating route.

async fn dataset_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_datasets(request)),
        Err(response) => response,
    }
}

async fn plan_list(State(state): State<ServiceState>, RawQuery(query): RawQuery) -> Response {
    match adapter::parse_query_envelope::<ListRequest>(query, vec![]) {
        Ok(request) => adapter::ok_response(state.api.list_plans(request)),
        Err(response) => response,
    }
}

async fn plan_version_list(
    State(state): State<ServiceState>,
    Path(plan_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match adapter::parse_query_envelope::<ListPlanVersionsRequest>(
        query,
        vec![("planId".to_owned(), plan_id)],
    ) {
        Ok(request) => adapter::ok_response(state.api.list_plan_versions(request)),
        Err(response) => response,
    }
}
