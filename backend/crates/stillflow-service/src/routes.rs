//! Manifest → axum route table (PR-1 client-loop subset; contract §6 T7).
//! Every route delegates to exactly one `ApiService` method; the registered
//! set is always a subset of the authoritative manifest. `artifact.content`
//! is intentionally absent: its `BatchEnvelope` payload has no transport wire
//! format yet (contract §6 staging note), unlike artifact metadata routes.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;

use stillflow_api::event_stream::EventStreamService;
use stillflow_api::{
    ApiService, ArchiveWorkspaceRequest, CancelJobRequest, ClonePlanRequest,
    CollectExportGarbageRequest, CreateDatasetRequest, CreatePlanRequest, CreateSessionRequest,
    CreateWorkspaceRequest, DiscoverAssetsRequest, EmptyRequest, ExportDownloadRequest,
    InspectAssetRequest, ListArtifactsRequest, ListEventsRequest, ListExportFilesRequest,
    ListJobsRequest, ListRequest, ListRunsRequest, ObjectIdRequest, PlanDiffRequest,
    PublishPlanVersionRequest, RegisterSourceConnectionRequest, SavePlanVersionRequest,
    SubmitDriftComparisonRequest, SubmitExportRequest, SubmitJobRequest,
    TestSourceConnectionRequest, TombstoneExportRequest, ValidatePlanRequest,
};

use crate::adapter;

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
        .route("/v1/connections/test", post(connection_test))
        .route(
            "/v1/connections",
            post(connection_register).get(connection_list),
        )
        .route("/v1/connections/{objectId}", get(connection_read))
        .route("/v1/assets", get(asset_list))
        .route("/v1/assets/discover", post(asset_discover))
        .route("/v1/assets/inspect", post(asset_inspect))
        .route("/v1/datasets", post(dataset_create))
        .route("/v1/datasets/{objectId}", get(dataset_read))
        .route("/v1/datasets/{objectId}/archive", post(dataset_archive))
        .route("/v1/plans", post(plan_create))
        .route("/v1/plans/clone", post(plan_clone))
        .route("/v1/plans/diff", post(plan_diff))
        .route("/v1/plans/validate", post(plan_validate))
        .route("/v1/plans/{objectId}", get(plan_load))
        .route("/v1/plans/{planId}/versions", post(plan_version_save))
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
        .route("/v1/events", get(event_list))
        .route("/v1/events/stream", get(crate::sse::events_stream))
        .route("/v1/artifacts/{objectId}", get(artifact_read))
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
