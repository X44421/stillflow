//! Typed, transport-neutral E5-A1 application service.
//!
//! This module is deliberately thin: storage owns durable object state and
//! compare-and-set transitions, Plan owns canonicalization, Engine owns
//! preview/execution semantics, and JobRuntime owns submission/cancellation.
//! The service only validates wire bounds, enforces Workspace scoping, and
//! maps those authorities to stable API DTOs.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use stillflow_connectors::ConnectorRegistry;
use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, ConnectionStatus, ConnectorKind, ControlPlaneEventType,
    ControlPlaneInput, DatasetState, DiscoverRequest, EventStreamKind, InspectRequest, JobState,
    LogicalSchema, PreviewRequest, RequestContext, RunState, SamplingStrategy, SessionState,
    SourceAsset, SourceConnection, SourceConnectionState, TestConnectionRequest,
};
use stillflow_engine::{ExecutionEngine, JobRuntime, PreviewRequest as EnginePreviewOpRequest};
use stillflow_plan::{LogicalPlan, PlanNodeId};
use stillflow_storage::{
    ArtifactCursor, ArtifactPage, ArtifactRefRecord, ArtifactSectionId, ControlPlaneStore,
    DatasetRecord, EventCursor, EventPage, JobCursor, JobPage, JobRecord, JobSubmission,
    PlanRecord, PlanVersionDraft, PlanVersionRecord, RunCursor, RunPage, RunRecord, SessionRecord,
    SnapshotStore, SourceAssetRecord, SourceConnectionRecord, SubmitOutcome, WorkspaceRecord,
};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    ApiError, ApiLimits, ApiRequest, ApiResponse, ApiResult, ApiVersion, RouteManifest,
    BOOTSTRAP_MANIFEST, SUPPORTED_API_VERSIONS,
};

/// A version negotiation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeRequest {
    pub requested_version: ApiVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeResponse {
    pub selected_version: ApiVersion,
    pub supported_versions: Vec<ApiVersion>,
    pub manifest: RouteManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceView {
    pub id: Uuid,
    pub state: stillflow_core::WorkspaceState,
    pub created_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub state: SessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceConnectionView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub kind: ConnectorKind,
    pub name: String,
    pub safe_config: Value,
    pub credential_ref: String,
    pub state: SourceConnectionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceAssetView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub connection_id: Uuid,
    pub kind: AssetKind,
    pub name: String,
    pub safe_locator: Value,
    pub state: stillflow_core::SourceAssetState,
    pub discovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub source_asset_id: Uuid,
    pub name: String,
    pub state: DatasetState,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub state: stillflow_core::PlanState,
    pub current_version_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanVersionView {
    pub id: Uuid,
    pub plan_id: Uuid,
    pub workspace_id: Uuid,
    pub version_number: u32,
    pub parent_version_id: Option<Uuid>,
    pub logical_plan: Value,
    pub canonical_plan_digest: String,
    pub plan_fingerprint: String,
    pub state: stillflow_core::PlanVersionState,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: String,
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub state: JobState,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub run_id: Option<Uuid>,
    pub failure: Option<stillflow_storage::FailureInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub job_id: Uuid,
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: String,
    pub plan_fingerprint: String,
    pub inputs: Vec<ControlPlaneInput>,
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub state: RunState,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure: Option<stillflow_storage::FailureInfo>,
    pub snapshot_ref: Option<Uuid>,
    pub bundle_ref: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventView {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub sequence: u64,
    pub event_type: ControlPlaneEventType,
    pub event_version: u16,
    pub occurred_at: DateTime<Utc>,
    pub job_id: Uuid,
    pub run_id: Option<Uuid>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactView {
    pub artifact_id: Uuid,
    pub workspace_id: Uuid,
    pub run_id: Uuid,
    pub artifact_kind: stillflow_core::ArtifactKind,
    pub external_ref_kind: String,
    pub external_ref_id: Uuid,
    pub content_digest: String,
    pub metadata: Value,
    pub state: stillflow_core::ArtifactRefState,
    pub created_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectList<T> {
    pub items: Vec<T>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWorkspaceRequest {
    pub workspace_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionRequest {
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterSourceConnectionRequest {
    pub connection_id: Uuid,
    pub kind: ConnectorKind,
    pub name: String,
    pub safe_config: Value,
    pub credential_ref: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSourceConnectionRequest {
    pub connection_id: Uuid,
    pub name: String,
    pub safe_config: Value,
    pub credential_ref: String,
    pub expected_updated_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectIdRequest {
    pub object_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveWorkspaceRequest {
    pub workspace_id: Uuid,
    pub archived_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseSessionRequest {
    pub session_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetireSourceConnectionRequest {
    pub connection_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestSourceConnectionRequest {
    pub connection_id: Uuid,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDatasetRequest {
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub source_asset_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePlanRequest {
    pub plan_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavePlanVersionRequest {
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub version_number: u32,
    pub parent_version_id: Option<Uuid>,
    pub logical_plan: LogicalPlan,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishPlanVersionRequest {
    pub plan_version_id: Uuid,
    pub expected_current_version_id: Option<Uuid>,
    pub published_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClonePlanRequest {
    pub source_version_id: Uuid,
    pub new_plan_id: Uuid,
    pub new_plan_version_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanDiffRequest {
    pub left_version_id: Uuid,
    pub right_version_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanDiffView {
    pub changed: bool,
    pub left_canonical_plan_digest: String,
    pub right_canonical_plan_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatePlanRequest {
    pub logical_plan: LogicalPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatePlanView {
    pub canonical_plan_digest: String,
    pub plan_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitJobRequest {
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    pub job_id: Uuid,
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub queued_at: DateTime<Utc>,
    pub event_id: Uuid,
    pub correlation_id: String,
    pub actor_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelJobRequest {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListRequest {
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListJobsRequest {
    pub limit: usize,
    pub cursor: Option<JobCursorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobCursorView {
    pub queued_at: DateTime<Utc>,
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListRunsRequest {
    pub limit: usize,
    pub cursor: Option<RunCursorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCursorView {
    pub started_at: DateTime<Utc>,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListEventsRequest {
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub cursor: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventPageView {
    pub events: Vec<EventView>,
    pub next_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListArtifactsRequest {
    pub run_id: Uuid,
    pub limit: usize,
    pub cursor: Option<ArtifactCursorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactCursorView {
    pub created_at: DateTime<Utc>,
    pub artifact_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactPageView {
    pub artifacts: Vec<ArtifactView>,
    pub next: Option<ArtifactCursorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverAssetsRequest {
    pub connection_id: Uuid,
    pub parent_path: Option<String>,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectAssetRequest {
    pub connection_id: Uuid,
    pub asset_id: Uuid,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewAssetRequest {
    pub connection_id: Uuid,
    pub asset_id: Uuid,
    pub row_limit: usize,
    pub byte_limit: usize,
    pub timeout_seconds: Option<u64>,
}

/// The connector preview is typed binary data; it is not coerced into an
/// unbounded JSON value. Callers can inspect the bounded Arrow envelopes.
#[derive(Debug, Clone)]
pub struct PreviewView {
    pub schema: LogicalSchema,
    pub batches: Vec<stillflow_core::BatchEnvelope>,
    pub rows_returned: usize,
    pub bytes_returned: usize,
    pub rows_truncated: bool,
    pub bytes_truncated: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnginePreviewRequest {
    pub plan: LogicalPlan,
    pub target_node_id: PlanNodeId,
    pub connection_id: Uuid,
    pub asset_id: Uuid,
    pub batch_size: usize,
    pub row_limit: usize,
    pub byte_limit: usize,
    pub timeout_seconds: Option<u64>,
}

/// Engine preview stays a typed Arrow result and never creates a Job, Run, or
/// Artifact record.
#[derive(Debug, Clone)]
pub struct EnginePreviewView {
    pub plan_fingerprint: String,
    pub target_node_id: PlanNodeId,
    pub schema: LogicalSchema,
    pub batches: Vec<stillflow_core::BatchEnvelope>,
    pub rows_returned: usize,
    pub bytes_returned: usize,
    pub source_rows_scanned: usize,
    pub source_bytes_scanned: usize,
    pub rows_truncated: bool,
    pub bytes_truncated: bool,
    pub scan_truncated: bool,
    pub source_exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactContentRequest {
    pub bundle_id: Uuid,
    pub artifact_id: Uuid,
    pub section_id: ArtifactSectionId,
    pub after_partition_sequence: Option<u32>,
    pub max_rows: usize,
    pub max_bytes: usize,
}

/// One bounded report page. The iterator is stopped before the next partition
/// would exceed either caller bound, so the service never collects an entire
/// artifact in memory.
#[derive(Debug, Clone)]
pub struct ArtifactContentPage {
    pub batches: Vec<stillflow_core::BatchEnvelope>,
    pub next_partition_sequence: Option<u32>,
}

pub struct ApiService {
    control_plane: Arc<ControlPlaneStore>,
    connectors: Option<Arc<ConnectorRegistry>>,
    engine: Option<Arc<ExecutionEngine>>,
    runtime: Option<Arc<JobRuntime>>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    limits: ApiLimits,
}

impl std::fmt::Debug for ApiService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiService")
            .field("limits", &self.limits)
            .field("connectors_configured", &self.connectors.is_some())
            .field("engine_configured", &self.engine.is_some())
            .field("runtime_configured", &self.runtime.is_some())
            .field("snapshot_store_configured", &self.snapshot_store.is_some())
            .finish_non_exhaustive()
    }
}

impl ApiService {
    pub fn new(control_plane: Arc<ControlPlaneStore>) -> Self {
        Self {
            control_plane,
            connectors: None,
            engine: None,
            runtime: None,
            snapshot_store: None,
            limits: ApiLimits::default(),
        }
    }

    pub fn with_connectors(mut self, connectors: Arc<ConnectorRegistry>) -> Self {
        self.connectors = Some(connectors);
        self
    }

    pub fn with_engine(mut self, engine: Arc<ExecutionEngine>) -> Self {
        self.engine = Some(engine);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<JobRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_snapshot_store(mut self, snapshot_store: Arc<SnapshotStore>) -> Self {
        self.snapshot_store = Some(snapshot_store);
        self
    }

    pub fn with_limits(mut self, limits: ApiLimits) -> Self {
        self.limits = limits.bounded();
        self
    }

    pub fn limits(&self) -> ApiLimits {
        self.limits
    }

    pub fn handshake(
        &self,
        request: ApiRequest<HandshakeRequest>,
    ) -> ApiResult<ApiResponse<HandshakeResponse>> {
        self.validate_meta(&request, false)?;
        let requested = request.body.requested_version;
        if !requested.is_supported() {
            return Err(ApiError::unsupported_version(requested.value()));
        }
        Ok(ApiResponse::new(
            request.meta.request_id,
            HandshakeResponse {
                selected_version: requested,
                supported_versions: SUPPORTED_API_VERSIONS.to_vec(),
                manifest: BOOTSTRAP_MANIFEST,
            },
        ))
    }

    pub fn create_workspace(
        &self,
        request: ApiRequest<CreateWorkspaceRequest>,
    ) -> ApiResult<ApiResponse<WorkspaceView>> {
        self.validate_meta(&request, true)?;
        if request.meta.workspace_id != request.body.workspace_id {
            return Err(ApiError::invalid("workspace request identity mismatch"));
        }
        let record = self
            .control_plane
            .create_workspace(request.body.workspace_id, request.body.created_at)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            workspace_view(record),
        ))
    }

    pub fn archive_workspace(
        &self,
        request: ApiRequest<ArchiveWorkspaceRequest>,
    ) -> ApiResult<ApiResponse<WorkspaceView>> {
        self.validate_meta(&request, true)?;
        self.scope_workspace(request.body.workspace_id, request.meta.workspace_id)?;
        let record = self
            .control_plane
            .archive_workspace(request.body.workspace_id, request.body.archived_at)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            workspace_view(record),
        ))
    }

    pub fn create_session(
        &self,
        request: ApiRequest<CreateSessionRequest>,
    ) -> ApiResult<ApiResponse<SessionView>> {
        self.validate_meta(&request, true)?;
        let record = self.control_plane.create_session(
            request.meta.workspace_id,
            request.body.session_id,
            request.body.created_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            session_view(record),
        ))
    }

    pub fn list_sessions(
        &self,
        request: ApiRequest<ListRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<SessionView>>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self
            .control_plane
            .list_sessions(request.meta.workspace_id, limit)?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(session_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub fn read_session(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<SessionView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_session(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            session_view(record),
        ))
    }

    pub fn close_session(
        &self,
        request: ApiRequest<CloseSessionRequest>,
    ) -> ApiResult<ApiResponse<SessionView>> {
        self.validate_meta(&request, true)?;
        let record = self.control_plane.get_session(request.body.session_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        let target = match record.state {
            SessionState::Open => SessionState::Closed,
            SessionState::Closing => SessionState::Closed,
            SessionState::Closed => {
                return Ok(ApiResponse::new(
                    request.meta.request_id,
                    session_view(record),
                ))
            }
        };
        let record = self.control_plane.transition_session(
            request.body.session_id,
            target,
            request.body.updated_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            session_view(record),
        ))
    }

    pub fn register_source_connection(
        &self,
        request: ApiRequest<RegisterSourceConnectionRequest>,
    ) -> ApiResult<ApiResponse<SourceConnectionView>> {
        self.validate_meta(&request, true)?;
        let credential_ref = stillflow_core::CredentialRef::new(request.body.credential_ref)
            .map_err(ApiError::from)?;
        let record = self.control_plane.create_source_connection(
            request.meta.workspace_id,
            request.body.connection_id,
            request.body.kind,
            request.body.name,
            request.body.safe_config,
            credential_ref,
            request.body.created_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            source_connection_view(record),
        ))
    }

    pub fn list_source_connections(
        &self,
        request: ApiRequest<ListRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<SourceConnectionView>>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self
            .control_plane
            .list_source_connections(request.meta.workspace_id, limit)?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(source_connection_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub async fn test_source_connection(
        &self,
        request: ApiRequest<TestSourceConnectionRequest>,
    ) -> ApiResult<ApiResponse<ConnectionStatus>> {
        self.validate_meta(&request, false)?;
        let connection_record = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        self.ensure_scope(connection_record.workspace_id, request.meta.workspace_id)?;
        let registry = self
            .connectors
            .as_ref()
            .ok_or_else(|| ApiError::conflict("connector registry is not configured"))?;
        let result = registry
            .test_connection(
                &source_connection_domain(&connection_record)?,
                TestConnectionRequest {
                    context: self.request_context(request.body.timeout_seconds)?,
                },
            )
            .await
            .map_err(ApiError::from)?;
        Ok(ApiResponse::new(request.meta.request_id, result))
    }

    pub fn read_source_connection(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<SourceConnectionView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .get_source_connection(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            source_connection_view(record),
        ))
    }

    pub fn update_source_connection(
        &self,
        request: ApiRequest<UpdateSourceConnectionRequest>,
    ) -> ApiResult<ApiResponse<SourceConnectionView>> {
        self.validate_meta(&request, true)?;
        let current = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        self.ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let credential_ref = stillflow_core::CredentialRef::new(request.body.credential_ref)
            .map_err(ApiError::from)?;
        let record = self.control_plane.update_source_connection(
            request.body.connection_id,
            request.body.name,
            request.body.safe_config,
            credential_ref,
            request.body.expected_updated_at,
            request.body.updated_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            source_connection_view(record),
        ))
    }

    pub fn transition_source_connection(
        &self,
        request: ApiRequest<TransitionSourceConnectionRequest>,
    ) -> ApiResult<ApiResponse<SourceConnectionView>> {
        self.validate_meta(&request, true)?;
        let current = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        self.ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let record = self.control_plane.transition_source_connection(
            request.body.connection_id,
            request.body.target,
            request.body.updated_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            source_connection_view(record),
        ))
    }

    pub fn retire_source_connection(
        &self,
        request: ApiRequest<RetireSourceConnectionRequest>,
    ) -> ApiResult<ApiResponse<SourceConnectionView>> {
        self.transition_source_connection(ApiRequest {
            meta: request.meta,
            body: TransitionSourceConnectionRequest {
                connection_id: request.body.connection_id,
                target: SourceConnectionState::Retired,
                updated_at: request.body.updated_at,
            },
        })
    }

    pub fn list_source_assets(
        &self,
        request: ApiRequest<ListRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<SourceAssetView>>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self
            .control_plane
            .list_source_assets(request.meta.workspace_id, limit)?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(source_asset_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub async fn discover_source_assets(
        &self,
        request: ApiRequest<DiscoverAssetsRequest>,
    ) -> ApiResult<ApiResponse<Vec<SourceAssetView>>> {
        self.validate_meta(&request, true)?;
        let connection_record = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        self.ensure_scope(connection_record.workspace_id, request.meta.workspace_id)?;
        let connection = source_connection_domain(&connection_record)?;
        let context = self.request_context(request.body.timeout_seconds)?;
        let registry = self
            .connectors
            .as_ref()
            .ok_or_else(|| ApiError::conflict("connector registry is not configured"))?;
        let assets = registry
            .discover(
                &connection,
                DiscoverRequest {
                    context,
                    parent_path: request.body.parent_path,
                },
            )
            .await
            .map_err(ApiError::from)?;
        let mut views = Vec::with_capacity(assets.len());
        for asset in assets {
            let locator = serde_json::to_value(&asset.locator).map_err(|_| ApiError::internal())?;
            let record = self.control_plane.create_source_asset(
                request.meta.workspace_id,
                connection_record.id,
                asset.id,
                asset.kind,
                asset.name,
                locator,
                asset.discovered_at,
            )?;
            views.push(source_asset_view(record));
        }
        Ok(ApiResponse::new(request.meta.request_id, views))
    }

    pub async fn inspect_source_asset(
        &self,
        request: ApiRequest<InspectAssetRequest>,
    ) -> ApiResult<ApiResponse<AssetMetadata>> {
        self.validate_meta(&request, false)?;
        let connection_record = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        let asset_record = self.control_plane.get_source_asset(request.body.asset_id)?;
        self.ensure_scope(connection_record.workspace_id, request.meta.workspace_id)?;
        self.ensure_scope(asset_record.workspace_id, request.meta.workspace_id)?;
        if asset_record.connection_id != connection_record.id {
            return Err(ApiError::not_found());
        }
        let registry = self
            .connectors
            .as_ref()
            .ok_or_else(|| ApiError::conflict("connector registry is not configured"))?;
        let metadata = registry
            .inspect(
                &source_connection_domain(&connection_record)?,
                InspectRequest {
                    context: self.request_context(request.body.timeout_seconds)?,
                    asset: source_asset_domain(&asset_record)?,
                },
            )
            .await
            .map_err(ApiError::from)?;
        Ok(ApiResponse::new(request.meta.request_id, metadata))
    }

    pub async fn preview_source_asset(
        &self,
        request: ApiRequest<PreviewAssetRequest>,
    ) -> ApiResult<ApiResponse<PreviewView>> {
        self.validate_meta(&request, false)?;
        let connection_record = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        let asset_record = self.control_plane.get_source_asset(request.body.asset_id)?;
        self.ensure_scope(connection_record.workspace_id, request.meta.workspace_id)?;
        self.ensure_scope(asset_record.workspace_id, request.meta.workspace_id)?;
        if asset_record.connection_id != connection_record.id {
            return Err(ApiError::not_found());
        }
        let registry = self
            .connectors
            .as_ref()
            .ok_or_else(|| ApiError::conflict("connector registry is not configured"))?;
        let mut preview = PreviewRequest::new(
            source_asset_domain(&asset_record)?,
            request.body.row_limit,
            request.body.byte_limit,
        );
        preview.context = self.request_context(request.body.timeout_seconds)?;
        preview.sampling = SamplingStrategy::Head;
        let result = registry
            .preview(&source_connection_domain(&connection_record)?, preview)
            .await
            .map_err(ApiError::from)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            PreviewView {
                schema: result.schema,
                batches: result.batches,
                rows_returned: result.rows_returned,
                bytes_returned: result.bytes_returned,
                rows_truncated: result.rows_truncated,
                bytes_truncated: result.bytes_truncated,
                warnings: result.warnings,
            },
        ))
    }

    pub fn create_dataset(
        &self,
        request: ApiRequest<CreateDatasetRequest>,
    ) -> ApiResult<ApiResponse<DatasetView>> {
        self.validate_meta(&request, true)?;
        let record = self.control_plane.create_dataset(
            request.meta.workspace_id,
            request.body.session_id,
            request.body.source_asset_id,
            request.body.dataset_id,
            request.body.name,
            request.body.created_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            dataset_view(record),
        ))
    }

    pub fn list_datasets(
        &self,
        request: ApiRequest<ListRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<DatasetView>>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self
            .control_plane
            .list_datasets(request.meta.workspace_id, limit)?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(dataset_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub fn read_dataset(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<DatasetView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_dataset(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            dataset_view(record),
        ))
    }

    pub fn archive_dataset(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<DatasetView>> {
        self.validate_meta(&request, true)?;
        let current = self.control_plane.get_dataset(request.body.object_id)?;
        self.ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let record = self.control_plane.archive_dataset(request.body.object_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            dataset_view(record),
        ))
    }

    pub fn create_plan(
        &self,
        request: ApiRequest<CreatePlanRequest>,
    ) -> ApiResult<ApiResponse<PlanView>> {
        self.validate_meta(&request, true)?;
        let record = self.control_plane.create_plan(
            request.meta.workspace_id,
            request.body.plan_id,
            request.body.created_at,
        )?;
        Ok(ApiResponse::new(request.meta.request_id, plan_view(record)))
    }

    pub fn list_plans(
        &self,
        request: ApiRequest<ListRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<PlanView>>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self
            .control_plane
            .list_plans(request.meta.workspace_id, limit)?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(plan_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub fn load_plan(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<PlanView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_plan(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(request.meta.request_id, plan_view(record)))
    }

    pub fn save_plan_version(
        &self,
        request: ApiRequest<SavePlanVersionRequest>,
    ) -> ApiResult<ApiResponse<PlanVersionView>> {
        self.validate_meta(&request, true)?;
        let plan = self.control_plane.get_plan(request.body.plan_id)?;
        self.ensure_scope(plan.workspace_id, request.meta.workspace_id)?;
        let canonical = request
            .body
            .logical_plan
            .canonical_bytes()
            .map_err(|_| ApiError::invalid("logical plan failed authoritative validation"))?;
        let draft = PlanVersionDraft {
            workspace_id: request.meta.workspace_id,
            plan_id: request.body.plan_id,
            plan_version_id: request.body.plan_version_id,
            version_number: request.body.version_number,
            parent_version_id: request.body.parent_version_id,
            logical_plan: serde_json::to_value(&request.body.logical_plan)
                .map_err(|_| ApiError::invalid("logical plan cannot be serialized"))?,
            canonical_plan_digest: sha256(&canonical),
            canonical_plan_bytes: canonical,
            plan_fingerprint: *request
                .body
                .logical_plan
                .fingerprint()
                .map_err(|_| ApiError::invalid("logical plan fingerprint failed"))?
                .as_bytes(),
            created_at: request.body.created_at,
        };
        let record = self.control_plane.create_plan_version(draft)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            plan_version_view(record),
        ))
    }

    pub fn list_plan_versions(
        &self,
        request: ApiRequest<ListPlanVersionsRequest>,
    ) -> ApiResult<ApiResponse<ObjectList<PlanVersionView>>> {
        self.validate_meta(&request, false)?;
        let plan = self.control_plane.get_plan(request.body.plan_id)?;
        self.ensure_scope(plan.workspace_id, request.meta.workspace_id)?;
        let limit = self.page_limit(request.body.limit)?;
        let records = self.control_plane.list_plan_versions(
            request.meta.workspace_id,
            request.body.plan_id,
            limit,
        )?;
        let has_more = records.len() == limit;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ObjectList {
                items: records.into_iter().map(plan_version_view).collect(),
                page: PageInfo { has_more },
            },
        ))
    }

    pub fn load_plan_version(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<PlanVersionView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .get_plan_version(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            plan_version_view(record),
        ))
    }

    pub fn publish_plan_version(
        &self,
        request: ApiRequest<PublishPlanVersionRequest>,
    ) -> ApiResult<ApiResponse<PlanVersionView>> {
        self.validate_meta(&request, true)?;
        let version = self
            .control_plane
            .get_plan_version(request.body.plan_version_id)?;
        self.ensure_scope(version.workspace_id, request.meta.workspace_id)?;
        let record = self.control_plane.publish_plan_version(
            request.body.plan_version_id,
            request.body.expected_current_version_id,
            request.body.published_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            plan_version_view(record),
        ))
    }

    pub fn clone_plan(
        &self,
        request: ApiRequest<ClonePlanRequest>,
    ) -> ApiResult<ApiResponse<PlanVersionView>> {
        self.validate_meta(&request, true)?;
        let source = self
            .control_plane
            .get_plan_version(request.body.source_version_id)?;
        self.ensure_scope(source.workspace_id, request.meta.workspace_id)?;
        self.control_plane.create_plan(
            request.meta.workspace_id,
            request.body.new_plan_id,
            request.body.created_at,
        )?;
        let record = self.control_plane.create_plan_version(PlanVersionDraft {
            workspace_id: request.meta.workspace_id,
            plan_id: request.body.new_plan_id,
            plan_version_id: request.body.new_plan_version_id,
            version_number: 1,
            parent_version_id: None,
            logical_plan: source.logical_plan,
            canonical_plan_bytes: source.canonical_plan_bytes,
            canonical_plan_digest: source.canonical_plan_digest,
            plan_fingerprint: source.plan_fingerprint,
            created_at: request.body.created_at,
        })?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            plan_version_view(record),
        ))
    }

    pub fn diff_plans(
        &self,
        request: ApiRequest<PlanDiffRequest>,
    ) -> ApiResult<ApiResponse<PlanDiffView>> {
        self.validate_meta(&request, false)?;
        let left = self
            .control_plane
            .get_plan_version(request.body.left_version_id)?;
        let right = self
            .control_plane
            .get_plan_version(request.body.right_version_id)?;
        self.ensure_scope(left.workspace_id, request.meta.workspace_id)?;
        self.ensure_scope(right.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            PlanDiffView {
                changed: left.canonical_plan_digest != right.canonical_plan_digest,
                left_canonical_plan_digest: digest_hex(&left.canonical_plan_digest),
                right_canonical_plan_digest: digest_hex(&right.canonical_plan_digest),
            },
        ))
    }

    pub fn validate_plan(
        &self,
        request: ApiRequest<ValidatePlanRequest>,
    ) -> ApiResult<ApiResponse<ValidatePlanView>> {
        self.validate_meta(&request, false)?;
        let canonical = request
            .body
            .logical_plan
            .canonical_bytes()
            .map_err(|_| ApiError::invalid("logical plan failed authoritative validation"))?;
        let fingerprint = request
            .body
            .logical_plan
            .fingerprint()
            .map_err(|_| ApiError::invalid("logical plan fingerprint failed"))?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ValidatePlanView {
                canonical_plan_digest: digest_hex(&sha256(&canonical)),
                plan_fingerprint: fingerprint.to_string(),
            },
        ))
    }

    pub async fn preview_plan(
        &self,
        request: ApiRequest<EnginePreviewRequest>,
    ) -> ApiResult<ApiResponse<EnginePreviewView>> {
        self.validate_meta(&request, false)?;
        let connection_record = self
            .control_plane
            .get_source_connection(request.body.connection_id)?;
        let asset_record = self.control_plane.get_source_asset(request.body.asset_id)?;
        self.ensure_scope(connection_record.workspace_id, request.meta.workspace_id)?;
        self.ensure_scope(asset_record.workspace_id, request.meta.workspace_id)?;
        if asset_record.connection_id != connection_record.id {
            return Err(ApiError::not_found());
        }
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| ApiError::conflict("execution engine is not configured"))?;
        let context = self.request_context(request.body.timeout_seconds)?;
        let mut engine_request = EnginePreviewOpRequest::new(
            request.body.plan,
            request.body.target_node_id,
            source_connection_domain(&connection_record)?,
            source_asset_domain(&asset_record)?,
        );
        engine_request.batch_size = request.body.batch_size;
        engine_request.row_limit = request.body.row_limit;
        engine_request.byte_limit = request.body.byte_limit;
        engine_request.context = context;
        let result = engine.preview(engine_request).await?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            EnginePreviewView {
                plan_fingerprint: result.plan_fingerprint.to_string(),
                target_node_id: result.target_node_id,
                schema: result.schema,
                batches: result.batches,
                rows_returned: result.rows_returned,
                bytes_returned: result.bytes_returned,
                source_rows_scanned: result.source_rows_scanned,
                source_bytes_scanned: result.source_bytes_scanned,
                rows_truncated: result.rows_truncated,
                bytes_truncated: result.bytes_truncated,
                scan_truncated: result.scan_truncated,
                source_exhausted: result.source_exhausted,
            },
        ))
    }

    pub fn submit_job(
        &self,
        request: ApiRequest<SubmitJobRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, true)?;
        let idempotency_key = request
            .meta
            .idempotency_key
            .clone()
            .ok_or_else(|| ApiError::invalid("job submission requires an idempotency key"))?;
        let version = self
            .control_plane
            .get_plan_version(request.body.plan_version_id)?;
        self.ensure_scope(version.workspace_id, request.meta.workspace_id)?;
        let submission = JobSubmission::try_new(
            request.meta.workspace_id,
            request.body.session_id,
            request.body.plan_version_id,
            version.canonical_plan_digest,
            request.body.job_id,
            idempotency_key,
            request.body.inputs,
            request.body.execution_policy,
            request.body.output_policy,
            request.body.queued_at,
            request.body.event_id,
            request.meta.request_id.to_string(),
            request.body.correlation_id,
            request.body.actor_ref,
        )?;
        let outcome = if let Some(runtime) = &self.runtime {
            runtime.submit_job(submission)?
        } else {
            // The control-plane method is the same durable primitive used by
            // JobRuntime; this fallback keeps the library boundary usable in
            // deterministic tests when no worker runtime is attached.
            self.control_plane.submit_job(submission)?
        };
        let job = match outcome {
            SubmitOutcome::Created(job) | SubmitOutcome::Replayed(job) => job,
        };
        Ok(ApiResponse::new(request.meta.request_id, job_view(job)))
    }

    pub async fn cancel_job(
        &self,
        request: ApiRequest<CancelJobRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, true)?;
        let current = self.control_plane.get_job(request.body.job_id)?;
        self.ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| ApiError::conflict("JobRuntime is required for cancellation"))?;
        let job = runtime
            .cancel(request.body.job_id, request.meta.request_id.to_string())
            .await?;
        Ok(ApiResponse::new(request.meta.request_id, job_view(job)))
    }

    pub fn read_job(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_job(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(request.meta.request_id, job_view(record)))
    }

    pub fn get_job_status(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.read_job(request)
    }

    pub fn list_jobs(
        &self,
        request: ApiRequest<ListJobsRequest>,
    ) -> ApiResult<ApiResponse<JobPageView>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let cursor = request.body.cursor.map(|value| JobCursor {
            workspace_id: request.meta.workspace_id,
            queued_at_utc: value.queued_at,
            job_id: value.job_id,
        });
        let page = self
            .control_plane
            .list_jobs(request.meta.workspace_id, cursor, limit)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            job_page_view(page),
        ))
    }

    pub fn read_run(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<RunView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_run(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(request.meta.request_id, run_view(record)))
    }

    pub fn get_run_status(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<RunView>> {
        self.read_run(request)
    }

    pub fn list_runs(
        &self,
        request: ApiRequest<ListRunsRequest>,
    ) -> ApiResult<ApiResponse<RunPageView>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let cursor = request.body.cursor.map(|value| RunCursor {
            workspace_id: request.meta.workspace_id,
            started_at_utc: value.started_at,
            run_id: value.run_id,
        });
        let page = self
            .control_plane
            .list_runs(request.meta.workspace_id, cursor, limit)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            run_page_view(page),
        ))
    }

    pub fn list_events(
        &self,
        request: ApiRequest<ListEventsRequest>,
    ) -> ApiResult<ApiResponse<EventPageView>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let cursor = request.body.cursor.map(|sequence| EventCursor {
            workspace_id: request.meta.workspace_id,
            stream_kind: request.body.stream_kind,
            stream_id: request.body.stream_id,
            sequence,
        });
        let page = self.control_plane.list_events(
            request.meta.workspace_id,
            request.body.stream_kind,
            request.body.stream_id,
            cursor,
            limit,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            event_page_view(page),
        ))
    }

    pub fn get_artifact_metadata(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<ArtifactView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .get_artifact_ref(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            artifact_view(record),
        ))
    }

    pub fn list_artifact_metadata(
        &self,
        request: ApiRequest<ListArtifactsRequest>,
    ) -> ApiResult<ApiResponse<ArtifactPageView>> {
        self.validate_meta(&request, false)?;
        let limit = self.page_limit(request.body.limit)?;
        let cursor = request.body.cursor.map(|value| ArtifactCursor {
            workspace_id: request.meta.workspace_id,
            run_id: request.body.run_id,
            created_at_utc: value.created_at,
            artifact_id: value.artifact_id,
        });
        let page = self.control_plane.list_artifact_refs(
            request.meta.workspace_id,
            request.body.run_id,
            cursor,
            limit,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            artifact_page_view(page),
        ))
    }

    pub fn read_artifact_content(
        &self,
        request: ApiRequest<ArtifactContentRequest>,
    ) -> ApiResult<ApiResponse<ArtifactContentPage>> {
        self.validate_meta(&request, false)?;
        if request.body.max_rows == 0 || request.body.max_rows > self.limits.max_rows_per_page {
            return Err(ApiError::limit("artifact row page exceeds the API bound"));
        }
        if request.body.max_bytes == 0
            || request.body.max_bytes > self.limits.max_artifact_page_bytes
        {
            return Err(ApiError::limit("artifact byte page exceeds the API bound"));
        }
        let artifact = self
            .control_plane
            .get_artifact_ref(request.body.artifact_id)?;
        self.ensure_scope(artifact.workspace_id, request.meta.workspace_id)?;
        let mut reader = self
            .snapshot_store
            .as_ref()
            .ok_or_else(|| ApiError::conflict("snapshot store is not configured"))?
            .open_artifact_section(
                request.body.bundle_id,
                request.body.artifact_id,
                request.body.section_id,
            )
            .map_err(ApiError::from)?;
        let partitions = reader.section().partitions().to_vec();
        let after = request.body.after_partition_sequence;
        let mut batches = Vec::new();
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut next = None;
        for (index, partition) in partitions.iter().enumerate() {
            if after.is_some_and(|cursor| partition.sequence() <= cursor) {
                let _ = reader.next().transpose().map_err(ApiError::from)?;
                continue;
            }
            let partition_rows = usize::try_from(partition.row_count())
                .map_err(|_| ApiError::limit("artifact row count exceeds addressable memory"))?;
            let partition_bytes = usize::try_from(partition.stored_byte_count())
                .map_err(|_| ApiError::limit("artifact byte count exceeds addressable memory"))?;
            if partition_rows > request.body.max_rows || partition_bytes > request.body.max_bytes {
                if batches.is_empty() {
                    return Err(ApiError::limit("artifact partition exceeds page bound"));
                }
                next = batches
                    .last()
                    .and_then(|_batch: &stillflow_core::BatchEnvelope| {
                        partitions
                            .get(index.saturating_sub(1))
                            .map(|previous| previous.sequence())
                    });
                break;
            }
            if rows.saturating_add(partition_rows) > request.body.max_rows
                || bytes.saturating_add(partition_bytes) > request.body.max_bytes
            {
                next = batches
                    .last()
                    .and_then(|_: &stillflow_core::BatchEnvelope| {
                        partitions
                            .get(index.saturating_sub(1))
                            .map(|previous| previous.sequence())
                    });
                break;
            }
            let batch = reader
                .next()
                .transpose()
                .map_err(ApiError::from)?
                .ok_or_else(ApiError::internal)?;
            rows = rows.saturating_add(batch.row_count());
            bytes = bytes.saturating_add(batch.byte_count());
            batches.push(batch);
            if index + 1 < partitions.len() {
                next = partitions.get(index).map(|current| current.sequence());
            } else {
                next = None;
            }
        }
        Ok(ApiResponse::new(
            request.meta.request_id,
            ArtifactContentPage {
                batches,
                next_partition_sequence: next,
            },
        ))
    }

    fn validate_meta<T>(&self, request: &ApiRequest<T>, mutation: bool) -> ApiResult<()> {
        request.validate_version()?;
        if request.meta.request_id.is_nil() || request.meta.workspace_id.is_nil() {
            return Err(ApiError::invalid(
                "request and workspace identities are required",
            ));
        }
        if mutation {
            if let Some(key) = &request.meta.idempotency_key {
                if key.is_empty() || key.len() > 128 || key.trim() != key {
                    return Err(ApiError::invalid("invalid idempotency key"));
                }
                if key.chars().any(char::is_control) {
                    return Err(ApiError::invalid("invalid idempotency key"));
                }
            }
        }
        Ok(())
    }

    fn page_limit(&self, requested: usize) -> ApiResult<usize> {
        if requested == 0 || requested > self.limits.max_rows_per_page {
            return Err(ApiError::limit("page size exceeds the API bound"));
        }
        Ok(requested.min(stillflow_core::MAX_EVENT_PAGE_SIZE))
    }

    fn request_context(&self, timeout_seconds: Option<u64>) -> ApiResult<RequestContext> {
        let seconds = timeout_seconds.unwrap_or(self.limits.max_timeout_seconds);
        if seconds == 0 || seconds > self.limits.max_timeout_seconds {
            return Err(ApiError::limit("request timeout exceeds the API bound"));
        }
        Ok(RequestContext::with_deadline(
            Instant::now() + Duration::from_secs(seconds),
        ))
    }

    fn scope_workspace(&self, object_id: Uuid, workspace_id: Uuid) -> ApiResult<()> {
        let record = self.control_plane.get_workspace(object_id)?;
        self.ensure_scope(record.id, workspace_id)
    }

    fn ensure_scope(&self, object_workspace: Uuid, request_workspace: Uuid) -> ApiResult<()> {
        if object_workspace == request_workspace {
            Ok(())
        } else {
            Err(ApiError::not_found())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransitionSourceConnectionRequest {
    pub connection_id: Uuid,
    pub target: SourceConnectionState,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPlanVersionsRequest {
    pub plan_id: Uuid,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobPageView {
    pub jobs: Vec<JobView>,
    pub next: Option<JobCursorView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPageView {
    pub runs: Vec<RunView>,
    pub next: Option<RunCursorView>,
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn digest_hex(bytes: &[u8; 32]) -> String {
    let mut result = String::with_capacity(64);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

fn workspace_view(record: WorkspaceRecord) -> WorkspaceView {
    WorkspaceView {
        id: record.id,
        state: record.state,
        created_at: record.created_at,
        archived_at: record.archived_at,
    }
}

fn session_view(record: SessionRecord) -> SessionView {
    SessionView {
        id: record.id,
        workspace_id: record.workspace_id,
        state: record.state,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn source_connection_view(record: SourceConnectionRecord) -> SourceConnectionView {
    SourceConnectionView {
        id: record.id,
        workspace_id: record.workspace_id,
        kind: record.kind,
        name: record.name,
        safe_config: record.safe_config,
        credential_ref: record.credential_ref,
        state: record.state,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn source_asset_view(record: SourceAssetRecord) -> SourceAssetView {
    SourceAssetView {
        id: record.id,
        workspace_id: record.workspace_id,
        connection_id: record.connection_id,
        kind: record.kind,
        name: record.name,
        safe_locator: record.safe_locator,
        state: record.state,
        discovered_at: record.discovered_at,
    }
}

fn dataset_view(record: DatasetRecord) -> DatasetView {
    DatasetView {
        id: record.id,
        workspace_id: record.workspace_id,
        session_id: record.session_id,
        source_asset_id: record.source_asset_id,
        name: record.name,
        state: record.state,
        created_at: record.created_at,
    }
}

fn plan_view(record: PlanRecord) -> PlanView {
    PlanView {
        id: record.id,
        workspace_id: record.workspace_id,
        state: record.state,
        current_version_id: record.current_version_id,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn plan_version_view(record: PlanVersionRecord) -> PlanVersionView {
    PlanVersionView {
        id: record.id,
        plan_id: record.plan_id,
        workspace_id: record.workspace_id,
        version_number: record.version_number,
        parent_version_id: record.parent_version_id,
        logical_plan: record.logical_plan,
        canonical_plan_digest: digest_hex(&record.canonical_plan_digest),
        plan_fingerprint: digest_hex(&record.plan_fingerprint),
        state: record.state,
        created_at: record.created_at,
        published_at: record.published_at,
        archived_at: record.archived_at,
    }
}

fn job_view(record: JobRecord) -> JobView {
    JobView {
        id: record.id,
        workspace_id: record.workspace_id,
        session_id: record.session_id,
        plan_version_id: record.plan_version_id,
        canonical_plan_digest: digest_hex(&record.canonical_plan_digest),
        inputs: record.inputs,
        execution_policy: record.execution_policy,
        output_policy: record.output_policy,
        state: record.state,
        queued_at: record.queued_at,
        started_at: record.started_at,
        finished_at: record.finished_at,
        run_id: record.run_id,
        failure: record.failure,
    }
}

fn run_view(record: RunRecord) -> RunView {
    RunView {
        id: record.id,
        workspace_id: record.workspace_id,
        session_id: record.session_id,
        job_id: record.job_id,
        plan_id: record.plan_id,
        plan_version_id: record.plan_version_id,
        canonical_plan_digest: digest_hex(&record.canonical_plan_digest),
        plan_fingerprint: digest_hex(&record.plan_fingerprint),
        inputs: record.inputs,
        engine_contract_version: record.engine_contract_version,
        engine_build: record.engine_build,
        state: record.state,
        started_at: record.started_at,
        finished_at: record.finished_at,
        failure: record.failure,
        snapshot_ref: record.snapshot_ref,
        bundle_ref: record.bundle_ref,
    }
}

fn event_view(record: stillflow_storage::EventRecord) -> EventView {
    EventView {
        event_id: record.event_id,
        workspace_id: record.workspace_id,
        session_id: record.session_id,
        stream_kind: record.stream_kind,
        stream_id: record.stream_id,
        sequence: record.sequence,
        event_type: record.event_type,
        event_version: record.event_version,
        occurred_at: record.occurred_at,
        job_id: record.job_id,
        run_id: record.run_id,
        payload: record.payload,
    }
}

fn artifact_view(record: ArtifactRefRecord) -> ArtifactView {
    ArtifactView {
        artifact_id: record.artifact_id,
        workspace_id: record.workspace_id,
        run_id: record.run_id,
        artifact_kind: record.artifact_kind,
        external_ref_kind: record.external_ref_kind.as_str().to_owned(),
        external_ref_id: record.external_ref_id,
        content_digest: digest_hex(&record.content_digest),
        metadata: record.metadata,
        state: record.state,
        created_at: record.created_at,
        committed_at: record.committed_at,
        tombstoned_at: record.tombstoned_at,
    }
}

fn job_page_view(page: JobPage) -> JobPageView {
    JobPageView {
        jobs: page.jobs.into_iter().map(job_view).collect(),
        next: page.next.map(|cursor| JobCursorView {
            queued_at: cursor.queued_at_utc,
            job_id: cursor.job_id,
        }),
    }
}

fn run_page_view(page: RunPage) -> RunPageView {
    RunPageView {
        runs: page.runs.into_iter().map(run_view).collect(),
        next: page.next.map(|cursor| RunCursorView {
            started_at: cursor.started_at_utc,
            run_id: cursor.run_id,
        }),
    }
}

fn event_page_view(page: EventPage) -> EventPageView {
    EventPageView {
        next_sequence: page.next.map(|cursor| cursor.sequence),
        events: page.events.into_iter().map(event_view).collect(),
    }
}

fn artifact_page_view(page: ArtifactPage) -> ArtifactPageView {
    ArtifactPageView {
        artifacts: page.artifacts.into_iter().map(artifact_view).collect(),
        next: page.next.map(|cursor| ArtifactCursorView {
            created_at: cursor.created_at_utc,
            artifact_id: cursor.artifact_id,
        }),
    }
}

fn source_connection_domain(record: &SourceConnectionRecord) -> ApiResult<SourceConnection> {
    serde_json::from_value(serde_json::json!({
        "id": record.id,
        "kind": record.kind,
        "name": record.name,
        "config": record.safe_config,
        "credentialRef": record.credential_ref,
        "createdAt": record.created_at,
        "updatedAt": record.updated_at,
    }))
    .map_err(|_| ApiError::internal())
}

fn source_asset_domain(record: &SourceAssetRecord) -> ApiResult<SourceAsset> {
    let locator: AssetLocator =
        serde_json::from_value(record.safe_locator.clone()).map_err(|_| ApiError::internal())?;
    Ok(SourceAsset {
        id: record.id,
        connection_id: record.connection_id,
        kind: record.kind,
        name: record.name.clone(),
        locator,
        discovered_at: record.discovered_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::BTreeMap;
    use stillflow_plan::{PlanNode, PlanNodeKind};

    fn metadata(workspace_id: Uuid) -> crate::RequestMetadata {
        crate::RequestMetadata::new(Uuid::from_u128(2), workspace_id)
    }

    #[test]
    fn unknown_handshake_version_fails_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let service = ApiService::new(store);
        let request = ApiRequest {
            meta: metadata(Uuid::from_u128(1)),
            body: HandshakeRequest {
                requested_version: ApiVersion::new(2),
            },
        };
        assert_eq!(
            service
                .handshake(request)
                .expect_err("unknown version")
                .code,
            crate::ApiErrorCode::UnsupportedVersion
        );
    }

    #[test]
    fn workspace_scope_is_fail_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        store
            .create_workspace(
                Uuid::from_u128(1),
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )
            .expect("workspace");
        let service = ApiService::new(store);
        let request = ApiRequest {
            meta: metadata(Uuid::from_u128(2)),
            body: ObjectIdRequest {
                object_id: Uuid::from_u128(1),
            },
        };
        assert_eq!(
            service
                .read_session(request)
                .expect_err("missing object")
                .code,
            crate::ApiErrorCode::NotFound
        );
    }

    #[test]
    fn job_submission_replays_by_workspace_key_and_canonical_plan_digest() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let workspace_id = Uuid::from_u128(10);
        let session_id = Uuid::from_u128(11);
        let plan_id = Uuid::from_u128(12);
        let version_id = Uuid::from_u128(13);
        let job_id = Uuid::from_u128(14);
        let event_id = Uuid::from_u128(15);
        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        store.create_workspace(workspace_id, at).expect("workspace");
        store
            .create_session(workspace_id, session_id, at)
            .expect("session");
        let service = ApiService::new(Arc::clone(&store));
        let meta = crate::RequestMetadata {
            idempotency_key: Some("job-key-1".to_owned()),
            ..metadata(workspace_id)
        };
        service
            .create_plan(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(20), workspace_id),
                body: CreatePlanRequest {
                    plan_id,
                    created_at: at,
                },
            })
            .expect("plan");
        let scan = PlanNodeId::from_uuid(Uuid::from_u128(21));
        let mut nodes = BTreeMap::new();
        nodes.insert(
            scan,
            PlanNode::new(
                PlanNodeKind::Scan {
                    source_asset_id: Uuid::from_u128(22),
                    projection: vec![stillflow_core::ColumnId::from_uuid(Uuid::from_u128(23))],
                    predicate: None,
                },
                Vec::new(),
            ),
        );
        let logical_plan = LogicalPlan::new(scan, nodes).expect("plan validates");
        service
            .save_plan_version(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(24), workspace_id),
                body: SavePlanVersionRequest {
                    plan_id,
                    plan_version_id: version_id,
                    version_number: 1,
                    parent_version_id: None,
                    logical_plan,
                    created_at: at,
                },
            })
            .expect("plan version");
        service
            .publish_plan_version(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(25), workspace_id),
                body: PublishPlanVersionRequest {
                    plan_version_id: version_id,
                    expected_current_version_id: None,
                    published_at: at,
                },
            })
            .expect("publish plan version");
        let body = SubmitJobRequest {
            session_id,
            plan_version_id: version_id,
            job_id,
            inputs: Vec::new(),
            execution_policy: serde_json::json!({"mode": "materialize"}),
            output_policy: serde_json::json!({}),
            queued_at: at,
            event_id,
            correlation_id: "corr-1".to_owned(),
            actor_ref: "actor-1".to_owned(),
        };
        let first = service
            .submit_job(ApiRequest {
                meta: meta.clone(),
                body: body.clone(),
            })
            .expect("first submission");
        let replay = service
            .submit_job(ApiRequest { meta, body })
            .expect("replay submission");
        assert_eq!(first.body.id, replay.body.id);
        assert_eq!(first.body.state, JobState::Queued);
        assert_eq!(
            first.body.canonical_plan_digest,
            digest_hex(
                &store
                    .get_plan_version(version_id)
                    .expect("version")
                    .canonical_plan_digest
            )
        );
    }

    #[test]
    fn connection_registration_rejects_embedded_secret_without_mutation() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let workspace_id = Uuid::from_u128(30);
        let connection_id = Uuid::from_u128(31);
        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        store.create_workspace(workspace_id, at).expect("workspace");
        let service = ApiService::new(Arc::clone(&store));
        let error = service
            .register_source_connection(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(32), workspace_id),
                body: RegisterSourceConnectionRequest {
                    connection_id,
                    kind: ConnectorKind::LocalFile,
                    name: "secret-test".to_owned(),
                    safe_config: serde_json::json!({"password": "must-not-persist"}),
                    credential_ref: "cred://vault/test".to_owned(),
                    created_at: at,
                },
            })
            .expect_err("embedded secret");
        assert_eq!(error.code, crate::ApiErrorCode::InvalidRequest);
        assert!(store.get_source_connection(connection_id).is_err());
    }

    #[tokio::test]
    async fn cancel_job_scopes_before_runtime_lookup() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let service = ApiService::new(store);
        let error = service
            .cancel_job(ApiRequest {
                meta: metadata(Uuid::from_u128(40)),
                body: CancelJobRequest {
                    job_id: Uuid::from_u128(41),
                },
            })
            .await
            .expect_err("unknown Job must fail closed before runtime lookup");
        assert_eq!(error.code, crate::ApiErrorCode::NotFound);
    }
}
