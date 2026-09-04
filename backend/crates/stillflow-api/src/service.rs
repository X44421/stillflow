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

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use stillflow_connectors::ConnectorRegistry;
use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, ConnectionStatus, ConnectorKind, ControlPlaneEventType,
    ControlPlaneInput, DatasetState, DiscoverRequest, DriftComparisonRequest, EventStreamKind,
    ExportRequestV1, ExportShape, InspectRequest, JobOperation, JobState, LogLevel, LogicalSchema,
    MetricName, OperationDescriptorV1, OperationKind, PreviewRequest, RequestContext, RunState,
    SamplingStrategy, SessionState, SnapshotRef, SourceAsset, SourceConnection,
    SourceConnectionState, Telemetry, TelemetryComponent, TelemetryLabels, TelemetryOperation,
    TelemetryOutcome, TestConnectionRequest,
};
use stillflow_engine::{ExecutionEngine, JobRuntime, PreviewRequest as EnginePreviewOpRequest};
use stillflow_plan::{LogicalPlan, PlanNodeId};
use stillflow_storage::{
    ArtifactCursor, ArtifactPage, ArtifactRefRecord, ArtifactSectionId, ControlPlaneStore,
    CredentialOwner, CredentialRefDraft, CredentialRefRecord, CredentialState, DatasetRecord,
    EventCursor, EventPage, ExportManifest, ExportManifestFile, ExternalRefKind,
    GarbageCollectionReport, IdentityState, JobCursor, JobPage, JobRecord, JobSubmission,
    MemberRecord, PlanRecord, PlanVersionDraft, PlanVersionRecord, PrincipalKind,
    ProfileHistoryCursor, ProfileHistoryEntry, ProfileHistoryState, RoleRecord, RunCursor, RunPage,
    RunRecord, ServiceAccountRecord, SessionRecord, SnapshotStore, SourceAssetRecord,
    SourceConnectionRecord, SubmitOutcome, TerminalOutputRef, WorkspaceRecord,
};
use tokio::time::Instant;
use uuid::Uuid;

use crate::authorization::AuthorizationGate;
use crate::observability::{
    health_view, liveness_view, metrics_view, readiness_view, EmptyRequest, HealthView,
    MetricsView, ReadinessDependencies,
};
use crate::{
    ApiError, ApiLimits, ApiRequest, ApiResponse, ApiResult, ApiVersion, AuthorizationMode,
    Capability, RequestPrincipal, RequestPrincipalKind, RouteManifest, BOOTSTRAP_MANIFEST,
    SUPPORTED_API_VERSIONS,
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
pub struct MemberView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub subject_ref: String,
    pub state: IdentityState,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub capabilities: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleAssignmentView {
    pub workspace_id: Uuid,
    pub member_id: Uuid,
    pub role_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub state: IdentityState,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrincipalView {
    pub kind: RequestPrincipalKind,
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRefView {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub owner: PrincipalView,
    pub provider_kind: String,
    pub credential_ref: String,
    pub state: CredentialState,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
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
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: String,
    pub operation_kind: Option<OperationKind>,
    pub operation_version: Option<u16>,
    pub operation: Option<JobOperation>,
    pub operation_descriptor_digest: Option<String>,
    pub request_digest: Option<String>,
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub state: JobState,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub run_id: Option<Uuid>,
    pub failure: Option<stillflow_storage::FailureInfo>,
    pub outputs: Vec<TerminalOutputRef>,
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
    pub operation_kind: Option<OperationKind>,
    pub operation_version: Option<u16>,
    pub operation: Option<JobOperation>,
    pub operation_descriptor_digest: Option<String>,
    pub inputs: Vec<ControlPlaneInput>,
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub state: RunState,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure: Option<stillflow_storage::FailureInfo>,
    pub snapshot_ref: Option<Uuid>,
    pub bundle_ref: Option<Uuid>,
    pub outputs: Vec<TerminalOutputRef>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemberRequest {
    pub member_id: Uuid,
    pub subject_ref: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeMemberRequest {
    pub member_id: Uuid,
    pub revoked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoleRequest {
    pub role_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetRoleCapabilitiesRequest {
    pub role_id: Uuid,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignRoleRequest {
    pub member_id: Uuid,
    pub role_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateServiceAccountRequest {
    pub service_account_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeServiceAccountRequest {
    pub service_account_id: Uuid,
    pub revoked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRefDraftRequest {
    pub id: Uuid,
    pub owner: RequestPrincipal,
    pub provider_kind: String,
    pub credential_ref: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterCredentialReferenceRequest {
    pub credential_id: Uuid,
    pub owner: RequestPrincipal,
    pub provider_kind: String,
    pub credential_ref: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BeginCredentialRotationRequest {
    pub credential_id: Uuid,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompleteCredentialRotationRequest {
    pub old_credential_id: Uuid,
    pub replacement: CredentialRefDraftRequest,
    pub rotated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeCredentialRequest {
    pub credential_id: Uuid,
    pub revoked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoverCredentialRequest {
    pub credential_id: Uuid,
    pub recovered_at: DateTime<Utc>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListProfileHistoryRequest {
    pub dataset_id: Uuid,
    #[serde(default)]
    pub state: Option<ProfileHistoryState>,
    #[serde(default)]
    pub columns: Vec<String>,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileHistoryEntryView {
    pub history_id: Uuid,
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub profile_artifact_id: Uuid,
    pub producing_run_id: Uuid,
    pub profile_digest: String,
    pub profile_contract_version: u16,
    pub drift_contract_version: u16,
    pub profile_policy_version: u16,
    pub top_k: usize,
    pub histogram_buckets: usize,
    pub schema_fingerprint: String,
    pub schema: LogicalSchema,
    pub row_count_scanned: u64,
    pub scanned_bytes: u64,
    pub truncated: bool,
    pub profile_sequence: u64,
    pub state: ProfileHistoryState,
    pub created_at: DateTime<Utc>,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileHistoryPageView {
    pub entries: Vec<ProfileHistoryEntryView>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitDriftComparisonRequest {
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    #[serde(default)]
    pub plan_id: Option<Uuid>,
    pub job_id: Uuid,
    pub comparison: DriftComparisonRequest,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub queued_at: DateTime<Utc>,
    pub event_id: Uuid,
    pub correlation_id: String,
    pub actor_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitExportRequest {
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    #[serde(default)]
    pub plan_id: Option<Uuid>,
    pub job_id: Uuid,
    pub snapshot: SnapshotRef,
    pub export_request: ExportRequestV1,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub queued_at: DateTime<Utc>,
    pub event_id: Uuid,
    pub correlation_id: String,
    pub actor_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportInputView {
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub source_asset_id: Uuid,
    pub schema_fingerprint: String,
    pub snapshot_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportDestinationView {
    pub kind: String,
    pub relative_components: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportFileView {
    pub name: String,
    pub byte_count: u64,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportManifestView {
    pub manifest_version: u16,
    pub export_id: Uuid,
    pub run_id: Uuid,
    pub input: ExportInputView,
    pub format: stillflow_core::ExportFormat,
    pub shape: ExportShape,
    pub format_contract_version: u16,
    pub encoder_version: String,
    pub jsonl_float_encoder: String,
    pub text_float_encoder: String,
    pub storage_schema_version: u16,
    pub engine_contract_version: u16,
    pub created_at: DateTime<Utc>,
    pub row_count: u64,
    pub byte_count: u64,
    pub files: Vec<ExportFileView>,
    pub set_digest: String,
    pub destination: ExportDestinationView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExportFilesRequest {
    pub export_id: Uuid,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportFilePageView {
    pub export_id: Uuid,
    pub manifest_digest: String,
    pub files: Vec<ExportFileView>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportDownloadRequest {
    pub export_id: Uuid,
    #[serde(default)]
    pub file_name: Option<String>,
    pub max_bytes: usize,
    #[serde(default)]
    pub handle: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportDownloadPage {
    pub export_id: Uuid,
    pub file_name: String,
    pub offset: u64,
    /// Base64-encoded bytes from one bounded file chunk.
    pub data: String,
    pub byte_count: usize,
    pub total_bytes: u64,
    pub digest: String,
    pub eof: bool,
    pub next: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TombstoneExportRequest {
    pub export_id: Uuid,
    pub tombstoned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportLifecycleView {
    pub export_id: Uuid,
    pub state: String,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectExportGarbageRequest {
    pub now: DateTime<Utc>,
    pub retention_seconds: u64,
    pub max_candidates: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportGarbageCollectionView {
    pub examined: u32,
    pub deleted: u32,
    pub retained: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportView {
    pub artifact: ArtifactView,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFindingsRequest {
    pub artifact_id: Uuid,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub column_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindingPageView {
    pub artifact_id: Uuid,
    pub report_digest: String,
    pub findings: Vec<Value>,
    pub next: Option<String>,
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
    #[serde(default)]
    pub plan_id: Option<Uuid>,
    pub job_id: Uuid,
    #[serde(default)]
    pub operation: Option<JobOperation>,
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
    authorization: AuthorizationGate,
    telemetry: Telemetry,
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
            .field("authorization_mode", &self.authorization.mode())
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
            authorization: AuthorizationGate::new(Arc::clone(&control_plane)),
            control_plane,
            telemetry: Telemetry::noop(),
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

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn telemetry(&self) -> Telemetry {
        self.telemetry.clone()
    }

    pub fn with_authorization_mode(mut self, mode: AuthorizationMode) -> Self {
        self.authorization = self.authorization.clone().with_mode(mode);
        self
    }

    pub fn with_server_authorization(self) -> Self {
        self.with_authorization_mode(AuthorizationMode::Server)
    }

    pub fn authorization_mode(&self) -> AuthorizationMode {
        self.authorization.mode()
    }

    pub fn limits(&self) -> ApiLimits {
        self.limits
    }

    pub fn liveness(
        &self,
        request: ApiRequest<EmptyRequest>,
    ) -> ApiResult<ApiResponse<HealthView>> {
        self.validate_meta_unscoped(&request, false)?;
        self.telemetry.counter(
            MetricName::ApiRequestsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Api)
                .operation(TelemetryOperation::Health)
                .outcome(TelemetryOutcome::Success),
            1,
        );
        Ok(ApiResponse::new(request.meta.request_id, liveness_view()))
    }

    pub fn readiness(
        &self,
        request: ApiRequest<EmptyRequest>,
    ) -> ApiResult<ApiResponse<HealthView>> {
        self.validate_meta_unscoped(&request, false)?;
        let dependencies = ReadinessDependencies {
            control_plane: true,
            connectors: self.connectors.is_some(),
            engine: self.engine.is_some(),
            runtime: self.runtime.is_some(),
            snapshot_store: self.snapshot_store.is_some(),
        };
        Ok(ApiResponse::new(
            request.meta.request_id,
            readiness_view(dependencies),
        ))
    }

    pub fn health(&self, request: ApiRequest<EmptyRequest>) -> ApiResult<ApiResponse<HealthView>> {
        self.validate_meta_unscoped(&request, false)?;
        let dependencies = ReadinessDependencies {
            control_plane: true,
            connectors: self.connectors.is_some(),
            engine: self.engine.is_some(),
            runtime: self.runtime.is_some(),
            snapshot_store: self.snapshot_store.is_some(),
        };
        Ok(ApiResponse::new(
            request.meta.request_id,
            health_view(dependencies),
        ))
    }

    pub fn metrics(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<MetricsView>> {
        self.validate_meta(&request, false)?;
        self.scope_workspace(request.body.object_id, request.meta.workspace_id)?;
        let queue_depth = self.control_plane.queue_depth(request.meta.workspace_id)?;
        self.telemetry.gauge(
            MetricName::QueueDepth,
            TelemetryLabels::new().component(TelemetryComponent::Queue),
            queue_depth,
        );
        Ok(ApiResponse::new(
            request.meta.request_id,
            metrics_view(self.telemetry.snapshot(), queue_depth),
        ))
    }

    pub fn handshake(
        &self,
        request: ApiRequest<HandshakeRequest>,
    ) -> ApiResult<ApiResponse<HandshakeResponse>> {
        self.validate_meta_unscoped(&request, false)?;
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
        self.validate_meta_unscoped(&request, true)?;
        if !self.authorization.is_local_trusted() {
            return Err(ApiError::unauthorized());
        }
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

    pub fn read_workspace(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<WorkspaceView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_workspace(request.body.object_id)?;
        self.ensure_scope(record.id, request.meta.workspace_id)?;
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

    pub fn create_member(
        &self,
        request: ApiRequest<CreateMemberRequest>,
    ) -> ApiResult<ApiResponse<MemberView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let record = self.control_plane.identity().create_member(
            request.meta.workspace_id,
            request.body.member_id,
            &request.body.subject_ref,
            request.body.created_at,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(
            request.meta.request_id,
            member_view(record),
        ))
    }

    pub fn read_member(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<MemberView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .identity()
            .get_member(request.meta.workspace_id, request.body.object_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            member_view(record),
        ))
    }

    pub fn revoke_member(
        &self,
        request: ApiRequest<RevokeMemberRequest>,
    ) -> ApiResult<ApiResponse<MemberView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let record = self.control_plane.identity().revoke_member(
            request.meta.workspace_id,
            request.body.member_id,
            request.body.revoked_at,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(
            request.meta.request_id,
            member_view(record),
        ))
    }

    pub fn create_role(
        &self,
        request: ApiRequest<CreateRoleRequest>,
    ) -> ApiResult<ApiResponse<RoleView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let record = self.control_plane.identity().create_role(
            request.meta.workspace_id,
            request.body.role_id,
            &request.body.name,
            request.body.created_at,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(request.meta.request_id, role_view(record)))
    }

    pub fn read_role(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<RoleView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .identity()
            .get_role(request.meta.workspace_id, request.body.object_id)?;
        Ok(ApiResponse::new(request.meta.request_id, role_view(record)))
    }

    pub fn set_role_capabilities(
        &self,
        request: ApiRequest<SetRoleCapabilitiesRequest>,
    ) -> ApiResult<ApiResponse<RoleView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let capabilities = validated_capabilities(&request.body.capabilities)?;
        let record = self.control_plane.identity().set_role_capabilities(
            request.meta.workspace_id,
            request.body.role_id,
            &capabilities,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(request.meta.request_id, role_view(record)))
    }

    pub fn assign_role(
        &self,
        request: ApiRequest<AssignRoleRequest>,
    ) -> ApiResult<ApiResponse<RoleAssignmentView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        self.control_plane.identity().assign_role(
            request.meta.workspace_id,
            request.body.member_id,
            request.body.role_id,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(
            request.meta.request_id,
            RoleAssignmentView {
                workspace_id: request.meta.workspace_id,
                member_id: request.body.member_id,
                role_id: request.body.role_id,
            },
        ))
    }

    pub fn create_service_account(
        &self,
        request: ApiRequest<CreateServiceAccountRequest>,
    ) -> ApiResult<ApiResponse<ServiceAccountView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let record = self.control_plane.identity().create_service_account(
            request.meta.workspace_id,
            request.body.service_account_id,
            &request.body.name,
            request.body.created_at,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(
            request.meta.request_id,
            service_account_view(record),
        ))
    }

    pub fn read_service_account(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<ServiceAccountView>> {
        self.validate_meta(&request, false)?;
        let record = self
            .control_plane
            .identity()
            .get_service_account(request.meta.workspace_id, request.body.object_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            service_account_view(record),
        ))
    }

    pub fn revoke_service_account(
        &self,
        request: ApiRequest<RevokeServiceAccountRequest>,
    ) -> ApiResult<ApiResponse<ServiceAccountView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::IdentityManage)?;
        let record = self.control_plane.identity().revoke_service_account(
            request.meta.workspace_id,
            request.body.service_account_id,
            request.body.revoked_at,
        )?;
        self.authorization
            .invalidate_workspace(request.meta.workspace_id);
        Ok(ApiResponse::new(
            request.meta.request_id,
            service_account_view(record),
        ))
    }

    pub fn register_credential_reference(
        &self,
        request: ApiRequest<RegisterCredentialReferenceRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let record = self
            .control_plane
            .identity()
            .register_credential_reference(CredentialRefDraft {
                id: request.body.credential_id,
                workspace_id: request.meta.workspace_id,
                owner: credential_owner(request.body.owner),
                provider_kind: request.body.provider_kind.clone(),
                credential_ref: stillflow_core::CredentialRef::new(
                    request.body.credential_ref.clone(),
                )
                .map_err(ApiError::from)?,
                created_at: request.body.created_at,
                expires_at: request.body.expires_at,
            })?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
        ))
    }

    pub fn read_credential_reference(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let record = self
            .control_plane
            .identity()
            .get_credential_reference(request.meta.workspace_id, request.body.object_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
        ))
    }

    pub fn begin_credential_rotation(
        &self,
        request: ApiRequest<BeginCredentialRotationRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let record = self.control_plane.identity().begin_credential_rotation(
            request.meta.workspace_id,
            request.body.credential_id,
            request.body.started_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
        ))
    }

    pub fn complete_credential_rotation(
        &self,
        request: ApiRequest<CompleteCredentialRotationRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let replacement = &request.body.replacement;
        let record = self.control_plane.identity().complete_credential_rotation(
            request.meta.workspace_id,
            request.body.old_credential_id,
            CredentialRefDraft {
                id: replacement.id,
                workspace_id: request.meta.workspace_id,
                owner: credential_owner(replacement.owner),
                provider_kind: replacement.provider_kind.clone(),
                credential_ref: stillflow_core::CredentialRef::new(
                    replacement.credential_ref.clone(),
                )
                .map_err(ApiError::from)?,
                created_at: replacement.created_at,
                expires_at: replacement.expires_at,
            },
            request.body.rotated_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
        ))
    }

    pub fn revoke_credential(
        &self,
        request: ApiRequest<RevokeCredentialRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let record = self.control_plane.identity().revoke_credential(
            request.meta.workspace_id,
            request.body.credential_id,
            request.body.revoked_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
        ))
    }

    pub fn recover_credential(
        &self,
        request: ApiRequest<RecoverCredentialRequest>,
    ) -> ApiResult<ApiResponse<CredentialRefView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::CredentialManage)?;
        let record = self.control_plane.identity().recover_credential(
            request.meta.workspace_id,
            request.body.credential_id,
            request.body.recovered_at,
        )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            credential_ref_view(record),
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
        let _span = self
            .telemetry
            .span("connector.test", &request.meta.request_id.to_string());
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::ConnectorTest)?;
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
        self.telemetry.counter(
            MetricName::ConnectorCallsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Connector)
                .operation(TelemetryOperation::Test)
                .outcome(TelemetryOutcome::Success)
                .connector(connection_record.kind),
            1,
        );
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
        let _span = self
            .telemetry
            .span("job.submit", &request.meta.request_id.to_string());
        self.validate_meta(&request, true)?;
        if request
            .body
            .operation
            .as_ref()
            .is_some_and(|operation| operation.operation_kind == OperationKind::Export)
        {
            self.require_capability(&request, Capability::ExportWrite)?;
        }
        let idempotency_key = request
            .meta
            .idempotency_key
            .clone()
            .ok_or_else(|| ApiError::invalid("job submission requires an idempotency key"))?;
        let version = self
            .control_plane
            .get_plan_version(request.body.plan_version_id)?;
        self.ensure_scope(version.workspace_id, request.meta.workspace_id)?;
        if let Some(plan_id) = request.body.plan_id {
            if plan_id != version.plan_id {
                return Err(ApiError::invalid(
                    "submitted Plan identity does not match the PlanVersion",
                ));
            }
        }
        let submission = match request.body.operation.clone() {
            Some(operation) => JobSubmission::try_new_with_operation_and_plan(
                request.meta.workspace_id,
                request.body.session_id,
                version.plan_id,
                request.body.plan_version_id,
                version.canonical_plan_digest,
                operation,
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
            )?,
            None => JobSubmission::try_new(
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
            )?,
        };
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
        self.telemetry.counter(
            MetricName::JobOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Job)
                .operation(TelemetryOperation::Submit)
                .outcome(TelemetryOutcome::Success),
            1,
        );
        self.telemetry.counter(
            MetricName::EngineOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Engine)
                .operation(TelemetryOperation::Dispatch)
                .outcome(TelemetryOutcome::Success),
            1,
        );
        Ok(ApiResponse::new(request.meta.request_id, job_view(job)))
    }

    /// Submits a ProfileHistory comparison through the existing E5 Job
    /// authority. Drift has no generic E5 input reference; its typed
    /// operation carries the complete Q-D1 comparison identity.
    pub fn submit_drift_comparison(
        &self,
        request: ApiRequest<SubmitDriftComparisonRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, true)?;
        if request.body.comparison.workspace_id != request.meta.workspace_id {
            return Err(ApiError::invalid(
                "Drift comparison is outside the request Workspace",
            ));
        }
        let operation = JobOperation::try_new(
            OperationKind::Drift,
            OperationDescriptorV1::Drift {
                comparison: request.body.comparison,
            },
        )
        .map_err(|_| ApiError::invalid("invalid Drift comparison request"))?;
        self.submit_job(ApiRequest {
            meta: request.meta,
            body: SubmitJobRequest {
                session_id: request.body.session_id,
                plan_version_id: request.body.plan_version_id,
                plan_id: request.body.plan_id,
                job_id: request.body.job_id,
                operation: Some(operation),
                inputs: Vec::new(),
                execution_policy: request.body.execution_policy,
                output_policy: request.body.output_policy,
                queued_at: request.body.queued_at,
                event_id: request.body.event_id,
                correlation_id: request.body.correlation_id,
                actor_ref: request.body.actor_ref,
            },
        })
    }

    /// Submits an Export through the existing typed JobOperation/JobRuntime
    /// path. Export owns no independent queue or state machine at this layer.
    pub fn submit_export(
        &self,
        request: ApiRequest<SubmitExportRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, true)?;
        if request.body.snapshot.workspace_id != request.meta.workspace_id {
            return Err(ApiError::invalid(
                "Export Snapshot is outside the request Workspace",
            ));
        }
        if request.body.snapshot.session_id != request.body.session_id {
            return Err(ApiError::invalid(
                "Export Snapshot is outside the request Session",
            ));
        }
        let operation = JobOperation::try_new(
            OperationKind::Export,
            OperationDescriptorV1::Export {
                snapshot: request.body.snapshot,
                export_request: request.body.export_request,
            },
        )
        .map_err(|_| ApiError::invalid("invalid Export request"))?;
        let operation_input = operation.input();
        self.submit_job(ApiRequest {
            meta: request.meta,
            body: SubmitJobRequest {
                session_id: request.body.session_id,
                plan_version_id: request.body.plan_version_id,
                plan_id: request.body.plan_id,
                job_id: request.body.job_id,
                operation: Some(operation),
                inputs: vec![operation_input],
                execution_policy: request.body.execution_policy,
                output_policy: request.body.output_policy,
                queued_at: request.body.queued_at,
                event_id: request.body.event_id,
                correlation_id: request.body.correlation_id,
                actor_ref: request.body.actor_ref,
            },
        })
    }

    pub fn read_export_job(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, false)?;
        let record = self.control_plane.get_job(request.body.object_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        ensure_export_job(&record)?;
        Ok(ApiResponse::new(request.meta.request_id, job_view(record)))
    }

    pub fn get_export_status(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.read_export_job(request)
    }

    pub async fn cancel_export_job(
        &self,
        request: ApiRequest<CancelJobRequest>,
    ) -> ApiResult<ApiResponse<JobView>> {
        self.validate_meta(&request, true)?;
        let record = self.control_plane.get_job(request.body.job_id)?;
        self.ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        ensure_export_job(&record)?;
        self.cancel_job(request).await
    }

    pub fn read_export_manifest(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<ExportManifestView>> {
        self.validate_meta(&request, false)?;
        let manifest =
            self.scoped_export_manifest(request.body.object_id, request.meta.workspace_id)?;
        let view = export_manifest_view(&manifest)?;
        ensure_response_bound(&view, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn list_export_files(
        &self,
        request: ApiRequest<ListExportFilesRequest>,
    ) -> ApiResult<ApiResponse<ExportFilePageView>> {
        self.validate_meta(&request, false)?;
        let limit = bounded_export_file_limit(request.body.limit)?;
        let manifest =
            self.scoped_export_manifest(request.body.export_id, request.meta.workspace_id)?;
        let digest = manifest.set_digest().to_owned();
        let cursor = request
            .body
            .cursor
            .as_deref()
            .map(decode_export_file_cursor)
            .transpose()?;
        if let Some(cursor) = &cursor {
            if cursor.api_version != request.meta.api_version.value()
                || cursor.workspace_id != request.meta.workspace_id
                || cursor.export_id != manifest.export_id()
                || cursor.manifest_digest != digest
                || cursor.sort_direction != X_A1_EXPORT_FILE_SORT
            {
                return Err(ApiError::invalid(
                    "Export file cursor is outside its scope or manifest",
                ));
            }
        }
        let offset = cursor.map_or(0, |value| value.offset);
        if offset > manifest.files().len() {
            return Err(ApiError::invalid(
                "Export file cursor is outside the manifest",
            ));
        }
        let end = offset.saturating_add(limit).min(manifest.files().len());
        let next = (end < manifest.files().len()).then(|| {
            encode_cursor(&ExportFileCursorWire {
                api_version: request.meta.api_version.value(),
                workspace_id: request.meta.workspace_id,
                export_id: manifest.export_id(),
                manifest_digest: digest.clone(),
                sort_direction: X_A1_EXPORT_FILE_SORT.to_owned(),
                offset: end,
            })
        });
        let response = ExportFilePageView {
            export_id: manifest.export_id(),
            manifest_digest: digest,
            files: manifest.files()[offset..end]
                .iter()
                .map(export_file_view)
                .collect(),
            next: next.transpose()?,
        };
        ensure_response_bound(&response, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, response))
    }

    pub fn download_export(
        &self,
        request: ApiRequest<ExportDownloadRequest>,
    ) -> ApiResult<ApiResponse<ExportDownloadPage>> {
        self.validate_meta(&request, false)?;
        let manifest =
            self.scoped_export_manifest(request.body.export_id, request.meta.workspace_id)?;
        let max_bytes = bounded_export_download_size(request.body.max_bytes, &self.limits)?;
        let (file_name, offset, handle_max_bytes) = match request.body.handle.as_deref() {
            None => (
                request
                    .body
                    .file_name
                    .as_deref()
                    .ok_or_else(|| ApiError::invalid("initial Export download needs a file name"))?
                    .to_owned(),
                0,
                max_bytes,
            ),
            Some(encoded) => {
                let cursor = decode_export_download_handle(encoded)?;
                if cursor.api_version != request.meta.api_version.value()
                    || cursor.workspace_id != request.meta.workspace_id
                    || cursor.export_id != manifest.export_id()
                    || cursor.manifest_digest != manifest.set_digest()
                    || cursor.max_bytes != max_bytes
                    || request.body.file_name.is_some()
                {
                    return Err(ApiError::invalid(
                        "Export download handle is outside its scope or chunk bound",
                    ));
                }
                (cursor.file_name, cursor.offset, cursor.max_bytes)
            }
        };
        let file = manifest
            .files()
            .iter()
            .find(|candidate| candidate.name() == file_name)
            .ok_or_else(ApiError::not_found)?;
        let chunk = self
            .snapshot_store
            .as_ref()
            .ok_or_else(|| ApiError::conflict("snapshot store is not configured"))?
            .read_export_file_chunk(manifest.export_id(), &file_name, offset, handle_max_bytes)
            .map_err(ApiError::from)?;
        let next = (!chunk.eof).then(|| {
            encode_cursor(&ExportDownloadHandleWire {
                api_version: request.meta.api_version.value(),
                workspace_id: request.meta.workspace_id,
                export_id: manifest.export_id(),
                manifest_digest: manifest.set_digest().to_owned(),
                file_name: file_name.clone(),
                offset: chunk.offset.saturating_add(chunk.bytes.len() as u64),
                max_bytes,
            })
        });
        let response = ExportDownloadPage {
            export_id: manifest.export_id(),
            file_name,
            offset: chunk.offset,
            data: BASE64.encode(&chunk.bytes),
            byte_count: chunk.bytes.len(),
            total_bytes: file.byte_count(),
            digest: file.digest().to_owned(),
            eof: chunk.eof,
            next: next.transpose()?,
        };
        ensure_response_bound(&response, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, response))
    }

    pub fn tombstone_export(
        &self,
        request: ApiRequest<TombstoneExportRequest>,
    ) -> ApiResult<ApiResponse<ExportLifecycleView>> {
        self.validate_meta(&request, true)?;
        let manifest =
            self.scoped_export_manifest(request.body.export_id, request.meta.workspace_id)?;
        self.snapshot_store
            .as_ref()
            .ok_or_else(|| ApiError::conflict("snapshot store is not configured"))?
            .tombstone_export(manifest.export_id(), request.body.tombstoned_at)
            .map_err(ApiError::from)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            ExportLifecycleView {
                export_id: manifest.export_id(),
                state: "tombstoned".to_owned(),
                tombstoned_at: Some(request.body.tombstoned_at),
            },
        ))
    }

    pub fn collect_export_garbage(
        &self,
        request: ApiRequest<CollectExportGarbageRequest>,
    ) -> ApiResult<ApiResponse<ExportGarbageCollectionView>> {
        self.validate_meta(&request, true)?;
        self.scope_workspace(request.meta.workspace_id, request.meta.workspace_id)?;
        let report = self
            .snapshot_store
            .as_ref()
            .ok_or_else(|| ApiError::conflict("snapshot store is not configured"))?
            .collect_export_garbage(
                request.meta.workspace_id,
                request.body.now,
                Duration::from_secs(request.body.retention_seconds),
                request.body.max_candidates,
            )?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            export_garbage_collection_view(report),
        ))
    }

    fn scoped_export_manifest(
        &self,
        export_id: Uuid,
        workspace_id: Uuid,
    ) -> ApiResult<ExportManifest> {
        let manifest = self
            .snapshot_store
            .as_ref()
            .ok_or_else(|| ApiError::conflict("snapshot store is not configured"))?
            .load_export_manifest(export_id)
            .map_err(ApiError::from)?;
        let run_id = manifest.run_id().ok_or_else(ApiError::not_found)?;
        let run = self.control_plane.get_run(run_id)?;
        self.ensure_scope(run.workspace_id, workspace_id)?;
        let Some(operation) = run.operation.as_ref() else {
            return Err(ApiError::not_found());
        };
        match &operation.descriptor {
            OperationDescriptorV1::Export {
                snapshot,
                export_request,
            } if operation.operation_kind == OperationKind::Export
                && export_request.export_id == manifest.export_id()
                && snapshot.workspace_id == workspace_id => {}
            _ => return Err(ApiError::not_found()),
        }
        let artifact_id = run.outputs.iter().find_map(|output| match output {
            TerminalOutputRef::Artifact {
                artifact_id,
                artifact_kind: stillflow_core::ArtifactKind::ExportArtifact,
                ..
            } => Some(*artifact_id),
            _ => None,
        });
        let Some(artifact_id) = artifact_id else {
            return Err(ApiError::not_found());
        };
        let artifact = self.control_plane.get_artifact_ref(artifact_id)?;
        let digest = hex_decode(manifest.set_digest())
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(|| ApiError::invalid("Export set digest is invalid"))?;
        if artifact.workspace_id != workspace_id
            || artifact.run_id != run.id
            || artifact.artifact_kind != stillflow_core::ArtifactKind::ExportArtifact
            || artifact.external_ref_kind != ExternalRefKind::Artifact
            || artifact.external_ref_id != manifest.export_id()
            || artifact.content_digest != digest
        {
            return Err(ApiError::not_found());
        }
        Ok(manifest)
    }

    pub fn list_profile_history(
        &self,
        request: ApiRequest<ListProfileHistoryRequest>,
    ) -> ApiResult<ApiResponse<ProfileHistoryPageView>> {
        self.validate_meta(&request, false)?;
        let dataset = self.control_plane.get_dataset(request.body.dataset_id)?;
        self.ensure_scope(dataset.workspace_id, request.meta.workspace_id)?;
        let columns = normalize_history_columns(&request.body.columns)?;
        let cursor = request
            .body
            .cursor
            .as_deref()
            .map(decode_history_cursor)
            .transpose()?;
        if let Some(cursor) = &cursor {
            if cursor.api_version != request.meta.api_version.value()
                || cursor.workspace_id != request.meta.workspace_id
                || cursor.dataset_id != request.body.dataset_id
                || cursor.state != request.body.state
                || cursor.sort_direction != Q_A1_HISTORY_SORT
                || cursor.columns != columns
            {
                return Err(ApiError::invalid(
                    "ProfileHistory cursor is outside its scope or filter",
                ));
            }
        }
        let page = self.control_plane.list_profile_history(
            request.meta.workspace_id,
            request.body.dataset_id,
            request.body.state,
            cursor.as_ref().map(|value| ProfileHistoryCursor {
                workspace_id: value.workspace_id,
                dataset_id: value.dataset_id,
                state: value.state,
                profile_sequence: value.profile_sequence,
                history_id: value.history_id,
            }),
            bounded_history_limit(request.body.limit)?,
        )?;
        for entry in &page.entries {
            ensure_response_bound(&entry.schema, Q_A1_HISTORY_METADATA_BYTES)?;
        }
        let entries = page
            .entries
            .into_iter()
            .filter(|entry| history_matches_columns(entry, &columns))
            .map(profile_history_entry_view)
            .collect::<Vec<_>>();
        let next = page.next.map(|value| {
            encode_cursor(&HistoryCursorWire {
                api_version: request.meta.api_version.value(),
                workspace_id: value.workspace_id,
                dataset_id: value.dataset_id,
                state: value.state,
                sort_direction: Q_A1_HISTORY_SORT.to_owned(),
                columns: columns.clone(),
                profile_sequence: value.profile_sequence,
                history_id: value.history_id,
            })
        });
        let next = next.transpose()?;
        let response = ProfileHistoryPageView { entries, next };
        ensure_response_bound(&response, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, response))
    }

    pub fn read_drift_report(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<ReportView>> {
        self.read_report(
            request,
            stillflow_core::ArtifactKind::DriftReport,
            "drift_report.v1",
        )
    }

    pub fn read_quality_report(
        &self,
        request: ApiRequest<ObjectIdRequest>,
    ) -> ApiResult<ApiResponse<ReportView>> {
        self.read_report(
            request,
            stillflow_core::ArtifactKind::QualityReport,
            "quality_report",
        )
    }

    pub fn list_report_findings(
        &self,
        request: ApiRequest<ListFindingsRequest>,
    ) -> ApiResult<ApiResponse<FindingPageView>> {
        self.validate_meta(&request, false)?;
        let limit = bounded_report_limit(request.body.limit)?;
        let filters = FindingFilterWire::from_request(&request.body)?;
        let artifact = self
            .control_plane
            .get_artifact_ref(request.body.artifact_id)?;
        self.ensure_scope(artifact.workspace_id, request.meta.workspace_id)?;
        if !matches!(
            artifact.artifact_kind,
            stillflow_core::ArtifactKind::DriftReport | stillflow_core::ArtifactKind::QualityReport
        ) {
            return Err(ApiError::not_found());
        }
        let body = self.control_plane.get_artifact_body(artifact.artifact_id)?;
        if body.workspace_id != request.meta.workspace_id
            || body.artifact_kind != artifact.artifact_kind
            || body.artifact_version != 1
            || body.content_digest != artifact.content_digest
        {
            return Err(ApiError::invalid(
                "report Artifact body identity is inconsistent",
            ));
        }
        let value: Value = serde_json::from_slice(&body.body)
            .map_err(|_| ApiError::invalid("report body is not valid canonical JSON"))?;
        let all_findings = value
            .get("findings")
            .and_then(Value::as_array)
            .ok_or_else(|| ApiError::invalid("report body has no findings array"))?;
        if all_findings.len() > stillflow_core::DRIFT_MAX_FINDINGS_PER_REPORT {
            return Err(ApiError::limit("report findings exceed the API bound"));
        }
        let filtered = all_findings
            .iter()
            .filter(|finding| finding_matches(finding, &filters))
            .cloned()
            .collect::<Vec<_>>();
        let cursor = request
            .body
            .cursor
            .as_deref()
            .map(decode_finding_cursor)
            .transpose()?;
        let digest = digest_hex(&body.content_digest);
        if let Some(cursor) = &cursor {
            if cursor.api_version != request.meta.api_version.value()
                || cursor.workspace_id != request.meta.workspace_id
                || cursor.artifact_id != artifact.artifact_id
                || cursor.report_digest != digest
                || cursor.sort_direction != Q_A1_FINDING_SORT
                || cursor.filters != filters
            {
                return Err(ApiError::invalid(
                    "report findings cursor is outside its scope or filter",
                ));
            }
        }
        let offset = cursor.map_or(0, |value| value.offset);
        if offset > filtered.len() {
            return Err(ApiError::invalid(
                "report findings cursor is outside the report",
            ));
        }
        let end = offset.saturating_add(limit).min(filtered.len());
        let next = (end < filtered.len()).then(|| {
            encode_cursor(&FindingCursorWire {
                api_version: request.meta.api_version.value(),
                workspace_id: request.meta.workspace_id,
                artifact_id: artifact.artifact_id,
                report_digest: digest.clone(),
                sort_direction: Q_A1_FINDING_SORT.to_owned(),
                filters: filters.clone(),
                offset: end,
            })
        });
        let response = FindingPageView {
            artifact_id: artifact.artifact_id,
            report_digest: digest,
            findings: filtered[offset..end].to_vec(),
            next: next.transpose()?,
        };
        ensure_response_bound(&response, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, response))
    }

    fn read_report(
        &self,
        request: ApiRequest<ObjectIdRequest>,
        expected_kind: stillflow_core::ArtifactKind,
        expected_type: &str,
    ) -> ApiResult<ApiResponse<ReportView>> {
        self.validate_meta(&request, false)?;
        let artifact = self
            .control_plane
            .get_artifact_ref(request.body.object_id)?;
        self.ensure_scope(artifact.workspace_id, request.meta.workspace_id)?;
        if artifact.artifact_kind != expected_kind {
            return Err(ApiError::not_found());
        }
        let body = self.control_plane.get_artifact_body(artifact.artifact_id)?;
        if body.workspace_id != request.meta.workspace_id
            || body.run_id != artifact.run_id
            || body.artifact_kind != expected_kind
            || body.artifact_version != 1
            || body.content_digest != artifact.content_digest
        {
            return Err(ApiError::invalid(
                "report Artifact body identity is inconsistent",
            ));
        }
        let value: Value = serde_json::from_slice(&body.body)
            .map_err(|_| ApiError::invalid("report body is not valid canonical JSON"))?;
        if value.get("artifact_type").and_then(Value::as_str) != Some(expected_type) {
            return Err(ApiError::invalid("report Artifact type is inconsistent"));
        }
        if value.get("artifact_body_version").and_then(Value::as_u64) != Some(1) {
            return Err(ApiError::invalid(
                "report Artifact body version is inconsistent",
            ));
        }
        let response = ReportView {
            artifact: artifact_view(artifact),
            body: value,
        };
        ensure_response_bound(&response, self.limits.max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, response))
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
        self.telemetry.counter(
            MetricName::JobOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Job)
                .operation(TelemetryOperation::Read)
                .outcome(TelemetryOutcome::Success),
            1,
        );
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
        self.telemetry.counter(
            MetricName::JobOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Job)
                .operation(TelemetryOperation::Read)
                .outcome(TelemetryOutcome::Success),
            1,
        );
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
        self.telemetry.counter(
            MetricName::RunOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Run)
                .operation(TelemetryOperation::Read)
                .outcome(TelemetryOutcome::Success),
            1,
        );
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
        self.telemetry.counter(
            MetricName::RunOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Run)
                .operation(TelemetryOperation::Read)
                .outcome(TelemetryOutcome::Success),
            1,
        );
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
        self.require_capability(&request, Capability::ArtifactDownload)?;
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

    fn validate_meta_unscoped<T>(&self, request: &ApiRequest<T>, mutation: bool) -> ApiResult<()> {
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
        let operation = if mutation {
            TelemetryOperation::Write
        } else {
            TelemetryOperation::Read
        };
        self.telemetry.counter(
            MetricName::ApiRequestsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Api)
                .operation(operation)
                .outcome(TelemetryOutcome::Success),
            1,
        );
        self.telemetry.counter(
            MetricName::StorageOperationsTotal,
            TelemetryLabels::new()
                .component(TelemetryComponent::Storage)
                .operation(operation)
                .outcome(TelemetryOutcome::Success),
            1,
        );
        self.telemetry.log(
            LogLevel::Debug,
            "api.request.accepted",
            &request.meta.request_id.to_string(),
            [("mutation".to_owned(), mutation.to_string())],
        );
        Ok(())
    }

    fn validate_meta<T>(&self, request: &ApiRequest<T>, mutation: bool) -> ApiResult<()> {
        self.validate_meta_unscoped(request, mutation)?;
        let capability = if mutation {
            Capability::WorkspaceWrite
        } else {
            Capability::WorkspaceRead
        };
        self.require_capability(request, capability)
    }

    fn require_capability<T>(
        &self,
        request: &ApiRequest<T>,
        capability: Capability,
    ) -> ApiResult<()> {
        let result = self.authorization.authorize(
            request.meta.workspace_id,
            request.meta.principal,
            capability,
        );
        if result.is_err() {
            self.telemetry.counter(
                MetricName::ApiErrorsTotal,
                TelemetryLabels::new()
                    .component(TelemetryComponent::Api)
                    .operation(TelemetryOperation::Request)
                    .outcome(TelemetryOutcome::Rejected),
                1,
            );
            self.telemetry.log(
                LogLevel::Warn,
                "api.authorization.rejected",
                &request.meta.request_id.to_string(),
                [("capability".to_owned(), capability.as_str().to_owned())],
            );
        }
        result
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryCursorWire {
    api_version: u16,
    workspace_id: Uuid,
    dataset_id: Uuid,
    state: Option<ProfileHistoryState>,
    sort_direction: String,
    columns: Vec<String>,
    profile_sequence: u64,
    history_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FindingCursorWire {
    api_version: u16,
    workspace_id: Uuid,
    artifact_id: Uuid,
    report_digest: String,
    sort_direction: String,
    filters: FindingFilterWire,
    offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FindingFilterWire {
    severity: Option<String>,
    category: Option<String>,
    origin: Option<String>,
    kind: Option<String>,
    column_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportFileCursorWire {
    api_version: u16,
    workspace_id: Uuid,
    export_id: Uuid,
    manifest_digest: String,
    sort_direction: String,
    offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportDownloadHandleWire {
    api_version: u16,
    workspace_id: Uuid,
    export_id: Uuid,
    manifest_digest: String,
    file_name: String,
    offset: u64,
    max_bytes: usize,
}

impl FindingFilterWire {
    fn from_request(request: &ListFindingsRequest) -> ApiResult<Self> {
        Ok(Self {
            severity: normalize_filter(request.severity.as_deref(), "severity")?,
            category: normalize_filter(request.category.as_deref(), "category")?,
            origin: normalize_filter(request.origin.as_deref(), "origin")?,
            kind: normalize_filter(request.kind.as_deref(), "kind")?,
            column_name: normalize_filter(request.column_name.as_deref(), "columnName")?,
        })
    }
}

const Q_A1_HISTORY_PAGE_SIZE: usize = 100;
const Q_A1_REPORT_PAGE_SIZE: usize = 100;
const Q_A1_HISTORY_METADATA_BYTES: usize = 1024 * 1024;
const Q_A1_CURSOR_BYTES: usize = 16 * 1024;
const Q_A1_HISTORY_SORT: &str = "profile_sequence_desc_history_id_desc";
const Q_A1_FINDING_SORT: &str = "report_order_asc";
const X_A1_EXPORT_FILE_SORT: &str = "manifest_file_order_asc";
const X_A1_EXPORT_FILE_PAGE_SIZE: usize = 100;
const X_A1_DOWNLOAD_RESPONSE_OVERHEAD: usize = 2048;

fn bounded_export_file_limit(limit: usize) -> ApiResult<usize> {
    if limit == 0 || limit > X_A1_EXPORT_FILE_PAGE_SIZE {
        Err(ApiError::limit(
            "Export file page size exceeds the X-A1 bound",
        ))
    } else {
        Ok(limit)
    }
}

fn bounded_export_download_size(limit: usize, api_limits: &ApiLimits) -> ApiResult<usize> {
    let response_payload_limit = api_limits
        .max_response_bytes
        .saturating_sub(X_A1_DOWNLOAD_RESPONSE_OVERHEAD)
        .saturating_mul(3)
        / 4;
    if limit == 0 || limit > api_limits.max_artifact_page_bytes || limit > response_payload_limit {
        Err(ApiError::limit(
            "Export download chunk exceeds the API bounded-read limit",
        ))
    } else {
        Ok(limit)
    }
}

fn bounded_history_limit(limit: usize) -> ApiResult<usize> {
    if limit == 0 || limit > Q_A1_HISTORY_PAGE_SIZE {
        Err(ApiError::limit(
            "ProfileHistory page size exceeds the Q-A1 bound",
        ))
    } else {
        Ok(limit)
    }
}

fn bounded_report_limit(limit: usize) -> ApiResult<usize> {
    if limit == 0 || limit > Q_A1_REPORT_PAGE_SIZE {
        Err(ApiError::limit(
            "report finding page size exceeds the Q-A1 bound",
        ))
    } else {
        Ok(limit)
    }
}

fn normalize_history_columns(columns: &[String]) -> ApiResult<Vec<String>> {
    if columns.len() > stillflow_core::DRIFT_MAX_COMPARE_COLUMNS {
        return Err(ApiError::limit(
            "ProfileHistory column filter exceeds the Q-A1 bound",
        ));
    }
    let mut normalized = Vec::with_capacity(columns.len());
    for column in columns {
        let value = normalize_filter(Some(column.as_str()), "columnName")?
            .expect("a present column filter remains present");
        normalized.push(value);
    }
    normalized.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    normalized.dedup();
    if normalized.len() != columns.len() {
        return Err(ApiError::invalid("ProfileHistory columns must be unique"));
    }
    Ok(normalized)
}

fn normalize_filter(value: Option<&str>, name: &str) -> ApiResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() || value.len() > 256 || value.trim() != value {
        return Err(ApiError::invalid(format!("invalid {name} filter")));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::invalid(format!("invalid {name} filter")));
    }
    Ok(Some(value.to_owned()))
}

fn encode_cursor<T: Serialize>(value: &T) -> ApiResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|_| ApiError::internal())?;
    if bytes.len() > Q_A1_CURSOR_BYTES {
        return Err(ApiError::limit("cursor exceeds the API bound"));
    }
    Ok(hex_encode(&bytes))
}

fn decode_cursor<T: DeserializeOwned>(value: &str) -> ApiResult<T> {
    if value.is_empty() || value.len() > Q_A1_CURSOR_BYTES.saturating_mul(2) {
        return Err(ApiError::invalid("cursor is invalid"));
    }
    let bytes = hex_decode(value).ok_or_else(|| ApiError::invalid("cursor is invalid"))?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("cursor is invalid"))
}

fn decode_history_cursor(value: &str) -> ApiResult<HistoryCursorWire> {
    decode_cursor(value)
}

fn decode_finding_cursor(value: &str) -> ApiResult<FindingCursorWire> {
    decode_cursor(value)
}

fn decode_export_file_cursor(value: &str) -> ApiResult<ExportFileCursorWire> {
    decode_cursor(value)
}

fn decode_export_download_handle(value: &str) -> ApiResult<ExportDownloadHandleWire> {
    decode_cursor(value)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn ensure_response_bound<T: Serialize>(value: &T, max_bytes: usize) -> ApiResult<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ApiError::internal())?;
    if bytes.len() > max_bytes {
        Err(ApiError::limit("API response exceeds its bound"))
    } else {
        Ok(())
    }
}

fn history_matches_columns(entry: &ProfileHistoryEntry, columns: &[String]) -> bool {
    columns.iter().all(|column| {
        entry
            .schema
            .fields
            .iter()
            .any(|field| field.name == *column)
    })
}

fn profile_history_entry_view(entry: ProfileHistoryEntry) -> ProfileHistoryEntryView {
    ProfileHistoryEntryView {
        history_id: entry.history_id,
        workspace_id: entry.workspace_id,
        dataset_id: entry.dataset_id,
        profile_artifact_id: entry.profile_artifact_id,
        producing_run_id: entry.producing_run_id,
        profile_digest: digest_hex(&entry.profile_digest),
        profile_contract_version: entry.profile_contract_version,
        drift_contract_version: entry.drift_contract_version,
        profile_policy_version: entry.profile_policy_version,
        top_k: entry.top_k,
        histogram_buckets: entry.histogram_buckets,
        schema_fingerprint: digest_hex(&entry.schema_fingerprint),
        schema: entry.schema,
        row_count_scanned: entry.row_count_scanned,
        scanned_bytes: entry.scanned_bytes,
        truncated: entry.truncated,
        profile_sequence: entry.profile_sequence,
        state: entry.state,
        created_at: entry.created_at,
        tombstoned_at: entry.tombstoned_at,
    }
}

fn ensure_export_job(record: &JobRecord) -> ApiResult<()> {
    if record
        .operation
        .as_ref()
        .is_some_and(|operation| operation.operation_kind == OperationKind::Export)
    {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn export_file_view(file: &ExportManifestFile) -> ExportFileView {
    ExportFileView {
        name: file.name().to_owned(),
        byte_count: file.byte_count(),
        digest: file.digest().to_owned(),
    }
}

fn export_manifest_view(manifest: &ExportManifest) -> ApiResult<ExportManifestView> {
    let run_id = manifest.run_id().ok_or_else(ApiError::not_found)?;
    Ok(ExportManifestView {
        manifest_version: manifest.manifest_version(),
        export_id: manifest.export_id(),
        run_id,
        input: ExportInputView {
            snapshot_id: manifest.input().snapshot_id(),
            dataset_id: manifest.input().dataset_id(),
            session_id: manifest.input().session_id(),
            source_asset_id: manifest.input().source_asset_id(),
            schema_fingerprint: digest_hex(manifest.input().schema_fingerprint().as_bytes()),
            snapshot_version: manifest.input().snapshot_version(),
        },
        format: manifest.format(),
        shape: manifest.shape(),
        format_contract_version: manifest.format_contract_version(),
        encoder_version: manifest.encoder_version().to_owned(),
        jsonl_float_encoder: manifest.jsonl_float_encoder().to_owned(),
        text_float_encoder: manifest.text_float_encoder().to_owned(),
        storage_schema_version: manifest.storage_schema_version(),
        engine_contract_version: manifest.engine_contract_version(),
        created_at: *manifest.created_at(),
        row_count: manifest.row_count(),
        byte_count: manifest.byte_count(),
        files: manifest.files().iter().map(export_file_view).collect(),
        set_digest: manifest.set_digest().to_owned(),
        destination: ExportDestinationView {
            kind: "managedLocal".to_owned(),
            relative_components: manifest.destination_relative().to_vec(),
        },
    })
}

fn export_garbage_collection_view(report: GarbageCollectionReport) -> ExportGarbageCollectionView {
    ExportGarbageCollectionView {
        examined: report.examined(),
        deleted: report.deleted(),
        retained: report.retained(),
    }
}

fn finding_matches(finding: &Value, filters: &FindingFilterWire) -> bool {
    matches_filter(finding, "severity", filters.severity.as_deref())
        && matches_filter(finding, "category", filters.category.as_deref())
        && matches_filter(finding, "origin", filters.origin.as_deref())
        && matches_filter(finding, "kind", filters.kind.as_deref())
        && (matches_filter(finding, "column_name", filters.column_name.as_deref())
            || matches_filter(finding, "columnName", filters.column_name.as_deref()))
}

fn matches_filter(value: &Value, key: &str, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| value.get(key).and_then(Value::as_str) == Some(expected))
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

fn validated_capabilities(values: &[String]) -> ApiResult<Vec<&str>> {
    let mut validated = Vec::with_capacity(values.len());
    for value in values {
        if Capability::parse(value).is_none() {
            return Err(ApiError::invalid("unknown capability"));
        }
        if validated.contains(&value.as_str()) {
            return Err(ApiError::invalid("duplicate capability"));
        }
        validated.push(value.as_str());
    }
    Ok(validated)
}

fn credential_owner(principal: RequestPrincipal) -> CredentialOwner {
    CredentialOwner {
        kind: match principal.kind {
            RequestPrincipalKind::Member => PrincipalKind::Member,
            RequestPrincipalKind::ServiceAccount => PrincipalKind::ServiceAccount,
        },
        id: principal.id,
    }
}

fn member_view(record: MemberRecord) -> MemberView {
    MemberView {
        id: record.id,
        workspace_id: record.workspace_id,
        subject_ref: record.subject_ref,
        state: record.state,
        created_at: record.created_at,
        revoked_at: record.revoked_at,
    }
}

fn role_view(record: RoleRecord) -> RoleView {
    RoleView {
        id: record.id,
        workspace_id: record.workspace_id,
        name: record.name,
        capabilities: record.capabilities,
        created_at: record.created_at,
    }
}

fn service_account_view(record: ServiceAccountRecord) -> ServiceAccountView {
    ServiceAccountView {
        id: record.id,
        workspace_id: record.workspace_id,
        name: record.name,
        state: record.state,
        created_at: record.created_at,
        revoked_at: record.revoked_at,
    }
}

fn credential_ref_view(record: CredentialRefRecord) -> CredentialRefView {
    CredentialRefView {
        id: record.id,
        workspace_id: record.workspace_id,
        owner: PrincipalView {
            kind: match record.owner.kind {
                PrincipalKind::Member => RequestPrincipalKind::Member,
                PrincipalKind::ServiceAccount => RequestPrincipalKind::ServiceAccount,
            },
            id: record.owner.id,
        },
        provider_kind: record.provider_kind,
        credential_ref: record.credential_ref.as_str().to_owned(),
        state: record.state,
        created_at: record.created_at,
        expires_at: record.expires_at,
        revoked_at: record.revoked_at,
    }
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
        plan_id: record.plan_id,
        plan_version_id: record.plan_version_id,
        canonical_plan_digest: digest_hex(&record.canonical_plan_digest),
        operation_kind: record
            .operation
            .as_ref()
            .map(|operation| operation.operation_kind),
        operation_version: record
            .operation
            .as_ref()
            .map(|operation| operation.operation_version),
        operation_descriptor_digest: record.operation.as_ref().and_then(|operation| {
            operation
                .descriptor_digest()
                .ok()
                .map(|digest| digest_hex(&digest))
        }),
        operation: record.operation,
        request_digest: record.request_digest.as_ref().map(digest_hex),
        inputs: record.inputs,
        execution_policy: record.execution_policy,
        output_policy: record.output_policy,
        state: record.state,
        queued_at: record.queued_at,
        started_at: record.started_at,
        finished_at: record.finished_at,
        run_id: record.run_id,
        failure: record.failure,
        outputs: record.outputs,
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
        operation_kind: record
            .operation
            .as_ref()
            .map(|operation| operation.operation_kind),
        operation_version: record
            .operation
            .as_ref()
            .map(|operation| operation.operation_version),
        operation_descriptor_digest: record.operation_descriptor_digest.as_ref().map(digest_hex),
        operation: record.operation,
        inputs: record.inputs,
        engine_contract_version: record.engine_contract_version,
        engine_build: record.engine_build,
        state: record.state,
        started_at: record.started_at,
        finished_at: record.finished_at,
        failure: record.failure,
        snapshot_ref: record.snapshot_ref,
        bundle_ref: record.bundle_ref,
        outputs: record.outputs,
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
            plan_id: Some(plan_id),
            job_id,
            operation: None,
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
    fn q_a1_cursor_is_bound_to_version_scope_and_filters() {
        let workspace_id = Uuid::from_u128(100);
        let dataset_id = Uuid::from_u128(101);
        let history_id = Uuid::from_u128(102);
        let cursor = HistoryCursorWire {
            api_version: 1,
            workspace_id,
            dataset_id,
            state: Some(ProfileHistoryState::Active),
            sort_direction: Q_A1_HISTORY_SORT.to_owned(),
            columns: vec!["amount".to_owned()],
            profile_sequence: 7,
            history_id,
        };
        let encoded = encode_cursor(&cursor).expect("cursor encoding");
        let decoded = decode_history_cursor(&encoded).expect("cursor decoding");
        assert_eq!(decoded, cursor);
        assert!(decode_history_cursor(&encoded[..encoded.len() - 2]).is_err());
        assert_ne!(
            decoded,
            HistoryCursorWire {
                columns: vec!["other".to_owned()],
                ..cursor
            }
        );
    }

    #[test]
    fn q_a1_finding_filters_are_bounded_and_exact() {
        let request = ListFindingsRequest {
            artifact_id: Uuid::from_u128(110),
            limit: 1,
            cursor: None,
            severity: Some("Warning".to_owned()),
            category: Some("Schema".to_owned()),
            origin: Some("Deterministic".to_owned()),
            kind: Some("schema_column_added".to_owned()),
            column_name: Some("amount".to_owned()),
        };
        let filters = FindingFilterWire::from_request(&request).expect("filters");
        let finding = serde_json::json!({
            "severity": "Warning",
            "category": "Schema",
            "origin": "Deterministic",
            "kind": "schema_column_added",
            "column_name": "amount"
        });
        assert!(finding_matches(&finding, &filters));
        assert!(!finding_matches(
            &serde_json::json!({"severity": "Info", "category": "Schema", "origin": "Deterministic", "kind": "schema_column_added", "column_name": "amount"}),
            &filters
        ));
        let mut invalid = request;
        invalid.severity = Some("bad\nvalue".to_owned());
        assert!(FindingFilterWire::from_request(&invalid).is_err());
    }

    #[test]
    fn q_a1_drift_submission_uses_typed_job_without_generic_inputs() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let workspace_id = Uuid::from_u128(120);
        let session_id = Uuid::from_u128(121);
        let plan_id = Uuid::from_u128(122);
        let version_id = Uuid::from_u128(123);
        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        store.create_workspace(workspace_id, at).expect("workspace");
        store
            .create_session(workspace_id, session_id, at)
            .expect("session");
        let service = ApiService::new(Arc::clone(&store));
        service
            .create_plan(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(124), workspace_id),
                body: CreatePlanRequest {
                    plan_id,
                    created_at: at,
                },
            })
            .expect("plan");
        let scan = PlanNodeId::from_uuid(Uuid::from_u128(125));
        let mut nodes = BTreeMap::new();
        nodes.insert(
            scan,
            PlanNode::new(
                PlanNodeKind::Scan {
                    source_asset_id: Uuid::from_u128(126),
                    projection: vec![stillflow_core::ColumnId::from_uuid(Uuid::from_u128(127))],
                    predicate: None,
                },
                Vec::new(),
            ),
        );
        let logical_plan = LogicalPlan::new(scan, nodes).expect("plan validates");
        service
            .save_plan_version(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(127), workspace_id),
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
                meta: crate::RequestMetadata::new(Uuid::from_u128(128), workspace_id),
                body: PublishPlanVersionRequest {
                    plan_version_id: version_id,
                    expected_current_version_id: None,
                    published_at: at,
                },
            })
            .expect("published plan version");
        let job_id = Uuid::from_u128(129);
        let response = service
            .submit_drift_comparison(ApiRequest {
                meta: crate::RequestMetadata {
                    idempotency_key: Some("q-a1-drift-1".to_owned()),
                    ..metadata(workspace_id)
                },
                body: SubmitDriftComparisonRequest {
                    session_id,
                    plan_version_id: version_id,
                    plan_id: Some(plan_id),
                    job_id,
                    comparison: DriftComparisonRequest {
                        workspace_id,
                        dataset_id: Uuid::from_u128(130),
                        candidate_history_id: Uuid::from_u128(131),
                        baseline: stillflow_core::DriftBaselineMode::LatestEligible,
                        threshold_policy_version: stillflow_core::DRIFT_THRESHOLD_POLICY_VERSION,
                        observation_window: None,
                        report_contract_version:
                            stillflow_core::PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
                    },
                    execution_policy: serde_json::json!({"deadlineSeconds": 30}),
                    output_policy: serde_json::json!({}),
                    queued_at: at,
                    event_id: Uuid::from_u128(132),
                    correlation_id: "q-a1-correlation".to_owned(),
                    actor_ref: "actor:q-a1-test".to_owned(),
                },
            })
            .expect("drift submission");
        assert_eq!(response.body.operation_kind, Some(OperationKind::Drift));
        assert!(response.body.inputs.is_empty());
        assert!(matches!(
            response.body.operation,
            Some(JobOperation {
                descriptor: OperationDescriptorV1::Drift { .. },
                ..
            })
        ));
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

    #[test]
    fn server_rbac_is_workspace_scoped_and_cache_invalidates() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let workspace_id = Uuid::from_u128(200);
        let other_workspace_id = Uuid::from_u128(201);
        let member_id = Uuid::from_u128(202);
        let role_id = Uuid::from_u128(203);
        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let bootstrap = ApiService::new(Arc::clone(&store));
        for id in [workspace_id, other_workspace_id] {
            bootstrap
                .create_workspace(ApiRequest {
                    meta: crate::RequestMetadata::new(Uuid::new_v4(), id),
                    body: CreateWorkspaceRequest {
                        workspace_id: id,
                        created_at: at,
                    },
                })
                .expect("workspace");
        }
        bootstrap
            .create_member(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: CreateMemberRequest {
                    member_id,
                    subject_ref: "user:sec-a1".to_owned(),
                    created_at: at,
                },
            })
            .expect("member");
        bootstrap
            .create_role(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: CreateRoleRequest {
                    role_id,
                    name: "operator".to_owned(),
                    created_at: at,
                },
            })
            .expect("role");
        bootstrap
            .set_role_capabilities(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: SetRoleCapabilitiesRequest {
                    role_id,
                    capabilities: vec![
                        "workspace:read".to_owned(),
                        "workspace:write".to_owned(),
                        "identity:manage".to_owned(),
                    ],
                },
            })
            .expect("capabilities");
        bootstrap
            .assign_role(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: AssignRoleRequest { member_id, role_id },
            })
            .expect("assignment");

        let server = ApiService::new(Arc::clone(&store)).with_server_authorization();
        let principal = RequestPrincipal::member(member_id);
        let request_meta = |request_id, workspace_id| {
            crate::RequestMetadata::new(request_id, workspace_id).with_principal(principal)
        };
        server
            .read_workspace(ApiRequest {
                meta: request_meta(Uuid::from_u128(204), workspace_id),
                body: ObjectIdRequest {
                    object_id: workspace_id,
                },
            })
            .expect("authorized workspace read");
        let service_account = server
            .create_service_account(ApiRequest {
                meta: request_meta(Uuid::from_u128(209), workspace_id),
                body: CreateServiceAccountRequest {
                    service_account_id: Uuid::from_u128(210),
                    name: "managed-worker".to_owned(),
                    created_at: at,
                },
            })
            .expect("service account create");
        assert_eq!(service_account.body.state, IdentityState::Active);
        server
            .read_service_account(ApiRequest {
                meta: request_meta(Uuid::from_u128(211), workspace_id),
                body: ObjectIdRequest {
                    object_id: Uuid::from_u128(210),
                },
            })
            .expect("service account read");
        let revoked_service_account = server
            .revoke_service_account(ApiRequest {
                meta: request_meta(Uuid::from_u128(212), workspace_id),
                body: RevokeServiceAccountRequest {
                    service_account_id: Uuid::from_u128(210),
                    revoked_at: at + chrono::Duration::seconds(1),
                },
            })
            .expect("service account revoke");
        assert_eq!(revoked_service_account.body.state, IdentityState::Revoked);
        let cross_workspace = server
            .read_workspace(ApiRequest {
                meta: request_meta(Uuid::from_u128(205), workspace_id),
                body: ObjectIdRequest {
                    object_id: other_workspace_id,
                },
            })
            .expect_err("cross-workspace object must be hidden");
        assert_eq!(cross_workspace.code, crate::ApiErrorCode::NotFound);
        let missing_principal = server
            .read_workspace(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(206), workspace_id),
                body: ObjectIdRequest {
                    object_id: workspace_id,
                },
            })
            .expect_err("server mode requires a principal");
        assert_eq!(missing_principal.code, crate::ApiErrorCode::Unauthorized);

        server
            .set_role_capabilities(ApiRequest {
                meta: request_meta(Uuid::from_u128(207), workspace_id),
                body: SetRoleCapabilitiesRequest {
                    role_id,
                    capabilities: vec!["workspace:write".to_owned(), "identity:manage".to_owned()],
                },
            })
            .expect("role update");
        let after_invalidation = server
            .read_workspace(ApiRequest {
                meta: request_meta(Uuid::from_u128(208), workspace_id),
                body: ObjectIdRequest {
                    object_id: workspace_id,
                },
            })
            .expect_err("role update must invalidate cached read capability");
        assert_eq!(after_invalidation.code, crate::ApiErrorCode::Unauthorized);
    }

    #[test]
    fn credential_permissions_are_separate_and_unknown_capabilities_fail_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
        let workspace_id = Uuid::from_u128(210);
        let member_id = Uuid::from_u128(211);
        let role_id = Uuid::from_u128(212);
        let credential_id = Uuid::from_u128(213);
        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let bootstrap = ApiService::new(Arc::clone(&store));
        bootstrap
            .create_workspace(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: CreateWorkspaceRequest {
                    workspace_id,
                    created_at: at,
                },
            })
            .expect("workspace");
        bootstrap
            .create_member(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: CreateMemberRequest {
                    member_id,
                    subject_ref: "user:credential-test".to_owned(),
                    created_at: at,
                },
            })
            .expect("member");
        bootstrap
            .create_role(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: CreateRoleRequest {
                    role_id,
                    name: "reader".to_owned(),
                    created_at: at,
                },
            })
            .expect("role");
        bootstrap
            .set_role_capabilities(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: SetRoleCapabilitiesRequest {
                    role_id,
                    capabilities: vec![
                        "workspace:read".to_owned(),
                        "workspace:write".to_owned(),
                        "identity:manage".to_owned(),
                    ],
                },
            })
            .expect("capabilities");
        bootstrap
            .assign_role(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
                body: AssignRoleRequest { member_id, role_id },
            })
            .expect("assignment");
        let server = ApiService::new(Arc::clone(&store)).with_server_authorization();
        let principal = RequestPrincipal::member(member_id);
        let request_meta = |request_id| {
            crate::RequestMetadata::new(request_id, workspace_id).with_principal(principal)
        };
        let denied = server
            .register_credential_reference(ApiRequest {
                meta: request_meta(Uuid::from_u128(214)),
                body: RegisterCredentialReferenceRequest {
                    credential_id,
                    owner: principal,
                    provider_kind: "external".to_owned(),
                    credential_ref: "cred://external/reference".to_owned(),
                    created_at: at,
                    expires_at: None,
                },
            })
            .expect_err("credential permission is not implicit");
        assert_eq!(denied.code, crate::ApiErrorCode::Unauthorized);
        let invalid = server
            .set_role_capabilities(ApiRequest {
                meta: request_meta(Uuid::from_u128(215)),
                body: SetRoleCapabilitiesRequest {
                    role_id,
                    capabilities: vec!["future:permission".to_owned()],
                },
            })
            .expect_err("unknown policy state must fail closed");
        assert_eq!(invalid.code, crate::ApiErrorCode::InvalidRequest);
        server
            .set_role_capabilities(ApiRequest {
                meta: request_meta(Uuid::from_u128(216)),
                body: SetRoleCapabilitiesRequest {
                    role_id,
                    capabilities: vec![
                        "workspace:read".to_owned(),
                        "workspace:write".to_owned(),
                        "credential:manage".to_owned(),
                    ],
                },
            })
            .expect("credential capability");
        let credential = server
            .register_credential_reference(ApiRequest {
                meta: request_meta(Uuid::from_u128(217)),
                body: RegisterCredentialReferenceRequest {
                    credential_id,
                    owner: principal,
                    provider_kind: "external".to_owned(),
                    credential_ref: "cred://external/reference".to_owned(),
                    created_at: at,
                    expires_at: None,
                },
            })
            .expect("opaque credential ref");
        assert_eq!(credential.body.credential_ref, "cred://external/reference");
    }
}
