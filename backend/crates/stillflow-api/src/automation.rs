//! AUT-A1 transport-neutral Automation API.
//!
//! The API projects the durable AUT-J1 schedule state, uses CAS transitions,
//! and hands manual triggers to the existing E5 Job submission service.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use stillflow_core::{AutomationSchedule, ControlPlaneInput, JobOperation};
use stillflow_storage::{
    AuditActor, AuditActorKind, AuditEventDraft, AuditObjectRef, AutomationExecutionCreateOutcome,
    AutomationExecutionCursor, AutomationExecutionDraft, AutomationExecutionRecord,
    AutomationExecutionState, AutomationScheduleCursor, AutomationScheduleDraft,
    AutomationScheduleRecord, AutomationScheduleState, MAX_AUTOMATION_HISTORY_PAGE_SIZE,
    MAX_AUTOMATION_NAME_BYTES, MAX_AUTOMATION_TEMPLATE_BYTES,
};
use uuid::Uuid;

use crate::{ApiError, ApiRequest, ApiResponse, ApiResult, ApiService, Capability};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationRunTemplate {
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    #[serde(default)]
    pub plan_id: Option<Uuid>,
    #[serde(default)]
    pub operation: Option<JobOperation>,
    #[serde(default)]
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAutomationRequest {
    pub automation_id: Uuid,
    pub name: String,
    pub schedule: AutomationSchedule,
    pub timezone: String,
    pub run_template: AutomationRunTemplate,
    pub first_run_at: DateTime<Utc>,
    pub max_submission_attempts: u8,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAutomationRequest {
    pub automation_id: Uuid,
    pub expected_revision: u64,
    pub name: String,
    pub schedule: AutomationSchedule,
    pub timezone: String,
    pub run_template: AutomationRunTemplate,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationIdRequest {
    pub automation_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationTransitionRequest {
    pub automation_id: Uuid,
    pub expected_revision: u64,
    pub changed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAutomationsRequest {
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationPageView {
    pub automations: Vec<AutomationView>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationView {
    pub automation_id: Uuid,
    pub automation_version: u16,
    pub workspace_id: Uuid,
    pub name: String,
    pub state: String,
    pub trigger_kind: String,
    pub schedule: AutomationSchedule,
    pub schedule_version: u16,
    pub timezone: String,
    pub run_template: AutomationRunTemplate,
    pub first_run_at: DateTime<Utc>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_submitted_at: Option<DateTime<Utc>>,
    pub last_occurrence_key: Option<String>,
    pub max_submission_attempts: u8,
    pub last_submission_attempt: u8,
    pub revision: u64,
    pub last_failure: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationNextRunView {
    pub automation_id: Uuid,
    pub revision: u64,
    pub next_run_at: Option<DateTime<Utc>>,
    pub next_occurrence_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAutomationHistoryRequest {
    pub automation_id: Uuid,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationExecutionView {
    pub execution_id: Uuid,
    pub automation_id: Uuid,
    pub workspace_id: Uuid,
    pub trigger_kind: String,
    pub occurrence_key: String,
    pub idempotency_key: String,
    pub job_id: Uuid,
    pub state: String,
    pub failure: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationHistoryPageView {
    pub executions: Vec<AutomationExecutionView>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerAutomationRequest {
    pub automation_id: Uuid,
    pub triggered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutomationScheduleCursorWire {
    api_version: u16,
    workspace_id: Uuid,
    created_at: DateTime<Utc>,
    automation_id: Uuid,
    sort: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutomationHistoryCursorWire {
    api_version: u16,
    workspace_id: Uuid,
    automation_id: Uuid,
    created_at: DateTime<Utc>,
    execution_id: Uuid,
    sort: String,
}

impl ApiService {
    pub fn create_automation(
        &self,
        request: ApiRequest<CreateAutomationRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::AutomationCreate)?;
        validate_automation_id(request.body.automation_id)?;
        validate_name(&request.body.name)?;
        validate_run_template(&request.body.run_template)?;
        let template = stored_template(&request.body.name, &request.body.run_template)?;
        let record = self
            .control_plane
            .create_automation_schedule(AutomationScheduleDraft {
                id: request.body.automation_id,
                workspace_id: request.meta.workspace_id,
                schedule: request.body.schedule,
                timezone: request.body.timezone.clone(),
                template,
                first_run_at: request.body.first_run_at,
                max_submission_attempts: request.body.max_submission_attempts,
                created_at: request.body.created_at,
            })?;
        let view = automation_view(record.clone())?;
        append_automation_audit(self, &request, "automation.create", &record, false)?;
        ensure_response_bound(&view, self.limits().max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn list_automations(
        &self,
        request: ApiRequest<ListAutomationsRequest>,
    ) -> ApiResult<ApiResponse<AutomationPageView>> {
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::AutomationRead)?;
        let limit = bounded_automation_limit(request.body.limit)?;
        let cursor = request
            .body
            .cursor
            .as_deref()
            .map(|value| {
                decode_schedule_cursor(
                    value,
                    request.meta.api_version.value(),
                    request.meta.workspace_id,
                )
            })
            .transpose()?;
        let cursor = cursor.map(|value| AutomationScheduleCursor {
            created_at: value.created_at,
            execution_id: value.automation_id,
        });
        let records = self.control_plane.list_automation_schedules_page(
            request.meta.workspace_id,
            cursor,
            limit,
        )?;
        let has_more = records.len() == limit;
        let next = if has_more {
            records
                .last()
                .map(|record| {
                    encode_cursor(&AutomationScheduleCursorWire {
                        api_version: request.meta.api_version.value(),
                        workspace_id: request.meta.workspace_id,
                        created_at: record.created_at,
                        automation_id: record.id,
                        sort: "created_at_asc_automation_id_asc".to_owned(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        let automations = records
            .into_iter()
            .map(automation_view)
            .collect::<ApiResult<Vec<_>>>()?;
        let view = AutomationPageView { automations, next };
        ensure_response_bound(&view, self.limits().max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn read_automation(
        &self,
        request: ApiRequest<AutomationIdRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::AutomationRead)?;
        let record = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        let view = automation_view(record)?;
        ensure_response_bound(&view, self.limits().max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn update_automation(
        &self,
        request: ApiRequest<UpdateAutomationRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::AutomationUpdate)?;
        validate_name(&request.body.name)?;
        validate_run_template(&request.body.run_template)?;
        let current = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let template = stored_template(&request.body.name, &request.body.run_template)?;
        let record = self.control_plane.update_automation_schedule(
            request.body.automation_id,
            request.body.expected_revision,
            request.body.schedule,
            &request.body.timezone,
            template,
            request.body.updated_at,
        )?;
        let view = automation_view(record.clone())?;
        append_automation_audit(self, &request, "automation.update", &record, true)?;
        ensure_response_bound(&view, self.limits().max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn pause_automation(
        &self,
        request: ApiRequest<AutomationTransitionRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.transition_automation(
            request,
            AutomationScheduleState::Paused,
            Capability::AutomationPause,
            "automation.pause",
        )
    }

    pub fn resume_automation(
        &self,
        request: ApiRequest<AutomationTransitionRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.transition_automation(
            request,
            AutomationScheduleState::Active,
            Capability::AutomationResume,
            "automation.resume",
        )
    }

    pub fn delete_automation(
        &self,
        request: ApiRequest<AutomationTransitionRequest>,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::AutomationDelete)?;
        let current = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let record = self.control_plane.delete_automation_schedule(
            request.body.automation_id,
            request.body.expected_revision,
            request.body.changed_at,
        )?;
        let view = automation_view(record.clone())?;
        append_automation_audit(self, &request, "automation.delete", &record, true)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn next_automation_run(
        &self,
        request: ApiRequest<AutomationIdRequest>,
    ) -> ApiResult<ApiResponse<AutomationNextRunView>> {
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::AutomationRead)?;
        let record = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(record.workspace_id, request.meta.workspace_id)?;
        Ok(ApiResponse::new(
            request.meta.request_id,
            AutomationNextRunView {
                automation_id: record.id,
                revision: record.revision,
                next_run_at: record.next_run_at,
                next_occurrence_key: record.next_run_at.map(|value| value.to_rfc3339()),
            },
        ))
    }

    pub fn list_automation_history(
        &self,
        request: ApiRequest<ListAutomationHistoryRequest>,
    ) -> ApiResult<ApiResponse<AutomationHistoryPageView>> {
        self.validate_meta(&request, false)?;
        self.require_capability(&request, Capability::AutomationRead)?;
        let limit = bounded_automation_limit(request.body.limit)?;
        let automation = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(automation.workspace_id, request.meta.workspace_id)?;
        let cursor = request
            .body
            .cursor
            .as_deref()
            .map(|value| {
                decode_history_cursor(
                    value,
                    request.meta.api_version.value(),
                    request.meta.workspace_id,
                    request.body.automation_id,
                )
            })
            .transpose()?;
        let records = self.control_plane.list_automation_executions(
            request.meta.workspace_id,
            request.body.automation_id,
            cursor,
            limit,
        )?;
        let has_more = records.len() == limit;
        let next = if has_more {
            records
                .last()
                .map(|record| {
                    encode_cursor(&AutomationHistoryCursorWire {
                        api_version: request.meta.api_version.value(),
                        workspace_id: request.meta.workspace_id,
                        automation_id: request.body.automation_id,
                        created_at: record.created_at,
                        execution_id: record.execution_id,
                        sort: "created_at_desc_execution_id_desc".to_owned(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        let view = AutomationHistoryPageView {
            executions: records.into_iter().map(execution_view).collect(),
            next,
        };
        ensure_response_bound(&view, self.limits().max_response_bytes)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }

    pub fn trigger_automation(
        &self,
        request: ApiRequest<TriggerAutomationRequest>,
    ) -> ApiResult<ApiResponse<super::JobView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, Capability::AutomationTrigger)?;
        let idempotency_key = request.meta.idempotency_key.as_deref().ok_or_else(|| {
            ApiError::invalid("manual automation trigger requires an idempotency key")
        })?;
        let schedule = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(schedule.workspace_id, request.meta.workspace_id)?;
        if schedule.state != AutomationScheduleState::Active {
            return Err(ApiError::conflict("automation is not active"));
        }
        let run_template = automation_run_template(&schedule)?;
        validate_run_template(&run_template)?;
        let request_digest = template_digest(&schedule.id, idempotency_key, &run_template)?;
        let execution_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let occurrence_key = format!("manual:{}", digest_hex(&request_digest));
        let execution_key = format!(
            "automation:{}:manual:{}",
            schedule.id,
            digest_hex(&request_digest)
        );
        let execution =
            self.control_plane
                .create_automation_execution(AutomationExecutionDraft {
                    execution_id,
                    workspace_id: request.meta.workspace_id,
                    schedule_id: schedule.id,
                    trigger_kind: "manual".to_owned(),
                    occurrence_key,
                    idempotency_key: idempotency_key.to_owned(),
                    request_digest,
                    job_id,
                    created_at: request.body.triggered_at,
                })?;
        let execution = match execution {
            AutomationExecutionCreateOutcome::Created(record)
            | AutomationExecutionCreateOutcome::Replayed(record) => record,
        };
        if execution.state == AutomationExecutionState::Submitted {
            let job = self.control_plane.get_job(execution.job_id)?;
            ensure_scope(job.workspace_id, request.meta.workspace_id)?;
            return Ok(ApiResponse::new(
                request.meta.request_id,
                super::job_view(job),
            ));
        }
        let mut submit_meta = request.meta.clone();
        submit_meta.idempotency_key = Some(execution_key);
        let job = self
            .submit_job(ApiRequest {
                meta: submit_meta,
                body: super::SubmitJobRequest {
                    session_id: run_template.session_id,
                    plan_version_id: run_template.plan_version_id,
                    plan_id: run_template.plan_id,
                    job_id: execution.job_id,
                    operation: run_template.operation,
                    inputs: run_template.inputs,
                    execution_policy: run_template.execution_policy,
                    output_policy: run_template.output_policy,
                    queued_at: request.body.triggered_at,
                    event_id: Uuid::new_v4(),
                    correlation_id: format!("automation:{}", schedule.id),
                    actor_ref: audit_actor(&request.meta).actor_ref,
                },
            })?
            .body;
        let _updated = self.control_plane.mark_automation_execution_submitted(
            execution.execution_id,
            job.id,
            request.body.triggered_at,
        )?;
        append_automation_audit(
            self,
            &request,
            "automation.trigger.submitted",
            &schedule,
            true,
        )?;
        Ok(ApiResponse::new(request.meta.request_id, job))
    }

    fn transition_automation(
        &self,
        request: ApiRequest<AutomationTransitionRequest>,
        target: AutomationScheduleState,
        capability: Capability,
        action: &str,
    ) -> ApiResult<ApiResponse<AutomationView>> {
        self.validate_meta(&request, true)?;
        self.require_capability(&request, capability)?;
        let current = self
            .control_plane
            .get_automation_schedule(request.body.automation_id)?;
        ensure_scope(current.workspace_id, request.meta.workspace_id)?;
        let record = self.control_plane.set_automation_schedule_state(
            request.body.automation_id,
            request.body.expected_revision,
            target,
            request.body.changed_at,
        )?;
        let view = automation_view(record.clone())?;
        append_automation_audit(self, &request, action, &record, true)?;
        Ok(ApiResponse::new(request.meta.request_id, view))
    }
}

fn automation_view(record: AutomationScheduleRecord) -> ApiResult<AutomationView> {
    let object = record
        .template
        .as_object()
        .ok_or_else(|| ApiError::invalid("automation template is invalid"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("automation name is missing"))?
        .to_owned();
    let run_template = object
        .get("runTemplate")
        .cloned()
        .ok_or_else(|| ApiError::invalid("automation RunTemplate is missing"))?;
    let run_template = serde_json::from_value(run_template)
        .map_err(|_| ApiError::invalid("automation RunTemplate is invalid"))?;
    Ok(AutomationView {
        automation_id: record.id,
        automation_version: 1,
        workspace_id: record.workspace_id,
        name,
        state: schedule_state_text(record.state).to_owned(),
        trigger_kind: "schedule".to_owned(),
        schedule: record.schedule,
        schedule_version: 1,
        timezone: record.timezone,
        run_template,
        first_run_at: record.first_run_at,
        next_run_at: record.next_run_at,
        last_submitted_at: record.last_submitted_at,
        last_occurrence_key: record.last_occurrence_key,
        max_submission_attempts: record.max_submission_attempts,
        last_submission_attempt: record.last_submission_attempt,
        revision: record.revision,
        last_failure: record.last_failure,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn automation_run_template(record: &AutomationScheduleRecord) -> ApiResult<AutomationRunTemplate> {
    let object = record
        .template
        .as_object()
        .ok_or_else(|| ApiError::invalid("automation template is invalid"))?;
    serde_json::from_value(
        object
            .get("runTemplate")
            .cloned()
            .ok_or_else(|| ApiError::invalid("automation RunTemplate is missing"))?,
    )
    .map_err(|_| ApiError::invalid("automation RunTemplate is invalid"))
}

fn stored_template(name: &str, template: &AutomationRunTemplate) -> ApiResult<Value> {
    let value = json!({"name": name, "runTemplate": template});
    let bytes = serde_json::to_vec(&value).map_err(|_| ApiError::internal())?;
    if bytes.len() > MAX_AUTOMATION_TEMPLATE_BYTES {
        return Err(ApiError::limit(
            "automation template exceeds its byte bound",
        ));
    }
    Ok(value)
}

fn validate_run_template(template: &AutomationRunTemplate) -> ApiResult<()> {
    if template.session_id.is_nil() || template.plan_version_id.is_nil() {
        return Err(ApiError::invalid(
            "Automation RunTemplate identities are required",
        ));
    }
    if template.plan_id.is_some_and(|id| id.is_nil()) {
        return Err(ApiError::invalid("Automation Plan identity is invalid"));
    }
    let value = serde_json::to_value(template).map_err(|_| ApiError::internal())?;
    let bytes = serde_json::to_vec(&value).map_err(|_| ApiError::internal())?;
    if bytes.len() > MAX_AUTOMATION_TEMPLATE_BYTES {
        return Err(ApiError::limit(
            "automation RunTemplate exceeds its byte bound",
        ));
    }
    let lower = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
    if ["password=", "token=", "api_key=", "secret=", "bearer "]
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return Err(ApiError::invalid(
            "Automation RunTemplate contains forbidden secret material",
        ));
    }
    Ok(())
}

fn validate_automation_id(id: Uuid) -> ApiResult<()> {
    if id.is_nil() {
        Err(ApiError::invalid("automation identity is required"))
    } else {
        Ok(())
    }
}

fn validate_name(name: &str) -> ApiResult<()> {
    if name.is_empty()
        || name.len() > MAX_AUTOMATION_NAME_BYTES
        || name.trim() != name
        || name.chars().any(char::is_control)
        || ["password=", "token=", "api_key=", "secret=", "bearer "]
            .iter()
            .any(|marker| name.to_ascii_lowercase().contains(marker))
    {
        return Err(ApiError::invalid("automation name is invalid"));
    }
    Ok(())
}

fn bounded_automation_limit(limit: usize) -> ApiResult<usize> {
    if limit == 0 || limit > MAX_AUTOMATION_HISTORY_PAGE_SIZE {
        Err(ApiError::limit("automation page size exceeds its bound"))
    } else {
        Ok(limit)
    }
}

fn ensure_scope(object_workspace: Uuid, request_workspace: Uuid) -> ApiResult<()> {
    if object_workspace == request_workspace {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn schedule_state_text(state: AutomationScheduleState) -> &'static str {
    match state {
        AutomationScheduleState::Active => "active",
        AutomationScheduleState::Paused => "paused",
        AutomationScheduleState::Failed => "failed",
        AutomationScheduleState::Deleted => "deleted",
    }
}

fn execution_view(record: AutomationExecutionRecord) -> AutomationExecutionView {
    AutomationExecutionView {
        execution_id: record.execution_id,
        automation_id: record.schedule_id,
        workspace_id: record.workspace_id,
        trigger_kind: record.trigger_kind,
        occurrence_key: record.occurrence_key,
        idempotency_key: record.idempotency_key,
        job_id: record.job_id,
        state: match record.state {
            AutomationExecutionState::Accepted => "accepted",
            AutomationExecutionState::Submitted => "submitted",
            AutomationExecutionState::Failed => "failed",
            AutomationExecutionState::Skipped => "skipped",
        }
        .to_owned(),
        failure: record.failure,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn template_digest(
    automation_id: &Uuid,
    idempotency_key: &str,
    template: &AutomationRunTemplate,
) -> ApiResult<[u8; 32]> {
    let bytes = serde_json::to_vec(&json!({
        "automationId": automation_id,
        "idempotencyKey": idempotency_key,
        "runTemplate": template,
    }))
    .map_err(|_| ApiError::internal())?;
    Ok(Sha256::digest(bytes).into())
}

fn digest_hex(value: &[u8; 32]) -> String {
    let mut result = String::with_capacity(64);
    for byte in value {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

fn audit_actor(meta: &crate::RequestMetadata) -> AuditActor {
    let (kind, actor_ref) = match meta.principal {
        Some(principal) => match principal.kind {
            crate::RequestPrincipalKind::Member => {
                (AuditActorKind::User, format!("member:{}", principal.id))
            }
            crate::RequestPrincipalKind::ServiceAccount => (
                AuditActorKind::ServiceAccount,
                format!("service-account:{}", principal.id),
            ),
        },
        None => (AuditActorKind::System, "local-trusted".to_owned()),
    };
    AuditActor { kind, actor_ref }
}

fn append_automation_audit<T>(
    service: &ApiService,
    request: &ApiRequest<T>,
    action: &str,
    record: &AutomationScheduleRecord,
    before: bool,
) -> ApiResult<()> {
    let before_value = before.then(|| json!({"revision": record.revision.saturating_sub(1)}));
    service.control_plane.audit().append(AuditEventDraft {
        event_id: Uuid::new_v4(),
        audit_version: 1,
        workspace_id: request.meta.workspace_id,
        occurred_at: record.updated_at,
        actor: audit_actor(&request.meta),
        action: action.to_owned(),
        reason_code: "AUT-A1-API".to_owned(),
        request_id: request.meta.request_id.to_string(),
        correlation_id: Some(format!("automation:{}", record.id)),
        trace_id: None,
        object: AuditObjectRef {
            kind: "automation".to_owned(),
            id: record.id,
        },
        before: before_value,
        after: Some(json!({
            "automationId": record.id,
            "state": schedule_state_text(record.state),
            "revision": record.revision,
            "nextRunAt": record.next_run_at,
        })),
        lineage: Vec::new(),
        source_event_id: None,
        payload: json!({"action": action, "automationId": record.id}),
        idempotency_key: Some(format!("{}:{}", action, request.meta.request_id)),
    })?;
    Ok(())
}

fn encode_cursor<T: Serialize>(value: &T) -> ApiResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|_| ApiError::internal())?;
    if bytes.len() > 2 * 1024 {
        return Err(ApiError::limit("automation cursor exceeds its bound"));
    }
    Ok(hex_encode(&bytes))
}

fn decode_schedule_cursor(
    value: &str,
    api_version: u16,
    workspace_id: Uuid,
) -> ApiResult<AutomationScheduleCursorWire> {
    let cursor: AutomationScheduleCursorWire = decode_cursor(value)?;
    if cursor.api_version != api_version
        || cursor.workspace_id != workspace_id
        || cursor.automation_id.is_nil()
        || cursor.sort != "created_at_asc_automation_id_asc"
    {
        return Err(ApiError::invalid("automation cursor is invalid"));
    }
    Ok(cursor)
}

fn decode_history_cursor(
    value: &str,
    api_version: u16,
    workspace_id: Uuid,
    automation_id: Uuid,
) -> ApiResult<AutomationExecutionCursor> {
    let cursor: AutomationHistoryCursorWire = decode_cursor(value)?;
    if cursor.api_version != api_version
        || cursor.workspace_id != workspace_id
        || cursor.automation_id != automation_id
        || cursor.execution_id.is_nil()
        || cursor.sort != "created_at_desc_execution_id_desc"
    {
        return Err(ApiError::invalid("automation history cursor is invalid"));
    }
    Ok(AutomationExecutionCursor {
        created_at: cursor.created_at,
        execution_id: cursor.execution_id,
    })
}

fn decode_cursor<T: for<'de> Deserialize<'de>>(value: &str) -> ApiResult<T> {
    if value.is_empty() || value.len() > 4096 {
        return Err(ApiError::invalid("automation cursor is invalid"));
    }
    let bytes =
        hex_decode(value).ok_or_else(|| ApiError::invalid("automation cursor is invalid"))?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("automation cursor is invalid"))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?))
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn ensure_response_bound<T: Serialize>(value: &T, max_bytes: usize) -> ApiResult<()> {
    if serde_json::to_vec(value)
        .map_err(|_| ApiError::internal())?
        .len()
        > max_bytes
    {
        Err(ApiError::limit("API response exceeds its bound"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind};
    use stillflow_storage::ControlPlaneStore;
    use tempfile::TempDir;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn request<T>(workspace_id: Uuid, body: T) -> ApiRequest<T> {
        ApiRequest {
            meta: crate::RequestMetadata::new(Uuid::new_v4(), workspace_id),
            body,
        }
    }

    fn template() -> AutomationRunTemplate {
        AutomationRunTemplate {
            session_id: Uuid::from_u128(2),
            plan_version_id: Uuid::from_u128(3),
            plan_id: None,
            operation: None,
            inputs: Vec::new(),
            execution_policy: json!({}),
            output_policy: json!({}),
        }
    }

    fn service(temp: &TempDir, workspace_id: Uuid) -> ApiService {
        let control_plane = Arc::new(ControlPlaneStore::open(temp.path()).expect("open storage"));
        control_plane
            .create_workspace(workspace_id, at(1))
            .expect("create workspace");
        ApiService::new(control_plane)
    }

    #[test]
    fn automation_crud_cas_next_run_history_and_workspace_scope_are_bounded() {
        let temp = tempfile::tempdir().expect("temporary storage root");
        let workspace_id = Uuid::from_u128(1);
        let automation_id = Uuid::from_u128(4);
        let service = service(&temp, workspace_id);
        let created = service
            .create_automation(request(
                workspace_id,
                CreateAutomationRequest {
                    automation_id,
                    name: "nightly".to_owned(),
                    schedule: AutomationSchedule::Interval { period_seconds: 60 },
                    timezone: "UTC".to_owned(),
                    run_template: template(),
                    first_run_at: at(100),
                    max_submission_attempts: 3,
                    created_at: at(1),
                },
            ))
            .expect("create automation")
            .body;
        assert_eq!(created.name, "nightly");
        assert_eq!(created.revision, 1);
        let next_before = service
            .next_automation_run(request(workspace_id, AutomationIdRequest { automation_id }))
            .expect("read next run")
            .body;
        assert_eq!(next_before.revision, 1);
        assert_eq!(next_before.next_run_at, Some(at(100)));

        let paused = service
            .pause_automation(request(
                workspace_id,
                AutomationTransitionRequest {
                    automation_id,
                    expected_revision: 1,
                    changed_at: at(2),
                },
            ))
            .expect("pause automation")
            .body;
        assert_eq!(paused.state, "paused");
        assert_eq!(paused.revision, 2);
        assert!(matches!(
            service.resume_automation(request(
                workspace_id,
                AutomationTransitionRequest {
                    automation_id,
                    expected_revision: 1,
                    changed_at: at(3),
                },
            )),
            Err(ApiError {
                code: crate::ApiErrorCode::LimitExceeded,
                ..
            })
        ));
        let resumed = service
            .resume_automation(request(
                workspace_id,
                AutomationTransitionRequest {
                    automation_id,
                    expected_revision: 2,
                    changed_at: at(3),
                },
            ))
            .expect("resume automation")
            .body;
        assert_eq!(resumed.state, "active");
        assert_eq!(resumed.revision, 3);

        let history = service
            .list_automation_history(request(
                workspace_id,
                ListAutomationHistoryRequest {
                    automation_id,
                    limit: 10,
                    cursor: None,
                },
            ))
            .expect("list history")
            .body;
        assert!(history.executions.is_empty());
        assert!(matches!(
            service.read_automation(request(
                Uuid::from_u128(99),
                AutomationIdRequest { automation_id },
            )),
            Err(ApiError {
                code: crate::ApiErrorCode::NotFound,
                ..
            })
        ));
    }

    #[test]
    fn server_automation_mutation_requires_explicit_capability() {
        let temp = tempfile::tempdir().expect("temporary storage root");
        let workspace_id = Uuid::from_u128(10);
        let service = service(&temp, workspace_id).with_server_authorization();
        let result = service.create_automation(request(
            workspace_id,
            CreateAutomationRequest {
                automation_id: Uuid::from_u128(11),
                name: "blocked".to_owned(),
                schedule: AutomationSchedule::Interval { period_seconds: 60 },
                timezone: "UTC".to_owned(),
                run_template: template(),
                first_run_at: at(100),
                max_submission_attempts: 3,
                created_at: at(1),
            },
        ));
        assert!(matches!(
            result,
            Err(ApiError {
                code: crate::ApiErrorCode::Unauthorized,
                ..
            })
        ));
    }

    #[test]
    fn manual_trigger_reuses_e5_idempotency_and_projects_history() {
        let temp = tempfile::tempdir().expect("temporary storage root");
        let workspace_id = Uuid::from_u128(20);
        let session_id = Uuid::from_u128(21);
        let plan_id = Uuid::from_u128(22);
        let version_id = Uuid::from_u128(23);
        let store = Arc::new(ControlPlaneStore::open(temp.path()).expect("open storage"));
        store
            .create_workspace(workspace_id, at(1))
            .expect("create workspace");
        store
            .create_session(workspace_id, session_id, at(1))
            .expect("create session");
        let service = ApiService::new(Arc::clone(&store));
        service
            .create_plan(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(24), workspace_id),
                body: crate::CreatePlanRequest {
                    plan_id,
                    created_at: at(1),
                },
            })
            .expect("create plan");
        let scan = PlanNodeId::from_uuid(Uuid::from_u128(25));
        let mut nodes = BTreeMap::new();
        nodes.insert(
            scan,
            PlanNode::new(
                PlanNodeKind::Scan {
                    source_asset_id: Uuid::from_u128(26),
                    projection: vec![stillflow_core::ColumnId::from_uuid(Uuid::from_u128(27))],
                    predicate: None,
                },
                Vec::new(),
            ),
        );
        service
            .save_plan_version(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(27), workspace_id),
                body: crate::SavePlanVersionRequest {
                    plan_id,
                    plan_version_id: version_id,
                    version_number: 1,
                    parent_version_id: None,
                    logical_plan: LogicalPlan::new(scan, nodes).expect("plan"),
                    created_at: at(1),
                },
            })
            .expect("save plan version");
        service
            .publish_plan_version(ApiRequest {
                meta: crate::RequestMetadata::new(Uuid::from_u128(28), workspace_id),
                body: crate::PublishPlanVersionRequest {
                    plan_version_id: version_id,
                    expected_current_version_id: None,
                    published_at: at(1),
                },
            })
            .expect("publish plan version");
        let automation_id = Uuid::from_u128(29);
        let run_template = AutomationRunTemplate {
            session_id,
            plan_version_id: version_id,
            plan_id: Some(plan_id),
            operation: None,
            inputs: Vec::new(),
            execution_policy: json!({"mode": "materialize"}),
            output_policy: json!({}),
        };
        service
            .create_automation(request(
                workspace_id,
                CreateAutomationRequest {
                    automation_id,
                    name: "manual-e2e".to_owned(),
                    schedule: AutomationSchedule::Interval { period_seconds: 60 },
                    timezone: "UTC".to_owned(),
                    run_template,
                    first_run_at: at(100),
                    max_submission_attempts: 3,
                    created_at: at(1),
                },
            ))
            .expect("create automation");
        let mut meta = crate::RequestMetadata::new(Uuid::from_u128(30), workspace_id);
        meta.idempotency_key = Some("manual-once".to_owned());
        let first = service
            .trigger_automation(ApiRequest {
                meta: meta.clone(),
                body: TriggerAutomationRequest {
                    automation_id,
                    triggered_at: at(2),
                },
            })
            .expect("manual trigger");
        let replay = service
            .trigger_automation(ApiRequest {
                meta: crate::RequestMetadata {
                    request_id: Uuid::from_u128(31),
                    ..meta
                },
                body: TriggerAutomationRequest {
                    automation_id,
                    triggered_at: at(2),
                },
            })
            .expect("manual trigger replay");
        assert_eq!(first.body.id, replay.body.id);
        let history = service
            .list_automation_history(request(
                workspace_id,
                ListAutomationHistoryRequest {
                    automation_id,
                    limit: 10,
                    cursor: None,
                },
            ))
            .expect("history")
            .body;
        assert_eq!(history.executions.len(), 1);
        assert_eq!(history.executions[0].state, "submitted");
        assert_eq!(history.executions[0].job_id, first.body.id);
    }
}
