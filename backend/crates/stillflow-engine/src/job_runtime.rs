//! Bounded durable Job runtime.
//!
//! The control plane remains the queue authority. This module owns only a
//! bounded set of worker wakeups and active cancellation handles; every Job
//! claim, state transition, and output reference is persisted through
//! `ControlPlaneStore`.

use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use stillflow_core::{
    ControlPlaneEventType, ErrorCategory, EventStreamKind, JobState, LogicalSchema, RequestContext,
    RunState, SourceAsset, SourceConnection,
};
use stillflow_plan::LogicalPlan;
use stillflow_storage::{
    ControlPlaneStore, EventDraft, FailureInfo, JobRecord, JobRecoveryDraft, JobSubmission,
    RunRecord, SnapshotStore, StorageError, SubmitOutcome,
};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::EngineError;
use crate::{
    ExecutionEngine, ExecutionIdentities, ExecutionRequest, ENGINE_BUILD, ENGINE_CONTRACT_VERSION,
    ENGINE_DEFAULT_DEADLINE, ENGINE_MAX_DEADLINE, MAX_ENGINE_CONCURRENT_RUNS,
};

pub const JOB_RUNTIME_WAKE_CAPACITY: usize = MAX_ENGINE_CONCURRENT_RUNS as usize;
const RECONCILIATION_LIMIT: usize = 1_024;
const BUSY_RETRY_DELAY: Duration = Duration::from_millis(25);

/// Inputs needed to turn one durable Job into an Engine execution request.
/// The resolver is the integration seam for loading connector credentials and
/// typed source descriptors; no secret is copied into control-plane rows.
pub struct JobExecutionSpec {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
    pub batch_size: usize,
    /// A previously committed VerificationBundle, if this Job publishes one.
    /// The runtime only binds it after Storage verifies ownership and commit.
    pub bundle_ref: Option<Uuid>,
}

pub type JobResolution =
    Pin<Box<dyn Future<Output = Result<JobExecutionSpec, JobRuntimeError>> + Send + 'static>>;

/// Resolves durable Job/Run records into an owned Engine request.
pub trait JobRequestResolver: Send + Sync {
    fn resolve(&self, job: JobRecord, run: RunRecord, context: RequestContext) -> JobResolution;
}

impl<F, Fut> JobRequestResolver for F
where
    F: Fn(JobRecord, RunRecord, RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<JobExecutionSpec, JobRuntimeError>> + Send + 'static,
{
    fn resolve(&self, job: JobRecord, run: RunRecord, context: RequestContext) -> JobResolution {
        Box::pin((self)(job, run, context))
    }
}

pub trait JobRuntimeIdentityProvider: Send + Sync {
    fn next_id(&self) -> Uuid;
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemJobRuntimeIdentityProvider;

impl JobRuntimeIdentityProvider for SystemJobRuntimeIdentityProvider {
    fn next_id(&self) -> Uuid {
        Uuid::new_v4()
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Error)]
pub enum JobRuntimeError {
    #[error("control-plane persistence failed")]
    Storage(#[source] StorageError),
    #[error("engine execution failed")]
    Engine(#[source] EngineError),
    #[error("invalid Job runtime input: {0}")]
    Invalid(&'static str),
    #[error("Job runtime is shutting down")]
    Shutdown,
    #[error("Job resolver exceeded its deadline")]
    ResolverTimeout,
    #[error("Job resolver panicked")]
    ResolverPanic,
    #[error("Job execution worker panicked")]
    WorkerPanic,
}

impl From<StorageError> for JobRuntimeError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<EngineError> for JobRuntimeError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

struct ActiveJob {
    job_id: Uuid,
    cancellation: CancellationToken,
}

struct RuntimeInner {
    workspace_id: Uuid,
    control_plane: Arc<ControlPlaneStore>,
    snapshot_store: Arc<SnapshotStore>,
    engine: Arc<ExecutionEngine>,
    resolver: Arc<dyn JobRequestResolver>,
    identity: Arc<dyn JobRuntimeIdentityProvider>,
    wake_tx: mpsc::Sender<()>,
    wake_rx: Mutex<mpsc::Receiver<()>>,
    shutdown: CancellationToken,
    active: Mutex<Vec<ActiveJob>>,
}

pub struct JobRuntime {
    inner: Arc<RuntimeInner>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl JobRuntime {
    pub fn new(
        workspace_id: Uuid,
        control_plane: Arc<ControlPlaneStore>,
        snapshot_store: Arc<SnapshotStore>,
        engine: Arc<ExecutionEngine>,
        resolver: Arc<dyn JobRequestResolver>,
        identity: Arc<dyn JobRuntimeIdentityProvider>,
    ) -> Result<Self, JobRuntimeError> {
        if workspace_id.is_nil() {
            return Err(JobRuntimeError::Invalid("workspace id must not be nil"));
        }
        let (wake_tx, wake_rx) = mpsc::channel(JOB_RUNTIME_WAKE_CAPACITY);
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                workspace_id,
                control_plane,
                snapshot_store,
                engine,
                resolver,
                identity,
                wake_tx,
                wake_rx: Mutex::new(wake_rx),
                shutdown: CancellationToken::new(),
                active: Mutex::new(Vec::with_capacity(MAX_ENGINE_CONCURRENT_RUNS as usize)),
            }),
            workers: Mutex::new(Vec::with_capacity(MAX_ENGINE_CONCURRENT_RUNS as usize)),
        })
    }

    pub fn new_with_system_identity(
        workspace_id: Uuid,
        control_plane: Arc<ControlPlaneStore>,
        snapshot_store: Arc<SnapshotStore>,
        engine: Arc<ExecutionEngine>,
        resolver: Arc<dyn JobRequestResolver>,
    ) -> Result<Self, JobRuntimeError> {
        Self::new(
            workspace_id,
            control_plane,
            snapshot_store,
            engine,
            resolver,
            Arc::new(SystemJobRuntimeIdentityProvider),
        )
    }

    /// Reconciles abandoned active rows before starting the fixed four-worker
    /// loop. Re-running start on an already started runtime is idempotent.
    pub async fn start(&self) -> Result<(), JobRuntimeError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(JobRuntimeError::Shutdown);
        }
        let mut workers = self.workers.lock().await;
        if !workers.is_empty() {
            return Ok(());
        }
        self.reconcile_on_start()?;
        for _ in 0..MAX_ENGINE_CONCURRENT_RUNS {
            workers.push(tokio::spawn(worker_loop(Arc::clone(&self.inner))));
        }
        drop(workers);
        self.wake();
        Ok(())
    }

    pub fn wake(&self) {
        let _ = self.inner.wake_tx.try_send(());
    }

    /// Persists a Job through the canonical idempotent queue and emits only a
    /// bounded wakeup signal after the durable commit succeeds.
    pub fn submit_job(&self, submission: JobSubmission) -> Result<SubmitOutcome, JobRuntimeError> {
        let outcome = self.inner.control_plane.submit_job(submission)?;
        self.wake();
        Ok(outcome)
    }

    /// Cancels a queued or running Job. State transitions are durable and
    /// idempotent; the in-memory token is only a prompt for connector cleanup.
    pub async fn cancel(
        &self,
        job_id: Uuid,
        request_id: impl Into<String>,
    ) -> Result<JobRecord, JobRuntimeError> {
        let request_id = request_id.into();
        if request_id.is_empty() {
            return Err(JobRuntimeError::Invalid(
                "cancel request id must be non-empty",
            ));
        }
        let job = self.inner.control_plane.get_job(job_id)?;
        let now = self.identity_time_at_least(job.queued_at);
        match job.state {
            JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::Cancelling => {
                return Ok(job)
            }
            JobState::Queued => {
                let cancelling = job_event(
                    &self.inner,
                    job.id,
                    ControlPlaneEventType::JobCancelling,
                    now,
                    &request_id,
                    serde_json::json!({"state": "cancelling"}),
                );
                let cancelled = job_event(
                    &self.inner,
                    job.id,
                    ControlPlaneEventType::JobCancelled,
                    now,
                    &request_id,
                    serde_json::json!({"state": "cancelled"}),
                );
                match self
                    .inner
                    .control_plane
                    .cancel_queued_job(job.id, cancelling, cancelled)
                {
                    Ok(record) => {
                        self.wake();
                        return Ok(record);
                    }
                    Err(error) if is_maintenance_busy(&error) => return Err(error.into()),
                    Err(StorageError::InvalidDraft(_)) | Err(StorageError::Busy(_)) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            JobState::Running => {}
        }

        let current = self.inner.control_plane.get_job(job_id)?;
        if current.state != JobState::Running {
            return Ok(current);
        }
        let run_id = current
            .run_id
            .ok_or(JobRuntimeError::Invalid("running Job has no Run"))?;
        let now = self.identity_time_at_least(current.started_at.unwrap_or(current.queued_at));
        let job_event = job_event(
            &self.inner,
            current.id,
            ControlPlaneEventType::JobCancelling,
            now,
            &request_id,
            serde_json::json!({"state": "cancelling"}),
        );
        let run_event = run_event(
            &self.inner,
            run_id,
            current.id,
            ControlPlaneEventType::RunCancelling,
            now,
            &request_id,
            serde_json::json!({"state": "cancelling"}),
        );
        match self
            .inner
            .control_plane
            .cancel_running_job(current.id, job_event, run_event)
        {
            Ok((record, _run)) => {
                let active = self.inner.active.lock().await;
                if let Some(active_job) = active.iter().find(|active| active.job_id == job_id) {
                    active_job.cancellation.cancel();
                }
                drop(active);
                self.wake();
                Ok(record)
            }
            Err(error) if is_maintenance_busy(&error) => Err(error.into()),
            Err(StorageError::InvalidDraft(_)) | Err(StorageError::Busy(_)) => {
                Ok(self.inner.control_plane.get_job(job_id)?)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Stops workers after prompting active connectors to clean up. A later
    /// process start will mark any row that remains active as worker_lost.
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let active = self.inner.active.lock().await;
        for active_job in active.iter() {
            active_job.cancellation.cancel();
        }
        drop(active);
        let mut workers = self.workers.lock().await;
        let handles = std::mem::take(&mut *workers);
        drop(workers);
        for handle in handles {
            let _ = handle.await;
        }
    }

    fn reconcile_on_start(&self) -> Result<(), JobRuntimeError> {
        let now = self.inner.identity.now();
        self.inner
            .snapshot_store
            .recover(now, Duration::ZERO, RECONCILIATION_LIMIT as u32)?;
        let candidates = self
            .inner
            .control_plane
            .list_reconciliation_candidates(self.inner.workspace_id, RECONCILIATION_LIMIT)?;
        let mut drafts = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let job = self.inner.control_plane.get_job(candidate.job_id)?;
            let event_time = self.identity_time_at_least(job.started_at.unwrap_or(job.queued_at));
            let request_id = format!("job-runtime:recovery:{}", candidate.job_id);
            let correlation_id = format!("job-runtime:recovery:{}", candidate.run_id);
            drafts.push(JobRecoveryDraft {
                job_id: candidate.job_id,
                run_id: candidate.run_id,
                reconciled_event: EventDraft::new(
                    self.inner.identity.next_id(),
                    EventStreamKind::Run,
                    candidate.run_id,
                    candidate.job_id,
                    Some(candidate.run_id),
                    ControlPlaneEventType::RunReconciled,
                    event_time,
                    request_id.clone(),
                    correlation_id.clone(),
                    "actor:job-runtime",
                    serde_json::json!({
                        "from": [job_state_text(candidate.job_state), run_state_text(candidate.run_state)],
                        "to": "failed",
                        "reason": "worker_lost"
                    }),
                ),
                run_failed_event: EventDraft::new(
                    self.inner.identity.next_id(),
                    EventStreamKind::Run,
                    candidate.run_id,
                    candidate.job_id,
                    Some(candidate.run_id),
                    ControlPlaneEventType::RunFailed,
                    event_time,
                    request_id.clone(),
                    correlation_id.clone(),
                    "actor:job-runtime",
                    serde_json::json!({"state": "failed", "reason": "worker_lost"}),
                ),
                job_failed_event: EventDraft::new(
                    self.inner.identity.next_id(),
                    EventStreamKind::Job,
                    candidate.job_id,
                    candidate.job_id,
                    None,
                    ControlPlaneEventType::JobFailed,
                    event_time,
                    request_id,
                    correlation_id,
                    "actor:job-runtime",
                    serde_json::json!({"state": "failed", "reason": "worker_lost"}),
                ),
                failure: FailureInfo::try_new(
                    "worker_lost",
                    false,
                    "worker was lost during process restart",
                )?,
            });
        }
        self.inner.control_plane.reconcile_jobs(&drafts)?;
        Ok(())
    }

    fn identity_time_at_least(&self, minimum: DateTime<Utc>) -> DateTime<Utc> {
        let now = self.inner.identity.now();
        if now < minimum {
            minimum
        } else {
            now
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerProgress {
    Processed,
    Idle,
    RetryLater,
}

async fn worker_loop(inner: Arc<RuntimeInner>) {
    loop {
        let signal = tokio::select! {
            _ = inner.shutdown.cancelled() => break,
            signal = async {
                let mut receiver = inner.wake_rx.lock().await;
                receiver.recv().await
            } => signal,
        };
        if signal.is_none() {
            break;
        }
        loop {
            match process_one(Arc::clone(&inner)).await {
                WorkerProgress::Processed => continue,
                WorkerProgress::Idle => break,
                WorkerProgress::RetryLater => {
                    tokio::select! {
                        _ = inner.shutdown.cancelled() => return,
                        _ = time::sleep(BUSY_RETRY_DELAY) => {}
                    }
                }
            }
        }
    }
}

async fn process_one(inner: Arc<RuntimeInner>) -> WorkerProgress {
    let job = match inner.control_plane.next_queued_job(inner.workspace_id) {
        Ok(Some(job)) => job,
        Ok(None) => return WorkerProgress::Idle,
        Err(_) => return WorkerProgress::RetryLater,
    };
    let permit = match inner.engine.try_acquire_run_permit() {
        Ok(permit) => permit,
        Err(EngineError::Busy) => return WorkerProgress::RetryLater,
        Err(_) => return WorkerProgress::RetryLater,
    };

    let deadline = match job_deadline(&job) {
        Ok(deadline) => deadline,
        Err(error) => {
            fail_queued_job(&inner, &job, failure_for_runtime_error(&error)).await;
            drop(permit);
            return WorkerProgress::Processed;
        }
    };
    let started_at = at_least(&inner, job.queued_at);
    let run_id = inner.identity.next_id();
    let request_id = format!("job-runtime:run:{}", job.id);
    let claimed = inner.control_plane.claim_job(
        job.id,
        run_id,
        started_at,
        ENGINE_CONTRACT_VERSION,
        ENGINE_BUILD,
        job_event(
            &inner,
            job.id,
            ControlPlaneEventType::JobRunning,
            started_at,
            &request_id,
            serde_json::json!({"state": "running"}),
        ),
        run_event(
            &inner,
            run_id,
            job.id,
            ControlPlaneEventType::RunRunning,
            started_at,
            &request_id,
            serde_json::json!({"state": "running"}),
        ),
    );
    let run = match claimed {
        Ok(run) => run,
        Err(StorageError::Busy(_)) => {
            drop(permit);
            return WorkerProgress::Processed;
        }
        Err(error) => {
            fail_queued_job(&inner, &job, failure_from_storage(&error)).await;
            drop(permit);
            return WorkerProgress::Processed;
        }
    };

    let cancellation = CancellationToken::new();
    {
        let mut active = inner.active.lock().await;
        active.push(ActiveJob {
            job_id: job.id,
            cancellation: cancellation.clone(),
        });
    }
    if matches!(
        inner.control_plane.get_job(job.id),
        Ok(JobRecord {
            state: JobState::Cancelling,
            ..
        })
    ) {
        cancellation.cancel();
    }
    let context = RequestContext::with_cancellation_and_deadline(
        cancellation.clone(),
        Instant::now() + deadline,
    );
    let outcome = execute_claimed(&inner, &job, &run, context.clone(), permit).await;
    remove_active(&inner, job.id).await;
    match outcome {
        Ok((manifest_id, bundle_ref)) => {
            finish_claimed(
                &inner,
                &job,
                &run,
                RunState::Succeeded,
                JobState::Succeeded,
                None,
                Some(manifest_id),
                bundle_ref,
            )
            .await;
        }
        Err(error) => {
            let current = inner.control_plane.get_job(job.id).ok();
            let cancelled = current
                .as_ref()
                .is_some_and(|current| current.state == JobState::Cancelling);
            let (run_state, job_state, failure) = if cancelled {
                (RunState::Cancelled, JobState::Cancelled, None)
            } else {
                (
                    RunState::Failed,
                    JobState::Failed,
                    Some(failure_for_runtime_error(&error)),
                )
            };
            finish_claimed(
                &inner, &job, &run, run_state, job_state, failure, None, None,
            )
            .await;
        }
    }
    WorkerProgress::Processed
}

async fn execute_claimed(
    inner: &RuntimeInner,
    job: &JobRecord,
    run: &RunRecord,
    context: RequestContext,
    permit: OwnedSemaphorePermit,
) -> Result<(Uuid, Option<Uuid>), JobRuntimeError> {
    let resolved = AssertUnwindSafe(async {
        let future = inner
            .resolver
            .resolve(job.clone(), run.clone(), context.clone());
        let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
        tokio::select! {
            _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
            result = time::timeout_at(deadline, future) => match result {
                Ok(result) => result,
                Err(_) => Err(JobRuntimeError::ResolverTimeout),
            },
        }
    })
    .catch_unwind()
    .await;
    let spec = match resolved {
        Ok(Ok(spec)) => spec,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Err(JobRuntimeError::ResolverPanic),
    };
    let request = ExecutionRequest {
        plan: spec.plan,
        connection: spec.connection,
        asset: spec.asset,
        schema_override: spec.schema_override,
        identities: ExecutionIdentities {
            snapshot_id: spec.snapshot_id,
            dataset_id: spec.dataset_id,
            session_id: job.session_id,
            created_at: job.queued_at,
            started_at: run.started_at,
            lineage: spec.lineage,
            quality_score: spec.quality_score,
        },
        context: context.clone(),
        batch_size: spec.batch_size,
        store: inner.snapshot_store.as_ref(),
    };
    let bundle_ref = spec.bundle_ref;
    let executed = AssertUnwindSafe(async {
        let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
        tokio::select! {
            _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
            result = time::timeout_at(deadline, inner.engine.materialize_with_permit(request, permit)) => match result {
                Ok(Ok((manifest, _memory))) => Ok(manifest.snapshot().id()),
                Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
            },
        }
    })
    .catch_unwind()
    .await;
    match executed {
        Ok(Ok(snapshot_id)) => Ok((snapshot_id, bundle_ref)),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(JobRuntimeError::WorkerPanic),
    }
}

async fn fail_queued_job(inner: &RuntimeInner, job: &JobRecord, failure: FailureInfo) {
    let at = at_least(inner, job.queued_at);
    let _ = inner.control_plane.transition_job(
        job.id,
        JobState::Failed,
        job_event(
            inner,
            job.id,
            ControlPlaneEventType::JobFailed,
            at,
            &format!("job-runtime:failed:{}", job.id),
            serde_json::json!({"state": "failed", "category": failure.category}),
        ),
        Some(failure),
    );
}

#[allow(clippy::too_many_arguments)]
async fn finish_claimed(
    inner: &RuntimeInner,
    job: &JobRecord,
    run: &RunRecord,
    run_state: RunState,
    job_state: JobState,
    failure: Option<FailureInfo>,
    snapshot_ref: Option<Uuid>,
    bundle_ref: Option<Uuid>,
) {
    let at = at_least(inner, run.started_at.max(job.queued_at));
    let request_id = format!("job-runtime:terminal:{}", job.id);
    let result = inner.control_plane.finish_run_and_job_with_outputs(
        run.id,
        run_state,
        job_state,
        snapshot_ref,
        bundle_ref,
        run_event(
            inner,
            run.id,
            job.id,
            event_type_for_run_state(run_state),
            at,
            &request_id,
            serde_json::json!({"state": run_state_text(run_state)}),
        ),
        job_event(
            inner,
            job.id,
            event_type_for_job_state(job_state),
            at,
            &request_id,
            serde_json::json!({"state": job_state_text(job_state)}),
        ),
        failure,
    );
    if result.is_ok() {
        return;
    }

    // A cancellation or another terminal CAS may have won while the Engine
    // was publishing. If output validation itself failed, fail the Run
    // without references rather than leaving an active durable row behind.
    let Ok(current) = inner.control_plane.get_job(job.id) else {
        return;
    };
    let (fallback_run_state, fallback_job_state, fallback_failure) =
        if current.state == JobState::Cancelling {
            (RunState::Cancelled, JobState::Cancelled, None)
        } else if matches!(current.state, JobState::Running) {
            (
                RunState::Failed,
                JobState::Failed,
                Some(fallback_failure("terminal publication failed")),
            )
        } else {
            return;
        };
    let fallback_at = at_least(inner, run.started_at.max(job.queued_at));
    let fallback_request_id = format!("job-runtime:terminal-recovery:{}", job.id);
    let _ = inner.control_plane.finish_run_and_job_with_outputs(
        run.id,
        fallback_run_state,
        fallback_job_state,
        None,
        None,
        run_event(
            inner,
            run.id,
            job.id,
            event_type_for_run_state(fallback_run_state),
            fallback_at,
            &fallback_request_id,
            serde_json::json!({"state": run_state_text(fallback_run_state)}),
        ),
        job_event(
            inner,
            job.id,
            event_type_for_job_state(fallback_job_state),
            fallback_at,
            &fallback_request_id,
            serde_json::json!({"state": job_state_text(fallback_job_state)}),
        ),
        fallback_failure,
    );
}

async fn remove_active(inner: &RuntimeInner, job_id: Uuid) {
    let mut active = inner.active.lock().await;
    active.retain(|active| active.job_id != job_id);
}

fn job_deadline(job: &JobRecord) -> Result<Duration, JobRuntimeError> {
    let seconds = match job.execution_policy.get("deadlineSeconds") {
        None => ENGINE_DEFAULT_DEADLINE.as_secs(),
        Some(value) => value.as_u64().ok_or(JobRuntimeError::Invalid(
            "deadlineSeconds must be an integer",
        ))?,
    };
    let deadline = Duration::from_secs(seconds);
    if deadline.is_zero() || deadline > ENGINE_MAX_DEADLINE {
        return Err(JobRuntimeError::Invalid(
            "deadlineSeconds must be within 1..=1800",
        ));
    }
    Ok(deadline)
}

fn at_least(inner: &RuntimeInner, minimum: DateTime<Utc>) -> DateTime<Utc> {
    let now = inner.identity.now();
    now.max(minimum)
}

fn is_maintenance_busy(error: &StorageError) -> bool {
    matches!(error, StorageError::Busy(message) if *message == "maintenance is active")
        || matches!(error, StorageError::Busy(message) if *message == "storage activity prevents maintenance")
}

fn job_event(
    inner: &RuntimeInner,
    job_id: Uuid,
    event_type: ControlPlaneEventType,
    occurred_at: DateTime<Utc>,
    request_id: &str,
    payload: serde_json::Value,
) -> EventDraft {
    EventDraft::new(
        inner.identity.next_id(),
        EventStreamKind::Job,
        job_id,
        job_id,
        None,
        event_type,
        occurred_at,
        request_id,
        format!("job-runtime:{job_id}"),
        "actor:job-runtime",
        payload,
    )
}

fn run_event(
    inner: &RuntimeInner,
    run_id: Uuid,
    job_id: Uuid,
    event_type: ControlPlaneEventType,
    occurred_at: DateTime<Utc>,
    request_id: &str,
    payload: serde_json::Value,
) -> EventDraft {
    EventDraft::new(
        inner.identity.next_id(),
        EventStreamKind::Run,
        run_id,
        job_id,
        Some(run_id),
        event_type,
        occurred_at,
        request_id,
        format!("job-runtime:{job_id}"),
        "actor:job-runtime",
        payload,
    )
}

fn event_type_for_job_state(state: JobState) -> ControlPlaneEventType {
    match state {
        JobState::Running => ControlPlaneEventType::JobRunning,
        JobState::Cancelling => ControlPlaneEventType::JobCancelling,
        JobState::Succeeded => ControlPlaneEventType::JobSucceeded,
        JobState::Failed => ControlPlaneEventType::JobFailed,
        JobState::Cancelled => ControlPlaneEventType::JobCancelled,
        JobState::Queued => ControlPlaneEventType::JobQueued,
    }
}

fn event_type_for_run_state(state: RunState) -> ControlPlaneEventType {
    match state {
        RunState::Running => ControlPlaneEventType::RunRunning,
        RunState::Cancelling => ControlPlaneEventType::RunCancelling,
        RunState::Succeeded => ControlPlaneEventType::RunSucceeded,
        RunState::Failed => ControlPlaneEventType::RunFailed,
        RunState::Cancelled => ControlPlaneEventType::RunCancelled,
    }
}

fn job_state_text(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Cancelling => "cancelling",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

fn run_state_text(state: RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Cancelling => "cancelling",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
    }
}

fn failure_for_runtime_error(error: &JobRuntimeError) -> FailureInfo {
    match error {
        JobRuntimeError::Engine(error) => {
            let summary = error.sanitized_summary();
            FailureInfo::try_new(
                error_category_text(summary.category),
                summary.retryable,
                summary.message(),
            )
            .unwrap_or_else(|_| fallback_failure("engine execution failed"))
        }
        JobRuntimeError::Storage(error) => failure_from_storage(error),
        JobRuntimeError::Invalid(message) => fallback_failure(message),
        JobRuntimeError::ResolverTimeout => {
            FailureInfo::try_new("timeout", false, "Job resolver exceeded its deadline")
                .unwrap_or_else(|_| fallback_failure("Job resolver exceeded its deadline"))
        }
        JobRuntimeError::ResolverPanic | JobRuntimeError::WorkerPanic => {
            fallback_failure("Job worker panicked")
        }
        JobRuntimeError::Shutdown => fallback_failure("Job worker stopped before completion"),
    }
}

fn failure_from_storage(error: &StorageError) -> FailureInfo {
    FailureInfo::try_new(
        "storage",
        matches!(error, StorageError::Busy(_)),
        "Job persistence operation failed",
    )
    .unwrap_or_else(|_| fallback_failure("Job persistence operation failed"))
}

fn fallback_failure(message: &'static str) -> FailureInfo {
    FailureInfo::try_new("internal", false, message).expect("static failure is safe")
}

fn error_category_text(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Authentication => "authentication",
        ErrorCategory::Authorization => "authorization",
        ErrorCategory::NotFound => "not_found",
        ErrorCategory::InvalidConfiguration => "invalid_configuration",
        ErrorCategory::InvalidData => "invalid_data",
        ErrorCategory::SchemaDrift => "schema_drift",
        ErrorCategory::RateLimited => "rate_limited",
        ErrorCategory::Timeout => "timeout",
        ErrorCategory::Cancelled => "cancelled",
        ErrorCategory::UnsupportedCapability => "unsupported_capability",
        ErrorCategory::TransientSource => "transient_source",
        ErrorCategory::Internal => "internal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use stillflow_connectors::ConnectorRegistry;
    use stillflow_storage::{JobSubmission, PlanVersionDraft, SnapshotStore, SubmitOutcome};
    use tempfile::TempDir;

    fn job_with_policy(execution_policy: serde_json::Value) -> JobRecord {
        JobRecord {
            id: Uuid::from_u128(1),
            workspace_id: Uuid::from_u128(2),
            session_id: Uuid::from_u128(3),
            plan_version_id: Uuid::from_u128(4),
            canonical_plan_digest: [0; 32],
            inputs: Vec::new(),
            execution_policy,
            output_policy: serde_json::json!({}),
            state: JobState::Queued,
            queued_at: DateTime::from_timestamp(1, 0).expect("timestamp"),
            started_at: None,
            finished_at: None,
            run_id: None,
            failure: None,
        }
    }

    #[test]
    fn runtime_deadline_uses_default_and_rejects_out_of_bound_values() {
        assert_eq!(
            job_deadline(&job_with_policy(serde_json::json!({}))).expect("default deadline"),
            ENGINE_DEFAULT_DEADLINE
        );
        assert_eq!(
            job_deadline(&job_with_policy(serde_json::json!({"deadlineSeconds": 30})))
                .expect("configured deadline"),
            Duration::from_secs(30)
        );
        assert!(matches!(
            job_deadline(&job_with_policy(
                serde_json::json!({"deadlineSeconds": 1801})
            )),
            Err(JobRuntimeError::Invalid(
                "deadlineSeconds must be within 1..=1800"
            ))
        ));
    }

    #[test]
    fn runtime_wakeup_capacity_matches_engine_run_gate() {
        assert_eq!(
            JOB_RUNTIME_WAKE_CAPACITY,
            MAX_ENGINE_CONCURRENT_RUNS as usize
        );
    }

    #[test]
    fn resolver_deadline_failure_uses_timeout_category() {
        let failure = failure_for_runtime_error(&JobRuntimeError::ResolverTimeout);
        assert_eq!(failure.category, "timeout");
        assert!(!failure.retryable);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_cancel_is_idempotent_and_creates_no_run() {
        let temp = TempDir::new().expect("temporary storage");
        let snapshot_store =
            Arc::new(SnapshotStore::open(temp.path(), Default::default()).expect("store"));
        let control_plane = Arc::new(snapshot_store.control_plane());
        let workspace_id = Uuid::from_u128(2);
        let session_id = Uuid::from_u128(3);
        let plan_id = Uuid::from_u128(4);
        let plan_version_id = Uuid::from_u128(5);
        let timestamp = |seconds| DateTime::from_timestamp(seconds, 0).expect("timestamp");
        control_plane
            .create_workspace(workspace_id, timestamp(1))
            .expect("workspace");
        control_plane
            .create_session(workspace_id, session_id, timestamp(2))
            .expect("session");
        control_plane
            .create_plan(workspace_id, plan_id, timestamp(3))
            .expect("plan");
        let canonical_plan_bytes = b"runtime-test-plan".to_vec();
        let plan_digest = Sha256::digest(&canonical_plan_bytes);
        let mut plan_digest_array = [0; 32];
        plan_digest_array.copy_from_slice(&plan_digest);
        control_plane
            .create_plan_version(PlanVersionDraft {
                workspace_id,
                plan_id,
                plan_version_id,
                version_number: 1,
                parent_version_id: None,
                logical_plan: serde_json::json!({"version": 1}),
                canonical_plan_bytes,
                canonical_plan_digest: plan_digest_array,
                plan_fingerprint: [7; 32],
                created_at: timestamp(4),
            })
            .expect("PlanVersion");
        control_plane
            .publish_plan_version(plan_version_id, None, timestamp(5))
            .expect("publish PlanVersion");
        let submission = JobSubmission::try_new(
            workspace_id,
            session_id,
            plan_version_id,
            plan_digest_array,
            Uuid::from_u128(6),
            "runtime-cancel",
            Vec::new(),
            serde_json::json!({"deadlineSeconds": 900}),
            serde_json::json!({}),
            timestamp(10),
            Uuid::from_u128(7),
            "submit",
            "submit",
            "actor:test",
        )
        .expect("submission");
        let resolver: Arc<dyn JobRequestResolver> = Arc::new(|_, _, _| async {
            Err(JobRuntimeError::Invalid("test resolver must not run"))
        });
        let runtime = JobRuntime::new_with_system_identity(
            workspace_id,
            control_plane.clone(),
            snapshot_store,
            Arc::new(ExecutionEngine::new(ConnectorRegistry::new())),
            resolver,
        )
        .expect("runtime");
        let job = match runtime.submit_job(submission).expect("submit Job") {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let cancelled = runtime.cancel(job.id, "cancel-1").await.expect("cancel");
        assert_eq!(cancelled.state, JobState::Cancelled);
        let replay = runtime
            .cancel(job.id, "cancel-2")
            .await
            .expect("cancel replay");
        assert_eq!(replay.state, JobState::Cancelled);
        assert!(control_plane
            .list_runs(workspace_id, None, 10)
            .expect("Runs")
            .runs
            .is_empty());
    }
}
