//! Bounded durable Job runtime.
//!
//! The control plane remains the queue authority. This module owns only a
//! bounded set of worker wakeups and active cancellation handles; every Job
//! claim, state transition, and output reference is persisted through
//! `ControlPlaneStore`.

use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use sha2::{Digest as _, Sha256};
use stillflow_core::{
    ArtifactKind, ControlPlaneEventType, ErrorCategory, EventStreamKind, ExportDestination,
    ExportPolicy, InputRef, JobState, LogicalInputRef, LogicalSchema, OperationDescriptorV1,
    ProfileColumnsV1, RequestContext, RunState, SourceAsset, SourceConnection,
};
use stillflow_plan::LogicalPlan;
use stillflow_storage::{
    ArtifactOutputRef, ArtifactRefDraft, ControlPlaneStore, EventDraft, ExternalRefKind,
    FailureInfo, JobRecord, JobRecoveryDraft, JobSubmission, RunRecord, SnapshotOutputRef,
    SnapshotStore, StorageError, SubmitOutcome, TerminalOutputRef,
};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::{map_context_error, EngineError};
use crate::{
    run_export_with_run, ExecutionEngine, ExecutionIdentities, ExecutionRequest, ExportRequest,
    FindingProvenance, ProfileColumns, QualityRequest, VerificationIdentities, VerificationRequest,
    ENGINE_BUILD, ENGINE_CONTRACT_VERSION, ENGINE_DEFAULT_DEADLINE, ENGINE_MAX_DEADLINE,
    MAX_ENGINE_CONCURRENT_RUNS,
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
        Ok(outcome) => {
            finish_claimed(
                &inner,
                &job,
                &run,
                RunState::Succeeded,
                JobState::Succeeded,
                None,
                outcome.snapshot_ref,
                outcome.bundle_ref,
                outcome.terminal_outputs,
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
                &inner, &job, &run, run_state, job_state, failure, None, None, None,
            )
            .await;
        }
    }
    WorkerProgress::Processed
}

#[derive(Debug)]
struct ExecutionOutcome {
    snapshot_ref: Option<Uuid>,
    bundle_ref: Option<Uuid>,
    terminal_outputs: Option<Vec<TerminalOutputRef>>,
}

async fn execute_claimed(
    inner: &RuntimeInner,
    job: &JobRecord,
    run: &RunRecord,
    context: RequestContext,
    permit: OwnedSemaphorePermit,
) -> Result<ExecutionOutcome, JobRuntimeError> {
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
    let operation = match (&job.operation, &run.operation) {
        (Some(job_operation), Some(run_operation)) if job_operation != run_operation => {
            return Err(JobRuntimeError::Invalid(
                "Job and Run operation identities disagree",
            ));
        }
        (Some(operation), _) | (_, Some(operation)) => Some(operation.clone()),
        (None, None) => None,
    };
    if operation.is_some() {
        validate_resolved_plan(&spec.plan, job.canonical_plan_digest)?;
    }

    let executed = AssertUnwindSafe(async move {
        match operation {
            // Compatibility path for E5-J1 rows written before the typed
            // operation columns existed. New submissions never use it.
            None => {
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
                let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
                let result = tokio::select! {
                    _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
                    result = time::timeout_at(deadline, inner.engine.materialize_with_permit(request, permit)) => match result {
                        Ok(Ok((manifest, _memory))) => Ok(manifest.snapshot().id()),
                        Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                        Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
                    },
                }?;
                Ok(ExecutionOutcome {
                    snapshot_ref: Some(result),
                    bundle_ref,
                    terminal_outputs: None,
                })
            }
            Some(operation) => match operation.descriptor.clone() {
                OperationDescriptorV1::Materialize {
                    source_asset,
                    materialize_policy,
                } => {
                    validate_source_binding(job, &spec, &source_asset)?;
                    let snapshot_id = inner.identity.next_id();
                    let request = ExecutionRequest {
                        plan: spec.plan,
                        connection: spec.connection,
                        asset: spec.asset,
                        schema_override: spec.schema_override,
                        identities: ExecutionIdentities {
                            snapshot_id,
                            dataset_id: spec.dataset_id,
                            session_id: job.session_id,
                            created_at: job.queued_at,
                            started_at: run.started_at,
                            lineage: spec.lineage,
                            quality_score: spec.quality_score,
                        },
                        context: context.clone(),
                        batch_size: materialize_policy.batch_size,
                        store: inner.snapshot_store.as_ref(),
                    };
                    let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
                    let result = tokio::select! {
                        _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
                        result = time::timeout_at(deadline, inner.engine.materialize_with_permit(request, permit)) => match result {
                            Ok(Ok((manifest, _memory))) => Ok(manifest),
                            Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                            Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
                        },
                    }?;
                    let output_id = result.snapshot().id();
                    let version_digest = inner
                        .snapshot_store
                        .version_digest(output_id)
                        .map_err(JobRuntimeError::Storage)?;
                    let snapshot = result.snapshot();
                    Ok(ExecutionOutcome {
                        snapshot_ref: None,
                        bundle_ref: None,
                        terminal_outputs: Some(vec![TerminalOutputRef::Snapshot {
                            workspace_id: job.workspace_id,
                            session_id: snapshot.session_id(),
                            dataset_id: snapshot.dataset_id(),
                            snapshot_id: output_id,
                            version_digest,
                            schema_fingerprint: *snapshot.schema_fingerprint().as_bytes(),
                            snapshot_version: snapshot.version(),
                            committed: true,
                        }]),
                    })
                }
                OperationDescriptorV1::Verification {
                    snapshot,
                    verification_policy,
                } => {
                    let logical_input = LogicalInputRef {
                        input: InputRef::Snapshot {
                            snapshot_id: snapshot.snapshot_id,
                        },
                        version_digest: snapshot.version_digest,
                    };
                    let identities = VerificationIdentities {
                        run_id: run.id,
                        bundle_id: inner.identity.next_id(),
                        bundle_artifact_id: inner.identity.next_id(),
                        // Verification is Snapshot-backed: the accepted
                        // bundle child is the committed input Snapshot, not
                        // a second materialized Snapshot identity.
                        snapshot_id: snapshot.snapshot_id,
                        dataset_id: snapshot.dataset_id,
                        validation_report_artifact_id: inner.identity.next_id(),
                        rejected_rows_artifact_id: verification_policy
                            .publish_rejected_rows
                            .then(|| inner.identity.next_id()),
                        deduplication_report_artifact_id: inner.identity.next_id(),
                        session_id: job.session_id,
                        logical_input,
                        canonical_plan_digest: job.canonical_plan_digest,
                        created_at: job.queued_at,
                        started_at: run.started_at,
                        committed_at: at_least(inner, run.started_at),
                        lineage: spec.lineage,
                        quality_score: spec.quality_score,
                    };
                    let request = VerificationRequest {
                        plan: spec.plan,
                        connection: spec.connection,
                        asset: spec.asset,
                        schema_override: spec.schema_override,
                        identities,
                        context: context.clone(),
                        batch_size: verification_policy.batch_size,
                        store: inner.snapshot_store.as_ref(),
                    };
                    let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
                    let bundle = tokio::select! {
                        _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
                        result = time::timeout_at(deadline, inner.engine.materialize_verification_snapshot_with_permit(request, snapshot.snapshot_id, &permit)) => match result {
                            Ok(Ok(bundle)) => Ok(bundle),
                            Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                            Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
                        },
                    }?;
                    let bundle_id = bundle.membership().bundle_id();
                    let version_digest = inner
                        .snapshot_store
                        .verification_bundle_version_digest(bundle_id)
                        .map_err(JobRuntimeError::Storage)?;
                    let accepted = bundle.accepted().manifest().snapshot();
                    let accepted_snapshot = SnapshotOutputRef {
                        workspace_id: job.workspace_id,
                        session_id: accepted.session_id(),
                        dataset_id: accepted.dataset_id(),
                        snapshot_id: accepted.id(),
                        version_digest: inner
                            .snapshot_store
                            .version_digest(accepted.id())
                            .map_err(JobRuntimeError::Storage)?,
                        schema_fingerprint: *accepted.schema_fingerprint().as_bytes(),
                        snapshot_version: accepted.version(),
                        committed: true,
                    };
                    let mut members = vec![
                        artifact_output_ref(
                            job.workspace_id,
                            run.id,
                            bundle.validation_report().manifest().artifact_id(),
                            bundle.validation_report().manifest().kind(),
                            bundle.validation_report().manifest().version(),
                            bundle.validation_report().provenance().content_digest,
                        ),
                        artifact_output_ref(
                            job.workspace_id,
                            run.id,
                            bundle.deduplication_report().manifest().artifact_id(),
                            bundle.deduplication_report().manifest().kind(),
                            bundle.deduplication_report().manifest().version(),
                            bundle.deduplication_report().provenance().content_digest,
                        ),
                    ];
                    if let Some(rejected) = bundle.rejected_rows() {
                        members.push(artifact_output_ref(
                            job.workspace_id,
                            run.id,
                            rejected.manifest().artifact_id(),
                            rejected.manifest().kind(),
                            rejected.manifest().version(),
                            rejected.provenance().content_digest,
                        ));
                    }
                    Ok(ExecutionOutcome {
                        snapshot_ref: None,
                        bundle_ref: None,
                        terminal_outputs: Some(vec![TerminalOutputRef::VerificationBundle {
                            workspace_id: job.workspace_id,
                            run_id: run.id,
                            bundle_id,
                            bundle_version: bundle
                                .provenance()
                                .draft
                                .verification_contract_version,
                            version_digest,
                            accepted_snapshot,
                            members,
                        }]),
                    })
                }
                OperationDescriptorV1::Profile {
                    snapshot,
                    profile_request,
                } => {
                    let columns = match profile_request.columns {
                        ProfileColumnsV1::All => ProfileColumns::All,
                        ProfileColumnsV1::Explicit(columns) => ProfileColumns::Explicit(columns),
                    };
                    let deadline = context.deadline().ok_or(JobRuntimeError::ResolverTimeout)?;
                    let profile = tokio::select! {
                        _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
                        result = time::timeout_at(deadline, inner.engine.profile_snapshot_with_permit(
                            inner.snapshot_store.as_ref(),
                            snapshot.snapshot_id,
                            columns,
                            profile_request.top_k,
                            profile_request.histogram_buckets,
                            context.clone(),
                            run.id,
                        )) => match result {
                            Ok(Ok(profile)) => Ok(profile),
                            Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                            Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
                        },
                    }?;
                    let profile_body = profile.canonical_body.clone();
                    let profile_rows = profile.profile.dataset.row_count_scanned;
                    let profile_bytes = profile.profile.dataset.scanned_bytes;
                    let profile_truncated = profile.profile.dataset.truncated;
                    let profile_contract_version = profile.profile.profiling_contract_version;
                    let profile_digest_hex = profile.canonical_digest.clone();
                    let profile_digest = parse_digest_hex(
                        &profile_digest_hex,
                        "profile report digest is invalid",
                    )?;
                    let request_digest = digest_hex_bytes(
                        &job
                            .request_digest
                            .ok_or(JobRuntimeError::Invalid("typed Job request digest is missing"))?,
                    );
                    let provenance = FindingProvenance::deterministic(
                        run.id,
                        format!("snapshot:{}", snapshot.snapshot_id),
                        request_digest,
                        Some(digest_hex_bytes(&run.plan_fingerprint)),
                    );
                    let quality_request = QualityRequest::new(
                        profile,
                        context.clone(),
                        provenance,
                    )
                        .map_err(JobRuntimeError::Engine)?;
                    let quality = tokio::select! {
                        _ = inner.shutdown.cancelled() => Err(JobRuntimeError::Shutdown),
                        result = time::timeout_at(deadline, inner.engine.quality_with_permit(quality_request)) => match result {
                            Ok(Ok(quality)) => Ok(quality),
                            Ok(Err(error)) => Err(JobRuntimeError::Engine(error)),
                            Err(_) => Err(JobRuntimeError::Engine(EngineError::Timeout)),
                        },
                    }?;
                    let quality_body = quality.canonical_body.clone();
                    let created_at = at_least(inner, run.started_at);
                    let profile_artifact_id = inner.identity.next_id();
                    let quality_artifact_id = inner.identity.next_id();
                    let operation_descriptor_digest = digest_hex_bytes(
                        &operation
                            .descriptor_digest()
                            .map_err(|_| JobRuntimeError::Invalid("typed operation digest is invalid"))?,
                    );
                    let request_digest_hex = digest_hex_bytes(
                        &job
                            .request_digest
                            .ok_or(JobRuntimeError::Invalid("typed Job request digest is missing"))?,
                    );
                    let common_provenance = serde_json::json!({
                        "workspaceId": job.workspace_id,
                        "sessionId": job.session_id,
                        "runId": run.id,
                        "planId": job.plan_id,
                        "planVersionId": job.plan_version_id,
                        "canonicalPlanDigest": digest_hex_bytes(&job.canonical_plan_digest),
                        "operationKind": "profile",
                        "operationVersion": operation.operation_version,
                        "operationDescriptorDigest": operation_descriptor_digest,
                        "requestDigest": request_digest_hex,
                        "snapshotId": snapshot.snapshot_id,
                        "snapshotVersionDigest": digest_hex_bytes(&snapshot.version_digest),
                        "schemaFingerprint": digest_hex_bytes(&snapshot.schema_fingerprint),
                        "snapshotVersion": snapshot.snapshot_version,
                        "profilingContractVersion": profile_contract_version,
                        "scan": {
                            "rowCountScanned": profile_rows,
                            "scannedBytes": profile_bytes,
                            "truncated": profile_truncated
                        }
                    });
                    let profile_metadata = serde_json::json!({
                        "artifactType": "profile_report",
                        "artifactBodyVersion": 1,
                        "canonicalDigest": profile_digest_hex,
                        "provenance": common_provenance.clone()
                    });
                    inner.control_plane.create_artifact_ref_with_body(ArtifactRefDraft {
                        workspace_id: job.workspace_id,
                        run_id: run.id,
                        artifact_id: profile_artifact_id,
                        artifact_kind: ArtifactKind::ProfileReport,
                        external_ref_kind: ExternalRefKind::Artifact,
                        external_ref_id: snapshot.snapshot_id,
                        content_digest: profile_digest,
                        metadata: profile_metadata,
                        created_at,
                    }, profile_body)?;
                    let quality_digest = parse_digest_hex(
                        &quality.canonical_digest,
                        "quality report digest is invalid",
                    )?;
                    let quality_metadata = serde_json::json!({
                        "artifactType": "quality_report",
                        "artifactBodyVersion": 1,
                        "canonicalDigest": quality.canonical_digest,
                        "profileReportArtifactId": profile_artifact_id,
                        "profileReportDigest": quality.report.profile_report_digest,
                        "qualityScoreVersion": quality.report.score.version,
                        "detectorContractVersion": crate::DETECTOR_CONTRACT_VERSION,
                        "findingCount": quality.report.findings.len(),
                        "provenance": common_provenance
                    });
                    inner.control_plane.create_artifact_ref_with_body(ArtifactRefDraft {
                        workspace_id: job.workspace_id,
                        run_id: run.id,
                        artifact_id: quality_artifact_id,
                        artifact_kind: ArtifactKind::QualityReport,
                        external_ref_kind: ExternalRefKind::Artifact,
                        external_ref_id: profile_artifact_id,
                        content_digest: quality_digest,
                        metadata: quality_metadata,
                        created_at,
                    }, quality_body)?;
                    Ok(ExecutionOutcome {
                        snapshot_ref: None,
                        bundle_ref: None,
                        terminal_outputs: Some(vec![
                            TerminalOutputRef::Artifact {
                                workspace_id: job.workspace_id,
                                run_id: run.id,
                                artifact_id: profile_artifact_id,
                                artifact_kind: ArtifactKind::ProfileReport,
                                artifact_version: 1,
                                content_digest: profile_digest,
                                state: stillflow_core::ArtifactRefState::Committed,
                            },
                            TerminalOutputRef::Artifact {
                                workspace_id: job.workspace_id,
                                run_id: run.id,
                                artifact_id: quality_artifact_id,
                                artifact_kind: ArtifactKind::QualityReport,
                                artifact_version: 1,
                                content_digest: quality_digest,
                                state: stillflow_core::ArtifactRefState::Committed,
                            },
                        ]),
                    })
                }
                OperationDescriptorV1::Export {
                    snapshot,
                    export_request,
                } => {
                    let destination = export_destination(
                        export_request.destination,
                        export_request.format,
                        export_request.shape,
                    )?;
                    context.ensure_active().map_err(map_context_error)?;
                    if inner.shutdown.is_cancelled() {
                        return Err(JobRuntimeError::Shutdown);
                    }
                    let result = run_export_with_run(
                        inner.snapshot_store.as_ref(),
                        ExportRequest {
                            export_id: export_request.export_id,
                            snapshot_id: snapshot.snapshot_id,
                            format: export_request.format,
                            policy: ExportPolicy {
                                shape: export_request.shape,
                            },
                            destination,
                            created_at: job.queued_at,
                            context: context.clone(),
                        },
                        run.id,
                    )?;
                    context.ensure_active().map_err(map_context_error)?;
                    let content_digest = parse_digest_hex(
                        result.set_digest(),
                        "export set digest is invalid",
                    )?;
                    let artifact_id = inner.identity.next_id();
                    inner.control_plane.create_artifact_ref(ArtifactRefDraft {
                        workspace_id: job.workspace_id,
                        run_id: run.id,
                        artifact_id,
                        artifact_kind: ArtifactKind::ExportArtifact,
                        external_ref_kind: ExternalRefKind::Artifact,
                        external_ref_id: export_request.export_id,
                        content_digest,
                        metadata: serde_json::json!({
                            "artifactType": "export_artifact",
                            "artifactBodyVersion": 1,
                            "exportId": export_request.export_id,
                            "snapshotId": snapshot.snapshot_id,
                            "rowCount": result.row_count(),
                            "byteCount": result.byte_count(),
                            "fileCount": result.files().len(),
                            "manifestVersion": result.manifest_version(),
                            "setDigest": result.set_digest(),
                        }),
                        created_at: at_least(inner, run.started_at),
                    })?;
                    Ok(ExecutionOutcome {
                        snapshot_ref: None,
                        bundle_ref: None,
                        terminal_outputs: Some(vec![TerminalOutputRef::Artifact {
                            workspace_id: job.workspace_id,
                            run_id: run.id,
                            artifact_id,
                            artifact_kind: ArtifactKind::ExportArtifact,
                            artifact_version: 1,
                            content_digest,
                            state: stillflow_core::ArtifactRefState::Committed,
                        }]),
                    })
                }
            },
        }
    })
    .catch_unwind()
    .await;
    match executed {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(JobRuntimeError::WorkerPanic),
    }
}

fn validate_resolved_plan(
    plan: &LogicalPlan,
    expected_digest: [u8; 32],
) -> Result<(), JobRuntimeError> {
    let bytes = plan
        .canonical_bytes()
        .map_err(|_| JobRuntimeError::Invalid("resolved plan canonicalization failed"))?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    if digest != expected_digest {
        return Err(JobRuntimeError::Invalid(
            "resolved plan digest does not match the durable Job",
        ));
    }
    Ok(())
}

fn validate_source_binding(
    job: &JobRecord,
    spec: &JobExecutionSpec,
    source_asset: &stillflow_core::SourceAssetRef,
) -> Result<(), JobRuntimeError> {
    if source_asset.workspace_id != job.workspace_id
        || spec.connection.id() != source_asset.source_connection_id
        || spec.asset.id != source_asset.source_asset_id
        || spec.asset.connection_id != source_asset.source_connection_id
    {
        return Err(JobRuntimeError::Invalid(
            "resolved SourceAsset binding does not match the durable operation",
        ));
    }
    Ok(())
}

fn export_destination(
    destination: stillflow_core::ExportDestinationV1,
    format: stillflow_core::ExportFormat,
    shape: stillflow_core::ExportShape,
) -> Result<ExportDestination, JobRuntimeError> {
    match destination {
        stillflow_core::ExportDestinationV1::Local { root, components } => {
            ExportDestination::local(PathBuf::from(root), components, format, shape)
                .map_err(|_| JobRuntimeError::Invalid("export destination is invalid"))
        }
        stillflow_core::ExportDestinationV1::ObjectStore { prefix } => {
            Ok(ExportDestination::object_store(prefix))
        }
    }
}

fn artifact_output_ref(
    workspace_id: Uuid,
    run_id: Uuid,
    artifact_id: Uuid,
    artifact_kind: ArtifactKind,
    artifact_version: u16,
    content_digest: [u8; 32],
) -> ArtifactOutputRef {
    ArtifactOutputRef {
        workspace_id,
        run_id,
        artifact_id,
        artifact_kind,
        artifact_version,
        content_digest,
        state: stillflow_core::ArtifactRefState::Committed,
    }
}

fn parse_digest_hex(value: &str, error: &'static str) -> Result<[u8; 32], JobRuntimeError> {
    if value.len() != 64 {
        return Err(JobRuntimeError::Invalid(error));
    }
    let bytes = value.as_bytes();
    let mut digest = [0_u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2]).ok_or(JobRuntimeError::Invalid(error))?;
        let low = hex_nibble(bytes[index * 2 + 1]).ok_or(JobRuntimeError::Invalid(error))?;
        *slot = (high << 4) | low;
    }
    Ok(digest)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn digest_hex_bytes(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(64);
    for byte in digest {
        let _ = write!(value, "{byte:02x}");
    }
    value
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
    terminal_outputs: Option<Vec<TerminalOutputRef>>,
) {
    let at = at_least(inner, run.started_at.max(job.queued_at));
    let request_id = format!("job-runtime:terminal:{}", job.id);
    let run_terminal_event = run_event(
        inner,
        run.id,
        job.id,
        event_type_for_run_state(run_state),
        at,
        &request_id,
        serde_json::json!({"state": run_state_text(run_state)}),
    );
    let job_terminal_event = job_event(
        inner,
        job.id,
        event_type_for_job_state(job_state),
        at,
        &request_id,
        serde_json::json!({"state": job_state_text(job_state)}),
    );
    let typed_outputs = terminal_outputs
        .or_else(|| (job.operation.is_some() || run.operation.is_some()).then(Vec::new));
    let result = match typed_outputs {
        Some(outputs) => inner
            .control_plane
            .finish_run_and_job_with_terminal_outputs(
                run.id,
                run_state,
                job_state,
                outputs,
                run_terminal_event,
                job_terminal_event,
                failure,
            ),
        None => inner.control_plane.finish_run_and_job_with_outputs(
            run.id,
            run_state,
            job_state,
            snapshot_ref,
            bundle_ref,
            run_terminal_event,
            job_terminal_event,
            failure,
        ),
    };
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
    let fallback_run_event = run_event(
        inner,
        run.id,
        job.id,
        event_type_for_run_state(fallback_run_state),
        fallback_at,
        &fallback_request_id,
        serde_json::json!({"state": run_state_text(fallback_run_state)}),
    );
    let fallback_job_event = job_event(
        inner,
        job.id,
        event_type_for_job_state(fallback_job_state),
        fallback_at,
        &fallback_request_id,
        serde_json::json!({"state": job_state_text(fallback_job_state)}),
    );
    if job.operation.is_some() || run.operation.is_some() {
        let _ = inner
            .control_plane
            .finish_run_and_job_with_terminal_outputs(
                run.id,
                fallback_run_state,
                fallback_job_state,
                Vec::new(),
                fallback_run_event,
                fallback_job_event,
                fallback_failure,
            );
    } else {
        let _ = inner.control_plane.finish_run_and_job_with_outputs(
            run.id,
            fallback_run_state,
            fallback_job_state,
            None,
            None,
            fallback_run_event,
            fallback_job_event,
            fallback_failure,
        );
    }
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
            plan_id: Uuid::from_u128(4),
            plan_version_id: Uuid::from_u128(4),
            canonical_plan_digest: [0; 32],
            operation: None,
            request_digest: None,
            inputs: Vec::new(),
            execution_policy,
            output_policy: serde_json::json!({}),
            state: JobState::Queued,
            queued_at: DateTime::from_timestamp(1, 0).expect("timestamp"),
            started_at: None,
            finished_at: None,
            run_id: None,
            failure: None,
            outputs: Vec::new(),
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
