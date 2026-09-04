//! Bounded AUT-J1 automation scheduler.
//!
//! This module is deliberately a trigger coordinator. It persists schedule
//! claims, builds an existing E5 `JobSubmission`, and hands that submission to
//! `JobRuntime`; it never executes a plan or owns Job/Run lifecycle state.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use stillflow_storage::{
    AutomationScheduleDraft, AutomationScheduleRecord, AutomationTrigger, ControlPlaneStore,
    JobSubmission, StorageError, SubmitOutcome, DEFAULT_AUTOMATION_CLAIM_LEASE_SECONDS,
};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    JobRuntime, JobRuntimeError, JobRuntimeIdentityProvider, SystemJobRuntimeIdentityProvider,
};

pub const SCHEDULER_WAKE_CAPACITY: usize = 16;
pub const SCHEDULER_MAX_DUE_PER_TICK: usize = 64;
pub const SCHEDULER_DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomationSchedulerConfig {
    pub poll_interval: Duration,
    pub claim_lease: Duration,
    pub max_due_per_tick: usize,
}

impl Default for AutomationSchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: SCHEDULER_DEFAULT_POLL_INTERVAL,
            claim_lease: Duration::from_secs(DEFAULT_AUTOMATION_CLAIM_LEASE_SECONDS),
            max_due_per_tick: SCHEDULER_MAX_DUE_PER_TICK,
        }
    }
}

impl AutomationSchedulerConfig {
    fn validate(self) -> Result<Self, AutomationSchedulerError> {
        if self.poll_interval.is_zero()
            || self.claim_lease.is_zero()
            || self.claim_lease > Duration::from_secs(3_600)
            || self.max_due_per_tick == 0
            || self.max_due_per_tick > SCHEDULER_MAX_DUE_PER_TICK
        {
            return Err(AutomationSchedulerError::Invalid(
                "scheduler bound is outside the supported range",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct E5SubmissionReceipt {
    pub replayed: bool,
}

/// The only execution seam AUT-J1 is allowed to call.
pub trait E5JobSubmitter: Send + Sync {
    fn submit_job(&self, submission: JobSubmission)
        -> Result<E5SubmissionReceipt, JobRuntimeError>;
}

impl E5JobSubmitter for JobRuntime {
    fn submit_job(
        &self,
        submission: JobSubmission,
    ) -> Result<E5SubmissionReceipt, JobRuntimeError> {
        match self.submit_job(submission)? {
            SubmitOutcome::Created(_) => Ok(E5SubmissionReceipt { replayed: false }),
            SubmitOutcome::Replayed(_) => Ok(E5SubmissionReceipt { replayed: true }),
        }
    }
}

/// Converts one claimed trigger into an already-defined E5 Job submission.
/// It must not execute the template or access secret material.
pub trait AutomationJobFactory: Send + Sync {
    fn build(
        &self,
        trigger: &AutomationTrigger,
        idempotency_key: &str,
    ) -> Result<JobSubmission, AutomationSchedulerError>;
}

impl<F> AutomationJobFactory for F
where
    F: Fn(&AutomationTrigger, &str) -> Result<JobSubmission, AutomationSchedulerError>
        + Send
        + Sync
        + 'static,
{
    fn build(
        &self,
        trigger: &AutomationTrigger,
        idempotency_key: &str,
    ) -> Result<JobSubmission, AutomationSchedulerError> {
        self(trigger, idempotency_key)
    }
}

#[derive(Debug, Error)]
pub enum AutomationSchedulerError {
    #[error("automation scheduler persistence failed")]
    Storage(#[source] StorageError),
    #[error("automation scheduler E5 submission failed")]
    JobRuntime(#[source] JobRuntimeError),
    #[error("invalid automation scheduler input: {0}")]
    Invalid(&'static str),
    #[error("automation scheduler is shutting down")]
    Shutdown,
}

impl From<StorageError> for AutomationSchedulerError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<JobRuntimeError> for AutomationSchedulerError {
    fn from(error: JobRuntimeError) -> Self {
        Self::JobRuntime(error)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AutomationTickReport {
    pub due_schedules: usize,
    pub claimed: usize,
    pub submitted: usize,
    pub replayed: usize,
    pub failed: usize,
}

struct SchedulerInner {
    workspace_id: Uuid,
    control_plane: Arc<ControlPlaneStore>,
    submitter: Arc<dyn E5JobSubmitter>,
    factory: Arc<dyn AutomationJobFactory>,
    identity: Arc<dyn JobRuntimeIdentityProvider>,
    config: AutomationSchedulerConfig,
    wake_tx: mpsc::Sender<()>,
    wake_rx: Mutex<mpsc::Receiver<()>>,
    shutdown: CancellationToken,
    last_observed_at: Mutex<Option<DateTime<Utc>>>,
}

pub struct AutomationScheduler {
    inner: Arc<SchedulerInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl AutomationScheduler {
    pub fn new(
        workspace_id: Uuid,
        control_plane: Arc<ControlPlaneStore>,
        submitter: Arc<dyn E5JobSubmitter>,
        factory: Arc<dyn AutomationJobFactory>,
        identity: Arc<dyn JobRuntimeIdentityProvider>,
        config: AutomationSchedulerConfig,
    ) -> Result<Self, AutomationSchedulerError> {
        if workspace_id.is_nil() {
            return Err(AutomationSchedulerError::Invalid(
                "scheduler workspace id must not be nil",
            ));
        }
        let config = config.validate()?;
        let (wake_tx, wake_rx) = mpsc::channel(SCHEDULER_WAKE_CAPACITY);
        Ok(Self {
            inner: Arc::new(SchedulerInner {
                workspace_id,
                control_plane,
                submitter,
                factory,
                identity,
                config,
                wake_tx,
                wake_rx: Mutex::new(wake_rx),
                shutdown: CancellationToken::new(),
                last_observed_at: Mutex::new(None),
            }),
            worker: Mutex::new(None),
        })
    }

    pub fn new_with_job_runtime(
        workspace_id: Uuid,
        control_plane: Arc<ControlPlaneStore>,
        job_runtime: Arc<JobRuntime>,
        factory: Arc<dyn AutomationJobFactory>,
        config: AutomationSchedulerConfig,
    ) -> Result<Self, AutomationSchedulerError> {
        Self::new(
            workspace_id,
            control_plane,
            job_runtime,
            factory,
            Arc::new(SystemJobRuntimeIdentityProvider),
            config,
        )
    }

    pub async fn start(&self) -> Result<(), AutomationSchedulerError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(AutomationSchedulerError::Shutdown);
        }
        let mut worker = self.worker.lock().await;
        if worker.is_none() {
            let inner = Arc::clone(&self.inner);
            *worker = Some(tokio::spawn(scheduler_loop(inner)));
        }
        drop(worker);
        self.wake();
        Ok(())
    }

    pub fn wake(&self) {
        let _ = self.inner.wake_tx.try_send(());
    }

    /// Processes at most `max_due_per_tick` schedules. A backward wall-clock
    /// jump is clamped to the last observed instant, so it cannot duplicate a
    /// trigger or scan an unbounded historical interval.
    pub async fn tick_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<AutomationTickReport, AutomationSchedulerError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(AutomationSchedulerError::Shutdown);
        }
        tick_inner(&self.inner, now).await
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let mut worker = self.worker.lock().await;
        let handle = worker.take();
        drop(worker);
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

async fn scheduler_loop(inner: Arc<SchedulerInner>) {
    loop {
        tokio::select! {
            _ = inner.shutdown.cancelled() => break,
            _ = async {
                let mut receiver = inner.wake_rx.lock().await;
                let _ = receiver.recv().await;
            } => {},
            _ = time::sleep(inner.config.poll_interval) => {},
        }
        if inner.shutdown.is_cancelled() {
            break;
        }
        let now = inner.identity.now();
        let _ = tick_inner(&inner, now).await;
    }
}

async fn tick_inner(
    inner: &SchedulerInner,
    now: DateTime<Utc>,
) -> Result<AutomationTickReport, AutomationSchedulerError> {
    let effective_now = {
        let mut last_observed = inner.last_observed_at.lock().await;
        let effective = last_observed.map_or(now, |last| last.max(now));
        *last_observed = Some(effective);
        effective
    };
    let due = inner.control_plane.list_due_automation_schedule_ids(
        inner.workspace_id,
        effective_now,
        inner.config.max_due_per_tick,
    )?;
    let mut report = AutomationTickReport {
        due_schedules: due.len(),
        ..AutomationTickReport::default()
    };
    let lease_seconds = inner.config.claim_lease.as_secs();
    for schedule_id in due {
        let trigger = match inner.control_plane.claim_due_automation_schedule(
            schedule_id,
            effective_now,
            inner.identity.next_id(),
            lease_seconds,
        ) {
            Ok(Some(trigger)) => trigger,
            Ok(None) => continue,
            Err(StorageError::Busy(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        report.claimed += 1;
        let idempotency_key = occurrence_idempotency_key(&trigger);
        let submission = match inner.factory.build(&trigger, &idempotency_key) {
            Ok(submission) => submission,
            Err(_) => {
                inner.control_plane.fail_automation_trigger(
                    &trigger,
                    "automation could not build an E5 Job submission",
                    effective_now,
                )?;
                report.failed += 1;
                continue;
            }
        };
        if submission.idempotency_key != idempotency_key
            || submission.workspace_id != trigger.workspace_id
        {
            inner.control_plane.fail_automation_trigger(
                &trigger,
                "automation factory returned an invalid E5 Job submission",
                effective_now,
            )?;
            report.failed += 1;
            continue;
        }
        let receipt = match inner.submitter.submit_job(submission) {
            Ok(receipt) => receipt,
            Err(_) => {
                inner.control_plane.fail_automation_trigger(
                    &trigger,
                    "E5 Job submission failed",
                    effective_now,
                )?;
                report.failed += 1;
                continue;
            }
        };
        let next_run_at = trigger
            .schedule
            .next_after(trigger.occurrence_at, &trigger.timezone)
            .map_err(|_| AutomationSchedulerError::Invalid("automation next run is invalid"))?;
        inner
            .control_plane
            .acknowledge_automation_trigger(&trigger, next_run_at, effective_now)?;
        report.submitted += 1;
        if receipt.replayed {
            report.replayed += 1;
        }
    }
    Ok(report)
}

pub fn occurrence_idempotency_key(trigger: &AutomationTrigger) -> String {
    format!(
        "automation:{}:{}",
        trigger.schedule_id,
        trigger
            .occurrence_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
    )
}

pub fn next_run_for_record(
    record: &AutomationScheduleRecord,
) -> Result<Option<DateTime<Utc>>, AutomationSchedulerError> {
    record
        .next_run_at
        .map(|current| {
            record
                .schedule
                .next_after(current, &record.timezone)
                .map_err(|_| AutomationSchedulerError::Invalid("automation next run is invalid"))
        })
        .transpose()
}

pub fn create_schedule(
    control_plane: &ControlPlaneStore,
    draft: AutomationScheduleDraft,
) -> Result<AutomationScheduleRecord, AutomationSchedulerError> {
    Ok(control_plane.create_automation_schedule(draft)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("UTC timestamp")
    }

    struct CountingSubmitter {
        calls: Arc<AtomicUsize>,
    }

    impl E5JobSubmitter for CountingSubmitter {
        fn submit_job(
            &self,
            _submission: JobSubmission,
        ) -> Result<E5SubmissionReceipt, JobRuntimeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(E5SubmissionReceipt { replayed: false })
        }
    }

    fn draft(id: Uuid, workspace_id: Uuid) -> AutomationScheduleDraft {
        AutomationScheduleDraft {
            id,
            workspace_id,
            schedule: stillflow_core::AutomationSchedule::Interval { period_seconds: 60 },
            timezone: "UTC".to_owned(),
            template: json!({"plan_version_id": Uuid::new_v4()}),
            first_run_at: at(100),
            max_submission_attempts: 3,
            created_at: at(0),
        }
    }

    #[tokio::test]
    async fn scheduler_submits_only_existing_e5_job_with_bounded_tick() {
        let root = tempdir().expect("temporary storage root");
        let control_plane = Arc::new(ControlPlaneStore::open(root.path()).expect("open storage"));
        let workspace_id = Uuid::new_v4();
        control_plane
            .create_workspace(workspace_id, at(0))
            .expect("create workspace");
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        control_plane
            .create_automation_schedule(draft(first_id, workspace_id))
            .expect("create first schedule");
        control_plane
            .create_automation_schedule(draft(second_id, workspace_id))
            .expect("create second schedule");

        let calls = Arc::new(AtomicUsize::new(0));
        let submitter = Arc::new(CountingSubmitter {
            calls: Arc::clone(&calls),
        });
        let factory: Arc<dyn AutomationJobFactory> = Arc::new(
            |trigger: &AutomationTrigger,
             idempotency_key: &str|
             -> Result<JobSubmission, AutomationSchedulerError> {
                Ok(JobSubmission {
                    workspace_id: trigger.workspace_id,
                    session_id: Uuid::new_v4(),
                    plan_id: None,
                    plan_version_id: Uuid::new_v4(),
                    canonical_plan_digest: [0; 32],
                    operation: None,
                    job_id: Uuid::new_v4(),
                    idempotency_key: idempotency_key.to_owned(),
                    inputs: Vec::new(),
                    execution_policy: json!({}),
                    output_policy: json!({}),
                    request_digest: [0; 32],
                    queued_at: trigger.occurrence_at,
                    event_id: Uuid::new_v4(),
                    request_id: "aut-j1-test".to_owned(),
                    correlation_id: "aut-j1-test".to_owned(),
                    actor_ref: "aut-j1-test".to_owned(),
                })
            },
        );
        let scheduler = AutomationScheduler::new(
            workspace_id,
            Arc::clone(&control_plane),
            submitter,
            factory,
            Arc::new(SystemJobRuntimeIdentityProvider),
            AutomationSchedulerConfig {
                poll_interval: Duration::from_secs(1),
                claim_lease: Duration::from_secs(60),
                max_due_per_tick: 1,
            },
        )
        .expect("create scheduler");

        let first_report = scheduler.tick_at(at(100)).await.expect("first tick");
        assert_eq!(first_report.due_schedules, 1);
        assert_eq!(first_report.claimed, 1);
        assert_eq!(first_report.submitted, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let first_record = control_plane
            .get_automation_schedule(first_id)
            .expect("read first schedule");
        assert_eq!(first_record.next_run_at, Some(at(160)));

        let second_report = scheduler.tick_at(at(100)).await.expect("second tick");
        assert_eq!(second_report.due_schedules, 1);
        assert_eq!(second_report.claimed, 1);
        assert_eq!(second_report.submitted, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let replay_guard_report = scheduler.tick_at(at(100)).await.expect("replay guard tick");
        assert_eq!(replay_guard_report.due_schedules, 0);
        assert_eq!(replay_guard_report.claimed, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
