#![cfg(feature = "event-stream")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::json;
use stillflow_api::event_stream::{
    EventStreamError, EventStreamService, ReplayRequest, ResumeCursor, StreamKey, MAX_REPLAY_EVENTS,
};
use stillflow_core::{ControlPlaneEventType, EventStreamKind, JobState, RunState};
use stillflow_storage::{
    ContentDigest, ControlPlaneStore, EventDraft, JobRecord, JobSubmission, PlanVersionDraft,
    SubmitOutcome,
};
use uuid::Uuid;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid timestamp")
}

struct Fixture {
    root: PathBuf,
    store: ControlPlaneStore,
    workspace_id: Uuid,
    session_id: Uuid,
    plan_version_id: Uuid,
    plan_digest: [u8; 32],
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("stillflow-e5-e1-{}", Uuid::new_v4()));
        let store = ControlPlaneStore::open(&root).expect("open control plane");
        let workspace_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let plan_id = Uuid::new_v4();
        let plan_version_id = Uuid::new_v4();
        let plan_digest = *ContentDigest::try_from_hex(
            "080d877cf7c6461f423daf6d39644d938dff4f633976edf7f2b88f32ad4325e3",
        )
        .expect("SHA-256 fixture")
        .as_bytes();
        store
            .create_workspace(workspace_id, at(1))
            .expect("workspace");
        store
            .create_session(workspace_id, session_id, at(2))
            .expect("session");
        store
            .create_plan(workspace_id, plan_id, at(3))
            .expect("plan");
        store
            .create_plan_version(PlanVersionDraft {
                workspace_id,
                plan_id,
                plan_version_id,
                version_number: 1,
                parent_version_id: None,
                logical_plan: json!({"version": 1}),
                canonical_plan_bytes: b"canonical-plan-v1".to_vec(),
                canonical_plan_digest: plan_digest,
                plan_fingerprint: [8; 32],
                created_at: at(4),
            })
            .expect("PlanVersion");
        store
            .publish_plan_version(plan_version_id, None, at(5))
            .expect("publish PlanVersion");
        Self {
            root,
            store,
            workspace_id,
            session_id,
            plan_version_id,
            plan_digest,
        }
    }

    fn submit(&self) -> JobRecord {
        let job_id = Uuid::new_v4();
        let submission = JobSubmission::try_new(
            self.workspace_id,
            self.session_id,
            self.plan_version_id,
            self.plan_digest,
            job_id,
            format!("job-key-{job_id}"),
            Vec::new(),
            json!({"deadlineSeconds": 900}),
            json!({"kind": "verificationBundle"}),
            at(10),
            Uuid::new_v4(),
            format!("request-{job_id}"),
            format!("correlation-{job_id}"),
            "actor:test",
        )
        .expect("submission");
        match self.store.submit_job(submission).expect("submit Job") {
            SubmitOutcome::Created(job) | SubmitOutcome::Replayed(job) => job,
        }
    }

    fn claim(&self, job: &JobRecord) -> stillflow_storage::RunRecord {
        let run_id = Uuid::new_v4();
        self.store
            .claim_job(
                job.id,
                run_id,
                at(20),
                1,
                "engine-test",
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobRunning,
                    at(20),
                    "request-claim",
                    "correlation-claim",
                    "actor:test",
                    json!({"state": "running"}),
                ),
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Run,
                    run_id,
                    job.id,
                    Some(run_id),
                    ControlPlaneEventType::RunRunning,
                    at(20),
                    "request-claim",
                    "correlation-claim",
                    "actor:test",
                    json!({"state": "running"}),
                ),
            )
            .expect("claim Job")
    }

    fn run_key(&self, run_id: Uuid) -> StreamKey {
        StreamKey::new(self.workspace_id, EventStreamKind::Run, run_id)
    }

    fn job_key(&self, job_id: Uuid) -> StreamKey {
        StreamKey::new(self.workspace_id, EventStreamKind::Job, job_id)
    }
}

#[test]
fn replay_orders_by_durable_sequence_and_resumes_without_skipping() {
    let fixture = Fixture::new();
    let job = fixture.submit();
    let _run = fixture.claim(&job);
    let service = EventStreamService::new(Arc::new(fixture.store.clone()));
    let key = fixture.job_key(job.id);

    let first = service
        .replay(ReplayRequest {
            key,
            cursor: None,
            limit: 1,
        })
        .expect("first page");
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1]
    );
    let cursor = first.events[0].cursor();
    assert!(first.next.is_some());

    let resumed = service
        .replay(ReplayRequest {
            key,
            cursor: Some(cursor),
            limit: MAX_REPLAY_EVENTS,
        })
        .expect("resumed page");
    assert_eq!(
        resumed
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [2]
    );
    assert!(resumed.events[0].occurred_at >= first.events[0].occurred_at);
}

#[test]
fn cursor_scope_and_replay_bounds_fail_closed() {
    let fixture = Fixture::new();
    let job = fixture.submit();
    let service = EventStreamService::new(Arc::new(fixture.store.clone()));
    let key = fixture.job_key(job.id);

    let foreign = ResumeCursor::new(
        StreamKey::new(Uuid::new_v4(), EventStreamKind::Job, job.id),
        1,
    );
    assert_eq!(
        service
            .replay(ReplayRequest {
                key,
                cursor: Some(foreign),
                limit: 1,
            })
            .expect_err("foreign cursor"),
        EventStreamError::InvalidCursor
    );
    assert_eq!(
        service
            .replay(ReplayRequest {
                key,
                cursor: None,
                limit: MAX_REPLAY_EVENTS + 1,
            })
            .expect_err("oversized replay"),
        EventStreamError::InvalidRequest
    );
    assert_eq!(
        service
            .replay(ReplayRequest {
                key: StreamKey::new(Uuid::new_v4(), EventStreamKind::Job, job.id),
                cursor: None,
                limit: 1,
            })
            .expect_err("foreign workspace stream"),
        EventStreamError::StreamNotFound
    );
}

#[test]
fn replay_is_bounded_to_one_thousand_events_and_paginates_durable_history() {
    let fixture = Fixture::new();
    let job = fixture.submit();
    let run = fixture.claim(&job);
    for ordinal in 0..1_001 {
        fixture
            .store
            .append_event(EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Run,
                run.id,
                job.id,
                Some(run.id),
                ControlPlaneEventType::RunReconciled,
                at(100 + ordinal),
                format!("request-reconcile-{ordinal}"),
                format!("correlation-reconcile-{ordinal}"),
                "actor:test",
                json!({"ordinal": ordinal}),
            ))
            .expect("append durable reconciliation event");
    }
    let service = EventStreamService::new(Arc::new(fixture.store.clone()));
    let key = fixture.run_key(run.id);
    let first = service
        .replay(ReplayRequest {
            key,
            cursor: None,
            limit: MAX_REPLAY_EVENTS,
        })
        .expect("bounded first page");
    assert_eq!(first.events.len(), 1_000);
    assert_eq!(first.events[0].sequence, 1);
    assert_eq!(first.events[999].sequence, 1_000);
    assert_eq!(first.next.expect("next page").sequence, 1_000);

    let second = service
        .replay(ReplayRequest {
            key,
            cursor: first.next,
            limit: MAX_REPLAY_EVENTS,
        })
        .expect("bounded second page");
    assert_eq!(second.events.len(), 2);
    assert_eq!(second.events[0].sequence, 1_001);
    assert_eq!(second.events[1].sequence, 1_002);
}

#[tokio::test]
async fn subscriber_limit_and_slow_consumer_policy_are_bounded() {
    let fixture = Fixture::new();
    let job = fixture.submit();
    let run = fixture.claim(&job);
    let service = EventStreamService::with_bounds(Arc::new(fixture.store.clone()), 1, 1)
        .expect("bounded service");

    let held = service
        .subscribe(ReplayRequest {
            key: fixture.job_key(job.id),
            cursor: None,
            limit: 1,
        })
        .await
        .expect("first subscriber");
    assert_eq!(
        service
            .subscribe(ReplayRequest {
                key: fixture.job_key(job.id),
                cursor: None,
                limit: 1,
            })
            .await
            .expect_err("subscriber cap"),
        EventStreamError::SubscriberLimit
    );
    drop(held);

    let mut subscriber = service
        .subscribe(ReplayRequest {
            key: fixture.run_key(run.id),
            cursor: Some(ResumeCursor::new(fixture.run_key(run.id), 1)),
            limit: 1,
        })
        .await
        .expect("run subscriber");
    for ordinal in 0..3 {
        fixture
            .store
            .append_event(EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Run,
                run.id,
                job.id,
                Some(run.id),
                ControlPlaneEventType::RunReconciled,
                at(200 + ordinal),
                format!("request-slow-{ordinal}"),
                format!("correlation-slow-{ordinal}"),
                "actor:test",
                json!({"ordinal": ordinal}),
            ))
            .expect("append slow-consumer event");
    }
    let first = tokio::time::timeout(Duration::from_secs(2), subscriber.next_event())
        .await
        .expect("first event timeout")
        .expect("first event result")
        .expect("first event success");
    assert_eq!(first.sequence, 2);
    let error = tokio::time::timeout(Duration::from_secs(2), subscriber.next_event())
        .await
        .expect("slow error timeout")
        .expect("slow error result")
        .expect_err("slow consumer must be disconnected");
    assert_eq!(error, EventStreamError::SlowConsumer);
}

#[test]
fn terminal_frames_match_durable_job_and_run_state_after_restart() {
    let (root, workspace_id, job_id, run_id) = {
        let fixture = Fixture::new();
        let job = fixture.submit();
        let run = fixture.claim(&job);
        fixture
            .store
            .finish_run_and_job(
                run.id,
                RunState::Succeeded,
                JobState::Succeeded,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Run,
                    run.id,
                    job.id,
                    Some(run.id),
                    ControlPlaneEventType::RunSucceeded,
                    at(30),
                    "request-finish",
                    "correlation-finish",
                    "actor:test",
                    json!({"state": "succeeded"}),
                ),
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobSucceeded,
                    at(30),
                    "request-finish",
                    "correlation-finish",
                    "actor:test",
                    json!({"state": "succeeded"}),
                ),
                None,
            )
            .expect("finish Job and Run");
        (fixture.root.clone(), fixture.workspace_id, job.id, run.id)
    };

    let reopened = ControlPlaneStore::open(&root).expect("reopen storage");
    let service = EventStreamService::new(Arc::new(reopened.clone()));
    let job_page = service
        .replay(ReplayRequest {
            key: StreamKey::new(workspace_id, EventStreamKind::Job, job_id),
            cursor: None,
            limit: MAX_REPLAY_EVENTS,
        })
        .expect("replayed Job events");
    let run_page = service
        .replay(ReplayRequest {
            key: StreamKey::new(workspace_id, EventStreamKind::Run, run_id),
            cursor: None,
            limit: MAX_REPLAY_EVENTS,
        })
        .expect("replayed Run events");
    assert_eq!(
        job_page
            .events
            .last()
            .expect("Job terminal event")
            .event_type,
        ControlPlaneEventType::JobSucceeded
    );
    assert_eq!(
        run_page
            .events
            .last()
            .expect("Run terminal event")
            .event_type,
        ControlPlaneEventType::RunSucceeded
    );
    assert_eq!(
        reopened.get_job(job_id).expect("Job").state,
        JobState::Succeeded
    );
    assert_eq!(
        reopened.get_run(run_id).expect("Run").state,
        RunState::Succeeded
    );
    drop(service);
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}
