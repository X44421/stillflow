// Durable, bounded, SSE-ready event stream representation for E5-E1.
//
// Storage owns event identity and ordering. This module only validates the
// stream scope, projects already-sanitized metadata, replays a bounded page,
// and polls durable storage for subsequent events. It deliberately keeps no
// event history of its own.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore};
use uuid::Uuid;

use stillflow_core::{
    ensure_safe_event_metadata, ControlPlaneEventType, EventStreamKind, JobState, RunState,
    MAX_EVENT_PAGE_SIZE, MAX_EVENT_PAYLOAD_BYTES,
};
use stillflow_storage::{ControlPlaneStore, EventCursor, EventRecord, StorageError};

use crate::ApiLimits;

pub const MAX_REPLAY_EVENTS: usize = MAX_EVENT_PAGE_SIZE;
pub const DEFAULT_SUBSCRIBER_LIMIT: usize = ApiLimits::DEFAULT.max_concurrent_requests;
pub const DEFAULT_SUBSCRIBER_BUFFER: usize = 64;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const PUMP_PAGE_SIZE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamKey {
    pub workspace_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
}

impl StreamKey {
    pub const fn new(
        workspace_id: Uuid,
        stream_kind: EventStreamKind,
        stream_id: Uuid,
    ) -> Self {
        Self {
            workspace_id,
            stream_kind,
            stream_id,
        }
    }

    fn validate(self) -> Result<Self, EventStreamError> {
        if self.workspace_id.is_nil() || self.stream_id.is_nil() {
            return Err(EventStreamError::InvalidRequest);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeCursor {
    pub workspace_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub sequence: u64,
}

impl ResumeCursor {
    pub const fn new(key: StreamKey, sequence: u64) -> Self {
        Self {
            workspace_id: key.workspace_id,
            stream_kind: key.stream_kind,
            stream_id: key.stream_id,
            sequence,
        }
    }

    pub const fn key(self) -> StreamKey {
        StreamKey::new(self.workspace_id, self.stream_kind, self.stream_id)
    }

    fn to_storage(self) -> EventCursor {
        EventCursor {
            workspace_id: self.workspace_id,
            stream_kind: self.stream_kind,
            stream_id: self.stream_id,
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventFrame {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub sequence: u64,
    pub event_type: ControlPlaneEventType,
    pub event_version: u16,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub job_id: Uuid,
    pub run_id: Option<Uuid>,
    pub payload: Value,
}

impl EventFrame {
    pub const fn cursor(&self) -> ResumeCursor {
        ResumeCursor::new(
            StreamKey::new(self.workspace_id, self.stream_kind, self.stream_id),
            self.sequence,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayRequest {
    pub key: StreamKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ResumeCursor>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayPage {
    pub events: Vec<EventFrame>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<ResumeCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventStreamError {
    #[error("invalid event stream request")]
    InvalidRequest,
    #[error("event stream cursor is bound to another workspace or stream")]
    InvalidCursor,
    #[error("event stream does not identify a readable Job or Run")]
    StreamNotFound,
    #[error("event stream subscriber limit reached")]
    SubscriberLimit,
    #[error("event stream slow consumer disconnected at the bounded buffer")]
    SlowConsumer,
    #[error("durable terminal state does not match its terminal event")]
    TerminalStateMismatch,
    #[error("event payload is unsafe or exceeds the 64 KiB bound")]
    UnsafePayload,
    #[error("durable event state is unavailable")]
    DurableStateUnavailable,
}

#[derive(Debug, Clone)]
pub struct EventStreamService {
    store: Arc<ControlPlaneStore>,
    subscriber_slots: Arc<Semaphore>,
    buffer_capacity: usize,
    poll_interval: Duration,
}

impl EventStreamService {
    pub fn new(store: Arc<ControlPlaneStore>) -> Self {
        Self::with_bounds(store, DEFAULT_SUBSCRIBER_LIMIT, DEFAULT_SUBSCRIBER_BUFFER)
            .expect("default event stream bounds are valid")
    }

    pub fn with_bounds(
        store: Arc<ControlPlaneStore>,
        max_subscribers: usize,
        buffer_capacity: usize,
    ) -> Result<Self, EventStreamError> {
        if max_subscribers == 0 || buffer_capacity == 0 {
            return Err(EventStreamError::InvalidRequest);
        }
        Ok(Self {
            store,
            subscriber_slots: Arc::new(Semaphore::new(max_subscribers)),
            buffer_capacity,
            poll_interval: DEFAULT_POLL_INTERVAL,
        })
    }

    pub fn replay(&self, request: ReplayRequest) -> Result<ReplayPage, EventStreamError> {
        let key = request.key.validate()?;
        if request.limit == 0 || request.limit > MAX_REPLAY_EVENTS {
            return Err(EventStreamError::InvalidRequest);
        }
        if let Some(cursor) = request.cursor {
            if cursor.key() != key {
                return Err(EventStreamError::InvalidCursor);
            }
        }
        self.validate_stream_exists(key)?;
        let page = self
            .store
            .list_events(
                key.workspace_id,
                key.stream_kind,
                key.stream_id,
                request.cursor.map(ResumeCursor::to_storage),
                request.limit,
            )
            .map_err(map_storage_error)?;
        let events = page
            .events
            .into_iter()
            .map(|event| self.frame(key, event))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ReplayPage {
            events,
            next: page.next.map(|cursor| {
                ResumeCursor::new(
                    StreamKey::new(cursor.workspace_id, cursor.stream_kind, cursor.stream_id),
                    cursor.sequence,
                )
            }),
        })
    }

    pub async fn subscribe(
        &self,
        request: ReplayRequest,
    ) -> Result<EventSubscription, EventStreamError> {
        let permit = self
            .subscriber_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| EventStreamError::SubscriberLimit)?;
        let key = request.key.validate()?;
        let observed_cursor = request
            .cursor
            .unwrap_or_else(|| ResumeCursor::new(key, 0));
        let page = match self.replay(request) {
            Ok(page) => page,
            Err(error) => {
                drop(permit);
                return Err(error);
            }
        };
        let start_cursor = page
            .events
            .last()
            .map(EventFrame::cursor)
            .or_else(|| page.next)
            .unwrap_or(observed_cursor);
        let replay = VecDeque::from(page.events);
        let (sender, receiver) = mpsc::channel(self.buffer_capacity);
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_error = Arc::new(Mutex::new(None));
        let worker_cancelled = Arc::clone(&cancelled);
        let worker_error_slot = Arc::clone(&worker_error);
        let store = Arc::clone(&self.store);
        let poll_interval = self.poll_interval;
        tokio::spawn(async move {
            pump_events(
                store,
                key,
                start_cursor,
                sender,
                worker_cancelled,
                worker_error_slot,
                poll_interval,
            )
            .await;
        });
        Ok(EventSubscription {
            key,
            cursor: Some(observed_cursor),
            replay,
            receiver,
            cancelled,
            worker_error,
            _permit: Some(permit),
        })
    }

    fn validate_stream_exists(&self, key: StreamKey) -> Result<(), EventStreamError> {
        let workspace_matches = match key.stream_kind {
            EventStreamKind::Job => self
                .store
                .get_job(key.stream_id)
                .map(|job| job.workspace_id == key.workspace_id),
            EventStreamKind::Run => self
                .store
                .get_run(key.stream_id)
                .map(|run| run.workspace_id == key.workspace_id),
        };
        match workspace_matches {
            Ok(true) => Ok(()),
            Ok(false) | Err(StorageError::NotFound(_)) => Err(EventStreamError::StreamNotFound),
            Err(_) => Err(EventStreamError::DurableStateUnavailable),
        }
    }

    fn frame(&self, key: StreamKey, event: EventRecord) -> Result<EventFrame, EventStreamError> {
        frame_from_store(&self.store, key, event)
    }
}

pub struct EventSubscription {
    key: StreamKey,
    cursor: Option<ResumeCursor>,
    replay: VecDeque<EventFrame>,
    receiver: mpsc::Receiver<Result<EventFrame, EventStreamError>>,
    cancelled: Arc<AtomicBool>,
    worker_error: Arc<Mutex<Option<EventStreamError>>>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl std::fmt::Debug for EventSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventSubscription")
            .field("key", &self.key)
            .field("cursor", &self.cursor)
            .field("replay_len", &self.replay.len())
            .finish_non_exhaustive()
    }
}

impl EventSubscription {
    pub const fn key(&self) -> StreamKey {
        self.key
    }

    pub const fn cursor(&self) -> Option<ResumeCursor> {
        self.cursor
    }

    pub async fn next_event(&mut self) -> Option<Result<EventFrame, EventStreamError>> {
        if let Some(event) = self.replay.pop_front() {
            self.cursor = Some(event.cursor());
            return Some(Ok(event));
        }
        match self.receiver.recv().await {
            Some(Ok(event)) => {
                self.cursor = Some(event.cursor());
                Some(Ok(event))
            }
            Some(Err(error)) => Some(Err(error)),
            None => self
                .worker_error
                .lock()
                .ok()
                .and_then(|mut error| error.take())
                .map(Err),
        }
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

async fn pump_events(
    store: Arc<ControlPlaneStore>,
    key: StreamKey,
    mut cursor: ResumeCursor,
    sender: mpsc::Sender<Result<EventFrame, EventStreamError>>,
    cancelled: Arc<AtomicBool>,
    worker_error: Arc<Mutex<Option<EventStreamError>>>,
    poll_interval: Duration,
) {
    loop {
        if cancelled.load(Ordering::Acquire) {
            return;
        }
        let page = match store.list_events(
            key.workspace_id,
            key.stream_kind,
            key.stream_id,
            Some(cursor.to_storage()),
            PUMP_PAGE_SIZE,
        ) {
            Ok(page) => page,
            Err(error) => {
                set_worker_error(&worker_error, map_storage_error(error));
                return;
            }
        };
        if page.events.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }
        for event in page.events {
            if cancelled.load(Ordering::Acquire) {
                return;
            }
            let frame = match frame_from_store(&store, key, event) {
                Ok(frame) => frame,
                Err(error) => {
                    set_worker_error(&worker_error, error);
                    return;
                }
            };
            cursor = frame.cursor();
            match sender.try_send(Ok(frame)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    set_worker_error(&worker_error, EventStreamError::SlowConsumer);
                    return;
                }
            }
        }
    }
}

fn frame_from_store(
    store: &ControlPlaneStore,
    key: StreamKey,
    event: EventRecord,
) -> Result<EventFrame, EventStreamError> {
    if event.workspace_id != key.workspace_id
        || event.stream_kind != key.stream_kind
        || event.stream_id != key.stream_id
    {
        return Err(EventStreamError::DurableStateUnavailable);
    }
    let payload_bytes = serde_json::to_vec(&event.payload)
        .map_err(|_| EventStreamError::UnsafePayload)?;
    if payload_bytes.len() > MAX_EVENT_PAYLOAD_BYTES
        || ensure_safe_event_metadata(&event.payload).is_err()
    {
        return Err(EventStreamError::UnsafePayload);
    }
    match (event.stream_kind, event.event_type) {
        (EventStreamKind::Job, ControlPlaneEventType::JobSucceeded)
        | (EventStreamKind::Job, ControlPlaneEventType::JobFailed)
        | (EventStreamKind::Job, ControlPlaneEventType::JobCancelled) => {
            let job = store
                .get_job(event.job_id)
                .map_err(|_| EventStreamError::DurableStateUnavailable)?;
            let expected = match event.event_type {
                ControlPlaneEventType::JobSucceeded => JobState::Succeeded,
                ControlPlaneEventType::JobFailed => JobState::Failed,
                ControlPlaneEventType::JobCancelled => JobState::Cancelled,
                _ => unreachable!(),
            };
            if job.state != expected {
                return Err(EventStreamError::TerminalStateMismatch);
            }
        }
        (EventStreamKind::Run, ControlPlaneEventType::RunSucceeded)
        | (EventStreamKind::Run, ControlPlaneEventType::RunFailed)
        | (EventStreamKind::Run, ControlPlaneEventType::RunCancelled) => {
            let run_id = event.run_id.ok_or(EventStreamError::DurableStateUnavailable)?;
            let run = store
                .get_run(run_id)
                .map_err(|_| EventStreamError::DurableStateUnavailable)?;
            let expected = match event.event_type {
                ControlPlaneEventType::RunSucceeded => RunState::Succeeded,
                ControlPlaneEventType::RunFailed => RunState::Failed,
                ControlPlaneEventType::RunCancelled => RunState::Cancelled,
                _ => unreachable!(),
            };
            if run.state != expected {
                return Err(EventStreamError::TerminalStateMismatch);
            }
        }
        _ => {}
    }
    Ok(EventFrame {
        event_id: event.event_id,
        workspace_id: event.workspace_id,
        session_id: event.session_id,
        stream_kind: event.stream_kind,
        stream_id: event.stream_id,
        sequence: event.sequence,
        event_type: event.event_type,
        event_version: event.event_version,
        occurred_at: event.occurred_at,
        job_id: event.job_id,
        run_id: event.run_id,
        payload: event.payload,
    })
}

fn set_worker_error(slot: &Mutex<Option<EventStreamError>>, error: EventStreamError) {
    if let Ok(mut current) = slot.lock() {
        *current = Some(error);
    }
}

fn map_storage_error(error: StorageError) -> EventStreamError {
    match error {
        StorageError::InvalidDraft(_) => EventStreamError::InvalidCursor,
        StorageError::NotFound(_) => EventStreamError::StreamNotFound,
        _ => EventStreamError::DurableStateUnavailable,
    }
}
