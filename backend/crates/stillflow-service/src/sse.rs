//! SSE transport extension `GET /v1/events/stream` (contract §3.4). Frames are
//! `EventFrame` JSON payloads projected from the durable log by
//! `EventStreamService`; this endpoint adds no second log. Pre-stream failures
//! map through the §3.2 table; a `SlowConsumer` after the stream opens
//! terminates the stream and clients recover by cursor replay.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{RawQuery, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use uuid::Uuid;

use stillflow_api::event_stream::{EventStreamError, ReplayRequest, ResumeCursor, StreamKey};
use stillflow_api::ApiError;
use stillflow_core::EventStreamKind;

use crate::adapter::{error_response, parse_query};
use crate::routes::ServiceState;

fn stream_error_to_api_error(error: EventStreamError) -> ApiError {
    match error {
        EventStreamError::InvalidRequest | EventStreamError::InvalidCursor => {
            ApiError::invalid("event stream request is invalid")
        }
        EventStreamError::StreamNotFound => ApiError::not_found(),
        EventStreamError::SubscriberLimit => ApiError::limit("event stream subscriber limit"),
        EventStreamError::SlowConsumer | EventStreamError::TerminalStateMismatch => {
            ApiError::conflict("event stream subscription terminated")
        }
        EventStreamError::UnsafePayload | EventStreamError::DurableStateUnavailable => {
            ApiError::internal()
        }
    }
}

pub async fn events_stream(
    State(state): State<ServiceState>,
    RawQuery(query): RawQuery,
) -> Response {
    let mut workspace_id: Option<Uuid> = None;
    let mut stream_id: Option<Uuid> = None;
    let mut kind: Option<EventStreamKind> = None;
    let mut cursor: Option<u64> = None;
    let mut limit: Option<usize> = None;
    let mut request_id = Uuid::new_v4();
    if let Some(query) = query.as_deref() {
        for (key, value) in parse_query(query) {
            match key.as_str() {
                "workspaceId" => workspace_id = Uuid::parse_str(&value).ok(),
                "streamId" => stream_id = Uuid::parse_str(&value).ok(),
                "streamKind" => {
                    kind = serde_json::from_value::<EventStreamKind>(Value::String(value)).ok()
                }
                "cursor" => cursor = value.parse::<u64>().ok(),
                "limit" => limit = value.parse::<usize>().ok(),
                "requestId" => request_id = Uuid::parse_str(&value).unwrap_or(request_id),
                _ => {}
            }
        }
    }
    let Some(workspace_id) = workspace_id else {
        return error_response(request_id, ApiError::invalid("workspaceId is required"));
    };
    let Some(stream_id) = stream_id else {
        return error_response(request_id, ApiError::invalid("streamId is required"));
    };
    let Some(kind) = kind else {
        return error_response(request_id, ApiError::invalid("streamKind is required"));
    };
    let key = StreamKey::new(workspace_id, kind, stream_id);
    let request = ReplayRequest {
        key,
        cursor: cursor.map(|sequence| ResumeCursor::new(key, sequence)),
        limit: limit.unwrap_or(1024),
    };
    match state.events.subscribe(request).await {
        Ok(subscription) => {
            let stream = futures::stream::unfold(subscription, |mut subscription| async move {
                match subscription.next_event().await {
                    Some(Ok(frame)) => {
                        let payload = serde_json::to_string(&frame).unwrap_or_default();
                        Some((
                            Ok::<Event, Infallible>(Event::default().event("frame").data(payload)),
                            subscription,
                        ))
                    }
                    Some(Err(_)) => None,
                    None => None,
                }
            });
            Sse::new(stream)
                .keep_alive(
                    KeepAlive::new()
                        .interval(Duration::from_secs(15))
                        .text("keep-alive"),
                )
                .into_response()
        }
        Err(error) => error_response(request_id, stream_error_to_api_error(error)),
    }
}
