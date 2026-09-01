//! Stable value types for the E5 unified control plane.
//!
//! These types deliberately contain no persistence, transport, scheduler, or
//! execution behaviour. Storage owns durable representations and state
//! transitions; the core crate owns the vocabulary shared by those layers.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::verification::{InputRef, LogicalInputRef};

pub const MAX_QUEUED_JOBS_PER_WORKSPACE: usize = 256;
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceState {
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionState {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceConnectionState {
    Active,
    Disabled,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceAssetState {
    Active,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DatasetState {
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlanState {
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlanVersionState {
    Draft,
    Published,
    Superseded,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunState {
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactRefState {
    Staged,
    Committed,
    Tombstoned,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventStreamKind {
    Job,
    Run,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlPlaneEventType {
    #[serde(rename = "job.queued")]
    JobQueued,
    #[serde(rename = "job.running")]
    JobRunning,
    #[serde(rename = "job.cancelling")]
    JobCancelling,
    #[serde(rename = "job.succeeded")]
    JobSucceeded,
    #[serde(rename = "job.failed")]
    JobFailed,
    #[serde(rename = "job.cancelled")]
    JobCancelled,
    #[serde(rename = "run.running")]
    RunRunning,
    #[serde(rename = "run.cancelling")]
    RunCancelling,
    #[serde(rename = "run.succeeded")]
    RunSucceeded,
    #[serde(rename = "run.failed")]
    RunFailed,
    #[serde(rename = "run.cancelled")]
    RunCancelled,
    #[serde(rename = "run.reconciled")]
    RunReconciled,
    #[serde(rename = "artifact.committed")]
    ArtifactCommitted,
    #[serde(rename = "artifact.tombstoned")]
    ArtifactTombstoned,
}

/// E5's execution input identity, including the immutable logical version.
pub type ControlPlaneInput = LogicalInputRef;

pub const fn asset_input(asset_id: Uuid, version_digest: [u8; 32]) -> ControlPlaneInput {
    LogicalInputRef {
        input: InputRef::Asset { asset_id },
        version_digest,
    }
}

pub const fn snapshot_input(snapshot_id: Uuid, version_digest: [u8; 32]) -> ControlPlaneInput {
    LogicalInputRef {
        input: InputRef::Snapshot { snapshot_id },
        version_digest,
    }
}
