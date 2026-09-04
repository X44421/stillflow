//! API-facing health and telemetry views.

use serde::{Deserialize, Serialize};
use stillflow_core::{MetricPoint, TelemetrySnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthView {
    pub status: HealthStatus,
    pub checks: Vec<HealthCheck>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsView {
    pub metrics: Vec<MetricPoint>,
    pub queue_depth: u64,
    pub retained_events: usize,
    pub dropped_events: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessDependencies {
    pub control_plane: bool,
    pub connectors: bool,
    pub engine: bool,
    pub runtime: bool,
    pub snapshot_store: bool,
}

impl ReadinessDependencies {
    pub const fn all_ready() -> Self {
        Self {
            control_plane: true,
            connectors: true,
            engine: true,
            runtime: true,
            snapshot_store: true,
        }
    }

    fn checks(self) -> [(&'static str, bool); 5] {
        [
            ("controlPlane", self.control_plane),
            ("connectors", self.connectors),
            ("engine", self.engine),
            ("runtime", self.runtime),
            ("snapshotStore", self.snapshot_store),
        ]
    }
}

pub fn liveness_view() -> HealthView {
    HealthView {
        status: HealthStatus::Healthy,
        checks: vec![HealthCheck {
            name: "process".to_owned(),
            status: HealthStatus::Healthy,
        }],
    }
}

pub fn readiness_view(dependencies: ReadinessDependencies) -> HealthView {
    let checks = dependencies
        .checks()
        .into_iter()
        .map(|(name, ready)| HealthCheck {
            name: name.to_owned(),
            status: if ready {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unavailable
            },
        })
        .collect::<Vec<_>>();
    let status = if dependencies.control_plane
        && dependencies.connectors
        && dependencies.engine
        && dependencies.runtime
        && dependencies.snapshot_store
    {
        HealthStatus::Healthy
    } else if dependencies.control_plane {
        HealthStatus::Degraded
    } else {
        HealthStatus::Unavailable
    };
    HealthView { status, checks }
}

pub fn health_view(dependencies: ReadinessDependencies) -> HealthView {
    readiness_view(dependencies)
}

pub fn metrics_view(snapshot: TelemetrySnapshot, queue_depth: u64) -> MetricsView {
    MetricsView {
        metrics: snapshot.metrics,
        queue_depth,
        retained_events: snapshot.retained_events,
        dropped_events: snapshot.dropped_events,
    }
}
