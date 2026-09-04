//! Provider-neutral observability contracts shared by every Stillflow layer.
//!
//! This module deliberately contains no logging, metrics, or OpenTelemetry
//! SDK dependency. Callers emit a small, sanitized event vocabulary through a
//! [`TelemetrySink`]; a process may provide an in-memory sink, a structured
//! logger, or a provider adapter at the application boundary.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::events::ConnectorKind;

pub const MAX_TELEMETRY_EVENTS: usize = 4_096;
pub const MAX_TELEMETRY_METRICS: usize = 1_024;
pub const MAX_TELEMETRY_LABELS: usize = 4;
pub const MAX_TELEMETRY_TEXT_BYTES: usize = 256;
pub const MAX_CORRELATION_ID_BYTES: usize = 128;
pub const REDACTED: &str = "[REDACTED]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TelemetryComponent {
    Api,
    Queue,
    Job,
    Run,
    Engine,
    Connector,
    Storage,
}

impl TelemetryComponent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Queue => "queue",
            Self::Job => "job",
            Self::Run => "run",
            Self::Engine => "engine",
            Self::Connector => "connector",
            Self::Storage => "storage",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TelemetryOperation {
    Request,
    Health,
    Read,
    Write,
    Submit,
    Dispatch,
    Execute,
    Test,
    Observe,
}

impl TelemetryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Health => "health",
            Self::Read => "read",
            Self::Write => "write",
            Self::Submit => "submit",
            Self::Dispatch => "dispatch",
            Self::Execute => "execute",
            Self::Test => "test",
            Self::Observe => "observe",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TelemetryOutcome {
    Success,
    Failure,
    Rejected,
    Degraded,
}

impl TelemetryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Rejected => "rejected",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricKind {
    Counter,
    Gauge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricName {
    ApiRequestsTotal,
    ApiErrorsTotal,
    QueueDepth,
    JobOperationsTotal,
    RunOperationsTotal,
    EngineOperationsTotal,
    ConnectorCallsTotal,
    StorageOperationsTotal,
}

impl MetricName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiRequestsTotal => "stillflow_api_requests_total",
            Self::ApiErrorsTotal => "stillflow_api_errors_total",
            Self::QueueDepth => "stillflow_queue_depth",
            Self::JobOperationsTotal => "stillflow_job_operations_total",
            Self::RunOperationsTotal => "stillflow_run_operations_total",
            Self::EngineOperationsTotal => "stillflow_engine_operations_total",
            Self::ConnectorCallsTotal => "stillflow_connector_calls_total",
            Self::StorageOperationsTotal => "stillflow_storage_operations_total",
        }
    }

    pub const fn kind(self) -> MetricKind {
        match self {
            Self::QueueDepth => MetricKind::Gauge,
            _ => MetricKind::Counter,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "stillflow_api_requests_total" => Some(Self::ApiRequestsTotal),
            "stillflow_api_errors_total" => Some(Self::ApiErrorsTotal),
            "stillflow_queue_depth" => Some(Self::QueueDepth),
            "stillflow_job_operations_total" => Some(Self::JobOperationsTotal),
            "stillflow_run_operations_total" => Some(Self::RunOperationsTotal),
            "stillflow_engine_operations_total" => Some(Self::EngineOperationsTotal),
            "stillflow_connector_calls_total" => Some(Self::ConnectorCallsTotal),
            "stillflow_storage_operations_total" => Some(Self::StorageOperationsTotal),
            _ => None,
        }
    }
}

/// Fixed-key labels. Identifiers, paths, workspace names, and payload values
/// are intentionally impossible to add to a metric point.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryLabels {
    component: Option<TelemetryComponent>,
    operation: Option<TelemetryOperation>,
    outcome: Option<TelemetryOutcome>,
    connector: Option<ConnectorKind>,
}

impl TelemetryLabels {
    pub const fn new() -> Self {
        Self {
            component: None,
            operation: None,
            outcome: None,
            connector: None,
        }
    }

    pub const fn component(mut self, value: TelemetryComponent) -> Self {
        self.component = Some(value);
        self
    }

    pub const fn operation(mut self, value: TelemetryOperation) -> Self {
        self.operation = Some(value);
        self
    }

    pub const fn outcome(mut self, value: TelemetryOutcome) -> Self {
        self.outcome = Some(value);
        self
    }

    pub const fn connector(mut self, value: ConnectorKind) -> Self {
        self.connector = Some(value);
        self
    }

    pub fn as_map(self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        if let Some(value) = self.component {
            labels.insert("component".to_owned(), value.as_str().to_owned());
        }
        if let Some(value) = self.operation {
            labels.insert("operation".to_owned(), value.as_str().to_owned());
        }
        if let Some(value) = self.outcome {
            labels.insert("outcome".to_owned(), value.as_str().to_owned());
        }
        if let Some(value) = self.connector {
            labels.insert(
                "connector".to_owned(),
                connector_kind_name(value).to_owned(),
            );
        }
        labels
    }
}

fn connector_kind_name(kind: ConnectorKind) -> &'static str {
    match kind {
        ConnectorKind::LocalFile => "localFile",
        ConnectorKind::ObjectStore => "objectStore",
        ConnectorKind::SqlDatabase => "sqlDatabase",
        ConnectorKind::ExcelWorkbook => "excelWorkbook",
        ConnectorKind::DocumentWorker => "documentWorker",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricPoint {
    pub name: MetricName,
    pub kind: MetricKind,
    pub value: u64,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredLog {
    pub level: LogLevel,
    pub event: String,
    pub correlation_id: String,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub name: String,
    pub correlation_id: String,
    pub outcome: Option<TelemetryOutcome>,
    pub duration_micros: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TelemetryEvent {
    Metric(MetricPoint),
    StructuredLog(StructuredLog),
    SpanStarted(SpanRecord),
    SpanFinished(SpanRecord),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    pub metrics: Vec<MetricPoint>,
    pub retained_events: usize,
    pub dropped_events: u64,
}

pub trait TelemetrySink: Send + Sync {
    fn emit(&self, event: TelemetryEvent);

    fn snapshot(&self) -> Option<TelemetrySnapshot> {
        None
    }
}

#[derive(Debug, Default)]
pub struct NoopTelemetrySink;

impl TelemetrySink for NoopTelemetrySink {
    fn emit(&self, _event: TelemetryEvent) {}
}

#[derive(Debug, Default)]
pub struct InMemoryTelemetry {
    events: Mutex<Vec<TelemetryEvent>>,
    dropped_events: Mutex<u64>,
}

impl InMemoryTelemetry {
    pub fn events(&self) -> Vec<TelemetryEvent> {
        self.events
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default()
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        let events = self.events();
        let mut metrics: Vec<MetricPoint> = Vec::new();
        for event in &events {
            let TelemetryEvent::Metric(point) = event else {
                continue;
            };
            if let Some(existing) = metrics.iter_mut().find(|candidate: &&mut MetricPoint| {
                candidate.name == point.name
                    && candidate.kind == point.kind
                    && candidate.labels == point.labels
            }) {
                match point.kind {
                    MetricKind::Counter => {
                        existing.value = existing.value.saturating_add(point.value);
                    }
                    MetricKind::Gauge => existing.value = point.value,
                }
            } else if metrics.len() < MAX_TELEMETRY_METRICS {
                metrics.push(point.clone());
            }
        }
        TelemetrySnapshot {
            metrics,
            retained_events: events.len(),
            dropped_events: self
                .dropped_events
                .lock()
                .map(|count| *count)
                .unwrap_or_default(),
        }
    }
}

impl TelemetrySink for InMemoryTelemetry {
    fn emit(&self, event: TelemetryEvent) {
        let Ok(mut events) = self.events.lock() else {
            return;
        };
        if events.len() >= MAX_TELEMETRY_EVENTS {
            events.remove(0);
            if let Ok(mut dropped) = self.dropped_events.lock() {
                *dropped = dropped.saturating_add(1);
            }
        }
        events.push(event);
    }

    fn snapshot(&self) -> Option<TelemetrySnapshot> {
        Some(InMemoryTelemetry::snapshot(self))
    }
}

pub struct ExportingTelemetrySink {
    exporter: Arc<dyn TelemetryExporter>,
}

impl fmt::Debug for ExportingTelemetrySink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExportingTelemetrySink")
            .finish_non_exhaustive()
    }
}

impl ExportingTelemetrySink {
    pub fn new(exporter: Arc<dyn TelemetryExporter>) -> Self {
        Self { exporter }
    }
}

impl TelemetrySink for ExportingTelemetrySink {
    fn emit(&self, event: TelemetryEvent) {
        let _ = self.exporter.export(&event);
    }
}

/// Adapter seam for an OpenTelemetry or other provider implementation. The
/// core only passes sanitized events and ignores exporter failures so a
/// telemetry outage cannot alter data or execution semantics.
pub trait TelemetryExporter: Send + Sync {
    fn export(&self, event: &TelemetryEvent) -> Result<(), TelemetryExportError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryExportError;

#[derive(Clone)]
pub struct Telemetry {
    sink: Arc<dyn TelemetrySink>,
}

impl fmt::Debug for Telemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Telemetry").finish_non_exhaustive()
    }
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::noop()
    }
}

impl Telemetry {
    pub fn noop() -> Self {
        Self {
            sink: Arc::new(NoopTelemetrySink),
        }
    }

    pub fn from_sink(sink: Arc<dyn TelemetrySink>) -> Self {
        Self { sink }
    }

    pub fn in_memory() -> (Self, Arc<InMemoryTelemetry>) {
        let sink = Arc::new(InMemoryTelemetry::default());
        (
            Self::from_sink(Arc::clone(&sink) as Arc<dyn TelemetrySink>),
            sink,
        )
    }

    pub fn from_exporter(exporter: Arc<dyn TelemetryExporter>) -> Self {
        Self::from_sink(Arc::new(ExportingTelemetrySink::new(exporter)))
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        self.sink.snapshot().unwrap_or_default()
    }

    pub fn counter(&self, name: MetricName, labels: TelemetryLabels, value: u64) {
        self.emit_metric(name, MetricKind::Counter, labels, value);
    }

    pub fn gauge(&self, name: MetricName, labels: TelemetryLabels, value: u64) {
        self.emit_metric(name, MetricKind::Gauge, labels, value);
    }

    fn emit_metric(&self, name: MetricName, kind: MetricKind, labels: TelemetryLabels, value: u64) {
        let labels = labels.as_map();
        if labels.len() > MAX_TELEMETRY_LABELS || kind != name.kind() {
            return;
        }
        self.sink.emit(TelemetryEvent::Metric(MetricPoint {
            name,
            kind,
            value,
            labels,
        }));
    }

    pub fn log(
        &self,
        level: LogLevel,
        event: &str,
        correlation_id: &str,
        fields: impl IntoIterator<Item = (String, String)>,
    ) {
        let mut sanitized_fields = BTreeMap::new();
        for (key, value) in fields {
            let key = truncate_text(&key);
            sanitized_fields.insert(key.clone(), redact_telemetry_value(&key, &value));
        }
        self.sink.emit(TelemetryEvent::StructuredLog(StructuredLog {
            level,
            event: truncate_text(event),
            correlation_id: sanitize_correlation_id(correlation_id),
            fields: sanitized_fields,
        }));
    }

    pub fn span(&self, name: &str, correlation_id: &str) -> TelemetrySpan {
        let record = SpanRecord {
            name: truncate_text(name),
            correlation_id: sanitize_correlation_id(correlation_id),
            outcome: None,
            duration_micros: None,
        };
        self.sink.emit(TelemetryEvent::SpanStarted(record.clone()));
        TelemetrySpan {
            telemetry: self.clone(),
            record,
            started_at: Instant::now(),
        }
    }
}

pub struct TelemetrySpan {
    telemetry: Telemetry,
    record: SpanRecord,
    started_at: Instant,
}

impl TelemetrySpan {
    pub fn finish(mut self, outcome: TelemetryOutcome) {
        self.record.outcome = Some(outcome);
    }
}

impl Drop for TelemetrySpan {
    fn drop(&mut self) {
        self.record.duration_micros = Some(
            self.started_at
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        self.telemetry
            .sink
            .emit(TelemetryEvent::SpanFinished(self.record.clone()));
    }
}

pub fn redact_telemetry_value(key: &str, value: &str) -> String {
    let lower = key.to_ascii_lowercase();
    if [
        "secret",
        "credential",
        "token",
        "password",
        "private",
        "raw",
        "cell",
        "value",
        "path",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        REDACTED.to_owned()
    } else {
        truncate_text(value)
    }
}

fn sanitize_correlation_id(value: &str) -> String {
    if value.is_empty()
        || value.len() > MAX_CORRELATION_ID_BYTES
        || value.chars().any(char::is_control)
    {
        REDACTED.to_owned()
    } else {
        value.to_owned()
    }
}

fn truncate_text(value: &str) -> String {
    if value.len() <= MAX_TELEMETRY_TEXT_BYTES && !value.chars().any(char::is_control) {
        return value.to_owned();
    }
    let mut output = value
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_TELEMETRY_TEXT_BYTES)
        .collect::<String>();
    if output.len() > MAX_TELEMETRY_TEXT_BYTES {
        output.truncate(MAX_TELEMETRY_TEXT_BYTES);
    }
    output
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn labels_are_fixed_and_metric_names_are_parseable() {
        let labels = TelemetryLabels::new()
            .component(TelemetryComponent::Api)
            .operation(TelemetryOperation::Read)
            .outcome(TelemetryOutcome::Success)
            .connector(ConnectorKind::LocalFile)
            .as_map();
        assert_eq!(labels.len(), 4);
        assert!(!labels.values().any(|value| value.contains("workspace")));
        assert_eq!(
            MetricName::parse(MetricName::QueueDepth.as_str()),
            Some(MetricName::QueueDepth)
        );
        assert_eq!(MetricName::parse("unknown"), None);
    }

    #[test]
    fn telemetry_aggregates_counters_and_replaces_gauges() {
        let (telemetry, sink) = Telemetry::in_memory();
        let labels = TelemetryLabels::new().component(TelemetryComponent::Api);
        telemetry.counter(MetricName::ApiRequestsTotal, labels, 2);
        telemetry.counter(MetricName::ApiRequestsTotal, labels, 3);
        telemetry.gauge(MetricName::QueueDepth, TelemetryLabels::new(), 8);
        telemetry.gauge(MetricName::QueueDepth, TelemetryLabels::new(), 1);
        let snapshot = sink.snapshot();
        assert_eq!(snapshot.metrics.len(), 2);
        assert!(snapshot
            .metrics
            .iter()
            .any(|point| { point.name == MetricName::ApiRequestsTotal && point.value == 5 }));
        assert!(snapshot
            .metrics
            .iter()
            .any(|point| point.name == MetricName::QueueDepth && point.value == 1));
    }

    #[test]
    fn logs_redact_credentials_cells_paths_and_bound_text() {
        let (telemetry, sink) = Telemetry::in_memory();
        telemetry.log(
            LogLevel::Info,
            "connector.read",
            "corr-1",
            [
                ("credential_ref".to_owned(), "cred://secret".to_owned()),
                ("raw_cell".to_owned(), "sensitive cell".to_owned()),
                ("path".to_owned(), "/private/file.csv".to_owned()),
                ("operation".to_owned(), "read".to_owned()),
            ],
        );
        let events = sink.events();
        let TelemetryEvent::StructuredLog(record) = &events[0] else {
            panic!("expected structured log");
        };
        assert_eq!(record.fields["credential_ref"], REDACTED);
        assert_eq!(record.fields["raw_cell"], REDACTED);
        assert_eq!(record.fields["path"], REDACTED);
        assert_eq!(record.fields["operation"], "read");
    }

    #[test]
    fn spans_emit_start_and_finish_without_provider_dependency() {
        let (telemetry, sink) = Telemetry::in_memory();
        {
            let span = telemetry.span("job.execute", "corr-2");
            span.finish(TelemetryOutcome::Success);
        }
        let events = sink.events();
        assert!(matches!(events[0], TelemetryEvent::SpanStarted(_)));
        let TelemetryEvent::SpanFinished(record) = &events[1] else {
            panic!("expected span finish");
        };
        assert_eq!(record.outcome, Some(TelemetryOutcome::Success));
        assert!(record.duration_micros.is_some());
    }

    #[test]
    fn exporter_failures_are_observational_only() {
        struct FailingExporter {
            calls: AtomicUsize,
        }

        impl TelemetryExporter for FailingExporter {
            fn export(&self, _event: &TelemetryEvent) -> Result<(), TelemetryExportError> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Err(TelemetryExportError)
            }
        }

        let exporter = Arc::new(FailingExporter {
            calls: AtomicUsize::new(0),
        });
        let telemetry = Telemetry::from_exporter(exporter.clone());
        telemetry.counter(MetricName::ApiRequestsTotal, TelemetryLabels::new(), 1);
        assert_eq!(exporter.calls.load(Ordering::Relaxed), 1);
    }
}
