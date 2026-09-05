//! Measurement-only instrumentation for the memory-admission predictor
//! (`src/predict.rs`), added by issue #284 ([O0-P1]).
//!
//! Contract:
//! - Every counter/timer is inert unless the `predict-metrics` cargo feature
//!   is enabled. With the feature disabled, all hooks compile to zero-sized,
//!   `#[inline(always)]` no-ops and the optimizer removes them entirely, so
//!   production behavior and predicted values are unchanged.
//! - With the feature enabled, hooks only touch process-global `AtomicU64`
//!   counters (`Relaxed` ordering) and `std::time::Instant` wall timers. They
//!   never influence predictor control flow, estimates, or chunk boundaries.
//! - Counters are process-global aggregates, not per-thread. Engine runs are
//!   serialized by the engine semaphore in production; the measurement
//!   fixtures run under a single engine instance at a time.
//! - Error paths (`?` before a recording point) are not counted; measurement
//!   fixtures do not trigger predictor errors.
//!
//! Attribution model: counters are cumulative. Per-`largest_feasible_k`-call
//! attribution is obtained by taking snapshot deltas around a single call
//! (see `predict_metrics_tests`), which is exact because engine runs in the
//! fixtures are serialized.

// The read-side API (`snapshot`/`reset`/`to_json`) is consumed by the
// measurement tests only, and `RuleKind::Other` exists for future branches,
// so dead-code analysis flags them in non-test builds. This is expected for
// measurement-only code.
#![allow(dead_code)]

// The snapshot struct and recording API are compiled in both modes so call
// sites do not need `#[cfg]`; the `imp` module behind them swaps between a
// real implementation and zero-cost no-ops.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PredictMetricsSnapshot {
    /// Calls to `largest_feasible_k` (all call sites).
    pub lfk_calls: u64,
    /// Wall time spent inside `largest_feasible_k`, nanoseconds.
    pub lfk_wall_ns: u64,
    /// Calls to `predict` (probes, including the mandatory single-row probe).
    pub predict_probes: u64,
    /// Wall time spent inside `predict`, nanoseconds.
    pub predict_wall_ns: u64,
    /// `PredictedSchema` clones at `predict` entry (initial working copy).
    pub clone_working_init: u64,
    /// `PredictedSchema` clones in the `Project` step.
    pub clone_project: u64,
    /// `PredictedSchema` clones in the `Filter` step.
    pub clone_filter: u64,
    /// `PredictedSchema` clones at `predict_rule` entry (one per rule).
    pub clone_rule: u64,
    /// Calls to `PredictedSchema::to_logical_schema` (expression typing).
    pub to_logical_schema_calls: u64,
    /// Calls to `refresh_source_widths`.
    pub refresh_source_widths_calls: u64,
    /// Wall time in `refresh_source_widths`, nanoseconds.
    pub refresh_source_widths_wall_ns: u64,
    /// Source columns whose stored width was refreshed.
    pub source_columns_refreshed: u64,
    /// Calls to `max_variable_width` (variable-width source scans).
    pub max_variable_width_calls: u64,
    /// Rows examined row-by-row inside `max_variable_width` loops.
    pub width_scan_rows: u64,
    /// Sum of value widths observed by `max_variable_width` scans.
    pub width_scan_value_bytes: u64,
    /// Calls to `variable_data_bytes`.
    pub variable_data_bytes_calls: u64,
    /// Rows covered by `variable_data_bytes` (including O(1) offset-span reads).
    pub variable_data_rows: u64,
    /// Data bytes computed by `variable_data_bytes` (offset-span or per-row sum).
    pub variable_data_span_bytes: u64,
    /// Calls to `list_physical_bytes` (nested list scans).
    pub list_scans: u64,
    /// Calls to `struct_physical_bytes` (nested struct scans).
    pub struct_scans: u64,
    /// Calls to `column_physical_sum`.
    pub column_physical_sum_calls: u64,
    /// Wall time in `column_physical_sum`, nanoseconds.
    pub column_physical_sum_wall_ns: u64,
    /// Columns visited by `column_physical_sum` (calls x schema width).
    pub column_physical_sum_columns: u64,
    /// Calls to `column_physical_bytes`.
    pub column_physical_bytes_calls: u64,
    /// Project-induced full-column recomputations (`column_physical_sum` after a Project).
    pub project_full_recomputes: u64,
    /// Rule-induced full-column recomputations (`column_physical_sum` after a rule).
    pub rule_full_recomputes: u64,
    /// Rule-induced full-column recomputations from `DropColumn`.
    pub rule_recompute_drop_column: u64,
    /// Rule-induced full-column recomputations from `Trim`.
    pub rule_recompute_trim: u64,
    /// Rule-induced full-column recomputations from `ReplaceLiteral`.
    pub rule_recompute_replace_literal: u64,
    /// Rule-induced full-column recomputations from `FillNull`.
    pub rule_recompute_fill_null: u64,
    /// Rule-induced full-column recomputations from `Cast`.
    pub rule_recompute_cast: u64,
    /// Single-column temporary byte computations for `DeriveColumn`.
    pub derive_temp_byte_calls: u64,
    /// Calls to `predict_export_transition`.
    pub export_transition_calls: u64,
    /// Wall time in `predict_export_transition`, nanoseconds.
    pub export_transition_wall_ns: u64,
    /// Columns visited by the export-transition computation.
    pub export_transition_columns: u64,
    /// `column_physical_bytes` calls made by the export-transition pass.
    pub export_transition_column_byte_calls: u64,
    /// `largest_feasible_k` calls issued by the ingest chunk loop (`consume_envelope`).
    pub site_ingest_lfk_calls: u64,
    /// Wall time of `largest_feasible_k` at the ingest site, nanoseconds.
    pub site_ingest_lfk_wall_ns: u64,
    /// Wall time of the ingest chunk loop (predictor + surrounding work), nanoseconds.
    pub site_ingest_chunk_loop_wall_ns: u64,
    /// `largest_feasible_k` calls issued by the preview chunk loop.
    pub site_preview_lfk_calls: u64,
    /// Wall time of `largest_feasible_k` at the preview site, nanoseconds.
    pub site_preview_lfk_wall_ns: u64,
    /// Wall time of the preview chunk loop (predictor + surrounding work), nanoseconds.
    pub site_preview_chunk_loop_wall_ns: u64,
}

impl PredictMetricsSnapshot {
    /// Total `PredictedSchema` clones across all instrumented call sites.
    pub(crate) fn clone_total(&self) -> u64 {
        self.clone_working_init
            .saturating_add(self.clone_project)
            .saturating_add(self.clone_filter)
            .saturating_add(self.clone_rule)
    }

    /// Stable JSON projection used by the measurement fixtures.
    pub(crate) fn to_json(self) -> serde_json::Value {
        let pairs: [(&str, u64); 43] = [
            ("lfk_calls", self.lfk_calls),
            ("lfk_wall_ns", self.lfk_wall_ns),
            ("predict_probes", self.predict_probes),
            ("predict_wall_ns", self.predict_wall_ns),
            ("clone_working_init", self.clone_working_init),
            ("clone_project", self.clone_project),
            ("clone_filter", self.clone_filter),
            ("clone_rule", self.clone_rule),
            ("clone_total", self.clone_total()),
            ("to_logical_schema_calls", self.to_logical_schema_calls),
            (
                "refresh_source_widths_calls",
                self.refresh_source_widths_calls,
            ),
            (
                "refresh_source_widths_wall_ns",
                self.refresh_source_widths_wall_ns,
            ),
            ("source_columns_refreshed", self.source_columns_refreshed),
            ("max_variable_width_calls", self.max_variable_width_calls),
            ("width_scan_rows", self.width_scan_rows),
            ("width_scan_value_bytes", self.width_scan_value_bytes),
            ("variable_data_bytes_calls", self.variable_data_bytes_calls),
            ("variable_data_rows", self.variable_data_rows),
            ("variable_data_span_bytes", self.variable_data_span_bytes),
            ("list_scans", self.list_scans),
            ("struct_scans", self.struct_scans),
            ("column_physical_sum_calls", self.column_physical_sum_calls),
            (
                "column_physical_sum_wall_ns",
                self.column_physical_sum_wall_ns,
            ),
            (
                "column_physical_sum_columns",
                self.column_physical_sum_columns,
            ),
            (
                "column_physical_bytes_calls",
                self.column_physical_bytes_calls,
            ),
            ("project_full_recomputes", self.project_full_recomputes),
            ("rule_full_recomputes", self.rule_full_recomputes),
            (
                "rule_recompute_drop_column",
                self.rule_recompute_drop_column,
            ),
            ("rule_recompute_trim", self.rule_recompute_trim),
            (
                "rule_recompute_replace_literal",
                self.rule_recompute_replace_literal,
            ),
            ("rule_recompute_fill_null", self.rule_recompute_fill_null),
            ("rule_recompute_cast", self.rule_recompute_cast),
            ("derive_temp_byte_calls", self.derive_temp_byte_calls),
            ("export_transition_calls", self.export_transition_calls),
            ("export_transition_wall_ns", self.export_transition_wall_ns),
            ("export_transition_columns", self.export_transition_columns),
            (
                "export_transition_column_byte_calls",
                self.export_transition_column_byte_calls,
            ),
            ("site_ingest_lfk_calls", self.site_ingest_lfk_calls),
            ("site_ingest_lfk_wall_ns", self.site_ingest_lfk_wall_ns),
            (
                "site_ingest_chunk_loop_wall_ns",
                self.site_ingest_chunk_loop_wall_ns,
            ),
            ("site_preview_lfk_calls", self.site_preview_lfk_calls),
            ("site_preview_lfk_wall_ns", self.site_preview_lfk_wall_ns),
            (
                "site_preview_chunk_loop_wall_ns",
                self.site_preview_chunk_loop_wall_ns,
            ),
        ];
        let mut map = serde_json::Map::with_capacity(pairs.len());
        for (name, value) in pairs {
            map.insert(name.to_owned(), serde_json::Value::from(value));
        }
        serde_json::Value::Object(map)
    }
}

/// Where a `PredictedSchema` clone originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloneSite {
    /// `predict` entry: initial working copy of the input schema.
    WorkingInit,
    /// `CompiledStep::Project` handling.
    Project,
    /// `CompiledStep::Filter` handling.
    Filter,
    /// `predict_rule` entry: one clone per rule application.
    Rule,
}

/// Which rule branch triggered a full-column recompute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuleKind {
    DropColumn,
    Trim,
    ReplaceLiteral,
    FillNull,
    Cast,
    Other,
}

/// Engine call site that invoked the predictor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Site {
    /// Ingest chunk loop (`consume_envelope` in `engine.rs`).
    Ingest,
    /// Preview chunk loop (`preview.rs`).
    Preview,
}

#[cfg(feature = "predict-metrics")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use super::{CloneSite, PredictMetricsSnapshot, RuleKind, Site};

    macro_rules! counter {
        ($name:ident) => {
            static $name: AtomicU64 = AtomicU64::new(0);
        };
    }

    counter!(LFK_CALLS);
    counter!(LFK_WALL_NS);
    counter!(PREDICT_PROBES);
    counter!(PREDICT_WALL_NS);
    counter!(CLONE_WORKING_INIT);
    counter!(CLONE_PROJECT);
    counter!(CLONE_FILTER);
    counter!(CLONE_RULE);
    counter!(TO_LOGICAL_SCHEMA_CALLS);
    counter!(REFRESH_SOURCE_WIDTHS_CALLS);
    counter!(REFRESH_SOURCE_WIDTHS_WALL_NS);
    counter!(SOURCE_COLUMNS_REFRESHED);
    counter!(MAX_VARIABLE_WIDTH_CALLS);
    counter!(WIDTH_SCAN_ROWS);
    counter!(WIDTH_SCAN_VALUE_BYTES);
    counter!(VARIABLE_DATA_BYTES_CALLS);
    counter!(VARIABLE_DATA_ROWS);
    counter!(VARIABLE_DATA_SPAN_BYTES);
    counter!(LIST_SCANS);
    counter!(STRUCT_SCANS);
    counter!(COLUMN_PHYSICAL_SUM_CALLS);
    counter!(COLUMN_PHYSICAL_SUM_WALL_NS);
    counter!(COLUMN_PHYSICAL_SUM_COLUMNS);
    counter!(COLUMN_PHYSICAL_BYTES_CALLS);
    counter!(PROJECT_FULL_RECOMPUTES);
    counter!(RULE_FULL_RECOMPUTES);
    counter!(RULE_RECOMPUTE_DROP_COLUMN);
    counter!(RULE_RECOMPUTE_TRIM);
    counter!(RULE_RECOMPUTE_REPLACE_LITERAL);
    counter!(RULE_RECOMPUTE_FILL_NULL);
    counter!(RULE_RECOMPUTE_CAST);
    counter!(DERIVE_TEMP_BYTE_CALLS);
    counter!(EXPORT_TRANSITION_CALLS);
    counter!(EXPORT_TRANSITION_WALL_NS);
    counter!(EXPORT_TRANSITION_COLUMNS);
    counter!(EXPORT_TRANSITION_COLUMN_BYTE_CALLS);
    counter!(SITE_INGEST_LFK_CALLS);
    counter!(SITE_INGEST_LFK_WALL_NS);
    counter!(SITE_INGEST_CHUNK_LOOP_WALL_NS);
    counter!(SITE_PREVIEW_LFK_CALLS);
    counter!(SITE_PREVIEW_LFK_WALL_NS);
    counter!(SITE_PREVIEW_CHUNK_LOOP_WALL_NS);

    fn add_u64(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    /// Wall timer that accumulates elapsed nanoseconds into `sink` on drop.
    /// Drop-based so early returns and `?` are still measured.
    pub(crate) struct ScopedTimer {
        start: Instant,
        sink: fn(u64),
    }

    impl Drop for ScopedTimer {
        fn drop(&mut self) {
            (self.sink)(self.start.elapsed().as_nanos() as u64);
        }
    }

    /// Timer sinks (passed to [`scoped_timer`] by the predictor call sites).
    pub(crate) fn add_predict_wall(ns: u64) {
        add_u64(&PREDICT_WALL_NS, ns);
    }

    pub(crate) fn add_lfk_wall(ns: u64) {
        add_u64(&LFK_WALL_NS, ns);
    }

    pub(crate) fn add_refresh_wall(ns: u64) {
        add_u64(&REFRESH_SOURCE_WIDTHS_WALL_NS, ns);
    }

    pub(crate) fn add_sum_wall(ns: u64) {
        add_u64(&COLUMN_PHYSICAL_SUM_WALL_NS, ns);
    }

    pub(crate) fn add_export_wall(ns: u64) {
        add_u64(&EXPORT_TRANSITION_WALL_NS, ns);
    }

    pub(crate) fn scoped_timer(sink: fn(u64)) -> ScopedTimer {
        ScopedTimer {
            start: Instant::now(),
            sink,
        }
    }

    /// Timer for a predictor call at an engine call site (wall time only;
    /// pair with [`record_site_predict_call`] for the call count).
    pub(crate) struct SitePredictGuard {
        start: Instant,
        sink: fn(u64),
    }

    impl Drop for SitePredictGuard {
        fn drop(&mut self) {
            (self.sink)(self.start.elapsed().as_nanos() as u64);
        }
    }

    /// Timer for an engine chunk loop. Records total loop wall time on drop,
    /// so `break`/early-return paths are included. Chunk counts are covered by
    /// the per-site predictor call counters (one call per accepted chunk).
    pub(crate) struct SiteLoopGuard {
        start: Instant,
        wall_sink: fn(u64),
    }

    impl Drop for SiteLoopGuard {
        fn drop(&mut self) {
            (self.wall_sink)(self.start.elapsed().as_nanos() as u64);
        }
    }

    pub(crate) fn site_predict_scoped(site: Site) -> SitePredictGuard {
        match site {
            Site::Ingest => SitePredictGuard {
                start: Instant::now(),
                sink: add_ingest_predict_wall,
            },
            Site::Preview => SitePredictGuard {
                start: Instant::now(),
                sink: add_preview_predict_wall,
            },
        }
    }

    pub(crate) fn site_loop_scoped(site: Site) -> SiteLoopGuard {
        match site {
            Site::Ingest => SiteLoopGuard {
                start: Instant::now(),
                wall_sink: add_ingest_loop_wall,
            },
            Site::Preview => SiteLoopGuard {
                start: Instant::now(),
                wall_sink: add_preview_loop_wall,
            },
        }
    }

    fn add_ingest_predict_wall(ns: u64) {
        add_u64(&SITE_INGEST_LFK_WALL_NS, ns);
    }

    fn add_ingest_loop_wall(ns: u64) {
        add_u64(&SITE_INGEST_CHUNK_LOOP_WALL_NS, ns);
    }

    fn add_preview_predict_wall(ns: u64) {
        add_u64(&SITE_PREVIEW_LFK_WALL_NS, ns);
    }

    fn add_preview_loop_wall(ns: u64) {
        add_u64(&SITE_PREVIEW_CHUNK_LOOP_WALL_NS, ns);
    }

    pub(crate) fn record_lfk_call() {
        LFK_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_predict_probe() {
        PREDICT_PROBES.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_clone_site(site: CloneSite) {
        match site {
            CloneSite::WorkingInit => add_u64(&CLONE_WORKING_INIT, 1),
            CloneSite::Project => add_u64(&CLONE_PROJECT, 1),
            CloneSite::Filter => add_u64(&CLONE_FILTER, 1),
            CloneSite::Rule => add_u64(&CLONE_RULE, 1),
        }
    }

    pub(crate) fn record_to_logical_schema() {
        TO_LOGICAL_SCHEMA_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_refresh_source_widths() {
        REFRESH_SOURCE_WIDTHS_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_source_columns_refreshed(count: u64) {
        add_u64(&SOURCE_COLUMNS_REFRESHED, count);
    }

    pub(crate) fn record_max_variable_width_scan(rows: u64, value_bytes: u64) {
        add_u64(&MAX_VARIABLE_WIDTH_CALLS, 1);
        add_u64(&WIDTH_SCAN_ROWS, rows);
        add_u64(&WIDTH_SCAN_VALUE_BYTES, value_bytes);
    }

    pub(crate) fn record_variable_data_bytes(rows: u64, span_bytes: u64) {
        add_u64(&VARIABLE_DATA_BYTES_CALLS, 1);
        add_u64(&VARIABLE_DATA_ROWS, rows);
        add_u64(&VARIABLE_DATA_SPAN_BYTES, span_bytes);
    }

    pub(crate) fn record_list_scan() {
        LIST_SCANS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_struct_scan() {
        STRUCT_SCANS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_column_physical_sum(columns: usize) {
        add_u64(&COLUMN_PHYSICAL_SUM_CALLS, 1);
        add_u64(&COLUMN_PHYSICAL_SUM_COLUMNS, columns as u64);
    }

    pub(crate) fn record_column_physical_bytes() {
        COLUMN_PHYSICAL_BYTES_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_project_full_recompute() {
        PROJECT_FULL_RECOMPUTES.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rule_full_recompute(kind: RuleKind) {
        RULE_FULL_RECOMPUTES.fetch_add(1, Ordering::Relaxed);
        match kind {
            RuleKind::DropColumn => add_u64(&RULE_RECOMPUTE_DROP_COLUMN, 1),
            RuleKind::Trim => add_u64(&RULE_RECOMPUTE_TRIM, 1),
            RuleKind::ReplaceLiteral => add_u64(&RULE_RECOMPUTE_REPLACE_LITERAL, 1),
            RuleKind::FillNull => add_u64(&RULE_RECOMPUTE_FILL_NULL, 1),
            RuleKind::Cast => add_u64(&RULE_RECOMPUTE_CAST, 1),
            RuleKind::Other => {}
        }
    }

    pub(crate) fn record_derive_temp_bytes() {
        DERIVE_TEMP_BYTE_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_export_transition(columns: usize, column_byte_calls: usize) {
        add_u64(&EXPORT_TRANSITION_CALLS, 1);
        add_u64(&EXPORT_TRANSITION_COLUMNS, columns as u64);
        add_u64(
            &EXPORT_TRANSITION_COLUMN_BYTE_CALLS,
            column_byte_calls as u64,
        );
    }

    pub(crate) fn record_site_predict_call(site: Site) {
        match site {
            Site::Ingest => SITE_INGEST_LFK_CALLS.fetch_add(1, Ordering::Relaxed),
            Site::Preview => SITE_PREVIEW_LFK_CALLS.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Current cumulative counter values.
    pub(crate) fn snapshot() -> PredictMetricsSnapshot {
        PredictMetricsSnapshot {
            lfk_calls: LFK_CALLS.load(Ordering::Relaxed),
            lfk_wall_ns: LFK_WALL_NS.load(Ordering::Relaxed),
            predict_probes: PREDICT_PROBES.load(Ordering::Relaxed),
            predict_wall_ns: PREDICT_WALL_NS.load(Ordering::Relaxed),
            clone_working_init: CLONE_WORKING_INIT.load(Ordering::Relaxed),
            clone_project: CLONE_PROJECT.load(Ordering::Relaxed),
            clone_filter: CLONE_FILTER.load(Ordering::Relaxed),
            clone_rule: CLONE_RULE.load(Ordering::Relaxed),
            to_logical_schema_calls: TO_LOGICAL_SCHEMA_CALLS.load(Ordering::Relaxed),
            refresh_source_widths_calls: REFRESH_SOURCE_WIDTHS_CALLS.load(Ordering::Relaxed),
            refresh_source_widths_wall_ns: REFRESH_SOURCE_WIDTHS_WALL_NS.load(Ordering::Relaxed),
            source_columns_refreshed: SOURCE_COLUMNS_REFRESHED.load(Ordering::Relaxed),
            max_variable_width_calls: MAX_VARIABLE_WIDTH_CALLS.load(Ordering::Relaxed),
            width_scan_rows: WIDTH_SCAN_ROWS.load(Ordering::Relaxed),
            width_scan_value_bytes: WIDTH_SCAN_VALUE_BYTES.load(Ordering::Relaxed),
            variable_data_bytes_calls: VARIABLE_DATA_BYTES_CALLS.load(Ordering::Relaxed),
            variable_data_rows: VARIABLE_DATA_ROWS.load(Ordering::Relaxed),
            variable_data_span_bytes: VARIABLE_DATA_SPAN_BYTES.load(Ordering::Relaxed),
            list_scans: LIST_SCANS.load(Ordering::Relaxed),
            struct_scans: STRUCT_SCANS.load(Ordering::Relaxed),
            column_physical_sum_calls: COLUMN_PHYSICAL_SUM_CALLS.load(Ordering::Relaxed),
            column_physical_sum_wall_ns: COLUMN_PHYSICAL_SUM_WALL_NS.load(Ordering::Relaxed),
            column_physical_sum_columns: COLUMN_PHYSICAL_SUM_COLUMNS.load(Ordering::Relaxed),
            column_physical_bytes_calls: COLUMN_PHYSICAL_BYTES_CALLS.load(Ordering::Relaxed),
            project_full_recomputes: PROJECT_FULL_RECOMPUTES.load(Ordering::Relaxed),
            rule_full_recomputes: RULE_FULL_RECOMPUTES.load(Ordering::Relaxed),
            rule_recompute_drop_column: RULE_RECOMPUTE_DROP_COLUMN.load(Ordering::Relaxed),
            rule_recompute_trim: RULE_RECOMPUTE_TRIM.load(Ordering::Relaxed),
            rule_recompute_replace_literal: RULE_RECOMPUTE_REPLACE_LITERAL.load(Ordering::Relaxed),
            rule_recompute_fill_null: RULE_RECOMPUTE_FILL_NULL.load(Ordering::Relaxed),
            rule_recompute_cast: RULE_RECOMPUTE_CAST.load(Ordering::Relaxed),
            derive_temp_byte_calls: DERIVE_TEMP_BYTE_CALLS.load(Ordering::Relaxed),
            export_transition_calls: EXPORT_TRANSITION_CALLS.load(Ordering::Relaxed),
            export_transition_wall_ns: EXPORT_TRANSITION_WALL_NS.load(Ordering::Relaxed),
            export_transition_columns: EXPORT_TRANSITION_COLUMNS.load(Ordering::Relaxed),
            export_transition_column_byte_calls: EXPORT_TRANSITION_COLUMN_BYTE_CALLS
                .load(Ordering::Relaxed),
            site_ingest_lfk_calls: SITE_INGEST_LFK_CALLS.load(Ordering::Relaxed),
            site_ingest_lfk_wall_ns: SITE_INGEST_LFK_WALL_NS.load(Ordering::Relaxed),
            site_ingest_chunk_loop_wall_ns: SITE_INGEST_CHUNK_LOOP_WALL_NS.load(Ordering::Relaxed),
            site_preview_lfk_calls: SITE_PREVIEW_LFK_CALLS.load(Ordering::Relaxed),
            site_preview_lfk_wall_ns: SITE_PREVIEW_LFK_WALL_NS.load(Ordering::Relaxed),
            site_preview_chunk_loop_wall_ns: SITE_PREVIEW_CHUNK_LOOP_WALL_NS
                .load(Ordering::Relaxed),
        }
    }

    /// Zero all counters (used to bracket measurement windows).
    pub(crate) fn reset() {
        LFK_CALLS.store(0, Ordering::Relaxed);
        LFK_WALL_NS.store(0, Ordering::Relaxed);
        PREDICT_PROBES.store(0, Ordering::Relaxed);
        PREDICT_WALL_NS.store(0, Ordering::Relaxed);
        CLONE_WORKING_INIT.store(0, Ordering::Relaxed);
        CLONE_PROJECT.store(0, Ordering::Relaxed);
        CLONE_FILTER.store(0, Ordering::Relaxed);
        CLONE_RULE.store(0, Ordering::Relaxed);
        TO_LOGICAL_SCHEMA_CALLS.store(0, Ordering::Relaxed);
        REFRESH_SOURCE_WIDTHS_CALLS.store(0, Ordering::Relaxed);
        REFRESH_SOURCE_WIDTHS_WALL_NS.store(0, Ordering::Relaxed);
        SOURCE_COLUMNS_REFRESHED.store(0, Ordering::Relaxed);
        MAX_VARIABLE_WIDTH_CALLS.store(0, Ordering::Relaxed);
        WIDTH_SCAN_ROWS.store(0, Ordering::Relaxed);
        WIDTH_SCAN_VALUE_BYTES.store(0, Ordering::Relaxed);
        VARIABLE_DATA_BYTES_CALLS.store(0, Ordering::Relaxed);
        VARIABLE_DATA_ROWS.store(0, Ordering::Relaxed);
        VARIABLE_DATA_SPAN_BYTES.store(0, Ordering::Relaxed);
        LIST_SCANS.store(0, Ordering::Relaxed);
        STRUCT_SCANS.store(0, Ordering::Relaxed);
        COLUMN_PHYSICAL_SUM_CALLS.store(0, Ordering::Relaxed);
        COLUMN_PHYSICAL_SUM_WALL_NS.store(0, Ordering::Relaxed);
        COLUMN_PHYSICAL_SUM_COLUMNS.store(0, Ordering::Relaxed);
        COLUMN_PHYSICAL_BYTES_CALLS.store(0, Ordering::Relaxed);
        PROJECT_FULL_RECOMPUTES.store(0, Ordering::Relaxed);
        RULE_FULL_RECOMPUTES.store(0, Ordering::Relaxed);
        RULE_RECOMPUTE_DROP_COLUMN.store(0, Ordering::Relaxed);
        RULE_RECOMPUTE_TRIM.store(0, Ordering::Relaxed);
        RULE_RECOMPUTE_REPLACE_LITERAL.store(0, Ordering::Relaxed);
        RULE_RECOMPUTE_FILL_NULL.store(0, Ordering::Relaxed);
        RULE_RECOMPUTE_CAST.store(0, Ordering::Relaxed);
        DERIVE_TEMP_BYTE_CALLS.store(0, Ordering::Relaxed);
        EXPORT_TRANSITION_CALLS.store(0, Ordering::Relaxed);
        EXPORT_TRANSITION_WALL_NS.store(0, Ordering::Relaxed);
        EXPORT_TRANSITION_COLUMNS.store(0, Ordering::Relaxed);
        EXPORT_TRANSITION_COLUMN_BYTE_CALLS.store(0, Ordering::Relaxed);
        SITE_INGEST_LFK_CALLS.store(0, Ordering::Relaxed);
        SITE_INGEST_LFK_WALL_NS.store(0, Ordering::Relaxed);
        SITE_INGEST_CHUNK_LOOP_WALL_NS.store(0, Ordering::Relaxed);
        SITE_PREVIEW_LFK_CALLS.store(0, Ordering::Relaxed);
        SITE_PREVIEW_LFK_WALL_NS.store(0, Ordering::Relaxed);
        SITE_PREVIEW_CHUNK_LOOP_WALL_NS.store(0, Ordering::Relaxed);
    }
}

#[cfg(not(feature = "predict-metrics"))]
mod imp {
    //! Zero-cost stubs. Every function is a no-op the optimizer removes; the
    //! timer guards are zero-sized types with empty `Drop` impls. Disabled
    //! snapshots always read zero, which the targeted tests assert.

    use super::{CloneSite, PredictMetricsSnapshot, RuleKind, Site};

    #[derive(Debug, Copy, Clone)]
    pub(crate) struct ScopedTimer;

    #[derive(Debug, Copy, Clone)]
    pub(crate) struct SitePredictGuard;

    #[derive(Debug, Copy, Clone)]
    pub(crate) struct SiteLoopGuard;

    #[inline(always)]
    pub(crate) fn add_predict_wall(_ns: u64) {}

    #[inline(always)]
    pub(crate) fn add_lfk_wall(_ns: u64) {}

    #[inline(always)]
    pub(crate) fn add_refresh_wall(_ns: u64) {}

    #[inline(always)]
    pub(crate) fn add_sum_wall(_ns: u64) {}

    #[inline(always)]
    pub(crate) fn add_export_wall(_ns: u64) {}

    #[inline(always)]
    pub(crate) fn scoped_timer(_sink: fn(u64)) -> ScopedTimer {
        ScopedTimer
    }

    #[inline(always)]
    pub(crate) fn site_predict_scoped(_site: Site) -> SitePredictGuard {
        SitePredictGuard
    }

    #[inline(always)]
    pub(crate) fn site_loop_scoped(_site: Site) -> SiteLoopGuard {
        SiteLoopGuard
    }

    #[inline(always)]
    pub(crate) fn record_lfk_call() {}

    #[inline(always)]
    pub(crate) fn record_predict_probe() {}

    #[inline(always)]
    pub(crate) fn record_clone_site(_site: CloneSite) {}

    #[inline(always)]
    pub(crate) fn record_to_logical_schema() {}

    #[inline(always)]
    pub(crate) fn record_refresh_source_widths() {}

    #[inline(always)]
    pub(crate) fn record_source_columns_refreshed(_count: u64) {}

    #[inline(always)]
    pub(crate) fn record_max_variable_width_scan(_rows: u64, _value_bytes: u64) {}

    #[inline(always)]
    pub(crate) fn record_variable_data_bytes(_rows: u64, _span_bytes: u64) {}

    #[inline(always)]
    pub(crate) fn record_list_scan() {}

    #[inline(always)]
    pub(crate) fn record_struct_scan() {}

    #[inline(always)]
    pub(crate) fn record_column_physical_sum(_columns: usize) {}

    #[inline(always)]
    pub(crate) fn record_column_physical_bytes() {}

    #[inline(always)]
    pub(crate) fn record_project_full_recompute() {}

    #[inline(always)]
    pub(crate) fn record_rule_full_recompute(_kind: RuleKind) {}

    #[inline(always)]
    pub(crate) fn record_derive_temp_bytes() {}

    #[inline(always)]
    pub(crate) fn record_export_transition(_columns: usize, _column_byte_calls: usize) {}

    #[inline(always)]
    pub(crate) fn record_site_predict_call(_site: Site) {}

    #[inline(always)]
    pub(crate) fn snapshot() -> PredictMetricsSnapshot {
        PredictMetricsSnapshot::default()
    }

    #[inline(always)]
    pub(crate) fn reset() {}
}

#[allow(unused_imports)]
pub(crate) use imp::{
    add_export_wall, add_lfk_wall, add_predict_wall, add_refresh_wall, add_sum_wall,
    record_clone_site, record_column_physical_bytes, record_column_physical_sum,
    record_derive_temp_bytes, record_export_transition, record_lfk_call, record_list_scan,
    record_max_variable_width_scan, record_predict_probe, record_project_full_recompute,
    record_refresh_source_widths, record_rule_full_recompute, record_site_predict_call,
    record_source_columns_refreshed, record_struct_scan, record_to_logical_schema,
    record_variable_data_bytes, reset, scoped_timer, site_loop_scoped, site_predict_scoped,
    snapshot,
};
