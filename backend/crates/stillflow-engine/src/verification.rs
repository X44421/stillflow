//! Experimental `materialize_verification` orchestration.

use std::sync::Arc;

use futures::StreamExt;
use polars::prelude::DataFrame;
use sha2::{Digest, Sha256};
use stillflow_core::{
    ArtifactKind, ArtifactProvenanceDraft, ArtifactProvenanceInput, BatchEnvelope, LogicalSchema,
    LogicalSchemaFingerprint, ReadRequest, RequestContext, SourceRowRef, MAX_BATCH_BYTES,
};
use stillflow_plan::{LogicalPlan, PlanNodeId, Rule, ValidationSeverity};
use stillflow_storage::{
    DedupIndex, DedupInsert, SnapshotDraft, VerificationBundle, VerificationBundleDraft,
    VerificationBundleWriter, MAX_SNAPSHOT_ROWS,
};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;
use uuid::Uuid;

use crate::canonical::canonical_key_bytes;
use crate::error::{map_context_error, EngineError};
use crate::ffi::{dataframe_to_record_batch, record_batch_to_dataframe};
use crate::lower::{self, PredicateOutcome};
use crate::memory::MemoryTracker;
use crate::predict::{largest_feasible_k, PredictedSchema};
use crate::preflight::{CompiledStep, PreparedPlan};
use crate::remainder::CanonicalRebatcher;
use crate::report::{
    self, DedupRuleAccumulator, DuplicateFindingRow, ValidationFindingRow,
    ValidationRuleAccumulator,
};
use crate::{
    ExecutionEngine, VerificationRequest, ENGINE_DEFAULT_DEADLINE, ENGINE_MAX_DEADLINE,
    MAX_VALIDATION_FINDINGS_PER_ROW, VERIFICATION_CONTRACT_VERSION,
};

struct PendingReject {
    ordinal: u64,
    kind: &'static str,
    node_id: Uuid,
    rule_ordinal: u32,
    scan_row: usize,
}

struct RoutingState {
    next_ordinal: u64,
    validation_rules: Vec<ValidationRuleAccumulator>,
    validation_findings: Vec<ValidationFindingRow>,
    dedup_rules: Vec<DedupRuleAccumulator>,
    duplicate_findings: Vec<DuplicateFindingRow>,
    pending_rejected_rows: Option<DataFrame>,
    pending_rejected_ordinals: Vec<u64>,
    pending_rejected_kinds: Vec<String>,
    pending_rejected_nodes: Vec<Uuid>,
    pending_rejected_rules: Vec<u32>,
}

pub(crate) async fn materialize_verification(
    engine: &ExecutionEngine,
    request: VerificationRequest<'_>,
) -> Result<VerificationBundle, EngineError> {
    let mut context = request.context.clone();
    if context.deadline().is_none() {
        context = RequestContext::with_cancellation_and_deadline(
            context.cancellation().clone(),
            Instant::now() + ENGINE_DEFAULT_DEADLINE,
        );
    }
    context.ensure_active().map_err(map_context_error)?;
    if request.batch_size < ReadRequest::MIN_BATCH_SIZE
        || request.batch_size > ReadRequest::MAX_BATCH_SIZE
    {
        return Err(EngineError::BoundExceeded(
            "batch_size is outside 1..=65536",
        ));
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining > ENGINE_MAX_DEADLINE)
    {
        return Err(EngineError::BoundExceeded(
            "request deadline exceeds ENGINE_MAX_DEADLINE",
        ));
    }

    let permit = Arc::clone(&engine.run_gate)
        .try_acquire_owned()
        .map_err(|_| EngineError::Busy)?;
    run_with_permit(engine, request, context, permit).await
}

async fn run_with_permit(
    engine: &ExecutionEngine,
    request: VerificationRequest<'_>,
    context: RequestContext,
    _permit: OwnedSemaphorePermit,
) -> Result<VerificationBundle, EngineError> {
    let prepared = crate::preflight::preflight(
        &engine.registry,
        &request.plan,
        &request.connection,
        &request.asset,
        request.schema_override.as_ref(),
        &context,
        crate::preflight::PreflightMode::Verification,
    )
    .await?;
    let plan_digest = canonical_plan_digest(&request.plan)?;
    if plan_digest != request.identities.canonical_plan_digest {
        return Err(EngineError::InvalidPlan(
            "canonical plan digest does not match the injected value",
        ));
    }
    validate_verification_identities(&request, &prepared)?;
    context.ensure_active().map_err(map_context_error)?;

    let fingerprint = request
        .plan
        .fingerprint()
        .map_err(|_| EngineError::InvalidPlan("plan fingerprint failed"))?;
    let provenance = ArtifactProvenanceDraft {
        input: ArtifactProvenanceInput {
            run_id: request.identities.run_id,
            bundle_id: request.identities.bundle_id,
            artifact_id: request.identities.bundle_artifact_id,
            artifact_kind: ArtifactKind::VerificationBundle,
            session_id: request.identities.session_id,
            input: request.identities.logical_input,
            lineage: request.identities.lineage.clone(),
            created_at: request.identities.created_at,
            started_at: request.identities.started_at,
            committed_at: request.identities.committed_at,
        },
        plan_fingerprint: *fingerprint.as_bytes(),
        canonical_plan_digest: plan_digest,
        engine_contract_version: crate::ENGINE_CONTRACT_VERSION,
        engine_build: env!("CARGO_PKG_VERSION").to_owned(),
        verification_contract_version: VERIFICATION_CONTRACT_VERSION,
    };
    let accepted = SnapshotDraft::try_new(
        request.identities.snapshot_id,
        request.identities.dataset_id,
        request.identities.session_id,
        request.asset.id,
        prepared.materialize_schema.clone(),
        request.identities.lineage.clone(),
        request.identities.quality_score,
        request.identities.created_at,
    )
    .map_err(EngineError::from_storage)?;
    let mut writer = request
        .store
        .begin_verification_bundle(
            VerificationBundleDraft {
                provenance: provenance.clone(),
                accepted,
                validation_report_artifact_id: request.identities.validation_report_artifact_id,
                rejected_rows_artifact_id: request.identities.rejected_rows_artifact_id,
                deduplication_report_artifact_id: request
                    .identities
                    .deduplication_report_artifact_id,
            },
            request.identities.started_at,
        )
        .map_err(EngineError::from_storage)?;
    bind_report_schemas(&mut writer, &prepared.scan_output, &request)?;
    let mut dedup = request
        .store
        .open_dedup_index(
            request.identities.run_id,
            request.identities.bundle_id,
            request.identities.started_at,
        )
        .map_err(EngineError::from_storage)?;

    let result = stream_verification(
        engine,
        &request,
        &context,
        &prepared,
        &provenance,
        &mut writer,
        &mut dedup,
    )
    .await;
    match result {
        Ok(()) => {
            context.ensure_active().map_err(map_context_error)?;
            dedup
                .close_and_delete()
                .map_err(EngineError::from_storage)?;
            context.ensure_active().map_err(map_context_error)?;
            writer
                .commit(request.identities.committed_at)
                .map_err(EngineError::from_storage)
        }
        Err(error) => {
            drop(writer);
            drop(dedup);
            Err(error)
        }
    }
}

async fn stream_verification(
    engine: &ExecutionEngine,
    request: &VerificationRequest<'_>,
    context: &RequestContext,
    prepared: &PreparedPlan,
    provenance: &ArtifactProvenanceDraft,
    writer: &mut VerificationBundleWriter,
    dedup: &mut DedupIndex,
) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    let read = ReadRequest {
        context: context.clone(),
        asset: request.asset.clone(),
        schema_override: Some(prepared.expected_connector.clone()),
        projection: prepared
            .push_projection
            .then(|| prepared.scan_projection.clone()),
        filter: None,
        checkpoint: None,
        batch_size: request.batch_size,
    };
    let mut stream = engine
        .registry
        .read_batches(&request.connection, read)
        .await
        .map_err(EngineError::from_connector)?;

    let mut tracker = MemoryTracker::new();
    let mut accepted = CanonicalRebatcher::new(
        Arc::new(prepared.materialize_schema.clone()),
        request.asset.id,
        request.batch_size,
    )?;
    tracker.hold_remainder(accepted.remainder_bytes())?;
    let rejected_schema = report::rejected_schema(&prepared.scan_output)?;
    let mut rejected = CanonicalRebatcher::new(
        Arc::new(rejected_schema.clone()),
        request.asset.id,
        request.batch_size,
    )?;
    let predicted = PredictedSchema::from_scan_output(&prepared.scan_output);
    let output_schema = stillflow_core::logical_schema_to_arrow(&prepared.materialize_schema)
        .map_err(|_| EngineError::Internal("materialize arrow schema failed"))?;
    let rejected_arrow = stillflow_core::logical_schema_to_arrow(&rejected_schema)
        .map_err(|_| EngineError::Internal("rejected arrow schema failed"))?;
    let expected_fingerprint =
        LogicalSchemaFingerprint::try_from_schema(&prepared.expected_connector)
            .map_err(|_| EngineError::Internal("connector schema fingerprint failed"))?;
    let mut routing = RoutingState::new(prepared)?;

    while let Some(item) = stream.next().await {
        context.ensure_active().map_err(map_context_error)?;
        let envelope = item.map_err(EngineError::from_connector)?;
        if envelope.schema() != &prepared.expected_connector
            || envelope.schema_fingerprint() != expected_fingerprint
        {
            return Err(EngineError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }
        tracker.hold_envelope(envelope.byte_count())?;
        consume_verification_envelope(
            envelope,
            prepared,
            provenance,
            &predicted,
            &output_schema,
            &rejected_schema,
            &rejected_arrow,
            &mut accepted,
            &mut rejected,
            writer,
            dedup,
            &mut routing,
            &mut tracker,
            context,
        )?;
        tracker.drop_envelope()?;
    }

    accepted.finish(&mut tracker, |envelope, tracker| {
        append_accepted(writer, envelope, tracker, context)
    })?;
    rejected.finish(&mut tracker, |envelope, tracker| {
        append_rejected(writer, envelope, tracker, context)
    })?;
    publish_reports(writer, provenance, &routing, context)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn consume_verification_envelope(
    envelope: BatchEnvelope,
    prepared: &PreparedPlan,
    provenance: &ArtifactProvenanceDraft,
    predicted: &PredictedSchema,
    output_schema: &arrow_schema::SchemaRef,
    rejected_schema: &LogicalSchema,
    rejected_arrow: &arrow_schema::SchemaRef,
    accepted: &mut CanonicalRebatcher,
    rejected: &mut CanonicalRebatcher,
    writer: &mut VerificationBundleWriter,
    dedup: &mut DedupIndex,
    routing: &mut RoutingState,
    tracker: &mut MemoryTracker,
    context: &RequestContext,
) -> Result<(), EngineError> {
    let mut offset = 0_usize;
    let row_count = envelope.payload().num_rows();
    while offset < row_count {
        context.ensure_active().map_err(map_context_error)?;
        let k = largest_feasible_k(
            row_count,
            offset,
            envelope.payload().columns(),
            predicted,
            &prepared.steps,
        )?;
        let slice = envelope.payload().slice(offset, k);
        let frame = {
            let _polars_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::Polars);
            let frame = record_batch_to_dataframe(&slice)?;
            tracker.hold_polars(frame.estimated_size().max(slice.get_array_memory_size()))?;
            tracker.record_chunk(k, accepted.remainder_live());
            if frame.estimated_size() > MAX_BATCH_BYTES {
                return Err(EngineError::Internal(
                    "polars working set exceeded MAX_BATCH_BYTES",
                ));
            }
            frame
        };
        offset += k;
        let routed = route_chunk(frame, prepared, provenance, routing, dedup)?;
        tracker.drop_polars()?;
        if routed.accepted.height() > 0 {
            let batch = dataframe_to_record_batch(
                routed.accepted,
                &prepared.materialize_schema,
                output_schema,
                &routed.deferred,
            )?;
            accepted.push(batch, tracker, |envelope, tracker| {
                append_accepted(writer, envelope, tracker, context)
            })?;
        }
        if let Some(rejected_rows) = routed.rejected {
            let batch = rejected_batch(
                rejected_rows,
                &prepared.scan_output,
                rejected_schema,
                rejected_arrow,
                provenance,
                &routed.rejected_ordinals,
                &routed.rejected_kinds,
                &routed.rejected_nodes,
                &routed.rejected_rules,
            )?;
            rejected.push(batch, tracker, |envelope, tracker| {
                append_rejected(writer, envelope, tracker, context)
            })?;
        }
    }
    Ok(())
}

struct RoutedChunk {
    accepted: DataFrame,
    deferred: Vec<(String, stillflow_core::ScalarValue)>,
    rejected: Option<DataFrame>,
    rejected_ordinals: Vec<u64>,
    rejected_kinds: Vec<String>,
    rejected_nodes: Vec<Uuid>,
    rejected_rules: Vec<u32>,
}

fn route_chunk(
    frame: DataFrame,
    prepared: &PreparedPlan,
    provenance: &ArtifactProvenanceDraft,
    routing: &mut RoutingState,
    dedup: &mut DedupIndex,
) -> Result<RoutedChunk, EngineError> {
    let scan_steps = &prepared.steps[..prepared.scan_step_count];
    let later_steps = &prepared.steps[prepared.scan_step_count..];
    let (mut working, mut deferred) =
        crate::lower::transform(frame, &prepared.scan_output, scan_steps)?;
    let mut scan_df = working.clone();
    let mut ordinals = assign_ordinals(working.height(), routing)?;
    let mut schema = prepared.scan_output.clone();

    for step in later_steps {
        match step {
            CompiledStep::Project { columns } => {
                working = crate::lower::transform(
                    working,
                    &schema,
                    &[CompiledStep::Project {
                        columns: columns.clone(),
                    }],
                )?
                .0;
                schema = crate::preflight::project_schema(&schema, columns)?;
            }
            CompiledStep::Filter { predicate } => {
                let outcomes = lower::predicate_outcomes(&working, predicate, &schema)?;
                let keep: Vec<bool> = outcomes
                    .iter()
                    .map(|outcome| matches!(outcome, PredicateOutcome::True))
                    .collect();
                working = lower::filter_rows(working, &keep)?;
                scan_df = lower::filter_rows(scan_df, &keep)?;
                retain_ordinals(&mut ordinals, &keep);
            }
            CompiledStep::Rules { node_id, rules } => {
                apply_verification_rules(
                    &mut working,
                    &mut scan_df,
                    &mut ordinals,
                    &mut schema,
                    &mut deferred,
                    *node_id,
                    rules,
                    provenance,
                    routing,
                    dedup,
                )?;
            }
        }
    }

    Ok(RoutedChunk {
        accepted: working,
        deferred,
        rejected: routing.take_rejected_frame(),
        rejected_ordinals: std::mem::take(&mut routing.pending_rejected_ordinals),
        rejected_kinds: std::mem::take(&mut routing.pending_rejected_kinds),
        rejected_nodes: std::mem::take(&mut routing.pending_rejected_nodes),
        rejected_rules: std::mem::take(&mut routing.pending_rejected_rules),
    })
}

fn assign_ordinals(height: usize, routing: &mut RoutingState) -> Result<Vec<u64>, EngineError> {
    let mut ordinals = Vec::with_capacity(height);
    for _ in 0..height {
        let ordinal = routing.next_ordinal;
        if ordinal >= MAX_SNAPSHOT_ROWS {
            return Err(EngineError::BoundExceeded(
                "logical scan output exceeded MAX_SNAPSHOT_ROWS",
            ));
        }
        routing.next_ordinal = routing
            .next_ordinal
            .checked_add(1)
            .ok_or(EngineError::BoundExceeded("source_row_ordinal overflow"))?;
        ordinals.push(ordinal);
    }
    Ok(ordinals)
}

#[allow(clippy::too_many_arguments)]
fn apply_verification_rules(
    working: &mut DataFrame,
    scan_df: &mut DataFrame,
    ordinals: &mut Vec<u64>,
    schema: &mut LogicalSchema,
    deferred: &mut Vec<(String, stillflow_core::ScalarValue)>,
    node_id: PlanNodeId,
    rules: &[Rule],
    provenance: &ArtifactProvenanceDraft,
    routing: &mut RoutingState,
    dedup: &mut DedupIndex,
) -> Result<(), EngineError> {
    for (rule_ordinal, rule) in rules.iter().enumerate() {
        let rule_ordinal = u32::try_from(rule_ordinal)
            .map_err(|_| EngineError::BoundExceeded("rule ordinal overflow"))?;
        match rule {
            Rule::Validate {
                predicate,
                severity,
                ..
            } => apply_validate(
                working,
                scan_df,
                ordinals,
                schema,
                node_id.as_uuid(),
                rule_ordinal,
                predicate,
                *severity,
                provenance,
                routing,
            )?,
            Rule::Deduplicate { keys } => apply_dedup(
                working,
                scan_df,
                ordinals,
                schema,
                node_id.as_uuid(),
                rule_ordinal,
                keys,
                provenance,
                routing,
                dedup,
            )?,
            other => {
                *working = crate::lower::apply_rule(
                    std::mem::replace(working, DataFrame::empty()),
                    schema,
                    deferred,
                    other,
                )?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_validate(
    working: &mut DataFrame,
    scan_df: &mut DataFrame,
    ordinals: &mut Vec<u64>,
    schema: &LogicalSchema,
    node_id: Uuid,
    rule_ordinal: u32,
    predicate: &stillflow_core::Expr,
    severity: ValidationSeverity,
    provenance: &ArtifactProvenanceDraft,
    routing: &mut RoutingState,
) -> Result<(), EngineError> {
    let outcomes = lower::predicate_outcomes(working, predicate, schema)?;
    let mut keep = Vec::with_capacity(outcomes.len());
    let mut rejects = Vec::new();
    for (index, outcome) in outcomes.iter().enumerate() {
        let rule = routing
            .validation_rule_mut(node_id, rule_ordinal)
            .ok_or(EngineError::Internal("validation rule accumulator missing"))?;
        rule.evaluated_count = rule.evaluated_count.saturating_add(1);
        let ordinal = ordinals[index];
        match outcome {
            PredicateOutcome::True => {
                rule.pass_count = rule.pass_count.saturating_add(1);
                keep.push(true);
            }
            PredicateOutcome::False | PredicateOutcome::Null => {
                if matches!(outcome, PredicateOutcome::Null) {
                    rule.null_count = rule.null_count.saturating_add(1);
                } else {
                    rule.false_count = rule.false_count.saturating_add(1);
                }
                rule.fail_count = rule.fail_count.saturating_add(1);
                let is_null = matches!(outcome, PredicateOutcome::Null);
                match severity {
                    ValidationSeverity::Warning => {
                        rule.warning_count = rule.warning_count.saturating_add(1);
                        keep.push(true);
                    }
                    ValidationSeverity::Error => {
                        rule.error_count = rule.error_count.saturating_add(1);
                        keep.push(false);
                        rejects.push(PendingReject {
                            ordinal,
                            kind: "validation_error",
                            node_id,
                            rule_ordinal,
                            scan_row: index,
                        });
                    }
                }
                routing.push_validation_finding(
                    provenance,
                    ordinal,
                    node_id,
                    rule_ordinal,
                    severity,
                    if is_null { "null" } else { "false" },
                )?;
            }
        }
    }
    apply_keeps(working, scan_df, ordinals, &keep, rejects, routing)
}

#[allow(clippy::too_many_arguments)]
fn apply_dedup(
    working: &mut DataFrame,
    scan_df: &mut DataFrame,
    ordinals: &mut Vec<u64>,
    schema: &LogicalSchema,
    node_id: Uuid,
    rule_ordinal: u32,
    keys: &[stillflow_core::ColumnId],
    provenance: &ArtifactProvenanceDraft,
    routing: &mut RoutingState,
    dedup: &mut DedupIndex,
) -> Result<(), EngineError> {
    let mut keep = Vec::with_capacity(working.height());
    let mut rejects = Vec::new();
    for (index, ordinal) in ordinals.iter().copied().enumerate() {
        let rule = routing
            .dedup_rule_mut(node_id, rule_ordinal)
            .ok_or(EngineError::Internal("dedup rule accumulator missing"))?;
        rule.evaluated_count = rule.evaluated_count.saturating_add(1);
        let key_bytes = canonical_key_bytes(working, index, schema, keys)?;
        match dedup
            .insert_first(node_id, rule_ordinal, &key_bytes, ordinal)
            .map_err(EngineError::from_storage)?
        {
            DedupInsert::Inserted { .. } => {
                rule.unique_count = rule.unique_count.saturating_add(1);
                keep.push(true);
            }
            DedupInsert::Duplicate {
                first_source_row_ordinal,
            } => {
                rule.duplicate_count = rule.duplicate_count.saturating_add(1);
                routing.duplicate_findings.push(DuplicateFindingRow {
                    source: SourceRowRef {
                        input: provenance.input.input,
                        source_row_ordinal: ordinal,
                    },
                    first_source_row_ordinal,
                    node_id,
                    rule_ordinal,
                    key_column_count: u32::try_from(keys.len())
                        .map_err(|_| EngineError::BoundExceeded("dedup key count overflow"))?,
                    encoded_key_byte_count: u32::try_from(key_bytes.len()).map_err(|_| {
                        EngineError::BoundExceeded("encoded key byte count overflow")
                    })?,
                });
                keep.push(false);
                rejects.push(PendingReject {
                    ordinal,
                    kind: "duplicate",
                    node_id,
                    rule_ordinal,
                    scan_row: index,
                });
            }
        }
    }
    apply_keeps(working, scan_df, ordinals, &keep, rejects, routing)
}

fn apply_keeps(
    working: &mut DataFrame,
    scan_df: &mut DataFrame,
    ordinals: &mut Vec<u64>,
    keep: &[bool],
    rejects: Vec<PendingReject>,
    routing: &mut RoutingState,
) -> Result<(), EngineError> {
    routing.store_rejects(scan_df, &rejects)?;
    if keep.iter().all(|keep_row| *keep_row) {
        return Ok(());
    }
    *working = lower::filter_rows(std::mem::replace(working, DataFrame::empty()), keep)?;
    *scan_df = lower::filter_rows(std::mem::replace(scan_df, DataFrame::empty()), keep)?;
    retain_ordinals(ordinals, keep);
    Ok(())
}

fn retain_ordinals(ordinals: &mut Vec<u64>, keep: &[bool]) {
    *ordinals = ordinals
        .iter()
        .zip(keep)
        .filter_map(|(ordinal, keep_row)| keep_row.then_some(*ordinal))
        .collect();
}

impl RoutingState {
    fn new(prepared: &PreparedPlan) -> Result<Self, EngineError> {
        let mut validation_rules = Vec::new();
        let mut dedup_rules = Vec::new();
        for step in &prepared.steps {
            let CompiledStep::Rules { node_id, rules } = step else {
                continue;
            };
            for (rule_ordinal, rule) in rules.iter().enumerate() {
                let rule_ordinal = u32::try_from(rule_ordinal)
                    .map_err(|_| EngineError::BoundExceeded("rule ordinal overflow"))?;
                match rule {
                    Rule::Validate { message, .. } => {
                        validation_rules.push(ValidationRuleAccumulator {
                            node_id: node_id.as_uuid(),
                            rule_ordinal,
                            message: message.clone(),
                            evaluated_count: 0,
                            pass_count: 0,
                            fail_count: 0,
                            warning_count: 0,
                            error_count: 0,
                            null_count: 0,
                            false_count: 0,
                        });
                    }
                    Rule::Deduplicate { keys } => dedup_rules.push(DedupRuleAccumulator {
                        node_id: node_id.as_uuid(),
                        rule_ordinal,
                        key_column_count: u32::try_from(keys.len())
                            .map_err(|_| EngineError::BoundExceeded("dedup key count overflow"))?,
                        evaluated_count: 0,
                        unique_count: 0,
                        duplicate_count: 0,
                    }),
                    _ => {}
                }
            }
        }
        Ok(Self {
            next_ordinal: 0,
            validation_rules,
            validation_findings: Vec::new(),
            dedup_rules,
            duplicate_findings: Vec::new(),
            pending_rejected_rows: None,
            pending_rejected_ordinals: Vec::new(),
            pending_rejected_kinds: Vec::new(),
            pending_rejected_nodes: Vec::new(),
            pending_rejected_rules: Vec::new(),
        })
    }

    fn validation_rule_mut(
        &mut self,
        node_id: Uuid,
        rule_ordinal: u32,
    ) -> Option<&mut ValidationRuleAccumulator> {
        self.validation_rules
            .iter_mut()
            .find(|rule| rule.node_id == node_id && rule.rule_ordinal == rule_ordinal)
    }

    fn dedup_rule_mut(
        &mut self,
        node_id: Uuid,
        rule_ordinal: u32,
    ) -> Option<&mut DedupRuleAccumulator> {
        self.dedup_rules
            .iter_mut()
            .find(|rule| rule.node_id == node_id && rule.rule_ordinal == rule_ordinal)
    }

    fn push_validation_finding(
        &mut self,
        provenance: &ArtifactProvenanceDraft,
        ordinal: u64,
        node_id: Uuid,
        rule_ordinal: u32,
        severity: ValidationSeverity,
        predicate_outcome: &'static str,
    ) -> Result<(), EngineError> {
        let count = self
            .validation_findings
            .iter()
            .filter(|finding| finding.source.source_row_ordinal == ordinal)
            .count();
        if count >= MAX_VALIDATION_FINDINGS_PER_ROW {
            return Err(EngineError::BoundExceeded(
                "validation findings per row exceeded MAX_VALIDATION_FINDINGS_PER_ROW",
            ));
        }
        self.validation_findings.push(ValidationFindingRow {
            source: SourceRowRef {
                input: provenance.input.input,
                source_row_ordinal: ordinal,
            },
            node_id,
            rule_ordinal,
            severity,
            predicate_outcome,
        });
        Ok(())
    }

    fn store_rejects(
        &mut self,
        scan_df: &DataFrame,
        rejects: &[PendingReject],
    ) -> Result<(), EngineError> {
        if rejects.is_empty() {
            return Ok(());
        }
        let mut keep = vec![false; scan_df.height()];
        for reject in rejects {
            if let Some(slot) = keep.get_mut(reject.scan_row) {
                *slot = true;
            }
            self.pending_rejected_ordinals.push(reject.ordinal);
            self.pending_rejected_kinds.push(reject.kind.to_owned());
            self.pending_rejected_nodes.push(reject.node_id);
            self.pending_rejected_rules.push(reject.rule_ordinal);
        }
        let taken = lower::filter_rows(scan_df.clone(), &keep)?;
        if let Some(existing) = self.pending_rejected_rows.as_mut() {
            existing
                .vstack_mut(&taken)
                .map_err(|_| EngineError::Internal("rejected row concat failed"))?;
        } else {
            self.pending_rejected_rows = Some(taken);
        }
        Ok(())
    }

    fn take_rejected_frame(&mut self) -> Option<DataFrame> {
        self.pending_rejected_rows.take()
    }
}

#[allow(clippy::too_many_arguments)]
fn rejected_batch(
    rejected_rows: DataFrame,
    scan_schema: &LogicalSchema,
    _rejected_schema: &LogicalSchema,
    rejected_arrow: &arrow_schema::SchemaRef,
    provenance: &ArtifactProvenanceDraft,
    ordinals: &[u64],
    kinds: &[String],
    nodes: &[Uuid],
    rules: &[u32],
) -> Result<arrow_array::RecordBatch, EngineError> {
    let scan_arrow = stillflow_core::logical_schema_to_arrow(scan_schema)
        .map_err(|_| EngineError::Internal("scan output arrow schema failed"))?;
    let scan_batch = dataframe_to_record_batch(rejected_rows, scan_schema, &scan_arrow, &[])?;
    let mut columns = scan_batch.columns().to_vec();
    columns.extend(report::rejected_control_arrays(
        provenance, ordinals, kinds, nodes, rules,
    )?);
    arrow_array::RecordBatch::try_new(Arc::clone(rejected_arrow), columns)
        .map_err(|_| EngineError::Internal("rejected rows batch"))
}

fn append_accepted(
    writer: &mut VerificationBundleWriter,
    envelope: BatchEnvelope,
    _tracker: &mut MemoryTracker,
    context: &RequestContext,
) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    let _storage_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::StorageAppend);
    writer
        .append_accepted(&envelope)
        .map_err(EngineError::from_storage)
}

fn append_rejected(
    writer: &mut VerificationBundleWriter,
    envelope: BatchEnvelope,
    _tracker: &mut MemoryTracker,
    context: &RequestContext,
) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    let _storage_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::StorageAppend);
    writer
        .append_rejected_rows(&envelope)
        .map_err(EngineError::from_storage)
}

fn publish_reports(
    writer: &mut VerificationBundleWriter,
    provenance: &ArtifactProvenanceDraft,
    routing: &RoutingState,
    context: &RequestContext,
) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    if let Some(envelope) = report::validation_summary_batch(provenance, &routing.validation_rules)?
    {
        writer
            .append_validation_rule_summary(&envelope)
            .map_err(EngineError::from_storage)?;
    }
    if let Some(envelope) =
        report::validation_finding_batch(provenance, &routing.validation_findings)?
    {
        writer
            .append_validation_findings(&envelope)
            .map_err(EngineError::from_storage)?;
    }
    if let Some(envelope) = report::dedup_summary_batch(provenance, &routing.dedup_rules)? {
        writer
            .append_dedup_rule_summary(&envelope)
            .map_err(EngineError::from_storage)?;
    }
    if let Some(envelope) =
        report::duplicate_finding_batch(provenance, &routing.duplicate_findings)?
    {
        writer
            .append_duplicate_findings(&envelope)
            .map_err(EngineError::from_storage)?;
    }
    Ok(())
}

fn bind_report_schemas(
    writer: &mut VerificationBundleWriter,
    scan_output: &LogicalSchema,
    request: &VerificationRequest<'_>,
) -> Result<(), EngineError> {
    writer
        .bind_section_schema(
            request.identities.validation_report_artifact_id,
            stillflow_storage::ArtifactSectionId::ValidationRuleSummary,
            report::validation_summary_schema()?,
        )
        .map_err(EngineError::from_storage)?;
    writer
        .bind_section_schema(
            request.identities.validation_report_artifact_id,
            stillflow_storage::ArtifactSectionId::ValidationFinding,
            report::validation_finding_schema()?,
        )
        .map_err(EngineError::from_storage)?;
    writer
        .bind_section_schema(
            request.identities.deduplication_report_artifact_id,
            stillflow_storage::ArtifactSectionId::DedupRuleSummary,
            report::dedup_summary_schema()?,
        )
        .map_err(EngineError::from_storage)?;
    writer
        .bind_section_schema(
            request.identities.deduplication_report_artifact_id,
            stillflow_storage::ArtifactSectionId::DuplicateFinding,
            report::duplicate_finding_schema()?,
        )
        .map_err(EngineError::from_storage)?;
    if let Some(artifact_id) = request.identities.rejected_rows_artifact_id {
        writer
            .bind_section_schema(
                artifact_id,
                stillflow_storage::ArtifactSectionId::RejectedRows,
                report::rejected_schema(scan_output)?,
            )
            .map_err(EngineError::from_storage)?;
    }
    Ok(())
}

fn validate_verification_identities(
    request: &VerificationRequest<'_>,
    prepared: &PreparedPlan,
) -> Result<(), EngineError> {
    let identities = &request.identities;
    let mut ids = vec![
        identities.run_id,
        identities.bundle_id,
        identities.bundle_artifact_id,
        identities.snapshot_id,
        identities.dataset_id,
        identities.validation_report_artifact_id,
        identities.deduplication_report_artifact_id,
        identities.session_id,
    ];
    if let Some(rejected) = identities.rejected_rows_artifact_id {
        ids.push(rejected);
    }
    if ids.iter().any(Uuid::is_nil) {
        return Err(EngineError::InvalidPlan(
            "injected identities must not be nil",
        ));
    }
    let unique = ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != ids.len() {
        return Err(EngineError::InvalidPlan(
            "verification identities must be pairwise distinct",
        ));
    }
    if identities.lineage.iter().any(Uuid::is_nil) {
        return Err(EngineError::InvalidPlan(
            "injected identities must not be nil",
        ));
    }
    if identities.quality_score.is_some_and(|score| score > 100) {
        return Err(EngineError::InvalidPlan("quality score is outside 0..=100"));
    }
    if identities.created_at > identities.started_at
        || identities.started_at > identities.committed_at
    {
        return Err(EngineError::InvalidPlan(
            "created_at must not be after started_at",
        ));
    }
    match identities.logical_input.input {
        stillflow_core::InputRef::Asset { asset_id } if asset_id == request.asset.id => {}
        _ => {
            return Err(EngineError::InvalidPlan(
                "logical input must be the bound source asset",
            ));
        }
    }
    for field in &prepared.scan_output.fields {
        if report::reserved_control_names().contains(&field.name.as_str())
            || matches!(
                field.id,
                report::REJECTED_INPUT_KIND_COLUMN_ID
                    | report::REJECTED_INPUT_ID_COLUMN_ID
                    | report::REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID
                    | report::REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID
                    | report::REJECTED_KIND_COLUMN_ID
                    | report::REJECTED_PLAN_FINGERPRINT_COLUMN_ID
                    | report::REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID
                    | report::REJECTED_NODE_ID_COLUMN_ID
                    | report::REJECTED_RULE_ORDINAL_COLUMN_ID
            )
        {
            return Err(EngineError::InvalidPlan(
                "source schema collides with reserved rejected-row control identities",
            ));
        }
    }
    Ok(())
}

fn canonical_plan_digest(plan: &LogicalPlan) -> Result<[u8; 32], EngineError> {
    let bytes = plan
        .canonical_bytes()
        .map_err(|_| EngineError::InvalidPlan("canonical plan bytes failed"))?;
    Ok(Sha256::digest(bytes).into())
}
