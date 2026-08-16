use std::sync::Arc;

use futures::StreamExt;
use stillflow_connectors::ConnectorRegistry;
use stillflow_core::{
    BatchEnvelope, LogicalSchema, LogicalSchemaFingerprint, ReadRequest, RequestContext,
    SourceAsset, SourceConnection, MAX_BATCH_BYTES,
};
use stillflow_plan::LogicalPlan;
use stillflow_storage::{SnapshotDraft, SnapshotManifest, SnapshotWriter};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::{map_context_error, EngineError};
use crate::ffi::{dataframe_to_record_batch, record_batch_to_dataframe};
use crate::memory::{MemoryReport, MemoryTracker};
use crate::predict::{largest_feasible_k, PredictedSchema};
use crate::preflight::{self, PreparedPlan};
use crate::remainder::CanonicalRebatcher;
use crate::{
    ExecutionIdentities, ExecutionRequest, ENGINE_DEFAULT_DEADLINE, ENGINE_MAX_DEADLINE,
    MAX_ENGINE_CONCURRENT_RUNS,
};

pub struct ExecutionEngine {
    pub(crate) registry: ConnectorRegistry,
    pub(crate) run_gate: Arc<Semaphore>,
}

impl ExecutionEngine {
    pub fn new(registry: ConnectorRegistry) -> Self {
        Self {
            registry,
            run_gate: Arc::new(Semaphore::new(MAX_ENGINE_CONCURRENT_RUNS as usize)),
        }
    }

    pub async fn preflight(
        &self,
        plan: &LogicalPlan,
        connection: &SourceConnection,
        asset: &SourceAsset,
        schema_override: Option<&LogicalSchema>,
        context: &RequestContext,
    ) -> Result<PreparedPlan, EngineError> {
        preflight::preflight(
            &self.registry,
            plan,
            connection,
            asset,
            schema_override,
            context,
            None,
        )
        .await
    }

    pub async fn preview(
        &self,
        request: crate::PreviewRequest,
    ) -> Result<crate::PreviewResult, EngineError> {
        crate::preview::preview(self, request)
            .await
            .map(|(result, _report)| result)
    }

    #[cfg(test)]
    pub(crate) async fn preview_tracked(
        &self,
        request: crate::PreviewRequest,
    ) -> Result<(crate::PreviewResult, crate::memory::MemoryReport), EngineError> {
        crate::preview::preview(self, request).await
    }

    pub async fn materialize(
        &self,
        request: ExecutionRequest<'_>,
    ) -> Result<SnapshotManifest, EngineError> {
        self.materialize_inner(request)
            .await
            .map(|(manifest, _)| manifest)
    }

    #[cfg(test)]
    pub(crate) async fn materialize_tracked(
        &self,
        request: ExecutionRequest<'_>,
    ) -> Result<(SnapshotManifest, MemoryReport), EngineError> {
        self.materialize_inner(request).await
    }

    async fn materialize_inner(
        &self,
        request: ExecutionRequest<'_>,
    ) -> Result<(SnapshotManifest, MemoryReport), EngineError> {
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

        let permit = Arc::clone(&self.run_gate)
            .try_acquire_owned()
            .map_err(|_| EngineError::Busy)?;
        self.run_with_permit(request, context, permit).await
    }

    #[cfg(test)]
    pub(crate) fn try_hold_run_gate(&self) -> Result<OwnedSemaphorePermit, EngineError> {
        Arc::clone(&self.run_gate)
            .try_acquire_owned()
            .map_err(|_| EngineError::Busy)
    }

    async fn run_with_permit(
        &self,
        request: ExecutionRequest<'_>,
        context: RequestContext,
        _permit: OwnedSemaphorePermit,
    ) -> Result<(SnapshotManifest, MemoryReport), EngineError> {
        let prepared = self
            .preflight(
                &request.plan,
                &request.connection,
                &request.asset,
                request.schema_override.as_ref(),
                &context,
            )
            .await?;
        validate_identities(&request.identities, request.asset.id)?;

        let mut tracker = MemoryTracker::new();
        let draft = SnapshotDraft::try_new(
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
            .begin_snapshot(draft, request.identities.started_at)
            .map_err(EngineError::from_storage)?;

        let result = self
            .stream_and_publish(request, &context, &prepared, &mut writer, &mut tracker)
            .await;
        match result {
            Ok(()) => {
                let manifest = writer.commit().map_err(EngineError::from_storage)?;
                Ok((manifest, tracker.report()))
            }
            Err(error) => {
                drop(writer);
                Err(error)
            }
        }
    }

    async fn stream_and_publish(
        &self,
        request: ExecutionRequest<'_>,
        context: &RequestContext,
        prepared: &PreparedPlan,
        writer: &mut SnapshotWriter,
        tracker: &mut MemoryTracker,
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
        let mut stream = self
            .registry
            .read_batches(&request.connection, read)
            .await
            .map_err(EngineError::from_connector)?;

        let mut rebatcher = CanonicalRebatcher::new(
            Arc::new(prepared.materialize_schema.clone()),
            request.asset.id,
            request.batch_size,
        )?;
        tracker.hold_remainder(rebatcher.remainder_bytes())?;
        let predicted = PredictedSchema::from_scan_output(&prepared.scan_output);
        let output_schema =
            stillflow_core::logical_schema_to_arrow(&prepared.materialize_schema)
                .map_err(|_| EngineError::Internal("materialize arrow schema failed"))?;
        let expected_fingerprint =
            LogicalSchemaFingerprint::try_from_schema(&prepared.expected_connector)
                .map_err(|_| EngineError::Internal("connector schema fingerprint failed"))?;

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
            consume_envelope(
                envelope,
                prepared,
                &predicted,
                &output_schema,
                &mut rebatcher,
                writer,
                tracker,
                context,
            )?;
            tracker.drop_envelope()?;
        }

        rebatcher.finish(tracker, |envelope, tracker| {
            append_envelope(writer, envelope, tracker, context)
        })?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn consume_envelope(
    envelope: BatchEnvelope,
    prepared: &PreparedPlan,
    predicted: &PredictedSchema,
    output_schema: &arrow_schema::SchemaRef,
    rebatcher: &mut CanonicalRebatcher,
    writer: &mut SnapshotWriter,
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
        let batch = {
            let _polars_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::Polars);
            let slice = envelope.payload().slice(offset, k);
            let frame = record_batch_to_dataframe(&slice)?;
            let working_bytes = frame.estimated_size();
            tracker.hold_polars(working_bytes.max(slice.get_array_memory_size()))?;
            tracker.record_chunk(k, rebatcher.remainder_live());
            if working_bytes > MAX_BATCH_BYTES {
                return Err(EngineError::Internal(
                    "polars working set exceeded MAX_BATCH_BYTES",
                ));
            }
            let (transformed, deferred) =
                crate::lower::transform(frame, &prepared.scan_output, &prepared.steps)?;
            let transformed_bytes = transformed.estimated_size();
            tracker.hold_polars(transformed_bytes)?;
            dataframe_to_record_batch(
                transformed,
                &prepared.materialize_schema,
                output_schema,
                &deferred,
            )?
        };
        offset += k;
        tracker.drop_polars()?;
        rebatcher.push(batch, tracker, |envelope, tracker| {
            append_envelope(writer, envelope, tracker, context)
        })?;
    }
    Ok(())
}

fn append_envelope(
    writer: &mut SnapshotWriter,
    envelope: BatchEnvelope,
    _tracker: &mut MemoryTracker,
    context: &RequestContext,
) -> Result<(), EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    let _storage_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::StorageAppend);
    writer.append(&envelope).map_err(EngineError::from_storage)
}

fn validate_identities(
    identities: &ExecutionIdentities,
    source_asset_id: Uuid,
) -> Result<(), EngineError> {
    if identities.snapshot_id.is_nil()
        || identities.dataset_id.is_nil()
        || identities.session_id.is_nil()
        || source_asset_id.is_nil()
        || identities.lineage.iter().any(Uuid::is_nil)
    {
        return Err(EngineError::InvalidPlan(
            "injected identities must not be nil",
        ));
    }
    if identities.quality_score.is_some_and(|score| score > 100) {
        return Err(EngineError::InvalidPlan("quality score is outside 0..=100"));
    }
    if identities.created_at > identities.started_at {
        return Err(EngineError::InvalidPlan(
            "created_at must not be after started_at",
        ));
    }
    Ok(())
}
