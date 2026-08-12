use std::collections::BTreeMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use stillflow_connectors::RawBatchStream;
use stillflow_core::{
    AssetMetadata, BatchEnvelope, BatchEnvelopeFactory, ConnectorError, ConnectorResult,
    ErrorCategory, LogicalSchema, RequestContext, SourceAsset,
};

use crate::access::{run_control, ObjectInfo, ObjectStorageAccess, StoreAccess};
use crate::schema::{logical_schema_from_source_arrow, ProjectionPlan};

#[derive(Clone)]
struct GuardedParquetReader {
    access: StoreAccess,
    key: String,
    expected: ObjectInfo,
    context: RequestContext,
}

impl AsyncFileReader for GuardedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let access = self.access.clone();
        let key = self.key.clone();
        let expected = self.expected.clone();
        let context = self.context.clone();
        async move {
            access
                .get_range_versioned(&key, range, &expected, &context)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let mut reader = ParquetMetaDataReader::new().with_metadata_options(metadata_options);
            if let Some(options) = options {
                let column_policy = options.column_index_policy();
                let offset_policy = options.offset_index_policy();
                if column_policy != PageIndexPolicy::Skip || offset_policy != PageIndexPolicy::Skip
                {
                    reader = reader
                        .with_column_index_policy(column_policy)
                        .with_offset_index_policy(offset_policy);
                }
            }
            let file_size = self.expected.size;
            Ok(Arc::new(reader.load_and_finish(self, file_size).await?))
        }
        .boxed()
    }
}

type ArrowBatchStream =
    Pin<Box<dyn Stream<Item = Result<arrow_array::RecordBatch, ParquetError>> + Send + 'static>>;

pub(crate) struct PreparedParquet {
    stream: ArrowBatchStream,
    plan: ProjectionPlan,
    envelope_factory: BatchEnvelopeFactory,
    context: RequestContext,
    timeout: std::time::Duration,
    sequence: u64,
}

impl PreparedParquet {
    pub(crate) fn output_schema(&self) -> LogicalSchema {
        self.plan.output_schema.clone()
    }

    pub(crate) async fn next_record_batch(
        &mut self,
    ) -> ConnectorResult<Option<arrow_array::RecordBatch>> {
        let context = self.context.clone();
        let next = run_control(&context, self.timeout, async {
            self.stream
                .next()
                .await
                .transpose()
                .map_err(map_parquet_error)
        })
        .await?;
        next.map(|batch| self.plan.adapt_batch(batch)).transpose()
    }

    async fn next_envelope(&mut self) -> ConnectorResult<Option<BatchEnvelope>> {
        let Some(batch) = self.next_record_batch().await? else {
            return Ok(None);
        };
        let envelope = self
            .envelope_factory
            .try_build(self.sequence, batch)
            .map_err(|_| {
                parquet_error(
                    ErrorCategory::InvalidData,
                    "decoded Parquet batch violates the public envelope bounds",
                )
            })?;
        self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
            parquet_error(
                ErrorCategory::InvalidData,
                "Parquet batch sequence exceeds the supported range",
            )
        })?;
        Ok(Some(envelope))
    }

    pub(crate) fn into_raw_stream(self) -> RawBatchStream {
        let stream = futures::stream::try_unfold(self, |mut state| async move {
            match state.next_envelope().await? {
                Some(envelope) => Ok(Some((envelope, state))),
                None => Ok(None),
            }
        });
        RawBatchStream::new(Box::pin(stream))
    }
}

pub(crate) async fn inspect_parquet(
    access: &StoreAccess,
    asset: &SourceAsset,
    context: &RequestContext,
) -> ConnectorResult<AssetMetadata> {
    let (info, builder, schema) = load_builder(access, asset, context).await?;
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
        parquet_error(
            ErrorCategory::InvalidData,
            "Parquet row count is outside the supported range",
        )
    })?;
    Ok(AssetMetadata {
        schema,
        format: "parquet".to_owned(),
        size_bytes: Some(info.size),
        row_count: Some(rows),
        modified_at: Some(info.last_modified),
        findings: Vec::new(),
        workbook: None,
    })
}

pub(crate) async fn prepare_parquet(
    access: &StoreAccess,
    asset: &SourceAsset,
    schema_override: Option<&LogicalSchema>,
    projection: Option<&[stillflow_core::ColumnId]>,
    batch_size: usize,
    max_rows: Option<usize>,
    context: &RequestContext,
) -> ConnectorResult<PreparedParquet> {
    let (_, builder, source_schema) = load_builder(access, asset, context).await?;
    let plan = ProjectionPlan::new(&source_schema, schema_override, projection)?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), plan.mask_indices.clone());
    let builder = builder.with_batch_size(batch_size).with_projection(mask);
    let builder = if let Some(max_rows) = max_rows {
        builder.with_limit(max_rows)
    } else {
        builder
    };
    let stream = builder.build().map_err(map_parquet_error)?;
    let envelope_factory =
        BatchEnvelopeFactory::try_new(Arc::new(plan.output_schema.clone()), asset.id).map_err(
            |_| {
                parquet_error(
                    ErrorCategory::InvalidData,
                    "projected Parquet schema cannot establish the batch boundary",
                )
            },
        )?;
    Ok(PreparedParquet {
        stream: Box::pin(stream),
        plan,
        envelope_factory,
        context: context.clone(),
        timeout: access.request_timeout(),
        sequence: 0,
    })
}

async fn load_builder(
    access: &StoreAccess,
    asset: &SourceAsset,
    context: &RequestContext,
) -> ConnectorResult<(
    ObjectInfo,
    ParquetRecordBatchStreamBuilder<GuardedParquetReader>,
    LogicalSchema,
)> {
    let info = access.head(&asset.locator.path, context).await?;
    if info.size < 12 {
        return Err(parquet_error(
            ErrorCategory::InvalidData,
            "Parquet object is too short to contain a valid footer",
        ));
    }
    let magic = access
        .get_range_versioned(&asset.locator.path, 0..4, &info, context)
        .await?;
    if magic.as_ref() != b"PAR1" {
        return Err(parquet_error(
            ErrorCategory::InvalidData,
            "Parquet object header magic is invalid",
        ));
    }
    let reader = GuardedParquetReader {
        access: access.clone(),
        key: asset.locator.path.clone(),
        expected: info.clone(),
        context: context.clone(),
    };
    let builder = run_control(context, access.request_timeout(), async {
        ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(map_parquet_error)
    })
    .await?;
    let schema = logical_schema_from_source_arrow(asset.id, builder.schema().as_ref())?;
    Ok((info, builder, schema))
}

fn map_parquet_error(error: ParquetError) -> ConnectorError {
    if let ParquetError::External(source) = error {
        return match source.downcast::<ConnectorError>() {
            Ok(error) => *error,
            Err(_) => parquet_error(
                ErrorCategory::InvalidData,
                "Parquet object could not be decoded",
            ),
        };
    }
    parquet_error(
        ErrorCategory::InvalidData,
        "Parquet object metadata or data is malformed",
    )
}

fn parquet_error(category: ErrorCategory, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, false, message, Vec::new(), BTreeMap::new())
}
