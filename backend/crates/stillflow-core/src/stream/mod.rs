use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::time::{Instant, Sleep};
use uuid::Uuid;

use crate::{
    BatchEnvelope, ConnectorError, ErrorCategory, LogicalSchema, LogicalSchemaFingerprint,
};

/// Result item carried by a connector batch stream.
pub type BatchItem = Result<BatchEnvelope, ConnectorError>;

/// Bounded asynchronous stream of versioned Arrow batch envelopes.
pub type BatchStream = Pin<Box<dyn Stream<Item = BatchItem> + Send>>;

/// Wraps an inner stream and enforces request and batch-boundary invariants.
struct CancellableBatchStream {
    inner: Option<BatchStream>,
    context: crate::RequestContext,
    expected_source_asset_id: Uuid,
    next_sequence: Option<u64>,
    stream_schema: Option<(LogicalSchemaFingerprint, Arc<LogicalSchema>)>,
    terminated: bool,
    deadline_sleep: Option<Pin<Box<Sleep>>>,
    cancellation_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl CancellableBatchStream {
    fn new(
        inner: BatchStream,
        context: crate::RequestContext,
        expected_source_asset_id: Uuid,
    ) -> Self {
        Self {
            inner: Some(inner),
            context,
            expected_source_asset_id,
            next_sequence: Some(0),
            stream_schema: None,
            terminated: false,
            deadline_sleep: None,
            cancellation_wait: None,
        }
    }

    fn terminal_error(&mut self, error: ConnectorError) -> Poll<Option<BatchItem>> {
        self.terminated = true;
        self.inner = None;
        Poll::Ready(Some(Err(error)))
    }

    fn finish(&mut self) -> Poll<Option<BatchItem>> {
        self.terminated = true;
        self.inner = None;
        Poll::Ready(None)
    }

    fn validate_envelope(&mut self, envelope: &BatchEnvelope) -> Result<(), ConnectorError> {
        if envelope.source_asset_id() != self.expected_source_asset_id {
            return Err(stream_error(
                ErrorCategory::InvalidData,
                format!(
                    "batch lineage {} does not match expected source asset {}",
                    envelope.source_asset_id(),
                    self.expected_source_asset_id
                ),
            ));
        }

        let Some(expected_sequence) = self.next_sequence else {
            return Err(stream_error(
                ErrorCategory::InvalidData,
                "batch sequence exceeded the version 1 range",
            ));
        };
        if envelope.sequence() != expected_sequence {
            return Err(stream_error(
                ErrorCategory::InvalidData,
                format!(
                    "batch sequence {} does not match expected sequence {expected_sequence}",
                    envelope.sequence()
                ),
            ));
        }

        match &self.stream_schema {
            None => {
                self.stream_schema = Some((
                    envelope.schema_fingerprint(),
                    Arc::clone(envelope.shared_schema()),
                ));
            }
            Some((fingerprint, schema))
                if *fingerprint != envelope.schema_fingerprint()
                    || schema.as_ref() != envelope.schema() =>
            {
                return Err(stream_error(
                    ErrorCategory::SchemaDrift,
                    format!("logical schema changed at batch sequence {expected_sequence}"),
                ));
            }
            Some(_) => {}
        }

        self.next_sequence = expected_sequence.checked_add(1);
        Ok(())
    }
}

impl Stream for CancellableBatchStream {
    type Item = BatchItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        if let Err(error) = self.context.ensure_active() {
            return self.terminal_error(error);
        }

        if self.cancellation_wait.is_none() {
            let cancel = self.context.cancellation().clone();
            self.cancellation_wait = Some(Box::pin(async move {
                cancel.cancelled().await;
            }));
        }
        let Some(cancellation_wait) = self.cancellation_wait.as_mut() else {
            return self.terminal_error(ConnectorError::internal(
                "cancellation wait initialization failed",
            ));
        };
        if let Poll::Ready(()) = cancellation_wait.as_mut().poll(cx) {
            return self.terminal_error(ConnectorError::cancelled());
        }

        if let Some(deadline) = self.context.deadline() {
            if Instant::now() >= deadline {
                return self.terminal_error(ConnectorError::timeout("request deadline exceeded"));
            }
            if self.deadline_sleep.is_none() {
                self.deadline_sleep = Some(Box::pin(tokio::time::sleep_until(deadline)));
            }
            let Some(deadline_sleep) = self.deadline_sleep.as_mut() else {
                return self.terminal_error(ConnectorError::internal(
                    "deadline wait initialization failed",
                ));
            };
            if let Poll::Ready(()) = deadline_sleep.as_mut().poll(cx) {
                return self.terminal_error(ConnectorError::timeout("request deadline exceeded"));
            }
        }

        let polled = match self.inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => return self.finish(),
        };
        match polled {
            Poll::Ready(Some(Ok(envelope))) => match self.validate_envelope(&envelope) {
                Ok(()) => Poll::Ready(Some(Ok(envelope))),
                Err(error) => self.terminal_error(error),
            },
            Poll::Ready(Some(Err(error))) => self.terminal_error(error),
            Poll::Ready(None) => self.finish(),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Builds a validated batch stream that honours cancellation and deadlines.
pub fn attach_request_context(
    inner: BatchStream,
    context: crate::RequestContext,
    expected_source_asset_id: Uuid,
) -> BatchStream {
    Box::pin(CancellableBatchStream::new(
        inner,
        context,
        expected_source_asset_id,
    ))
}

fn stream_error(category: ErrorCategory, message: impl Into<String>) -> ConnectorError {
    ConnectorError::with_category(
        category,
        false,
        message,
        Vec::new(),
        std::collections::BTreeMap::new(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use arrow_array::{Array, Int64Array, RecordBatch};
    use futures::{stream, StreamExt};
    use tokio::time::timeout;

    use super::*;
    use crate::{logical_schema_to_arrow, ColumnId, LogicalField, LogicalType};

    fn logical_schema(id: u128, name: &str) -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(id)),
                name,
                LogicalType::Int64,
                false,
            )
            .expect("field")])
            .expect("schema"),
        )
    }

    fn envelope(
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        values: Vec<i64>,
    ) -> BatchEnvelope {
        let arrow_schema = logical_schema_to_arrow(&schema).expect("Arrow schema");
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))])
            .expect("record batch");
        BatchEnvelope::try_new(schema, source_asset_id, sequence, batch).expect("envelope")
    }

    #[tokio::test]
    async fn cancelled_pending_stream_wakes_and_terminates_once() {
        let token = tokio_util::sync::CancellationToken::new();
        let context = crate::RequestContext::with_cancellation(token.clone());
        let inner: BatchStream = Box::pin(stream::pending());
        let mut wrapped = attach_request_context(inner, context, Uuid::from_u128(1));

        let token_for_cancel = token.clone();
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            token_for_cancel.cancel();
        });

        let first = timeout(Duration::from_secs(1), wrapped.next())
            .await
            .expect("should wake on cancellation")
            .expect("one item")
            .expect_err("cancelled");
        assert_eq!(first.category(), crate::ErrorCategory::Cancelled);
        assert!(
            wrapped.next().await.is_none(),
            "terminal error must not repeat"
        );

        cancel_task.await.expect("cancel task");
    }

    #[tokio::test]
    async fn expired_deadline_on_pending_stream_terminates_once() {
        let context =
            crate::RequestContext::with_deadline(Instant::now() + Duration::from_millis(20));
        let inner: BatchStream = Box::pin(stream::pending());
        let mut wrapped = attach_request_context(inner, context, Uuid::from_u128(1));

        let first = timeout(Duration::from_secs(1), wrapped.next())
            .await
            .expect("should wake on deadline")
            .expect("one item")
            .expect_err("timed out");
        assert_eq!(first.category(), crate::ErrorCategory::Timeout);
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test]
    async fn cancellation_and_deadline_can_coexist() {
        let token = tokio_util::sync::CancellationToken::new();
        let context = crate::RequestContext::with_cancellation_and_deadline(
            token.clone(),
            Instant::now() + Duration::from_secs(5),
        );
        token.cancel();
        assert_eq!(
            context.ensure_active().expect_err("cancelled").category(),
            crate::ErrorCategory::Cancelled
        );
    }

    #[tokio::test]
    async fn stream_forwards_valid_envelopes() {
        let source = Uuid::from_u128(1);
        let schema = logical_schema(1, "value");
        let expected = envelope(schema, source, 0, vec![1, 2, 3]);
        let inner: BatchStream = Box::pin(stream::iter(vec![Ok(expected)]));
        let mut wrapped = attach_request_context(inner, crate::RequestContext::default(), source);
        let item = wrapped.next().await.expect("batch").expect("ok");
        assert_eq!(item.row_count(), 3);
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test]
    async fn rejects_sequence_lineage_and_schema_violations_once() {
        let source = Uuid::from_u128(1);
        let schema = logical_schema(1, "value");
        let other_schema = logical_schema(2, "other");
        let cases: Vec<(Vec<BatchItem>, ErrorCategory)> = vec![
            (
                vec![Ok(envelope(Arc::clone(&schema), source, 1, vec![1]))],
                ErrorCategory::InvalidData,
            ),
            (
                vec![Ok(envelope(
                    Arc::clone(&schema),
                    Uuid::from_u128(2),
                    0,
                    vec![1],
                ))],
                ErrorCategory::InvalidData,
            ),
            (
                vec![
                    Ok(envelope(Arc::clone(&schema), source, 0, vec![1])),
                    Ok(envelope(Arc::clone(&schema), source, 0, vec![2])),
                ],
                ErrorCategory::InvalidData,
            ),
            (
                vec![
                    Ok(envelope(Arc::clone(&schema), source, 0, vec![1])),
                    Ok(envelope(Arc::clone(&schema), source, 2, vec![2])),
                ],
                ErrorCategory::InvalidData,
            ),
            (
                vec![
                    Ok(envelope(Arc::clone(&schema), source, 0, vec![1])),
                    Ok(envelope(other_schema, source, 1, vec![2])),
                ],
                ErrorCategory::SchemaDrift,
            ),
        ];

        for (items, category) in cases {
            let inner: BatchStream = Box::pin(stream::iter(items));
            let mut wrapped =
                attach_request_context(inner, crate::RequestContext::default(), source);
            let mut terminal = None;
            while let Some(item) = wrapped.next().await {
                if let Err(error) = item {
                    terminal = Some(error);
                    break;
                }
            }
            assert_eq!(terminal.expect("terminal error").category(), category);
            assert!(wrapped.next().await.is_none());
        }
    }

    async fn collect_partitioned_values(partitions: Vec<Vec<i64>>) -> Vec<i64> {
        let source = Uuid::from_u128(1);
        let schema = logical_schema(1, "value");
        let items = partitions
            .into_iter()
            .enumerate()
            .map(|(sequence, values)| {
                Ok(envelope(
                    Arc::clone(&schema),
                    source,
                    u64::try_from(sequence).expect("test sequence"),
                    values,
                ))
            })
            .collect::<Vec<BatchItem>>();
        let inner: BatchStream = Box::pin(stream::iter(items));
        let mut wrapped = attach_request_context(inner, crate::RequestContext::default(), source);
        let mut values = Vec::new();
        while let Some(item) = wrapped.next().await {
            let envelope = item.expect("valid envelope");
            let column = envelope
                .payload()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            values.extend(column.values().iter().copied());
        }
        values
    }

    #[tokio::test]
    async fn batch_partitioning_does_not_change_logical_rows() {
        let first = collect_partitioned_values(vec![vec![1], vec![2, 3], vec![4]]).await;
        let second = collect_partitioned_values(vec![vec![1, 2, 3, 4]]).await;
        assert_eq!(first, second);
    }

    struct CountingStream {
        items: VecDeque<BatchItem>,
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for CountingStream {
        type Item = BatchItem;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.items.pop_front())
        }
    }

    impl Drop for CountingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn wrapper_preserves_backpressure_and_early_drop() {
        let source = Uuid::from_u128(1);
        let schema = logical_schema(1, "value");
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let inner: BatchStream = Box::pin(CountingStream {
            items: VecDeque::from([Ok(envelope(schema, source, 0, vec![1]))]),
            polls: Arc::clone(&polls),
            dropped: Arc::clone(&dropped),
        });
        let mut wrapped = attach_request_context(inner, crate::RequestContext::default(), source);

        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "construction must not poll"
        );
        assert!(wrapped.next().await.expect("item").is_ok());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(wrapped);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
