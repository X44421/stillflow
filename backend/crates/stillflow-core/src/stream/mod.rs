use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use futures::Stream;
use tokio::time::{Instant, Sleep};

use crate::error::ConnectorError;

/// Result item carried by a connector batch stream.
pub type BatchItem = Result<RecordBatch, ConnectorError>;

/// Bounded asynchronous stream of Arrow record batches.
pub type BatchStream = Pin<Box<dyn Stream<Item = BatchItem> + Send>>;

/// Wraps an inner stream and enforces request cancellation and deadlines.
struct CancellableBatchStream {
    inner: BatchStream,
    context: crate::RequestContext,
    terminated: bool,
    deadline_sleep: Option<Pin<Box<Sleep>>>,
    cancellation_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl CancellableBatchStream {
    fn new(inner: BatchStream, context: crate::RequestContext) -> Self {
        Self {
            inner,
            context,
            terminated: false,
            deadline_sleep: None,
            cancellation_wait: None,
        }
    }

    fn terminal_error(&mut self, error: ConnectorError) -> Poll<Option<BatchItem>> {
        self.terminated = true;
        Poll::Ready(Some(Err(error)))
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
        if let Poll::Ready(()) = self
            .cancellation_wait
            .as_mut()
            .expect("cancellation wait")
            .as_mut()
            .poll(cx)
        {
            return self.terminal_error(ConnectorError::cancelled());
        }

        if let Some(deadline) = self.context.deadline() {
            if Instant::now() >= deadline {
                return self.terminal_error(ConnectorError::timeout("request deadline exceeded"));
            }
            if self.deadline_sleep.is_none() {
                self.deadline_sleep = Some(Box::pin(tokio::time::sleep_until(deadline)));
            }
            if let Poll::Ready(()) = self
                .deadline_sleep
                .as_mut()
                .expect("sleep")
                .as_mut()
                .poll(cx)
            {
                return self.terminal_error(ConnectorError::timeout("request deadline exceeded"));
            }
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Err(error))) => self.terminal_error(error),
            Poll::Ready(None) => {
                self.terminated = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Builds a batch stream that honours cancellation and deadlines.
pub fn attach_request_context(inner: BatchStream, context: crate::RequestContext) -> BatchStream {
    Box::pin(CancellableBatchStream::new(inner, context))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{stream, StreamExt};
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn cancelled_pending_stream_wakes_and_terminates_once() {
        let token = tokio_util::sync::CancellationToken::new();
        let context = crate::RequestContext::with_cancellation(token.clone());
        let inner: BatchStream = Box::pin(stream::pending());
        let mut wrapped = attach_request_context(inner, context);

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

        let second = wrapped.next().await;
        assert!(second.is_none(), "terminal error must not repeat");

        cancel_task.await.expect("cancel task");
    }

    #[tokio::test]
    async fn expired_deadline_on_pending_stream_terminates_once() {
        let context =
            crate::RequestContext::with_deadline(Instant::now() + Duration::from_millis(20));
        let inner: BatchStream = Box::pin(stream::pending());
        let mut wrapped = attach_request_context(inner, context);

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
    async fn stream_forwards_batches_when_active() {
        let schema = std::sync::Arc::new(arrow_schema::Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let inner: BatchStream = Box::pin(stream::iter(vec![Ok(batch.clone())]));
        let context = crate::RequestContext::default();
        let mut wrapped = attach_request_context(inner, context);
        let item = wrapped.next().await.expect("batch").expect("ok");
        assert_eq!(item.num_rows(), batch.num_rows());
    }
}
