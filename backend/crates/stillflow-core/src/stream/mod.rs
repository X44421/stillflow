use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use futures::Stream;

use crate::error::ConnectorError;

/// Result item carried by a connector batch stream.
pub type BatchItem = Result<RecordBatch, ConnectorError>;

/// Bounded asynchronous stream of Arrow record batches.
pub type BatchStream = Pin<Box<dyn Stream<Item = BatchItem> + Send>>;

/// Wraps an inner stream and enforces request cancellation and deadlines.
pub struct CancellableBatchStream {
    inner: BatchStream,
    context: crate::RequestContext,
}

impl CancellableBatchStream {
    pub fn new(inner: BatchStream, context: crate::RequestContext) -> Self {
        Self { inner, context }
    }
}

impl Stream for CancellableBatchStream {
    type Item = BatchItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Err(error) = self.context.ensure_active() {
            return Poll::Ready(Some(Err(error)));
        }
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Builds a batch stream that honours cancellation and deadlines.
pub fn attach_request_context(inner: BatchStream, context: crate::RequestContext) -> BatchStream {
    Box::pin(CancellableBatchStream::new(inner, context))
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};

    use super::*;

    #[tokio::test]
    async fn cancelled_stream_returns_cancelled_error() {
        let token = tokio_util::sync::CancellationToken::new();
        let context = crate::RequestContext::with_cancellation(token.clone());
        let inner: BatchStream = Box::pin(stream::pending());
        token.cancel();
        let mut wrapped = attach_request_context(inner, context);
        let item = wrapped.next().await.expect("one item");
        assert_eq!(
            item.expect_err("cancelled").category(),
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
