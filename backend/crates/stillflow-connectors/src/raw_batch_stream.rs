use futures::StreamExt;
use stillflow_core::BatchStream;

/// Envelope stream returned by connector implementations before boundary validation.
///
/// Callers must obtain a [`BatchStream`] through [`crate::ConnectorRegistry::read_batches`].
pub struct RawBatchStream(BatchStream);

impl RawBatchStream {
    /// Constructs an envelope stream for connector adapter implementations.
    pub fn new(inner: BatchStream) -> Self {
        Self(inner)
    }

    /// Retains a resource until this stream ends or is dropped.
    ///
    /// Adapter crates use this to bind temporary staging capabilities to the
    /// exact lifetime of a prepared reader without exposing the inner stream.
    pub fn with_drop_guard<G>(self, guard: G) -> Self
    where
        G: Send + 'static,
    {
        let stream = futures::stream::unfold((self.0, guard), |(mut inner, guard)| async move {
            inner.next().await.map(|item| (item, (inner, guard)))
        });
        Self(Box::pin(stream))
    }

    pub(crate) fn into_inner(self) -> BatchStream {
        self.0
    }
}
