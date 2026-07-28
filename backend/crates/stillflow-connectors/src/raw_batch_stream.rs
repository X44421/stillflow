use stillflow_core::BatchStream;

/// Batch stream returned by connector implementations before request context is attached.
///
/// Callers must obtain a [`BatchStream`] through [`crate::ConnectorRegistry::read_batches`].
pub struct RawBatchStream(BatchStream);

impl RawBatchStream {
    /// Constructs a raw batch stream for connector adapter implementations.
    pub fn new(inner: BatchStream) -> Self {
        Self(inner)
    }

    pub(crate) fn into_inner(self) -> BatchStream {
        self.0
    }
}
