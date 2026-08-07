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

    pub(crate) fn into_inner(self) -> BatchStream {
        self.0
    }
}
