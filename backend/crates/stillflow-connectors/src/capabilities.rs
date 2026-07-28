use stillflow_core::{ConnectorError, ConnectorResult};

/// Individual connector optimizations that can be negotiated at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    SchemaDiscovery,
    Preview,
    Streaming,
    IncrementalRead,
    PredicatePushdown,
    ColumnProjection,
    RangeRead,
    ChangeTracking,
}

/// Declared connector optimizations.
///
/// Connectors advertise supported capabilities up front. Callers must use
/// [`ConnectorCapabilities::ensure`] before requesting an optimization; when
/// unsupported, connectors return [`ConnectorError`] with category
/// `UnsupportedCapability` rather than silently degrading behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnectorCapabilities {
    pub schema_discovery: bool,
    pub preview: bool,
    pub streaming: bool,
    pub incremental_read: bool,
    pub predicate_pushdown: bool,
    pub column_projection: bool,
    pub range_read: bool,
    pub change_tracking: bool,
}

impl ConnectorCapabilities {
    pub fn supports(&self, capability: Capability) -> bool {
        match capability {
            Capability::SchemaDiscovery => self.schema_discovery,
            Capability::Preview => self.preview,
            Capability::Streaming => self.streaming,
            Capability::IncrementalRead => self.incremental_read,
            Capability::PredicatePushdown => self.predicate_pushdown,
            Capability::ColumnProjection => self.column_projection,
            Capability::RangeRead => self.range_read,
            Capability::ChangeTracking => self.change_tracking,
        }
    }

    pub fn ensure(&self, capability: Capability) -> ConnectorResult<()> {
        if self.supports(capability) {
            Ok(())
        } else {
            Err(ConnectorError::for_unsupported_capability(capability_name(
                capability,
            )))
        }
    }
}

fn capability_name(capability: Capability) -> &'static str {
    match capability {
        Capability::SchemaDiscovery => "schema_discovery",
        Capability::Preview => "preview",
        Capability::Streaming => "streaming",
        Capability::IncrementalRead => "incremental_read",
        Capability::PredicatePushdown => "predicate_pushdown",
        Capability::ColumnProjection => "column_projection",
        Capability::RangeRead => "range_read",
        Capability::ChangeTracking => "change_tracking",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stillflow_core::ErrorCategory;

    #[test]
    fn unsupported_capability_returns_typed_error() {
        let capabilities = ConnectorCapabilities::default();
        let error = capabilities
            .ensure(Capability::PredicatePushdown)
            .expect_err("predicate pushdown is unsupported");
        assert_eq!(error.category(), ErrorCategory::UnsupportedCapability);
        assert_eq!(error.missing_capability(), Some("predicate_pushdown"));
    }
}
