use std::collections::HashMap;

use stillflow_core::{ConnectorError, ConnectorKind, ConnectorResult};

use crate::connector::SourceConnectorRef;

/// Registry mapping connector kinds to object-safe implementations.
#[derive(Default)]
pub struct ConnectorRegistry {
    connectors: HashMap<ConnectorKind, SourceConnectorRef>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, connector: SourceConnectorRef) -> ConnectorResult<()> {
        let kind = connector.kind();
        if self.connectors.contains_key(&kind) {
            return Err(ConnectorError::invalid_configuration(format!(
                "connector kind `{kind:?}` is already registered"
            )));
        }
        self.connectors.insert(kind, connector);
        Ok(())
    }

    pub fn get(&self, kind: ConnectorKind) -> Option<SourceConnectorRef> {
        self.connectors.get(&kind).cloned()
    }

    pub fn require(&self, kind: ConnectorKind) -> ConnectorResult<SourceConnectorRef> {
        self.get(kind).ok_or_else(|| {
            ConnectorError::invalid_configuration(format!("unknown connector kind `{kind:?}`"))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::stream;
    use stillflow_core::{
        attach_request_context, AssetLocator, AssetMetadata, BatchStream, Checkpoint,
        ConnectionStatus, DiscoverRequest, PreviewData, PreviewRequest, ReadRequest, SourceAsset,
    };

    use super::*;
    use crate::capabilities::ConnectorCapabilities;
    use crate::connector::SourceConnector;

    struct StubConnector {
        kind: ConnectorKind,
    }

    #[async_trait]
    impl SourceConnector for StubConnector {
        fn kind(&self) -> ConnectorKind {
            self.kind
        }

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities {
                preview: true,
                streaming: true,
                ..ConnectorCapabilities::default()
            }
        }

        async fn test_connection(&self) -> ConnectorResult<ConnectionStatus> {
            Ok(ConnectionStatus::Ok)
        }

        async fn discover(&self, _request: DiscoverRequest) -> ConnectorResult<Vec<SourceAsset>> {
            Ok(Vec::new())
        }

        async fn inspect(&self, _asset: &SourceAsset) -> ConnectorResult<AssetMetadata> {
            Ok(AssetMetadata::new(
                Arc::new(arrow_schema::Schema::empty()),
                "stub",
            ))
        }

        async fn preview(&self, request: PreviewRequest) -> ConnectorResult<PreviewData> {
            request.context.ensure_active()?;
            Ok(PreviewData::empty(Arc::new(arrow_schema::Schema::empty())))
        }

        async fn read_batches(&self, request: ReadRequest) -> ConnectorResult<BatchStream> {
            request.context.ensure_active()?;
            let stream: BatchStream = Box::pin(stream::empty());
            Ok(attach_request_context(stream, request.context))
        }

        async fn checkpoint(&self, _asset: &SourceAsset) -> ConnectorResult<Option<Checkpoint>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn registry_supports_dynamic_dispatch() {
        let mut registry = ConnectorRegistry::new();
        let connector = Arc::new(StubConnector {
            kind: ConnectorKind::LocalFile,
        }) as SourceConnectorRef;
        registry.register(connector.clone()).expect("register");
        let resolved = registry.require(ConnectorKind::LocalFile).expect("resolve");
        assert_eq!(resolved.kind(), ConnectorKind::LocalFile);
        assert!(matches!(
            resolved.test_connection().await,
            Ok(ConnectionStatus::Ok)
        ));
    }

    #[tokio::test]
    async fn registry_rejects_duplicate_kinds() {
        let mut registry = ConnectorRegistry::new();
        let connector = Arc::new(StubConnector {
            kind: ConnectorKind::LocalFile,
        }) as SourceConnectorRef;
        registry
            .register(connector.clone())
            .expect("first register");
        assert!(registry.register(connector).is_err());
    }

    #[tokio::test]
    async fn preview_honours_cancellation() {
        let connector = StubConnector {
            kind: ConnectorKind::LocalFile,
        };
        let token = tokio_util::sync::CancellationToken::new();
        let context = stillflow_core::RequestContext::with_cancellation(token.clone());
        token.cancel();
        let asset = SourceAsset::new(
            uuid::Uuid::new_v4(),
            stillflow_core::AssetKind::File,
            "orders.csv",
            AssetLocator {
                path: "/orders.csv".to_owned(),
                container: None,
                schema: None,
                sheet: None,
            },
        );
        let request = PreviewRequest {
            context,
            asset,
            projection: None,
            filter: None,
            row_limit: 100,
            byte_limit: 1024,
            sampling: stillflow_core::SamplingStrategy::Head,
        };
        let error = connector
            .preview(request)
            .await
            .expect_err("cancelled preview");
        assert_eq!(error.category(), stillflow_core::ErrorCategory::Cancelled);
    }
}
