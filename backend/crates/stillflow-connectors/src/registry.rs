use std::collections::HashMap;

use stillflow_core::{
    attach_request_context, BatchStream, Checkpoint, ConnectionStatus, ConnectorError,
    ConnectorKind, ConnectorResult, DiscoverRequest, PreviewData, PreviewRequest, ReadRequest,
    SourceAsset, SourceConnection,
};

use crate::connector::SourceConnectorRef;

/// Registry mapping connector kinds to shared adapter implementations.
///
/// Multiple [`SourceConnection`] values of the same kind share one adapter
/// implementation. Connection-specific configuration is passed on every call.
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

    pub async fn test_connection(
        &self,
        connection: &SourceConnection,
    ) -> ConnectorResult<ConnectionStatus> {
        connection.validate()?;
        let connector = self.require(connection.kind())?;
        connector.test_connection(connection).await
    }

    pub async fn discover(
        &self,
        connection: &SourceConnection,
        request: DiscoverRequest,
    ) -> ConnectorResult<Vec<SourceAsset>> {
        connection.validate()?;
        request.validate()?;
        let connector = self.require(connection.kind())?;
        connector.discover(connection, request).await
    }

    pub async fn inspect(
        &self,
        connection: &SourceConnection,
        asset: &SourceAsset,
    ) -> ConnectorResult<stillflow_core::AssetMetadata> {
        connection.validate()?;
        let connector = self.require(connection.kind())?;
        connector.inspect(connection, asset).await
    }

    pub async fn preview(
        &self,
        connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        connection.validate()?;
        request.validate()?;
        let connector = self.require(connection.kind())?;
        connector.preview(connection, request).await
    }

    pub async fn read_batches(
        &self,
        connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<BatchStream> {
        connection.validate()?;
        request.validate()?;
        let context = request.context.clone();
        let connector = self.require(connection.kind())?;
        let raw = connector.read_batches(connection, request).await?;
        Ok(attach_request_context(raw.into_inner(), context))
    }

    pub async fn checkpoint(
        &self,
        connection: &SourceConnection,
        asset: &SourceAsset,
    ) -> ConnectorResult<Option<Checkpoint>> {
        connection.validate()?;
        let connector = self.require(connection.kind())?;
        connector.checkpoint(connection, asset).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::{stream, StreamExt};
    use stillflow_core::{
        AssetLocator, AssetMetadata, ConnectionStatus, CredentialRef, DiscoverRequest,
        PreviewRequest, ReadRequest, SourceAsset,
    };

    use super::*;
    use crate::capabilities::ConnectorCapabilities;
    use crate::connector::SourceConnector;
    use crate::raw_batch_stream::RawBatchStream;

    struct StubConnector;

    #[async_trait]
    impl SourceConnector for StubConnector {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::LocalFile
        }

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities {
                preview: true,
                streaming: true,
                ..ConnectorCapabilities::default()
            }
        }

        async fn test_connection(
            &self,
            _connection: &SourceConnection,
        ) -> ConnectorResult<ConnectionStatus> {
            Ok(ConnectionStatus::Ok)
        }

        async fn discover(
            &self,
            _connection: &SourceConnection,
            request: DiscoverRequest,
        ) -> ConnectorResult<Vec<SourceAsset>> {
            request.context.ensure_active()?;
            Ok(Vec::new())
        }

        async fn inspect(
            &self,
            _connection: &SourceConnection,
            _asset: &SourceAsset,
        ) -> ConnectorResult<AssetMetadata> {
            Ok(AssetMetadata::new(
                Arc::new(arrow_schema::Schema::empty()),
                "stub",
            ))
        }

        async fn preview(
            &self,
            _connection: &SourceConnection,
            request: PreviewRequest,
        ) -> ConnectorResult<stillflow_core::PreviewData> {
            request.context.ensure_active()?;
            request.validate()?;
            Ok(stillflow_core::PreviewData::empty(Arc::new(
                arrow_schema::Schema::empty(),
            )))
        }

        async fn read_batches(
            &self,
            _connection: &SourceConnection,
            request: ReadRequest,
        ) -> ConnectorResult<RawBatchStream> {
            request.context.ensure_active()?;
            request.validate()?;
            Ok(RawBatchStream::new(Box::pin(stream::empty())))
        }

        async fn checkpoint(
            &self,
            _connection: &SourceConnection,
            _asset: &SourceAsset,
        ) -> ConnectorResult<Option<stillflow_core::Checkpoint>> {
            Ok(None)
        }
    }

    fn sample_connection(name: &str) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::LocalFile,
            name,
            serde_json::json!({ "root": format!("/data/{name}") }),
            CredentialRef::new(format!("cred://local/{name}")),
        )
        .expect("connection")
    }

    fn sample_asset(connection_id: uuid::Uuid, name: &str) -> SourceAsset {
        SourceAsset::new(
            connection_id,
            stillflow_core::AssetKind::File,
            name,
            AssetLocator {
                path: format!("/{name}"),
                container: None,
                schema: None,
                sheet: None,
            },
        )
    }

    #[tokio::test]
    async fn registry_supports_dynamic_dispatch() {
        let mut registry = ConnectorRegistry::new();
        let connector = Arc::new(StubConnector) as SourceConnectorRef;
        registry.register(connector).expect("register");
        let connection = sample_connection("uploads");
        let resolved = registry.require(connection.kind()).expect("resolve");
        assert_eq!(resolved.kind(), ConnectorKind::LocalFile);
        assert!(matches!(
            registry.test_connection(&connection).await,
            Ok(ConnectionStatus::Ok)
        ));
    }

    #[tokio::test]
    async fn same_kind_adapter_serves_multiple_connections() {
        let mut registry = ConnectorRegistry::new();
        registry
            .register(Arc::new(StubConnector) as SourceConnectorRef)
            .expect("register");
        let first = sample_connection("warehouse-a");
        let second = sample_connection("warehouse-b");
        assert_ne!(first.id(), second.id());
        assert!(registry.test_connection(&first).await.is_ok());
        assert!(registry.test_connection(&second).await.is_ok());
    }

    #[tokio::test]
    async fn registry_wraps_batch_streams() {
        let mut registry = ConnectorRegistry::new();
        registry
            .register(Arc::new(StubConnector) as SourceConnectorRef)
            .expect("register");
        let connection = sample_connection("uploads");
        let asset = sample_asset(connection.id(), "orders.csv");
        let token = tokio_util::sync::CancellationToken::new();
        let context = stillflow_core::RequestContext::with_cancellation(token.clone());
        let request = ReadRequest {
            context,
            asset,
            projection: None,
            filter: None,
            checkpoint: None,
            batch_size: 1024,
        };
        let mut stream = registry
            .read_batches(&connection, request)
            .await
            .expect("stream");
        token.cancel();
        let error = stream
            .next()
            .await
            .expect("terminal item")
            .expect_err("cancelled");
        assert_eq!(error.category(), stillflow_core::ErrorCategory::Cancelled);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn preview_honours_cancellation_through_registry() {
        let mut registry = ConnectorRegistry::new();
        registry
            .register(Arc::new(StubConnector) as SourceConnectorRef)
            .expect("register");
        let connection = sample_connection("uploads");
        let token = tokio_util::sync::CancellationToken::new();
        let context = stillflow_core::RequestContext::with_cancellation(token.clone());
        token.cancel();
        let asset = sample_asset(connection.id(), "orders.csv");
        let request = PreviewRequest {
            context,
            asset,
            projection: None,
            filter: None,
            row_limit: 100,
            byte_limit: 1024,
            sampling: stillflow_core::SamplingStrategy::Head,
        };
        let error = registry
            .preview(&connection, request)
            .await
            .expect_err("cancelled preview");
        assert_eq!(error.category(), stillflow_core::ErrorCategory::Cancelled);
    }
}