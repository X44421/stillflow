use std::collections::HashMap;

use stillflow_core::{
    attach_request_context, BatchStream, Checkpoint, CheckpointRequest, ConnectionStatus,
    ConnectorError, ConnectorKind, ConnectorResult, DiscoverRequest, InspectRequest, PreviewData,
    PreviewRequest, ReadRequest, SourceAsset, SourceConnection, TestConnectionRequest,
};

use crate::capabilities::Capability;
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

    pub(crate) fn get(&self, kind: ConnectorKind) -> Option<SourceConnectorRef> {
        self.connectors.get(&kind).cloned()
    }

    pub(crate) fn require(&self, kind: ConnectorKind) -> ConnectorResult<SourceConnectorRef> {
        self.get(kind).ok_or_else(|| {
            ConnectorError::invalid_configuration(format!("unknown connector kind `{kind:?}`"))
        })
    }

    pub async fn test_connection(
        &self,
        connection: &SourceConnection,
        request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus> {
        connection.validate()?;
        request.validate()?;
        let connector = self.require(connection.kind())?;
        connector.test_connection(connection, request).await
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
        request: InspectRequest,
    ) -> ConnectorResult<stillflow_core::AssetMetadata> {
        connection.validate()?;
        request.validate()?;
        validate_asset_belongs_to_connection(connection, &request.asset)?;
        let connector = self.require(connection.kind())?;
        connector
            .capabilities()
            .ensure(Capability::SchemaDiscovery)?;
        connector.inspect(connection, request).await
    }

    pub async fn preview(
        &self,
        connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        connection.validate()?;
        request.validate()?;
        validate_asset_belongs_to_connection(connection, &request.asset)?;
        let connector = self.require(connection.kind())?;
        connector.capabilities().ensure(Capability::Preview)?;
        if request.filter.is_some() {
            connector
                .capabilities()
                .ensure(Capability::PredicatePushdown)?;
        }
        if request.projection.is_some() {
            connector
                .capabilities()
                .ensure(Capability::ColumnProjection)?;
        }
        connector.preview(connection, request).await
    }

    pub async fn read_batches(
        &self,
        connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<BatchStream> {
        connection.validate()?;
        request.validate()?;
        validate_asset_belongs_to_connection(connection, &request.asset)?;
        let context = request.context.clone();
        let connector = self.require(connection.kind())?;
        connector.capabilities().ensure(Capability::Streaming)?;
        if request.filter.is_some() {
            connector
                .capabilities()
                .ensure(Capability::PredicatePushdown)?;
        }
        if request.projection.is_some() {
            connector
                .capabilities()
                .ensure(Capability::ColumnProjection)?;
        }
        if request.checkpoint.is_some() {
            connector
                .capabilities()
                .ensure(Capability::IncrementalRead)?;
        }
        let raw = connector.read_batches(connection, request).await?;
        Ok(attach_request_context(raw.into_inner(), context))
    }

    pub async fn checkpoint(
        &self,
        connection: &SourceConnection,
        request: CheckpointRequest,
    ) -> ConnectorResult<Option<Checkpoint>> {
        connection.validate()?;
        request.validate()?;
        validate_asset_belongs_to_connection(connection, &request.asset)?;
        let connector = self.require(connection.kind())?;
        connector
            .capabilities()
            .ensure(Capability::IncrementalRead)?;
        connector.checkpoint(connection, request).await
    }
}

fn validate_asset_belongs_to_connection(
    connection: &SourceConnection,
    asset: &SourceAsset,
) -> ConnectorResult<()> {
    if asset.connection_id != connection.id() {
        return Err(ConnectorError::invalid_configuration(
            "asset does not belong to the provided connection",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::{stream, StreamExt};
    use stillflow_core::{
        AssetLocator, AssetMetadata, ConnectionStatus, CredentialRef, DiscoverRequest,
        InspectRequest, PreviewRequest, ReadRequest, SourceAsset, TestConnectionRequest,
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
                schema_discovery: true,
                incremental_read: true,
                predicate_pushdown: true,
                column_projection: true,
                ..ConnectorCapabilities::default()
            }
        }

        async fn test_connection(
            &self,
            _connection: &SourceConnection,
            request: TestConnectionRequest,
        ) -> ConnectorResult<ConnectionStatus> {
            request.context.ensure_active()?;
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
            request: InspectRequest,
        ) -> ConnectorResult<AssetMetadata> {
            request.context.ensure_active()?;
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
            request: CheckpointRequest,
        ) -> ConnectorResult<Option<stillflow_core::Checkpoint>> {
            request.context.ensure_active()?;
            Ok(None)
        }
    }

    fn sample_connection(name: &str) -> SourceConnection {
        SourceConnection::try_new(
            ConnectorKind::LocalFile,
            name,
            serde_json::json!({ "root": format!("/data/{name}") }),
            CredentialRef::new(format!("cred://local/{name}")).expect("credential ref"),
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
            registry
                .test_connection(
                    &connection,
                    TestConnectionRequest {
                        context: stillflow_core::RequestContext::default(),
                    }
                )
                .await,
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
        let request = TestConnectionRequest {
            context: stillflow_core::RequestContext::default(),
        };
        assert!(registry
            .test_connection(&first, request.clone())
            .await
            .is_ok());
        assert!(registry.test_connection(&second, request).await.is_ok());
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

    #[tokio::test]
    async fn rejects_mismatched_asset_connection() {
        let mut registry = ConnectorRegistry::new();
        registry
            .register(Arc::new(StubConnector) as SourceConnectorRef)
            .expect("register");
        let connection = sample_connection("uploads");
        let other_connection = sample_connection("warehouse");
        let asset = sample_asset(other_connection.id(), "orders.csv");
        let request = PreviewRequest {
            context: stillflow_core::RequestContext::default(),
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
            .expect_err("mismatched asset");
        assert_eq!(
            error.category(),
            stillflow_core::ErrorCategory::InvalidConfiguration
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_preview_capability() {
        struct LimitedConnector;

        #[async_trait]
        impl SourceConnector for LimitedConnector {
            fn kind(&self) -> ConnectorKind {
                ConnectorKind::LocalFile
            }

            fn capabilities(&self) -> ConnectorCapabilities {
                ConnectorCapabilities::default()
            }

            async fn test_connection(
                &self,
                _connection: &SourceConnection,
                _request: TestConnectionRequest,
            ) -> ConnectorResult<ConnectionStatus> {
                Ok(ConnectionStatus::Ok)
            }

            async fn discover(
                &self,
                _connection: &SourceConnection,
                _request: DiscoverRequest,
            ) -> ConnectorResult<Vec<SourceAsset>> {
                Ok(Vec::new())
            }

            async fn inspect(
                &self,
                _connection: &SourceConnection,
                _request: InspectRequest,
            ) -> ConnectorResult<AssetMetadata> {
                Ok(AssetMetadata::new(
                    Arc::new(arrow_schema::Schema::empty()),
                    "stub",
                ))
            }

            async fn preview(
                &self,
                _connection: &SourceConnection,
                _request: PreviewRequest,
            ) -> ConnectorResult<stillflow_core::PreviewData> {
                Ok(stillflow_core::PreviewData::empty(Arc::new(
                    arrow_schema::Schema::empty(),
                )))
            }

            async fn read_batches(
                &self,
                _connection: &SourceConnection,
                _request: ReadRequest,
            ) -> ConnectorResult<RawBatchStream> {
                Ok(RawBatchStream::new(Box::pin(stream::empty())))
            }

            async fn checkpoint(
                &self,
                _connection: &SourceConnection,
                _request: CheckpointRequest,
            ) -> ConnectorResult<Option<stillflow_core::Checkpoint>> {
                Ok(None)
            }
        }

        let mut registry = ConnectorRegistry::new();
        registry
            .register(Arc::new(LimitedConnector) as SourceConnectorRef)
            .expect("register");
        let connection = sample_connection("uploads");
        let asset = sample_asset(connection.id(), "orders.csv");
        let request = PreviewRequest {
            context: stillflow_core::RequestContext::default(),
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
            .expect_err("unsupported preview");
        assert_eq!(
            error.category(),
            stillflow_core::ErrorCategory::UnsupportedCapability
        );
    }
}
