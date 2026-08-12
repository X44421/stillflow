//! Bounded local and S3-compatible object storage connector.

#![deny(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::indexing_slicing))]

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use stillflow_connectors::{ConnectorCapabilities, RawBatchStream, SourceConnector};
use stillflow_core::{
    AssetKind, AssetLocator, AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus,
    ConnectorError, ConnectorKind, ConnectorResult, DiscoverRequest, InspectRequest, PreviewData,
    PreviewRequest, ReadRequest, SourceAsset, SourceConnection, TestConnectionRequest,
};
use uuid::Uuid;

mod access;
mod config;
mod credentials;
mod parquet;
mod preview;
mod schema;
mod staged;

pub use access::{ObjectByteStream, ObjectInfo, ObjectStorageAccess};
pub use credentials::{ObjectStoreCredentialResolver, S3CredentialMaterial};

use access::StoreAccess;
use credentials::RejectingCredentialResolver;

const ASSET_NAMESPACE: Uuid = Uuid::from_u128(0x4d691cd7_8ced_55cb_b9ae_49fef71b79ca);

/// Object storage connector with an injected server-side credential resolver.
pub struct ObjectStoreConnector {
    resolver: Arc<dyn ObjectStoreCredentialResolver>,
}

impl std::fmt::Debug for ObjectStoreConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreConnector")
            .field("resolver", &"[INJECTED]")
            .finish()
    }
}

impl Default for ObjectStoreConnector {
    fn default() -> Self {
        Self::new(Arc::new(RejectingCredentialResolver))
    }
}

impl ObjectStoreConnector {
    pub fn new(resolver: Arc<dyn ObjectStoreCredentialResolver>) -> Self {
        Self { resolver }
    }

    /// Opens the provider-neutral byte layer for server-side composition.
    pub async fn open_access(
        &self,
        connection: &SourceConnection,
        context: &stillflow_core::RequestContext,
    ) -> ConnectorResult<Arc<dyn ObjectStorageAccess>> {
        Ok(Arc::new(
            StoreAccess::open(connection, self.resolver.as_ref(), context).await?,
        ))
    }

    async fn access(
        &self,
        connection: &SourceConnection,
        context: &stillflow_core::RequestContext,
    ) -> ConnectorResult<StoreAccess> {
        StoreAccess::open(connection, self.resolver.as_ref(), context).await
    }
}

#[async_trait]
impl SourceConnector for ObjectStoreConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::ObjectStore
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            schema_discovery: true,
            preview: true,
            streaming: true,
            incremental_read: false,
            predicate_pushdown: false,
            column_projection: true,
            range_read: true,
            change_tracking: false,
        }
    }

    async fn test_connection(
        &self,
        connection: &SourceConnection,
        request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus> {
        request.validate()?;
        let access = self.access(connection, &request.context).await?;
        access.probe(&request.context).await?;
        Ok(ConnectionStatus::Ok)
    }

    async fn discover(
        &self,
        connection: &SourceConnection,
        request: DiscoverRequest,
    ) -> ConnectorResult<Vec<SourceAsset>> {
        request.validate()?;
        let access = self.access(connection, &request.context).await?;
        let prefix = request.parent_path.as_deref().unwrap_or("");
        let scope = access.identity_scope();
        let mut assets = Vec::new();
        for object in access.list(prefix, &request.context).await? {
            request.context.ensure_active()?;
            if !is_supported_tabular_key(&object.key) || object.size > access.max_object_bytes() {
                continue;
            }
            let name = object
                .key
                .rsplit_once('/')
                .map_or(object.key.as_str(), |(_, name)| name)
                .to_owned();
            let identity = format!("{}|{}|{}", connection.id(), scope, object.key);
            assets.push(SourceAsset {
                id: Uuid::new_v5(&ASSET_NAMESPACE, identity.as_bytes()),
                connection_id: connection.id(),
                kind: AssetKind::File,
                name,
                locator: AssetLocator {
                    path: object.key,
                    container: Some(access.container().to_owned()),
                    schema: None,
                    sheet: None,
                    workbook_region: None,
                },
                discovered_at: Utc::now(),
            });
        }
        assets.sort_by(|left, right| left.locator.path.cmp(&right.locator.path));
        Ok(assets)
    }

    async fn inspect(
        &self,
        connection: &SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<AssetMetadata> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        let access = self.access(connection, &request.context).await?;
        ensure_asset_container(&access, &request.asset)?;
        if is_parquet_key(&request.asset.locator.path) {
            parquet::inspect_parquet(&access, &request.asset, &request.context).await
        } else {
            staged::inspect_text(&access, &request.asset, &request.context).await
        }
    }

    async fn preview(
        &self,
        connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        reject_filter(request.filter.is_some())?;
        if request.sampling != stillflow_core::SamplingStrategy::Head {
            return Err(ConnectorError::for_unsupported_capability(
                "preview_sampling",
            ));
        }
        let access = self.access(connection, &request.context).await?;
        ensure_asset_container(&access, &request.asset)?;
        if is_parquet_key(&request.asset.locator.path) {
            preview::preview_parquet(&access, request).await
        } else {
            staged::preview_text(&access, request).await
        }
    }

    async fn read_batches(
        &self,
        connection: &SourceConnection,
        request: ReadRequest,
    ) -> ConnectorResult<RawBatchStream> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        reject_filter(request.filter.is_some())?;
        if request.checkpoint.is_some() {
            return Err(ConnectorError::for_unsupported_capability(
                "incremental_read",
            ));
        }
        let access = self.access(connection, &request.context).await?;
        ensure_asset_container(&access, &request.asset)?;
        if is_parquet_key(&request.asset.locator.path) {
            Ok(parquet::prepare_parquet(
                &access,
                &request.asset,
                request.schema_override.as_ref(),
                request.projection.as_deref(),
                request.batch_size,
                None,
                &request.context,
            )
            .await?
            .into_raw_stream())
        } else {
            staged::read_text(&access, request).await
        }
    }

    async fn checkpoint(
        &self,
        connection: &SourceConnection,
        request: CheckpointRequest,
    ) -> ConnectorResult<Option<Checkpoint>> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        Ok(None)
    }
}

fn ensure_asset_connection(
    connection: &SourceConnection,
    asset: &SourceAsset,
) -> ConnectorResult<()> {
    if asset.connection_id != connection.id() {
        return Err(ConnectorError::invalid_configuration(
            "asset does not belong to the provided connection",
        ));
    }
    if asset.locator.container.is_none() || !is_supported_tabular_key(&asset.locator.path) {
        return Err(ConnectorError::invalid_configuration(
            "asset is not a supported object storage tabular file",
        ));
    }
    Ok(())
}

fn ensure_asset_container(access: &StoreAccess, asset: &SourceAsset) -> ConnectorResult<()> {
    if asset.locator.container.as_deref() != Some(access.container()) {
        return Err(ConnectorError::invalid_configuration(
            "asset container does not match the object storage connection",
        ));
    }
    Ok(())
}

fn reject_filter(present: bool) -> ConnectorResult<()> {
    if present {
        Err(ConnectorError::for_unsupported_capability(
            "predicate_pushdown",
        ))
    } else {
        Ok(())
    }
}

fn is_parquet_key(key: &str) -> bool {
    key.rsplit_once('.')
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("parquet"))
}

fn is_supported_tabular_key(key: &str) -> bool {
    let extension = key
        .rsplit_once('.')
        .map(|(_, extension)| extension)
        .unwrap_or_default();
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "csv" | "tsv" | "json" | "jsonl" | "ndjson" | "parquet"
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use stillflow_core::{CredentialRef, RequestContext};

    use super::*;

    #[tokio::test]
    async fn discovery_is_sorted_and_has_stable_ids() {
        let directory = tempdir().expect("tempdir");
        std::fs::write(directory.path().join("b.csv"), b"b\n1\n").expect("fixture");
        std::fs::write(directory.path().join("a.ndjson"), b"{\"a\":1}\n")
            .expect("fixture");
        std::fs::write(directory.path().join("ignored.txt"), b"ignored").expect("fixture");
        let connection = SourceConnection::try_new(
            ConnectorKind::ObjectStore,
            "objects",
            serde_json::json!({"provider":"local", "root":directory.path()}),
            CredentialRef::new("cred://tests/local").expect("credential ref"),
        )
        .expect("connection");
        let connector = ObjectStoreConnector::default();
        let first = connector
            .discover(
                &connection,
                DiscoverRequest {
                    context: RequestContext::new(),
                    parent_path: None,
                },
            )
            .await
            .expect("discover");
        let second = connector
            .discover(
                &connection,
                DiscoverRequest {
                    context: RequestContext::new(),
                    parent_path: None,
                },
            )
            .await
            .expect("discover again");
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].name, "a.ndjson");
        assert_eq!(first[0].id, second[0].id);
        assert!(RequestContext::new().ensure_active().is_ok());
    }
}
