//! Bounded local tabular connector for CSV, TSV, JSON, NDJSON and Parquet files.

#![deny(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::indexing_slicing))]

use async_trait::async_trait;
use stillflow_connectors::{ConnectorCapabilities, RawBatchStream, SourceConnector};
use stillflow_core::{
    AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus, ConnectorError, ConnectorKind,
    ConnectorResult, DiscoverRequest, InspectRequest, PreviewData, PreviewRequest, ReadRequest,
    SamplingStrategy, SourceAsset, SourceConnection, TestConnectionRequest,
};

mod bridge;
mod config;
mod format;
mod inference;
mod inspect;
mod json_stream;
mod path;
mod preview;
mod read;
mod schema;

// E24-JSON-A2 direct projected NDJSON writer (issue #158), productionized by
// O1-J1 (issue #296): both projected-row paths are always compiled and the
// runtime routing switch is the per-connection config key
// `jsonDirectProjectedWriter` (default `false` = the generic DOM path, which
// is the rollback point). The former #151 temporal enablement blocker was
// fixed connector-side (PR #225) and revalidated under issue #283. Default
// enablement remains a separate productionization decision. See
// docs/issues/issue-296-o1-j1-json-direct-read-path-contract.md and the
// module docs in `direct_projected.rs`.
mod direct_projected;

/// Polars-backed local tabular connector.
#[derive(Debug, Default)]
pub struct LocalTabularConnector;

#[async_trait]
impl SourceConnector for LocalTabularConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::LocalFile
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            schema_discovery: true,
            preview: true,
            streaming: true,
            incremental_read: false,
            predicate_pushdown: false,
            column_projection: true,
            range_read: false,
            change_tracking: false,
        }
    }

    async fn test_connection(
        &self,
        connection: &SourceConnection,
        request: TestConnectionRequest,
    ) -> ConnectorResult<ConnectionStatus> {
        request.validate()?;
        let config = config::LocalTabularConfig::parse(connection)?;
        path::RootSet::open(&config)?;
        request.context.ensure_active()?;
        Ok(ConnectionStatus::Ok)
    }

    async fn discover(
        &self,
        connection: &SourceConnection,
        request: DiscoverRequest,
    ) -> ConnectorResult<Vec<SourceAsset>> {
        request.validate()?;
        let config = config::LocalTabularConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        roots.discover(
            connection.id(),
            request.parent_path.as_deref(),
            &request.context,
            config.max_discovery_depth,
            config.max_discovered_assets,
        )
    }

    async fn inspect(
        &self,
        connection: &SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<AssetMetadata> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        let config = config::LocalTabularConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        let opened = roots.open_asset(&request.asset)?;
        inspect::inspect_opened_asset(opened, &request.asset, &config, &request.context)
    }

    async fn preview(
        &self,
        connection: &SourceConnection,
        request: PreviewRequest,
    ) -> ConnectorResult<PreviewData> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        reject_filter(request.filter.is_some())?;
        if request.sampling != SamplingStrategy::Head {
            return Err(ConnectorError::for_unsupported_capability(
                "preview_sampling",
            ));
        }
        let config = config::LocalTabularConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        preview::preview_asset(&config, &roots, request).await
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
        let config = config::LocalTabularConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        let reader = read::prepare_reader(
            &config,
            &roots,
            &request.asset,
            read::PrepareOptions {
                schema_override: request.schema_override.as_ref(),
                projection_ids: request.projection.as_deref(),
                batch_size: request.batch_size,
                max_rows: None,
                context: &request.context,
            },
        )?;
        Ok(reader.into_raw_stream())
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
