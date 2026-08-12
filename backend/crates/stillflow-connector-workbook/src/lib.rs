//! Bounded Calamine workbook analysis and Arrow streaming connector.

#![deny(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::indexing_slicing))]

use async_trait::async_trait;
use stillflow_connectors::{ConnectorCapabilities, RawBatchStream, SourceConnector};
use stillflow_core::{
    AssetMetadata, Checkpoint, CheckpointRequest, ConnectionStatus, ConnectorError, ConnectorKind,
    ConnectorResult, DiscoverRequest, InspectRequest, PreviewData, PreviewRequest, ReadRequest,
    SamplingStrategy, SourceAsset, SourceConnection, TestConnectionRequest,
};

mod analysis;
mod config;
mod discovery;
mod format;
mod inspect;
mod path;
mod preflight;
mod preview;
mod read;
mod schema;
mod workbook;

/// Calamine-backed local workbook connector.
#[derive(Debug, Default)]
pub struct WorkbookConnector;

#[async_trait]
impl SourceConnector for WorkbookConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::ExcelWorkbook
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
        let config = config::WorkbookConfig::parse(connection)?;
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
        let config = config::WorkbookConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        discovery::discover_sheets(
            &roots,
            connection.id(),
            request.parent_path.as_deref(),
            &config,
            &request.context,
        )
    }

    async fn inspect(
        &self,
        connection: &SourceConnection,
        request: InspectRequest,
    ) -> ConnectorResult<AssetMetadata> {
        request.validate()?;
        ensure_asset_connection(connection, &request.asset)?;
        let config = config::WorkbookConfig::parse(connection)?;
        let roots = path::RootSet::open(&config)?;
        let opened = roots.open_asset(&request.asset)?;
        inspect::inspect_opened(opened, &request.asset, &config, &request.context)
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
        let config = config::WorkbookConfig::parse(connection)?;
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
        let config = config::WorkbookConfig::parse(connection)?;
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
