use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{RawBatchStream, SourceConnector};
use stillflow_core::{
    AssetLocator, AssetMetadata, ConnectorError, ConnectorKind, ConnectorResult, CredentialRef,
    ErrorCategory, FindingSeverity, InspectRequest, InspectionFinding, PreviewData, PreviewRequest,
    ReadRequest, SourceAsset, SourceConnection,
};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use crate::access::{ObjectInfo, ObjectStorageAccess, StoreAccess};

const STAGED_BASENAME: &str = "source";

pub(crate) async fn inspect_text(
    access: &StoreAccess,
    remote: &SourceAsset,
    context: &stillflow_core::RequestContext,
) -> ConnectorResult<AssetMetadata> {
    let info = access.head(&remote.locator.path, context).await?;
    let staged = stage_preview(access, remote, &info, context).await?;
    let mut metadata = LocalTabularConnector
        .inspect(
            &staged.connection,
            InspectRequest {
                context: context.clone(),
                asset: staged.asset.clone(),
            },
        )
        .await?;
    metadata.size_bytes = Some(info.size);
    metadata.modified_at = Some(info.last_modified);
    if staged.source_truncated {
        metadata.findings.push(InspectionFinding {
            code: "inspect.remote_source_range_truncated".to_owned(),
            message: "schema inference stopped at the remote preview byte bound".to_owned(),
            severity: FindingSeverity::Warning,
        });
    }
    Ok(metadata)
}

pub(crate) async fn preview_text(
    access: &StoreAccess,
    remote_request: PreviewRequest,
) -> ConnectorResult<PreviewData> {
    let info = access
        .head(&remote_request.asset.locator.path, &remote_request.context)
        .await?;
    let staged = stage_preview(
        access,
        &remote_request.asset,
        &info,
        &remote_request.context,
    )
    .await?;
    let mut local_request = remote_request;
    local_request.asset = staged.asset.clone();
    let mut preview = LocalTabularConnector
        .preview(&staged.connection, local_request)
        .await?;
    if staged.source_truncated {
        preview.rows_truncated = true;
        preview.bytes_truncated = true;
        preview
            .warnings
            .push("preview.remote_source_range_truncated".to_owned());
    }
    Ok(preview)
}

pub(crate) async fn read_text(
    access: &StoreAccess,
    remote_request: ReadRequest,
) -> ConnectorResult<RawBatchStream> {
    let staged = stage_complete(access, &remote_request.asset, &remote_request.context).await?;
    let mut local_request = remote_request;
    local_request.asset = staged.asset.clone();
    let stream = LocalTabularConnector
        .read_batches(&staged.connection, local_request)
        .await?;
    Ok(stream.with_drop_guard(staged.directory))
}

struct StagedSource {
    directory: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    source_truncated: bool,
}

async fn stage_preview(
    access: &StoreAccess,
    remote: &SourceAsset,
    info: &ObjectInfo,
    context: &stillflow_core::RequestContext,
) -> ConnectorResult<StagedSource> {
    context.ensure_active()?;
    let maximum = access.max_preview_source_bytes();
    let maximum_u64 = u64::try_from(maximum).map_err(|_| {
        staging_error(
            ErrorCategory::Internal,
            false,
            "remote preview byte bound exceeds the platform range",
        )
    })?;
    let requested = info.size.min(maximum_u64);
    let source_truncated = requested < info.size;
    let bytes = if requested == 0 {
        Bytes::new()
    } else {
        access
            .get_range_versioned(&remote.locator.path, 0..requested, info, context)
            .await?
    };
    let bytes = if source_truncated {
        close_truncated_text(&remote.locator.path, &bytes)?
    } else {
        bytes.to_vec()
    };
    stage_bytes(remote, &bytes, maximum, source_truncated).await
}

async fn stage_complete(
    access: &StoreAccess,
    remote: &SourceAsset,
    context: &stillflow_core::RequestContext,
) -> ConnectorResult<StagedSource> {
    context.ensure_active()?;
    let directory = tempfile::tempdir().map_err(|_| {
        staging_error(
            ErrorCategory::TransientSource,
            true,
            "temporary object staging directory could not be created",
        )
    })?;
    let file_name = staged_file_name(&remote.locator.path)?;
    let path = directory.path().join(&file_name);
    let mut file = tokio::fs::File::create(&path).await.map_err(|_| {
        staging_error(
            ErrorCategory::TransientSource,
            true,
            "temporary object staging file could not be created",
        )
    })?;
    let mut stream = access.stream(&remote.locator.path, context).await?;
    while let Some(bytes) = stream.next().await.transpose()? {
        context.ensure_active()?;
        file.write_all(&bytes).await.map_err(|_| {
            staging_error(
                ErrorCategory::TransientSource,
                true,
                "remote object could not be staged",
            )
        })?;
    }
    file.flush().await.map_err(|_| {
        staging_error(
            ErrorCategory::TransientSource,
            true,
            "temporary object staging file could not be flushed",
        )
    })?;
    drop(file);
    staged_source(
        directory,
        remote,
        file_name,
        access.max_preview_source_bytes(),
        false,
    )
}

async fn stage_bytes(
    remote: &SourceAsset,
    bytes: &[u8],
    inference_bytes: usize,
    source_truncated: bool,
) -> ConnectorResult<StagedSource> {
    let directory = tempfile::tempdir().map_err(|_| {
        staging_error(
            ErrorCategory::TransientSource,
            true,
            "temporary object staging directory could not be created",
        )
    })?;
    let file_name = staged_file_name(&remote.locator.path)?;
    tokio::fs::write(directory.path().join(&file_name), bytes)
        .await
        .map_err(|_| {
            staging_error(
                ErrorCategory::TransientSource,
                true,
                "remote object preview range could not be staged",
            )
        })?;
    staged_source(
        directory,
        remote,
        file_name,
        inference_bytes,
        source_truncated,
    )
}

fn staged_source(
    directory: TempDir,
    remote: &SourceAsset,
    file_name: String,
    inference_bytes: usize,
    source_truncated: bool,
) -> ConnectorResult<StagedSource> {
    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "internal object staging",
        serde_json::json!({
            "allowedRoots": [directory.path()],
            "maxDiscoveryDepth": 1,
            "maxDiscoveredAssets": 1,
            "schemaInference": {
                "maxRows": 10_000,
                "maxBytes": inference_bytes
            }
        }),
        CredentialRef::new("cred://internal/object-staging")?,
    )?;
    let mut asset = remote.clone();
    asset.connection_id = connection.id();
    asset.name = file_name.clone();
    asset.locator = AssetLocator {
        path: file_name,
        container: Some("root-0".to_owned()),
        schema: None,
        sheet: None,
        workbook_region: None,
    };
    Ok(StagedSource {
        directory,
        connection,
        asset,
        source_truncated,
    })
}

fn staged_file_name(remote_key: &str) -> ConnectorResult<String> {
    let extension = Path::new(remote_key)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| {
            ConnectorError::invalid_configuration(
                "remote tabular object has no supported extension",
            )
        })?;
    if !matches!(
        extension.as_str(),
        "csv" | "tsv" | "json" | "jsonl" | "ndjson"
    ) {
        return Err(ConnectorError::invalid_configuration(
            "remote object is not a staged text format",
        ));
    }
    Ok(format!("{STAGED_BASENAME}.{extension}"))
}

fn close_truncated_text(remote_key: &str, bytes: &[u8]) -> ConnectorResult<Vec<u8>> {
    let extension = Path::new(remote_key)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "csv" | "tsv" => close_delimited_prefix(bytes),
        "jsonl" | "ndjson" => close_line_prefix(bytes),
        "json" => close_json_array_prefix(bytes),
        _ => Err(ConnectorError::invalid_configuration(
            "remote object is not a staged text format",
        )),
    }
}

fn close_line_prefix(bytes: &[u8]) -> ConnectorResult<Vec<u8>> {
    let Some(end) = bytes.iter().rposition(|byte| *byte == b'\n') else {
        return Err(staging_error(
            ErrorCategory::InvalidData,
            false,
            "first remote record exceeds the preview source byte bound",
        ));
    };
    Ok(bytes.get(..=end).unwrap_or_default().to_vec())
}

fn close_delimited_prefix(bytes: &[u8]) -> ConnectorResult<Vec<u8>> {
    let mut in_quotes = false;
    let mut last_boundary = None;
    let mut index = 0_usize;
    while let Some(byte) = bytes.get(index) {
        match *byte {
            b'"' if in_quotes && bytes.get(index + 1) == Some(&b'"') => {
                index = index.saturating_add(1);
            }
            b'"' => in_quotes = !in_quotes,
            b'\n' if !in_quotes => last_boundary = index.checked_add(1),
            _ => {}
        }
        index = index.saturating_add(1);
    }
    let Some(end) = last_boundary else {
        return Err(staging_error(
            ErrorCategory::InvalidData,
            false,
            "first remote record exceeds the preview source byte bound",
        ));
    };
    Ok(bytes.get(..end).unwrap_or_default().to_vec())
}

fn close_json_array_prefix(bytes: &[u8]) -> ConnectorResult<Vec<u8>> {
    let start = bytes
        .iter()
        .position(|byte| {
            !byte.is_ascii_whitespace() && *byte != 0xEF && *byte != 0xBB && *byte != 0xBF
        })
        .ok_or_else(|| {
            staging_error(
                ErrorCategory::InvalidData,
                false,
                "remote JSON preview range is empty",
            )
        })?;
    if bytes.get(start) != Some(&b'[') {
        return Err(staging_error(
            ErrorCategory::InvalidData,
            false,
            "remote JSON source must be one top-level array",
        ));
    }
    let mut objects: Vec<&[u8]> = Vec::new();
    let mut object_start = None;
    let mut curly_depth = 0_usize;
    let mut square_depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().copied().enumerate().skip(start + 1) {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => {
                if curly_depth == 0 && square_depth == 0 {
                    object_start = Some(index);
                }
                curly_depth = curly_depth.saturating_add(1);
            }
            b'}' if curly_depth > 0 => {
                curly_depth -= 1;
                if curly_depth == 0 && square_depth == 0 {
                    if let Some(object_start) = object_start.take() {
                        if let Some(object) = bytes.get(object_start..=index) {
                            objects.push(object);
                        }
                    }
                }
            }
            b'[' if curly_depth > 0 => square_depth = square_depth.saturating_add(1),
            b']' if square_depth > 0 => square_depth -= 1,
            _ => {}
        }
    }
    if objects.is_empty() {
        return Err(staging_error(
            ErrorCategory::InvalidData,
            false,
            "first remote JSON object exceeds the preview source byte bound",
        ));
    }
    let capacity = objects
        .iter()
        .try_fold(2_usize, |total, object| total.checked_add(object.len() + 1))
        .ok_or_else(|| {
            staging_error(
                ErrorCategory::InvalidData,
                false,
                "remote JSON preview range is too large",
            )
        })?;
    let mut output = Vec::with_capacity(capacity);
    output.push(b'[');
    for (index, object) in objects.into_iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(object);
    }
    output.push(b']');
    Ok(output)
}

fn staging_error(
    category: ErrorCategory,
    retryable: bool,
    message: &'static str,
) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closes_quoted_csv_ndjson_and_json_prefixes() {
        assert_eq!(
            close_delimited_prefix(b"id,text\n1,\"hello\nworld\"\n2,partial").expect("CSV"),
            b"id,text\n1,\"hello\nworld\"\n"
        );
        assert_eq!(
            close_line_prefix(b"{\"a\":1}\n{\"a\":").expect("NDJSON"),
            b"{\"a\":1}\n"
        );
        assert_eq!(
            close_json_array_prefix(b"[ {\"a\":1}, {\"a\": {\"nested\": 2}}, {\"a\":")
                .expect("JSON"),
            b"[{\"a\":1},{\"a\": {\"nested\": 2}}]"
        );
    }
}
