use std::{path::Path, sync::Arc};

use axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, Query, State},
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
        Response, StatusCode,
    },
    Json,
};
use chrono::Utc;
use tokio::fs;
use uuid::Uuid;

use crate::{
    error::{ApiError, ApiResult},
    models::{
        DatasetListResponse, DownloadQuery, HealthResponse, ImportDatasetResponse, PreviewQuery,
        PreviewResponse, RunPipelineRequest, RunPipelineResponse, StoredDataset,
    },
    pipeline::{
        build_preview, execute_pipeline, read_csv_file, write_csv_file, PipelineError,
    },
    storage::Storage,
};

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "stillflow-backend",
    })
}

pub async fn list_datasets(
    State(state): State<AppState>,
) -> Json<DatasetListResponse> {
    let datasets = state
        .storage
        .list()
        .await
        .iter()
        .map(StoredDataset::to_dto)
        .collect();
    Json(DatasetListResponse { datasets })
}

pub async fn import_dataset(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<ImportDatasetResponse>)> {
    let mut upload = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("Invalid multipart upload: {error}")))?
    {
        if field.name() != Some("file") {
            continue;
        }

        let file_name = field
            .file_name()
            .map(safe_display_name)
            .unwrap_or_else(|| "upload.csv".to_owned());
        let bytes = field
            .bytes()
            .await
            .map_err(|error| ApiError::bad_request(format!("Could not read upload: {error}")))?;
        upload = Some((file_name, bytes));
        break;
    }

    let (file_name, bytes) =
        upload.ok_or_else(|| ApiError::bad_request("Multipart field 'file' is required"))?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request("Uploaded CSV is empty"));
    }
    if !file_name.to_lowercase().ends_with(".csv") {
        return Err(ApiError::bad_request("Only .csv files are supported"));
    }

    let id = Uuid::new_v4();
    let upload_path = state.storage.upload_path(id);
    fs::write(&upload_path, &bytes).await?;

    let inspection_path = upload_path.clone();
    let table = match tokio::task::spawn_blocking(move || read_csv_file(&inspection_path)).await {
        Ok(Ok(table)) => table,
        Ok(Err(error)) => {
            let _ = fs::remove_file(&upload_path).await;
            return Err(pipeline_request_error("Invalid CSV", error));
        }
        Err(error) => {
            let _ = fs::remove_file(&upload_path).await;
            return Err(ApiError::internal(format!(
                "CSV inspection task failed: {error}"
            )));
        }
    };

    let dataset = StoredDataset {
        id,
        name: file_name,
        category: "source".to_owned(),
        source: "local".to_owned(),
        row_count: table.rows.len(),
        columns: table.headers,
        storage_path: format!("uploads/{id}.csv"),
        created_at: Utc::now(),
    };

    if let Err(error) = state.storage.insert(dataset.clone()).await {
        let _ = fs::remove_file(&upload_path).await;
        return Err(error.into());
    }

    Ok((
        StatusCode::CREATED,
        Json(ImportDatasetResponse {
            dataset: dataset.to_dto(),
        }),
    ))
}

pub async fn preview_dataset(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> ApiResult<Json<PreviewResponse>> {
    let dataset = state
        .storage
        .get(id)
        .await
        .ok_or_else(|| ApiError::not_found("Dataset not found"))?;
    let path = state.storage.resolve(&dataset);
    let limit = query.limit.unwrap_or(100).clamp(1, 500);

    let table = tokio::task::spawn_blocking(move || read_csv_file(&path))
        .await
        .map_err(|error| ApiError::internal(format!("CSV preview task failed: {error}")))?
        .map_err(|error| pipeline_request_error("Could not preview CSV", error))?;
    let (columns, rows) = build_preview(&table, limit);

    Ok(Json(PreviewResponse {
        table_name: dataset.id.to_string(),
        columns,
        rows,
        total_rows: table.rows.len(),
    }))
}

pub async fn run_pipeline(
    State(state): State<AppState>,
    Json(request): Json<RunPipelineRequest>,
) -> ApiResult<Json<RunPipelineResponse>> {
    if request.nodes.is_empty() {
        return Err(ApiError::bad_request(
            "Pipeline must contain at least one enabled node",
        ));
    }
    if request.nodes.len() > 100 {
        return Err(ApiError::bad_request(
            "Pipeline cannot contain more than 100 nodes",
        ));
    }

    let source = state
        .storage
        .get(request.dataset_id)
        .await
        .ok_or_else(|| ApiError::not_found("Source dataset not found"))?;
    if source.category != "source" {
        return Err(ApiError::bad_request(
            "Pipeline input must be a source dataset",
        ));
    }

    let job_id = Uuid::new_v4();
    let source_path = state.storage.resolve(&source);
    let export_path = state.storage.export_path(job_id);
    let nodes = request.nodes;
    let blocking_export_path = export_path.clone();

    let task_result = tokio::task::spawn_blocking(move || {
        let table = read_csv_file(&source_path)?;
        let output = execute_pipeline(table, &nodes)?;
        write_csv_file(&blocking_export_path, &output.table)?;
        Ok::<_, PipelineError>(output)
    })
    .await
    .map_err(|error| ApiError::internal(format!("Pipeline task failed: {error}")));
    let output = match task_result {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            let _ = fs::remove_file(&export_path).await;
            return Err(pipeline_request_error("Pipeline failed", error));
        }
        Err(error) => {
            let _ = fs::remove_file(&export_path).await;
            return Err(error);
        }
    };

    let output_name = request
        .output_name
        .as_deref()
        .map(safe_display_name)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| cleaned_file_name(&source.name));
    let dataset = StoredDataset {
        id: job_id,
        name: ensure_csv_extension(output_name),
        category: "output".to_owned(),
        source: "generated".to_owned(),
        row_count: output.table.rows.len(),
        columns: output.table.headers.clone(),
        storage_path: format!("exports/{job_id}.csv"),
        created_at: Utc::now(),
    };

    if let Err(error) = state.storage.insert(dataset.clone()).await {
        let _ = fs::remove_file(&export_path).await;
        return Err(error.into());
    }

    let download_url = format!("/api/exports/{job_id}/download");
    Ok(Json(RunPipelineResponse {
        job_id,
        status: "completed",
        dataset: dataset.to_dto(),
        executions: output.executions,
        total_duration: output.total_duration,
        download_url,
    }))
}

pub async fn download_export(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<DownloadQuery>,
) -> ApiResult<Response<Body>> {
    let dataset = state
        .storage
        .get(id)
        .await
        .ok_or_else(|| ApiError::not_found("Export not found"))?;
    if dataset.category != "output" {
        return Err(ApiError::not_found("Export not found"));
    }

    let bytes = fs::read(state.storage.resolve(&dataset)).await?;
    let disposition = if query.download.unwrap_or(true) {
        "attachment"
    } else {
        "inline"
    };
    let file_name = ascii_file_name(&dataset.name);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            CONTENT_DISPOSITION,
            format!("{disposition}; filename=\"{file_name}\""),
        )
        .body(Body::from(bytes))
        .map_err(|error| ApiError::internal(format!("Could not build response: {error}")))
}

fn pipeline_request_error(context: &str, error: PipelineError) -> ApiError {
    match error {
        PipelineError::Invalid(message) => {
            ApiError::bad_request(format!("{context}: {message}"))
        }
        PipelineError::Csv(error) => {
            ApiError::bad_request(format!("{context}: {error}"))
        }
        PipelineError::Io(error) => {
            tracing::error!(%error, "pipeline file operation failed");
            ApiError::internal(format!("{context}: file operation failed"))
        }
    }
}

fn safe_display_name(raw: &str) -> String {
    let normalized = raw.replace('\\', "/");
    Path::new(&normalized)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload.csv")
        .trim()
        .chars()
        .take(180)
        .collect()
}

fn cleaned_file_name(source_name: &str) -> String {
    let stem = Path::new(source_name)
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("dataset");
    format!("{stem}_cleaned.csv")
}

fn ensure_csv_extension(name: String) -> String {
    if name.to_lowercase().ends_with(".csv") {
        name
    } else {
        format!("{name}.csv")
    }
}

fn ascii_file_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "cleaned.csv".to_owned()
    } else {
        sanitized
    }
}
