use std::{collections::HashSet, path::Path, sync::Arc};

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
        CreateProjectRequest, DatasetListResponse, DownloadQuery, HealthResponse,
        ImportDatasetResponse, ListDatasetsQuery, PreviewQuery, PreviewResponse,
        ProjectListResponse, ProjectNodeSnapshot, ProjectResponse, RenameDatasetRequest,
        RunPipelineRequest, RunPipelineResponse, SaveProjectWorkspaceRequest, StoredDataset,
        StoredProject, UpdateProjectRequest,
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
    Query(query): Query<ListDatasetsQuery>,
) -> Json<DatasetListResponse> {
    let datasets = state
        .storage
        .list_datasets(query.project_id)
        .await
        .iter()
        .map(StoredDataset::to_dto)
        .collect();
    Json(DatasetListResponse { datasets })
}

pub async fn rename_dataset(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<RenameDatasetRequest>,
) -> ApiResult<Json<ImportDatasetResponse>> {
    let name = normalize_dataset_name(&request.name)?;
    let dataset = state
        .storage
        .rename_dataset(id, name)
        .await?
        .ok_or_else(|| ApiError::not_found("Dataset not found"))?;
    Ok(Json(ImportDatasetResponse {
        dataset: dataset.to_dto(),
    }))
}

pub async fn delete_dataset(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let dataset = state
        .storage
        .remove_dataset(id)
        .await?
        .ok_or_else(|| ApiError::not_found("Dataset not found"))?;
    remove_dataset_file(&state.storage, &dataset).await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_projects(
    State(state): State<AppState>,
) -> Json<ProjectListResponse> {
    Json(ProjectListResponse {
        projects: state.storage.list_projects().await,
    })
}

pub async fn create_project(
    State(state): State<AppState>,
    Json(request): Json<CreateProjectRequest>,
) -> ApiResult<(StatusCode, Json<ProjectResponse>)> {
    let name = normalize_project_name(&request.name)?;
    validate_project_nodes(&request.nodes)?;
    let now = Utc::now();
    let project = StoredProject {
        id: Uuid::new_v4(),
        name,
        description: normalize_description(&request.description)?,
        selected_dataset_id: None,
        latest_output_id: None,
        nodes: request.nodes,
        created_at: now,
        updated_at: now,
    };
    state.storage.insert_project(project.clone()).await?;
    Ok((
        StatusCode::CREATED,
        Json(ProjectResponse { project }),
    ))
}

pub async fn get_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<ProjectResponse>> {
    let project = state
        .storage
        .get_project(id)
        .await
        .ok_or_else(|| ApiError::not_found("Project not found"))?;
    Ok(Json(ProjectResponse { project }))
}

pub async fn update_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<UpdateProjectRequest>,
) -> ApiResult<Json<ProjectResponse>> {
    if request.name.is_none() && request.description.is_none() {
        return Err(ApiError::bad_request(
            "At least one project field is required",
        ));
    }
    let name = request
        .name
        .as_deref()
        .map(normalize_project_name)
        .transpose()?;
    let description = request
        .description
        .as_deref()
        .map(normalize_description)
        .transpose()?;
    let project = state
        .storage
        .update_project(id, name, description)
        .await?
        .ok_or_else(|| ApiError::not_found("Project not found"))?;
    Ok(Json(ProjectResponse { project }))
}

pub async fn save_project_workspace(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<SaveProjectWorkspaceRequest>,
) -> ApiResult<Json<ProjectResponse>> {
    if state.storage.get_project(id).await.is_none() {
        return Err(ApiError::not_found("Project not found"));
    }
    validate_project_nodes(&request.nodes)?;
    validate_dataset_reference(
        &state.storage,
        id,
        request.selected_dataset_id,
        None,
        "Selected dataset",
    )
    .await?;
    validate_dataset_reference(
        &state.storage,
        id,
        request.latest_output_id,
        Some("output"),
        "Latest output",
    )
    .await?;

    let project = state
        .storage
        .save_project_workspace(
            id,
            request.selected_dataset_id,
            request.latest_output_id,
            request.nodes,
        )
        .await?
        .ok_or_else(|| ApiError::not_found("Project not found"))?;
    Ok(Json(ProjectResponse { project }))
}

pub async fn delete_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    if state.storage.project_count().await <= 1 {
        return Err(ApiError::conflict("The last project cannot be deleted"));
    }
    let (_, datasets) = state
        .storage
        .remove_project(id)
        .await?
        .ok_or_else(|| ApiError::not_found("Project not found"))?;
    for dataset in datasets {
        remove_dataset_file(&state.storage, &dataset).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn import_dataset(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<ImportDatasetResponse>)> {
    let mut upload = None;
    let mut project_id = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("Invalid multipart upload: {error}")))?
    {
        let field_name = field.name().unwrap_or_default().to_owned();
        if field_name == "projectId" {
            let value = field.text().await.map_err(|error| {
                ApiError::bad_request(format!("Could not read projectId: {error}"))
            })?;
            project_id = Some(
                Uuid::parse_str(value.trim())
                    .map_err(|_| ApiError::bad_request("projectId must be a valid UUID"))?,
            );
        } else if field_name == "file" {
            let file_name = field
                .file_name()
                .map(safe_display_name)
                .unwrap_or_else(|| "upload.csv".to_owned());
            let bytes = field.bytes().await.map_err(|error| {
                ApiError::bad_request(format!("Could not read upload: {error}"))
            })?;
            upload = Some((file_name, bytes));
        }
    }

    let project_id =
        project_id.ok_or_else(|| ApiError::bad_request("Multipart field 'projectId' is required"))?;
    if state.storage.get_project(project_id).await.is_none() {
        return Err(ApiError::not_found("Project not found"));
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
        project_id: Some(project_id),
        name: file_name,
        category: "source".to_owned(),
        source: "local".to_owned(),
        row_count: table.rows.len(),
        columns: table.headers,
        storage_path: format!("uploads/{id}.csv"),
        created_at: Utc::now(),
    };

    if let Err(error) = state.storage.insert_dataset(dataset.clone()).await {
        let _ = fs::remove_file(&upload_path).await;
        return Err(error.into());
    }
    if let Err(error) = state
        .storage
        .update_project_dataset_state(project_id, Some(id), None)
        .await
    {
        tracing::warn!(%project_id, %error, "could not persist imported dataset selection");
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
        .get_dataset(id)
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
        .get_dataset(request.dataset_id)
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
        project_id: source.project_id,
        name: ensure_csv_extension(output_name),
        category: "output".to_owned(),
        source: "generated".to_owned(),
        row_count: output.table.rows.len(),
        columns: output.table.headers.clone(),
        storage_path: format!("exports/{job_id}.csv"),
        created_at: Utc::now(),
    };

    if let Err(error) = state.storage.insert_dataset(dataset.clone()).await {
        let _ = fs::remove_file(&export_path).await;
        return Err(error.into());
    }
    if let Some(project_id) = source.project_id {
        if let Err(error) = state
            .storage
            .update_project_dataset_state(project_id, Some(source.id), Some(job_id))
            .await
        {
            tracing::warn!(%project_id, %error, "could not persist pipeline output selection");
        }
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
        .get_dataset(id)
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

async fn validate_dataset_reference(
    storage: &Storage,
    project_id: Uuid,
    dataset_id: Option<Uuid>,
    required_category: Option<&str>,
    label: &str,
) -> ApiResult<()> {
    let Some(dataset_id) = dataset_id else {
        return Ok(());
    };
    let dataset = storage
        .get_dataset(dataset_id)
        .await
        .ok_or_else(|| ApiError::bad_request(format!("{label} was not found")))?;
    if dataset.project_id != Some(project_id) {
        return Err(ApiError::bad_request(format!(
            "{label} does not belong to this project"
        )));
    }
    if required_category.is_some_and(|category| dataset.category != category) {
        return Err(ApiError::bad_request(format!(
            "{label} has an invalid category"
        )));
    }
    Ok(())
}

fn validate_project_nodes(nodes: &[ProjectNodeSnapshot]) -> ApiResult<()> {
    if nodes.len() > 100 {
        return Err(ApiError::bad_request(
            "A project cannot contain more than 100 nodes",
        ));
    }

    let mut ids = HashSet::new();
    for node in nodes {
        if node.id.trim().is_empty() || node.id.chars().count() > 100 {
            return Err(ApiError::bad_request("Every node needs a valid id"));
        }
        if !ids.insert(node.id.as_str()) {
            return Err(ApiError::bad_request("Node ids must be unique"));
        }
        if !matches!(
            node.node_type.as_str(),
            "source" | "filter" | "deduplicate" | "normalize" | "export"
        ) {
            return Err(ApiError::bad_request(format!(
                "Unsupported node type: {}",
                node.node_type
            )));
        }
        if !matches!(
            node.status.as_str(),
            "completed" | "running" | "pending" | "failed" | "disabled"
        ) {
            return Err(ApiError::bad_request(format!(
                "Unsupported node status: {}",
                node.status
            )));
        }
        if node.name.trim().is_empty() || node.name.chars().count() > 180 {
            return Err(ApiError::bad_request("Every node needs a valid name"));
        }
        if node.description.chars().count() > 500 || node.rows.chars().count() > 100 {
            return Err(ApiError::bad_request("Node metadata is too long"));
        }
    }
    Ok(())
}

fn normalize_project_name(raw: &str) -> ApiResult<String> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("Project name is required"));
    }
    if name.chars().count() > 120 || name.chars().any(char::is_control) {
        return Err(ApiError::bad_request("Project name is invalid"));
    }
    Ok(name.to_owned())
}

fn normalize_description(raw: &str) -> ApiResult<String> {
    let description = raw.trim();
    if description.chars().count() > 1_000 {
        return Err(ApiError::bad_request(
            "Project description cannot exceed 1000 characters",
        ));
    }
    Ok(description.to_owned())
}

fn normalize_dataset_name(raw: &str) -> ApiResult<String> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("Dataset name is required"));
    }
    if name.chars().count() > 180 || name.chars().any(char::is_control) {
        return Err(ApiError::bad_request("Dataset name is invalid"));
    }
    Ok(ensure_csv_extension(safe_display_name(name)))
}

async fn remove_dataset_file(storage: &Storage, dataset: &StoredDataset) {
    if let Err(error) = fs::remove_file(storage.resolve(dataset)).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                dataset_id = %dataset.id,
                %error,
                "could not remove dataset file"
            );
        }
    }
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
