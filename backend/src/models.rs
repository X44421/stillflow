use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredDataset {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub source: String,
    pub row_count: usize,
    pub columns: Vec<String>,
    pub storage_path: String,
    pub created_at: DateTime<Utc>,
}

impl StoredDataset {
    pub fn to_dto(&self) -> DatasetDto {
        DatasetDto {
            id: self.id,
            name: self.name.clone(),
            dataset_type: "csv".to_owned(),
            category: self.category.clone(),
            size: format!("{} rows", self.row_count),
            source: self.source.clone(),
            table_name: Some(self.id.to_string()),
            row_count: self.row_count,
            columns: self.columns.clone(),
            download_url: (self.category == "output")
                .then(|| format!("/api/exports/{}/download", self.id)),
            created_at: self.created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDto {
    pub id: Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub dataset_type: String,
    pub category: String,
    pub size: String,
    pub source: String,
    pub table_name: Option<String>,
    pub row_count: usize,
    pub columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetListResponse {
    pub datasets: Vec<DatasetDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportDatasetResponse {
    pub dataset: DatasetDto,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PipelineNodeRequest {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub config: PipelineConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PipelineConfig {
    pub column: String,
    pub strategy: String,
    pub scope: String,
    pub null_handling: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            column: String::new(),
            strategy: "Keep first".to_owned(),
            scope: "Current dataset".to_owned(),
            null_handling: "Ignore".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPipelineRequest {
    pub dataset_id: Uuid,
    pub nodes: Vec<PipelineNodeRequest>,
    pub output_name: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineMetrics {
    pub rows_in: usize,
    pub rows_out: usize,
    pub duplicates: f64,
    pub missing: f64,
    pub null_columns: usize,
    pub quality_score: u8,
    pub duration: f64,
    pub memory: f64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineExecution {
    pub node_id: String,
    pub node_type: String,
    pub metrics: PipelineMetrics,
    pub table_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPipelineResponse {
    pub job_id: Uuid,
    pub status: &'static str,
    pub dataset: DatasetDto,
    pub executions: Vec<PipelineExecution>,
    pub total_duration: f64,
    pub download_url: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    pub null_count: usize,
    pub distinct_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewResponse {
    pub table_name: String,
    pub columns: Vec<PreviewColumn>,
    pub rows: Vec<BTreeMap<String, String>>,
    pub total_rows: usize,
}

#[derive(Debug, Deserialize)]
pub struct PreviewQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DownloadQuery {
    pub download: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}
