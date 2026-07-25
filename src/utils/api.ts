import type {
  DataPreviewResult,
  Dataset,
  PipelineMetrics,
  PipelineNode,
} from '../types';

const API_BASE = import.meta.env.VITE_API_URL?.replace(/\/$/, '') ?? '';

interface DatasetListResponse {
  datasets: Dataset[];
}

interface ImportDatasetResponse {
  dataset: Dataset;
}

export interface BackendPipelineResult {
  jobId: string;
  status: 'completed';
  dataset: Dataset;
  executions: {
    nodeId: string;
    nodeType: string;
    metrics: PipelineMetrics;
    tableName: string;
  }[];
  totalDuration: number;
  downloadUrl: string;
}

async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, init);
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = (await response.json()) as { error?: string };
      if (body.error) message = body.error;
    } catch {
      // Keep the HTTP fallback when the server did not return JSON.
    }
    throw new Error(message);
  }
  return response.json() as Promise<T>;
}

export async function listBackendDatasets(): Promise<Dataset[]> {
  const response = await apiRequest<DatasetListResponse>('/api/datasets');
  return response.datasets;
}

export async function importCsvDataset(file: File): Promise<Dataset> {
  const form = new FormData();
  form.append('file', file);
  const response = await apiRequest<ImportDatasetResponse>('/api/datasets/import', {
    method: 'POST',
    body: form,
  });
  return response.dataset;
}

export async function runBackendPipeline(
  datasetId: string,
  nodes: PipelineNode[]
): Promise<BackendPipelineResult> {
  return apiRequest<BackendPipelineResult>('/api/pipeline/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      datasetId,
      nodes: nodes.map((node) => ({
        id: node.id,
        type: node.type,
        config: node.config,
      })),
    }),
  });
}

export async function previewBackendDataset(
  datasetId: string,
  limit: 100 | 500 = 100
): Promise<DataPreviewResult> {
  return apiRequest<DataPreviewResult>(
    `/api/datasets/${encodeURIComponent(datasetId)}/preview?limit=${limit}`
  );
}

export function getExportUrl(datasetId: string, download = true): string {
  return `${API_BASE}/api/exports/${encodeURIComponent(datasetId)}/download?download=${download}`;
}
