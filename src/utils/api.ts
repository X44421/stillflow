import type {
  DataPreviewResult,
  Dataset,
  PipelineMetrics,
  PipelineNode,
  Project,
} from '../types';

const API_BASE = import.meta.env.VITE_API_URL?.replace(/\/$/, '') ?? '';

interface DatasetListResponse {
  datasets: Dataset[];
}

interface ImportDatasetResponse {
  dataset: Dataset;
}

interface ProjectListResponse {
  projects: Project[];
}

interface ProjectResponse {
  project: Project;
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
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export async function listBackendDatasets(projectId?: string): Promise<Dataset[]> {
  const query = projectId
    ? `?projectId=${encodeURIComponent(projectId)}`
    : '';
  const response = await apiRequest<DatasetListResponse>(`/api/datasets${query}`);
  return response.datasets;
}

export async function importCsvDataset(
  file: File,
  projectId: string
): Promise<Dataset> {
  const form = new FormData();
  form.append('projectId', projectId);
  form.append('file', file);
  const response = await apiRequest<ImportDatasetResponse>('/api/datasets/import', {
    method: 'POST',
    body: form,
  });
  return response.dataset;
}

export async function renameBackendDataset(
  datasetId: string,
  name: string
): Promise<Dataset> {
  const response = await apiRequest<ImportDatasetResponse>(
    `/api/datasets/${encodeURIComponent(datasetId)}`,
    {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    }
  );
  return response.dataset;
}

export async function deleteBackendDataset(datasetId: string): Promise<void> {
  await apiRequest<void>(`/api/datasets/${encodeURIComponent(datasetId)}`, {
    method: 'DELETE',
  });
}

export async function listProjects(): Promise<Project[]> {
  const response = await apiRequest<ProjectListResponse>('/api/projects');
  return response.projects;
}

export async function createProject(
  name: string,
  nodes: PipelineNode[],
  description = ''
): Promise<Project> {
  const response = await apiRequest<ProjectResponse>('/api/projects', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description, nodes }),
  });
  return response.project;
}

export async function updateProject(
  projectId: string,
  patch: { name?: string; description?: string }
): Promise<Project> {
  const response = await apiRequest<ProjectResponse>(
    `/api/projects/${encodeURIComponent(projectId)}`,
    {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }
  );
  return response.project;
}

export async function saveProjectWorkspace(
  projectId: string,
  workspace: {
    selectedDatasetId: string | null;
    latestOutputId: string | null;
    nodes: PipelineNode[];
  }
): Promise<Project> {
  const response = await apiRequest<ProjectResponse>(
    `/api/projects/${encodeURIComponent(projectId)}/workspace`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(workspace),
    }
  );
  return response.project;
}

export async function deleteProject(projectId: string): Promise<void> {
  await apiRequest<void>(`/api/projects/${encodeURIComponent(projectId)}`, {
    method: 'DELETE',
  });
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
