export interface Dataset {
  id: string;
  projectId?: string | null;
  name: string;
  type: 'csv' | 'parquet' | 'excel' | 's3' | 'table';
  category: 'source' | 'interim' | 'output';
  size: string;
  source?: 'sample' | 'local' | 'connected' | 'generated';
  tableName?: string;
  icon?: string;
  rowCount?: number;
  columns?: string[];
  downloadUrl?: string;
  createdAt?: string;
}

export type NodeType = 'source' | 'filter' | 'deduplicate' | 'normalize' | 'export';
export type NodeStatus = 'completed' | 'running' | 'pending' | 'failed' | 'disabled';
export type WorkspaceView = 'graph' | 'data';

export interface PipelineNodeConfig {
  column: string;
  strategy: string;
  scope: string;
  nullHandling: string;
}

export interface PipelineMetrics {
  rowsIn: number;
  rowsOut: number;
  duplicates: number;
  missing: number;
  nullColumns: number;
  qualityScore: number;
  duration: number;
  memory: number;
}

export interface PipelineNode {
  id: string;
  type: NodeType;
  name: string;
  description: string;
  rows: string;
  status: NodeStatus;
  metrics?: PipelineMetrics;
  error?: string;
  config: PipelineNodeConfig;
}

export interface Project {
  id: string;
  name: string;
  description: string;
  selectedDatasetId: string | null;
  latestOutputId: string | null;
  nodes: PipelineNode[];
  createdAt: string;
  updatedAt: string;
}

export interface PreviewColumn {
  name: string;
  type: string;
  nullCount: number;
  distinctCount: number;
}

export interface DataPreviewResult {
  tableName: string;
  columns: PreviewColumn[];
  rows: Record<string, unknown>[];
  totalRows: number;
}

export type PreviewLimit = 100 | 500;

export type EventLevel = 'info' | 'success' | 'error';

export interface WorkspaceEvent {
  id: string;
  objectId: string;
  objectName: string;
  action: string;
  detail: string;
  actor: 'User' | 'Engine' | 'System';
  level: EventLevel;
  timestamp: string;
}

export interface TransformObject {
  id: string;
  name: string;
  description: string;
  category: 'source' | 'transform' | 'output' | 'ai';
  icon: string;
  available: boolean;
}
