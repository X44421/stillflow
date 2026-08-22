export interface Dataset {
  id: string;
  name: string;
  type: 'csv' | 'parquet' | 'excel' | 's3' | 'table';
  category: 'source' | 'interim' | 'output';
  size: string;
  source?: 'sample' | 'local' | 'connected';
  tableName?: string;
  icon?: string;
}

export type NodeType = 'source' | 'filter' | 'deduplicate' | 'normalize' | 'export';
export type NodeStatus = 'completed' | 'running' | 'pending' | 'failed' | 'disabled';

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

export interface TransformObject {
  id: string;
  name: string;
  description: string;
  category: 'source' | 'transform' | 'output' | 'ai';
  icon: string;
  available: boolean;
}
