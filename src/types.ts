export interface Dataset {
  id: string;
  name: string;
  type: 'csv' | 'parquet' | 'excel' | 's3' | 'table';
  category: 'source' | 'interim' | 'output';
  size: string;
  icon?: string;
}

export type NodeType = 'source' | 'filter' | 'deduplicate' | 'normalize' | 'export';

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
  status: 'completed' | 'running' | 'pending';
  metrics?: PipelineMetrics;
  config: PipelineNodeConfig;
}

export interface TransformObject {
  id: string;
  name: string;
  description: string;
  category: 'source' | 'transform' | 'output' | 'ai';
  icon: string;
}