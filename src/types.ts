export interface Dataset {
  id: string;
  name: string;
  type: 'csv' | 'parquet' | 'excel' | 's3' | 'table';
  category: 'source' | 'interim' | 'output';
  size: string;
  icon?: string;
}

export interface PipelineNode {
  id: string;
  type: 'source' | 'filter' | 'deduplicate' | 'normalize' | 'export';
  name: string;
  description: string;
  rows: string;
  status: 'completed' | 'running' | 'pending';
}

export interface TransformObject {
  id: string;
  name: string;
  description: string;
  category: 'source' | 'transform' | 'output' | 'ai';
  icon: string;
}
