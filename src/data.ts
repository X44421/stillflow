import type { Dataset, NodeType, PipelineNode, PipelineNodeConfig, TransformObject } from './types';

export const datasets: Dataset[] = [
  { id: '1', name: 'raw_customers.csv', type: 'csv', category: 'source', size: '2.4M rows' },
  { id: '2', name: 'transactions_2024.csv', type: 'csv', category: 'source', size: '12.8M rows' },
  { id: '3', name: 'web_events.parquet', type: 'parquet', category: 'source', size: '85.6M rows' },
  { id: '4', name: 'marketing_data.xlsx', type: 'excel', category: 'source', size: '320K rows' },
  { id: '5', name: 's3://data/logs/', type: 's3', category: 'source', size: '128 files' },
  { id: '6', name: 'stg_customers', type: 'table', category: 'interim', size: '1.2M rows' },
  { id: '7', name: 'stg_transactions', type: 'table', category: 'interim', size: '8.7M rows' },
  { id: '8', name: 'stg_events', type: 'table', category: 'interim', size: '50.1M rows' },
  { id: '9', name: 'clean_customers', type: 'table', category: 'output', size: '1.1M rows' },
  { id: '10', name: 'customer_report.csv', type: 'csv', category: 'output', size: '1.1M rows' },
];

export const defaultConfig: PipelineNodeConfig = {
  column: '',
  strategy: 'Keep first',
  scope: 'Current dataset',
  nullHandling: 'Ignore',
};

/**
 * Rule defaults that match each node type's semantics — a Filter must never
 * inherit Deduplicate options like "Keep first" (and vice versa).
 */
export function defaultConfigFor(type: NodeType): PipelineNodeConfig {
  if (type === 'filter') {
    return {
      column: '',
      strategy: '',
      scope: 'Current dataset',
      nullHandling: 'Treat as non-match',
      mode: 'Keep matching rows',
      operator: 'is not empty',
      value: '',
    };
  }
  if (type === 'normalize') {
    return {
      column: '',
      strategy: '',
      scope: 'Current dataset',
      nullHandling: 'Ignore',
    };
  }
  return { ...defaultConfig };
}

export const initialPipelineNodes: PipelineNode[] = [
  { id: 'n1', type: 'source', name: 'raw_customers.csv', description: 'CSV File · 2.4M rows', rows: '2.4M', status: 'completed', config: { ...defaultConfig } },
  { id: 'n2', type: 'filter', name: 'Filter', description: 'Keep valid customers', rows: '1.8M', status: 'completed', config: { ...defaultConfigFor('filter'), column: 'status', mode: 'Keep matching rows', operator: 'is not empty', value: '' } },
  { id: 'n3', type: 'deduplicate', name: 'Deduplicate', description: 'Remove repeated records', rows: '1.2M', status: 'running', config: { ...defaultConfig } },
  { id: 'n4', type: 'normalize', name: 'Normalize Text', description: 'Standardize name & email', rows: '1.2M', status: 'pending', config: { ...defaultConfigFor('normalize') } },
  { id: 'n5', type: 'export', name: 'Export CSV', description: 'Write cleaned data', rows: '1.2M', status: 'pending', config: { ...defaultConfig } },
];

export const transformObjects: TransformObject[] = [
  {
    id: 't4',
    name: 'Filter',
    description: 'Keep matched rows',
    category: 'transform',
    icon: 'filter',
    available: true,
  },
  {
    id: 't5',
    name: 'Deduplicate',
    description: 'Remove repeated records',
    category: 'transform',
    icon: 'copy',
    available: true,
  },
  {
    id: 't6',
    name: 'Normalize Text',
    description: 'Clean string values',
    category: 'transform',
    icon: 'type',
    available: true,
  },
  {
    id: 't7',
    name: 'Export CSV',
    description: 'Write cleaned data',
    category: 'output',
    icon: 'upload',
    available: true,
  },
];
