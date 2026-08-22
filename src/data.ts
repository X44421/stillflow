import { Dataset, PipelineNode, TransformObject, PipelineNodeConfig } from './types';

export const datasets: Dataset[] = [
  // Source
  { id: '1', name: 'raw_customers.csv', type: 'csv', category: 'source', size: '2.4M rows' },
  { id: '2', name: 'transactions_2024.csv', type: 'csv', category: 'source', size: '12.8M rows' },
  { id: '3', name: 'web_events.parquet', type: 'parquet', category: 'source', size: '85.6M rows' },
  { id: '4', name: 'marketing_data.xlsx', type: 'excel', category: 'source', size: '320K rows' },
  { id: '5', name: 's3://data/logs/', type: 's3', category: 'source', size: '128 files' },
  // Interim
  { id: '6', name: 'stg_customers', type: 'table', category: 'interim', size: '1.2M rows' },
  { id: '7', name: 'stg_transactions', type: 'table', category: 'interim', size: '8.7M rows' },
  { id: '8', name: 'stg_events', type: 'table', category: 'interim', size: '50.1M rows' },
  // Output
  { id: '9', name: 'clean_customers', type: 'table', category: 'output', size: '1.1M rows' },
  { id: '10', name: 'customer_report.csv', type: 'csv', category: 'output', size: '1.1M rows' },
];

export const defaultConfig: PipelineNodeConfig = {
  column: 'customer_id',
  strategy: 'Keep first',
  scope: 'Current dataset',
  nullHandling: 'Ignore',
};

export const initialPipelineNodes: PipelineNode[] = [
  { id: 'n1', type: 'source', name: 'raw_customers.csv', description: 'CSV File · 2.4M rows', rows: '2.4M rows', status: 'completed', config: { ...defaultConfig } },
  { id: 'n2', type: 'filter', name: 'Filter', description: 'Keep valid customers', rows: '1.8M rows', status: 'completed', config: { ...defaultConfig, column: 'status' } },
  { id: 'n3', type: 'deduplicate', name: 'Deduplicate', description: 'Remove repeated records', rows: '1.2M rows', status: 'running', config: { ...defaultConfig } },
  { id: 'n4', type: 'normalize', name: 'Normalize Text', description: 'Standardize name & email', rows: '1.2M rows', status: 'pending', config: { ...defaultConfig } },
  { id: 'n5', type: 'export', name: 'Export CSV', description: 'Write cleaned data', rows: '1.2M rows', status: 'pending', config: { ...defaultConfig } },
];

export const transformObjects: TransformObject[] = [
  { id: 't1', name: 'CSV File', description: 'Import local CSV file', category: 'source', icon: 'file-text', available: true },
  { id: 't2', name: 'S3 Storage', description: 'Read data from S3', category: 'source', icon: 'cloud', available: true },
  { id: 't3', name: 'Database', description: 'Connect to SQL database', category: 'source', icon: 'database', available: true },
  { id: 't4', name: 'Filter', description: 'Keep matched rows', category: 'transform', icon: 'filter', available: true },
  { id: 't5', name: 'Deduplicate', description: 'Remove repeated records', category: 'transform', icon: 'copy', available: true },
  { id: 't6', name: 'Normalize Text', description: 'Clean string values', category: 'transform', icon: 'type', available: true },
  { id: 't7', name: 'Export CSV', description: 'Write to CSV file', category: 'output', icon: 'upload', available: true },
  { id: 't8', name: 'Vector Index', description: 'Publish to vector store', category: 'ai', icon: 'sparkles', available: true },
];
